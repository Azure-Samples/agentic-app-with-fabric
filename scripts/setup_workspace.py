#!/usr/bin/env python3
"""
setup_workspace.py  —  One-Command Fabric Workspace Setup
==========================================================
Automates the entire environment bootstrapping process from scratch:

  1.  Authenticate (az login must be done first, OR supply SP creds via env)
  2.  Select / create a Fabric workspace
  3.  Deploy every artifact from Fabric_artifacts/ in dependency order
  4.  Fetch the Lakehouse SQL analytics endpoint and patch expressions.tmdl
  5.  Re-deploy the SemanticModel with the patched expressions.tmdl
  6.  Create the Power BI SQL views (create_views.sql)
  7.  Retrieve SQL Database / Cosmos DB / EventHub connection details
  8.  Activate the Eventstream pipeline (best-effort)
  9.  Write all connection values into backend/.env

After this script completes you only need to:
  • Run the frontend / backend  (npm run dev  +  python launcher.py)
  • Do one test chat so the Eventstream can detect the data schema
    (then optionally publish the real-time dashboard via the Fabric UI)

Usage
-----
    # Guided interactive run:
    python scripts/setup_workspace.py

    # Non-interactive (CI / headless):
    python scripts/setup_workspace.py \\
        --workspace-name  "AgenticBankingDemo" \\
        --capacity-id     "<fabric-capacity-guid>" \\
        --no-interactive

    # Use an existing workspace instead of creating one:
    python scripts/setup_workspace.py \\
        --workspace-id    "<existing-workspace-guid>"

    # Dry run (no API writes, shows what would happen):
    python scripts/setup_workspace.py --dry-run

Prerequisites
-------------
    pip install requests azure-identity pyodbc python-dotenv
    az login   (or set AZURE_CLIENT_ID / AZURE_TENANT_ID / AZURE_CLIENT_SECRET)
    ODBC Driver 18 for SQL Server installed
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Optional

import requests

# ── Optional dependencies (warn instead of crash) ─────────────────────────────
try:
    import pyodbc
    HAS_PYODBC = True
except ImportError:
    HAS_PYODBC = False

try:
    from azure.identity import (
        AzureCliCredential, ChainedTokenCredential,
        ClientSecretCredential, ManagedIdentityCredential,
        WorkloadIdentityCredential,
    )
    from azure.core.exceptions import ClientAuthenticationError
except ImportError:
    print("ERROR: azure-identity not installed.  Run: pip install azure-identity requests")
    sys.exit(1)

# ── Import shared helpers from direct_deploy_fabric ───────────────────────────
_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE))

try:
    from direct_deploy_fabric import (
        ARTIFACTS_DIR, DEPLOY_PHASES, EXCLUDE_FILES, LOGICAL_ID_TO_TYPE,
        DirectDeployer, build_credential, scan_artifacts,
        load_state, save_state,
        info, ok, warn, err, head,
        G, Y, R, C, B, X,
    )
except ImportError as e:
    print(f"ERROR: Could not import direct_deploy_fabric.py — {e}")
    sys.exit(1)

# ── Constants ──────────────────────────────────────────────────────────────────

FABRIC_API    = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE  = "https://api.fabric.microsoft.com/.default"
SQL_SCOPE     = "https://database.windows.net/.default"
ENV_PATH      = Path("backend/.env")
ENV_SAMPLE    = Path("backend/.env.sample")
VIEWS_SQL     = Path("Data_Ingest/create_views.sql")
EXPRESSIONS   = Path("Fabric_artifacts/banking_semantic_model.SemanticModel/definition/expressions.tmdl")
# Table SQL files in dependency order (FKs: accounts→users, transactions→accounts)
SQL_DB_TABLES_DIR = Path("Fabric_artifacts/agentic_app_db.SQLDatabase/dbo/Tables")
TABLE_CREATION_ORDER = [
    "users.sql", "accounts.sql", "transactions.sql",
    "agent_definitions.sql", "chat_sessions.sql", "agent_traces.sql",
    "tool_definitions.sql", "chat_history.sql", "tool_usage.sql",
    "DocsChunks_Embeddings.sql", "PDF_RawChunks.sql",
]

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


class _InfoOnlyFilter(logging.Filter):
    """Suppress WARNING and ERROR from live output; only INFO passes through."""
    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= logging.INFO

# Apply filter to all root handlers so warn()/err() are silent during the run
for _h in logging.root.handlers:
    _h.addFilter(_InfoOnlyFilter())

STEP_WIDTH = 60

def step(n: int, total: int, msg: str) -> None:
    bar = f"[{n}/{total}]"
    log.info(f"\n{B}{bar:<8}{msg}{X}")


# ── Fabric REST helpers ────────────────────────────────────────────────────────

class FabricAPI:
    """Minimal REST client with token caching and async polling."""

    def __init__(self, credential: ChainedTokenCredential, dry_run: bool = False) -> None:
        self._cred    = credential
        self.dry_run  = dry_run
        self._token: Optional[str] = None
        self._expires: float = 0.0

    def _hdrs(self) -> dict:
        if not self._token or time.time() >= self._expires - 60:
            try:
                t = self._cred.get_token(FABRIC_SCOPE)
                self._token, self._expires = t.token, float(t.expires_on)
            except ClientAuthenticationError as e:
                err(f"Authentication failed: {e}")
                raise SystemExit(1) from e
        return {"Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json"}

    def _raise(self, r: requests.Response) -> None:
        if r.status_code >= 400:
            try:    detail = r.json()
            except Exception: detail = r.text[:500]
            err(f"HTTP {r.status_code}: {detail}")
            r.raise_for_status()

    def _poll(self, location: str, label: str = "", timeout: int = 600) -> Optional[dict]:
        deadline = time.time() + timeout
        while time.time() < deadline:
            r = requests.get(location, headers=self._hdrs(), timeout=30)
            self._raise(r)
            body  = r.json()
            state = body.get("status", "").upper()
            pct   = body.get("percentComplete", 0)
            if state == "SUCCEEDED":
                return body
            if state in ("FAILED", "CANCELLED"):
                err(f"Operation {state}: {body.get('error', {})}")
                raise RuntimeError(f"Async op failed: {label}")
            info(f"  {label}: {state}  {pct}%")
            time.sleep(5)
        raise TimeoutError(f"Timed out: {label}")

    def get(self, path: str, **kw) -> dict:
        r = requests.get(f"{FABRIC_API}{path}", headers=self._hdrs(), timeout=30, **kw)
        self._raise(r)
        return r.json() if r.content else {}

    def post(self, path: str, body: dict, timeout: int = 60) -> dict:
        if self.dry_run:
            ok(f"[DRY RUN] POST {path}")
            return {}
        r = requests.post(f"{FABRIC_API}{path}", headers=self._hdrs(),
                          json=body, timeout=timeout)
        if r.status_code == 202:
            loc = r.headers.get("Location", "")
            if loc:
                result = self._poll(loc, path)
                return result or {}
        if r.status_code in (200, 201):
            return r.json() if r.content else {}
        self._raise(r)
        return {}

    # ── Workspace ──────────────────────────────────────────────────────────────

    def list_capacities(self) -> list[dict]:
        data = self.get("/capacities")
        return data.get("value", [])

    def list_workspaces(self) -> list[dict]:
        """Return all workspaces, following continuation tokens for pagination."""
        items: list[dict] = []
        url = f"{FABRIC_API}/workspaces"
        while url:
            r = requests.get(url, headers=self._hdrs(), timeout=30)
            self._raise(r)
            body = r.json()
            items.extend(body.get("value", []))
            token = body.get("continuationToken") or body.get("continuationUri")
            # continuationUri is a full URL; continuationToken is a query param value
            if token:
                if token.startswith("http"):
                    url = token
                else:
                    sep = "&" if "?" in url else "?"
                    url = f"{FABRIC_API}/workspaces{sep}continuationToken={token}"
            else:
                url = ""
        return items

    def create_workspace(self, name: str, capacity_id: str) -> dict:
        info(f"Creating workspace '{name}' on capacity {capacity_id[:8]}…")
        body = {"displayName": name, "capacityId": capacity_id}
        r = requests.post(f"{FABRIC_API}/workspaces", headers=self._hdrs(),
                          json=body, timeout=60)
        if r.status_code == 202:
            loc = r.headers.get("Location", "")
            if loc:
                self._poll(loc, f"create workspace/{name}")
                # After polling, find the workspace by name
                for ws in self.list_workspaces():
                    if ws.get("displayName") == name:
                        return ws
        if r.status_code in (200, 201):
            return r.json()
        if r.status_code == 409:
            # Workspace already exists (race condition or list_workspaces missed it).
            # Look it up by name and return it so the caller can proceed normally.
            warn(f"Workspace '{name}' already exists (409) — looking it up to reuse…")
            for ws in self.list_workspaces():
                if ws.get("displayName") == name:
                    ok(f"Found existing workspace '{name}'  id={ws['id'][:8]}…")
                    return ws
            # Couldn't find it by name — re-raise the original error
            self._raise(r)
        self._raise(r)
        return {}

    # ── Item properties ────────────────────────────────────────────────────────

    def get_lakehouse(self, ws_id: str, lh_id: str) -> dict:
        return self.get(f"/workspaces/{ws_id}/lakehouses/{lh_id}")

    def get_sql_database(self, ws_id: str, db_id: str) -> dict:
        # Try type-specific endpoint first, fall back to generic items API
        try:
            return self.get(f"/workspaces/{ws_id}/sqlDatabases/{db_id}")
        except Exception:
            return self.get(f"/workspaces/{ws_id}/items/{db_id}")

    def get_cosmosdb(self, ws_id: str, cosmos_id: str) -> dict:
        try:
            return self.get(f"/workspaces/{ws_id}/cosmosdbDatabases/{cosmos_id}")
        except Exception:
            return self.get(f"/workspaces/{ws_id}/items/{cosmos_id}")

    def get_data_agent(self, ws_id: str, agent_id: str) -> dict:
        try:
            return self.get(f"/workspaces/{ws_id}/dataAgents/{agent_id}")
        except Exception:
            return self.get(f"/workspaces/{ws_id}/items/{agent_id}")

    def get_eventstream(self, ws_id: str, stream_id: str) -> dict:
        try:
            return self.get(f"/workspaces/{ws_id}/eventstreams/{stream_id}")
        except Exception:
            return self.get(f"/workspaces/{ws_id}/items/{stream_id}")

    def run_eventstream(self, ws_id: str, stream_id: str) -> bool:
        """Try to activate/run the eventstream. Returns True if successful."""
        try:
            r = requests.post(
                f"{FABRIC_API}/workspaces/{ws_id}/eventstreams/{stream_id}/run",
                headers=self._hdrs(), json={}, timeout=30,
            )
            if r.status_code in (200, 202, 204):
                return True
        except Exception:
            pass
        return False

    def update_item_definition(self, ws_id: str, item_id: str, parts: list[dict],
                               label: str = "") -> None:
        if self.dry_run:
            ok(f"[DRY RUN] update definition for {label}")
            return
        r = requests.post(
            f"{FABRIC_API}/workspaces/{ws_id}/items/{item_id}/updateDefinition",
            headers=self._hdrs(),
            json={"definition": {"parts": parts}},
            timeout=60,
        )
        if r.status_code == 202:
            loc = r.headers.get("Location", "")
            if loc:
                self._poll(loc, f"update {label}")
        elif r.status_code not in (200, 204):
            self._raise(r)


# ── Connection detail extraction ───────────────────────────────────────────────

def _extract_sql_db_conn(item: dict, display_name: str) -> Optional[str]:
    """
    Build an ODBC connection string from a SQLDatabase item response.
    Fabric SQL Database response includes a 'properties' block with
    'serverFqdn' or 'connectionString'.
    """
    props = item.get("properties", {})

    # Case 1: direct connectionString field
    conn_str = props.get("connectionString", "")
    if conn_str and "Server=" in conn_str:
        # Normalise auth
        conn_str = re.sub(r"Authentication=\w+", "Authentication=ActiveDirectoryCli", conn_str)
        if "Authentication=" not in conn_str:
            conn_str = conn_str.rstrip(";") + ";Authentication=ActiveDirectoryCli"
        return conn_str

    # Case 2: serverFqdn + databaseName fields
    server = props.get("serverFqdn") or props.get("sqlEndpointProperties", {}).get("connectionString")
    db     = props.get("databaseName") or display_name
    if server:
        return (f"Driver={{ODBC Driver 18 for SQL Server}};"
                f"Server={server},1433;Database={db};"
                f"Encrypt=yes;TrustServerCertificate=no;"
                f"Connection Timeout=30;Authentication=ActiveDirectoryCli")

    return None


def _extract_lakehouse_sql_endpoint(item: dict) -> tuple[Optional[str], Optional[str]]:
    """
    Return (server_connection_string, lakehouse_sql_guid) from a Lakehouse item response.
    The server is used in Sql.Database() and the GUID as the "database" argument.
    """
    props = item.get("properties", {})
    sql_ep = props.get("sqlEndpointProperties", {})
    server = sql_ep.get("connectionString")
    guid   = sql_ep.get("id")
    return server, guid


def _extract_cosmos_endpoint(item: dict) -> Optional[str]:
    props = item.get("properties", {})
    # Try common top-level field names (Fabric-native CosmosDB and mirrored variants)
    for key in (
        "endpoint", "cosmosEndpoint", "accountEndpoint",
        "documentEndpoint", "serviceEndpoint", "databaseEndpoint",
        "connectionString",
    ):
        val = props.get(key)
        if val and (
            "cosmos" in val.lower()
            or "documents.azure.com" in val.lower()
            or "fabric.microsoft.com" in val.lower()
        ):
            return val
    # Try nested connectionInfo block
    for nested_key in ("connectionInfo", "connectionDetails", "sourceProperties"):
        nested = props.get(nested_key, {})
        if isinstance(nested, dict):
            for key in ("endpoint", "accountEndpoint", "connectionString"):
                val = nested.get(key)
                if val:
                    return val
    return None


def _extract_data_agent_details(
    item: dict, ws_id: str, agent_id: str
) -> tuple[str, str]:
    """
    Return (server_url, tool_name) for a deployed DataAgent item.

    server_url  — the MCP /run endpoint used by FABRIC_DATA_AGENT_SERVER_URL.
                  Constructed from the standard Fabric API pattern; the API
                  response is also checked first in case Fabric exposes it
                  directly via a properties field.
    tool_name   — the MCP tool name used by FABRIC_DATA_AGENT_TOOL_NAME.
                  Read from the response's displayName if a dedicated field
                  is not present (this matches what the Fabric portal shows
                  under the "Model Context Protocol" tab).
    """
    props = item.get("properties", {})

    # Some Fabric API versions surface the URL directly
    server_url = (
        props.get("serverUrl")
        or props.get("mcpServerUrl")
        or props.get("endpointUrl")
    )
    if not server_url:
        server_url = (
            f"https://api.fabric.microsoft.com/v1"
            f"/workspaces/{ws_id}/dataAgents/{agent_id}/run"
        )

    tool_name = (
        props.get("toolName")
        or props.get("mcpToolName")
        or item.get("displayName", "")
        or "Banking_DataAgent"
    )

    return server_url, tool_name


def _extract_eventstream_details(item: dict) -> tuple[Optional[str], Optional[str]]:
    """Return (connection_string, event_hub_name) from an Eventstream item response."""
    props = item.get("properties", {})

    # Try direct fields
    conn = props.get("eventHubConnectionString") or props.get("connectionString")
    name = props.get("eventHubName") or props.get("name")
    if conn and name:
        return conn, name

    # Try digging into sources
    sources = props.get("sources", []) or item.get("sources", [])
    for src in sources:
        src_props = src.get("properties", {})
        conn = src_props.get("connectionString") or src_props.get("eventHubConnectionString")
        name = src_props.get("eventHubName") or src.get("name")
        if conn:
            return conn, name

    return None, None


# ── expressions.tmdl patching ──────────────────────────────────────────────────

def patch_expressions_tmdl(
    server: str,
    lakehouse_guid: str,
    tmdl_path: Path = EXPRESSIONS,
) -> str:
    """
    Patch the expressions.tmdl file in-place and return the updated content.
    The file contains:
        Sql.Database("replace with connection SQL string of lakehouse analytics endpoint",
                     "lakehouse analytics GUID")
    """
    content = tmdl_path.read_text(encoding="utf-8")
    original = content

    # Replace the server argument (first string in Sql.Database(...))
    content = re.sub(
        r'(Sql\.Database\s*\(\s*")([^"]*?)(")',
        lambda m: m.group(1) + server + m.group(3),
        content,
        count=1,
    )

    # Replace the lakehouse GUID argument (second string in Sql.Database(...))
    content = re.sub(
        r'(Sql\.Database\s*\(\s*"[^"]*"\s*,\s*")([^"]*?)(")',
        lambda m: m.group(1) + lakehouse_guid + m.group(3),
        content,
        count=1,
    )

    if content != original:
        tmdl_path.write_text(content, encoding="utf-8")
        ok(f"Patched expressions.tmdl  server={server}  guid={lakehouse_guid[:8]}…")
    else:
        warn("expressions.tmdl: no substitution made (pattern not found — check manually)")

    return content


# ── backend/.env writer ────────────────────────────────────────────────────────

def write_env(values: dict[str, str], env_path: Path = ENV_PATH,
              sample_path: Path = ENV_SAMPLE) -> None:
    """
    Update backend/.env with the supplied key=value pairs.
    Creates the file from .env.sample if it doesn't exist yet.
    """
    # Bootstrap from sample if .env missing
    if not env_path.exists():
        if sample_path.exists():
            import shutil
            shutil.copy(sample_path, env_path)
            info(f"Created {env_path} from {sample_path}")
        else:
            env_path.write_text("", encoding="utf-8")
            info(f"Created empty {env_path}")

    lines = env_path.read_text(encoding="utf-8").splitlines(keepends=True)
    updated_keys: set[str] = set()
    new_lines: list[str] = []

    for line in lines:
        stripped = line.strip()
        matched = False
        for key, val in values.items():
            if stripped.startswith(f"{key}=") or stripped.startswith(f"{key} ="):
                new_lines.append(f'{key}="{val}"\n')
                updated_keys.add(key)
                matched = True
                break
        if not matched:
            new_lines.append(line)

    # Append any keys that weren't already in the file
    for key, val in values.items():
        if key not in updated_keys:
            new_lines.append(f'{key}="{val}"\n')

    env_path.write_text("".join(new_lines), encoding="utf-8")
    ok(f"Updated {env_path} with: {', '.join(values.keys())}")


# ── SQL views ──────────────────────────────────────────────────────────────────

def run_sql_views(connection_string: str, sql_file: Path = VIEWS_SQL,
                  dry_run: bool = False) -> bool:
    """Execute the views SQL file against the Lakehouse SQL analytics endpoint."""
    if not HAS_PYODBC:
        warn("pyodbc not installed — skipping SQL views. Install with: pip install pyodbc")
        return False
    if not sql_file.exists():
        warn(f"SQL file not found: {sql_file}")
        return False
    if dry_run:
        ok(f"[DRY RUN] Would run {sql_file} against SQL analytics endpoint")
        return True

    # Import the existing setup_sql_views helper
    try:
        from setup_sql_views import run_views_setup
        rc = run_views_setup(
            connection_string=connection_string,
            sql_file=sql_file,
            drop_existing=False,
        )
        return rc == 0
    except ImportError:
        warn("setup_sql_views.py not found — running inline SQL execution")

    # Inline fallback
    sql = sql_file.read_text(encoding="utf-8")
    batches = [b.strip() for b in re.split(r"^\s*GO\s*$", sql,
               flags=re.IGNORECASE | re.MULTILINE) if b.strip()]
    conn_str = re.sub(r"Authentication=\w+", "Authentication=ActiveDirectoryCli",
                      connection_string)
    try:
        conn = pyodbc.connect(conn_str)
        conn.autocommit = True
        cur = conn.cursor()
        for batch in batches:
            try:
                cur.execute(batch)
            except Exception as e:
                warn(f"View batch error (continuing): {e}")
        cur.close()
        conn.close()
        ok(f"SQL views created ({len(batches)} batches executed)")
        return True
    except Exception as e:
        err(f"SQL views setup failed: {e}")
        return False


# ── Interactive prompts ────────────────────────────────────────────────────────

def prompt_capacity(capacities: list[dict]) -> str:
    """Ask the user to pick a Fabric capacity and return its ID."""
    if not capacities:
        err("No Fabric capacities found. Ensure your account has access to Fabric capacity.")
        raise SystemExit(1)

    print(f"\n{B}Available Fabric Capacities:{X}")
    for i, cap in enumerate(capacities):
        sku   = cap.get("sku", "?")
        state = cap.get("state", "?")
        name  = cap.get("displayName", "?")
        cid   = cap.get("id", "?")
        print(f"  [{i+1}] {name:<35} SKU={sku:<6} State={state}  id={cid[:8]}…")

    while True:
        try:
            choice = input(f"\nSelect capacity [1-{len(capacities)}]: ").strip()
            idx = int(choice) - 1
            if 0 <= idx < len(capacities):
                return capacities[idx]["id"]
        except (ValueError, KeyboardInterrupt):
            pass
        print("  Invalid choice, try again.")


def prompt_workspace(workspaces: list[dict]) -> Optional[str]:
    """
    Ask whether to create a new workspace or use an existing one.
    Returns existing workspace ID, or None to create new.
    """
    print(f"\n{B}Existing Workspaces (showing first 20):{X}")
    for i, ws in enumerate(workspaces[:20]):
        print(f"  [{i+1}] {ws.get('displayName','?'):<40}  id={ws['id'][:8]}…")
    print(f"  [N] Create a NEW workspace")

    while True:
        try:
            choice = input(f"\nChoose [1-{min(20,len(workspaces))} / N]: ").strip().upper()
            if choice == "N":
                return None
            idx = int(choice) - 1
            if 0 <= idx < min(20, len(workspaces)):
                return workspaces[idx]["id"]
        except (ValueError, KeyboardInterrupt):
            pass
        print("  Invalid choice, try again.")


# ── Main orchestration class ───────────────────────────────────────────────────

class WorkspaceSetup:
    # Pipeline:
    #  1  Workspace
    #  2  SQL Database (deploy)
    #  3  SQL Database Tables (create via ODBC)
    #  4  Cosmos DB (deploy)
    #  5  Lakehouse (deploy + shortcuts)
    #  6  Wait for Lakehouse SQL Analytics Endpoint
    #  7  Patch + Deploy Semantic Model
    #  8  Deploy Report & DataAgent
    #  9  Deploy Eventhouse, KQL, Eventstream & Dashboards
    # 10  Deploy Notebook
    # 11  Retrieve Connection Details
    # 12  Populate backend/.env
    # 13  Activate Eventstream
    # 14  Summary
    TOTAL_STEPS = 14

    def __init__(
        self,
        workspace_name:  str,
        workspace_id:    Optional[str],
        capacity_id:     Optional[str],
        artifacts_dir:   Path,
        env_path:        Path,
        dry_run:         bool,
        interactive:     bool,
        force_redeploy:  bool,
    ) -> None:
        self.workspace_name  = workspace_name
        self.workspace_id    = workspace_id
        self.capacity_id     = capacity_id
        self.artifacts_dir   = artifacts_dir
        self.env_path        = env_path
        self.dry_run         = dry_run
        self.interactive     = interactive
        self.force_redeploy  = force_redeploy

        self.credential = build_credential()
        self.api        = FabricAPI(self.credential, dry_run)

        # logicalId → actual item ID; kept in sync after every deploy_phase() call
        self.deployed_ids: dict[str, str] = {}
        self.connection_details: dict[str, str] = {}
        self.warnings: list[str] = []
        self.deployer: Optional[DirectDeployer] = None

    # ── Step helpers ──────────────────────────────────────────────────────────

    def _step(self, n: int, msg: str) -> None:
        step(n, self.TOTAL_STEPS, msg)

    def _id_for_type(self, item_type: str) -> Optional[str]:
        """Reverse-lookup: find the deployed item ID for a given item type."""
        for lid, itype in LOGICAL_ID_TO_TYPE.items():
            if itype == item_type:
                return self.deployed_ids.get(lid)
        return None

    def _make_deployer(self, workspace_id: str) -> DirectDeployer:
        """Create a single DirectDeployer that persists state across all phase calls."""
        return DirectDeployer(
            workspace_id=workspace_id,
            credential=self.credential,
            artifacts_dir=self.artifacts_dir,
            dry_run=self.dry_run,
            force=self.force_redeploy,
        )

    def _sync_ids(self, deployer: DirectDeployer) -> None:
        """
        Pull the latest logical→actual ID mappings from the deployer into
        self.deployed_ids so subsequent step methods (which query self.deployed_ids)
        can find the newly deployed item IDs.
        """
        self.deployed_ids.update(deployer.logical_to_actual)
        # Also absorb any IDs that were previously persisted to disk.
        # Skip non-dict entries (e.g. top-level metadata like "workspace_id").
        state = load_state()
        for entry in state.values():
            if not isinstance(entry, dict):
                continue
            lid = entry.get("logicalId", "")
            aid = entry.get("itemId", "")
            if lid and aid and lid not in self.deployed_ids:
                self.deployed_ids[lid] = aid

    def _get_sql_db_conn_str(self, workspace_id: str) -> str:
        """
        Poll the Fabric API until the SQL Database reports status 'Online'
        AND returns a valid connection string.

        Fabric SQL Database is created asynchronously.  The REST API returns
        a 201 Accepted immediately, but the underlying SQL Server instance —
        and especially its permission / auth subsystem — can take 3-10 minutes
        to fully initialise.  Connecting via ODBC before it is 'Online' causes
        error 28000 "Validation of user's permissions failed" even for the
        workspace admin, because the Fabric auth layer isn't ready yet.

        We poll up to 15 minutes (45 × 20 s) so there is no need for a
        separate sleep in the caller.
        """
        sql_logical = "35fad3ec-f95a-87e5-435e-e81d13cb5ae1"
        sql_id = self.deployed_ids.get(sql_logical)
        if not sql_id or self.dry_run:
            return "dry-run-sql-conn" if self.dry_run else ""

        info("Waiting for Fabric SQL Database to reach Online status…")
        deadline     = time.time() + 900   # 15-minute hard cap
        last_conn    = ""
        poll_attempt = 0

        while time.time() < deadline:
            poll_attempt += 1
            try:
                db_item  = self.api.get_sql_database(workspace_id, sql_id)
                props    = db_item.get("properties", {})

                # Fabric may surface readiness through different field names.
                status = (
                    props.get("status")
                    or props.get("provisioningStatus")
                    or props.get("state")
                    or ""
                ).lower()

                conn_str = _extract_sql_db_conn(db_item, "agentic_app_db")
                if conn_str:
                    last_conn = conn_str

                if conn_str and (not status or status in ("online", "ready", "succeeded")):
                    ok(f"SQL Database is Online (poll #{poll_attempt}).")
                    return conn_str

                info(f"  Poll #{poll_attempt}: status={status or '?'}, "
                     f"conn={'✓' if conn_str else '…'}  — waiting 20 s…")
            except Exception as exc:
                info(f"  Poll #{poll_attempt}: API call failed ({exc}) — waiting 20 s…")

            time.sleep(20)

        # Timed out — return whatever we have and let the ODBC loop try anyway
        if last_conn:
            warn("SQL Database did not report Online within 15 min — attempting ODBC anyway.")
            return last_conn

        warn("Could not retrieve SQL Database connection string from Fabric API.")
        self.warnings.append(
            "FABRIC_SQL_CONNECTION_URL_AGENTIC not auto-populated. "
            "Get it from: Fabric workspace → agentic_app_db → Settings → "
            "Connection strings → ODBC"
        )
        return ""

    # ── Step 1: Workspace ──────────────────────────────────────────────────────

    def step_workspace(self) -> str:
        self._step(1, "Create / Select Workspace")

        if self.workspace_id:
            ok(f"Using existing workspace: {self.workspace_id}")
            return self.workspace_id

        # Need a capacity
        if not self.capacity_id:
            capacities = self.api.list_capacities()
            if len(capacities) == 1:
                self.capacity_id = capacities[0]["id"]
                ok(f"Auto-selected capacity: {capacities[0].get('displayName')} ({self.capacity_id[:8]}…)")
            elif self.interactive:
                self.capacity_id = prompt_capacity(capacities)
            else:
                # Auto-pick first active capacity
                for cap in capacities:
                    if cap.get("state", "").upper() in ("ACTIVE", "RUNNING"):
                        self.capacity_id = cap["id"]
                        ok(f"Auto-selected capacity: {cap.get('displayName')} ({self.capacity_id[:8]}…)")
                        break
                if not self.capacity_id:
                    err("No active capacity found. Pass --capacity-id explicitly.")
                    raise SystemExit(1)

        # Check if workspace with this name already exists
        workspaces = self.api.list_workspaces()
        existing = next((w for w in workspaces if w.get("displayName") == self.workspace_name), None)
        if existing:
            ws_id = existing["id"]
            if self.interactive:
                use_existing = prompt_workspace([existing])
                if use_existing:
                    ok(f"Using existing workspace '{self.workspace_name}'  id={ws_id}")
                    return ws_id
                # Create new with a different name
                self.workspace_name = self.workspace_name + "-new"
            else:
                ok(f"Workspace '{self.workspace_name}' already exists — reusing (id={ws_id[:8]}…)")
                return ws_id

        if self.dry_run:
            ok(f"[DRY RUN] Would create workspace '{self.workspace_name}'")
            return "dryrun-workspace-id"

        ws = self.api.create_workspace(self.workspace_name, self.capacity_id)
        ws_id = ws.get("id", "")
        ok(f"Created workspace '{self.workspace_name}'  id={ws_id}")
        return ws_id

    # ── Step 6: Wait for Lakehouse SQL endpoint ────────────────────────────────

    def step_wait_for_lakehouse_sql(self, workspace_id: str) -> tuple[str, str]:
        """
        Poll the Lakehouse item until its SQL analytics endpoint is provisioned.
        Returns (server_connection_string, lakehouse_sql_guid).
        """
        self._step(6, "Wait for Lakehouse SQL Analytics Endpoint")

        lh_logical = "fb30712b-d7a8-a06a-4004-373442ecab99"  # agentic_lake logicalId
        lh_id      = self.deployed_ids.get(lh_logical)

        if not lh_id:
            warn("Lakehouse item ID not found in deployed state. Skipping SQL endpoint polling.")
            return "", ""

        if self.dry_run:
            ok("[DRY RUN] Would wait for SQL analytics endpoint")
            return "dry-run-server.datawarehouse.fabric.microsoft.com", "dry-run-guid"

        info(f"Polling Lakehouse SQL analytics endpoint (item={lh_id[:8]}…)…")
        deadline = time.time() + 300  # 5-minute max wait
        while time.time() < deadline:
            try:
                lh = self.api.get_lakehouse(workspace_id, lh_id)
                server, guid = _extract_lakehouse_sql_endpoint(lh)
                if server and guid:
                    ok(f"SQL analytics endpoint ready: {server}")
                    ok(f"Lakehouse SQL GUID: {guid}")
                    return server, guid
            except Exception as e:
                info(f"  Still waiting for SQL endpoint… ({e})")
            time.sleep(10)

        warn("SQL analytics endpoint did not provision within 5 minutes.")
        warn("You may need to manually patch expressions.tmdl later.")
        return "", ""

    # ── Step 4: Patch semantic model ───────────────────────────────────────────

    def step_patch_semantic_model(
        self, workspace_id: str, sql_server: str, lakehouse_guid: str,
        deployer,
    ) -> None:
        self._step(7, "Patch & Deploy Semantic Model")

        if not sql_server or not lakehouse_guid:
            warn("Missing SQL server or lakehouse GUID — skipping semantic model patch.")
            self.warnings.append(
                "SemanticModel NOT patched. Manually edit expressions.tmdl "
                f"and run: python scripts/direct_deploy_fabric.py deploy "
                f"--workspace-id {workspace_id}"
            )
            return

        # Patch expressions.tmdl in-place with the real server + GUID.
        # On re-runs the file is already patched, so patch_expressions_tmdl
        # will report "no substitution made" — that is expected and fine.
        patch_expressions_tmdl(sql_server, lakehouse_guid)

        # Deploy (create on first run, update on re-run) the SemanticModel via
        # deploy_phase so it goes through the same ID-substitution and async-
        # polling logic as every other artifact.  We no longer call
        # update_item_definition directly because the item may not exist yet.
        info("Deploying SemanticModel (create or update)…")
        if not deployer.deploy_phase(["SemanticModel"]):
            warn("SemanticModel deployment may have failed — check output above.")
            self.warnings.append(
                "SemanticModel may not be correctly deployed. "
                "Check output and re-run if needed."
            )
        else:
            ok("SemanticModel patched and deployed.")

    # ── Step 5: SQL views ──────────────────────────────────────────────────────

    def step_sql_views(self, workspace_id: str, sql_server: str, lakehouse_guid: str) -> None:
        self._step(7, "Create SQL Views on Lakehouse Analytics Endpoint")

        if not sql_server:
            warn("No SQL analytics server available — skipping views setup.")
            return

        if not HAS_PYODBC:
            warn("pyodbc not installed — skipping SQL views. Install with: pip install pyodbc")
            return

        if self.dry_run:
            ok(f"[DRY RUN] Would run {VIEWS_SQL} against SQL analytics endpoint")
            return

        if not VIEWS_SQL.exists():
            warn(f"Views SQL file not found: {VIEWS_SQL}")
            return

        # ── Authentication: same token-injection approach as step_create_sql_tables
        # We use self.credential (the workspace deployment identity) so the ODBC
        # identity always matches the workspace admin.  setup_sql_views.py uses its
        # own AzureCliCredential which may differ when the workspace was deployed
        # by a service principal, leading to 28000 / 18456 auth errors.
        import struct
        import pyodbc as _pyodbc

        conn_str = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={sql_server},1433;"
            f"Database=agentic_lake;"
            f"Encrypt=yes;TrustServerCertificate=no;"
            f"Connection Timeout=30"
        )

        info("Acquiring Azure AD token for SQL Analytics Endpoint (deployment credential)…")
        try:
            _tok = self.credential.get_token("https://database.windows.net/.default")

            token_bytes = _tok.token.encode("utf-16-le")

            connect_kwargs: dict = {

                "attrs_before": {

                    1256: struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)

                }

            }
            ok("Token acquired.")
        except Exception as exc:
            err(f"Could not acquire Azure AD token: {exc}")
            return

        # Retry loop — Lakehouse SQL endpoint can take a moment after shortcut
        # creation before the auth subsystem accepts new connections.
        conn = None
        for attempt in range(1, 6):
            info(f"Connecting to SQL Analytics Endpoint (attempt {attempt}/5)…")
            try:
                conn = _pyodbc.connect(conn_str, **connect_kwargs)
                conn.autocommit = True
                ok("Connected to Lakehouse SQL Analytics Endpoint.")
                break
            except Exception as exc:
                if attempt < 5:
                    warn(f"  Connection attempt {attempt} failed: {exc}")
                    info("  Waiting 30 s…")
                    time.sleep(30)
                else:
                    err(f"Failed to connect after 5 attempts: {exc}")
                    return

        # ── Execute views SQL ──────────────────────────────────────────────────
        sql = VIEWS_SQL.read_text(encoding="utf-8")
        batches = [
            b.strip() for b in
            re.split(r"^\s*GO\s*$", sql, flags=re.IGNORECASE | re.MULTILINE)
            if b.strip()
        ]
        info(f"Executing {len(batches)} SQL batch(es) from {VIEWS_SQL.name}…")

        cursor = conn.cursor()
        created = skipped = failed = 0

        for i, batch in enumerate(batches, 1):
            view_match = re.search(
                r"CREATE\s+VIEW\s+(?:\[?dbo\]?\.\[?)?(\w+)\]?",
                batch, re.IGNORECASE
            )
            view_name = view_match.group(1) if view_match else f"batch_{i}"

            # Skip if view already exists
            try:
                cursor.execute(
                    "SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = ?",
                    (view_name,)
                )
                if cursor.fetchone():
                    info(f"  View '{view_name}' already exists — skipping.")
                    skipped += 1
                    continue
            except Exception:
                pass  # If existence check fails, attempt creation anyway

            try:
                cursor.execute(batch)
                ok(f"  Created view: {view_name}")
                created += 1
            except Exception as exc:
                warn(f"  Failed to create view '{view_name}': {exc}")
                failed += 1

        cursor.close()
        conn.close()

        if failed == 0:
            ok(f"SQL views setup complete: {created} created, {skipped} skipped.")
        else:
            warn(f"SQL views completed with errors: {created} created, {skipped} skipped, {failed} FAILED.")

    # ── Step 7: Create SQL Database tables ────────────────────────────────────

    def step_create_sql_tables(self, sql_db_conn: str) -> None:
        self._step(3, "Create SQL Database Tables")

        if not HAS_PYODBC:
            warn("pyodbc not installed — skipping table creation. Install with: pip install pyodbc")
            self.warnings.append("SQL Database tables NOT created. Run agentic_data_tables_setup_day0.sql manually.")
            return
        if not sql_db_conn:
            warn("No SQL Database connection string — skipping table creation.")
            self.warnings.append(
                "SQL Database tables NOT created. Get the connection string from the Fabric workspace "
                "→ agentic_app_db → Settings → Connection strings → ODBC, then run "
                "Data_Ingest/agentic_data_tables_setup_day0.sql manually."
            )
            return
        if self.dry_run:
            ok("[DRY RUN] Would create SQL Database tables")
            return

        # Collect SQL files in dependency order, then any remainder not in the list
        tables_dir = SQL_DB_TABLES_DIR
        if not tables_dir.exists():
            warn(f"Tables directory not found: {tables_dir} — skipping table creation.")
            return

        ordered = [tables_dir / f for f in TABLE_CREATION_ORDER if (tables_dir / f).exists()]
        all_files = set(tables_dir.glob("*.sql"))
        remainder = sorted(f for f in all_files if f.name not in TABLE_CREATION_ORDER)
        sql_files = ordered + remainder

        if not sql_files:
            warn("No .sql files found in tables directory — skipping.")
            return

        # Authentication: token injection via SQL_COPT_SS_ACCESS_TOKEN (attr 1256)
        # -----------------------------------------------------------------------
        # We deliberately avoid Authentication=ActiveDirectoryCli because it is
        # not supported on all ODBC Driver 18 builds/platforms.
        #
        # We use self.credential — the SAME credential chain that deployed the
        # workspace and SQL Database — so the ODBC identity always matches the
        # workspace admin that Fabric authorises.
        #
        # Using a different credential (e.g. AzureCliCredential specifically when
        # the workspace was deployed by a service principal) causes error 28000
        # "Validation of user's permissions failed / Read item permission" because
        # the ODBC identity is not a workspace member.
        import struct
        import pyodbc as _pyodbc

        base       = sql_db_conn.strip().strip('"').strip("'")
        clean_conn = re.sub(r";?Authentication=[^;]+", "", base).rstrip(";")

        info("Acquiring Azure AD token for SQL Database (using deployment credential)…")
        try:
            _tok = self.credential.get_token("https://database.windows.net/.default")

            token_bytes = _tok.token.encode("utf-16-le")

            connect_kwargs: dict = {

                "attrs_before": {

                    1256: struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)

                }

            }
            ok("Token acquired.")
        except Exception as exc:
            err(f"Could not acquire Azure AD token: {exc}")
            err("Ensure az login has been run (developer) or credentials are configured (CI).")
            self.warnings.append(
                "SQL Database tables NOT created — token unavailable. "
                "Run 'az login', then re-run setup, or execute "
                "Data_Ingest/agentic_data_tables_setup_day0.sql manually."
            )
            return

        # Fabric SQL Database can take a few minutes to become accessible after
        # creation.  Retry with 30-second intervals for up to 5 minutes.
        conn = None
        max_attempts = 10
        for attempt in range(1, max_attempts + 1):
            info(f"Connecting to SQL Database to create tables (attempt {attempt}/{max_attempts})…")
            try:
                conn = _pyodbc.connect(clean_conn, **connect_kwargs)
                conn.autocommit = True
                ok("Connected to SQL Database.")
                break
            except Exception as exc:
                if attempt < max_attempts:
                    warn(f"  Connection attempt {attempt} failed: {exc}")
                    info("  Waiting 30 s for SQL Database to become accessible…")
                    time.sleep(30)
                else:
                    err(f"Failed to connect to SQL Database after {max_attempts} attempts: {exc}")
                    self.warnings.append(
                        "SQL Database tables NOT created (connection failed after retries). "
                        "Run Data_Ingest/agentic_data_tables_setup_day0.sql manually."
                    )
                    return

        ok("Connected to SQL Database.")
        cursor = conn.cursor()
        created = skipped = failed = 0

        for sql_file in sql_files:
            sql = sql_file.read_text(encoding="utf-8")
            batches = [b.strip() for b in re.split(r"^\s*GO\s*$", sql,
                       flags=re.IGNORECASE | re.MULTILINE) if b.strip()]
            for batch in batches:
                table_match = re.search(r"CREATE\s+TABLE\s+(?:\[?dbo\]?\.\[?)?(\w+)\]?",
                                        batch, re.IGNORECASE)
                table_name = table_match.group(1) if table_match else sql_file.stem
                try:
                    cursor.execute(
                        "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?",
                        (table_name,)
                    )
                    if cursor.fetchone():
                        info(f"  Table '{table_name}' already exists — skipping.")
                        skipped += 1
                        continue
                    cursor.execute(batch)
                    ok(f"  Created table: {table_name}")
                    created += 1
                except Exception as exc:
                    err(f"  Failed to create table '{table_name}': {exc}")
                    failed += 1

        cursor.close()
        conn.close()

        if failed == 0:
            ok(f"SQL tables setup complete: {created} created, {skipped} skipped.")
        else:
            warn(f"SQL tables setup finished with errors: {created} created, {skipped} skipped, {failed} FAILED.")
            self.warnings.append(
                f"Some SQL tables failed to create ({failed} errors). "
                "Check output above and re-run Data_Ingest/agentic_data_tables_setup_day0.sql manually if needed."
            )

    # ── Step 7: Get all connection details ─────────────────────────────────────

    def step_get_connections(self, workspace_id: str) -> dict[str, str]:
        self._step(11, "Retrieve Connection Details")
        details: dict[str, str] = {}

        # ── SQL Database (agentic_app_db) ──────────────────────────────────────
        sql_logical = "35fad3ec-f95a-87e5-435e-e81d13cb5ae1"
        sql_id = self.deployed_ids.get(sql_logical)
        if sql_id and not self.dry_run:
            info(f"Getting SQL Database connection details ({sql_id[:8]}…)…")
            try:
                db_item = self.api.get_sql_database(workspace_id, sql_id)
                conn_str = _extract_sql_db_conn(db_item, "agentic_app_db")
                if conn_str:
                    details["FABRIC_SQL_CONNECTION_URL_AGENTIC"] = conn_str
                    ok("SQL Database connection string retrieved.")
                else:
                    warn("Could not extract SQL connection string from API response.")
                    self.warnings.append(
                        "FABRIC_SQL_CONNECTION_URL_AGENTIC not populated automatically. "
                        "Get it from: Fabric workspace → agentic_app_db → Settings → "
                        "Connection strings → ODBC (change ActiveDirectoryInteractive → ActiveDirectoryCli)"
                    )
            except Exception as e:
                warn(f"SQL Database API call failed: {e}")
                self.warnings.append(
                    "FABRIC_SQL_CONNECTION_URL_AGENTIC: get manually from workspace → "
                    "agentic_app_db → Settings → Connection strings → ODBC"
                )
        elif self.dry_run:
            details["FABRIC_SQL_CONNECTION_URL_AGENTIC"] = "dry-run-value"

        # ── Cosmos DB ──────────────────────────────────────────────────────────
        cosmos_logical = "87fcaa33-7ad6-b4ca-4394-25e7ce7118b7"
        cosmos_id = self.deployed_ids.get(cosmos_logical)
        if cosmos_id and not self.dry_run:
            info(f"Getting Cosmos DB endpoint ({cosmos_id[:8]}…)…")
            try:
                cosmos_item = self.api.get_cosmosdb(workspace_id, cosmos_id)
                endpoint = _extract_cosmos_endpoint(cosmos_item)
                if endpoint:
                    details["COSMOS_DB_ENDPOINT"] = endpoint
                    ok(f"Cosmos DB endpoint: {endpoint}")
                else:
                    warn("Could not extract Cosmos DB endpoint from API response.")
                    self.warnings.append(
                        "COSMOS_DB_ENDPOINT not populated. "
                        "Get it from: workspace → agentic_cosmos_db → Settings → Connection"
                    )
            except Exception as e:
                warn(f"Cosmos DB API call failed: {e}")
                self.warnings.append(
                    "COSMOS_DB_ENDPOINT: get manually from workspace → "
                    "agentic_cosmos_db → Settings → Connection"
                )
        elif self.dry_run:
            details["COSMOS_DB_ENDPOINT"] = "dry-run-value"

        # ── DataAgent (MCP server URL + tool name) ────────────────────────────
        agent_logical = "3be97f14-70a3-80eb-48bb-96c6e3600c29"
        agent_id = self.deployed_ids.get(agent_logical)
        if agent_id and not self.dry_run:
            info(f"Getting DataAgent details ({agent_id[:8]}…)…")
            try:
                agent_item = self.api.get_data_agent(workspace_id, agent_id)
                server_url, tool_name = _extract_data_agent_details(
                    agent_item, workspace_id, agent_id
                )
                details["FABRIC_DATA_AGENT_SERVER_URL"] = server_url
                details["FABRIC_DATA_AGENT_TOOL_NAME"]  = tool_name
                details["USE_FABRIC_DATA_AGENT"]        = "false"
                ok(f"DataAgent server URL: {server_url}")
                ok(f"DataAgent tool name : {tool_name}")
            except Exception as e:
                warn(f"DataAgent API call failed: {e}")
                # Still construct the URL from known IDs
                server_url = (
                    f"https://api.fabric.microsoft.com/v1"
                    f"/workspaces/{workspace_id}/dataAgents/{agent_id}/run"
                )
                details["FABRIC_DATA_AGENT_SERVER_URL"] = server_url
                details["FABRIC_DATA_AGENT_TOOL_NAME"]  = "Banking_DataAgent"
                details["USE_FABRIC_DATA_AGENT"]        = "false"
                ok(f"DataAgent server URL (constructed): {server_url}")
        elif self.dry_run:
            details["FABRIC_DATA_AGENT_SERVER_URL"] = "dry-run-value"
            details["FABRIC_DATA_AGENT_TOOL_NAME"]  = "dry-run-tool"
            details["USE_FABRIC_DATA_AGENT"]        = "false"

        # ── Eventstream (EventHub) ─────────────────────────────────────────────
        stream_logical = "b43b90ba-f1e3-843b-4a09-80ea104eee0d"
        stream_id = self.deployed_ids.get(stream_logical)
        if stream_id and not self.dry_run:
            info(f"Getting Eventstream connection details ({stream_id[:8]}…)…")
            try:
                stream_item = self.api.get_eventstream(workspace_id, stream_id)
                conn, name = _extract_eventstream_details(stream_item)
                if conn and name:
                    details["FABRIC_EVENT_HUB_CONNECTION_STRING"] = conn
                    details["FABRIC_EVENT_HUB_NAME"]               = name
                    ok(f"EventHub name: {name}")
                    ok("EventHub connection string retrieved.")
                else:
                    warn("EventHub connection string not available via API.")
                    self.warnings.append(
                        "FABRIC_EVENT_HUB_CONNECTION_STRING / FABRIC_EVENT_HUB_NAME: "
                        "get manually from workspace → agentic_stream → "
                        "CustomEndpoint block → SAS Key Authentication tab"
                    )
            except Exception as e:
                warn(f"Eventstream API call failed: {e}")
                self.warnings.append(
                    "Eventstream connection details: open workspace → agentic_stream → "
                    "CustomEndpoint → SAS Key Authentication → copy Connection string-primary key"
                )
        elif self.dry_run:
            details["FABRIC_EVENT_HUB_CONNECTION_STRING"] = "dry-run-value"
            details["FABRIC_EVENT_HUB_NAME"]               = "dry-run-name"

        # ── Cosmos DB database name (static) ───────────────────────────────────
        details.setdefault("COSMOS_DB_DATABASE_NAME", "agentic_cosmos_db")

        return details

    # ── Step 8: Write .env ─────────────────────────────────────────────────────

    def step_write_env(self, connection_details: dict[str, str]) -> None:
        self._step(12, "Populate backend/.env")
        if not connection_details:
            warn("No connection details to write.")
            return
        write_env(connection_details, self.env_path)

    # ── Step 9: Activate Eventstream ──────────────────────────────────────────

    def step_activate_eventstream(self, workspace_id: str) -> None:
        self._step(13, "Activate Eventstream Pipeline")

        stream_logical = "b43b90ba-f1e3-843b-4a09-80ea104eee0d"
        stream_id = self.deployed_ids.get(stream_logical)

        if not stream_id or self.dry_run:
            ok("[DRY RUN] Would attempt to run Eventstream")
            return

        info(f"Attempting to activate Eventstream ({stream_id[:8]}…)…")
        success = self.api.run_eventstream(workspace_id, stream_id)
        if success:
            ok("Eventstream activated via API.")
        else:
            warn("Eventstream 'run' API not available (this is normal).")
            warn("After your first test chat with the app, the streaming will work.")
            self.warnings.append(
                "Eventstream auto-activation not supported via REST API. "
                "After the first app run: open workspace → agentic_stream → "
                "click 'Edit' → 'Publish' to activate real-time streaming."
            )

    # ── Step 10: Final summary ─────────────────────────────────────────────────

    def step_summary(self, workspace_id: str) -> None:
        self._step(14, "Setup Complete")

        print(f"\n{'─'*70}")
        print(f"{B}{'✅ Fabric Workspace Ready':^70}{X}")
        print(f"{'─'*70}\n")

        print(f"  Workspace ID : {G}{workspace_id}{X}")
        print(f"  Workspace URL: {C}https://app.fabric.microsoft.com/groups/{workspace_id}{X}")
        print(f"  .env         : {self.env_path.resolve()}\n")

        # ── Deployed items report ──────────────────────────────────────────────
        state = load_state()
        deployed_entries = [
            (name, meta)
            for name, meta in state.items()
            if isinstance(meta, dict) and meta.get("itemId")
        ]
        if deployed_entries:
            deployed_entries.sort(key=lambda x: (x[1].get("type", ""), x[0]))
            print(f"{B}Deployed items ({len(deployed_entries)}):{X}")
            for name, meta in deployed_entries:
                item_type = meta.get("type", "?")
                item_id   = meta.get("itemId", "?")
                print(f"  {G}✓{X}  {item_type:<22} {name}  {Y}({item_id[:8]}…){X}")
            print()

        # ── Artifact errors ────────────────────────────────────────────────────
        failed_artifacts = self.deployer.failed if self.deployer else []
        if failed_artifacts:
            print(f"{R}Artifacts that failed to create ({len(failed_artifacts)}):{X}")
            for item in failed_artifacts:
                print(f"  {R}✗{X}  {item}")
            print()

        print(f"{Y}⚠  Steps remaining:{X}")
        print(f"  {Y}1.{X} Run:  {C}python scripts/finalize_views_and_report.py{X}  to finalize the deployment")
        print(f"     (creates SQL views, ensures Semantic Model is linked to lakehouse and deploys Power BI Report)")
        for i, w in enumerate(self.warnings, 2):
            print(f"  {Y}{i}.{X} {w}")
        print()

        print(f"{B}Next steps:{X}")
        print(f"  1. Review backend/.env and fill in any missing values shown above")
        print(f"  2. Run:  {C}az login{X}  (use your Fabric account)")
        print(f"  3. Run:  {C}python backend/launcher.py{X}  (Terminal 1)")
        print(f"  4. Run:  {C}npm run dev{X}                  (Terminal 2)")
        print(f"  5. Open: {C}http://localhost:5173{X}")
        print(f"  6. Do one test chat → then publish the Eventstream in Fabric UI\n")

    # ── Full run ───────────────────────────────────────────────────────────────

    def run(self) -> int:
        head("🏦 Agentic Banking App — One-Command Workspace Setup")

        try:
            # ── Step 1: Workspace ──────────────────────────────────────────────
            ws_id = self.step_workspace()

            # Persist workspace_id to deploy-state.json so finalize_views_and_report.py
            # can auto-discover it without requiring a --workspace-id argument.
            _state = load_state()
            _state["workspace_id"] = ws_id
            save_state(_state)

            # Single deployer instance — persists logical→actual ID state across
            # all deploy_phase() calls so each phase can substitute IDs from
            # previously deployed items.
            self.deployer = deployer = self._make_deployer(ws_id)

            # ── Step 2: SQL Database ───────────────────────────────────────────
            # Deploy first so tables can be created immediately after.
            self._step(2, "Deploy SQL Database")
            deployer.deploy_phase(["SQLDatabase"])
            self._sync_ids(deployer)

            # ── Step 3: SQL Database Tables ────────────────────────────────────
            # Create all tables via ODBC right after the DB is provisioned.
            # The retry loop in step_create_sql_tables waits up to 5 min for the
            # DB to become accessible (Fabric SQL DB can take a few minutes).
            sql_db_conn = self._get_sql_db_conn_str(ws_id)
            self.step_create_sql_tables(sql_db_conn)

            # ── Step 4: Cosmos DB ──────────────────────────────────────────────
            # Independent of SQL DB; Lakehouse shortcuts will reference it.
            self._step(4, "Deploy Cosmos DB")
            deployer.deploy_phase(["CosmosDBDatabase"])
            self._sync_ids(deployer)

            # ── Step 5: Lakehouse ──────────────────────────────────────────────
            # After deploying, shortcuts to SQL DB tables AND Cosmos DB containers
            # are created automatically via the Fabric Shortcuts REST API.
            # Both SQL DB (step 2) and Cosmos DB (step 4) must exist first so the
            # shortcut targets resolve correctly.
            self._step(5, "Deploy Lakehouse + Shortcuts")
            deployer.deploy_phase(["Lakehouse"])
            self._sync_ids(deployer)

            # ── Step 6: Wait for Lakehouse SQL Analytics Endpoint ─────────────
            # Fabric provisions the SQL analytics endpoint asynchronously after
            # Lakehouse creation.  We poll until it is ready (up to 5 min).
            sql_server, lakehouse_guid = self.step_wait_for_lakehouse_sql(ws_id)

            # ── Step 7: Semantic Model ─────────────────────────────────────────
            # Patch expressions.tmdl with the real SQL analytics server + GUID,
            # then deploy (or re-deploy) the SemanticModel via deploy_phase so
            # that ID substitution and async polling are handled correctly.
            # On a first run this CREATES the item; on a re-run it UPDATES it.
            self.step_patch_semantic_model(ws_id, sql_server, lakehouse_guid, deployer)
            # Pick up the newly deployed / updated SemanticModel ID so the
            # Report (step 8) can substitute it into definition.pbir.
            self._sync_ids(deployer)

            # ── Step 8: Report & DataAgent ─────────────────────────────────────
            # Both reference the SemanticModel GUID which is now in deployed_ids.
            self._step(8, "Deploy Report & DataAgent")
            deployer.deploy_phase(["Report", "DataAgent"])
            self._sync_ids(deployer)

            # ── Step 9: Eventhouse, KQL, Eventstream, Dashboards ──────────────
            # Deploy in strict dependency order:
            #   Eventhouse → KQLDatabase (child) → Eventstream → Dashboards
            self._step(9, "Deploy Eventhouse, KQL, Eventstream & Dashboards")
            deployer.deploy_phase(["Eventhouse"])
            deployer.deploy_phase(["KQLDatabase"])
            deployer.deploy_phase(["Eventstream"])
            deployer.deploy_phase(["KQLDashboard", "KQLQueryset"])
            self._sync_ids(deployer)

            # ── Step 10: Notebook ──────────────────────────────────────────────
            self._step(10, "Deploy Notebook")
            deployer.deploy_phase(["Notebook"])
            self._sync_ids(deployer)

            # ── Step 11 + 12: Connection details → .env ───────────────────────
            conn = self.step_get_connections(ws_id)
            self.step_write_env(conn)

            # ── Step 13: Activate Eventstream ─────────────────────────────────
            self.step_activate_eventstream(ws_id)

            # ── Step 14: Summary ───────────────────────────────────────────────
            self.step_summary(ws_id)

            return 0

        except KeyboardInterrupt:
            print("\nInterrupted by user.")
            return 130
        except SystemExit:
            raise
        except Exception as e:
            err(f"Unexpected error: {e}")
            import traceback
            traceback.print_exc()
            return 1


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="One-command Fabric workspace setup for the Agentic Banking App.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive guided setup (recommended first-time):
  python scripts/setup_workspace.py

  # Fully automated (CI / non-interactive):
  python scripts/setup_workspace.py \\
      --workspace-name "AgenticBanking-Prod" \\
      --capacity-id "00000000-0000-0000-0000-000000000000" \\
      --no-interactive

  # Redeploy to an existing workspace:
  python scripts/setup_workspace.py \\
      --workspace-id "00000000-0000-0000-0000-000000000000"
""",
    )
    p.add_argument("--workspace-name", "-n",
                   default=os.getenv("FABRIC_WORKSPACE_NAME", "AgenticBankingApp"),
                   help="Name for the new workspace (default: AgenticBankingApp)")
    p.add_argument("--workspace-id", "-w",
                   default=os.getenv("FABRIC_WORKSPACE_ID"),
                   help="Use an existing workspace instead of creating one")
    p.add_argument("--capacity-id", "-c",
                   default=os.getenv("FABRIC_CAPACITY_ID"),
                   help="Fabric capacity GUID (auto-selected if only one is available)")
    p.add_argument("--artifacts-dir",
                   type=Path, default=ARTIFACTS_DIR,
                   help=f"Path to Fabric_artifacts/ (default: {ARTIFACTS_DIR})")
    p.add_argument("--env-file",
                   type=Path, default=ENV_PATH,
                   help=f"backend/.env path (default: {ENV_PATH})")
    p.add_argument("--dry-run",
                   action="store_true",
                   help="Show what would happen without making any API calls")
    p.add_argument("--no-interactive",
                   action="store_true",
                   help="Disable interactive prompts (use for CI / automation)")
    p.add_argument("--force",
                   action="store_true",
                   help="Force re-deploy all items even if they already exist")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    setup = WorkspaceSetup(
        workspace_name  = args.workspace_name,
        workspace_id    = args.workspace_id,
        capacity_id     = args.capacity_id,
        artifacts_dir   = args.artifacts_dir,
        env_path        = args.env_file,
        dry_run         = args.dry_run,
        interactive     = not args.no_interactive,
        force_redeploy  = args.force,
    )
    sys.exit(setup.run())


if __name__ == "__main__":
    main()
