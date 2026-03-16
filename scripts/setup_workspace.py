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

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

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
    # Try common field names
    for key in ("endpoint", "cosmosEndpoint", "accountEndpoint", "connectionString"):
        val = props.get(key)
        if val and ("cosmos" in val.lower() or "documents.azure.com" in val.lower()
                    or "fabric.microsoft.com" in val.lower()):
            return val
    return None


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
    TOTAL_STEPS = 9

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

        # Will be populated during the run
        self.deployed_ids: dict[str, str] = {}  # logicalId → actual item ID
        self.connection_details: dict[str, str] = {}
        self.warnings: list[str] = []

    # ── Step helpers ──────────────────────────────────────────────────────────

    def _step(self, n: int, msg: str) -> None:
        step(n, self.TOTAL_STEPS, msg)

    def _id_for_type(self, item_type: str) -> Optional[str]:
        """Reverse-lookup: find the deployed item ID for a given item type."""
        for lid, itype in LOGICAL_ID_TO_TYPE.items():
            if itype == item_type:
                return self.deployed_ids.get(lid)
        return None

    # ── Step 1: Workspace ──────────────────────────────────────────────────────

    def step_workspace(self) -> str:
        self._step(1, "Workspace")

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

    # ── Step 2: Deploy artifacts ───────────────────────────────────────────────

    def step_deploy_artifacts(self, workspace_id: str) -> None:
        self._step(2, "Deploy Fabric Artifacts")

        deployer = DirectDeployer(
            workspace_id=workspace_id,
            credential=self.credential,
            artifacts_dir=self.artifacts_dir,
            dry_run=self.dry_run,
            force=self.force_redeploy,
        )
        rc = deployer.run()
        if rc != 0:
            err("Some artifacts failed to deploy (see above). Continuing with available items.")

        # Capture the logical → actual ID mapping from the deployer
        self.deployed_ids = deployer.logical_to_actual.copy()

        # Also load from state file (for reruns)
        state = load_state()
        for entry in state.values():
            lid = entry.get("logicalId", "")
            aid = entry.get("itemId", "")
            if lid and aid and lid not in self.deployed_ids:
                self.deployed_ids[lid] = aid

    # ── Step 3: Wait for Lakehouse SQL endpoint ────────────────────────────────

    def step_wait_for_lakehouse_sql(self, workspace_id: str) -> tuple[str, str]:
        """
        Poll the Lakehouse item until its SQL analytics endpoint is provisioned.
        Returns (server_connection_string, lakehouse_sql_guid).
        """
        self._step(3, "Wait for Lakehouse SQL Analytics Endpoint")

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
        self, workspace_id: str, sql_server: str, lakehouse_guid: str
    ) -> None:
        self._step(4, "Patch & Re-deploy SemanticModel")

        if not sql_server or not lakehouse_guid:
            warn("Missing SQL server or lakehouse GUID — skipping semantic model patch.")
            self.warnings.append(
                "SemanticModel NOT patched. Manually edit expressions.tmdl "
                f"and run: python scripts/direct_deploy_fabric.py deploy "
                f"--workspace-id {workspace_id}"
            )
            return

        # Patch the local file
        patch_expressions_tmdl(sql_server, lakehouse_guid)

        # Re-upload the SemanticModel definition
        sm_logical = "c1a22701-9f6d-b640-4e9c-8d1ac4aeec57"
        sm_id = self.deployed_ids.get(sm_logical)
        if not sm_id:
            warn("SemanticModel item ID not found — cannot re-upload definition.")
            return

        sm_folder = self.artifacts_dir / "banking_semantic_model.SemanticModel"
        parts = []
        for f in sorted(sm_folder.rglob("*")):
            if not f.is_file() or f.name in EXCLUDE_FILES:
                continue
            rel = f.relative_to(sm_folder).as_posix()
            content = f.read_bytes()
            encoded = base64.b64encode(content).decode("ascii")
            parts.append({"path": rel, "payload": encoded, "payloadType": "InlineBase64"})

        info(f"Re-uploading SemanticModel definition ({len(parts)} parts)…")
        if not self.dry_run:
            self.api.update_item_definition(
                workspace_id, sm_id, parts, "banking_semantic_model"
            )
        ok("SemanticModel patched and re-deployed.")

    # ── Step 5: SQL views ──────────────────────────────────────────────────────

    def step_sql_views(self, workspace_id: str, sql_server: str, lakehouse_guid: str) -> None:
        self._step(5, "Create SQL Views on Lakehouse Analytics Endpoint")

        if not sql_server:
            warn("No SQL analytics server available — skipping views setup.")
            self.warnings.append("SQL views NOT created. Run setup_sql_views.py manually.")
            return

        # Build ODBC connection string for the Lakehouse SQL analytics endpoint
        conn_str = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={sql_server},1433;"
            f"Database={lakehouse_guid};"
            f"Encrypt=yes;TrustServerCertificate=no;"
            f"Connection Timeout=30;Authentication=ActiveDirectoryCli"
        )
        success = run_sql_views(conn_str, dry_run=self.dry_run)
        if not success:
            self.warnings.append(
                "SQL views may not have been created fully. "
                "Run: python scripts/setup_sql_views.py --connection-string '...' manually."
            )

    # ── Step 6: Get all connection details ─────────────────────────────────────

    def step_get_connections(self, workspace_id: str) -> dict[str, str]:
        self._step(6, "Retrieve Connection Details")
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

    # ── Step 7: Write .env ─────────────────────────────────────────────────────

    def step_write_env(self, connection_details: dict[str, str]) -> None:
        self._step(7, "Populate backend/.env")
        if not connection_details:
            warn("No connection details to write.")
            return
        write_env(connection_details, self.env_path)

    # ── Step 8: Activate Eventstream ──────────────────────────────────────────

    def step_activate_eventstream(self, workspace_id: str) -> None:
        self._step(8, "Activate Eventstream Pipeline")

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

    # ── Step 9: Final summary ──────────────────────────────────────────────────

    def step_summary(self, workspace_id: str) -> None:
        self._step(9, "Setup Complete")

        print(f"\n{'─'*70}")
        print(f"{B}{'✅ Fabric Workspace Ready':^70}{X}")
        print(f"{'─'*70}\n")

        print(f"  Workspace ID : {G}{workspace_id}{X}")
        print(f"  Workspace URL: {C}https://app.fabric.microsoft.com/groups/{workspace_id}{X}")
        print(f"  Artifacts    : {len(self.deployed_ids)} item(s) deployed")
        print(f"  .env         : {self.env_path.resolve()}\n")

        if self.warnings:
            print(f"{Y}⚠  Manual steps remaining:{X}")
            for i, w in enumerate(self.warnings, 1):
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
            # 1. Workspace
            ws_id = self.step_workspace()

            # 2. Deploy all artifacts
            self.step_deploy_artifacts(ws_id)

            # 3. Wait for Lakehouse SQL endpoint
            sql_server, lakehouse_guid = self.step_wait_for_lakehouse_sql(ws_id)

            # 4. Patch + re-deploy SemanticModel
            self.step_patch_semantic_model(ws_id, sql_server, lakehouse_guid)

            # 5. SQL views
            self.step_sql_views(ws_id, sql_server, lakehouse_guid)

            # 6. Get connection details
            conn = self.step_get_connections(ws_id)

            # 7. Write .env
            self.step_write_env(conn)

            # 8. Activate Eventstream
            self.step_activate_eventstream(ws_id)

            # 9. Summary
            self.step_summary(ws_id)

            return 0 if not self.warnings else 0  # warnings ≠ failure

        except KeyboardInterrupt:
            print("\nInterrupted by user.")
            return 130
        except SystemExit as e:
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
