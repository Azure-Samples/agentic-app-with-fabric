#!/usr/bin/env python3
"""
direct_deploy_fabric.py — Deploy Fabric Artifacts via REST API (No Git Required)
=================================================================================
Reads every artifact from the local Fabric_artifacts/ directory and creates or
updates the corresponding item in a Fabric workspace using the Fabric REST API.

This script works completely independently of any Fabric ↔ GitHub Git integration.
It is suitable for deploying to multiple environments (dev / test / prod) by
supplying different --workspace-id values.

How it works
------------
1. Scan Fabric_artifacts/ and parse each item's .platform file.
2. Fetch all existing items in the target workspace.
3. Deploy items in dependency-aware phases:
     Phase 1 — independent items: SQLDatabase, CosmosDBDatabase, Eventhouse
     Phase 2 — need Phase 1 IDs: KQLDatabase (child of Eventhouse), Lakehouse
     Phase 3 — needs Phase 2 IDs: Eventstream
     Phase 4 — analytics layer:   SemanticModel, Report, Notebook, DataAgent,
                                   KQLDashboard, KQLQueryset
4. Before uploading each item's definition, substitute any logicalId placeholders
   that appear in cross-reference files (shortcuts.metadata.json, eventstream.json,
   DatabaseProperties.json, notebook-content.py) with the real deployed item IDs.
5. Save a deployment state file (deploy-state.json) so reruns skip already-current
   items.

Usage
-----
    # Status check (dry run)
    python scripts/direct_deploy_fabric.py --workspace-id <id> --dry-run

    # Full deploy
    python scripts/direct_deploy_fabric.py --workspace-id <id>

    # Deploy to a different environment
    python scripts/direct_deploy_fabric.py --workspace-id <prod-id> \\
        --sql-server-connection "Server=xyz.database.fabric.microsoft.com" \\
        --lakehouse-guid "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

    # Force re-deploy even if items already exist
    python scripts/direct_deploy_fabric.py --workspace-id <id> --force

Authentication
--------------
    Checked in order: GitHub Actions OIDC → Service Principal secret →
                      Managed Identity → az login (local dev)

    Required: The identity must have Admin or Contributor role on the workspace.

Environment variables / GitHub Secrets
---------------------------------------
    FABRIC_WORKSPACE_ID                  workspace GUID
    AZURE_CLIENT_ID                      SP / OIDC client ID
    AZURE_TENANT_ID                      tenant ID
    AZURE_CLIENT_SECRET                  SP secret (not needed for OIDC)
    AZURE_SUBSCRIPTION_ID                subscription ID
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

import requests

try:
    from azure.identity import (
        AzureCliCredential,
        ChainedTokenCredential,
        ClientSecretCredential,
        ManagedIdentityCredential,
        WorkloadIdentityCredential,
    )
    from azure.core.exceptions import ClientAuthenticationError
except ImportError:
    print("ERROR: azure-identity not installed — run: pip install azure-identity requests")
    sys.exit(1)

# ── Constants ──────────────────────────────────────────────────────────────────

FABRIC_API     = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE   = "https://api.fabric.microsoft.com/.default"
ARTIFACTS_DIR  = Path("Fabric_artifacts")
STATE_FILE     = Path("scripts/deploy-state.json")
PLATFORM_FILE  = ".platform"

# logicalId → type for cross-reference substitution
# These are the logicalIds embedded in cross-reference definition files
LOGICAL_ID_TO_TYPE = {
    "35fad3ec-f95a-87e5-435e-e81d13cb5ae1": "SQLDatabase",       # agentic_app_db
    "87fcaa33-7ad6-b4ca-4394-25e7ce7118b7": "CosmosDBDatabase",  # agentic_cosmos_db
    "51e669b7-3c9a-bcad-444b-9003b479cd41": "Eventhouse",        # AgenticEventHouse
    "b5524f9b-9b13-84b0-4931-d533ccff02c9": "KQLDatabase",       # app_events
    "fb30712b-d7a8-a06a-4004-373442ecab99": "Lakehouse",         # agentic_lake
    "b43b90ba-f1e3-843b-4a09-80ea104eee0d": "Eventstream",       # agentic_stream
    "c1a22701-9f6d-b640-4e9c-8d1ac4aeec57": "SemanticModel",     # banking_semantic_model
    "b4cc5129-4df8-8b4e-4df5-0e83f3a01efb": "Report",            # Agentic_Insights
    "fb6eb403-7f69-b029-4042-3791852c6b3c": "Notebook",          # QA_Evaluation_Notebook
    "3be97f14-70a3-80eb-48bb-96c6e3600c29": "DataAgent",         # Banking_DataAgent
    "e192cca9-882d-bfce-4689-14d6fe8e92ca": "KQLDashboard",      # ContentSafetyMonitoring
    "1c1fcf8e-a488-804c-4cd4-0a116ba12b46": "KQLQueryset",       # QueryWorkBench
}

# Deployment phases — strict dependency order.
# Phase 1  SQLDatabase      : tables created via ODBC right after (in setup_workspace.py)
# Phase 2  CosmosDBDatabase : shortcuts in the Lakehouse will point here
# Phase 3  Lakehouse        : shortcuts (SQL DB + Cosmos) created via REST API after deploy
# Phase 4  SemanticModel    : needs Lakehouse SQL endpoint + views to already exist
# Phase 5  Report+DataAgent : reference SemanticModel GUID
# Phase 6  Eventhouse       : parent of KQLDatabase
# Phase 7  KQLDatabase      : child of Eventhouse
# Phase 8  Eventstream      : references Eventhouse / KQL IDs
# Phase 9  KQLDashboard+KQLQueryset : consumers of the KQL layer
# Phase 10 Notebook         : standalone; last
DEPLOY_PHASES: list[list[str]] = [
    ["SQLDatabase"],
    ["CosmosDBDatabase"],
    ["Lakehouse"],
    ["SemanticModel"],
    ["Report", "DataAgent"],
    ["Eventhouse"],
    ["KQLDatabase"],
    ["Eventstream"],
    ["KQLDashboard", "KQLQueryset"],
    ["Notebook"],
]

# Files to exclude from definition parts (metadata-only or CI tooling)
EXCLUDE_FILES = {".platform", ".gitignore", ".gitkeep", ".DS_Store"}

# ── Colour helpers ─────────────────────────────────────────────────────────────

_TTY = sys.stdout.isatty()
G = "\033[32m" if _TTY else ""  # green
Y = "\033[33m" if _TTY else ""  # yellow
R = "\033[31m" if _TTY else ""  # red
C = "\033[36m" if _TTY else ""  # cyan
B = "\033[1m"  if _TTY else ""  # bold
X = "\033[0m"  if _TTY else ""  # reset

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s",
                    datefmt="%H:%M:%S", stream=sys.stdout)
log = logging.getLogger(__name__)

def info(m): log.info(f"{C}·{X} {m}")
def ok(m):   log.info(f"{G}✓{X} {m}")
def warn(m): log.warning(f"{Y}⚠{X} {m}")
def err(m):  log.error(f"{R}✗{X} {m}")
def head(m): log.info(f"\n{B}{m}{X}")


# ── Authentication ─────────────────────────────────────────────────────────────

def build_credential() -> ChainedTokenCredential:
    creds = []
    if os.getenv("AZURE_FEDERATED_TOKEN_FILE") or (
        os.getenv("AZURE_CLIENT_ID") and os.getenv("AZURE_TENANT_ID")
        and not os.getenv("AZURE_CLIENT_SECRET")
    ):
        try:
            creds.append(WorkloadIdentityCredential())
        except Exception:
            pass

    if all(os.getenv(k) for k in ("AZURE_CLIENT_ID", "AZURE_TENANT_ID", "AZURE_CLIENT_SECRET")):
        creds.append(ClientSecretCredential(
            os.environ["AZURE_TENANT_ID"],
            os.environ["AZURE_CLIENT_ID"],
            os.environ["AZURE_CLIENT_SECRET"],
        ))

    if os.getenv("IDENTITY_ENDPOINT") or os.getenv("IMDS_ENDPOINT"):
        creds.append(ManagedIdentityCredential())

    creds.append(AzureCliCredential())
    return ChainedTokenCredential(*creds)


# ── Fabric REST client ─────────────────────────────────────────────────────────

class FabricClient:
    def __init__(self, workspace_id: str, credential: ChainedTokenCredential,
                 dry_run: bool = False) -> None:
        self.workspace_id = workspace_id
        self._cred = credential
        self.dry_run = dry_run
        self._token: Optional[str] = None
        self._expires: float = 0.0

    def _hdrs(self) -> dict:
        if not self._token or time.time() >= self._expires - 60:
            try:
                t = self._cred.get_token(FABRIC_SCOPE)
                self._token, self._expires = t.token, float(t.expires_on)
            except ClientAuthenticationError as e:
                err(f"Auth failed: {e}")
                raise SystemExit(1) from e
        return {"Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json"}

    def _raise(self, r: requests.Response) -> None:
        if r.status_code >= 400:
            try:
                detail = r.json()
            except Exception:
                detail = r.text[:400]
            err(f"HTTP {r.status_code}: {detail}")
            r.raise_for_status()

    def _wait_async(self, op_url: str, label: str = "", timeout: int = 300) -> dict:
        """Poll a 202 Location URL until the operation completes.

        Always returns a dict.  When the operation creates an item the returned
        dict is guaranteed to contain ``{"id": "<item-guid>"}`` so callers can
        reliably use ``result.get("id")``.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            r = requests.get(op_url, headers=self._hdrs(), timeout=30)
            self._raise(r)
            body = r.json()
            state = body.get("status", "").upper()
            pct   = body.get("percentComplete", 0)
            if state == "SUCCEEDED":
                # Primary: Fabric returns the new item's GUID in createdItemId
                created_id = body.get("createdItemId")
                if _is_guid(created_id):
                    return {"id": created_id}

                # Fallback: some item types expose a /result sub-resource that
                # returns the full item JSON (including its real "id").
                try:
                    r2 = requests.get(op_url + "/result", headers=self._hdrs(), timeout=30)
                    if r2.status_code == 200 and r2.content:
                        r2_body = r2.json()
                        if _is_guid(r2_body.get("id")):
                            return r2_body
                except Exception:
                    pass

                # Last resort: return the raw operation body.  The caller will
                # detect the missing / non-GUID id and look the item up by name.
                return body

            if state in ("FAILED", "CANCELLED"):
                err(f"Operation {state}: {body.get('error', {})}")
                raise RuntimeError(f"Async operation failed for {label}")
            info(f"  {label}: {state} {pct}%")
            time.sleep(5)
        raise TimeoutError(f"Timed out waiting for operation: {label}")

    def _handle_response(self, r: requests.Response, label: str) -> dict:
        """Normalise 200/202 responses into a result dict."""
        if r.status_code == 200:
            return r.json() if r.content else {}
        if r.status_code == 201:
            return r.json() if r.content else {}
        if r.status_code == 202:
            location = r.headers.get("Location", "")
            if location:
                result = self._wait_async(location, label)
                if isinstance(result, str):
                    return {"id": result}
                return result or {}
            return {}
        self._raise(r)
        return {}

    # ── Item API ───────────────────────────────────────────────────────────────

    def list_items(self, item_type: Optional[str] = None) -> list[dict]:
        url = f"{FABRIC_API}/workspaces/{self.workspace_id}/items"
        if item_type:
            url += f"?type={item_type}"
        items, continuation = [], None
        while True:
            params = {}
            if continuation:
                params["continuationToken"] = continuation
            r = requests.get(url, headers=self._hdrs(), params=params, timeout=30)
            self._raise(r)
            body = r.json()
            items.extend(body.get("value", []))
            continuation = body.get("continuationToken")
            if not continuation:
                break
        return items

    def get_all_items(self) -> dict[tuple[str, str], dict]:
        """Return a dict keyed by (displayName, type) → item dict."""
        return {(i["displayName"], i["type"]): i for i in self.list_items()}

    def create_item(self, display_name: str, item_type: str,
                    parts: list[dict],
                    creation_payload: Optional[dict] = None,
                    description: str = "") -> dict:
        """POST a new item and return the created item dict."""
        if self.dry_run:
            ok(f"[DRY RUN] Would create {item_type}: {display_name}")
            return {"id": f"dryrun-{display_name}", "displayName": display_name, "type": item_type}

        body: dict = {
            "displayName": display_name,
            "type": item_type,
        }
        if description:
            body["description"] = description
        if parts:
            body["definition"] = {"parts": parts}
        if creation_payload:
            body["creationPayload"] = creation_payload

        r = requests.post(
            f"{FABRIC_API}/workspaces/{self.workspace_id}/items",
            headers=self._hdrs(), json=body, timeout=60,
        )
        result = self._handle_response(r, f"create {item_type}/{display_name}")

        # --- Extract a real item GUID from the response ---
        # _handle_response returns either:
        #   • {"id": "<item-guid>", ...}  — direct 200/201 or async with createdItemId
        #   • the raw operation body      — async without createdItemId (id = operation UUID)
        item_id = result.get("id") if result else None

        if not _is_guid(item_id):
            # The "id" we got is either an operation UUID or absent.
            # Look the item up by displayName + type to get the real item GUID.
            info(f"  Item GUID not in response — looking up {item_type}/{display_name} …")
            try:
                for item in self.list_items(item_type):
                    if item.get("displayName") == display_name:
                        item_id = item["id"]
                        result   = item
                        break
            except Exception as lookup_exc:
                warn(f"  Workspace lookup failed for {item_type}/{display_name}: {lookup_exc}")

        if not _is_guid(item_id):
            item_id = f"unknown-{display_name}"
            warn(f"  Could not determine item GUID for {item_type}/{display_name} — "
                 "cross-references to this item may fail in later phases")

        ok(f"Created {item_type}: {display_name}  id={item_id}")
        return result if result else {"id": item_id, "displayName": display_name, "type": item_type}

    def update_definition(self, item_id: str, parts: list[dict],
                          display_name: str = "", item_type: str = "") -> None:
        """POST updateDefinition for an existing item."""
        if self.dry_run:
            ok(f"[DRY RUN] Would update {item_type}: {display_name}")
            return

        r = requests.post(
            f"{FABRIC_API}/workspaces/{self.workspace_id}/items/{item_id}/updateDefinition",
            headers=self._hdrs(),
            json={"definition": {"parts": parts}},
            timeout=60,
        )
        if r.status_code == 202:
            location = r.headers.get("Location", "")
            if location:
                self._wait_async(location, f"update {item_type}/{display_name}")
        elif r.status_code not in (200, 204):
            self._raise(r)
        ok(f"Updated {item_type}: {display_name}")

    def create_shortcut(self, lakehouse_id: str, name: str, path: str,
                        target: dict) -> bool:
        """
        Create a single OneLake shortcut inside a Lakehouse via the Fabric REST API.
        Returns True on success or if the shortcut already exists (HTTP 409).

        Fabric REST API silently ignores shortcuts.metadata.json in Lakehouse
        definition parts — shortcuts MUST be created through this endpoint.
        """
        if self.dry_run:
            ok(f"[DRY RUN] Would create shortcut '{name}' at {path}")
            return True
        r = requests.post(
            f"{FABRIC_API}/workspaces/{self.workspace_id}/lakehouses/{lakehouse_id}/shortcuts",
            headers=self._hdrs(),
            json={"name": name, "path": path, "target": target},
            timeout=30,
        )
        if r.status_code == 409:
            info(f"  Shortcut '{name}' already exists — skipping.")
            return True
        if r.status_code >= 400:
            try:
                detail = r.json()
            except Exception:
                detail = r.text[:300]
            warn(f"  Shortcut '{name}' failed (HTTP {r.status_code}): {detail}")
            return False
        return True


# ── Artifact scanner ───────────────────────────────────────────────────────────

def read_platform(folder: Path) -> Optional[dict]:
    """Read and parse the .platform file in a folder. Returns None if not found."""
    p = folder / PLATFORM_FILE
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        warn(f"Could not parse {p}: {e}")
        return None


def folder_to_parts(folder: Path, base: Optional[Path] = None) -> list[dict]:
    """
    Walk a folder and return a list of definition parts (path + base64 payload).
    Skips files in EXCLUDE_FILES and the .children sub-directory.
    """
    if base is None:
        base = folder
    parts = []
    for f in sorted(folder.rglob("*")):
        if not f.is_file():
            continue
        # Skip excluded filenames
        if f.name in EXCLUDE_FILES:
            continue
        # Skip the .children sub-folder (those are separate items).
        # Use the path RELATIVE to base so that child artifacts (whose own
        # folder is inside .children) don't accidentally skip their own files.
        rel = f.relative_to(base).as_posix()
        if ".children" in f.relative_to(base).parts:
            continue
        content = f.read_bytes()
        encoded = base64.b64encode(content).decode("ascii")
        parts.append({"path": rel, "payload": encoded, "payloadType": "InlineBase64"})
    return parts


# ── SQLDatabase dacpac builder ─────────────────────────────────────────────────

def build_dacpac(folder: Path) -> Optional[Path]:
    """
    Compile a Fabric SQL Database project (.sqlproj) into a .dacpac file.

    Fabric's REST API requires a compiled .dacpac — it rejects raw .sql/.sqlproj files
    with: SqlDbNative.ALM.MissingDacpacFile

    Returns the Path to the built .dacpac, or None when:
      • No .sqlproj exists in the folder
      • 'dotnet' CLI is not installed
      • The build fails (stderr is logged as a warning)
    """
    sqlproj = next(folder.glob("*.sqlproj"), None)
    if not sqlproj:
        return None

    if not shutil.which("dotnet"):
        warn("'dotnet' CLI not found — cannot build .dacpac for SQLDatabase. "
             "Install .NET SDK (https://dotnet.microsoft.com/download) to deploy the schema. "
             "Falling back to empty database creation (backend will create tables on startup).")
        return None

    info(f"Building SQL project: {sqlproj.name} …")
    result = subprocess.run(
        ["dotnet", "build", str(sqlproj), "--configuration", "Release", "--nologo"],
        capture_output=True,
        text=True,
        cwd=str(folder),
    )

    if result.returncode != 0:
        warn(f"dotnet build failed (exit {result.returncode}):\n{result.stderr.strip()}")
        warn("Falling back to empty database creation — backend will create tables on startup.")
        return None

    # dotnet build puts output in bin/Release/ or bin/Debug/
    dacpac = next(folder.rglob("*.dacpac"), None)
    if dacpac:
        ok(f"Built dacpac: {dacpac.relative_to(folder)}")
        return dacpac

    warn("dotnet build succeeded but no .dacpac file was found in the output directory.")
    return None


def sql_db_parts(folder: Path) -> list[dict]:
    """
    Return definition parts for a SQLDatabase item.

    Tries to compile the .sqlproj into a .dacpac first.
    If compilation is not possible, returns an empty list so that the caller
    creates an empty database (the backend populates the schema on first startup).
    """
    dacpac = build_dacpac(folder)
    if dacpac is None:
        warn(f"SQLDatabase '{folder.name}': deploying as empty database "
             "(schema will be created by the backend on startup).")
        return []  # empty parts → Fabric creates a blank SQL database

    encoded = base64.b64encode(dacpac.read_bytes()).decode("ascii")
    return [{"path": dacpac.name, "payload": encoded, "payloadType": "InlineBase64"}]


def scan_artifacts(artifacts_dir: Path) -> list[dict]:
    """
    Scan Fabric_artifacts/ and return a list of artifact descriptors, each with:
        display_name, item_type, logical_id, folder, description, child_of (for KQLDatabase)
    """
    artifacts = []
    for folder in sorted(artifacts_dir.iterdir()):
        if not folder.is_dir() or folder.name.startswith("."):
            continue
        platform = read_platform(folder)
        if not platform:
            warn(f"No .platform in {folder.name} — skipping")
            continue
        meta = platform.get("metadata", {})
        cfg  = platform.get("config", {})
        artifact = {
            "display_name": meta.get("displayName", folder.name),
            "item_type":    meta.get("type", "Unknown"),
            "logical_id":   cfg.get("logicalId", ""),
            "description":  meta.get("description", ""),
            "folder":       folder,
            "child_of":     None,
        }
        artifacts.append(artifact)

        # Scan .children sub-folder (e.g. KQLDatabase inside Eventhouse)
        children_dir = folder / ".children"
        if children_dir.exists():
            for child_folder in sorted(children_dir.iterdir()):
                if not child_folder.is_dir():
                    continue
                child_platform = read_platform(child_folder)
                if not child_platform:
                    continue
                child_meta = child_platform.get("metadata", {})
                child_cfg  = child_platform.get("config", {})
                child_artifact = {
                    "display_name": child_meta.get("displayName", child_folder.name),
                    "item_type":    child_meta.get("type", "Unknown"),
                    "logical_id":   child_cfg.get("logicalId", ""),
                    "description":  child_meta.get("description", ""),
                    "folder":       child_folder,
                    "child_of":     artifact["logical_id"],  # parent's logicalId
                }
                artifacts.append(child_artifact)

    return artifacts


# ── GUID helper ────────────────────────────────────────────────────────────────

def _is_guid(value: object) -> bool:
    """Return True if *value* is a string that looks like a well-formed UUID/GUID."""
    if not isinstance(value, str):
        return False
    return bool(re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        value.lower(),
    ))


# ── ID substitution ────────────────────────────────────────────────────────────

def substitute_ids(
    content: str,
    logical_to_actual: dict[str, str],
    workspace_id: str,
) -> str:
    """
    Replace every known logicalId and the 00000000 workspace placeholder in
    a text blob (JSON, TMDL, Python) with the real deployed item IDs.
    """
    # Replace workspace placeholder
    content = content.replace("00000000-0000-0000-0000-000000000000", workspace_id)

    # Replace logicalIds with their actual deployed item IDs
    for logical_id, actual_id in logical_to_actual.items():
        if logical_id and actual_id and logical_id != actual_id:
            content = content.replace(logical_id, actual_id)

    return content


def parts_with_substitution(
    folder: Path,
    logical_to_actual: dict[str, str],
    workspace_id: str,
) -> list[dict]:
    """Build definition parts, applying ID substitution to text files."""
    base = folder
    parts = []
    for f in sorted(folder.rglob("*")):
        if not f.is_file():
            continue
        if f.name in EXCLUDE_FILES:
            continue
        # Use relative path so child-artifact files (inside a .children dir)
        # are not skipped when we're processing that child artifact's own folder.
        rel = f.relative_to(base).as_posix()
        if ".children" in f.relative_to(base).parts:
            continue

        # Apply substitution to text-based files
        suffix = f.suffix.lower()
        if suffix in (".json", ".tmdl", ".kql", ".py", ".pbir", ".pbism", ".sql", ".xml"):
            try:
                text = f.read_text(encoding="utf-8")
                text = substitute_ids(text, logical_to_actual, workspace_id)
                encoded = base64.b64encode(text.encode("utf-8")).decode("ascii")
            except Exception:
                encoded = base64.b64encode(f.read_bytes()).decode("ascii")
        else:
            encoded = base64.b64encode(f.read_bytes()).decode("ascii")

        parts.append({"path": rel, "payload": encoded, "payloadType": "InlineBase64"})
    return parts


# ── State file ─────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


# ── Core deployment logic ──────────────────────────────────────────────────────

class DirectDeployer:
    def __init__(
        self,
        workspace_id: str,
        credential: ChainedTokenCredential,
        artifacts_dir: Path = ARTIFACTS_DIR,
        dry_run: bool = False,
        force: bool = False,
        sql_server_connection: Optional[str] = None,
        lakehouse_guid: Optional[str] = None,
    ) -> None:
        self.workspace_id = workspace_id
        self.client = FabricClient(workspace_id, credential, dry_run)
        self.artifacts_dir = artifacts_dir
        self.dry_run = dry_run
        self.force = force
        self.sql_server_connection = sql_server_connection
        self.lakehouse_guid = lakehouse_guid

        # logicalId → actual deployed item ID
        self.logical_to_actual: dict[str, str] = {}
        # (displayName, type) → item dict from workspace
        self.existing_items: dict[tuple, dict] = {}

        self.state = load_state()
        self.deployed: list[str] = []
        self.skipped:  list[str] = []
        self.failed:   list[str] = []

        # Lazily populated on first call to _ensure_initialized() / deploy_phase()
        self._initialized: bool = False
        self._all_artifacts: list[dict] = []

    # ── Initialisation ─────────────────────────────────────────────────────────

    def _ensure_initialized(self) -> None:
        """Scan artifacts, pre-seed IDs from state, fetch existing items — once only."""
        if self._initialized:
            return
        self._all_artifacts = scan_artifacts(self.artifacts_dir)
        info(f"Found {len(self._all_artifacts)} artifact(s) in {self.artifacts_dir}")
        # Skip non-dict entries (e.g. top-level metadata like "workspace_id").
        for entry in self.state.values():
            if not isinstance(entry, dict):
                continue
            lid = entry.get("logicalId", "")
            aid = entry.get("itemId", "")
            if lid and _is_guid(aid):
                self.logical_to_actual[lid] = aid
        if not self.dry_run:
            info("Fetching existing workspace items…")
            try:
                self.existing_items = self.client.get_all_items()
                info(f"  Found {len(self.existing_items)} existing item(s)")
            except Exception as exc:
                err(f"Could not list workspace items: {exc}")
                raise
        self._initialized = True

    # ── Phase-level deploy ─────────────────────────────────────────────────────

    def deploy_phase(self, types: list[str]) -> bool:
        """
        Deploy only artifacts whose item_type is in `types`.
        Initialises on first call; persists state after every call.
        Returns True if every item succeeded.
        """
        self._ensure_initialized()
        artifacts = [a for a in self._all_artifacts if a["item_type"] in types]
        if not artifacts:
            info(f"  (no artifacts of type(s) {types} found — skipping)")
            return True
        ok_count = 0
        for artifact in artifacts:
            if self.deploy_artifact(artifact):
                ok_count += 1
        save_state(self.state)
        return ok_count == len(artifacts)

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _record(self, artifact: dict, item_id: str) -> None:
        """Record a successfully deployed artifact."""
        logical_id = artifact["logical_id"]
        if logical_id:
            self.logical_to_actual[logical_id] = item_id
        self.state[artifact["display_name"]] = {
            "itemId":    item_id,
            "type":      artifact["item_type"],
            "logicalId": logical_id,
        }
        self.deployed.append(f"{artifact['item_type']}/{artifact['display_name']}")

    def _get_creation_payload(self, artifact: dict) -> Optional[dict]:
        """For KQLDatabase, return the creationPayload referencing the parent Eventhouse."""
        if artifact["item_type"] != "KQLDatabase":
            return None
        parent_logical_id = artifact.get("child_of", "")
        if not parent_logical_id:
            warn(f"KQLDatabase {artifact['display_name']} has no parent logicalId")
            return None
        parent_actual_id = self.logical_to_actual.get(parent_logical_id, parent_logical_id)
        return {"databaseType": "ReadWrite", "parentEventhouseItemId": parent_actual_id}

    def _apply_expressions_override(self, parts: list[dict]) -> list[dict]:
        """
        If --sql-server-connection and --lakehouse-guid were supplied, patch
        the expressions.tmdl definition part in the semantic model with the
        correct database endpoint values.
        """
        if not (self.sql_server_connection or self.lakehouse_guid):
            return parts

        patched = []
        for part in parts:
            if part["path"] == "definition/expressions.tmdl":
                text = base64.b64decode(part["payload"]).decode("utf-8")
                if self.sql_server_connection:
                    # Replace the server value in the TMDL connection expression
                    text = re.sub(
                        r'(server\s*=\s*")[^"]*(")',
                        rf'\g<1>{self.sql_server_connection}\g<2>',
                        text, flags=re.IGNORECASE,
                    )
                if self.lakehouse_guid:
                    # Replace the lakehouse GUID in the source expression
                    text = re.sub(
                        r'(["\s])([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(["\s])',
                        lambda m: (
                            m.group(0).replace(m.group(2), self.lakehouse_guid)
                            if "lakehouse" in text[max(0, text.index(m.group(2))-60):text.index(m.group(2))].lower()
                            else m.group(0)
                        ),
                        text,
                    )
                part = {**part, "payload": base64.b64encode(text.encode()).decode("ascii")}
            patched.append(part)
        return patched

    # ── Lakehouse shortcuts ────────────────────────────────────────────────────

    def _create_lakehouse_shortcuts(self, folder: Path, lakehouse_id: str) -> None:
        """
        Read shortcuts.metadata.json, substitute logical IDs with real deployed IDs,
        and create each shortcut via the Fabric Shortcuts REST API.

        Fabric REST API silently ignores shortcuts.metadata.json in Lakehouse
        definition parts — shortcuts MUST be created through the dedicated endpoint.

        The shortcuts expose SQL Database and Cosmos DB tables in the Lakehouse SQL
        analytics endpoint, enabling views and the SemanticModel to work.
        """
        shortcuts_file = folder / "shortcuts.metadata.json"
        if not shortcuts_file.exists():
            return
        try:
            raw = shortcuts_file.read_text(encoding="utf-8")
            raw = substitute_ids(raw, self.logical_to_actual, self.workspace_id)
            shortcuts = json.loads(raw)
        except Exception as exc:
            warn(f"  Could not parse shortcuts.metadata.json: {exc}")
            return
        if not shortcuts:
            return

        info(f"  Creating {len(shortcuts)} Lakehouse shortcut(s) via API…")
        ok_count = 0
        for sc in shortcuts:
            name   = sc.get("name", "?")
            path   = sc.get("path", "/Tables").lstrip("/")  # strip leading slash
            target = sc.get("target", {})
            if self.client.create_shortcut(lakehouse_id, name, path, target):
                ok_count += 1
        ok(f"  {ok_count}/{len(shortcuts)} shortcut(s) created.")

    # ── Deploy one artifact ────────────────────────────────────────────────────

    def deploy_artifact(self, artifact: dict) -> bool:
        name       = artifact["display_name"]
        itype      = artifact["item_type"]
        folder     = artifact["folder"]
        logical_id = artifact["logical_id"]

        # ── SQLDatabase: needs a compiled .dacpac, not raw SQL/sqlproj files ──
        # The Fabric REST API returns SqlDbNative.ALM.MissingDacpacFile if you
        # upload raw source files.  We compile via 'dotnet build'; if dotnet is
        # absent we create an empty DB (the backend populates schema on startup).
        if itype == "SQLDatabase":
            parts = sql_db_parts(folder)
        elif itype == "KQLDatabase":
            # KQLDatabase is created via creationPayload (parentEventhouseItemId).
            # DatabaseProperties.json duplicates that payload — sending both causes
            # HTTP 400 InvalidInput.  Strip it; only include KQL schema files if any.
            parts = parts_with_substitution(folder, self.logical_to_actual, self.workspace_id)
            parts = [p for p in parts if p["path"] != "DatabaseProperties.json"]
        else:
            # All other item types: standard text files with ID substitution
            parts = parts_with_substitution(folder, self.logical_to_actual, self.workspace_id)

        # Extra override for SemanticModel expressions
        if itype == "SemanticModel":
            parts = self._apply_expressions_override(parts)

        existing = self.existing_items.get((name, itype))

        if existing and not self.force:
            item_id = existing["id"]
            # These item types are created via creationPayload and do not support
            # PATCH /updateDefinition. Re-running setup would otherwise mark them
            # as failed even though they are already deployed and functional.
            if itype in ("Lakehouse", "KQLDatabase", "CosmosDBDatabase"):
                info(f"Exists    {itype}: {name}  (id={item_id[:8]}…) — skipping update (not supported for this type)")
                self._record(artifact, item_id)
                self.skipped.append(f"{itype}/{name}")
                # Re-create shortcuts so they stay in sync after re-runs
                if itype == "Lakehouse" and _is_guid(item_id):
                    self._create_lakehouse_shortcuts(folder, item_id)
                return True
            info(f"Updating  {itype}: {name}  (id={item_id[:8]}…)")
            try:
                if parts:
                    self.client.update_definition(item_id, parts, name, itype)
                else:
                    info(f"  No definition parts for {name} — skipping update")
                self._record(artifact, item_id)
                return True
            except Exception as exc:
                err(f"Failed to update {itype}/{name}: {exc}")
                self.failed.append(f"{itype}/{name}")
                return False

        # Create new item
        info(f"Creating  {itype}: {name}")
        creation_payload = self._get_creation_payload(artifact)
        try:
            result = self.client.create_item(
                display_name=name,
                item_type=itype,
                parts=parts,
                creation_payload=creation_payload,
                description=artifact.get("description", ""),
            )
            item_id = result.get("id", f"unknown-{name}")
            self._record(artifact, item_id)

            # ── Lakehouse post-create: create shortcuts via dedicated API ──
            # Fabric REST API ignores shortcuts.metadata.json in the definition.
            # Shortcuts must be created explicitly so SQL DB and Cosmos DB
            # tables are visible in the Lakehouse SQL analytics endpoint.
            if itype == "Lakehouse" and _is_guid(item_id):
                self._create_lakehouse_shortcuts(folder, item_id)

            return True
        except Exception as exc:
            # Idempotency safety net: if create failed because the item already
            # exists, look it up and treat as success.
            try:
                for item in self.client.list_items(itype):
                    if item.get("displayName") == name:
                        item_id = item["id"]
                        warn(f"Create returned error for {itype}/{name} but item already exists "
                             f"(id={item_id[:8]}…) - treating as success.")
                        self._record(artifact, item_id)
                        self.skipped.append(f"{itype}/{name}")
                        if itype == "Lakehouse" and _is_guid(item_id):
                            self._create_lakehouse_shortcuts(folder, item_id)
                        return True
            except Exception as lookup_exc:
                warn(f"  Post-failure lookup also failed for {itype}/{name}: {lookup_exc}")
            err(f"Failed to create {itype}/{name}: {exc}")
            self.failed.append(f"{itype}/{name}")
            return False

    # ── Full deploy pipeline ───────────────────────────────────────────────────

    def run(self) -> int:
        head("Fabric Direct Deployment")
        info(f"Workspace : {self.workspace_id}")
        info(f"Artifacts : {self.artifacts_dir.resolve()}")
        info(f"Dry run   : {self.dry_run}")
        info(f"Force     : {self.force}")

        try:
            self._ensure_initialized()
        except Exception:
            return 1

        all_ok = True
        for phase_idx, phase_types in enumerate(DEPLOY_PHASES, 1):
            head(f"Phase {phase_idx}: {', '.join(phase_types)}")
            if not self.deploy_phase(phase_types):
                all_ok = False

        handled_types = {t for phase in DEPLOY_PHASES for t in phase}
        remainder = [a for a in self._all_artifacts if a["item_type"] not in handled_types]
        if remainder:
            head("Remaining items (types not in deploy phases)")
            for artifact in remainder:
                warn(f"  Item type '{artifact['item_type']}' not in phases — deploying anyway")
                if not self.deploy_artifact(artifact):
                    all_ok = False
            save_state(self.state)

        self._print_summary()
        self._write_gh_summary()
        return 0 if all_ok else 1

    def _print_summary(self) -> None:
        head("Deployment Summary")
        if self.deployed:
            ok(f"Deployed ({len(self.deployed)}):")
            for item in self.deployed:
                print(f"   {G}✓{X} {item}")
        if self.skipped:
            warn(f"Skipped  ({len(self.skipped)}):")
            for item in self.skipped:
                print(f"   {Y}–{X} {item}")
        if self.failed:
            err(f"Failed   ({len(self.failed)}):")
            for item in self.failed:
                print(f"   {R}✗{X} {item}")
        print()

        if self.logical_to_actual:
            head("Deployed Item IDs (save for reference)")
            print(f"{'Logical ID':<40}  {'Type':<20}  {'Actual Item ID'}")
            print("─" * 100)
            for logical_id, actual_id in self.logical_to_actual.items():
                itype = LOGICAL_ID_TO_TYPE.get(logical_id, "?")
                print(f"{logical_id:<40}  {itype:<20}  {actual_id}")

        # Semantic model reminder
        if not self.sql_server_connection:
            print()
            warn("SemanticModel reminder:")
            print(f"   The semantic model's expressions.tmdl still needs the correct SQL analytics")
            print(f"   endpoint. Re-run with:")
            print(f"   --sql-server-connection <server> --lakehouse-guid <guid>")
            print(f"   OR edit Fabric_artifacts/banking_semantic_model.SemanticModel/definition/expressions.tmdl")

    def _write_gh_summary(self) -> None:
        summary_file = os.getenv("GITHUB_STEP_SUMMARY")
        if not summary_file:
            return
        icon = "✅" if not self.failed else "⚠️"
        with open(summary_file, "a", encoding="utf-8") as f:
            f.write(f"\n## {icon} Fabric Direct Deployment\n\n")
            f.write(f"- **Workspace**: `{self.workspace_id}`\n")
            f.write(f"- **Deployed**: {len(self.deployed)}\n")
            f.write(f"- **Failed**: {len(self.failed)}\n\n")
            if self.failed:
                f.write("### Failed items\n")
                for item in self.failed:
                    f.write(f"- `{item}`\n")
            if self.logical_to_actual:
                f.write("\n### Deployed Item IDs\n\n")
                f.write("| Type | Logical ID | Actual Item ID |\n|---|---|---|\n")
                for lid, aid in self.logical_to_actual.items():
                    itype = LOGICAL_ID_TO_TYPE.get(lid, "?")
                    f.write(f"| {itype} | `{lid}` | `{aid}` |\n")


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Deploy Fabric artifacts via REST API — no Git integration needed.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--workspace-id", "-w",
                   default=os.getenv("FABRIC_WORKSPACE_ID"),
                   help="Target Fabric workspace GUID")
    p.add_argument("--artifacts-dir", type=Path, default=ARTIFACTS_DIR,
                   help=f"Path to artifacts directory (default: {ARTIFACTS_DIR})")
    p.add_argument("--dry-run", action="store_true",
                   help="Print what would be deployed without making API calls")
    p.add_argument("--force", action="store_true",
                   help="Re-deploy items that already exist (drop + recreate definition)")
    p.add_argument("--sql-server-connection",
                   default=os.getenv("FABRIC_SQL_ANALYTICS_SERVER"),
                   help="SQL analytics endpoint server string for expressions.tmdl patch "
                        "(e.g. x1y2z.datawarehouse.fabric.microsoft.com)")
    p.add_argument("--lakehouse-guid",
                   default=os.getenv("FABRIC_LAKEHOUSE_GUID"),
                   help="Lakehouse item GUID for expressions.tmdl patch")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if not args.workspace_id:
        err("No workspace ID. Use --workspace-id or set FABRIC_WORKSPACE_ID.")
        sys.exit(1)
    if not args.artifacts_dir.exists():
        err(f"Artifacts directory not found: {args.artifacts_dir}")
        sys.exit(1)

    credential = build_credential()
    deployer = DirectDeployer(
        workspace_id=args.workspace_id,
        credential=credential,
        artifacts_dir=args.artifacts_dir,
        dry_run=args.dry_run,
        force=args.force,
        sql_server_connection=args.sql_server_connection,
        lakehouse_guid=args.lakehouse_guid,
    )
    sys.exit(deployer.run())


if __name__ == "__main__":
    main()
