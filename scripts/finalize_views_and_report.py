#!/usr/bin/env python3
"""
finalize_views_and_report.py
=========================
Standalone retry script that re-runs ONLY the three steps that most
commonly fail on first deployment:

  1. Create / recreate SQL views on the Lakehouse SQL analytics endpoint
  2. Patch expressions.tmdl with the real server + GUID and redeploy
     the SemanticModel
  3. Deploy (or redeploy) the Power BI Report

Reads workspace ID and logical→actual ID mappings from
scripts/deploy-state.json (written by setup_workspace.py / direct_deploy_fabric.py).
Alternatively pass --workspace-id on the command line.

Usage
-----
    # Normal retry (reads workspace-id from state file):
    python scripts/finalize_views_and_report.py

    # Specify workspace explicitly:
    python scripts/finalize_views_and_report.py --workspace-id <guid>

    # Drop and recreate existing views (useful when view definitions changed):
    python scripts/finalize_views_and_report.py --drop-views

    # Dry run (shows what would happen without making API calls):
    python scripts/finalize_views_and_report.py --dry-run

Prerequisites
-------------
    pip install requests azure-identity pyodbc
    az login   (or set AZURE_CLIENT_ID / AZURE_TENANT_ID / AZURE_CLIENT_SECRET)
    ODBC Driver 18 for SQL Server installed
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import struct
import sys
import time
from pathlib import Path

import requests

# ── Optional deps ──────────────────────────────────────────────────────────────

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

# ── Shared helpers from direct_deploy_fabric ──────────────────────────────────

_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE))

try:
    from direct_deploy_fabric import (
        ARTIFACTS_DIR, EXCLUDE_FILES,
        DirectDeployer, build_credential,
        load_state, save_state,
        info, ok, warn, err,
        G, Y, R, C, B, X,
    )
except ImportError as e:
    print(f"ERROR: Could not import direct_deploy_fabric.py — {e}")
    sys.exit(1)

# ── Constants ──────────────────────────────────────────────────────────────────

FABRIC_API   = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
SQL_SCOPE    = "https://database.windows.net/.default"

VIEWS_SQL   = Path("Data_Ingest/create_views.sql")
EXPRESSIONS = Path("Fabric_artifacts/banking_semantic_model.SemanticModel/definition/expressions.tmdl")

# Logical IDs (fixed in source files — never change)
LAKEHOUSE_LOGICAL  = "fb30712b-d7a8-a06a-4004-373442ecab99"
SM_LOGICAL         = "c1a22701-9f6d-b640-4e9c-8d1ac4aeec57"
REPORT_LOGICAL     = "b4cc5129-4df8-8b4e-4df5-0e83f3a01efb"


# ── Tiny Fabric REST helper ────────────────────────────────────────────────────

class _API:
    def __init__(self, credential: ChainedTokenCredential) -> None:
        self._cred = credential
        self._token: str | None = None
        self._expires: float = 0.0

    def _hdrs(self) -> dict:
        if not self._token or time.time() >= self._expires - 60:
            t = self._cred.get_token(FABRIC_SCOPE)
            self._token, self._expires = t.token, float(t.expires_on)
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    def _poll(self, location: str, label: str, timeout: int = 600) -> dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            r = requests.get(location, headers=self._hdrs(), timeout=30)
            r.raise_for_status()
            body  = r.json()
            state = body.get("status", "").upper()
            if state == "SUCCEEDED":
                return body
            if state in ("FAILED", "CANCELLED"):
                raise RuntimeError(f"Operation {state}: {body.get('error', {})}")
            info(f"  {label}: {state} — waiting…")
            time.sleep(5)
        raise TimeoutError(f"Timed out waiting for: {label}")

    def get_lakehouse(self, ws_id: str, lh_id: str) -> dict:
        r = requests.get(
            f"{FABRIC_API}/workspaces/{ws_id}/lakehouses/{lh_id}",
            headers=self._hdrs(), timeout=30,
        )
        r.raise_for_status()
        return r.json()

    def get_items(self, ws_id: str) -> list[dict]:
        items: list[dict] = []
        url = f"{FABRIC_API}/workspaces/{ws_id}/items"
        while url:
            r = requests.get(url, headers=self._hdrs(), timeout=30)
            r.raise_for_status()
            body = r.json()
            items.extend(body.get("value", []))
            cont = body.get("continuationUri") or body.get("continuationToken")
            if cont:
                url = cont if cont.startswith("http") else f"{FABRIC_API}/workspaces/{ws_id}/items?continuationToken={cont}"
            else:
                url = ""
        return items

    def update_definition(self, ws_id: str, item_id: str,
                          parts: list[dict], label: str = "") -> None:
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
            try:
                detail = r.json()
            except Exception:
                detail = r.text[:300]
            raise RuntimeError(f"update_definition HTTP {r.status_code}: {detail}")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get_connect_kwargs(credential: ChainedTokenCredential) -> dict:
    """Pack an Azure AD token into the SQL_COPT_SS_ACCESS_TOKEN byte structure."""
    _tok = credential.get_token("https://database.windows.net/.default")
    token_bytes = _tok.token.encode("utf-16-le")
    connect_kwargs: dict = {
        "attrs_before": {
            1256: struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
        }
    }
    ok("Token acquired.")
    return connect_kwargs


def _poll_lakehouse_sql(api: _API, ws_id: str, lh_id: str,
                        timeout: int = 300) -> tuple[str, str]:
    """
    Wait until the Lakehouse SQL analytics endpoint is ready.
    Returns (connection_string, lakehouse_sql_guid).
    """
    info("Polling Lakehouse SQL analytics endpoint…")
    deadline = time.time() + timeout
    attempt  = 0
    while time.time() < deadline:
        attempt += 1
        try:
            item  = api.get_lakehouse(ws_id, lh_id)
            props = item.get("properties", {})
            ep    = props.get("sqlEndpointProperties", {})
            server = ep.get("connectionString", "")
            guid   = ep.get("id", "")
            status = ep.get("provisioningStatus", "").lower()
            if server and guid and status not in ("notprovisioned", ""):
                ok(f"SQL analytics endpoint ready (poll #{attempt}): {server[:60]}…")
                return server, guid
            info(f"  Poll #{attempt}: status={status or '?'} — waiting 10 s…")
        except Exception as exc:
            info(f"  Poll #{attempt}: {exc} — waiting 10 s…")
        time.sleep(10)
    raise TimeoutError("Lakehouse SQL analytics endpoint did not become ready within 5 minutes.")


def _patch_expressions(server: str, guid: str) -> bool:
    """
    Patch expressions.tmdl with the real server + GUID.
    Returns True if the file was modified, False if already patched.
    """
    if not EXPRESSIONS.exists():
        warn(f"expressions.tmdl not found at {EXPRESSIONS}")
        return False

    content  = EXPRESSIONS.read_text(encoding="utf-8")
    original = content

    content = re.sub(
        r'(Sql\.Database\s*\(\s*")([^"]*?)(")',
        lambda m: m.group(1) + server + m.group(3),
        content, count=1,
    )
    content = re.sub(
        r'(Sql\.Database\s*\(\s*"[^"]*"\s*,\s*")([^"]*?)(")',
        lambda m: m.group(1) + guid + m.group(3),
        content, count=1,
    )

    if content != original:
        EXPRESSIONS.write_text(content, encoding="utf-8")
        ok(f"Patched expressions.tmdl  server={server[:50]}…  guid={guid[:8]}…")
        return True

    # File might already be patched with real values (re-run scenario).
    # Verify it contains the real server value; if so, treat as OK.
    if server in content:
        ok("expressions.tmdl already contains the correct server — no patch needed.")
        return True

    warn("expressions.tmdl: pattern not found and server value not present. "
         "Check the file manually.")
    return False


def _build_parts(folder: Path, logical_to_actual: dict, ws_id: str) -> list[dict]:
    """Build definition parts for an artifact folder with ID substitution."""
    from direct_deploy_fabric import substitute_ids, EXCLUDE_FILES

    parts = []
    for f in sorted(folder.rglob("*")):
        if not f.is_file() or f.name in EXCLUDE_FILES:
            continue
        rel = f.relative_to(folder).as_posix()
        suffix = f.suffix.lower()
        if suffix in (".json", ".tmdl", ".kql", ".py", ".pbir", ".pbism", ".sql", ".xml"):
            try:
                text    = f.read_text(encoding="utf-8")
                text    = substitute_ids(text, logical_to_actual, ws_id)
                encoded = base64.b64encode(text.encode("utf-8")).decode("ascii")
            except Exception:
                encoded = base64.b64encode(f.read_bytes()).decode("ascii")
        else:
            encoded = base64.b64encode(f.read_bytes()).decode("ascii")
        parts.append({"path": rel, "payload": encoded, "payloadType": "InlineBase64"})
    return parts


# ── Step functions ─────────────────────────────────────────────────────────────

def step_sql_views(
    credential: ChainedTokenCredential,
    sql_server: str,
    drop_views: bool = False,
    dry_run: bool = False,
) -> bool:
    """Create / recreate SQL views on the Lakehouse SQL analytics endpoint."""

    print(f"\n{B}[A/3] Create SQL Views on Lakehouse SQL Analytics Endpoint{X}")

    if not HAS_PYODBC:
        err("pyodbc is not installed.  Run: pip install pyodbc")
        return False

    if not VIEWS_SQL.exists():
        err(f"Views SQL file not found: {VIEWS_SQL}")
        return False

    if dry_run:
        ok(f"[DRY RUN] Would execute {VIEWS_SQL} against {sql_server}")
        return True

    conn_str = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={sql_server},1433;"
        f"Database=agentic_lake;"
        f"Encrypt=yes;TrustServerCertificate=no;"
        f"Connection Timeout=30"
    )

    info("Acquiring Azure AD token for SQL Analytics Endpoint…")
    try:
        connect_kwargs = _get_connect_kwargs(credential)
        ok("Token acquired.")
    except Exception as exc:
        err(f"Could not acquire token: {exc}")
        return False

    conn = None
    for attempt in range(1, 6):
        info(f"Connecting to SQL Analytics Endpoint (attempt {attempt}/5)…")
        try:
            conn = pyodbc.connect(conn_str, **connect_kwargs)
            conn.autocommit = True
            ok("Connected.")
            break
        except Exception as exc:
            if attempt < 5:
                warn(f"  Attempt {attempt} failed: {exc}")
                info("  Waiting 30 s…")
                time.sleep(30)
            else:
                err(f"Failed to connect after 5 attempts: {exc}")
                return False

    sql     = VIEWS_SQL.read_text(encoding="utf-8")
    batches = [
        b.strip()
        for b in re.split(r"^\s*GO\s*$", sql, flags=re.IGNORECASE | re.MULTILINE)
        if b.strip()
    ]
    info(f"Executing {len(batches)} SQL batch(es) from {VIEWS_SQL.name}…")

    cursor  = conn.cursor()
    created = skipped = failed = 0

    for i, batch in enumerate(batches, 1):
        vm = re.search(
            r"CREATE\s+VIEW\s+(?:\[?dbo\]?\.\[?)?(\w+)\]?",
            batch, re.IGNORECASE,
        )
        view_name = vm.group(1) if vm else f"batch_{i}"

        # Check existence
        exists = False
        try:
            cursor.execute(
                "SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = ?",
                (view_name,),
            )
            exists = cursor.fetchone() is not None
        except Exception:
            pass

        if exists:
            if drop_views:
                info(f"  [{i}/{len(batches)}] Dropping existing view: {view_name}")
                try:
                    cursor.execute(f"DROP VIEW IF EXISTS [dbo].[{view_name}]")
                except Exception as exc:
                    warn(f"  Could not drop {view_name}: {exc}")
            else:
                info(f"  [{i}/{len(batches)}] View '{view_name}' already exists — skipping.")
                skipped += 1
                continue

        try:
            cursor.execute(batch)
            ok(f"  [{i}/{len(batches)}] Created: {view_name}")
            created += 1
        except Exception as exc:
            warn(f"  [{i}/{len(batches)}] Failed '{view_name}': {exc}")
            failed += 1

    cursor.close()
    conn.close()

    if failed == 0:
        ok(f"SQL views done: {created} created, {skipped} skipped.")
        return True
    else:
        warn(f"SQL views: {created} created, {skipped} skipped, {failed} FAILED.")
        return False


def step_semantic_model(
    deployer: DirectDeployer,
    ws_id: str,
    sql_server: str,
    lakehouse_guid: str,
    dry_run: bool = False,
) -> bool:
    """Patch expressions.tmdl and deploy (create/update) the SemanticModel."""

    print(f"\n{B}[B/3] Patch & Deploy Semantic Model{X}")

    patched = _patch_expressions(sql_server, lakehouse_guid)
    if not patched:
        warn("Proceeding with deployment even though patch may not have applied.")

    info("Deploying SemanticModel via deploy_phase…")
    success = deployer.deploy_phase(["SemanticModel"])
    if success:
        ok("SemanticModel deployed successfully.")
    else:
        warn("SemanticModel deployment reported errors — check output above.")
    return success


def step_report(deployer: DirectDeployer, dry_run: bool = False) -> bool:
    """Deploy (create/update) the Power BI Report."""

    print(f"\n{B}[C/3] Deploy Power BI Report{X}")

    success = deployer.deploy_phase(["Report"])
    if success:
        ok("Report deployed successfully.")
    else:
        warn("Report deployment reported errors — check output above.")
    return success


# ── Main ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Retry SQL views, SemanticModel, and Report deployment.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/retry_views_and_report.py
  python scripts/retry_views_and_report.py --workspace-id <guid>
  python scripts/retry_views_and_report.py --drop-views
  python scripts/retry_views_and_report.py --dry-run
""",
    )
    p.add_argument("--workspace-id", "-w",
                   help="Fabric workspace GUID (default: read from deploy-state.json)")
    p.add_argument("--drop-views", action="store_true",
                   help="DROP existing views before recreating them")
    p.add_argument("--dry-run", action="store_true",
                   help="Show what would happen without making any changes")
    p.add_argument("--skip-views", action="store_true",
                   help="Skip the SQL views step and go straight to SemanticModel + Report")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    # ── Resolve workspace ID ───────────────────────────────────────────────────
    state = load_state()

    ws_id = args.workspace_id or state.get("workspace_id", "")
    if not ws_id:
        err(
            "No workspace ID found.  Either:\n"
            "  • pass --workspace-id <guid>, or\n"
            "  • run setup_workspace.py first so deploy-state.json is created."
        )
        return 1

    info(f"Workspace ID: {ws_id}")

    # ── Build credential ───────────────────────────────────────────────────────
    credential = build_credential()

    # ── Build deployer (loads + pre-seeds ID state) ────────────────────────────
    deployer = DirectDeployer(
        workspace_id=ws_id,
        credential=credential,
        dry_run=args.dry_run,
    )

    # _ensure_initialized loads state and fetches existing items so
    # logical_to_actual is populated before we deploy anything.
    deployer._ensure_initialized()

    api = _API(credential)

    # ── Resolve Lakehouse ID ───────────────────────────────────────────────────
    lh_id = deployer.logical_to_actual.get(LAKEHOUSE_LOGICAL, "")
    if not lh_id:
        err(
            f"Lakehouse logical ID ({LAKEHOUSE_LOGICAL[:8]}…) not found in deploy state.\n"
            "Make sure the Lakehouse was deployed first (run setup_workspace.py or "
            "direct_deploy_fabric.py deploy)."
        )
        return 1

    info(f"Lakehouse item ID: {lh_id}")

    # ── Get SQL analytics endpoint ─────────────────────────────────────────────
    if args.dry_run:
        sql_server    = "dry-run.datawarehouse.fabric.microsoft.com"
        lakehouse_guid = "00000000-0000-0000-0000-000000000000"
    else:
        try:
            sql_server, lakehouse_guid = _poll_lakehouse_sql(api, ws_id, lh_id)
        except TimeoutError as exc:
            err(str(exc))
            return 1

    # ── Step A: SQL views ──────────────────────────────────────────────────────
    if not args.skip_views:
        views_ok = step_sql_views(
            credential=credential,
            sql_server=sql_server,
            drop_views=args.drop_views,
            dry_run=args.dry_run,
        )
    else:
        info("Skipping SQL views (--skip-views).")
        views_ok = True

    # ── Step B: SemanticModel ──────────────────────────────────────────────────
    sm_ok = step_semantic_model(
        deployer=deployer,
        ws_id=ws_id,
        sql_server=sql_server,
        lakehouse_guid=lakehouse_guid,
        dry_run=args.dry_run,
    )

    # Sync the newly deployed SemanticModel ID so the Report substitution works
    save_state(deployer.state)

    # ── Step C: Report ─────────────────────────────────────────────────────────
    report_ok = step_report(deployer=deployer, dry_run=args.dry_run)
    save_state(deployer.state)

    # ── Summary ────────────────────────────────────────────────────────────────
    print(f"\n{B}{'─' * 60}{X}")
    print(f"{B}Retry Summary{X}")
    print(f"{'─' * 60}")
    steps = [
        ("SQL Views",      views_ok if not args.skip_views else None),
        ("SemanticModel",  sm_ok),
        ("Report",         report_ok),
    ]
    all_ok = True
    for name, result in steps:
        if result is None:
            print(f"  {Y}–{X}  {name}: skipped")
        elif result:
            print(f"  {G}✓{X}  {name}: OK")
        else:
            print(f"  {R}✗{X}  {name}: FAILED — check output above")
            all_ok = False

    if all_ok:
        print(f"\n{G}All retry steps completed successfully.{X}")
        print("\nNext steps:")
        print("  • Open the Fabric workspace and verify the Report connects to the SemanticModel.")
        print("  • Check the Lakehouse SQL Analytics Endpoint for the new views.")
    else:
        print(f"\n{Y}Some steps failed — see messages above for details.{X}")
        if not views_ok and not args.skip_views:
            print("\n  Views tip: ensure the Lakehouse shortcuts are created and the SQL Database")
            print("  tables exist.  Then re-run:  python scripts/retry_views_and_report.py")
        print("\n  You can also run individual steps by combining flags:")
        print("    --skip-views          (skip views, only redeploy model + report)")
        print("    --drop-views          (drop and recreate existing views)")

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
