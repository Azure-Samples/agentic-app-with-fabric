#!/usr/bin/env python3
"""
setup_sql_views.py — SQL Views Setup for Fabric Lakehouse Analytics Endpoint
=============================================================================
Connects to a Fabric SQL analytics endpoint and executes the SQL views
required by the Power BI semantic model and reporting layer.

Works with both ActiveDirectoryCli (az login) and Service Principal auth.

Usage:
    # Using a connection string from CLI arg:
    python scripts/setup_sql_views.py \
        --sql-file Data_Ingest/create_views.sql \
        --connection-string "Driver={ODBC Driver 18 for SQL Server};Server=...;Authentication=ActiveDirectoryCli"

    # Using env vars:
    FABRIC_SQL_CONNECTION_URL_AGENTIC="..." python scripts/setup_sql_views.py

Required:
    pip install pyodbc azure-identity python-dotenv
    ODBC Driver 18 for SQL Server must be installed on the host.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import struct
import sys
import time
from pathlib import Path

try:
    import pyodbc
except ImportError:
    print("ERROR: pyodbc is not installed. Run: pip install pyodbc")
    sys.exit(1)

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
    print("ERROR: azure-identity is not installed. Run: pip install azure-identity")
    sys.exit(1)

# Load .env if present (useful for local runs)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env.deploy")
    load_dotenv(Path(__file__).parent.parent / "backend" / ".env")
except ImportError:
    pass

# ── Logging ───────────────────────────────────────────────────────────────────

_IS_TTY = sys.stdout.isatty()
GREEN  = "\033[32m" if _IS_TTY else ""
YELLOW = "\033[33m" if _IS_TTY else ""
RED    = "\033[31m" if _IS_TTY else ""
CYAN   = "\033[36m" if _IS_TTY else ""
RESET  = "\033[0m"  if _IS_TTY else ""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

info  = lambda msg: log.info(f"{CYAN}·{RESET} {msg}")
ok    = lambda msg: log.info(f"{GREEN}✓{RESET} {msg}")
warn  = lambda msg: log.warning(f"{YELLOW}⚠{RESET} {msg}")
err   = lambda msg: log.error(f"{RED}✗{RESET} {msg}")


# ── Authentication ─────────────────────────────────────────────────────────────

SQL_TOKEN_SCOPE = "https://database.windows.net/.default"


def _build_credential() -> ChainedTokenCredential:
    creds = []

    if os.getenv("AZURE_FEDERATED_TOKEN_FILE") or os.getenv("AZURE_CLIENT_ID"):
        try:
            creds.append(WorkloadIdentityCredential())
        except Exception:
            pass

    client_id     = os.getenv("AZURE_CLIENT_ID")
    tenant_id     = os.getenv("AZURE_TENANT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    if client_id and tenant_id and client_secret:
        creds.append(ClientSecretCredential(tenant_id, client_id, client_secret))

    if os.getenv("IDENTITY_ENDPOINT") or os.getenv("IMDS_ENDPOINT"):
        creds.append(ManagedIdentityCredential())

    creds.append(AzureCliCredential())
    return ChainedTokenCredential(*creds)


def _get_token_bytes(credential: ChainedTokenCredential) -> bytes:
    """
    Encode an Azure AD access token as the byte array pyodbc expects for
    SQL_COPT_SS_ACCESS_TOKEN.
    """
    token = credential.get_token(SQL_TOKEN_SCOPE)
    # pyodbc requires the token encoded as a UTF-16LE struct
    token_bytes = bytes(token.token, "UTF-8")
    expanded_bytes = bytes(struct.pack("=i", len(token_bytes)))
    for byte in token_bytes:
        expanded_bytes += bytes([byte, 0])
    return expanded_bytes


# ── SQL utilities ─────────────────────────────────────────────────────────────


def _split_sql_batches(sql: str) -> list[str]:
    """
    Split a SQL script on GO separators (case-insensitive, own line).
    Strips empty batches.
    """
    parts = re.split(r"^\s*GO\s*$", sql, flags=re.IGNORECASE | re.MULTILINE)
    return [p.strip() for p in parts if p.strip()]


def _view_already_exists(cursor: pyodbc.Cursor, view_name: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = ?",
        (view_name,),
    )
    return cursor.fetchone() is not None


def _extract_view_name(batch: str) -> str | None:
    """Extract the view name from a CREATE VIEW statement."""
    m = re.search(
        r"CREATE\s+VIEW\s+(?:\[?dbo\]?\.\[?)?(\w+)\]?",
        batch,
        re.IGNORECASE,
    )
    return m.group(1) if m else None


# ── Core function ─────────────────────────────────────────────────────────────


def run_views_setup(
    connection_string: str,
    sql_file: Path,
    drop_existing: bool = False,
    use_token_auth: bool = True,
) -> int:
    """
    Connect to Fabric SQL and execute the views SQL file.

    Returns 0 on success, 1 on failure.
    """
    if not sql_file.exists():
        err(f"SQL file not found: {sql_file}")
        return 1

    sql_content = sql_file.read_text(encoding="utf-8")
    batches = _split_sql_batches(sql_content)
    info(f"Read {len(batches)} SQL batch(es) from {sql_file.name}")

    # ── Build connection ──────────────────────────────────────────────────────

    # Ensure we use ActiveDirectoryCli auth in the connection string for
    # local/CLI runs, or strip it if we'll supply a token manually.
    conn_str = connection_string.strip().strip('"').strip("'")

    # Normalise auth mode
    if use_token_auth and "Authentication=ActiveDirectoryCli" not in conn_str:
        # Remove any existing Authentication= parameter
        conn_str = re.sub(r";?Authentication=[^;]+", "", conn_str)

    connect_kwargs: dict = {}

    if use_token_auth and "Authentication=ActiveDirectoryCli" not in conn_str:
        info("Using Azure AD token for SQL authentication…")
        try:
            credential = _build_credential()
            token_bytes = _get_token_bytes(credential)
            connect_kwargs["attrs_before"] = {1256: token_bytes}  # SQL_COPT_SS_ACCESS_TOKEN
        except ClientAuthenticationError as exc:
            err(f"Failed to acquire Azure AD token: {exc}")
            return 1
    else:
        info("Using ActiveDirectoryCli authentication (az login)…")

    info(f"Connecting to Fabric SQL endpoint…")
    try:
        conn = pyodbc.connect(conn_str, **connect_kwargs)
        conn.autocommit = True
    except pyodbc.Error as exc:
        err(f"Failed to connect: {exc}")
        err("Tip: ensure ODBC Driver 18 is installed and `az login` has been run.")
        return 1

    ok("Connected successfully.")

    # ── Execute batches ───────────────────────────────────────────────────────

    cursor = conn.cursor()
    created = 0
    skipped = 0
    failed  = 0

    for i, batch in enumerate(batches, start=1):
        view_name = _extract_view_name(batch)

        if view_name and _view_already_exists(cursor, view_name):
            if drop_existing:
                info(f"  [{i}/{len(batches)}] Dropping existing view: {view_name}")
                try:
                    cursor.execute(f"DROP VIEW IF EXISTS [dbo].[{view_name}]")
                except pyodbc.Error as exc:
                    warn(f"  Could not drop view {view_name}: {exc}")
            else:
                warn(f"  [{i}/{len(batches)}] View '{view_name}' already exists — skipping.")
                skipped += 1
                continue

        label = view_name or f"batch {i}"
        info(f"  [{i}/{len(batches)}] Creating: {label}")

        try:
            cursor.execute(batch)
            created += 1
        except pyodbc.Error as exc:
            err(f"  Failed to execute batch {i} ({label}): {exc}")
            failed += 1
            # Continue with remaining batches rather than aborting
            continue

    cursor.close()
    conn.close()

    # ── Summary ───────────────────────────────────────────────────────────────

    print()
    if failed == 0:
        ok(f"SQL views setup complete: {created} created, {skipped} skipped, {failed} failed.")
    else:
        warn(f"SQL views setup completed with errors: {created} created, {skipped} skipped, {failed} FAILED.")

    # Write to GitHub Actions step summary if available
    summary_file = os.getenv("GITHUB_STEP_SUMMARY")
    if summary_file:
        status_icon = "✅" if failed == 0 else "⚠️"
        with open(summary_file, "a", encoding="utf-8") as f:
            f.write(f"\n## {status_icon} SQL Views Setup\n\n")
            f.write(f"| Metric | Count |\n|---|---|\n")
            f.write(f"| Created | {created} |\n")
            f.write(f"| Skipped (already existed) | {skipped} |\n")
            f.write(f"| Failed | {failed} |\n")

    return 0 if failed == 0 else 1


# ── CLI ───────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create SQL views in a Fabric SQL analytics endpoint.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/setup_sql_views.py
  python scripts/setup_sql_views.py --sql-file Data_Ingest/create_views.sql
  python scripts/setup_sql_views.py --drop-existing
""",
    )
    parser.add_argument(
        "--connection-string", "-c",
        default=os.getenv("FABRIC_SQL_CONNECTION_URL_AGENTIC"),
        help="ODBC connection string (default: FABRIC_SQL_CONNECTION_URL_AGENTIC env var)",
    )
    parser.add_argument(
        "--sql-file", "-f",
        type=Path,
        default=Path("Data_Ingest/create_views.sql"),
        help="Path to the SQL views file (default: Data_Ingest/create_views.sql)",
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="DROP existing views before re-creating them",
    )
    parser.add_argument(
        "--no-token-auth",
        action="store_true",
        help="Disable token injection; rely on ActiveDirectoryCli in the connection string",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.connection_string:
        err(
            "No connection string provided. Use --connection-string or set "
            "FABRIC_SQL_CONNECTION_URL_AGENTIC."
        )
        sys.exit(1)

    rc = run_views_setup(
        connection_string=args.connection_string,
        sql_file=args.sql_file,
        drop_existing=args.drop_existing,
        use_token_auth=not args.no_token_auth,
    )
    sys.exit(rc)


if __name__ == "__main__":
    main()
