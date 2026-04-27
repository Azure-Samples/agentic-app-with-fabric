#!/usr/bin/env python3
"""
Provision an Azure Cosmos DB (NoSQL, serverless) account for the agentic-banking workshop
when native Cosmos DB in Fabric isn't available in your region.

Creates:
  - Resource group (if missing)
  - Cosmos DB account (serverless, NoSQL API, AAD-only auth)
  - Database 'agentic_cosmos_db'
  - Containers: longterm_memory, gen_ui_config
  - Grants signed-in user the 'Cosmos DB Built-in Data Contributor' role
  - Updates backend/.env with COSMOS_DB_ENDPOINT and COSMOS_DB_DATABASE

Run from the dev container after `az login`.
"""
import argparse
import json
import os
import re
import shutil
import string
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
ENV_FILE = REPO / "backend" / ".env"

DEFAULT_DB = "agentic_cosmos_db"
CONTAINERS = [
    ("longterm_memory", "/userId"),
    ("gen_ui_config", "/userId"),
]


def run(cmd, check=True, capture=True):
    print(f"  $ {' '.join(cmd) if isinstance(cmd, list) else cmd}")
    r = subprocess.run(
        cmd if isinstance(cmd, list) else cmd.split(),
        capture_output=capture,
        text=True,
    )
    if check and r.returncode != 0:
        print(f"  STDERR: {r.stderr.strip()}")
        sys.exit(f"Command failed: {cmd}")
    return r


def jrun(cmd):
    r = run(cmd)
    return json.loads(r.stdout) if r.stdout.strip() else None


def read_env_var(key: str) -> str:
    """Best-effort read of a key from backend/.env (no external deps)."""
    if not ENV_FILE.exists():
        return ""
    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()
        if line.startswith(key) and "=" in line:
            v = line.split("=", 1)[1].strip().strip('"').strip("'")
            return v
    return ""


def main():
    ap = argparse.ArgumentParser(
        description="Provision Azure Cosmos DB serverless for the agentic-banking workshop"
    )
    ap.add_argument("--name", help="Cosmos account name (auto-generated if omitted)")
    args = ap.parse_args()

    if not shutil.which("az"):
        sys.exit("az CLI not found in PATH")

    print("=== 1. Azure context ===")
    acct = jrun(["az", "account", "show", "-o", "json"])
    sub_id = acct["id"]
    user_oid = jrun(["az", "ad", "signed-in-user", "show", "--query", "id", "-o", "json"])
    print(f"  Subscription: {acct['name']} ({sub_id})")
    print(f"  User OID:     {user_oid}")

    region = read_env_var("AZURE_REGION")
    rg = read_env_var("AZURE_RESOURCE_GROUP")
    if not region:
        sys.exit(f"AZURE_REGION not set in {ENV_FILE}")
    if not rg:
        sys.exit(f"AZURE_RESOURCE_GROUP not set in {ENV_FILE}")

    # account name needs to be globally unique, 3-44 chars, lowercase + digits + hyphens
    suffix = re.sub(r"[^a-z0-9]", "", user_oid.lower())[:6]
    cosmos_name = args.name or os.environ.get("COSMOS_NAME") or f"agentic-cosmos-mr-{suffix}"
    cosmos_name = cosmos_name[:44]
    print(f"  Resource group: {rg}")
    print(f"  Region:         {region}")
    print(f"  Cosmos account: {cosmos_name}")

    print("\n=== 2. Resource group ===")
    rg_exists = run(["az", "group", "exists", "-n", rg]).stdout.strip() == "true"
    if not rg_exists:
        run(["az", "group", "create", "-n", rg, "-l", region, "-o", "none"])
    else:
        print(f"  Already exists")

    print("\n=== 3. Cosmos DB account (serverless, NoSQL, AAD-only) ===")
    existing = run(
        ["az", "cosmosdb", "show", "-n", cosmos_name, "-g", rg, "-o", "json"],
        check=False,
    )
    if existing.returncode == 0:
        acct_obj = json.loads(existing.stdout)
        print(f"  Already exists: {acct_obj['documentEndpoint']}")
    else:
        print("  Creating (this takes ~3-5 min)...")
        run([
            "az", "cosmosdb", "create",
            "-n", cosmos_name,
            "-g", rg,
            "--locations", f"regionName={region}",
            "--capabilities", "EnableServerless",
            "--default-consistency-level", "Session",
            "--disable-local-auth", "true",
            "-o", "none",
        ])
        acct_obj = jrun(["az", "cosmosdb", "show", "-n", cosmos_name, "-g", rg, "-o", "json"])
    endpoint = acct_obj["documentEndpoint"]

    print("\n=== 4. Database + containers ===")
    dbs = jrun([
        "az", "cosmosdb", "sql", "database", "list",
        "-a", cosmos_name, "-g", rg, "-o", "json",
    ]) or []
    db_names = {d["name"] for d in dbs}
    if DEFAULT_DB not in db_names:
        run([
            "az", "cosmosdb", "sql", "database", "create",
            "-a", cosmos_name, "-g", rg, "-n", DEFAULT_DB, "-o", "none",
        ])
        print(f"  + database {DEFAULT_DB}")
    else:
        print(f"  = database {DEFAULT_DB} (exists)")

    existing_containers = jrun([
        "az", "cosmosdb", "sql", "container", "list",
        "-a", cosmos_name, "-g", rg, "-d", DEFAULT_DB, "-o", "json",
    ]) or []
    have = {c["name"] for c in existing_containers}
    for name, pk in CONTAINERS:
        if name in have:
            print(f"  = container {name} (exists)")
            continue
        run([
            "az", "cosmosdb", "sql", "container", "create",
            "-a", cosmos_name, "-g", rg, "-d", DEFAULT_DB,
            "-n", name, "-p", pk, "-o", "none",
        ])
        print(f"  + container {name} (pk={pk})")

    print("\n=== 5. RBAC: grant signed-in user Cosmos Data Contributor ===")
    role_def_id = "00000000-0000-0000-0000-000000000002"  # Cosmos DB Built-in Data Contributor
    scope = f"/subscriptions/{sub_id}/resourceGroups/{rg}/providers/Microsoft.DocumentDB/databaseAccounts/{cosmos_name}"
    assignments = jrun([
        "az", "cosmosdb", "sql", "role", "assignment", "list",
        "-a", cosmos_name, "-g", rg, "-o", "json",
    ]) or []
    have_assignment = any(
        a.get("principalId", "").lower() == user_oid.lower()
        and a.get("roleDefinitionId", "").endswith(role_def_id)
        for a in assignments
    )
    if have_assignment:
        print("  = already assigned")
    else:
        run([
            "az", "cosmosdb", "sql", "role", "assignment", "create",
            "-a", cosmos_name, "-g", rg,
            "--scope", scope,
            "--principal-id", user_oid,
            "--role-definition-id", role_def_id,
            "-o", "none",
        ])
        print(f"  + granted to {user_oid}")

    print("\n=== 6. Update backend/.env ===")
    if not ENV_FILE.exists():
        sys.exit(f"{ENV_FILE} not found")
    lines = ENV_FILE.read_text().splitlines()
    updates = {
        "COSMOS_DB_ENDPOINT": endpoint,
        "COSMOS_DB_DATABASE": DEFAULT_DB,
    }
    seen = set()
    new_lines = []
    for line in lines:
        m = re.match(r"^([A-Z_][A-Z0-9_]*)=", line)
        if m and m.group(1) in updates:
            new_lines.append(f"{m.group(1)}={updates[m.group(1)]}")
            seen.add(m.group(1))
        else:
            new_lines.append(line)
    for k, v in updates.items():
        if k not in seen:
            new_lines.append(f"{k}={v}")
    ENV_FILE.write_text("\n".join(new_lines) + "\n")
    print(f"  COSMOS_DB_ENDPOINT={endpoint}")
    print(f"  COSMOS_DB_DATABASE={DEFAULT_DB}")

    print("\n✅ Done.\n")
    print("Next steps:")
    print("  1. Restart the backend so it picks up the new env vars:")
    print("       cd backend && python3 launcher.py")
    print("  2. Send a chat in the app — long-term memory + gen-UI now use Cosmos.")
    print(f"\nAccount endpoint: {endpoint}")


if __name__ == "__main__":
    main()
