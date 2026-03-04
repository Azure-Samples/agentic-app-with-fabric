"""
Fabric Data Agent Tool
Calls the Microsoft Fabric Data Agent endpoint using a Bearer token,
mirroring the pattern from the standalone app.py reference implementation.
"""

import json
import os
import requests
from langchain_core.tools import tool
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
load_dotenv(override=True)

FABRIC_SCOPE = "https://analysis.windows.net/powerbi/api/.default"

_credential = None


def _get_fabric_token() -> str:
    """Acquire a Bearer token for the Fabric Data Agent API.

    Uses DefaultAzureCredential which supports:
    - Managed Identity in Azure (production)
    - Service principal via AZURE_CLIENT_ID / AZURE_TENANT_ID / AZURE_CLIENT_SECRET (local dev)
    - Interactive browser auth as a local fallback
    """
    global _credential
    if _credential is None:
        _credential = DefaultAzureCredential()
    token = _credential.get_token(FABRIC_SCOPE)
    print("Acquired Fabric token")
    return token.token


def _call_fabric_data_agent(question: str, server_url: str, tool_name: str) -> str:
    """POST a natural-language question to the Fabric Data Agent MCP endpoint.

    The endpoint returns Server-Sent Events (SSE). We iterate over the lines and
    extract the first `data:` frame that contains a non-empty result, exactly as
    the reference app.py does.
    """
    headers = {
        "Authorization": f"Bearer {_get_fabric_token()}",
        "Content-Type": "application/json",
    }

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": {"userQuestion": question},
        },
    }

    response = requests.post(server_url, headers=headers, json=payload, timeout=120)
    response.raise_for_status()

    for line in response.text.split("\n"):
        if line.startswith("data: "):
            try:
                parsed = json.loads(line[6:])
                content = parsed.get("result", {}).get("content", [])
                if content:
                    return content[0].get("text", str(content))
                result = parsed.get("result")
                if result is not None:
                    return str(result)
            except json.JSONDecodeError:
                continue

    return response.text


def get_fabric_data_agent_tools(user_id: str):
    """Return the list of Fabric Data Agent tools for a given user.

    The server URL and tool name are read from environment variables so that
    they can be set per-deployment without code changes:

        FABRIC_DATA_AGENT_SERVER_URL  e.g. https://<workspace>.fabric.microsoft.com/...
        FABRIC_DATA_AGENT_TOOL_NAME   the MCP tool name registered in Fabric
    """
    server_url = os.getenv("FABRIC_DATA_AGENT_SERVER_URL", "")
    tool_name = os.getenv("FABRIC_DATA_AGENT_TOOL_NAME", "")
    print("Configuring Fabric Data Agent Tool")

    @tool
    def query_fabric_data_agent(question: str) -> str:
        """Query the Microsoft Fabric Data Agent for read-only information about
        user accounts, transactions, balances, and other banking data.

        Use this tool to answer natural-language questions about a user's financial
        data (e.g. "What are my recent transactions?", "What is my current balance?",
        "How much did I spend on groceries last month?").

        The Fabric Data Agent has full read-only access to the banking warehouse and
        will return formatted results.

        Args:
            question: Natural-language question about the user's banking data.
        """
        if not server_url or not tool_name:
            return json.dumps(
                {
                    "status": "error",
                    "message": (
                        "Fabric Data Agent is not configured. "
                        "Set FABRIC_DATA_AGENT_SERVER_URL and FABRIC_DATA_AGENT_TOOL_NAME "
                        "in your environment."
                    ),
                }
            )

        try:
            # Embed the user_id in the question so the Fabric agent can scope results
            scoped_question = f"[user_id: {user_id}] {question}"
            return _call_fabric_data_agent(scoped_question, server_url, tool_name)
        except Exception as exc:
            return json.dumps(
                {
                    "status": "error",
                    "message": f"Fabric Data Agent query failed: {exc}",
                }
            )

    return [query_fabric_data_agent]
