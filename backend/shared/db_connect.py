import os
import struct
import pyodbc
from dotenv import load_dotenv
import threading
from azure.identity import DefaultAzureCredential
import time

load_dotenv(override=True)

def fabricsql_connection_agentic_db():
    """
    Create connection for database.
    Supports both:
    - Azure Managed Identity (ActiveDirectoryMSI) for production
    - Standard SQL authentication for local development
    """
    conn_str = os.getenv("FABRIC_SQL_CONNECTION_URL_AGENTIC")
    if not conn_str:
        raise RuntimeError("FABRIC_SQL_CONNECTION_URL_AGENTIC is not set")
    if "ActiveDirectoryInteractive" in conn_str:
        try:
            # Try to connect with the connection string as-is
            # This works for both Managed Identity (Azure) and SQL Auth (local)
            return pyodbc.connect(conn_str, timeout=30)
        except pyodbc.Error as e:
            # If it fails and contains MSI-related error, provide helpful message
            error_msg = str(e)
            if "ActiveDirectoryMSI" in conn_str and ("token" in error_msg.lower() or "msi" in error_msg.lower()):
                raise RuntimeError(
                    "Failed to connect using Managed Identity. "
                    "For local development, update your .env file to use SQL Authentication. "
                    "Example: FABRIC_SQL_CONNECTION_URL_AGENTIC=Driver={ODBC Driver 18 for SQL Server};"
                    "Server=localhost;Database=BankingDB;UID=sa;PWD=YourPassword;"
                    "Encrypt=yes;TrustServerCertificate=yes;"
                ) from e
            else:
                raise
    if "Authentication=ActiveDirectoryCli" in conn_str:
        try:
            # Get access token (cached)
            token = _get_access_token()
            token_bytes = token.encode("utf-16-le")
            token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
            
            # Remove Authentication parameter from connection string
            parts = conn_str.split(";")
            conn_str_clean = ";".join([p for p in parts if not p.startswith("Authentication=")])
            
            # Connect with access token
            SQL_COPT_SS_ACCESS_TOKEN = 1256
            return pyodbc.connect(conn_str_clean, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct}, timeout=30)
        except pyodbc.Error as e:
            raise RuntimeError(f"Failed to connect using access token: {e}") from e
        

def fabricsql_connection_bank_db():
    """DEPRECATED: This function is now an alias for fabricsql_connection_agentic_db."""
    return fabricsql_connection_agentic_db()

def create_azuresql_connection():
    """Create connection for banking database (not used in this demo)."""
    raise NotImplementedError("create_azuresql_connection is not implemented.")

# Token cache
_token_cache = {"token": None, "expiry": 0}
_token_lock = threading.Lock()

def _get_access_token():
    """Get cached or fresh access token."""
    with _token_lock:
        current_time = time.time()
        # Refresh token if it's expired or will expire in next 5 minutes
        if _token_cache["token"] is None or current_time >= (_token_cache["expiry"] - 300):
            credential = DefaultAzureCredential()
            token_obj = credential.get_token("https://database.windows.net/.default")
            _token_cache["token"] = token_obj.token
            _token_cache["expiry"] = current_time + 3300  # 55 minutes
    return _token_cache["token"]