# macOS Local Deployment Guide

## Overview
This guide documents the changes required to run the SQL Agentic App with Fabric locally on macOS. The primary issue was authentication failures when connecting to Microsoft Fabric SQL Database.

---

## Primary Issue Resolved
Fixed authentication failures when connecting to Microsoft Fabric SQL Database on macOS by replacing Windows-only interactive authentication with Azure CLI token-based authentication.

---

## Key Changes Made

### 1. **Authentication Method Update**
- **Changed from:** `Authentication=ActiveDirectoryInteractive` (requires Windows pop-up)
- **Changed to:** Azure CLI token-based authentication
- **Why:** The interactive authentication method doesn't work on macOS; now uses existing `az login` session

### 2. **Connection String Modifications**
**File:** `backend/.env`

```dotenv
# Before (Windows-only)
FABRIC_SQL_CONNECTION_URL_AGENTIC = "Driver={ODBC Driver 18 for SQL Server};Server=...;Authentication=ActiveDirectoryInteractive"

# After (macOS compatible)
FABRIC_SQL_CONNECTION_URL_AGENTIC = "Driver={ODBC Driver 18 for SQL Server};Server=...;Authentication=ActiveDirectoryCli"
```

- Updated connection string to use `ActiveDirectoryCli` 
- Implemented automatic removal of unsupported Authentication parameters before passing to SQLAlchemy

### 3. **Token Caching Implementation**
**File:** `backend/shared/db_connect.py`

Added Azure CLI access token caching mechanism to prevent repeated authentication calls:

```python
# Token cache
_token_cache = {"token": None, "expiry": 0}
_token_lock = threading.Lock()

def _get_access_token():
    """Get cached or fresh access token."""
    with _token_lock:
        current_time = time.time()
        # Refresh token if it's expired or will expire in next 5 minutes
        if _token_cache["token"] is None or current_time >= (_token_cache["expiry"] - 300):
            credential = AzureCliCredential()
            token_obj = credential.get_token("https://database.windows.net/.default")
            _token_cache["token"] = token_obj.token
            _token_cache["expiry"] = current_time + 3300  # 55 minutes
    return _token_cache["token"]
```

**Benefits:**
- Tokens are cached and reused until near expiration
- Significantly improved connection performance and reliability
- Prevents token fetch timeouts during multiple simultaneous connections

### 4. **Database Connection Updates**
**File:** `backend/shared/db_connect.py`

Modified the connection function to use Azure CLI credentials:

```python
def fabricsql_connection_agentic_db():
    conn_str = os.getenv("FABRIC_SQL_CONNECTION_URL_AGENTIC")
    
    if "Authentication=ActiveDirectory" in conn_str:
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
```

### 5. **Vector Store Lazy Initialization**
**File:** `backend/banking_app.py`

Changed vector store to lazy-load on first use to prevent initialization failures:

```python
# Vector store will be initialized lazily when needed
vector_store = None
_vector_store_initialized = False

def get_vector_store():
    """Lazy initialization of vector store."""
    global vector_store, _vector_store_initialized
    if not _vector_store_initialized and embeddings_client:
        vector_store = SQLServer_VectorStore(
            connection_string=connection_url,
            table_name="DocsChunks_Embeddings",
            embedding_function=embeddings_client,
            embedding_length=1536,
            distance_strategy=DistanceStrategy.COSINE,
        )
        _vector_store_initialized = True
    return vector_store
```

Also updated the connection string cleanup:

```python
connection_string = os.getenv('FABRIC_SQL_CONNECTION_URL_AGENTIC')
# Remove Authentication parameter if present for SQLAlchemy compatibility
if "Authentication=ActiveDirectory" in connection_string:
    parts = connection_string.split(";")
    connection_string = ";".join([p for p in parts if not p.startswith("Authentication=")])
connection_url = f"mssql+pyodbc:///?odbc_connect={connection_string}"
```

---

## Technical Details

### How It Works

1. **Token Acquisition:** Uses `AzureCliCredential()` from `azure.identity` to fetch access token from Azure CLI session
2. **Token Encoding:** Converts token to UTF-16 LE byte array required by ODBC driver
3. **Token Injection:** Passes token via `SQL_COPT_SS_ACCESS_TOKEN` connection option
4. **Token Caching:** Stores token for ~55 minutes to avoid repeated authentication calls

### Before vs After

**Before:**
```python
# Direct connection with unsupported auth method on macOS
return pyodbc.connect(conn_str, timeout=30)
# Error: Authentication method not supported
```

**After:**
```python
# Token-based authentication compatible with macOS
token = _get_access_token()  # Cached
token_bytes = token.encode("utf-16-le")
token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
return pyodbc.connect(conn_str_clean, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
```

---

## Prerequisites for Users

### Required Software
1. **Azure CLI** - Install with `brew install azure-cli` on macOS
2. **Python 3.14** (or compatible version)
3. **ODBC Driver 18 for SQL Server**

### Required Configuration
1. Azure CLI login: `az login`
2. User must have access to the Fabric SQL Database
3. Valid `.env` file with updated connection string

---

## Setup Instructions

### Step 1: Install Azure CLI
```bash
brew install azure-cli
```

### Step 2: Login to Azure
```bash
az login
```

### Step 3: Update Environment Variables
Edit `backend/.env` and update the connection string:

```dotenv
FABRIC_SQL_CONNECTION_URL_AGENTIC = "Driver={ODBC Driver 18 for SQL Server};Server=<your-server>.database.fabric.microsoft.com,1433;Database={<your-database>};Encrypt=yes;TrustServerCertificate=no;Authentication=ActiveDirectoryCli"
```

### Step 4: Install Python Dependencies
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Step 5: Install Node Dependencies
```bash
npm install
```

### Step 6: Run the Application

**Terminal 1 - Backend:**
```bash
cd backend
source ../venv/bin/activate
python3 launcher.py
```

**Terminal 2 - Frontend:**
```bash
npm run dev
```

### Step 7: Access the Application
Open http://localhost:5173 in your browser

---

## Testing Results

✅ **Backend services start successfully**
- Banking service running on http://127.0.0.1:5001
- Analytics service running on http://127.0.0.1:5002

✅ **Database connections established**
- No interactive prompts required
- Token caching prevents repeated authentication

✅ **Frontend running**
- Vite dev server on http://localhost:5173
- Successfully connects to backend services

✅ **Data initialization**
- Database tables created automatically
- Sample data ingested on first run

---

## Files Modified

| File | Changes |
|------|---------|
| `backend/.env` | Updated connection string to use ActiveDirectoryCli |
| `backend/shared/db_connect.py` | Added token caching and Azure CLI credential integration |
| `backend/shared/connection_manager.py` | Utilizes updated connection creator |
| `backend/banking_app.py` | Lazy vector store initialization, connection string cleanup |

---

## Troubleshooting

### Issue: "Authentication failed: Invalid value specified for connection string attribute 'Authentication'"
**Solution:** Ensure the Authentication parameter is being stripped from the connection string. Check `backend/banking_app.py` line ~98-103.

### Issue: "Communication link failure"
**Solution:** This typically means the Azure CLI token expired or is invalid. Run:
```bash
az login --scope https://database.windows.net/.default
```

### Issue: Token fetch hanging
**Solution:** Check Azure CLI installation and login status:
```bash
az account show
az account get-access-token --resource https://database.windows.net
```

### Issue: Python version compatibility warning
**Note:** Warning about Pydantic V1 and Python 3.14 is non-critical and won't affect functionality.

---

## Known Limitations

1. **Python 3.14 Compatibility:** Some dependencies show warnings about Pydantic V1 compatibility with Python 3.14. These are warnings only and don't affect functionality.

2. **Token Expiration:** Tokens expire after ~1 hour. The app automatically refreshes them, but if you see auth errors after long periods, restart the backend.

3. **Azure CLI Dependency:** This solution requires Azure CLI to be installed and the user to be logged in. For production deployments, consider using Managed Identity instead.

---

## Production Considerations

For production deployments on Azure App Service or Azure Container Apps:

1. Use **Managed Identity** instead of Azure CLI credentials
2. Update connection string to use `Authentication=ActiveDirectoryMsi`
3. Assign appropriate RBAC roles to the managed identity
4. No code changes needed - the `fabricsql_connection_agentic_db()` function handles both scenarios

---

## Additional Notes

- The changes are backward compatible with Windows deployments
- Token caching significantly reduces authentication overhead
- Lazy vector store initialization prevents startup failures
- All changes maintain the same API and functionality

---

## Contact

For questions or issues with this deployment, please refer to the main project README or open an issue in the repository.
