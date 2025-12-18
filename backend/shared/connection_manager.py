from shared.db_connect import fabricsql_connection_agentic_db
import threading
import os
import tempfile

class FabricConnectionManager:
    _instance = None
    _lock = threading.Lock()
    _credentials_cached_file = os.path.join(tempfile.gettempdir(), "fabric_auth_cached.flag")
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(FabricConnectionManager, cls).__new__(cls)
        return cls._instance
    
    def _is_authenticated(self):
        """Check if credentials are already cached."""
        return os.path.exists(self._credentials_cached_file)
    
    def authenticate_once(self):
        """Authenticate once and cache credentials."""
        if not self._is_authenticated():
            with self._lock:
                if not self._is_authenticated():
                    print("[ConnectionManager] Initializing database credentials...")
                    print("You may be prompted for credentials...")
                    
                    try:
                        # Make a test connection to cache credentials
                        test_conn = fabricsql_connection_agentic_db()
                        cursor = test_conn.cursor()
                        cursor.execute("SELECT 1")
                        cursor.fetchone()
                        cursor.close()
                        test_conn.close()
                        
                        # Mark as authenticated
                        with open(self._credentials_cached_file, 'w') as f:
                            f.write("authenticated")
                        
                        print("[ConnectionManager] Credentials cached successfully")
                        
                    except Exception as e:
                        print(f"[ConnectionManager] Authentication failed: {e}")
                        # Remove cache file if it exists
                        if os.path.exists(self._credentials_cached_file):
                            os.remove(self._credentials_cached_file)
                        raise
        else:
            print("[ConnectionManager] Using cached credentials")
    
    def create_connection(self):
        """Create a new database connection (for SQLAlchemy)."""
        # Ensure we're authenticated (but don't re-authenticate if already done)
        if not self._is_authenticated():
            self.authenticate_once()
        
        return fabricsql_connection_agentic_db()
    
    def get_connection(self):
        """Get a database connection (alias for create_connection for compatibility)."""
        return self.create_connection()
    
    def cleanup(self):
        """Clean up authentication cache."""
        if os.path.exists(self._credentials_cached_file):
            os.remove(self._credentials_cached_file)

# Global instance
connection_manager = FabricConnectionManager()

def sqlalchemy_connection_creator():
    """Creator function for SQLAlchemy."""
    return connection_manager.create_connection()