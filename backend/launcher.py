import threading
import time
from shared.connection_manager import connection_manager

def run_banking_service():
    """Run banking service in a thread."""
    try:
        # Import here to avoid circular imports
        import banking_app
        banking_app.app.run(debug=False, port=5001, use_reloader=False, threaded=True)
    except Exception as e:
        print(f"Banking service error: {e}")

def run_analytics_service():
    """Run analytics service in a thread."""
    try:
        # Import here to avoid circular imports
        import agent_analytics
        agent_analytics.app.run(debug=False, port=5002, use_reloader=False, threaded=True)
    except Exception as e:
        print(f"Analytics service error: {e}")

def run_combined_services():
    """Run both services in the same process with different threads."""
    try:
        print("[0] Initializing database connection...")
        
        # Authenticate once for the entire process
        try:
            connection_manager.authenticate_once()
            print("✅ Database authentication successful")
        except Exception as e:
            print(f"❌ Failed to authenticate to database: {e}")
            return
        
        # Initialize both apps
        print("[1] Initializing services...")
        
        # Import and initialize both apps
        import banking_app
        import agent_analytics
        
        # Initialize databases for both apps
        from chat_data_model import ensure_chat_history_columns
        with banking_app.app.app_context():
            banking_app.db.create_all()
            ensure_chat_history_columns(banking_app.db)
            print("✅ Banking database tables initialized")

            # Run data ingestion
            try:
                from init_data import check_and_ingest_data
                check_and_ingest_data(banking_app.db.engine)
                print("✅ Data initialization complete")
            except Exception as e:
                print(f"⚠️ Data initialization warning: {e}")

        with agent_analytics.app.app_context():
            agent_analytics.db.create_all()
            ensure_chat_history_columns(agent_analytics.db)
            agent_analytics.initialize_tool_definitions()
            agent_analytics.initialize_agent_definitions()
            print("✅ Analytics database tables initialized")
        
        # Start both services in separate threads
        print("\n[2] Starting services...")
        
        banking_thread = threading.Thread(target=run_banking_service, daemon=True)
        analytics_thread = threading.Thread(target=run_analytics_service, daemon=True)
        
        banking_thread.start()
        time.sleep(2)  # Give banking service time to start
        
        analytics_thread.start()
        time.sleep(2)  # Give analytics service time to start
        
        print("✅ Banking Service: http://127.0.0.1:5001/")
        print("✅ Analytics Service: http://127.0.0.1:5002/")
        print("\n Both services are running in the same process!")
        print("Press Ctrl+C to stop all services...")
        
        # Keep the main thread alive
        try:
            while banking_thread.is_alive() or analytics_thread.is_alive():
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down services...")
            
    except Exception as e:
        print(f"Error in combined launcher: {e}")
    finally:
        print("Services stopped.")

if __name__ == '__main__':
    run_combined_services()