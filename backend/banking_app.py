# import urllib.parse
import uuid
from datetime import datetime
import json
import time
import traceback
from dateutil.relativedelta import relativedelta
# from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from shared.utils import _serialize_messages
from flask import Flask, jsonify, request, send_from_directory
import requests
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from dotenv import load_dotenv

from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from langchain_community.vectorstores.utils import DistanceStrategy
from langchain_sqlserver import SQLServer_VectorStore
from langchain_core.messages import HumanMessage, AIMessage
from langchain_core.tools import tool
from langgraph.store.memory import InMemoryStore

from shared.connection_manager import sqlalchemy_connection_creator
from tools.database_query import query_database
from analytics_service import get_chat_history_for_session
from chat_data_model import init_chat_db

from chat_data_model import init_chat_db
from ai_widget_model import init_ai_widget_db
from widget_queries import execute_widget_query  # NEW: Import for dynamic widgets

# Cosmos DB imports for widget configs and conversation history
import cosmos_widget_model
import cosmos_chat_model
# Load Environment variables and initialize app
import os
load_dotenv(override=True)

app = Flask(__name__, static_folder="static", static_url_path="/")
CORS(app)

def get_current_user_id():
    """Get user_id from request headers or query params"""
    user_id = request.headers.get('X-User-Id') or request.args.get('user_id')
    if not user_id:
        return 'user_5'  # Fallback to default demo user
    return user_id


# --- Azure OpenAI Configuration ---
AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")
AZURE_OPENAI_EMBEDDING_DEPLOYMENT = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT")

# Auto-detect environment for analytics service URL
# Local: http://127.0.0.1:5002
# Azure: /analytics (relative URL)
IS_LOCAL = not os.getenv("WEBSITE_SITE_NAME")  # Azure App Service sets this
ANALYTICS_SERVICE_URL = os.getenv(
    "ANALYTICS_SERVICE_URL",
    "http://127.0.0.1:5002" if IS_LOCAL else "/analytics"
)

if not all([AZURE_OPENAI_KEY, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_DEPLOYMENT, AZURE_OPENAI_EMBEDDING_DEPLOYMENT]):
    print("⚠️  Warning: One or more Azure OpenAI environment variables are not set.")
    ai_client = None
    embeddings_client = None
else:
    ai_client = AzureChatOpenAI(
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_version="2024-10-21",
        api_key=AZURE_OPENAI_KEY,
        azure_deployment=AZURE_OPENAI_DEPLOYMENT 
    )
    embeddings_client = AzureOpenAIEmbeddings(
        azure_deployment=AZURE_OPENAI_EMBEDDING_DEPLOYMENT,
        openai_api_version="2024-10-21",
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_key=AZURE_OPENAI_KEY,
    )

# Database configuration for Azure SQL (banking data)
app.config['SQLALCHEMY_DATABASE_URI'] = "mssql+pyodbc://"
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'creator': sqlalchemy_connection_creator,
    'poolclass': QueuePool,
    'pool_size': 5,
    'max_overflow': 10,
    'pool_pre_ping': True,
    'pool_recycle': 3600,
    'pool_reset_on_return': 'rollback'
}
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

init_chat_db(db)
AIWidget = init_ai_widget_db(db)

###################### Vector Store Initialization (for support document search) ######################

connection_string = os.getenv('FABRIC_SQL_CONNECTION_URL_AGENTIC')
# Remove Authentication parameter if present for SQLAlchemy compatibility
if "Authentication=ActiveDirectory" in connection_string:
    parts = connection_string.split(";")
    connection_string = ";".join([p for p in parts if not p.startswith("Authentication=")])
connection_url = f"mssql+pyodbc:///?odbc_connect={connection_string}"

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
vector_store = get_vector_store()

##############################################################################################################

# In-memory store for LangGraph
store = InMemoryStore()

def to_dict_helper(instance):
    d = {}
    for column in instance.__table__.columns:
        value = getattr(instance, column.name)
        if isinstance(value, datetime):
            d[column.name] = value.isoformat()
        else:
            d[column.name] = value
    return d

from langgraph.checkpoint.memory import MemorySaver
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage
from collections import defaultdict

def reconstruct_messages_from_history(history_data):
    """Converts DB history into LangChain message objects, sorted by trace_id and message order."""
    messages = []
    print("Reconstructing messages from history data:", history_data)
    
    if not history_data:
        return MemorySaver(), []
    
    # Group messages by trace_id
    traces = defaultdict(list)
    for msg_data in history_data:
        trace_id = msg_data.get('trace_id')
        if trace_id:
            traces[trace_id].append(msg_data)
    
    # Sort trace_ids chronologically
    sorted_trace_ids = sorted(traces.keys())
    
    # Process each trace in chronological order
    for trace_id in sorted_trace_ids:
        trace_messages = traces[trace_id]
        
        # Sort messages within each trace by message type priority
        message_priority = {
            'human': 1,
            'ai': 2
        }
        
        trace_messages.sort(key=lambda x: (
            message_priority.get(x.get('message_type'), 5),
            x.get('trace_end', ''),
        ))
        
        # Convert to LangChain message objects
        for msg_data in trace_messages:
            try:
                message_type = msg_data.get('message_type')
                content = msg_data.get('content', '')
                
                if message_type == 'human':
                    messages.append(HumanMessage(content=content))
                elif message_type == 'ai':
                    messages.append(AIMessage(content=content))
                
            except Exception as e:
                print(f"Error processing message in trace {trace_id}: {e}")
                continue
    
    print(f"Reconstructed {len(messages)} messages from {len(sorted_trace_ids)} traces")
    
    # Return both the memory saver and the historical messages
    return MemorySaver(), messages

# Banking Database Models
class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.String(255), primary_key=True, default=lambda: f"user_{uuid.uuid4()}")
    name = db.Column(db.String(255), nullable=False)
    email = db.Column(db.String(255), unique=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    accounts = db.relationship('Account', backref='user', lazy=True)

    def to_dict(self):
        return to_dict_helper(self)

class Account(db.Model):
    __tablename__ = 'accounts'
    id = db.Column(db.String(255), primary_key=True, default=lambda: f"acc_{uuid.uuid4()}")
    user_id = db.Column(db.String(255), db.ForeignKey('users.id'), nullable=False)
    account_number = db.Column(db.String(255), unique=True, nullable=False, default=lambda: str(uuid.uuid4().int)[:12])
    account_type = db.Column(db.String(50), nullable=False)
    balance = db.Column(db.Float, nullable=False)
    name = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        return to_dict_helper(self)

class Transaction(db.Model):
    __tablename__ = 'transactions'
    id = db.Column(db.String(255), primary_key=True, default=lambda: f"txn_{uuid.uuid4()}")
    from_account_id = db.Column(db.String(255), db.ForeignKey('accounts.id'))
    to_account_id = db.Column(db.String(255), db.ForeignKey('accounts.id'))
    amount = db.Column(db.Float, nullable=False)
    type = db.Column(db.String(50), nullable=False)
    description = db.Column(db.String(255))
    category = db.Column(db.String(255))
    status = db.Column(db.String(50), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        return to_dict_helper(self)

# Analytics Service Integration
def call_analytics_service(endpoint, method='POST', data=None):
    """Helper function to call analytics service"""
    try:
        call_start = time.time()
        # If ANALYTICS_SERVICE_URL is relative (e.g. '/analytics'),
        # requests will need the full host in Azure. For simplicity:
        base = ANALYTICS_SERVICE_URL.rstrip('/')
        url = f"{base}/api/{endpoint.lstrip('/')}"
        print(f"[analytics] Calling {method} {url}")
        if method == 'POST':
            response = requests.post(url, json=data, timeout=30)
        else:
            response = requests.get(url, timeout=15)
        call_duration = time.time() - call_start
        print(f"[analytics] Call to {url} completed in {call_duration:.2f}s "
              f"with status {getattr(response, 'status_code', 'N/A')}")
        return response.json() if response.status_code < 400 else response.status_code
    except Exception as e:
        print(f"Analytics service call failed: {e}")
        return e

# Add new user signup endpoint

@app.route('/api/auth/signup', methods=['POST'])
def signup():
    """Create a new fake user with random data"""
    try:
        data = request.json
        name = data.get('name', None)
        
        # Generate complete user data
        from user_generator import generate_user_data
        # Pass the optional name from the request to the generator
        user_data = generate_user_data(name=name)
        
        # --- BEGIN DEBUG PRE-FLIGHT CHECK ---
        print("\n\n[DEBUG] --- SIGNUP PRE-FLIGHT CHECK ---")
        account_ids = {acc['id'] for acc in user_data['accounts']}
        print(f"Generated {len(account_ids)} Account IDs: {account_ids}")
        
        orphan_txns = []
        for i, txn in enumerate(user_data['transactions']):
            from_id = txn.get('from_account_id')
            to_id = txn.get('to_account_id')
            
            if from_id and from_id not in account_ids:
                orphan_txns.append(f"Txn {i} (from_id): {from_id} --- NOT IN ACCOUNTS")
            if to_id and to_id not in account_ids:
                orphan_txns.append(f"Txn {i} (to_id): {to_id} --- NOT IN ACCOUNTS")

        if orphan_txns:
            print(f"\n[DEBUG] !!! FOUND {len(orphan_txns)} ORPHAN TRANSACTIONS !!!")
            for orphan in orphan_txns:
                print(f" - {orphan}")
        else:
            print("\n[DEBUG] --- All transaction account IDs are valid. ---")
        print("[DEBUG] --- END CHECK ---\n\n")
        # --- END DEBUG PRE-FLIGHT CHECK ---
        
        try:
            # --- STEP 1: CREATE USER ---
            print("[DEBUG] Attempting to add User...")
            new_user = User(
                id=user_data['user']['id'],
                name=user_data['user']['name'],
                email=user_data['user']['email'],
                created_at=user_data['user']['created_at']
            )
            db.session.add(new_user)
            print("[DEBUG] Attempting to commit User...")
            db.session.commit()
            print("[DEBUG] User committed successfully.")

            # --- STEP 2: CREATE ACCOUNTS ---
            print("[DEBUG] Attempting to add Accounts...")
            for acc_data in user_data['accounts']:
                new_account = Account(
                    id=acc_data['id'],
                    user_id=acc_data['user_id'],
                    account_number=acc_data['account_number'],
                    account_type=acc_data['account_type'],
                    balance=acc_data['balance'],
                    name=acc_data['name'],
                    created_at=acc_data['created_at']
                )
                db.session.add(new_account)
            print("[DEBUG] Attempting to commit Accounts...")
            db.session.commit()
            print("[DEBUG] Accounts committed successfully.")

            # --- STEP 3: CREATE TRANSACTIONS ---
            print("[DEBUG] Attempting to add Transactions...")
            for txn_data in user_data['transactions']:
                new_transaction = Transaction(
                    id=txn_data['id'],
                    from_account_id=txn_data['from_account_id'],
                    to_account_id=txn_data['to_account_id'],
                    amount=txn_data['amount'],
                    type=txn_data['type'],
                    description=txn_data['description'],
                    category=txn_data['category'],
                    status=txn_data['status'],
                    created_at=txn_data['created_at']
                )
                db.session.add(new_transaction)
            print("[DEBUG] Attempting to commit Transactions...")
            db.session.commit()
            print("[DEBUG] Transactions committed successfully.")

        except Exception as e:
            print(f"[DEBUG] ERROR during multi-step commit. Rolling back.")
            db.session.rollback()
            raise e # Re-raise the exception to be caught by the outer block
        
        return jsonify({
            'status': 'success',
            'user': {
                'id': new_user.id,
                'name': new_user.name,
                'email': new_user.email
            },
            'accounts_created': len(user_data['accounts']),
            'transactions_created': len(user_data['transactions'])
        }), 201
        
    except Exception as e:
        db.session.rollback()
        print("\n---!!!! ERROR DURING SIGNUP (OUTER CATCH) !!!! ---")
        traceback.print_exc() # This will print the full error traceback
        print("---!!!! -------------------- !!!! ---\n")
        
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/auth/users', methods=['GET'])
def get_users():
    """Get list of all users for switching"""
    try:
        users = User.query.all()
        return jsonify([{
            'id': user.id,
            'name': user.name,
            'email': user.email
        } for user in users])
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

# AI Chatbot Tool Definitions (same as before)

def get_user_accounts(user_id: str) -> str:
    """Retrieves all accounts for a given user."""
    try:
        accounts = Account.query.filter_by(user_id=user_id).all()
        if not accounts:
            return "No accounts found for this user."
        return json.dumps([
            {"name": acc.name, "account_type": acc.account_type, "balance": acc.balance} 
            for acc in accounts
        ])
    except Exception as e:
        return f"Error retrieving accounts: {str(e)}"


def get_transactions_summary(user_id: str, time_period: str = 'this year', account_name: str = None) -> str:
    """
        Provides a *categorical summary* of the user's spending for general periods.
        - Valid 'time_period' values are: 'this month', 'last 6 months', 'this year'.
        - **CRITICAL:** Do NOT use this tool for specific date ranges (e.g., 'in 2025', 'last 3 days'), or for detailed transaction lists.
        - For all specific, custom, or list-based queries, use the `query_database` tool.
        """
    try:
        query = db.session.query(Transaction.category, db.func.sum(Transaction.amount).label('total_spent')).filter(
            Transaction.type == 'payment'
        )
        if account_name:
            account = Account.query.filter_by(user_id=user_id, name=account_name).first()
            if not account:
                return json.dumps({"status": "error", "message": f"Account '{account_name}' not found."})
            query = query.filter(Transaction.from_account_id == account.id)
        else:
            user_accounts = Account.query.filter_by(user_id=user_id).all()
            account_ids = [acc.id for acc in user_accounts]
            query = query.filter(Transaction.from_account_id.in_(account_ids))

        end_date = datetime.utcnow()
        if 'last 6 months' in time_period.lower():
            start_date = end_date - relativedelta(months=6)
        elif 'this year' in time_period.lower():
            start_date = end_date.replace(month=1, day=1, hour=0, minute=0, second=0)
        else:
            start_date = end_date.replace(day=1, hour=0, minute=0, second=0)
        
        query = query.filter(Transaction.created_at.between(start_date, end_date))
        results = query.group_by(Transaction.category).order_by(db.func.sum(Transaction.amount).desc()).all()
        total_spending = sum(r.total_spent for r in results)
        
        summary_details = {
            "total_spending": round(total_spending, 2),
            "period": time_period,
            "account_filter": account_name or "All Accounts",
            "top_categories": [{"category": r.category, "amount": round(r.total_spent, 2)} for r in results[:3]]
        }

        if not results:
            return json.dumps({"status": "success", "summary": f"You have no spending for the period '{time_period}' in account '{account_name or 'All Accounts'}'."})

        return json.dumps({"status": "success", "summary": summary_details})
    except Exception as e:
        print(f"ERROR in get_transactions_summary: {e}")
        return json.dumps({"status": "error", "message": f"An error occurred while generating the transaction summary."})


def search_support_documents(user_question: str) -> str:
    """Searches the knowledge base for answers to customer support questions using vector search."""
    if not vector_store:
        return "The vector store is not configured."
    try:
        # Basic transient retry for dropped connections
        max_attempts = 3
        last_err = None
        for attempt in range(1, max_attempts + 1):
            try:
                results = vector_store.similarity_search_with_score(user_question, k=3)
                break
            except Exception as e:
                last_err = e
                err_str = str(e)
                # Only retry on connection-related errors
                if "08S01" in err_str or "TCP Provider" in err_str or "connection" in err_str.lower():
                    print(f"WARNING: search_support_documents transient error on attempt {attempt}: {e}")
                    continue
                else:
                    raise
        else:
            # All attempts failed
            print(f"ERROR in search_support_documents after {max_attempts} attempts: {last_err}")
            return "An error occurred while searching for support documents."

        relevant_docs = [doc.page_content for doc, score in results]
        print("-------------> ", relevant_docs)
        if not relevant_docs:
            return "No relevant support documents found to answer this question."

        context = "\n\n---\n\n".join(relevant_docs)
        return context

    except Exception as e:
        print(f"ERROR in search_support_documents: {e}")
        return "An error occurred while searching for support documents."


def create_new_account(user_id: str, account_type: str = 'checking', name: str = None, balance: float = 0.0) -> str:
    """Creates a new bank account for the user."""
    if not name:
        return json.dumps({"status": "error", "message": "An account name is required."})
    try:
        new_account = Account(user_id=user_id, account_type=account_type, balance=balance, name=name)
        db.session.add(new_account)
        db.session.commit()
        return json.dumps({
            "status": "success", "message": f"Successfully created new {account_type} account '{name}' with balance ${balance:.2f}.",
            "account_id": new_account.id, "account_name": new_account.name
        })
    except Exception as e:
        db.session.rollback()
        return f"Error creating account: {str(e)}"


def transfer_money(user_id: str, from_account_name: str = None, to_account_name: str = None, amount: float = 0.0, to_external_details: dict = None) -> str:
    """Transfers money between user's accounts or to an external account."""
    if not from_account_name or (not to_account_name and not to_external_details) or amount <= 0:
        return json.dumps({"status": "error", "message": "Missing required transfer details."})
    try:
        from_account = Account.query.filter_by(user_id=user_id, name=from_account_name).first()
        if not from_account:
            return json.dumps({"status": "error", "message": f"Account '{from_account_name}' not found."})
        if from_account.balance < amount:
            return json.dumps({"status": "error", "message": "Insufficient funds."})
        
        to_account = None
        if to_account_name:
            to_account = Account.query.filter_by(user_id=user_id, name=to_account_name).first()
            if not to_account:
                 return json.dumps({"status": "error", "message": f"Recipient account '{to_account_name}' not found."})
        
        new_transaction = Transaction(
            from_account_id=from_account.id, to_account_id=to_account.id if to_account else None,
            amount=amount, type='transfer', description=f"Transfer to {to_account_name or to_external_details.get('name', 'External')}",
            category='Transfer', status='completed'
        )
        from_account.balance -= amount
        if to_account:
            to_account.balance += amount
        db.session.add(new_transaction)
        db.session.commit()
        return json.dumps({"status": "success", "message": f"Successfully transferred ${amount:.2f}."})
    except Exception as e:
        db.session.rollback()
        return f"Error during transfer: {str(e)}"

def query_tool():
    """Wrapper for database query tool."""
    return query_database

# Banking API Routes
@app.route('/api/accounts', methods=['GET', 'POST'])
def handle_accounts():
    user_id = get_current_user_id()
    if request.method == 'GET':
        accounts = Account.query.filter_by(user_id=user_id).all()
        return jsonify([acc.to_dict() for acc in accounts])
    if request.method == 'POST':
        data = request.json
        account_str = create_new_account(user_id=user_id, account_type=data.get('account_type'), name=data.get('name'), balance=data.get('balance', 0))
        return jsonify(json.loads(account_str)), 201
    
@app.route('/api/transactions', methods=['GET', 'POST'])
def handle_transactions():
    user_id = get_current_user_id()
    if request.method == 'GET':
        accounts = Account.query.filter_by(user_id=user_id).all()
        account_ids = [acc.id for acc in accounts]
        transactions = Transaction.query.filter((Transaction.from_account_id.in_(account_ids)) | (Transaction.to_account_id.in_(account_ids))).order_by(Transaction.created_at.desc()).all()
        return jsonify([t.to_dict() for t in transactions])
    if request.method == 'POST':
        data = request.json
        result_str = transfer_money(
            user_id=user_id, from_account_name=data.get('from_account_name'), to_account_name=data.get('to_account_name'),
            amount=data.get('amount'), to_external_details=data.get('to_external_details')
        )
        result = json.loads(result_str)
        status_code = 201 if result.get("status") == "success" else 400
        return jsonify(result), status_code
# ============================================
# AI Widget API Routes - WITH DYNAMIC SUPPORT
# ============================================

@app.route('/api/ai-widgets', methods=['GET', 'POST'])
def handle_ai_widgets():
    '''Handle AI widget list and creation — backed by Cosmos DB gen_ui_config'''
    user_id = get_current_user_id()

    if request.method == 'GET':
        widgets = cosmos_widget_model.get_user_widgets(user_id)
        return jsonify(widgets)

    if request.method == 'POST':
        data = request.json
        widget = cosmos_widget_model.create_widget(
            user_id=user_id,
            title=data.get('title', 'Untitled Widget'),
            description=data.get('description', ''),
            widget_type=data.get('widget_type', 'chart'),
            config=data.get('config', {}),
            code=data.get('code'),
            data_mode=data.get('data_mode', 'static'),
            query_config=data.get('query_config')
        )
        return jsonify(widget), 201


@app.route('/api/ai-widgets/<widget_id>', methods=['GET', 'PUT', 'DELETE'])
def handle_ai_widget(widget_id):
    '''Handle individual AI widget operations — backed by Cosmos DB gen_ui_config'''
    user_id = get_current_user_id()

    if request.method == 'GET':
        widget = cosmos_widget_model.get_widget_by_id(widget_id, user_id)
        if not widget:
            return jsonify({"error": "Widget not found"}), 404
        return jsonify(widget)

    if request.method == 'PUT':
        data = request.json
        widget = cosmos_widget_model.update_widget(widget_id, user_id, data)
        if not widget:
            return jsonify({"error": "Widget not found or access denied"}), 404
        return jsonify(widget)

    if request.method == 'DELETE':
        success = cosmos_widget_model.delete_widget(widget_id, user_id)
        if not success:
            return jsonify({"error": "Widget not found or access denied"}), 404
        return jsonify({"status": "success", "message": "Widget deleted"})


@app.route('/api/ai-widgets/<widget_id>/refresh', methods=['POST'])
def refresh_ai_widget(widget_id):
    '''Refresh a dynamic widget with fresh data from the database'''
    user_id = get_current_user_id()
    
    print(f"\n[ai-widgets] ========== REFRESH REQUEST ==========")
    print(f"[ai-widgets] Widget ID: {widget_id}")
    print(f"[ai-widgets] User ID: {user_id}")
    
    # Get the widget from Cosmos DB
    widget = cosmos_widget_model.get_widget_by_id(widget_id, user_id)
    if not widget:
        print(f"[ai-widgets] ERROR: Widget not found")
        return jsonify({"error": "Widget not found"}), 404
    
    print(f"[ai-widgets] Widget found: {widget.get('title')}")
    print(f"[ai-widgets] Data mode: {widget.get('data_mode')}")
    
    # Check if it's a dynamic widget
    if widget.get('data_mode') != 'dynamic':
        return jsonify({
            "error": "This widget uses static data and cannot be refreshed",
            "data_mode": widget.get('data_mode', 'static')
        }), 400
    
    # Get query config
    query_config = widget.get('query_config')
    if not query_config:
        return jsonify({"error": "Widget has no query configuration"}), 400
    
    print(f"[ai-widgets] Query config: {query_config}")
    
    try:
        # Execute the query to get fresh data
        print(f"[ai-widgets] Executing query...")
        fresh_data = execute_widget_query(query_config, user_id, db.session)
        
        print(f"[ai-widgets] Query returned {len(fresh_data)} data points:")
        for item in fresh_data:
            print(f"[ai-widgets]   - {item}")
        
        # Get OLD data for comparison
        old_data = widget.get('config', {}).get('customProps', {}).get('data', [])
        print(f"[ai-widgets] Old data had {len(old_data)} points")
        
        # Update the widget with new data
        print(f"[ai-widgets] Updating widget data...")
        updated_widget = cosmos_widget_model.update_widget_data(widget_id, user_id, fresh_data)
        
        if not updated_widget:
            print(f"[ai-widgets] ERROR: update_widget_data returned None")
            return jsonify({"error": "Failed to update widget data"}), 500
        
        # Verify the update
        new_data_in_widget = updated_widget.get('config', {}).get('customProps', {}).get('data', [])
        print(f"[ai-widgets] After update, widget has {len(new_data_in_widget)} data points")
        print(f"[ai-widgets] ========== REFRESH COMPLETE ==========\n")
        
        return jsonify({
            "status": "success",
            "message": "Widget data refreshed",
            "widget": updated_widget,
            "data_points": len(fresh_data)
        })
        
    except Exception as e:
        print(f"[ai-widgets] Error refreshing widget {widget_id}: {e}")
        traceback.print_exc()
        return jsonify({"error": f"Failed to refresh data: {str(e)}"}), 500

####################################################################################
######################## MAIN CHATBOT FUNCTION #####################################
####################################################################################

from multi_agent_banking import create_multi_agent_banking_system, execute_trace
from chat_data_model import prep_multi_agent_log_load, handle_content_safety_error
import threading
@app.route('/api/chatbot', methods=['POST'])
def chatbot():
    """Main chatbot endpoint"""
    request_start = time.time()
    print("\n========== /api/chatbot request started ==========")

    if not ai_client:
        print("[chatbot] Azure OpenAI client is not configured.")
        return jsonify({"error": "Azure OpenAI client is not configured."}), 503

    try:
        parse_start = time.time()
        data = request.json
        messages = data.get("messages", [])
        session_id = data.get("session_id")
        user_id = data.get("user_id") or get_current_user_id()  # Get from request body or headers
        create_widget_hint = data.get("create_widget", False)  # Hint from frontend that user wants a widget
        edit_widget = data.get("edit_widget", None)  # Widget being edited (if any)
        parse_duration = time.time() - parse_start
        print(f"[chatbot] Parsed request in {parse_duration:.2f}s "
              f"(session_id={session_id}, user_id={user_id})")
        
        widget_instructions = ""
        if edit_widget:
            widget_instructions = f"""
            
            ## IMPORTANT: Widget EDIT Request Detected ##
            The user wants to MODIFY an existing widget. Here are the details:
            - Widget ID: {edit_widget.get('widget_id')}
            - Current Title: {edit_widget.get('title')}
            - Current Chart Type: {edit_widget.get('chart_type')}
            - Current Data Mode: {edit_widget.get('data_mode')}
            - Current Query Config: {edit_widget.get('query_config')}
            
            Use the `update_ai_widget_for_current_user` tool to modify this widget.
            You MUST pass widget_id="{edit_widget.get('widget_id')}" along with the fields to change.
            
            Common modifications:
            - To change chart type: chart_type="pie" (or "bar", "line", "area")
            - To change title: title="New Title"
            - To change time range: time_range="last_3_months"
            - To change query type: query_type="monthly_trend"
            """
        elif create_widget_hint:
            widget_instructions = """
            
            ## IMPORTANT: Widget Creation Request Detected ##
            The user wants to create a visualization in their AI Module. You MUST:
            1. Determine if the widget should be DYNAMIC or STATIC based on the request
            2. For DYNAMIC widgets: use query_type and time_range parameters, DO NOT fetch data manually
            3. For STATIC widgets: first gather data, then pass it in the 'data' parameter
            4. Tell the user the widget was created and whether it's dynamic (refreshable) or static
            """
        
        # Fetch chat history from Cosmos DB longterm_memory
        history_start = time.time()
        cosmos_history = cosmos_chat_model.get_conversation_history(
            session_id=session_id, user_id=user_id
        )
        history_duration = time.time() - history_start
        print(
            f"[chatbot] History fetch (Cosmos DB) duration: {history_duration:.2f}s "
            f"(records={len(cosmos_history) if cosmos_history else 0})"
        )

        # Convert Cosmos messages to LangChain message objects
        reconstruct_start = time.time()
        historical_messages = []
        for msg_data in (cosmos_history or []):
            msg_type = msg_data.get("type", "")
            content = msg_data.get("content", "")
            if msg_type == "human":
                historical_messages.append(HumanMessage(content=content))
            elif msg_type == "ai":
                historical_messages.append(AIMessage(content=content))
        reconstruct_duration = time.time() - reconstruct_start
        print(f"[chatbot] Reconstructed history in {reconstruct_duration:.2f}s: "
              f"{len(historical_messages)} historical messages")

        # Print debugging info (existing debug prints preserved)
        print("\n--- Context being passed to the agent ---")
        print(f"Current User ID: {user_id}")
        print(f"History data received: {len(cosmos_history) if cosmos_history else 0} messages")
        print(f"Historical messages reconstructed: {len(historical_messages)}")
        for i, msg in enumerate(historical_messages):
            print(f"[History {i}] {msg.__class__.__name__}: {msg.content[:120]}...")

        # Extract current user message
        # Build agent input
        agent_prep_start = time.time()

        banking_system = create_multi_agent_banking_system()
        user_message = messages[-1].get("content", "")
        
        all_messages = historical_messages + [HumanMessage(content=user_message)]

        
        # Create initial state
        initial_state = {
            "messages": all_messages,
            "from_agent": "",
            "current_agent": "",
            "task_type": "",
            "user_id": user_id,
            "session_id": session_id,
            "final_result": "",
            "time_taken":0,
            "widget_instructions": widget_instructions
        }
        print("state being passed: ", initial_state)


        
        # Thread config for session management
        thread_config = {"configurable": {"thread_id": session_id}}
        agent_prep_duration = time.time() - agent_prep_start
        print(f"[chatbot] Agent prep (prompt, tools, messages) completed in "
              f"{agent_prep_duration:.2f}s")

        trace_start_time = time.time()
        trace_id = str(uuid.uuid4())
        print("[New Trace] Starting new trace with ID:", trace_id)

        ##################### Initialize Event Hub Producer Client for analytics streaming #####################
        from azure.eventhub import EventHubProducerClient, EventData
        eventHubConnString = os.getenv("FABRIC_EVENT_HUB_CONNECTION_STRING")
        eventHubName = os.getenv("FABRIC_EVENT_HUB_NAME")
        stream_flag = False
        try:
            producer_events = EventHubProducerClient.from_connection_string(conn_str=eventHubConnString, eventhub_name=eventHubName)
            stream_flag = True
        except Exception as e:
            print(f"[chatbot] Warning: Failed to create EventHubProducerClient: {e}")


        #########################################################################################################
        from agent_analytics import stream_load
        flag = False
        try:
            trace_events, result= execute_trace(banking_system, initial_state, thread_config)
            end_time = time.time()
            trace_duration = int((end_time - trace_start_time) * 1000)

            analytics_data = prep_multi_agent_log_load(trace_events=trace_events,
                                                        session_id=session_id,
                                                        user_id=user_id,
                                                        trace_id = trace_id,
                                                        trace_duration=trace_duration)
            
            # simulate extremely sensitive content by using one of below trigger words in the user message
            sensitive_list = ["violence", "self_harm", "hate",
                              "sexual", "jailbreak"]
            
            if(user_message in sensitive_list):
                flag = True
                result = res_dict["content"] 
             
            # Save human + AI messages to Cosmos DB longterm_memory
            try:
                cosmos_messages = [
                    {"type": "human", "content": user_message},
                    {"type": "ai", "content": result},
                ]
                cosmos_chat_model.save_conversation_messages(
                    session_id=session_id,
                    user_id=user_id,
                    messages=cosmos_messages,
                )
            except Exception as cosmos_err:
                print(f"[chatbot] Warning: Failed to save to Cosmos DB longterm_memory: {cosmos_err}")

            # Send analytics in background thread - DON'T WAIT
            def log_analytics_async():
                try:
                    if stream_flag:
                        stream_load(producer_events=producer_events, 
                                result_dict=analytics_data,
                                user_msg=user_message)
                    call_analytics_service("chat/log-multi-agent-trace", 
                                        data=analytics_data)
                except Exception as e:
                    print(f"[Analytics] Background logging failed: {e}")
            
            # Start background thread
            analytics_thread = threading.Thread(target=log_analytics_async, daemon=True)
            analytics_thread.start()
                    
        # handling extremely sensitive content error that caused llm provider to block the response
        except Exception as e:
            end_time = time.time()
            trace_duration = int((end_time - trace_start_time) * 1000)
            
            from unsafe_content_simulator import  simulate_safety_error
            if(flag): 
                # in case of a simulated content safety error ... 
                if(user_message == "violence"):
                    simulate_error = simulate_safety_error(violence_severity="high", violence_filtered=True)
                elif(user_message == "self_harm"):
                    simulate_error = simulate_safety_error(self_harm_severity="high", self_harm_filtered=True)
                elif(user_message == "hate"):
                    simulate_error = simulate_safety_error(hate_severity="high", hate_filtered=True)
                elif(user_message == "sexual"):
                    simulate_error = simulate_safety_error(sexual_severity="high", sexual_filtered=True)
                else:
                    simulate_error = simulate_safety_error(jailbreak_detected=True, jailbreak_filtered=True)
                e=str(simulate_error.message)
            result_dict = handle_content_safety_error(trace_id=trace_id, session_id=session_id, user_id=user_id, error = e, user_message=user_message)
            result = result_dict["message"].get("content")

            # Save human + AI messages to Cosmos DB longterm_memory (safety response)
            try:
                cosmos_messages = [
                    {"type": "human", "content": user_message},
                    {"type": "ai", "content": result},
                ]
                cosmos_chat_model.save_conversation_messages(
                    session_id=session_id,
                    user_id=user_id,
                    messages=cosmos_messages,
                )
            except Exception as cosmos_err:
                print(f"[chatbot] Warning: Failed to save safety response to Cosmos DB: {cosmos_err}")

            def log_analytics_async():
                try:
                    if stream_flag:
                        stream_load(producer_events=producer_events, 
                                result_dict=result_dict,
                                user_msg=user_message, failed_response=True)
                    call_analytics_service("chat/log-content-safety-violation", 
                                        data=result_dict)
                except Exception as e:
                    print(f"[Analytics] Background logging failed: {e}")
        
            # Start background thread
            analytics_thread = threading.Thread(target=log_analytics_async, daemon=True)
            analytics_thread.start()


        total_duration = time.time() - request_start
        print(f"[chatbot] Total /api/chatbot request duration: {total_duration:.2f}s")
        print("========== /api/chatbot request finished ==========\n")

        return jsonify({
            "response": result,
            "session_id": session_id,
            "tools_used": []
        })
    except Exception as e:
        total_duration = time.time() - request_start
        print(f"[chatbot] ERROR after {total_duration:.2f}s: {e}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

def initialize_banking_app():
    """Initialize banking app when called from combined launcher."""
    with app.app_context():
        db.create_all()
        print("[Banking Service] Database tables initialized")

# ---------- Frontend (React) routes ----------

@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve_frontend(path):
    """
    Serve the React frontend.

    - If the requested path matches a file in the static folder (e.g. JS, CSS), serve that.
    - Otherwise, serve index.html so React Router can handle the route.
    """
    # If the request is for an API route, let other handlers deal with it
    if path.startswith("api") or path.startswith("analytics"):
        return jsonify({"error": "Not found"}), 404

    static_folder = app.static_folder  # "static"
    full_path = os.path.join(static_folder, path)

    if path != "" and os.path.exists(full_path):
        return send_from_directory(static_folder, path)
    else:
        # Fallback: always serve index.html
        return send_from_directory(static_folder, "index.html")