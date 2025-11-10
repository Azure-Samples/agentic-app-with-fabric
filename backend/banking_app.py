# import urllib.parse
import uuid
import asyncio
from datetime import datetime
import json
import time
from dateutil.relativedelta import relativedelta
# from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from dotenv import load_dotenv
from agent_framework import ChatAgent
from agent_framework.azure import AzureOpenAIChatClient
from openai import AsyncOpenAI
from shared.connection_manager import sqlalchemy_connection_creator, connection_manager
from shared.utils import get_user_id
import requests  # For calling analytics service
from shared.utils import _serialize_messages
from init_data import check_and_ingest_data
# Load Environment variables and initialize app
import os
from azure.identity import AzureCliCredential

load_dotenv(override=True)

app = Flask(__name__)
CORS(app)
global fixed_user_id
fixed_user_id = get_user_id()  # For simplicity, using a fixed user ID

# Analytics service URL
ANALYTICS_SERVICE_URL = "http://127.0.0.1:5002"

# Initialize OpenAI client for Agent Framework
chat_client = None

try:
  
    # Create chat client for Agent Framework
    chat_client = AzureOpenAIChatClient(
        deployment_name=os.environ["AZURE_OPENAI_DEPLOYMENT"],
        endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
        credential=AzureCliCredential()
    )
    print("✅ Agent Framework OpenAI client initialized successfully")
except Exception as e:
    print(f"❌ Failed to initialize OpenAI client: {e}")
    chat_client = None

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

connection_string = os.getenv('FABRIC_SQL_CONNECTION_URL_AGENTIC')

connection_url = f"mssql+pyodbc:///?odbc_connect={connection_string}"

# Vector search will be implemented as Agent Framework tools
# Agent Framework doesn't use direct vector store integration

def to_dict_helper(instance):
    d = {}
    for column in instance.__table__.columns:
        value = getattr(instance, column.name)
        if isinstance(value, datetime):
            d[column.name] = value.isoformat()
        else:
            d[column.name] = value
    return d

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
        url = f"{ANALYTICS_SERVICE_URL}/api/{endpoint}"
        if method == 'POST':
            response = requests.post(url, json=data, timeout=5)
        else:
            response = requests.get(url, timeout=5)
        return response.json() if response.status_code < 400 else None
    except Exception as e:
        print(f"Analytics service call failed: {e}")
        return None


# AI Chatbot Tool Definitions (same as before)
def get_user_accounts(user_id: str = fixed_user_id) -> str:
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

def get_transactions_summary(user_id: str = fixed_user_id, time_period: str = 'this month', account_name: str = None) -> str:
    """Provides a summary of the user's spending. Can be filtered by a time period and a specific account."""
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
    # TODO: Implement vector search using Agent Framework tools or Azure AI Search
    # For now, return a placeholder response
    return f"Document search functionality is being updated. Your question about '{user_question}' has been noted. Please contact customer support for immediate assistance."

def create_new_account(user_id: str = fixed_user_id, account_type: str = 'checking', name: str = None, balance: float = 0.0) -> str:
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

def transfer_money(user_id: str = fixed_user_id, from_account_name: str = None, to_account_name: str = None, amount: float = 0.0, to_external_details: dict = None) -> str:
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

# Banking API Routes
@app.route('/api/accounts', methods=['GET', 'POST'])
def handle_accounts():
    user_id = fixed_user_id
    if request.method == 'GET':
        accounts = Account.query.filter_by(user_id=user_id).all()
        return jsonify([acc.to_dict() for acc in accounts])
    if request.method == 'POST':
        data = request.json
        account_str = create_new_account(user_id=user_id, account_type=data.get('account_type'), name=data.get('name'), balance=data.get('balance', 0))
        return jsonify(json.loads(account_str)), 201
    
@app.route('/api/transactions', methods=['GET', 'POST'])
def handle_transactions():
    user_id = fixed_user_id
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
@app.route('/api/chatbot', methods=['POST'])
def chatbot():
    if not chat_client:
        return jsonify({"error": "Agent Framework chat client is not configured."}), 503

    data = request.json
    messages = data.get("messages", [])
    session_id = data.get("session_id")
    user_id = fixed_user_id
    
    # Extract current user message
    user_message = messages[-1].get("content", "")
    
    # Run the agent asynchronously
    try:
        response_text = asyncio.run(run_banking_agent(user_message, session_id))
        
        # Log to analytics service
        analytics_data = {
            "session_id": session_id,
            "user_id": user_id,
            "messages": [
                {"type": "human", "content": user_message},
                {"type": "ai", "content": response_text}
            ],
            "trace_duration": 0,  # Agent Framework handles timing internally
        }
        
        call_analytics_service("chat/log-trace", data=analytics_data)
        
        return jsonify({
            "response": response_text,
            "session_id": session_id,
            "tools_used": []
        })
        
    except Exception as e:
        print(f"Error in chatbot: {e}")
        return jsonify({"error": "An error occurred processing your request."}), 500

async def run_banking_agent(user_message: str, session_id: str) -> str:
    """Run the banking agent using Agent Framework"""
    if not chat_client:
        return "Chat client is not available."
    
    try:
        # Create agent with tools
        agent = chat_client.create_agent(
            name="BankingAgent",
            instructions="""You are a helpful banking customer support agent. 
            - Help users with their banking questions and tasks
            - Use the provided tools to access account information and perform operations
            - Be helpful and professional
            - If you cannot find information, inform the user politely""",
            tools=[
                get_user_accounts, 
                get_transactions_summary,
                search_support_documents,
                create_new_account,
                transfer_money
            ],
        )
        
        # Get or create thread for session persistence
        thread = agent.get_new_thread()  # You could implement session-based thread retrieval here
        
        # Run the agent
        result = await agent.run(user_message, thread=thread)
        return result.text
            
    except Exception as e:
        print(f"Error running banking agent: {e}")
        return "I apologize, but I'm having trouble processing your request right now. Please try again later."

def initialize_banking_app():
    """Initialize banking app when called from combined launcher."""
    with app.app_context():
        db.create_all()
        print("[Banking Service] Database tables initialized")