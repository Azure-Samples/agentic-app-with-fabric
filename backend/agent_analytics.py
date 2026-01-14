import uuid
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from dotenv import load_dotenv
from sqlalchemy.pool import QueuePool
import os
from chat_data_model import init_chat_db
from shared.connection_manager import sqlalchemy_connection_creator, connection_manager
from azure.eventhub import EventHubProducerClient, EventData
load_dotenv(override=True)

app = Flask(__name__)
CORS(app)

# Database configuration for Fabric SQL (analytics data)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
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

db = SQLAlchemy(app)

# Initialize chat history module with database
init_chat_db(db)
from chat_data_model import (
    ToolDefinition, ChatHistoryManager, AgentDefinition, AgentTrace,
    handle_chat_sessions, 
    clear_chat_history, clear_session_data, initialize_tool_definitions, 
    initialize_agent_definitions
)

# Chat History API Routes
@app.route('/api/chat/sessions', methods=['GET', 'POST'])
def chat_sessions_route():
    print("Handling chat sessions request...")
    return handle_chat_sessions(request)

@app.route('/api/chat/history/<session_id>', methods=['GET'])
def get_chat_history(session_id):
    """New endpoint to retrieve chat history for a session."""
    try:
        # Use the ChatHistoryManager to get the history
        chat_manager = ChatHistoryManager(session_id=session_id)
        history = chat_manager.get_conversation_history()
        return jsonify(history)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/admin/clear-chat-history', methods=['DELETE'])
def clear_chat_route():
    return clear_chat_history()

@app.route('/api/admin/clear-session/<session_id>', methods=['DELETE'])
def clear_session_route(session_id):
    return clear_session_data(session_id)

@app.route('/api/tools/definitions', methods=['GET', 'POST'])
def handle_tool_definitions():
    if request.method == 'GET':
        tools = ToolDefinition.query.filter_by(is_active=True).all()
        return jsonify([tool.to_dict() for tool in tools])
    
    if request.method == 'POST':
        data = request.json
        tool_def = ToolDefinition(
            name=data['name'],
            description=data.get('description'),
            input_schema=data['input_schema'],
            version=data.get('version', '1.0.0'),
            cost_per_call_cents=data.get('cost_per_call_cents', 0)
        )
        db.session.add(tool_def)
        db.session.commit()
        return jsonify(tool_def.to_dict()), 201
    
@app.route('/api/agents/definitions', methods=['GET', 'POST'])
def handle_agent_definitions():
    """Handle agent definitions"""
    if request.method == 'GET':
        agents = AgentDefinition.query.all()
        return jsonify([agent.to_dict() for agent in agents])
    
    if request.method == 'POST':
        data = request.json
        agent_def = AgentDefinition(
            name=data['name'],
            description=data.get('description'),
            agent_type=data.get('agent_type', 'specialist'),
            llm_config=data.get('llm_config', {}),
            prompt_template=data.get('prompt_template', '')
        )
        db.session.add(agent_def)
        db.session.commit()
        return jsonify(agent_def.to_dict()), 201
# Enhanced endpoint for logging multi-agent traces
    
@app.route('/api/chat/log-multi-agent-trace', methods=['POST'])
def log_multi_agent_trace():
    """Log multi-agent trace with enhanced context"""
    import traceback

    try:
        data = request.json
        chat_manager = ChatHistoryManager(
            session_id=data.get('session_id'),
            user_id=data.get('user_id')
        )
        
        # Log the multi-agent trace
        result = chat_manager.add_multi_agent_trace(
            trace_id =data.get('trace_id'),
            serialized_messages=data.get('messages'),
            trace_duration=data.get('trace_duration', 0),
            event_times=data.get('event_times', []),
            nodes_list=data.get('nodes_list', [])
        )

        return jsonify({
            "status": "success",
            "message": result,
            "agent_used": "multi-agent",
            "task_type": "banking"
        }), 201

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500
    
@app.route('/api/chat/log-content-safety-violation', methods=['POST'])   
def log_content_safety_violation():
    """
    Log a content safety violation to the chat history.
    
    This is a convenience wrapper that:
    1. Handles the BadRequestError
    2. Creates appropriate messages
    3. Logs them to the database
    
    Args:
        error: The openai.BadRequestError exception
        session_id: Current chat session ID
        user_id: Current user ID
        user_message: The user's original message (optional)
        agent_name: Name of the agent that triggered the error
    
    Returns:
        json payload with status and logged message details.
    """
    res_dict = request.json
    try:
        # Initialize chat history manager
        chat_manager = ChatHistoryManager(session_id=res_dict.get('session_id'), user_id=res_dict.get('user_id'))
        
        # Log the user's message if provided
        if res_dict.get("user_message"):
            user_msg = {
                "__class__": "HumanMessage",
                "id": f"msg_{uuid.uuid4()}",
                "content": res_dict.get("user_message")
            }
            chat_manager.add_human_message(
                message=user_msg,
                trace_id=res_dict.get("trace_id"),
                routing_step=0
            )
        
        # Log the safety response
        chat_manager.add_ai_message(
            message=res_dict["message"],
            trace_id=res_dict.get("trace_id"),
            step_duration=0,
            agent_name=res_dict["agent_name"],
            routing_step=1
        )
        
        # Log the agent trace
        chat_manager._log_agent_routing(
            trace_id=res_dict.get("trace_id"),
            step_number=1,
            from_agent="system",
            current_agent=res_dict["agent_name"],
            execution_duration_ms=0
        )
        
        print(f"[Content Safety Handler] Logged violation to database. Session: {res_dict.get('session_id')}")
        return jsonify({
            "status": "success",
            "message": res_dict["message"]["content"],
            "agent_used": "coordinator",
            "task_type": "banking"
        }), 201

        
    except Exception as log_error:
        print(f"[Content Safety Handler] Failed to log safety violation: {log_error}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(log_error), "traceback": traceback.format_exc()}), 500
    
# Endpoint for logging messages from banking service (single agent)
@app.route('/api/chat/log-trace', methods=['POST'])
def log_trace():
    import traceback

    try:
        data = request.json
        chat_manager = ChatHistoryManager(
            session_id=data.get('session_id'),
            user_id=data.get('user_id')
        )
        # call into the chat history manager (note: method expects 'message' kw)
        chat_manager.add_trace_messages(
            serialized_messages=data.get('messages'),
            trace_duration=data.get('trace_duration')
        )

        return jsonify({"status": "success"}), 201

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500

    
def initialize_analytics_app():
    """Initialize analytics app when called from combined launcher."""
    with app.app_context():
        db.create_all()
        initialize_tool_definitions()
        initialize_agent_definitions()
        print("[Analytics Service] Database tables initialized")


import json
from datetime import datetime
from azure.eventhub import EventData
from shared.utils import _to_json_primitive
def sendToEventsHub(jsonEvent, producer_events):
    event_data_batch = producer_events.create_batch() 
    event_data_batch.add(EventData(jsonEvent)) 
    producer_events.send_batch(event_data_batch)

def stream_load(result_dict: dict, user_msg: str,
                 producer_events, failed_response: bool = False):
    event_time = datetime.now().isoformat()
    try:
        if failed_response:
            stream_dict = {
                "timestamp": event_time,
                "trace_id": result_dict.get("trace_id"),
                "session_id": result_dict.get("session_id"),
                "user_id": result_dict.get("user_id"),
                "message": result_dict.get("message"),
                "agent_name": result_dict.get("agent_name"),
                "user_message": user_msg,
                "filter_category": result_dict.get("filter_category"),
                "content_filter_info": result_dict.get("content_filter_info")
            }
            sendToEventsHub(json.dumps(stream_dict), producer_events)
            print("event message sent for single blocked message")
            

        else:
            
            for i in range(len(result_dict.get("messages", []))):
                stream_dict = {
                "timestamp": event_time,
                "trace_id": result_dict.get("trace_id"),
                "session_id": result_dict.get("session_id"),
                "user_id": result_dict.get("user_id"),
                "message": _to_json_primitive(result_dict.get("messages", [])[i]),
                "agent_name" : result_dict.get("nodes_list", [])[i],                       
                "user_message": user_msg,
                "filter_category": "None",
                "content_filter_info": "User content Not blocked"
                }
                sendToEventsHub(json.dumps(stream_dict), producer_events)
                print("event message sent for iteration:", i)
            print("Number of stream batches sent:", len(result_dict.get("messages", [])))
            return True
    except Exception as e:
        print("Error in stream load to Event Hub:", str(e))
        return False