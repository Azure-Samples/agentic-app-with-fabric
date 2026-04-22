import time
import uuid
from datetime import datetime
from typing import List, Dict, Any

from shared.utils import _serialize_messages, _to_json_primitive
from shared.pricing import estimate_cost
from langchain_core.messages import BaseMessage
import os
from dotenv import load_dotenv

load_dotenv()



def get_chat_history_for_session(session_id: str, user_id: str) -> List[Dict[str, Any]]:
    """
    Retrieve conversation history from Cosmos DB longterm_memory container.
    Returns a list of message dicts: [{type, content, timestamp}, ...]
    """
    import cosmos_chat_model
    try:
        messages = cosmos_chat_model.get_conversation_history(
            session_id=session_id, user_id=user_id
        )
        # Map to the format expected by reconstruct_messages_from_history
        result = [
            {
                "trace_id": session_id,
                "message_type": msg.get("type", "unknown"),
                "content": msg.get("content", ""),
                "trace_end": msg.get("timestamp"),
            }
            for msg in messages
        ]
        return result

    except Exception as e:
        print(f"[analytics_service] Error fetching chat history from Cosmos DB: {e}")
        import traceback
        traceback.print_exc()
        return []

def log_chat_trace(
    session_id: str,
    user_id: str,
    messages: List[BaseMessage],
    trace_duration_ms: int,
) -> None:
    """
    In-process equivalent of POST /analytics/api/chat/log-trace.
    
    When called from banking_app, this directly inserts into the database
    to avoid context issues with ChatHistoryManager.
    """
    start = time.time()
    
    # Since we're being called from banking_app, use its database directly
    from banking_app import db
    
    # Import the model classes to access table structure
    from chat_data_model import ChatSession, ChatHistory, ToolUsage, ToolDefinition, AgentDefinition, ChatHistoryManager
    
    try:
        # First ensure the session exists - use db.session.query instead of Model.query
        session = db.session.query(ChatSession).filter(
            ChatSession.session_id == session_id
        ).first()
        
        if not session:
            session = ChatSession(
                session_id=session_id,
                title="New Session",
                user_id=user_id,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            db.session.add(session)
            db.session.commit()
            print(f"[analytics_service] Created new session: {session_id}")
        
        # Generate a trace_id for this batch of messages
        trace_id = str(uuid.uuid4())
        serialized_messages = _serialize_messages(messages)
        message_list = _to_json_primitive(serialized_messages)
        
        print(f"[analytics_service] Adding {len(message_list)} messages for trace_id: {trace_id}")
        
        # Process each message
        tool_info_dict = {}  # Track tool calls for later processing
        
        for msg in message_list:
            if msg['type'] == 'human':
                # Add human message
                entry = ChatHistory(
                    session_id=session_id,
                    user_id=user_id,
                    trace_id=trace_id,
                    message_id=str(uuid.uuid4()),
                    message_type="human",
                    content=msg.get('content', ''),
                    trace_end=datetime.now()
                )
                db.session.add(entry)
                print(f"[analytics_service] Added human message to chat history")
                
            elif msg['type'] == 'ai':
                # Add AI message
                agent_id = None
                if "name" in msg:
                    # Use db.session.query instead of Model.query
                    agent = db.session.query(AgentDefinition).filter(
                        AgentDefinition.name == msg["name"]
                    ).first()
                    if agent:
                        agent_id = agent.agent_id
                
                # Check if this is a tool call or regular AI message
                if msg.get("response_metadata", {}).get("finish_reason") == "tool_calls":
                    # This is a tool call message
                    tool_calls = msg.get("additional_kwargs", {}).get('tool_calls', [])
                    if tool_calls:
                        tool_call = tool_calls[0]  # Process first tool call
                        raw_tool_name = tool_call.get('function', {}).get("name")
                        
                        # Map wrapper tool names to canonical names
                        TOOL_NAME_MAP = {
                            "get_user_accounts_for_current_user": "get_user_accounts",
                            "get_transactions_summary_for_current_user": "get_transactions_summary",
                            "create_new_account_for_current_user": "create_new_account",
                            "transfer_money_for_current_user": "transfer_money",
                        }
                        canonical_tool_name = TOOL_NAME_MAP.get(raw_tool_name, raw_tool_name)
                        
                        # Get tool_id using db.session.query
                        tool = db.session.query(ToolDefinition).filter(
                            ToolDefinition.name == canonical_tool_name
                        ).first()
                        tool_id = tool.tool_id if tool else None
                        
                        entry = ChatHistory(
                            session_id=session_id,
                            user_id=user_id,
                            agent_id=agent_id,
                            trace_id=trace_id,
                            message_id=msg.get("id", str(uuid.uuid4())),
                            message_type='tool_call',
                            tool_id=tool_id,
                            tool_call_id=tool_call.get('id'),
                            tool_name=canonical_tool_name,
                            total_tokens=msg.get("response_metadata", {}).get("token_usage", {}).get('total_tokens'),
                            completion_tokens=msg.get("response_metadata", {}).get("token_usage", {}).get('completion_tokens'),
                            prompt_tokens=msg.get("response_metadata", {}).get("token_usage", {}).get('prompt_tokens'),
                            estimated_cost_usd=estimate_cost(
                                msg.get("response_metadata", {}).get('model_name'),
                                msg.get("response_metadata", {}).get("token_usage", {}).get('prompt_tokens'),
                                msg.get("response_metadata", {}).get("token_usage", {}).get('completion_tokens'),
                            ),
                            tool_input=tool_call.get('function', {}).get("arguments"),
                            model_name=msg.get("response_metadata", {}).get('model_name'),
                            finish_reason=msg.get("response_metadata", {}).get("finish_reason"),
                            trace_end=datetime.now()
                        )
                        db.session.add(entry)
                        
                        # Store tool info for later ToolUsage creation
                        tool_info_dict[tool_call.get('id')] = {
                            'tool_id': tool_id,
                            'tool_name': canonical_tool_name,
                            'tool_input': tool_call.get('function', {}).get("arguments"),
                            'total_tokens': msg.get("response_metadata", {}).get("token_usage", {}).get('total_tokens')
                        }
                        print(f"[analytics_service] Added tool call message")
                else:
                    # Regular AI message
                    entry = ChatHistory(
                        session_id=session_id,
                        user_id=user_id,
                        agent_id=agent_id,
                        message_id=msg.get("id", str(uuid.uuid4())),
                        trace_id=trace_id,
                        message_type="ai",
                        content=msg.get('content', ''),
                        total_tokens=msg.get("response_metadata", {}).get("token_usage", {}).get('total_tokens'),
                        completion_tokens=msg.get("response_metadata", {}).get("token_usage", {}).get('completion_tokens'),
                        prompt_tokens=msg.get("response_metadata", {}).get("token_usage", {}).get('prompt_tokens'),
                        estimated_cost_usd=estimate_cost(
                            msg.get("response_metadata", {}).get('model_name'),
                            msg.get("response_metadata", {}).get("token_usage", {}).get('prompt_tokens'),
                            msg.get("response_metadata", {}).get("token_usage", {}).get('completion_tokens'),
                        ),
                        model_name=msg.get("response_metadata", {}).get('model_name'),
                        finish_reason=msg.get("response_metadata", {}).get("finish_reason"),
                        response_time_ms=trace_duration_ms,
                        trace_end=datetime.now()
                    )
                    db.session.add(entry)
                    print(f"[analytics_service] Added AI message to chat history")
                    
            elif msg['type'] == 'tool':
                # Tool result message
                tool_call_id = msg.get("tool_call_id")
                tool_name = msg.get("name")
                
                # Get tool_id using db.session.query
                tool = db.session.query(ToolDefinition).filter(
                    ToolDefinition.name == tool_name
                ).first()
                tool_id = tool.tool_id if tool else None
                
                entry = ChatHistory(
                    session_id=session_id,
                    user_id=user_id,
                    message_id=msg.get("id", str(uuid.uuid4())),
                    tool_id=tool_id,
                    tool_call_id=tool_call_id,
                    trace_id=trace_id,
                    tool_name=tool_name,
                    message_type='tool_result',
                    content="",
                    tool_output=msg.get("content"),
                    trace_end=datetime.now()
                )
                db.session.add(entry)
                
                # Update tool_info for ToolUsage
                if tool_call_id in tool_info_dict:
                    tool_info_dict[tool_call_id]['tool_output'] = msg.get("content")
                    tool_info_dict[tool_call_id]['status'] = msg.get("status", "success")
                
                print(f"[analytics_service] Added tool result message")
                
                # Create ToolUsage entry
                if tool_call_id in tool_info_dict:
                    info = tool_info_dict[tool_call_id]
                    if info.get('tool_id'):  # Only create if we have a valid tool_id
                        tool_output = info.get('tool_output', {})
                        tool_msg = ''
                        if isinstance(tool_output, dict):
                            tool_msg = tool_output.get('message', '')
                        else:
                            tool_msg = str(tool_output)
                        
                        tool_status = "Errored" if "error" in str(tool_output).lower() else "Healthy"
                        
                        usage_entry = ToolUsage(
                            tool_call_id=tool_call_id,
                            session_id=session_id,
                            trace_id=trace_id,
                            tool_id=info.get('tool_id'),
                            tool_name=info.get('tool_name'),
                            tool_input=info.get('tool_input'),
                            tool_output=tool_output,
                            tool_message=tool_msg,
                            status=tool_status,
                            tokens_used=info.get('total_tokens')
                        )
                        db.session.add(usage_entry)
                        print(f"[analytics_service] Added tool usage entry")
        
        # Update session timestamp
        session.updated_at = datetime.now()
        
        # Commit all changes
        db.session.commit()
        print(f"[analytics_service] All trace messages committed successfully")
        
        elapsed = time.time() - start
        print(
            f"[analytics_service] Logged chat trace for session={session_id}, "
            f"user_id={user_id} in {elapsed:.2f}s "
            f"(trace_duration_ms={trace_duration_ms})"
        )
        
    except Exception as e:
        print(f"[analytics_service] Error logging chat trace: {e}")
        import traceback
        traceback.print_exc()
        db.session.rollback()
        raise

# Add to analytics_service.py

from flask import Flask, jsonify, request
from chat_data_model import ChatHistoryManager, AgentTrace

def log_multi_agent_trace(data: dict):
    """
    Log a complete multi-agent interaction with step-by-step traces
    
    Expected payload:
    {
        "session_id": "session_abc123",
        "user_id": "user_5",
        "messages": [...],  # Serialized messages
        "agent_used": "account_agent",
        "task_type": "account_management",
        "trace_duration": 1234,
        "step_traces": [
            {
                "step_name": "coordinator_routing",
                "agent_name": "coordinator_agent",
                "reasoning": "Matched 3 keywords",
                "timestamp": "2025-01-01T12:00:00",
                "duration_ms": 50,
                "tokens_used": {"total": 0, "prompt": 0, "completion": 0},
                "matched_keywords": ["account", "balance"],
                "selected_agent": "account_agent"
            },
            {
                "step_name": "account_agent_execution",
                "agent_name": "account_agent",
                "reasoning": "Processed account tasks",
                "timestamp": "2025-01-01T12:00:01",
                "duration_ms": 1200,
                "tokens_used": {"total": 150, "prompt": 80, "completion": 70}
            }
        ],
        "total_tokens": 150,
        "coordinator_tokens": 0,
        "coordinator_reasoning": "Matched 3 account keywords"
    }
    """
    try:
        data = request.json
        session_id = data.get('session_id')
        user_id = data.get('user_id')
        messages = data.get('messages', [])
        step_traces = data.get('step_traces', [])
        
        if not session_id or not user_id:
            return jsonify({"error": "session_id and user_id are required"}), 400
        
        # Initialize chat history manager
        chat_manager = ChatHistoryManager(session_id=session_id, user_id=user_id)
        
        # Generate trace_id if not provided
        import uuid
        trace_id = data.get('trace_id') or str(uuid.uuid4())
        
        # Log all messages (existing functionality)
        for msg in messages:
            msg_type = msg.get('type', 'unknown')
            content = msg.get('content', '')
            
            if msg_type == 'human':
                chat_manager.add_user_message(content, trace_id=trace_id)
            elif msg_type == 'ai':
                agent_name = data.get('agent_used', 'unknown_agent')
                chat_manager.add_ai_message(
                    content=content,
                    trace_id=trace_id,
                    agent_name=agent_name,
                    agent_type=data.get('task_type', 'unknown'),
                    model_name=os.getenv('AZURE_OPENAI_DEPLOYMENT', 'gpt-4'),
                    total_tokens=data.get('total_tokens', 0)
                )
            elif msg_type == 'tool':
                tool_name = msg.get('name', 'unknown_tool')
                tool_input = msg.get('args', {})
                chat_manager.add_tool_call_message(
                    tool_name=tool_name,
                    tool_input=tool_input,
                    trace_id=trace_id,
                    agent_name=data.get('agent_used')
                )
        
        # Log step traces (NEW)
        if step_traces:
            chat_manager.log_step_traces(trace_id=trace_id, step_traces=step_traces)
        
        return jsonify({
            "status": "success",
            "session_id": session_id,
            "trace_id": trace_id,
            "steps_logged": len(step_traces),
            "messages_logged": len(messages)
        }), 201
        
    except Exception as e:
        print(f"Error logging multi-agent trace: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500



def get_session_traces(session_id):
    from banking_app import db
    """
    Retrieve all step traces for a specific session
    
    Optional query params:
    - trace_id: Filter by specific trace
    - user_id: Verify user ownership
    """
    try:
        user_id = request.args.get('user_id')
        trace_id = request.args.get('trace_id')
        
        # Query traces
        query = AgentTrace.query.filter_by(session_id=session_id)
        
        if user_id:
            query = query.filter_by(user_id=user_id)
        
        if trace_id:
            query = query.filter_by(trace_id=trace_id)
        
        traces = query.order_by(
            AgentTrace.execution_start,
            AgentTrace.step_order
        ).all()
        
        # Group by trace_id
        traces_by_id = {}
        for trace in traces:
            tid = trace.trace_id
            if tid not in traces_by_id:
                traces_by_id[tid] = []
            traces_by_id[tid].append(trace.to_dict())
        
        return jsonify({
            "session_id": session_id,
            "total_traces": len(traces_by_id),
            "traces": traces_by_id
        })
        
    except Exception as e:
        print(f"Error retrieving traces: {e}")
        return jsonify({"error": str(e)}), 500



def get_traces_summary():
    from banking_app import db
    """
    Get aggregated trace analytics
    
    Query params:
    - user_id: Filter by user
    - start_date: ISO format
    - end_date: ISO format
    - agent_name: Filter by specific agent
    """
    try:
        from sqlalchemy import func
        
        user_id = request.args.get('user_id')
        agent_name = request.args.get('agent_name')
        
        # Build query
        query = db.session.query(
            AgentTrace.target_agent,
            AgentTrace.task_type,
            func.count(AgentTrace.trace_step_id).label('total_steps'),
            func.avg(AgentTrace.execution_duration_ms).label('avg_duration_ms'),
            func.sum(AgentTrace.tokens_used).label('total_tokens'),
            func.count(func.distinct(AgentTrace.session_id)).label('unique_sessions')
        )
        
        if user_id:
            query = query.filter(AgentTrace.user_id == user_id)
        
        if agent_name:
            query = query.filter(AgentTrace.target_agent == agent_name)
        
        results = query.group_by(
            AgentTrace.target_agent,
            AgentTrace.task_type
        ).all()
        
        summary = []
        for row in results:
            summary.append({
                "agent": row.target_agent,
                "task_type": row.task_type,
                "total_steps": row.total_steps,
                "avg_duration_ms": round(row.avg_duration_ms, 2) if row.avg_duration_ms else 0,
                "total_tokens": row.total_tokens or 0,
                "unique_sessions": row.unique_sessions
            })
        
        return jsonify({
            "summary": summary,
            "total_agents": len(summary)
        })
        
    except Exception as e:
        print(f"Error generating trace summary: {e}")
        return jsonify({"error": str(e)}), 500
