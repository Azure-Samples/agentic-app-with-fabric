import time
import uuid
from datetime import datetime
from typing import List, Dict, Any

from shared.utils import _serialize_messages, _to_json_primitive
from langchain_core.messages import BaseMessage


def get_chat_history_for_session(session_id: str, user_id: str) -> List[Dict[str, Any]]:
    """
    In-process equivalent of GET /analytics/api/chat/history/<session_id>.
    Returns a list of serialized messages for the given session.
    
    When called from banking_app, this bypasses ChatHistoryManager to avoid context issues.
    """
    # Since we're being called from banking_app, use its database directly
    from banking_app import db
    
    # Import the model classes (not instances) to access table structure
    from chat_data_model import ChatHistory
    
    try:
        # Use db.session directly to avoid the .query property which causes context issues
        from sqlalchemy import desc
        
        # Build query using session.query instead of Model.query
        messages = db.session.query(
            ChatHistory.trace_id, 
            ChatHistory.message_type, 
            ChatHistory.content, 
            ChatHistory.trace_end
        ).filter(
            ChatHistory.session_id == session_id
        ).order_by(
            desc(ChatHistory.trace_end)
        ).limit(50).all()
        
        # Format and return
        result = [
            {
                "trace_id": msg[0], 
                "message_type": msg[1], 
                "content": msg[2], 
                "trace_end": msg[3].isoformat() if msg[3] else None
            } 
            for msg in reversed(messages) if msg  # reversed to get chronological order
        ]
        
        return result
        
    except Exception as e:
        print(f"[analytics_service] Error fetching chat history: {e}")
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
    from chat_data_model import ChatSession, ChatHistory, ToolUsage, ToolDefinition, AgentDefinition
    
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
                            content_filter_results=msg.get("response_metadata", {}).get("prompt_filter_results", [{}])[0].get("content_filter_results"),
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
                        content_filter_results=msg.get("response_metadata", {}).get("prompt_filter_results", [{}])[0].get("content_filter_results"),
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