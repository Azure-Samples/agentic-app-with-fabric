import uuid
from datetime import datetime
import json
import pyodbc
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field, ConfigDict, field_validator
from flask import jsonify
from shared.utils import _to_json_primitive
from shared.utils import get_user_id
from shared.connection_manager import connection_manager

# Global database connection
_db_connection = None

def get_db_connection():
    """Get database connection using the shared connection manager"""
    global _db_connection
    if _db_connection is None:
        _db_connection = connection_manager.get_connection()
    return _db_connection

# Pydantic Models
class AgentDefinition(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    agent_id: str = Field(default_factory=lambda: f"agent_{uuid.uuid4()}")
    name: str
    description: Optional[str] = None
    llm_config: Dict[str, Any]
    prompt_template: str
    
    @field_validator('agent_id', mode='before')
    @classmethod
    def validate_agent_id(cls, v):
        if v is None or v == '':
            return f"agent_{uuid.uuid4()}"
        return v

class ChatSession(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    session_id: str = Field(default_factory=lambda: f"session_{uuid.uuid4()}")
    user_id: str
    title: Optional[str] = None
    created_at: Optional[datetime] = Field(default_factory=datetime.now)
    updated_at: Optional[datetime] = Field(default_factory=datetime.now)
    
    @field_validator('session_id', mode='before')
    @classmethod
    def validate_session_id(cls, v):
        if v is None or v == '':
            return f"session_{uuid.uuid4()}"
        return v

class ToolDefinition(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    tool_id: str = Field(default_factory=lambda: f"tooldef_{uuid.uuid4()}")
    name: str
    description: Optional[str] = None
    input_schema: Dict[str, Any]
    version: str = "1.0.0"
    is_active: bool = True
    cost_per_call_cents: int = 0
    created_at: Optional[datetime] = Field(default_factory=datetime.now)
    updated_at: Optional[datetime] = Field(default_factory=datetime.now)
    
    @field_validator('tool_id', mode='before')
    @classmethod
    def validate_tool_id(cls, v):
        if v is None or v == '':
            return f"tooldef_{uuid.uuid4()}"
        return v

class ToolUsage(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    tool_call_id: str = Field(default_factory=lambda: f"tool_{uuid.uuid4()}")
    session_id: str
    trace_id: Optional[str] = None
    tool_id: str
    tool_name: str
    tool_input: Dict[str, Any]
    tool_output: Optional[Dict[str, Any]] = None
    tool_message: Optional[str] = None
    status: Optional[str] = None
    tokens_used: Optional[int] = None

class ChatHistory(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    message_id: str = Field(default_factory=lambda: f"msg_{uuid.uuid4()}")
    session_id: str
    trace_id: str
    user_id: str
    agent_id: Optional[str] = None
    message_type: str  # 'human', 'ai', 'system', 'tool_call', 'tool_result'
    content: Optional[str] = None
    model_name: Optional[str] = None
    content_filter_results: Optional[Dict[str, Any]] = None
    total_tokens: Optional[int] = None
    completion_tokens: Optional[int] = None
    prompt_tokens: Optional[int] = None
    tool_id: Optional[str] = None
    tool_name: Optional[str] = None
    tool_input: Optional[Dict[str, Any]] = None
    tool_output: Optional[Dict[str, Any]] = None
    tool_call_id: Optional[str] = None
    finish_reason: Optional[str] = None
    response_time_ms: Optional[int] = None
    trace_end: Optional[datetime] = Field(default_factory=datetime.now)

# --- Chat History Management Class ---
class ChatHistoryManager:
    def __init__(self, session_id: str, user_id: str = 'user_1'):
        self.session_id = session_id
        self.user_id = user_id
        self._ensure_session_exists()

    def _ensure_session_exists(self):
        """Ensure the chat session exists in the database"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if session exists
        cursor.execute("SELECT session_id FROM chat_sessions WHERE session_id = ?", (self.session_id,))
        if not cursor.fetchone():
            # Create new session
            session = ChatSession(
                session_id=self.session_id,
                title="New Session",
                user_id=self.user_id,
            )
            cursor.execute("""
                INSERT INTO chat_sessions (session_id, user_id, title, created_at, updated_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (session.session_id, session.user_id, session.title, 
                  session.created_at, session.updated_at))
            conn.commit()
            print("-----------------> New chat session created: ", session.session_id)
            
    def add_trace_messages(self, serialized_messages: str, trace_duration: int):
        """Add all messages in a trace to the chat history"""
        trace_id = str(uuid.uuid4())
        message_list = _to_json_primitive(serialized_messages)
        print("New trace_id generated. Adding all messages for trace_id:", trace_id)
        
        for msg in message_list:
            if msg['type'] == 'human':
                print("Adding human message to chat history")
                _ = self.add_human_message(msg, trace_id)
            if msg['type'] == 'ai':
                print("Adding AI message to chat history")
                if msg.get("response_metadata", {}).get("finish_reason") != "tool_calls":
                    _ = self.add_ai_message(msg, trace_id, trace_duration)
                elif msg.get("response_metadata", {}).get("finish_reason") == "tool_calls":
                    tool_call_dict = self.add_tool_call_message(msg, trace_id)
            if msg['type'] == "tool":
                print("Adding tool message to chat history")
                tool_result_dict = self.add_tool_result_message(msg, trace_id)
                tool_call_dict.update(tool_result_dict)
                _ = self.log_tool_usage(tool_call_dict, trace_id)
                
        res = "All trace messages added..."
        self.update_session_timestamp()
        return res

    def add_human_message(self, message: dict, trace_id: str):
        """Add the human message to chat history"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        entry_message = ChatHistory(
            session_id=self.session_id,
            user_id=self.user_id,
            trace_id=trace_id,
            message_id=str(uuid.uuid4()),
            message_type="human",
            content=message['content'],
        )
        
        cursor.execute("""
            INSERT INTO chat_history (message_id, session_id, trace_id, user_id, message_type, content, trace_end)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (entry_message.message_id, entry_message.session_id, entry_message.trace_id, 
              entry_message.user_id, entry_message.message_type, entry_message.content, entry_message.trace_end))
        conn.commit()
        print("Human message added to chat history:", entry_message.message_id)
        return entry_message

    def add_ai_message(self, message: dict, trace_id: str, trace_duration: int):
        """Add the AI agent message to chat history"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get agent_id by name if provided
        agent_id = None
        if "name" in message:
            cursor.execute("SELECT agent_id FROM agent_definitions WHERE name = ?", (message["name"],))
            result = cursor.fetchone()
            if result:
                agent_id = result[0]
        
        entry_message = ChatHistory(
            session_id=self.session_id,
            user_id=self.user_id,
            agent_id=agent_id,
            message_id=message.get("id", f"msg_{uuid.uuid4()}"),
            trace_id=trace_id,
            message_type="ai",
            content=message["content"],
            total_tokens=message.get("response_metadata", {}).get("token_usage", {}).get('total_tokens'),
            completion_tokens=message.get("response_metadata", {}).get("token_usage", {}).get('completion_tokens'),
            prompt_tokens=message.get("response_metadata", {}).get("token_usage", {}).get('prompt_tokens'),
            model_name=message.get("response_metadata", {}).get('model_name'),
            content_filter_results=message.get("response_metadata", {}).get("prompt_filter_results", [{}])[0].get("content_filter_results"),
            finish_reason=message.get("response_metadata", {}).get("finish_reason"),
            response_time_ms=trace_duration,
        )
        
        cursor.execute("""
            INSERT INTO chat_history (message_id, session_id, trace_id, user_id, agent_id, message_type, content, 
                                    total_tokens, completion_tokens, prompt_tokens, model_name, 
                                    content_filter_results, finish_reason, response_time_ms, trace_end)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (entry_message.message_id, entry_message.session_id, entry_message.trace_id, entry_message.user_id,
              entry_message.agent_id, entry_message.message_type, entry_message.content, entry_message.total_tokens,
              entry_message.completion_tokens, entry_message.prompt_tokens, entry_message.model_name,
              json.dumps(entry_message.content_filter_results), entry_message.finish_reason, 
              entry_message.response_time_ms, entry_message.trace_end))
        conn.commit()
        print("AI message added to chat history:", entry_message.message_id)
        return entry_message

    def add_tool_call_message(self, message: dict, trace_id: str):
        """Log a tool call"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get agent_id by name if provided
        agent_id = None
        if "name" in message:
            cursor.execute("SELECT agent_id FROM agent_definitions WHERE name = ?", (message["name"],))
            result = cursor.fetchone()
            if result:
                agent_id = result[0]
        
        tool_name = message.get("additional_kwargs", {}).get('tool_calls', [{}])[0].get('function', {}).get("name")
        
        # Get tool_id by name
        tool_id = None
        if tool_name:
            cursor.execute("SELECT tool_id FROM tool_definitions WHERE name = ?", (tool_name,))
            result = cursor.fetchone()
            if result:
                tool_id = result[0]

        entry_message = ChatHistory(
            session_id=self.session_id,
            user_id=self.user_id,
            agent_id=agent_id,
            trace_id=trace_id,
            message_type='tool_call',
            tool_id=tool_id,
            tool_call_id=message.get("additional_kwargs", {}).get('tool_calls', [{}])[0].get('id'),
            tool_name=tool_name,
            total_tokens=message.get("response_metadata", {}).get("token_usage", {}).get('total_tokens'),
            completion_tokens=message.get("response_metadata", {}).get("token_usage", {}).get('completion_tokens'),
            prompt_tokens=message.get("response_metadata", {}).get("token_usage", {}).get('prompt_tokens'),
            tool_input=message.get("additional_kwargs", {}).get('tool_calls', [{}])[0].get('function', {}).get("arguments"),
            model_name=message.get("response_metadata", {}).get('model_name'),
            content_filter_results=message.get("response_metadata", {}).get("prompt_filter_results", [{}])[0].get("content_filter_results"),
            finish_reason=message.get("response_metadata", {}).get("finish_reason"),
        )
        
        cursor.execute("""
            INSERT INTO chat_history (message_id, session_id, trace_id, user_id, agent_id, message_type, 
                                    tool_id, tool_call_id, tool_name, total_tokens, completion_tokens, 
                                    prompt_tokens, tool_input, model_name, content_filter_results, 
                                    finish_reason, trace_end)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (entry_message.message_id, entry_message.session_id, entry_message.trace_id, entry_message.user_id,
              entry_message.agent_id, entry_message.message_type, entry_message.tool_id, entry_message.tool_call_id,
              entry_message.tool_name, entry_message.total_tokens, entry_message.completion_tokens, entry_message.prompt_tokens,
              json.dumps(entry_message.tool_input), entry_message.model_name, json.dumps(entry_message.content_filter_results),
              entry_message.finish_reason, entry_message.trace_end))
        conn.commit()
        print("Tool call message added to chat history:", entry_message.message_id)
        
        return {
            "tool_call_id": message.get("additional_kwargs", {}).get('tool_calls', [{}])[0].get('id'),
            "tool_id": tool_id, 
            "tool_name": tool_name,
            "tool_input": message.get("additional_kwargs", {}).get('tool_calls', [{}])[0].get('function', {}).get("arguments"),
            "total_tokens": message.get("response_metadata", {}).get("token_usage", {}).get('total_tokens')
        }

    def add_tool_result_message(self, message: dict, trace_id: str):
        """Log a tool result"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        tool_name = message["name"]
        
        # Get tool_id by name
        tool_id = None
        cursor.execute("SELECT tool_id FROM tool_definitions WHERE name = ?", (tool_name,))
        result = cursor.fetchone()
        if result:
            tool_id = result[0]
            
        entry_message = ChatHistory(
            message_id=message.get("id", f"msg_{uuid.uuid4()}"),
            session_id=self.session_id,
            user_id=self.user_id,
            tool_id=tool_id,
            tool_call_id=message["tool_call_id"],
            trace_id=trace_id,
            tool_name=message["name"],
            message_type='tool_result',
            content="",
            tool_output=message["content"],
        )
        
        cursor.execute("""
            INSERT INTO chat_history (message_id, session_id, trace_id, user_id, message_type, 
                                    tool_id, tool_call_id, tool_name, content, tool_output, trace_end)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (entry_message.message_id, entry_message.session_id, entry_message.trace_id, entry_message.user_id,
              entry_message.message_type, entry_message.tool_id, entry_message.tool_call_id, entry_message.tool_name,
              entry_message.content, json.dumps(entry_message.tool_output), entry_message.trace_end))
        conn.commit()
        print("Tool result message added to chat history:", entry_message.message_id)
        return {"tool_output": message["content"], "status": message["status"]}
        
    def update_session_timestamp(self):
        """Update the session's updated_at timestamp"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("UPDATE chat_sessions SET updated_at = ? WHERE session_id = ?", 
                      (datetime.now(), self.session_id))
        conn.commit()
        print("Session timestamp updated:", self.session_id)
     
    def log_tool_usage(self, tool_info: dict, trace_id: str):
        """Log detailed tool usage metrics"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if tool usage already exists
        cursor.execute("SELECT tool_call_id FROM tool_usage WHERE tool_call_id = ?", 
                      (tool_info.get("tool_call_id"),))
        existing = cursor.fetchone()
        
        tool_msg = ''
        if isinstance(tool_info.get("tool_output"), dict):
            tool_msg = tool_info.get("tool_output").get('message', '')
        else:
            tool_msg = str(tool_info.get("tool_output", ''))
            
        tool_call_status = "Errored" if "error" in tool_msg.lower() else "Healthy"

        if existing:
            # Update existing record
            cursor.execute("""
                UPDATE tool_usage 
                SET tool_output = ?, trace_id = ?, session_id = ?, tool_id = ?, 
                    tool_name = ?, tool_input = ?, tool_message = ?, status = ?, tokens_used = ?
                WHERE tool_call_id = ?
            """, (json.dumps(tool_info.get("tool_output")), trace_id, self.session_id,
                  tool_info.get("tool_id"), tool_info.get("tool_name"), 
                  json.dumps(tool_info.get("tool_input")), tool_msg, tool_call_status,
                  tool_info.get("total_tokens"), tool_info.get("tool_call_id")))
        else:
            # Insert new record
            tool_usage = ToolUsage(
                session_id=self.session_id,
                trace_id=trace_id,
                tool_call_id=tool_info.get("tool_call_id"),
                tool_id=tool_info.get("tool_id"),
                tool_name=tool_info.get("tool_name"),
                tool_input=tool_info.get("tool_input"),
                tool_output=tool_info.get("tool_output"),
                tool_message=tool_msg,
                status=tool_call_status,
                tokens_used=tool_info.get("total_tokens")
            )
            cursor.execute("""
                INSERT INTO tool_usage (tool_call_id, session_id, trace_id, tool_id, tool_name, 
                                      tool_input, tool_output, tool_message, status, tokens_used)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (tool_usage.tool_call_id, tool_usage.session_id, tool_usage.trace_id,
                  tool_usage.tool_id, tool_usage.tool_name, json.dumps(tool_usage.tool_input),
                  json.dumps(tool_usage.tool_output), tool_usage.tool_message, 
                  tool_usage.status, tool_usage.tokens_used))
        conn.commit()

    def get_conversation_history(self, limit: int = 50):
        """Retrieve conversation history for this session"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT trace_id, message_type, content, trace_end 
            FROM chat_history 
            WHERE session_id = ? 
            ORDER BY trace_end DESC 
            OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
        """, (self.session_id, limit))
        
        messages = cursor.fetchall()
        return [{"trace_id": msg[0], "message_type": msg[1], "content": msg[2], "trace_end": msg[3]} 
                for msg in reversed(messages)]


def handle_chat_sessions(request):
    """Handle chat sessions GET and POST requests"""
    user_id = get_user_id()  # In production, get from auth
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if request.method == 'GET':
        cursor.execute("""
            SELECT session_id, user_id, title, created_at, updated_at 
            FROM chat_sessions 
            WHERE user_id = ? 
            ORDER BY updated_at DESC
        """, (user_id,))
        sessions = cursor.fetchall()
        
        result = []
        for row in sessions:
            session = ChatSession(
                session_id=row[0],
                user_id=row[1],
                title=row[2],
                created_at=row[3],
                updated_at=row[4]
            )
            result.append(session.model_dump())
        return jsonify(result)
    
    if request.method == 'POST':
        data = request.json
        
        # Ensure we have a valid session_id
        session_id = data.get('session_id')
        if not session_id:
            session_id = f"session_{uuid.uuid4()}"
            
        session = ChatSession(
            session_id=session_id,
            user_id=user_id,
            title=data.get('title', 'New Chat Session'),
        )
        
        cursor.execute("""
            INSERT INTO chat_sessions (session_id, user_id, title, created_at, updated_at) 
            VALUES (?, ?, ?, ?, ?)
        """, (session.session_id, session.user_id, session.title, 
              session.created_at, session.updated_at))
        conn.commit()
        return jsonify(session.model_dump()), 201


def clear_chat_history():
    """Clear all chat history data - USE WITH CAUTION"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Delete in order to respect foreign key constraints
        cursor.execute("DELETE FROM tool_usage")
        cursor.execute("DELETE FROM chat_history")
        cursor.execute("DELETE FROM chat_sessions")
        conn.commit()
        
        return jsonify({"message": "All chat history cleared successfully"}), 200
        
    except Exception as e:
        conn.rollback()
        return jsonify({"error": f"Failed to clear chat history: {str(e)}"}), 500

def clear_session_data(session_id):
    """Clear chat history for a specific session"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Delete in order to respect foreign key constraints
        cursor.execute("DELETE FROM tool_usage WHERE session_id = ?", (session_id,))
        cursor.execute("DELETE FROM chat_history WHERE session_id = ?", (session_id,))
        cursor.execute("DELETE FROM chat_sessions WHERE session_id = ?", (session_id,))
        conn.commit()
        
        return jsonify({"message": f"Session {session_id} data cleared successfully"}), 200
        
    except Exception as e:
        conn.rollback()
        return jsonify({"error": f"Failed to clear session data: {str(e)}"}), 500
def initialize_tool_definitions():
    """Initialize tool definitions in the database"""
    tools_data = [
        {
            "name": "get_user_accounts",
            "description": "Retrieves all accounts for a given user",
            "input_schema": {"type": "object", "properties": {}},
            "cost_per_call_cents": 0
        },
        {
            "name": "get_transactions_summary",
            "description": "Provides spending summary with time period and account filters",
            "input_schema": {
                "type": "object",
                "properties": {
                    "time_period": {"type": "string"},
                    "account_name": {"type": "string"}
                }
            },
            "cost_per_call_cents": 0
        },
        {
            "name": "search_support_documents",
            "description": "Searches knowledge base for customer support answers",
            "input_schema": {
                "type": "object",
                "properties": {"user_question": {"type": "string"}},
                "required": ["user_question"]
            },
            "cost_per_call_cents": 2
        },
        {
            "name": "create_new_account",
            "description": "Creates a new bank account for the user",
            "input_schema": {
                "type": "object",
                "properties": {
                    "account_type": {"type": "string", "enum": ["checking", "savings", "credit"]},
                    "name": {"type": "string"},
                    "balance": {"type": "number"}
                },
                "required": ["account_type", "name"]
            },
            "cost_per_call_cents": 0
        },
        {
            "name": "transfer_money",
            "description": "Transfers money between accounts or to external accounts",
            "input_schema": {
                "type": "object",
                "properties": {
                    "from_account_name": {"type": "string"},
                    "to_account_name": {"type": "string"},
                    "amount": {"type": "number"},
                    "to_external_details": {"type": "object"}
                },
                "required": ["from_account_name", "amount"]
            },
            "cost_per_call_cents": 0
        }
    ]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for tool_data in tools_data:
        # Check if tool already exists
        cursor.execute("SELECT tool_id FROM tool_definitions WHERE name = ?", (tool_data["name"],))
        if not cursor.fetchone():
            tool_def = ToolDefinition(**tool_data)
            cursor.execute("""
                INSERT INTO tool_definitions (tool_id, name, description, input_schema, version, 
                                            is_active, cost_per_call_cents, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (tool_def.tool_id, tool_def.name, tool_def.description, 
                  json.dumps(tool_def.input_schema), tool_def.version, tool_def.is_active,
                  tool_def.cost_per_call_cents, tool_def.created_at, tool_def.updated_at))
    
    conn.commit()

def initialize_agent_definitions():
    """Initialize agent definitions in the database"""
    agents_data = [
        {
            "name": "banking_agent_v1",
            "description": "A customer support banking agent to help answer questions about their account and other general banking inquiries.",
            "llm_config": {
                "model": "gpt-4.1",
                "rate_limit": 50,
                "token_limit": 1000
            },
            "prompt_template": "You are a banking assistant. Answer the user's questions about their bank accounts."
        }
    ]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for agent in agents_data:
        # Check if agent already exists
        cursor.execute("SELECT agent_id FROM agent_definitions WHERE name = ?", (agent["name"],))
        if not cursor.fetchone():
            agent_def = AgentDefinition(**agent)
            cursor.execute("""
                INSERT INTO agent_definitions (agent_id, name, description, llm_config, prompt_template)
                VALUES (?, ?, ?, ?, ?)
            """, (agent_def.agent_id, agent_def.name, agent_def.description, 
                  json.dumps(agent_def.llm_config), agent_def.prompt_template))

    conn.commit()


# Backwards compatibility - expose classes globally (optional)
def init_chat_db(database=None):
    """Initialize function for backwards compatibility"""
    # This function is now a no-op since we use direct SQL
    # But kept for compatibility with existing code
    print("Chat data model initialized with direct SQL access")
