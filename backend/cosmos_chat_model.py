"""
Conversation Context Model - Cosmos DB implementation
Stores conversational messages (human + AI) in the longterm_memory container
for session replay and context reconstruction.
"""
import uuid
from datetime import datetime
from azure.cosmos import exceptions
from cosmos_db import get_memory_container


def save_conversation_messages(session_id: str, user_id: str, messages: list,
                                title: str = None):
    """
    Upsert a session document with conversation messages.

    Each message in the list should be a dict with:
      - type: 'human' or 'ai'
      - content: str

    If the session document already exists, the new messages are appended.
    """
    container = get_memory_container()
    now = datetime.utcnow().isoformat()

    # Format incoming messages with timestamps
    timestamped = []
    for msg in messages:
        timestamped.append({
            "type": msg.get("type", "unknown"),
            "content": msg.get("content", ""),
            "timestamp": msg.get("timestamp", now),
        })

    try:
        doc = container.read_item(item=session_id, partition_key=user_id)
        # Append new messages
        existing_messages = doc.get("messages", [])
        existing_messages.extend(timestamped)
        doc["messages"] = existing_messages
        doc["updated_at"] = now
        if title:
            doc["title"] = title
        container.replace_item(item=session_id, body=doc)
    except exceptions.CosmosResourceNotFoundError:
        # Create new session document
        doc = {
            "id": session_id,
            "user_id": user_id,
            "title": title or "New Session",
            "messages": timestamped,
            "created_at": now,
            "updated_at": now,
        }
        container.create_item(body=doc)

    return True


def get_conversation_history(session_id: str, user_id: str, limit: int = 50):
    """
    Retrieve conversation messages for a session.
    Returns a list of dicts: [{type, content, timestamp}, ...]
    """
    container = get_memory_container()
    try:
        doc = container.read_item(item=session_id, partition_key=user_id)
        messages = doc.get("messages", [])
        # Return most recent messages up to the limit
        return messages[-limit:]
    except exceptions.CosmosResourceNotFoundError:
        return []


def get_user_sessions(user_id: str):
    """List all sessions for a user, ordered by most recently updated"""
    container = get_memory_container()
    query = (
        "SELECT c.id, c.user_id, c.title, c.created_at, c.updated_at "
        "FROM c WHERE c.user_id = @user_id ORDER BY c.updated_at DESC"
    )
    items = list(
        container.query_items(
            query=query,
            parameters=[{"name": "@user_id", "value": user_id}],
            enable_cross_partition_query=False,
            partition_key=user_id,
        )
    )
    return [_clean_doc(item) for item in items]


def delete_session(session_id: str, user_id: str):
    """Remove a session document from Cosmos DB"""
    container = get_memory_container()
    try:
        container.delete_item(item=session_id, partition_key=user_id)
        return True
    except exceptions.CosmosResourceNotFoundError:
        return False


def get_session_by_id(session_id: str, user_id: str):
    """Get a single session document"""
    container = get_memory_container()
    try:
        doc = container.read_item(item=session_id, partition_key=user_id)
        return _clean_doc(doc)
    except exceptions.CosmosResourceNotFoundError:
        return None


def _clean_doc(doc: dict) -> dict:
    """Remove Cosmos DB system properties from a document"""
    return {k: v for k, v in doc.items() if not k.startswith("_")}
