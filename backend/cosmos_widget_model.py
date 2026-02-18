"""
AI Widget Data Model - Cosmos DB implementation
Stores user-specific AI-generated widgets in the gen_ui_config container.
Supports static, dynamic (query-based), and simulation (interactive) data modes.
"""
import copy
import uuid
from datetime import datetime
from azure.cosmos import exceptions
from cosmos_db import get_widget_container


def get_user_widgets(user_id: str):
    """Get all widgets for a specific user"""
    container = get_widget_container()
    query = "SELECT * FROM c WHERE c.user_id = @user_id ORDER BY c.updated_at DESC"
    items = list(
        container.query_items(
            query=query,
            parameters=[{"name": "@user_id", "value": user_id}],
            enable_cross_partition_query=False,
            partition_key=user_id,
        )
    )
    # Strip Cosmos system properties for clean API responses
    return [_clean_doc(item) for item in items]


def create_widget(
    user_id: str,
    title: str,
    description: str,
    widget_type: str,
    config: dict,
    code: str = None,
    data_mode: str = "static",
    query_config: dict = None,
    simulation_config: dict = None,
):
    """Create a new AI widget for a user"""
    now = datetime.utcnow().isoformat()
    doc = {
        "id": f"widget_{uuid.uuid4()}",
        "user_id": user_id,
        "title": title,
        "description": description,
        "widget_type": widget_type,
        "config": config,
        "code": code,
        "data_mode": data_mode,
        "query_config": query_config,
        "simulation_config": simulation_config,
        "last_refreshed": now if data_mode == "dynamic" else None,
        "created_at": now,
        "updated_at": now,
    }
    container = get_widget_container()
    created = container.create_item(body=doc)
    return _clean_doc(created)


def update_widget(widget_id: str, user_id: str, updates: dict):
    """Update an existing widget (only if owned by the user)"""
    container = get_widget_container()
    try:
        doc = container.read_item(item=widget_id, partition_key=user_id)
    except exceptions.CosmosResourceNotFoundError:
        return None

    allowed_fields = [
        "title", "description", "widget_type", "config", "code",
        "data_mode", "query_config", "simulation_config", "last_refreshed",
    ]
    for field in allowed_fields:
        if field in updates:
            doc[field] = updates[field]

    doc["updated_at"] = datetime.utcnow().isoformat()
    replaced = container.replace_item(item=widget_id, body=doc)
    return _clean_doc(replaced)


def update_widget_data(widget_id: str, user_id: str, new_data: list):
    """Update just the data portion of a widget's config (for refresh)"""
    container = get_widget_container()
    try:
        doc = container.read_item(item=widget_id, partition_key=user_id)
    except exceptions.CosmosResourceNotFoundError:
        return None

    config = copy.deepcopy(doc.get("config", {}))
    if "customProps" not in config:
        config["customProps"] = {}
    config["customProps"]["data"] = new_data

    doc["config"] = config
    doc["last_refreshed"] = datetime.utcnow().isoformat()
    doc["updated_at"] = datetime.utcnow().isoformat()

    replaced = container.replace_item(item=widget_id, body=doc)
    return _clean_doc(replaced)


def update_simulation_defaults(widget_id: str, user_id: str, new_defaults: dict):
    """Update simulation widget defaults"""
    container = get_widget_container()
    try:
        doc = container.read_item(item=widget_id, partition_key=user_id)
    except exceptions.CosmosResourceNotFoundError:
        return None

    if doc.get("widget_type") != "simulation":
        return None

    sim_config = copy.deepcopy(doc.get("simulation_config", {}))
    if "defaults" not in sim_config:
        sim_config["defaults"] = {}
    sim_config["defaults"].update(new_defaults)

    doc["simulation_config"] = sim_config
    doc["updated_at"] = datetime.utcnow().isoformat()

    replaced = container.replace_item(item=widget_id, body=doc)
    return _clean_doc(replaced)


def delete_widget(widget_id: str, user_id: str):
    """Delete a widget (only if owned by the user)"""
    container = get_widget_container()
    try:
        container.delete_item(item=widget_id, partition_key=user_id)
        return True
    except exceptions.CosmosResourceNotFoundError:
        return False


def get_widget_by_id(widget_id: str, user_id: str):
    """Get a specific widget by ID (only if owned by the user)"""
    container = get_widget_container()
    try:
        doc = container.read_item(item=widget_id, partition_key=user_id)
        return _clean_doc(doc)
    except exceptions.CosmosResourceNotFoundError:
        return None


def _clean_doc(doc: dict) -> dict:
    """Remove Cosmos DB system properties from a document"""
    return {k: v for k, v in doc.items() if not k.startswith("_")}
