"""
Cosmos DB Client - Singleton client and container access for gen_ui_config and longterm_memory
"""
import os
from azure.cosmos import CosmosClient, PartitionKey, exceptions

_client = None
_database = None
_widget_container = None
_memory_container = None
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()

def _get_client():
    """Get or create the Cosmos DB client singleton"""
    global _client
    if _client is None:
        endpoint = os.getenv("COSMOS_DB_ENDPOINT")
        key = os.getenv("COSMOS_DB_KEY")
        if not endpoint or not credential:
            raise RuntimeError(
                "COSMOS_DB_ENDPOINT and credential must be set"
            )
        _client = CosmosClient(endpoint, credential=credential)
    return _client


def _get_database():
    """Get or create the Cosmos DB database"""
    global _database
    if _database is None:
        client = _get_client()
        db_name = os.getenv("COSMOS_DB_DATABASE_NAME", "agentic_app_db")
        _database = client.create_database_if_not_exists(id=db_name)
    return _database


def get_widget_container():
    """Get or create the gen_ui_config container (partition key: /user_id)"""
    global _widget_container
    if _widget_container is None:
        db = _get_database()
        _widget_container = db.create_container_if_not_exists(
            id="gen_ui_config",
            partition_key=PartitionKey(path="/user_id"),
        )
    return _widget_container


def get_memory_container():
    """Get or create the longterm_memory container (partition key: /user_id)"""
    global _memory_container
    if _memory_container is None:
        db = _get_database()
        _memory_container = db.create_container_if_not_exists(
            id="longterm_memory",
            partition_key=PartitionKey(path="/user_id"),
        )
    return _memory_container
