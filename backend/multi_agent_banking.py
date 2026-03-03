from typing import Annotated, TypedDict, List
from langchain_core.messages import  BaseMessage
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
import time

# Import existing banking infrastructure
from agents import (
    create_account_management_agent,
    create_support_agent,
    create_visualization_agent,
    create_coordinator_agent,
    create_fabric_data_agent,
)

# Multi-Agent State
class BankingAgentState(TypedDict):
    messages: Annotated[List[BaseMessage], "The messages in the conversation"]
    current_agent: str
    pass_to: str
    user_id: str
    session_id: str
    final_result: str
    time_taken: float
    widget_instructions: str

# Multi-Agent Node Functions

def coordinator_node(state: BankingAgentState):
    """Route customer requests to appropriate specialist agent."""

    coordinator_agent = create_coordinator_agent()  # Pass user_id
    
    thread_config = {"configurable": {"thread_id": f"account_{state['session_id']}"}}
    
    start_time = time.time()
    response = coordinator_agent.invoke({"messages": state["messages"]}, config=thread_config)
    finish_time = time.time()
    time_taken = finish_time - start_time

    state["current_agent"] = "coordinator"
    state["messages"] = response["messages"]
    state["final_result"] = response["messages"][-1].content
    state["time_taken"] = time_taken

    
    # Use keyword-based routing for speed and reliability
    message_lower = state["final_result"].lower()

    if message_lower == "visualization_agent":
        state["pass_to"] = "visualization_agent"
        state["task_type"] = "visualization_management"
        print(f"[COORDINATOR] Routing to: visualization_agent")
    elif message_lower == "account_agent":
        state["pass_to"] = "account_agent"
        state["task_type"] = "account_management"
        print(f"[COORDINATOR] Routing to: account_agent")
    elif message_lower == "fabric_agent":
        state["pass_to"] = "fabric_agent"
        state["task_type"] = "fabric_data_query"
        print(f"[COORDINATOR] Routing to: fabric_agent")
    else:
        state["pass_to"] = "support_agent"
        state["task_type"] = "customer_support"
        print(f"[COORDINATOR] Routing to: support_agent")
    
    return state


def account_agent_node(state: BankingAgentState):
    """Handle account management tasks."""
    user_id = state["user_id"]  # Get user_id from state
    account_agent = create_account_management_agent(user_id)  # Pass user_id
    
    thread_config = {"configurable": {"thread_id": f"account_{state['session_id']}"}}
    
    start_time = time.time()
    response = account_agent.invoke({"messages": state["messages"]}, config=thread_config)
    finish_time = time.time()
    time_taken = finish_time - start_time
    
    
    state["current_agent"] = "account_agent"
    state["pass_to"] = None
    state["messages"] = response["messages"]
    state["final_result"] = response["messages"][-1].content
    state["time_taken"] = time_taken
    
    return state

def support_agent_node(state: BankingAgentState):
    """Handle customer support tasks."""
    support_agent = create_support_agent()
    
    thread_config = {"configurable": {"thread_id": f"support_{state['session_id']}"}}
    
    start_time = time.time()
    response = support_agent.invoke({"messages": state["messages"]}, config=thread_config)
    finish_time = time.time()
    time_taken = finish_time - start_time

    state["current_agent"] = "support_agent"
    state["pass_to"] = None
    state["messages"] = response["messages"]
    state["final_result"] = response["messages"][-1].content
    state["time_taken"] = time_taken
    
    return state

def visualization_agent_node(state: BankingAgentState):
    """Handle visualization/widget creation tasks."""
    user_id = state["user_id"]
    visualization_agent = create_visualization_agent(user_id, state["widget_instructions"])
    
    thread_config = {"configurable": {"thread_id": f"visualization_{state['session_id']}"}}
    start_time = time.time()
    response = visualization_agent.invoke({"messages": state["messages"]}, config=thread_config)
    finish_time = time.time()
    time_taken = finish_time - start_time
    
    state["current_agent"] = "visualization_agent"
    state["pass_to"] = None
    state["messages"] = response["messages"]
    state["final_result"] = response["messages"][-1].content
    state["time_taken"] = time_taken
    
    return state

def fabric_agent_node(state: BankingAgentState):
    """Handle read-only data queries via the Fabric Data Agent."""
    user_id = state["user_id"]
    fabric_agent = create_fabric_data_agent(user_id)

    thread_config = {"configurable": {"thread_id": f"fabric_{state['session_id']}"}}

    start_time = time.time()
    response = fabric_agent.invoke({"messages": state["messages"]}, config=thread_config)
    finish_time = time.time()
    time_taken = finish_time - start_time

    state["current_agent"] = "fabric_agent"
    state["pass_to"] = None
    state["messages"] = response["messages"]
    state["final_result"] = response["messages"][-1].content
    state["time_taken"] = time_taken

    return state


# Create Multi-Agent Banking System

def create_multi_agent_banking_system():
    """Create the multi-agent banking workflow."""

    workflow = StateGraph(BankingAgentState)
    
    # Add nodes
    workflow.add_node("coordinator", coordinator_node)
    workflow.add_node("account_agent", account_agent_node)
    workflow.add_node("support_agent", support_agent_node)
    workflow.add_node("visualization_agent", visualization_agent_node)
    workflow.add_node("fabric_agent", fabric_agent_node)

    # Set entry point
    workflow.set_entry_point("coordinator")
    
    # Add conditional routing
    def route_to_specialist(state: BankingAgentState):
        return state["pass_to"]
    
    workflow.add_conditional_edges(
        "coordinator",
        route_to_specialist,
        {
            "account_agent": "account_agent",
            "support_agent": "support_agent",
            "visualization_agent": "visualization_agent",
            "fabric_agent": "fabric_agent",
        }
    )

    # All agents end the workflow
    workflow.add_edge("account_agent", END)
    workflow.add_edge("support_agent", END)
    workflow.add_edge("visualization_agent", END)
    workflow.add_edge("fabric_agent", END)
    
    return workflow.compile(checkpointer=MemorySaver())


def execute_trace(banking_system, initial_state, thread_config):
    events = []
    final_result = None
    # i = 0
    for event in banking_system.stream(initial_state, config=thread_config, stream_mode = "updates"):
        node_name = list(event.keys())[0]
        events.append(event)

    final_result = event[node_name].get("final_result")
    return events, final_result