from typing import Annotated, TypedDict, List
from langchain_core.messages import BaseMessage, SystemMessage
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
import time
import json

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
    fabric_agent_error: bool       # True when fabric_agent failed and is handing off
    fabric_error_message: str      # Error details captured from fabric_agent failure

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
    """Handle account management tasks (also serves as fallback when fabric_agent errors)."""
    user_id = state["user_id"]
    account_agent = create_account_management_agent(user_id)

    messages = list(state["messages"])

    # When invoked as a fallback from fabric_agent, inject context so the agent
    # understands why it is being called and what it should do.
    if state.get("fabric_agent_error"):
        fabric_error = state.get("fabric_error_message", "unknown error")
        print(f"[ACCOUNT AGENT] Running as fallback after fabric_agent error: {fabric_error}")
        messages.append(
            SystemMessage(
                content=(
                    f"[SYSTEM HANDOFF] The Fabric Data Agent was unavailable "
                    f"(error: {fabric_error}). You are now handling this request as a "
                    f"fallback. Use your available tools to answer the user's question."
                )
            )
        )

    thread_config = {"configurable": {"thread_id": f"account_{state['session_id']}"}}

    start_time = time.time()
    response = account_agent.invoke({"messages": messages}, config=thread_config)
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

def _fabric_tool_error(messages) -> tuple[bool, str]:
    """Scan response messages for a fabric tool error.

    The fabric ReAct agent converts tool errors into a conversational AI reply,
    so checking the *final* AI message content for JSON never works.  The error
    payload `{"status": "error", "message": "..."}` lives inside a ToolMessage.
    This helper scans every ToolMessage and also checks ToolMessage.status for
    exceptions that LangChain captures at the framework level.

    Returns (error_occurred, error_message).
    """
    for msg in messages:
        cls_name = msg.__class__.__name__
        is_tool_msg = (getattr(msg, "type", None) == "tool") or cls_name == "ToolMessage"
        if not is_tool_msg:
            continue

        # 1. Structured JSON payload returned by query_fabric_data_agent on failure
        try:
            content = msg.content if isinstance(msg.content, str) else str(msg.content)
            payload = json.loads(content)
            if isinstance(payload, dict) and payload.get("status") == "error":
                err_msg = payload.get("message", "Fabric tool returned an error status")
                print(f"[FABRIC AGENT] ToolMessage carries error payload: {err_msg}")
                return True, err_msg
        except (json.JSONDecodeError, AttributeError, TypeError):
            pass

        # 2. Framework-level error (LangChain sets status="error" when a tool raises)
        if getattr(msg, "status", None) == "error":
            err_msg = str(getattr(msg, "content", "Fabric tool raised an exception"))
            print(f"[FABRIC AGENT] ToolMessage status=error: {err_msg}")
            return True, err_msg

    return False, ""


def fabric_agent_node(state: BankingAgentState):
    """Handle read-only data queries via the Fabric Data Agent.

    On any error (unhandled exception *or* a structured error payload inside a
    ToolMessage) the node sets fabric_agent_error=True and routes to
    account_agent as a fallback instead of ending the workflow.

    NOTE: The ReAct agent always wraps tool errors in a conversational AI reply,
    so we must inspect ToolMessages — not the final AIMessage content — to
    detect failures.
    """
    user_id = state["user_id"]
    fabric_agent = create_fabric_data_agent(user_id)

    thread_config = {"configurable": {"thread_id": f"fabric_{state['session_id']}"}}

    start_time = time.time()
    error_occurred = False
    error_message = ""
    response_messages = list(state["messages"])  # fallback: preserve originals

    try:
        response = fabric_agent.invoke({"messages": state["messages"]}, config=thread_config)
        finish_time = time.time()
        time_taken = finish_time - start_time

        response_messages = response["messages"]

        # Scan ToolMessages for an error payload (the LLM wraps errors in prose,
        # so the last AIMessage content is never JSON here)
        error_occurred, error_message = _fabric_tool_error(response_messages)

    except Exception as exc:
        finish_time = time.time()
        time_taken = finish_time - start_time
        error_occurred = True
        error_message = str(exc)
        print(f"[FABRIC AGENT] Exception during invocation: {error_message}")

    state["current_agent"] = "fabric_agent"
    state["time_taken"] = time_taken
    # Always write back messages so the event always carries a messages key,
    # which keeps prep_multi_agent_log_load and logging consistent.
    state["messages"] = response_messages

    if error_occurred:
        state["fabric_agent_error"] = True
        state["fabric_error_message"] = error_message
        state["pass_to"] = "account_agent"
        # Provide a placeholder final_result so execute_trace never reads None
        state["final_result"] = f"[fabric_agent error] {error_message}"
        print(f"[FABRIC AGENT] Handing off to account_agent. Error: {error_message}")
    else:
        state["fabric_agent_error"] = False
        state["fabric_error_message"] = ""
        state["pass_to"] = None
        state["final_result"] = response_messages[-1].content

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

    # Routing after fabric_agent: fall back to account_agent on error, else end
    def route_after_fabric(state: BankingAgentState):
        if state.get("fabric_agent_error"):
            print("[ROUTING] fabric_agent error → falling back to account_agent")
            return "account_agent"
        return END

    workflow.add_conditional_edges(
        "fabric_agent",
        route_after_fabric,
        {
            "account_agent": "account_agent",
            END: END,
        }
    )

    # All other agents end the workflow
    workflow.add_edge("account_agent", END)
    workflow.add_edge("support_agent", END)
    workflow.add_edge("visualization_agent", END)

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