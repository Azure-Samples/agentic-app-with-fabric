# backend/agent_tools.py
"""
Reusable Tool Definitions
Each tool is a standalone function that can be used by multiple agents
"""

from langchain_core.tools import tool
from banking_app import (
    get_user_accounts, create_new_account, transfer_money,
    get_transactions_summary, search_support_documents, db, app
)
from tools.database_query import query_database
import json
import traceback


# ============================================
# ACCOUNT MANAGEMENT TOOLS
# ============================================

def get_account_tools(user_id: str):
    """Create account management tools for a specific user"""
    
    @tool
    def get_user_accounts_tool() -> str:
        """Retrieves all bank accounts for the current user"""
        return get_user_accounts(user_id=user_id)
    
    @tool
    def create_new_account_tool(account_type: str = 'checking', name: str = None, balance: float = 0.0) -> str:
        """Creates a new bank account for the current user"""
        return create_new_account(user_id=user_id, account_type=account_type, name=name, balance=balance)
    
    @tool
    def transfer_money_tool(from_account_name: str = None, to_account_name: str = None, 
                           amount: float = 0.0, to_external_details: dict = None) -> str:
        """Transfers money between user accounts or to external accounts"""
        return transfer_money(user_id=user_id, from_account_name=from_account_name, 
                            to_account_name=to_account_name, amount=amount, 
                            to_external_details=to_external_details)
    
    @tool
    def get_transactions_summary_tool(time_period: str = 'this year', account_name: str = None) -> str:
        """Provides categorical spending summary for general time periods"""
        return get_transactions_summary(user_id=user_id, time_period=time_period, account_name=account_name)
    
    return [
        get_user_accounts_tool,
        create_new_account_tool,
        transfer_money_tool,
        get_transactions_summary_tool,
        query_database
    ]


# ============================================
# SUPPORT TOOLS
# ============================================

def get_support_tools():
    """Create customer support tools"""
    return [search_support_documents]


# ============================================
# VISUALIZATION TOOLS
# ============================================

def get_visualization_tools(user_id: str):
    """Create visualization/widget management tools for a specific user"""
    
    from cosmos_widget_model import create_widget, update_widget, delete_widget, get_widget_by_id, get_user_widgets
    from widget_queries import execute_widget_query
    
    @tool
    def create_ai_widget_tool(
        title: str,
        description: str = "",
        widget_type: str = "chart",
        chart_type: str = "bar",
        x_axis: str = "name",
        y_axis: str = "value",
        data: list = None,
        colors: list = None,
        data_mode: str = "static",
        query_type: str = None,
        time_range: str = "last_6_months"
    ) -> str:
        """
        Creates a new AI widget/chart in the user's AI Module dashboard.
        
        Args:
            title: Widget title
            chart_type: 'line', 'bar', 'pie', 'area'
            data_mode: 'static' or 'dynamic'
            query_type: For dynamic widgets - 'spending_by_category', 'monthly_trend', etc.
            time_range: For dynamic widgets - 'last_6_months', 'this_year', etc.
            data: For static widgets - array of data objects
        """
        try:
            config = {
                "chartType": chart_type,
                "xAxis": x_axis,
                "yAxis": y_axis,
                "customProps": {"data": []}
            }
            
            if colors:
                config["colors"] = colors
            
            query_config = None
            
            if data_mode == 'dynamic' and query_type:
                query_config = {
                    "query_type": query_type,
                    "time_range": time_range,
                    "filters": {}
                }
                
                try:
                    with app.app_context():
                        initial_data = execute_widget_query(query_config, user_id, db.session)
                    config["customProps"]["data"] = initial_data
                except Exception as e:
                    print(f"[widget] Error fetching initial data: {e}")
                    config["customProps"]["data"] = []
            elif data:
                config["customProps"]["data"] = data

            widget = create_widget(
                user_id=user_id,
                title=title,
                description=description,
                widget_type=widget_type,
                config=config,
                code=None,
                data_mode=data_mode,
                query_config=query_config
            )
            
            return json.dumps({
                "status": "success",
                "message": f"Created {data_mode} widget '{title}'",
                "widget_id": widget['id']
            })
        except Exception as e:
            traceback.print_exc()
            return json.dumps({"status": "error", "message": str(e)})
    
    @tool
    def update_ai_widget_tool(
        widget_id: str,
        title: str = None,
        description: str = None,
        chart_type: str = None,
        colors: list = None,
        data_mode: str = None,
        query_type: str = None,
        time_range: str = None
    ) -> str:
        """Updates an existing AI widget"""
        try:
            existing = get_widget_by_id(widget_id, user_id)
            if not existing:
                return json.dumps({"status": "error", "message": "Widget not found"})

            updates = {}
            if title is not None:
                updates['title'] = title
            if description is not None:
                updates['description'] = description

            config = existing.get('config', {})
            config_changed = False

            if chart_type is not None:
                config['chartType'] = chart_type
                config_changed = True
            if colors is not None:
                config['colors'] = colors
                config_changed = True

            if config_changed:
                updates['config'] = config

            if data_mode is not None:
                updates['data_mode'] = data_mode

            if query_type is not None or time_range is not None:
                query_config = existing.get('query_config') or {}
                if query_type:
                    query_config['query_type'] = query_type
                if time_range:
                    query_config['time_range'] = time_range
                updates['query_config'] = query_config

                if data_mode == 'dynamic' or existing.get('data_mode') == 'dynamic':
                    with app.app_context():
                        fresh_data = execute_widget_query(query_config, user_id, db.session)
                    if 'config' not in updates:
                        updates['config'] = config
                    updates['config']['customProps'] = {'data': fresh_data}

            updated = update_widget(widget_id, user_id, updates)
            
            return json.dumps({"status": "success", "message": "Widget updated"})
        except Exception as e:
            return json.dumps({"status": "error", "message": str(e)})
    
    @tool
    def create_simulation_widget_tool(
        title: str,
        description: str = "",
        simulation_type: str = "loan_repayment",
        defaults: dict = None
    ) -> str:
        """Creates interactive financial simulation widgets"""
        valid_types = ['loan_repayment', 'savings_projector', 'budget_planner', 
                      'retirement_calculator', 'emergency_fund']
        
        if simulation_type not in valid_types:
            return json.dumps({"status": "error", "message": f"Invalid simulation_type"})
        
        try:
            simulation_config = {
                "simulation_type": simulation_type,
                "defaults": defaults or {}
            }
            
            config = {"chartType": "simulation", "customProps": {}}
            
            widget = create_widget(
                user_id=user_id,
                title=title,
                description=description,
                widget_type="simulation",
                config=config,
                code=None,
                data_mode="static",
                query_config=None,
                simulation_config=simulation_config
            )
            
            return json.dumps({
                "status": "success",
                "message": f"Created {simulation_type} widget",
                "widget_id": widget['id']
            })
        except Exception as e:
            return json.dumps({"status": "error", "message": str(e)})
    
    @tool
    def list_user_widgets_tool() -> str:
        """Lists all widgets for the current user"""
        try:
            widgets = get_user_widgets(user_id)

            return json.dumps({
                "status": "success",
                "widgets": [{"id": w['id'], "title": w['title'], "type": w['widget_type']} 
                           for w in widgets]
            })
        except Exception as e:
            return json.dumps({"status": "error", "message": str(e)})
    
    @tool
    def delete_widget_tool(widget_id: str) -> str:
        """Deletes a widget from user's dashboard"""
        try:
            success = delete_widget(widget_id, user_id)

            return json.dumps({"status": "success" if success else "error"})
        except Exception as e:
            return json.dumps({"status": "error", "message": str(e)})
    
    return [
        create_ai_widget_tool,
        update_ai_widget_tool,
        create_simulation_widget_tool,
        list_user_widgets_tool,
        delete_widget_tool
    ]


# ============================================
# FABRIC DATA AGENT TOOLS
# ============================================

def get_fabric_data_agent_tools(user_id: str):
    """Create Fabric Data Agent tools for a specific user"""
    from tools.fabric_data_agent_tool import get_fabric_data_agent_tools as _get_tools
    return _get_tools(user_id)