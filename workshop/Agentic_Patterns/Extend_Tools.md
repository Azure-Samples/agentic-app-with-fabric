## Equip existing agent with new tools

In the previous exercises, we showed how to create an example multi-agent system in which each agent was equipped with its own specialized tools. 

First, run the app, choose a user and ask this question: **what was my largest payment transaction?**

Take note of the user and the trace_id number. We will repeat this question with the same user later. 

Now let's follow the same pattern and add a new tool to one of the agents in the current banking application. We will show an example below, but feel free to define and add your own customized tool to the agent of your choice. 

Add below code block to the file called **agent_tools.py**:

``` python 
@tool
def find_largest_transaction_tool(
    user_id: str,
    time_period: str = "all time",
    category: str = None,
    transaction_type: str = None
) -> str:
    """
    Finds the largest transaction(s) for the user.
    
    Args:
        time_period: 'this_month', 'last_3_months', 'this_year', 'all_time' (default: 'all_time')
        category: Optional - filter by category (e.g., 'Dining', 'Shopping', 'Travel')
        transaction_type: Optional - filter by type (e.g., 'payment', 'transfer', 'deposit')
    
    Returns:
        JSON string with the largest transaction details
    """
    try:
        from banking_app import Transaction, Account
        from datetime import datetime
        from dateutil.relativedelta import relativedelta
        
        # Get user's account IDs
        accounts = Account.query.filter_by(user_id=user_id).all()
        account_ids = [acc.id for acc in accounts]
        
        if not account_ids:
            return json.dumps({
                "status": "error",
                "message": "No accounts found for this user"
            })
        
        # Build base query
        query = Transaction.query.filter(
            Transaction.from_account_id.in_(account_ids)
        )
        
        # Apply time filter
        end_date = datetime.utcnow()
        if time_period == "this_month":
            start_date = end_date.replace(day=1, hour=0, minute=0, second=0)
        elif time_period == "last_3_months":
            start_date = end_date - relativedelta(months=3)
        elif time_period == "last_6_months":
            start_date = end_date - relativedelta(months=6)
        elif time_period == "this_year":
            start_date = end_date.replace(month=1, day=1, hour=0, minute=0, second=0)
        else:# all time (max 10 years back)
            start_date = end_date - relativedelta(months=120)

        
        query = query.filter(Transaction.created_at.between(start_date, end_date))
        
        # Apply optional filters
        if category:
            query = query.filter(Transaction.category == category)
        
        if transaction_type:
            query = query.filter(Transaction.type == transaction_type)
        
        # Get the largest transaction
        largest = query.order_by(Transaction.amount.desc()).first()
        
        if not largest:
            return json.dumps({
                "status": "success",
                "message": f"No transactions found for the specified criteria",
                "filters_applied": {
                    "time_period": time_period,
                    "category": category,
                    "type": transaction_type
                }
            })
        
        # Get account details
        from_acc = Account.query.get(largest.from_account_id) if largest.from_account_id else None
        to_acc = Account.query.get(largest.to_account_id) if largest.to_account_id else None
        
        return json.dumps({
            "status": "success",
            "largest_transaction": {
                "amount": round(largest.amount, 2),
                "date": largest.created_at.strftime("%Y-%m-%d"),
                "description": largest.description,
                "category": largest.category,
                "type": largest.type,
                "from_account": from_acc.name if from_acc else "External",
                "to_account": to_acc.name if to_acc else "External",
                "status": largest.status
            },
            "filters_applied": {
                "time_period": time_period,
                "category": category,
                "type": transaction_type
            }
        })
        
    except Exception as e:
        return json.dumps({
            "status": "error",
            "message": str(e)
        })


```

Now, add this new tool to collection of account agent tools, by adding **find_largest_transaction_tool** to the return **list** of "get_account_tools" function by modifying the return logic as below:

```python
    return [
        get_user_accounts_tool,
        create_new_account_tool,
        transfer_money_tool,
        get_transactions_summary_tool,
        query_database,
        find_largest_transaction_tool
    ]

```

Save your changes and run the app to test. Ask the same question for the same user again. Having the **trace_id** for the two runs, you can now compare the execution time (in agent_traces table) and total token usage (via tool_usage table) between the two traces.
