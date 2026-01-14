## Add a new specialized agent to the multi-agent team

In the last exercise, we added a new tool to an agent which was specifically defined to find the highest transaction amount. Let's assume the purpose of this for the user would be to find suspicious account activites or potential fraud. In that case, we need a much more sophisticated way of looking for outlier transactions, almost like, we need another specialized agent?

Let's define and add an agent that is an expert in identifying potential fraudulant transactions when user asks for it. 

We will do this by doing below:

1. Add a new tool to agent_tools.py 
2. Define a new agent, equipped with this tool in agents.py. In the same file, we will also need to adjust the prompt for coordinator.
3. Add the corresponding node, edge and workflow logic in multi_agent_banking.py

Below we will give example code for this case, but following the same steps you can add the agent of your choice to this multi-agent system.

## 1. Define a fraud detection tool

From backend folder, open **agent_tools.py** and add below code:

```python
def get_fraud_detection_tools(user_id: str):
    """Create fraud detection and security monitoring tools"""
    
    @tool
    def detect_suspicious_activity_tool(
        lookback_days: int = 7,
        check_type: str = "all"
    ) -> str:
        """
        Scans recent transactions for suspicious patterns that may indicate fraud.
        
        Args:
            lookback_days: Number of days to analyze (default: 7, max: 30)
            check_type: 'all', 'unusual_amounts', 'rapid_transactions', 'unusual_locations'
        
        Returns:
            JSON with suspicious activities found and risk assessment
        """
        try:
            from banking_app import Transaction, Account
            from datetime import datetime, timedelta
            from collections import defaultdict
            
            # Limit validation
            lookback_days = min(max(1, lookback_days), 30)
            
            # Get user's accounts
            accounts = Account.query.filter_by(user_id=user_id).all()
            account_ids = [acc.id for acc in accounts]
            
            if not account_ids:
                return json.dumps({
                    "status": "error",
                    "message": "No accounts found"
                })
            
            # Calculate date range
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=lookback_days)
            
            # Get recent transactions
            transactions = Transaction.query.filter(
                Transaction.from_account_id.in_(account_ids),
                Transaction.created_at.between(start_date, end_date),
                Transaction.status == 'completed'
            ).order_by(Transaction.created_at.desc()).all()
            
            if not transactions:
                return json.dumps({
                    "status": "success",
                    "message": "No transactions found in the specified period",
                    "risk_level": "NONE",
                    "suspicious_count": 0
                })
            
            # Calculate baseline statistics
            amounts = [t.amount for t in transactions]
            avg_amount = sum(amounts) / len(amounts) if amounts else 0
            max_amount = max(amounts) if amounts else 0
            
            suspicious_activities = []
            risk_score = 0
            
            # Check 1: Unusual amounts (transactions > 3x average)
            if check_type in ["all", "unusual_amounts"]:
                for txn in transactions:
                    if txn.amount > (avg_amount * 3) and txn.amount > 500:
                        suspicious_activities.append({
                            "type": "UNUSUAL_AMOUNT",
                            "transaction_id": txn.id,
                            "amount": round(txn.amount, 2),
                            "date": txn.created_at.strftime("%Y-%m-%d %H:%M"),
                            "description": txn.description,
                            "reason": f"Amount ${txn.amount:.2f} is 3x higher than your average ${avg_amount:.2f}",
                            "risk_points": 2
                        })
                        risk_score += 2
            
            # Check 2: Rapid succession transactions (multiple transactions within 5 minutes)
            if check_type in ["all", "rapid_transactions"]:
                transaction_times = defaultdict(list)
                for txn in transactions:
                    minute_bucket = txn.created_at.replace(second=0, microsecond=0)
                    transaction_times[minute_bucket].append(txn)
                
                for time_bucket, txns in transaction_times.items():
                    if len(txns) >= 3:
                        suspicious_activities.append({
                            "type": "RAPID_TRANSACTIONS",
                            "count": len(txns),
                            "time": time_bucket.strftime("%Y-%m-%d %H:%M"),
                            "total_amount": round(sum(t.amount for t in txns), 2),
                            "reason": f"{len(txns)} transactions within 5 minutes",
                            "risk_points": 3
                        })
                        risk_score += 3
            
            # Check 3: Late-night transactions (between 11pm-5am)
            if check_type in ["all", "unusual_time"]:
                for txn in transactions:
                    hour = txn.created_at.hour
                    if (hour >= 23 or hour <= 5) and txn.amount > 200:
                        suspicious_activities.append({
                            "type": "UNUSUAL_TIME",
                            "transaction_id": txn.id,
                            "amount": round(txn.amount, 2),
                            "time": txn.created_at.strftime("%Y-%m-%d %H:%M"),
                            "description": txn.description,
                            "reason": "Large transaction during unusual hours (11pm-5am)",
                            "risk_points": 1
                        })
                        risk_score += 1
            
            # Check 4: Round numbers (often indicates fraud)
            if check_type in ["all", "round_amounts"]:
                for txn in transactions:
                    if txn.amount >= 100 and txn.amount % 100 == 0:
                        suspicious_activities.append({
                            "type": "ROUND_AMOUNT",
                            "transaction_id": txn.id,
                            "amount": round(txn.amount, 2),
                            "date": txn.created_at.strftime("%Y-%m-%d"),
                            "description": txn.description,
                            "reason": "Round dollar amount (common in fraud)",
                            "risk_points": 1
                        })
                        risk_score += 1
            
            # Determine overall risk level
            if risk_score >= 10:
                risk_level = "HIGH"
                recommendation = "🚨 Contact your bank immediately to review these transactions"
            elif risk_score >= 5:
                risk_level = "MEDIUM"
                recommendation = "⚠️ Review these transactions carefully"
            elif risk_score > 0:
                risk_level = "LOW"
                recommendation = "ℹ️ Minor anomalies detected, monitor your account"
            else:
                risk_level = "NONE"
                recommendation = "✓ No suspicious activity detected"
            
            return json.dumps({
                "status": "success",
                "risk_level": risk_level,
                "risk_score": risk_score,
                "suspicious_count": len(suspicious_activities),
                "period_analyzed": f"{lookback_days} days",
                "total_transactions": len(transactions),
                "suspicious_activities": suspicious_activities[:10],  # Limit to top 10
                "recommendation": recommendation,
                "statistics": {
                    "average_amount": round(avg_amount, 2),
                    "highest_amount": round(max_amount, 2),
                    "transaction_count": len(transactions)
                }
            })
            
        except Exception as e:
            traceback.print_exc()
            return json.dumps({
                "status": "error",
                "message": str(e)
            })
    
    return [detect_suspicious_activity_tool]
```
Save the file. Take some time to review and understand the logic.

## 2. Define the fraud detection agent

From backend folder, open **agents.py** and add below code:

```python
# ============================================
# FRAUD DETECTION AGENT
# ============================================
from agent_tools import get_fraud_detection_tools
def create_fraud_detection_agent(user_id: str):
    """Agent specialized in detecting suspicious activity and fraud"""
    llm = ai_client
    tools = get_fraud_detection_tools(user_id)
    
    system_prompt = f"""You are a security specialist helping user_id: {user_id} monitor for fraudulent activity.

## Your Role ##
- Analyze transaction patterns for suspicious activity
- Identify potential fraud indicators
- Provide clear security recommendations
- Educate users about fraud prevention

## Your Capabilities ##
- Detect unusual transaction amounts (3x+ average)
- Identify rapid succession transactions
- Flag late-night large purchases
- Spot round-dollar transactions (fraud pattern)

## Fraud Indicators ##
1. **Unusual Amounts**: Transactions significantly larger than normal
2. **Rapid Transactions**: Multiple purchases within minutes
3. **Unusual Times**: Large transactions at night (11pm-5am)
4. **Round Numbers**: Exact $100, $500 amounts (common in fraud)

## Response Style ##
- Be calm and professional, not alarmist
- Use security-appropriate emojis (🚨, ⚠️, ✓, ℹ️)
- Provide specific details about flagged transactions
- Give clear next steps if fraud suspected

## Risk Levels ##
- **HIGH** (10+ points): Immediate action required
- **MEDIUM** (5-9 points): Review carefully
- **LOW** (1-4 points): Minor concerns
- **NONE** (0 points): All clear

## Important ##
- Always provide transaction details for flagged items
- Explain WHY something is suspicious
- Recommend contacting bank if HIGH risk
- Reassure user if no issues found

Be vigilant but balanced in your assessments!"""
    
    return create_react_agent(llm, tools, prompt=system_prompt, checkpointer=MemorySaver())
```

## 3. Modify the multi-agent graph

Open multi_agent_banking.py, first add below import at the top:

```python
# Import existing banking infrastructure
from agents import create_fraud_detection_agent 
```

Then, go to the coordinator_node function, scroll down to the keyword routing section, and add below before the final **else** block:

```python
    elif message_lower == "fraud_detection_agent":  #fraud
        state["pass_to"] = "fraud_detection_agent"
        state["task_type"] = "fraud_detection"
        print(f"[COORDINATOR] Routing to: fraud_detection_agent")
```

Now, we need to add this agent's node to the graph. Add below code:

```python

def fraud_detection_agent_node(state: BankingAgentState): #fraud
    """Handle fraud detection tasks."""
    user_id = state["user_id"]
    fraud_detection_agent = create_fraud_detection_agent(user_id)
    
    thread_config = {"configurable": {"thread_id": f"fraud_{state['session_id']}"}}
    
    start_time = time.time()
    response = fraud_detection_agent.invoke({"messages": state["messages"]}, config=thread_config)
    finish_time = time.time()
    time_taken = finish_time - start_time

    state["current_agent"] = "fraud_detection_agent"
    state["pass_to"] = None
    state["messages"] = response["messages"]
    state["final_result"] = response["messages"][-1].content
    state["time_taken"] = time_taken
    
    return state
```

And finally, go to the function called create_multi_agent_banking system, and do below adds:

- Add below under the "Add nodes" section:

```python
workflow.add_node("fraud_detection_agent", fraud_detection_agent_node)
```
- Add below as a new conditional edge right under the one for visualization_agent

```python
"fraud_detection_agent": "fraud_detection_agent"
```

And lastly, add this before the return statement:

```python
workflow.add_edge("fraud_detection_agent", END)  #fraud
```

Remember to save!

Run the app and test this new agent by asking a relevant question. You can then go to the Fabric SQL database and check to see the generated data. Also check agent_definitions and tool_definitions tables which now should have the new tool and agent added to them.