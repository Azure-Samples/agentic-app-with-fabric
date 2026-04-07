# ⚗️ Explore Fabric Workloads

← [Back to README](../README.md)

This guide walks you through the Fabric-side features of the app after it is running. Each section is independent — do them in any order.

---

## Table of Contents

- [Real-Time Monitoring (RTI)](#-real-time-monitoring-rti)
- [Agentic Analytics (Power BI)](#-agentic-analytics-power-bi)
- [Fabric Data Agent](#-fabric-data-agent)
- [Agent Performance Evaluation (Notebook)](#-agent-performance-evaluation-notebook)

---

## 📡 Real-Time Monitoring (RTI)

As the app runs, it streams content safety and usage events to Fabric Eventstream → Eventhouse (KQL). Follow these steps to complete the pipeline and enable the real-time dashboard.

### Step 1 — Do at Least One Test Chat

The Eventstream needs at least one data event to infer the incoming schema before you can configure the destination.

Run the app, log in, and send any message in the chat.

---

### Step 2 — Connect the Eventhouse to the Eventstream

1. In your Fabric workspace, open **agentic_stream** (the Eventstream artifact).

   ![eventstream](../assets/1.png)

2. Click the **agentic_stream** node → click **Refresh** → confirm at least one row of data is visible.

   ![eventhub data](../assets/eventhub_1.png)

3. The Eventhouse destination will show as **Unconfigured**. Click **Configure** and follow these screens:

   ![configure 1](../assets/conf_1.png)
   ![configure 2](../assets/conf2.png)
   ![configure 3](../assets/conf3.png)

4. Click **Close**, then click **Edit** (top right) → **Publish**.

   ![publish 1](../assets/conf4.png)
   ![publish 2](../assets/conf5.png)

---

### Step 3 — Verify Data is Flowing

Send another test chat, then open the **app_events** KQL Database in your workspace. It may take a few minutes on first run.

![kql data](../assets/kql1.png)

---

### Step 4 — Build the Real-Time Dashboard

1. Open **QueryWorkBench** in your workspace. Pre-written example queries are already there.

   ![workbench](../assets/workbench.png)
   ![query](../assets/query1.png)

2. Open the **ContentSafetyMonitoring** dashboard — it starts with two panels:

   ![dashboard initial](../assets/dash1.png)

3. To add a query as a new panel: click any query block in QueryWorkBench → **Add to dashboard**.

   ![add to dashboard 1](../assets/dash2.png)
   ![add to dashboard 2](../assets/dash3.png)

4. Edit panel name, visualization type, and filters in the dashboard's edit mode.

   ![edit panel 1](../assets/dash5.png)
   ![edit panel 2](../assets/dash6.png)

> 💡 **Test tip:** To simulate sensitive content without triggering real OpenAI filters, type a filter category name (e.g. `violence`, `jailbreak`) directly into the chat.

---

## 📊 Agentic Analytics (Power BI)

As the app is used, operational data flows automatically through the pipeline:

```
agentic_app_db (SQL Database)
       ↓
agentic_lake (Lakehouse)
       ↓
banking_semantic_model (Semantic Model)
       ↓
Agentic_Insights (Power BI Report)
```

Open **Agentic_Insights** in your workspace to explore agent performance, usage patterns, and SQL workload metrics. The report refreshes as you use the app — no manual refresh needed.

---

## 🤖 Fabric Data Agent

The **Banking_DataAgent** gives the app a read-only, natural-language interface to the banking data warehouse. It is deployed and wired up automatically, but you can also explore it directly in the Fabric portal.

### Verify the Data Agent is Connected

The deployment script sets these three variables in `backend/.env` automatically:

```dotenv
USE_FABRIC_DATA_AGENT="true"
FABRIC_DATA_AGENT_SERVER_URL="https://api.fabric.microsoft.com/v1/workspaces/.../dataAgents/.../run"
FABRIC_DATA_AGENT_TOOL_NAME="Banking_DataAgent"
```

### Explore the Agent in the Fabric Portal

1. Open **Banking_DataAgent** in your workspace.
2. Click **Settings** → **Model Context Protocol** tab.
3. Here you can see the MCP Server URL and tool name, test queries, and view the agent's data sources and AI instructions.

### Test It in the App

Log in to the app and ask a data question:

- *"How much did I spend last month?"*
- *"What are my top 5 largest transactions?"*
- *"What is my current balance across all accounts?"*

The Fabric Data Agent handles these with read-only SQL access scoped to the logged-in user.

---

## 🧪 Agent Performance Evaluation (Notebook)

The **QA_Evaluation_Notebook** in your workspace computes four quality scores for agent responses using [Azure AI Evaluation](https://learn.microsoft.com/en-us/azure/ai-foundry/how-to/develop/agent-evaluate-sdk):

| Score | What it measures |
|---|---|
| Intent Resolution | Did the agent understand the user's goal? |
| Relevance | Is the response relevant to the question? |
| Coherence | Is the response logically structured? |
| Fluency | Is the response well-written? |

### Setup

1. Create a `.env` file with your judge LLM credentials:

   ```dotenv
   AZURE_OPENAI_KEY="your key"
   AZURE_OPENAI_ENDPOINT="your model endpoint"
   AZURE_OPENAI_DEPLOYMENT="model name"
   AZURE_OPENAI_API_VERSION="api version"
   ```

2. Upload this `.env` file to the **Files** section of the notebook page in Fabric (found under Tables in the Lakehouse view).

3. Open **QA_Evaluation_Notebook** in Fabric and run all cells in order.

After a successful run, a new table called **answerqualityscores_withcontext** appears in **agentic_lake** with all evaluation scores.
