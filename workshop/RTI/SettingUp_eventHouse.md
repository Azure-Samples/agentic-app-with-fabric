## 📡 Real-Time Monitoring (RTI)

As the app runs, it streams content safety and usage events to Fabric Eventstream → Eventhouse (KQL). Follow these steps to complete the pipeline and enable the real-time dashboard.

### Step 1 — Launch the App and Do at Least One Test Chat 

The Eventstream needs at least one data event to infer the incoming schema before you can configure the destination.

Run the app, log in, and send any message in the chat.

---

### Step 2 — Connect the Eventhouse to the Eventstream

1. In your Fabric workspace, open **agentic_stream** (the Eventstream artifact).

   ![eventstream](../../assets/1.png)

2. Click the **agentic_stream** node → click **Refresh** → confirm at least one row of data is visible.

   ![eventhub data](../../assets/eventhub_1.png)

3. The Eventhouse destination will show as **Unconfigured**. Click **Configure**, click on **new table** and name it **agentic_events**, then follow below steps to finalize:

   ![configure 1](../../assets/conf_1.png)
   ![configure 2](../../assets/conf2.png)
   ![configure 3](../../assets/conf3.png)

4. Click **Close**, then click **Edit** (top right) → **Publish**.

   ![publish 1](../../assets/conf4.png)
   ![publish 2](../../assets/conf5.png)

---

### Step 3 — Verify Data is Flowing

Send another test chat, then open the **app_events** KQL Database in your workspace. It may take a few minutes on first run.

![kql data](../../assets/kql1.png)

---

### Step 4 — Build the Real-Time Dashboard

1. Open **QueryWorkBench** in your workspace and paste below queries there:

   ```query
   agentic_events

   | where filter_category startswith "None"

   | extend DateTime = todatetime(timestamp)

   | summarize eventCount = count() by DateTime

    

   agentic_events

   | where filter_category contains "violence"

   | extend DateTime = todatetime(timestamp)

   | summarize eventCount = count() by DateTime, filter_category

   | project DateTime, eventCount, filter_category

    

   agentic_events

   | where filter_category contains "self_harm"

   | extend DateTime = todatetime(timestamp)

   | summarize eventCount = count() by DateTime, filter_category

   | project DateTime, eventCount, filter_category

    

   agentic_events

   | where filter_category contains "hate"

   | extend DateTime = todatetime(timestamp)

   | summarize eventCount = count() by DateTime, filter_category

   | project DateTime, eventCount, filter_category

    

   agentic_events

   | where filter_category contains "jailbreak"

   | extend DateTime = todatetime(timestamp)

   | summarize eventCount = count() by DateTime, filter_category

   | project DateTime, eventCount, filter_category

    

    

   agentic_events

   | summarize total_count = count()

   ```
2. If you click on any of them and Run, you can see the results.

3. Now is the time to build your first real-time monitoring dashboard. Click on a query of your choice, then click on the "Save to Dashboard" drop down. Since this is a new dashboard, click on **To a new Dashboard** option and choose a name you desire (ex. ContentMonitoring)

   ![dashboard initial](../../assets/new_dash.png):



4. Now you can open your dashboard and see the first panel that you just added:

   ![dashboard initial](../../assets/example_dash.png)

   You can click on edit and modify the name, type of visualization, etc.

5. You can follow the same approach and add other queries to the same dashboard (or  a new one, if you desire.)


> 💡 **Test tip:** To simulate sensitive content without triggering real OpenAI filters, type a filter category name (e.g. `violence`, `jailbreak`) directly into the chat.

---