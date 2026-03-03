## Follow below steps to run the app locally!
Now that all resources are set up, follow below steps to run and test the app:
### 1. Configure Environment Variables

Before running the application, you need to configure your environment variables. This file stores all the secret keys and connection strings your application needs to connect to Azure and Microsoft Fabric resources.

Rename the sample file: In the backend directory, find the file named **.env.sample** and rename it to **.env**.

Edit the variables: Open the new .env file and fill in the values for the following variables:

**Microsoft Fabric SQL Databases**

**FABRIC_SQL_CONNECTION_URL_AGENTIC**: This is the connection string for the SQL Database that contains both **the agentic application's operational data** (e.g., chat history) and **the sample customer banking data**. You can find this in your Fabric workspace by navigating to the SQL-endpoint of this database, clicking the "settings" -> "Connection strings" -> go to "ODBC" tab and select and copy SQL connection string.

**Microsoft Fabric Eventhub Connection**

There are two variables in .env file that you need to populate to successfully send real-time app usage logs to your Fabric EventHub: **FABRIC_EVENT_HUB_CONNECTION_STRING** and **FABRIC_EVENT_HUB_NAME**

In your Fabric workspace, open your Eventstream:

![eventstream](../../assets/1.png)

Click on "CustomEndpoint" block, then click on "SAS Key Authentication" tab as shown below: 

![eventstream](../../assets/eventstream_lineage.png)

![customendpoint](../../assets/sas_key.png)

Lastly, copy the value shown for "Event hub name" and paste it in the .env file as the **FABRIC_EVENT_HUB_NAME** value. Then, first click on the eye button near "Connection string-primary key", then copy the value. Paste this as the value for **FABRIC_EVENT_HUB_CONNECTION_STRING** in your .env file.

![customendpoint](../../assets/3_blurred.png)


**Azure OpenAI Services**

**AZURE_OPENAI_KEY**: Your API key for the Azure OpenAI service. You can find this in the Azure Portal by navigating to your Azure OpenAI resource and selecting Keys and Endpoint.

**AZURE_OPENAI_ENDPOINT**: The endpoint URL for your Azure OpenAI service. This is found on the same Keys and Endpoint page in the Azure Portal.

**AZURE_OPENAI_DEPLOYMENT**: The name of your chat model deployment (e.g., "gpt-5-mini"). This is the custom name you gave the model when you deployed it in Azure OpenAI Studio.

**AZURE_OPENAI_EMBEDDING_DEPLOYMENT**: text-embedding-ada-002 
- -> **NOTE: you have to have text-embedding-ada-002 deployed for this demo since embeddings were generated using this model**

### 2. Install Backend Requirements (Flask API)
In the root project directory run below commands:

```bash
python3 -m venv venv # this creates the environment
.\venv\Scripts\activate # (on Windows)  -- this Activates the environment
pip install -r requirements.txt
```

---

### 3. Configure the Frontend (React + Vite)

From the root project directory:

```bash
npm install
```

---

### 4. Run the Application

Open **two** terminal windows.

#### Terminal 1: Start Backend

From backend folder, run below in terminal (**ensure you have your virtual environment activated for this!**  )

```bash
python launcher.py
```
This will launch two services:
1. Banking service on: [http://127.0.0.1:5001](http://127.0.0.1:5001)
2. Agent analytics service on: [http://127.0.0.1:5002](http://127.0.0.1:5002)

**You will be prompted for your Fabric credentials during this so watch out for window pop ups and in taskbar!**

#### Terminal 2: Start Frontend

Ensure you are in the root of your folder and run below:

```bash
npm run dev
```

Frontend will run on: [http://localhost:5173](http://localhost:5173)

---
