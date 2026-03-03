# Quick Start Guide - Azure OpenAI Version

## Installation

1. Navigate to the project directory:
```bash
cd fabric-data-agent-aoai
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Running the Application

Start the Flask application:
```bash
python app.py
```

The server will start on **http://127.0.0.1:5000**

## Configuration Steps

### 1. Authenticate with Microsoft
- Open http://127.0.0.1:5000 in your browser
- Click "Authenticate with Microsoft"
- Sign in with your Microsoft account that has Fabric access
- Wait for "Authentication successful!" message

### 2. Configure MCP Server
Enter your Fabric Data Agent details:
- **MCP Server Name**: Any descriptive name (e.g., "My Data Agent")
- **MCP Server URL**: Your Fabric MCP endpoint
  - Example: `https://msitapi.fabric.microsoft.com/v1/mcp/workspaces/{workspace-id}/dataagents/{agent-id}/agent`
- **MCP Server Tool Name**: Your Data Agent tool name
  - Example: `DataAgent_YourName_da`

### 3. Configure Azure OpenAI
Enter your Azure OpenAI credentials:
- **Azure OpenAI API Key**: From Azure Portal → Your OpenAI Resource → Keys and Endpoint
- **Azure OpenAI Endpoint**: Your Azure OpenAI resource endpoint
  - Example: `https://fabricguru1522398265.openai.azure.com`
  - **Important**: No trailing slash!
- **Deployment Name**: Your Azure OpenAI deployment name
  - Example: `gpt-4.1`, `gpt-35-turbo`, etc.
  - Use your deployment's name, not the base model name

### 4. Validate & Start Querying
- Click "Validate & Setup Agent"
- Wait for "Configuration validated! Ready to query." message
- Switch to "Query Interface" tab
- Start asking questions about your data!

## Example Configuration

```
Azure OpenAI API Key: ********************************
Azure OpenAI Endpoint: https://fabricguru1522398265.openai.azure.com
Deployment Name: gpt-4.1
```

## Example Queries

```
"What are the top 5 customers by revenue?"
"Show me sales trends from last quarter"
"Which products have the highest profit margins?"
"Give me a summary of recent transactions"
```

## Troubleshooting

### "Setup error: OpenAIChatModel.__init__() got an unexpected keyword argument"
- This has been fixed in the latest version
- Make sure you're using the updated app.py

### "Authentication failed"
- Ensure your Microsoft account has access to the Fabric workspace
- Try clearing browser cookies and re-authenticating

### "Configuration validated!" but queries fail
- Verify your Azure OpenAI API key is correct
- Check that your deployment is active in Azure Portal
- Ensure you have available quota for your deployment

### Connection timeout
- Check your network connection
- Verify the Azure OpenAI endpoint URL is correct (no trailing slash)
- Confirm the deployment name matches exactly

## Technical Details

### How It Works
1. **Frontend**: HTML/JS interface sends requests to Flask backend
2. **Backend**: Flask handles authentication, session management, and API calls
3. **Authentication**: MSAL (Microsoft Authentication Library) handles Fabric login
4. **AI Orchestration**: pydantic-ai with Azure OpenAI backend
5. **MCP Protocol**: JSON-RPC calls to Fabric Data Agent

### Azure OpenAI Configuration
The application uses:
- **AsyncAzureOpenAI client** from the openai package
- **OpenAIChatModel** from pydantic-ai
- **API version**: 2024-02-15-preview
- Client override method to inject Azure credentials

### Security Notes
- All credentials are stored in server-side session only
- No credentials are logged or saved to disk
- API keys are masked in the UI
- Sessions expire when the browser is closed

## Need Help?

- Check the main [README.md](README.md) for detailed documentation
- Review Azure OpenAI documentation: https://learn.microsoft.com/en-us/azure/ai-services/openai/
- Check Fabric Data Agent documentation for MCP server details
