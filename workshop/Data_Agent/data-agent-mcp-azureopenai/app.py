"""
Fabric Data Agent - Clean Web Interface with MSAL Interactive Auth and Azure OpenAI
"""

from flask import Flask, render_template, request, jsonify, session
import httpx
import json
import asyncio
import msal
import secrets
import threading
import time

app = Flask(__name__)
app.secret_key = secrets.token_hex(32)

# MSAL configuration
CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
AUTHORITY = "https://login.microsoftonline.com/common"
SCOPES = ["https://analysis.windows.net/powerbi/api/.default"]

# Session storage
sessions_data = {}
auth_status = {}  # Track authentication status by session

def get_session_id():
    if 'session_id' not in session:
        session['session_id'] = secrets.token_hex(16)
    return session['session_id']

def get_session_data():
    session_id = get_session_id()
    if session_id not in sessions_data:
        sessions_data[session_id] = {
            'token': None,
            'agent': None,
            'config': {},
            'history': []
        }
    return sessions_data[session_id]

@app.route('/')
def index():
    return render_template('index.html')

def do_interactive_auth(session_id):
    """Background thread for interactive authentication"""
    try:
        app_msal = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY)

        # Try silent auth first
        accounts = app_msal.get_accounts()
        if accounts:
            result = app_msal.acquire_token_silent(SCOPES, account=accounts[0])
            if result and "access_token" in result:
                data = sessions_data[session_id]
                data['token'] = result['access_token']
                auth_status[session_id] = {'status': 'success', 'message': 'Authentication successful!'}
                return

        # Interactive auth
        result = app_msal.acquire_token_interactive(scopes=SCOPES, prompt="select_account")

        if "access_token" in result:
            data = sessions_data[session_id]
            data['token'] = result['access_token']
            auth_status[session_id] = {'status': 'success', 'message': 'Authentication successful!'}
        else:
            error_msg = result.get('error_description', 'Authentication failed')
            auth_status[session_id] = {'status': 'error', 'message': error_msg}
    except Exception as e:
        auth_status[session_id] = {'status': 'error', 'message': f'Error: {str(e)}'}

@app.route('/authenticate', methods=['POST'])
def authenticate():
    try:
        session_id = get_session_id()

        # Ensure session data exists before starting background thread
        get_session_data()

        # Start auth in background thread
        auth_status[session_id] = {'status': 'pending', 'message': 'Opening login window...'}
        thread = threading.Thread(target=do_interactive_auth, args=(session_id,))
        thread.daemon = True
        thread.start()

        return jsonify({'status': 'pending', 'message': 'Opening login window... Please sign in.'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Error: {str(e)}'})

@app.route('/auth_status', methods=['GET'])
def check_auth_status():
    try:
        session_id = get_session_id()
        status = auth_status.get(session_id, {'status': 'unknown', 'message': 'No authentication in progress'})
        return jsonify(status)
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Error: {str(e)}'})

@app.route('/validate_config', methods=['POST'])
def validate_config():
    try:
        data = get_session_data()

        if not data['token']:
            return jsonify({'status': 'error', 'message': 'Please authenticate first'})

        config = request.json
        required_fields = ['server_url', 'tool_name', 'api_key', 'azure_endpoint', 'deployment_name']

        if not all(config.get(field) for field in required_fields):
            return jsonify({'status': 'error', 'message': 'Please fill in all fields'})

        data['config'] = config

        # Create query function
        from pydantic_ai import Agent, Tool

        async def query_fabric_data_agent(question: str) -> str:
            headers = {
                "Authorization": f"Bearer {data['token']}",
                "Content-Type": "application/json"
            }

            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": config['tool_name'],
                    "arguments": {"userQuestion": question}
                }
            }

            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(config['server_url'], headers=headers, json=payload)

            for line in response.text.split('\n'):
                if line.startswith('data: '):
                    try:
                        parsed = json.loads(line[6:])
                        content = parsed.get('result', {}).get('content', [])
                        if content:
                            return content[0].get('text', str(content))
                        return str(parsed.get('result', parsed))
                    except:
                        continue
            return response.text

        # Configure Azure OpenAI using AsyncAzureOpenAI
        import os
        from openai import AsyncAzureOpenAI
        from pydantic_ai.models.openai import OpenAIChatModel

        # Set environment variable to avoid default client creation errors
        os.environ['OPENAI_API_KEY'] = config['api_key']

        # Create Async Azure OpenAI client
        azure_client = AsyncAzureOpenAI(
            api_key=config['api_key'],
            api_version='2024-02-15-preview',
            azure_endpoint=config['azure_endpoint']
        )

        # Create model and override the client
        model = OpenAIChatModel(
            config['deployment_name'],
            provider='openai-chat'
        )
        model.client = azure_client

        data['agent'] = Agent(
            model,
            tools=[Tool(query_fabric_data_agent)],
            system_prompt="You are a data analyst assistant for Fabric Data Agent. Use the query_fabric_data_agent tool to answer questions about the data. Always use the tool to get real data - don't make up answers. Format your responses clearly and professionally."
        )

        data['history'] = []
        return jsonify({'status': 'success', 'message': 'Configuration validated! Ready to query.'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Setup error: {str(e)}'})

@app.route('/ask', methods=['POST'])
def ask():
    try:
        data = get_session_data()

        if not data['agent']:
            return jsonify({'status': 'error', 'message': 'Please complete configuration first'})

        question = request.json.get('question', '').strip()
        if not question:
            return jsonify({'status': 'error', 'message': 'Please enter a question'})

        async def get_answer():
            result = await data['agent'].run(question, message_history=data['history'] if data['history'] else None)
            data['history'] = result.all_messages()
            return result.output

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        answer = loop.run_until_complete(get_answer())
        loop.close()

        return jsonify({'status': 'success', 'question': question, 'answer': answer})
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Error: {str(e)}'})

@app.route('/clear', methods=['POST'])
def clear():
    try:
        data = get_session_data()
        data['history'] = []
        return jsonify({'status': 'success', 'message': 'Conversation cleared'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Error: {str(e)}'})

if __name__ == '__main__':
    print("=" * 60)
    print("  FABRIC DATA AGENT - Web Interface (Azure OpenAI)")
    print("=" * 60)
    print("\n  URL: http://127.0.0.1:5000")
    print("\n  Authentication: MSAL Interactive (browser popup)")
    print("  AI Provider: Azure OpenAI")
    print("=" * 60)
    app.run(debug=True, host='127.0.0.1', port=5000, use_reloader=False)
