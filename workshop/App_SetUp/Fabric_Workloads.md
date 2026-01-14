### Prerequisites check
Before diving into the required steps to start the app, let's first ensure all prerequisites are ready on your machine:
- This demo <span style="background-color: yellow">runs currently only on a Windows Machine </span>as it support ActiveDirectoryInteractive
- [Node.js](https://nodejs.org/) (v18 or later)
    - To check the version run this in terminal: ```node --version```
- [Python](https://www.python.org/) (3.11.9 or higher)
    - To check the version run this in terminal: ```python --version```
- A Fabric workspace with Git integration enbaled (you can check it in workspace settings)
- An [Azure OpenAI API Key](https://azure.microsoft.com/en-us/products/ai-services/openai-service)
- ODBC Driver for SQL Server 18
- Recommend VSCode as tested in VS Code only

---

Now that you have confirmed that you are meeting the minimum requirements, let's dive into deploying the Fabric artifacts!

## Set up required resources (One time)

### 1. Set up your repo
- Clone the repo: navigate to the workshop repository on GitHub: https://github.com/mehrsa/agentic-app-fabric-workshop
- Click on the "Code" button. Copy the URL provided for cloning.
- Open a terminal window on your machine and run below:

```bash
git clone https://github.com/mehrsa/agentic-app-fabric-workshop
cd agentic-app-fabric-workshop  # root folder of the repo
```
Now that you cloned the content of repo to your local machine, you need to create a private repo in your Git accoutn and push the content of this public repo there. Why? because you will need to put some sensitive credentials in your repo when deploying Fabric artifacts, so it has to be private. Follow below steps:
- Create a **private** repo in your Github account, with the same name. Since you will be adding sensitive credentials, **repo must be private**.
- Go back to terminal (you should be in the root folder of the repo you cloned) and push the content to your private repo by running below:

```bash
git push https://github.com/[replace with you git username]/[replace with your repo name].git
```
Refresh your private repo to confirm that it now has all the content.

### 2. Set up your Fabric account

- Log in to your Fabric account

- In Home tab (with Welcome to Fabric title), click on "New workspace" and proceed to create your workspace for this demo.

### 3. Automatic set up of all required Fabric resources and artifacts 
To easily set up your Fabric workspace with all required artifacts for this demo, you need to link your Fabric workspace with your repo (yes, the private one you just created). 

You only need to do below steps one time.

#### Step 1: Set up your database

1. Go to your workspace and click on "Workspace settings" on top right of the page
2. Go to Git integration tab -> Click on GitHub tile and click on "Add account"
3. Choose a name, paste your fine grained personal access token for the private repo you just created (don't know how to generate this? there are a lot of tutorials online such as: https://thetechdarts.com/generate-personal-access-token-in-github/)
4. paste the repo url and connect
5. After connecting to the repo, you will see the option in the same tab to provide the branch and folder name. Branch should be "main" and folder name should be "Fabric_artifacts"
    - Click on "Connect and Sync" 
    - Now the process of pulling all Fabric artifacts from the repo to your workspace starts. This may take a few minutes. Wait until all is done (you will see green check marks)
    
#### Step 2: Re-deploy to connect semantic model to the right database endpoint

- In the first step, data artifacts were deployed, but the semantic model needs to be redeloyed by provding the correct database endpoint parameters which you would need to obtain and provide manually as below:
1. Obtain below values (copy and keep somewhere)
    - **SQL server connection string**: First, go to the **SQL analytics endpoint** of the **agentic_lake**, go to settings -> SQL endpoint -> copy value under SQL connection string  (paste it somewhere to keep it for now)
    - **Lakehouse analytics GUID**: Look at the address bar, you should see something like this: *https://app.fabric.microsoft.com/groups/[first string]/mirroredwarehouses (or lakehouses)/**[second string]**?experience=fabric-developer*
        - copy the value you see in position of second string. 
2. Now go to: **/Fabric_artifacts/banking_semantic_model.SemanticModel/definition/**, open the file called **expressions.tmdl** and replace the values with the ones you just retrieved. *Save the file and push it to your repo*.

3. Now go back to your Fabric workspace and trigger an update via Source Control

4. This will start to set everything up and may take 1-2 minutes 

### 4. Populate your database with sample data

Data will be automatically populated, if not existing, in the SQL Database when you start the backend application.

**Add views to the SQL Analytics endpoint**
- go to the SQL analytics endpoint of your agentic_lake
- go to Data_Ingest folder and run all 3 queries that you see in file views.sql

**Congratulations!** Your Fabric artifacts are all set! You are halfway there to run the app!
