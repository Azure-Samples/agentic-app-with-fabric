# Workshop: Cosmos DB Operational and Search Fundamentals

This guide demonstrates:
- The basics of creating a container and doing CRUD operations with Cosmos DB.
- Fundamentals of search, including vectorizing, vector search, full-text search and hybrid search

All above are demonstrated via various exercises in a Fabric notebook, which you can find in this folder (called **Cosmos DB Search Exercise.ipynb**)

## How to start

You need to do below to be able to run the exercises in the Fabric Notebook.

1- In your Fabric workspace, click on "Import" then "Notebook" and then choose the exercise notebook to upload.

2- Open the notebook. On the left hand side, under the Explorer section, click on "..." near "Built-in", then click on "Upload files". Choose the PDF_RawChunks.json file to upload.

1. Again click on "...", now choose "New file". Name the file **.env.txt**. After file is created double click to open it. Now copy below to the file:
```
COSMOSDB_ENDPOINT=""
COSMOSDB_DATABASE="agentic_cosmos_db"
COSMOSDB_CREDENTIAL_SCOPE="https://cosmos.azure.com/.default"

OPENAI_ENDPOINT=""
OPENAI_COMPLETION_MODEL_NAME=""
OPENAI_API_VERSION=""
OPENAI_EMBEDDING_MODEL_NAME="text-embedding-ada-002"
OPENAI_EMBEDDING_DIMENSIONS=1536
OPENAI_KEY=""
```
Next, ll the empty values should be filled with your own info and credentials:
 - Fill in with your own Cosmos db conncetions string 
 - Fill in your OpenAI model resource credentials and info.
   
 **Save the file.**

Now your are ready to run the notebook blocks in order and follow the exercise.
