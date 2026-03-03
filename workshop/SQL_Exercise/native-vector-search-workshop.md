# Workshop: Native Vector Search with SQL Database and Azure OpenAI

This guide demonstrates how to implement **Vector Search** directly within a SQL Database (Microsoft Fabric or Azure SQL). You will learn how to connect your database to Azure OpenAI, generate embeddings for existing text data, and perform semantic search queries using the native `VECTOR` data type.

## Prerequisites

* **SQL Database** (Fabric SQL Database or Azure SQL Database)
* **Azure OpenAI Service** resource with:
    * The `text-embedding-ada-002` model deployed.
    * The **Endpoint URL** and **API Key**.
* **Existing Data**: This script assumes you have a source table named `[dbo].[PDF_RawChunks]` containing text data.

-----

## Step 1: Prepare the Vector Data Table

First, we create a new table based on our existing chunks and add a specialized column to store the vector embeddings.

```sql
-- 1. Copy data to a new working table
DROP TABLE IF EXISTS [dbo].[PDF_RawChunks_New];

SELECT *
INTO [dbo].[PDF_RawChunks_New]
FROM [dbo].[PDF_RawChunks];

-- 2. Add a Vector Column
-- Note: 1536 is the dimension size for the text-embedding-ada-002 model
ALTER TABLE [dbo].[PDF_RawChunks_New]
    ADD embeddings VECTOR(1536);

-- 3. Verify the table structure
SELECT * FROM [dbo].[PDF_RawChunks_New];
```

## Step 2: Configure Database Credentials

To allow the SQL Database to talk to the Azure OpenAI REST API, we need to store the credentials securely.

> **⚠️ ACTION REQUIRED:** Replace `https://XXX.openai.azure.com/` with your actual Azure OpenAI Endpoint and fill in your `api-key`.

```sql
-- 1. Create a Master Key for encryption (if it doesn't exist)
IF NOT EXISTS(SELECT * FROM sys.symmetric_keys WHERE [name] = '##MS_DatabaseMasterKey##')
BEGIN
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Pa$$_w0rd!ThatIS_L0Ng';
END
GO

-- 2. Manage Database Scoped Credentials
-- Replace the URL below with your specific Azure OpenAI Endpoint
IF EXISTS(SELECT * FROM sys.[database_scoped_credentials] WHERE name = 'https://XXX.openai.azure.com/')
BEGIN
    DROP DATABASE SCOPED CREDENTIAL [https://XXX.openai.azure.com/];
END

CREATE DATABASE SCOPED CREDENTIAL [https://XXX.openai.azure.com/]
WITH IDENTITY = 'HTTPEndpointHeaders', 
SECRET = '{"api-key": "YOUR_OPENAI_API_KEY_HERE"}';
GO

-- 3. Verify credential creation
SELECT * FROM sys.[database_scoped_credentials];
GO
```

## Step 3: Define the External AI Model

We create an `EXTERNAL MODEL` object that acts as a bridge between SQL and the embedding model.

> **⚠️ ACTION REQUIRED:** Update the `location` and `credential` URL to match your Azure OpenAI endpoint.

```sql
CREATE EXTERNAL MODEL Ada2Embeddings
WITH ( 
      LOCATION = 'https://XXX.openai.azure.com/openai/deployments/text-embedding-ada-002/embeddings?api-version=2024-08-01-preview',
      CREDENTIAL = [https://XXX.openai.azure.com/],
      API_FORMAT = 'Azure OpenAI',
      MODEL_TYPE = embeddings,
      MODEL = 'embeddings'
);
GO

-- Optional: Test the connection with a single generation
DECLARE @qv VECTOR(1536);
DROP TABLE IF EXISTS #t;
CREATE TABLE #t (v VECTOR(1536));

INSERT INTO #t 
SELECT AI_GENERATE_EMBEDDINGS(N'The foundation series by Isaac Asimov' USE MODEL Ada2Embeddings);

SELECT * FROM #t;
GO
```

## Step 4: Create the Embedding Procedure

We will create a versatile Stored Procedure that handles two scenarios:

1. **Ingestion:** Bulk generating embeddings for rows that are missing them.
2. **Search:** Generating an embedding for a specific user query.

```sql
CREATE OR ALTER PROCEDURE dbo.GenerateDocsEmbeddings
(
    @inputText NVARCHAR(MAX) = NULL,       -- Input text for search (Optional)
    @embedding VECTOR(1536) = NULL OUTPUT  -- Output vector for search (Optional)
)
AS
BEGIN
    SET NOCOUNT ON;

    -- MODE 1: Single Text Embedding (Search Mode)
    -- If input text is provided, generate the embedding and return it via OUTPUT parameter
    IF @inputText IS NOT NULL
    BEGIN
        SET @embedding = CAST(
            AI_GENERATE_EMBEDDINGS(
                @inputText USE MODEL Ada2Embeddings
            ) AS VECTOR(1536)
        );
    END

    -- MODE 2: Bulk Table Update (Ingestion Mode)
    -- If no input text is provided, generate embeddings for all table rows 
    -- where the embedding column is currently NULL
    ELSE
    BEGIN
        UPDATE dbo.PDF_RawChunks_New
        SET embeddings = CAST(
            AI_GENERATE_EMBEDDINGS(
                chunk_text USE MODEL Ada2Embeddings
            ) AS VECTOR(1536)
        )
        WHERE embeddings IS NULL;
    END
END;
GO
```

## Step 5: Process Data Embeddings

Now we run the procedure to vectorize our document chunks. This calls the Azure OpenAI API for every row in the table.

```sql
-- Run the bulk update
EXEC dbo.GenerateDocsEmbeddings;

-- Verify that the 'embeddings' column is now populated
SELECT * FROM [dbo].[PDF_RawChunks_New];
```

## Step 6: Perform Semantic Vector Search

Finally, we can query our data. We convert a natural language question into a vector and use `vector_distance` (Cosine Similarity) to find the most relevant chunks.

```sql
-- 1. Define the user's question
DECLARE @search_text NVARCHAR(MAX) = 'What are the fees on late Payments on Credit Card';
DECLARE @search_vector VECTOR(1536);

-- 2. Generate the embedding for the question
EXEC dbo.GenerateDocsEmbeddings 
    @inputText = @search_text, 
    @embedding = @search_vector OUTPUT;

-- 3. Find the closest 4 chunks using Cosine Distance
SELECT TOP(4) 
    p.chunk_text,
    vector_distance('cosine', @search_vector, p.embeddings) AS distance
FROM [dbo].[PDF_RawChunks_New] p
ORDER BY distance;
```
