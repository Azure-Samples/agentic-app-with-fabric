# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

%pip install azure-ai-evaluation azure-identity azure-ai-projects openai

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ## Step 1: Import Libraries

import os
import json
from datetime import datetime
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, udf, collect_list,
    struct, row_number, avg, count as sql_count
)
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, ArrayType
from pyspark.sql.window import Window

# Azure AI Evaluation SDK imports
from azure.ai.evaluation import (
    IntentResolutionEvaluator,
    RelevanceEvaluator,
    CoherenceEvaluator,
    FluencyEvaluator
)

print("Azure AI Evaluation SDK loaded successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ## Step 2: Load Configuration from .env file in Built-in

env_file_path = "/lakehouse/default/Files/.env"  # Adjust this path as needed

print("Loading configuration from .env file...")
try:
    with open(env_file_path, "r") as f:
        for line in f:
            line = line.strip()
            # Skip comments and empty lines
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                
                # --- FIX: Clean whitespace around key and value ---
                key = key.strip()
                value = value.strip()
                # --------------------------------------------------
                
                # Remove quotes from the value
                value = value.strip("'\"")
                
                # Set the variable in the environment
                os.environ[key] = value
                
                # Secure printing
                if "KEY" in key.upper() or "SECRET" in key.upper():
                    print(f"  > Loaded: {key} = ***(hidden)***")
                else:
                    print(f"  > Loaded: {key} = {value}")
                    
    print("\nSuccessfully loaded configuration from .env file.")

except FileNotFoundError:
    raise FileNotFoundError(f"Error: The .env file was not found at {env_file_path}")
except Exception as e:
    raise RuntimeError(f"Error reading or parsing .env file: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ## Step 3: Load, Prepare, and Filter Q&A Pairs

# --- INCREMENTAL Q&A EVALUATION SYSTEM WITH CONTEXT ---
"""
This code:
1. Loads all chat history.
2. Identifies and aligns Q&A pairs (human -> ai).
3. Builds the prior conversation history for each Q&A pair.
4. Checks against the scores table to find NEW Q&A pairs that haven't been evaluated.
5. Creates the final 'df_new_qa_pairs' DataFrame for evaluation.
"""

from pyspark.sql.functions import col, current_timestamp, lit, lag, array, when, lower
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

# ============================================================================
# CONFIGURATION
# ============================================================================
SOURCE_TABLE = "chat_history"
ALIGNED_QA_TABLE = "chat_qa_pairs_aligned"
SCORES_TABLE = "AnswerQualityScores_WithContext"

USER_MESSAGE_TYPE = 'human'
AGENT_MESSAGE_TYPE = 'ai'

# ============================================================================
# Check/Create Tables
# ============================================================================
def ensure_tables_exist():
    """Ensure the necessary tables exist with proper schema."""
    try:
        existing_scores = spark.read.table(SCORES_TABLE)
        print(f"✓ Scores table '{SCORES_TABLE}' exists with {existing_scores.count()} records")
    except Exception as e:
        print(f"! Scores table '{SCORES_TABLE}' doesn't exist yet - will be created on first run")
    
    try:
        existing_qa = spark.read.table(ALIGNED_QA_TABLE)
        print(f"✓ Aligned Q&A table '{ALIGNED_QA_TABLE}' exists with {existing_qa.count()} records")
    except Exception as e:
        print(f"! Aligned Q&A table '{ALIGNED_QA_TABLE}' doesn't exist yet - will be created on first run")

ensure_tables_exist()

# ============================================================================
# Get Already Evaluated Trace IDs
# ============================================================================
def get_already_evaluated_trace_ids():
    """Returns a set of trace_ids that have already been evaluated."""
    try:
        df_existing = spark.read.table(SCORES_TABLE)
        evaluated_trace_ids = df_existing.select("agent_trace_id").distinct()
        count = evaluated_trace_ids.count()
        print(f"\n{'='*80}")
        print(f"ALREADY EVALUATED: {count} trace_ids")
        print(f"{'='*80}")
        return evaluated_trace_ids
    except Exception as e:
        print(f"\n{'='*80}")
        print(f"FIRST RUN: No existing evaluations found")
        print(f"{'='*80}")
        return spark.createDataFrame([], "agent_trace_id STRING")

df_already_evaluated = get_already_evaluated_trace_ids()

# ============================================================================
# Extract and Align Q&A Pairs
# ============================================================================
print("\n" + "="*80)
print("BUILDING Q&A PAIRS WITH CONVERSATION CONTEXT")
print("="*80)

# Load chat history
df_chat_history = spark.read.table(SOURCE_TABLE)
total_messages = df_chat_history.count()
print(f"Total messages in chat history: {total_messages}")

# --- FIX: Check if 'agent_name' column exists and include it ---
source_columns = df_chat_history.columns
has_agent_name = "agent_name" in source_columns

# Define columns to select
cols_to_select = [
    "trace_id", "session_id", "user_id", "agent_id", "message_type", 
    "content", "tool_name", "response_time_ms", "model_name", 
    "total_tokens", "completion_tokens", "prompt_tokens", "trace_end"
]

if has_agent_name:
    print("✓ Found 'agent_name' column - including in selection")
    cols_to_select.append("agent_name")
else:
    print("! 'agent_name' column NOT found - proceeding with 'agent_id' only")

# Get conversation messages
df_conversation = df_chat_history.filter(
    col("message_type").isin([USER_MESSAGE_TYPE, AGENT_MESSAGE_TYPE])
).select(*cols_to_select).orderBy("session_id", "trace_end")

print(f"Total conversation messages: {df_conversation.count()}")

# ============================================================================
# Build Conversation Context for each message
# ============================================================================

# Add row number within each session to track conversation order
window_spec = Window.partitionBy("session_id").orderBy("trace_end")
df_with_order = df_conversation.withColumn("turn_number", row_number().over(window_spec))

# Separate questions
df_questions = df_with_order.filter(col("message_type") == USER_MESSAGE_TYPE).alias("q")

# Filter out answers where agent_id OR agent_name is "coordinator" (case-insensitive)
filter_condition = (col("message_type") == AGENT_MESSAGE_TYPE)
filter_condition = filter_condition & (lower(col("agent_id")) != "coordinator")

if has_agent_name:
    filter_condition = filter_condition & (lower(col("agent_name")) != "coordinator")

df_answers = df_with_order.filter(filter_condition).alias("a")
# ----------------------------------------------------------------

# Join questions with their answers
df_qa_pairs = df_questions.join(
    df_answers,
    (df_questions["trace_id"] == df_answers["trace_id"]) &
    (df_questions["session_id"] == df_answers["session_id"]),
    "inner"
).select(
    col("q.trace_id").alias("agent_trace_id"),
    col("q.session_id").alias("session_id"),
    col("q.user_id").alias("user_id"),
    col("q.agent_id").alias("agent_id"),
    col("q.content").alias("user_question"),
    col("a.content").alias("agent_answer"),
    col("q.turn_number").alias("turn_number"),
    col("a.tool_name").alias("invoked_tool_name"),
    col("a.response_time_ms").alias("response_time_ms"),
    col("a.model_name").alias("model_name"),
    col("a.total_tokens").alias("total_tokens"),
    col("a.completion_tokens").alias("completion_tokens"),
    col("a.prompt_tokens").alias("prompt_tokens"),
    col("q.trace_end").alias("trace_end")
).filter(
    col("user_question").isNotNull() & col("agent_answer").isNotNull()
)

total_qa_pairs = df_qa_pairs.count()
print(f"Total Q&A pairs (excluding coordinator): {total_qa_pairs}")

# ============================================================================
# Build Conversation History
# ============================================================================

# Self-join to get previous Q&A pairs in the same session
df_with_history = df_qa_pairs.alias("current").join(
    df_qa_pairs.alias("previous"),
    (col("current.session_id") == col("previous.session_id")) &
    (col("current.turn_number") > col("previous.turn_number")),
    "left"
).groupBy(
    col("current.agent_trace_id"),
    col("current.session_id"),
    col("current.user_id"),
    col("current.agent_id"),
    col("current.user_question"),
    col("current.agent_answer"),
    col("current.turn_number"),
    col("current.invoked_tool_name"),
    col("current.response_time_ms"),
    col("current.model_name"),
    col("current.total_tokens"),
    col("current.completion_tokens"),
    col("current.prompt_tokens"),
    col("current.trace_end")
).agg(
    collect_list(
        when(col("previous.user_question").isNotNull(),
             struct(
                 col("previous.user_question").alias("question"),
                 col("previous.agent_answer").alias("answer"),
                 col("previous.turn_number").alias("turn")
             )
        )
    ).alias("conversation_history")
)

print(f"Built conversation context for {df_with_history.count()} Q&A pairs")

# ============================================================================
# Filter out Already Evaluated Pairs
# ============================================================================
print("\n" + "="*80)
print("IDENTIFYING NEW Q&A PAIRS TO EVALUATE")
print("="*80)

df_new_qa_pairs = df_with_history.join(
    df_already_evaluated,
    df_with_history["agent_trace_id"] == df_already_evaluated["agent_trace_id"],
    "left_anti"
)

new_pairs_count = df_new_qa_pairs.count()
print(f"New Q&A pairs to evaluate: {new_pairs_count}")
print(f"Already evaluated: {total_qa_pairs - new_pairs_count}")

if new_pairs_count == 0:
    print("\n" + "="*80)
    print("✓ NO NEW Q&A PAIRS TO EVALUATE")
    print("All existing Q&A pairs have already been scored.")
    print("="*80)
else:
    print("\nSample of new Q&A pairs to be evaluated (with context):")
    df_new_qa_pairs.select(
        "agent_trace_id",
        "turn_number",
        "user_question",
        "agent_answer",
        "conversation_history"
    ).show(3, truncate=80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ## Step 4: Initialize Azure AI Evaluation Configuration
# Initialize model configuration for evaluators using dictionary format
# This is the correct format for newer versions of azure-ai-evaluation
model_config = {
    "azure_endpoint": os.environ.get("AZURE_OPENAI_ENDPOINT"),
    "api_key": os.environ.get("AZURE_OPENAI_KEY"),
    "api_version": os.environ.get("AZURE_OPENAI_API_VERSION"),
    "azure_deployment": os.environ.get("AZURE_OPENAI_DEPLOYMENT")
}

print(f"Model configuration initialized for endpoint: {os.environ.get('AZURE_OPENAI_ENDPOINT')}")

# Initialize evaluators
intent_resolution = IntentResolutionEvaluator(model_config=model_config)

relevance = RelevanceEvaluator(model_config=model_config)
coherence = CoherenceEvaluator(model_config=model_config)
fluency = FluencyEvaluator(model_config=model_config)

print("Evaluators initialized successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ## Step 5: Define Evaluation Function Using Azure AI SDK

# Get Azure OpenAI credentials to pass to UDF
azure_endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT")
api_key = os.environ.get("AZURE_OPENAI_KEY")
api_version = os.environ.get("AZURE_OPENAI_API_VERSION")
deployment_name = os.environ.get("AZURE_OPENAI_DEPLOYMENT")

print(f"\nUDF will use:")
print(f"  Endpoint: {azure_endpoint}")
print(f"  API Version: {api_version}")
print(f"  Deployment: {deployment_name}")
print(f"  API Key: {'***' if api_key else 'NOT SET'}")

def evaluate_with_azure_ai_sdk(conversation_history, current_question, current_answer):
    """
    Evaluates Q&A pairs using Azure AI Evaluation SDK evaluators.
    Returns a dictionary with all evaluation scores.
    """
    if not current_question or not current_answer:
        return {
            "intent_resolution": None,
            "relevance": None,
            "coherence": None,
            "fluency": None,
            "intent_resolution_reason": "",
            "relevance_reason": "",
            "coherence_reason": "",
            "fluency_reason": "",
            "evaluation_error": "Missing question or answer"
        }
    
    try:
        # Re-initialize model config and evaluators inside UDF (for Spark executor)
        from azure.ai.evaluation import (
            IntentResolutionEvaluator,
            RelevanceEvaluator,
            CoherenceEvaluator,
            FluencyEvaluator
        )
        
        # Use dictionary-based model configuration (newer SDK format)
        model_config = {
            "azure_endpoint": azure_endpoint,
            "api_key": api_key,
            "api_version": api_version,
            "azure_deployment": deployment_name
        }
        
        # Add is_reasoning_model=True for o1/o3/gpt-5-mini models ---
        # This switches the SDK to use 'max_completion_tokens' instead of 'max_tokens'
        intent_resolution_eval = IntentResolutionEvaluator(
            model_config=model_config, # Note: Check if this specific evaluator supports the flag if it fails,
            is_reasoning_model=True
        )
        
        relevance_eval = RelevanceEvaluator(
            model_config=model_config,
            is_reasoning_model=True
        )
        coherence_eval = CoherenceEvaluator(
            model_config=model_config,
            is_reasoning_model=True
        )
        fluency_eval = FluencyEvaluator(
            model_config=model_config,
            is_reasoning_model=True
        )
        # --------------------------------------------------------------------
        
        # Build conversation context for evaluators that support it
        context_messages = []
        
        # Add conversation history if exists
        if conversation_history and len(conversation_history) > 0:
            try:
                # Handle both dict and Row objects
                history_list = []
                for exchange in conversation_history:
                    if hasattr(exchange, 'asDict'):
                        history_list.append(exchange.asDict())
                    else:
                        history_list.append(exchange)
                
                # Sort by turn number
                sorted_history = sorted(history_list, key=lambda x: x.get('turn', 0))
                
                for exchange in sorted_history:
                    context_messages.append({
                        "role": "user",
                        "content": exchange.get("question", "")
                    })
                    context_messages.append({
                        "role": "assistant",
                        "content": exchange.get("answer", "")
                    })
            except Exception as e:
                pass
        
        # Current exchange
        context_messages.append({
            "role": "user",
            "content": current_question
        })
        context_messages.append({
            "role": "assistant",
            "content": current_answer
        })
        
        # Run evaluators
        results = {}
        
        # Intent Resolution
        try:
            # Note: If IntentResolutionEvaluator fails with the same error
            intent_result = intent_resolution_eval(
                query=current_question,
                response=current_answer
            )
            if isinstance(intent_result, dict):
                results["intent_resolution"] = intent_result.get("intent_resolution", None)
                results["intent_resolution_reason"] = intent_result.get("intent_resolution_reason", "")
                results["intent_resolution_result"] = intent_result.get("intent_resolution_result", "")
                results["intent_resolution_threshold"] = intent_result.get("intent_resolution_threshold", None)
            else:
                results["intent_resolution"] = None
                results["intent_resolution_reason"] = "Unexpected result format"
        except Exception as e:
            results["intent_resolution"] = None
            results["intent_resolution_reason"] = f"Error: {str(e)}"
            results["intent_resolution_result"] = ""
            results["intent_resolution_threshold"] = None
        
        # Relevance
        try:
            relevance_result = relevance_eval(
                query=current_question,
                response=current_answer
            )
            if isinstance(relevance_result, dict):
                results["relevance"] = relevance_result.get("relevance", None)
                results["relevance_reason"] = relevance_result.get("relevance_reason", "")
                results["relevance_result"] = relevance_result.get("relevance_result", "")
                results["relevance_threshold"] = relevance_result.get("relevance_threshold", None)
            else:
                results["relevance"] = None
        except Exception as e:
            results["relevance"] = None
            results["relevance_reason"] = f"Error: {str(e)}"
            results["relevance_result"] = ""
            results["relevance_threshold"] = None
        
        # Coherence
        try:
            coherence_result = coherence_eval(
                query=current_question,
                response=current_answer
            )
            if isinstance(coherence_result, dict):
                results["coherence"] = coherence_result.get("coherence", None)
                results["coherence_reason"] = coherence_result.get("coherence_reason", "")
                results["coherence_result"] = coherence_result.get("coherence_result", "")
                results["coherence_threshold"] = coherence_result.get("coherence_threshold", None)
            else:
                results["coherence"] = None
        except Exception as e:
            results["coherence"] = None
            results["coherence_reason"] = f"Error: {str(e)}"
            results["coherence_result"] = ""
            results["coherence_threshold"] = None
        
        # Fluency
        try:
            fluency_result = fluency_eval(
                query=current_question,
                response=current_answer
            )
            if isinstance(fluency_result, dict):
                results["fluency"] = fluency_result.get("fluency", None)
                results["fluency_reason"] = fluency_result.get("fluency_reason", "")
                results["fluency_result"] = fluency_result.get("fluency_result", "")
                results["fluency_threshold"] = fluency_result.get("fluency_threshold", None)
            else:
                results["fluency"] = None
        except Exception as e:
            results["fluency"] = None
            results["fluency_reason"] = f"Error: {str(e)}"
            results["fluency_result"] = ""
            results["fluency_threshold"] = None
        
        results["evaluation_error"] = None
        return results
        
    except Exception as e:
        return {
            "intent_resolution": None,
            "relevance": None,
            "coherence": None,
            "fluency": None,
            "intent_resolution_reason": "",
            "relevance_reason": "",
            "coherence_reason": "",
            "fluency_reason": "",
            "evaluation_error": f"Evaluation failed: {repr(e)}"
        }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ## Step 6: Create Spark UDF for Evaluation

# Define schema for evaluation results with additional metadata
evaluation_schema = StructType([
    StructField("intent_resolution", FloatType(), True),
    StructField("intent_resolution_reason", StringType(), True),
    StructField("intent_resolution_result", StringType(), True),  # pass/fail
    StructField("intent_resolution_threshold", FloatType(), True),
    
    StructField("relevance", FloatType(), True),
    StructField("relevance_reason", StringType(), True),
    StructField("relevance_result", StringType(), True),  # pass/fail
    StructField("relevance_threshold", FloatType(), True),
    
    StructField("coherence", FloatType(), True),
    StructField("coherence_reason", StringType(), True),
    StructField("coherence_result", StringType(), True),  # pass/fail
    StructField("coherence_threshold", FloatType(), True),
    
    StructField("fluency", FloatType(), True),
    StructField("fluency_reason", StringType(), True),
    StructField("fluency_result", StringType(), True),  # pass/fail
    StructField("fluency_threshold", FloatType(), True),
    
    StructField("evaluation_error", StringType(), True)
])

# Create UDF
evaluate_udf = udf(evaluate_with_azure_ai_sdk, evaluation_schema)

print("Evaluation UDF created successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ## Step 7: Evaluate New Q&A Pairs

# This cell depends on 'new_pairs_count' and 'df_new_qa_pairs'
# from the data preparation cell (Step 4)

if new_pairs_count > 0:
    print("\n" + "="*80)
    print(f"EVALUATING {new_pairs_count} NEW Q&A PAIRS WITH AZURE AI SDK")
    print("="*80)
    
    # Generate run ID
    run_timestamp = datetime.now().isoformat()
    current_run_id = f"azure_ai_sdk_run_{run_timestamp}"
    
    print(f"Evaluation run ID: {current_run_id}")
    print("Starting evaluation (this may take a few minutes)...")
    
    # Apply evaluation UDF
    df_scored = df_new_qa_pairs.withColumn(
        "evaluation_results",
        evaluate_udf(
            col("conversation_history"),
            col("user_question"),
            col("agent_answer")
        )
    )
    
    # Flatten the results
    df_new_scores = df_scored.select(
        "agent_trace_id",
        "session_id",
        "user_id",
        "agent_id",
        "turn_number",
        "user_question",
        "agent_answer",
        "invoked_tool_name",
        "response_time_ms",
        "model_name",
        "total_tokens",
        "completion_tokens",
        "prompt_tokens",
        "trace_end",
        col("evaluation_results.intent_resolution").alias("intent_resolution"),
        col("evaluation_results.intent_resolution_reason").alias("intent_resolution_reason"),
        col("evaluation_results.intent_resolution_result").alias("intent_resolution_result"),
        col("evaluation_results.intent_resolution_threshold").alias("intent_resolution_threshold"),
        col("evaluation_results.relevance").alias("relevance"),
        col("evaluation_results.relevance_reason").alias("relevance_reason"),
        col("evaluation_results.relevance_result").alias("relevance_result"),
        col("evaluation_results.relevance_threshold").alias("relevance_threshold"),
        col("evaluation_results.coherence").alias("coherence"),
        col("evaluation_results.coherence_reason").alias("coherence_reason"),
        col("evaluation_results.coherence_result").alias("coherence_result"),
        col("evaluation_results.coherence_threshold").alias("coherence_threshold"),
        col("evaluation_results.fluency").alias("fluency"),
        col("evaluation_results.fluency_reason").alias("fluency_reason"),
        col("evaluation_results.fluency_result").alias("fluency_result"),
        col("evaluation_results.fluency_threshold").alias("fluency_threshold"),
        col("evaluation_results.evaluation_error").alias("evaluation_error")
    ).withColumn(
        "evaluated_at",
        current_timestamp()
    ).withColumn(
        "evaluation_run_id",
        lit(current_run_id)
    ).withColumn(
        "evaluation_method",
        lit("azure_ai_sdk_with_context")
    )
    
    # Cache results to avoid re-computation
    print("Caching evaluation results...")
    df_new_scores.cache()
    cache_count = df_new_scores.count()
    print(f"✓ Caching complete. {cache_count} rows processed.")
    
    # Debug: Show raw evaluation results before saving
    print("\n" + "="*80)
    print("DEBUG: RAW EVALUATION RESULTS (First 3 rows)")
    print("="*80)
    df_new_scores.select(
        "agent_trace_id",
        "intent_resolution",
        "relevance",
        "coherence",
        "fluency",
        "evaluation_error"
    ).show(3, truncate=False)
    
    # Debug: Check for null values
    print("\n" + "="*80)
    print("DEBUG: NULL VALUE CHECK")
    print("="*80)
    null_counts = df_new_scores.select(
        [sql_count(when(col(c).isNull(), c)).alias(c) 
         for c in ["intent_resolution", "relevance", "coherence", "fluency"]]
    ).collect()[0].asDict()
    
    for metric, null_count in null_counts.items():
        total = cache_count
        non_null = total - null_count
        print(f"  {metric}: {non_null}/{total} have values ({null_count} nulls)")
    
    # Save to scores table
    print("\n" + "="*80)
    print("SAVING NEW EVALUATIONS")
    print("="*80)
    
    df_new_scores.write.mode("append").format("delta").saveAsTable(SCORES_TABLE)
    print(f"✓ Successfully appended {cache_count} new evaluations to '{SCORES_TABLE}'")
    
    # Display sample results
    print("\n" + "="*80)
    print("SAMPLE EVALUATION RESULTS")
    print("="*80)
    
    df_new_scores.select(
        "agent_trace_id",
        "turn_number",
        "user_question",
        "intent_resolution",
        "relevance",
        "coherence",
        "fluency"
    ).show(5, truncate=80)
    
    # Show reasons for a few samples
    print("\n" + "="*80)
    print("SAMPLE EVALUATION REASONS (with full error)")
    print("="*80)
    
    df_new_scores.select(
        "agent_trace_id",
        "intent_resolution_reason",
        "relevance_reason",
        "coherence_reason",
        "fluency_reason"
    ).show(3, truncate=False) # Set truncate=False to see the full error
    
    # Summary statistics
    print("\n" + "="*80)
    print("SUMMARY STATISTICS (NEW EVALUATIONS)")
    print("="*80)
    
    df_new_scores.select(
        "intent_resolution",
        "relevance",
        "coherence",
        "fluency"
    ).describe().show()
    
    # Check for errors
    error_count = df_new_scores.filter(col("evaluation_error").isNotNull()).count()
    if error_count > 0:
        print(f"\n⚠️ Warning: {error_count} evaluations had errors")
        print("Sample errors:")
        df_new_scores.filter(col("evaluation_error").isNotNull()).select(
            "agent_trace_id",
            "evaluation_error"
        ).show(3, truncate=False)
    
    # Clean up cache
    df_new_scores.unpersist()
elif new_pairs_count == 0:
    print("Skipping evaluation as 'new_pairs_count' is 0.")
else:
    print("Skipping evaluation as 'new_pairs_count' is not defined.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ## Step 8: Overall Statistics and Analysis

print("\n" + "="*80)
print("OVERALL EVALUATION STATISTICS")
print("="*80)

try:
    df_all_scores = spark.read.table(SCORES_TABLE)
    total_evaluated = df_all_scores.count()
    
    print(f"Total Q&A pairs evaluated (all time): {total_evaluated}")
    
    # Re-read new_pairs_count if kernel restarted, or use 0 if no new pairs were evaluated
    try:
        current_run_new_pairs = new_pairs_count
    except NameError:
        current_run_new_pairs = 0 # Assume 0 if variable is lost

    print(f"New pairs evaluated in this run: {current_run_new_pairs}")
    
    # Overall averages
    df_overall_stats = df_all_scores.agg(
        sql_count("*").alias("total_evaluations"),
        avg("intent_resolution").alias("avg_intent_resolution"),
        avg("relevance").alias("avg_relevance"),
        avg("coherence").alias("avg_coherence"),
        avg("fluency").alias("avg_fluency"),
        avg("response_time_ms").alias("avg_response_time_ms")
    )
    
    print("\nOverall averages:")
    df_overall_stats.show()
    
    # Compare first turn vs. follow-up turns
    print("\n" + "="*80)
    print("COMPARISON: First Turn vs. Follow-up Turns")
    print("="*80)
    
    df_all_scores.withColumn(
        "turn_type",
        when(col("turn_number") == 1, "First Turn").otherwise("Follow-up")
    ).groupBy("turn_type").agg(
        sql_count("*").alias("count"),
        avg("intent_resolution").alias("avg_intent_resolution"),
        avg("relevance").alias("avg_relevance"),
        avg("coherence").alias("avg_coherence"),
        avg("fluency").alias("avg_fluency")
    ).show()
    
    # Check for evaluation errors
    total_errors = df_all_scores.filter(col("evaluation_error").isNotNull()).count()
    if total_errors > 0:
        print(f"\n⚠️ Total evaluations with errors: {total_errors}")
    
except Exception as e:
    print(f"Could not load overall statistics: {e}")

print("\n" + "="*80)
print("✓ AZURE AI SDK EVALUATION COMPLETE")
print("="*80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ## Step 9: Advanced Analysis - Score Distribution

try:
    if 'df_all_scores' in locals() and total_evaluated > 0:
        print("\n" + "="*80)
        print("SCORE DISTRIBUTION ANALYSIS")
        print("="*80)
        
        # Score distribution for each metric
        metrics = ["intent_resolution", "relevance", "coherence", "fluency"]
        
        for metric in metrics:
            print(f"\n{metric.upper().replace('_', ' ')} Score Distribution:")
            df_all_scores.filter(col(metric).isNotNull()).groupBy(metric).count().orderBy(metric).show()
    else:
        print("Skipping score distribution analysis as no scores are loaded.")
except Exception as e:
    print(f"Could not run score distribution analysis: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
