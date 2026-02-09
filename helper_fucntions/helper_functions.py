from typing import Annotated
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import json
import mysql.connector
import hashlib
from langchain_openai import OpenAIEmbeddings
from decimal import Decimal
load_dotenv()
from config import MYSQL_MEMORY_CONFIG
from config import BASE_MYSQL_CONFIG
import re
import tiktoken


# -------------------------
# Helper Functions
# -------------------------

embeddings = OpenAIEmbeddings(model="text-embedding-3-small")

def load_metadata(filepath="./metadata.json"):
    with open(filepath, "r") as f:
        return json.load(f)

def load_shopify_metadata(filepath="./shopify_metadata.json"):
    with open(filepath, "r") as f:
        return json.load(f)

def load_marketing_formulae(filepath="./marketing_formulae.json"):
    with open(filepath, "r") as f:
        return json.load(f)

# counting tokens size
def count_tokens(text: str, model: str = "gpt-4o") -> int:
    """Count tokens for a given text. Fallback to cl100k_base if model not recognized."""
    try:
        enc = tiktoken.encoding_for_model(model)
    except KeyError:
        enc = tiktoken.get_encoding("cl100k_base")
    return len(enc.encode(text))


def chunk_text(text: str, model: str = "gpt-4o", max_tokens: int = None, overlap: int = 50):
    """
    Split text into chunks that fit into the model's context window.
    Fallback to cl100k_base if tokenizer not found for model.
    """
    try:
        enc = tiktoken.encoding_for_model(model)
    except KeyError:
        enc = tiktoken.get_encoding("cl100k_base")

    tokens = enc.encode(text)

    # Default to model max context
    if max_tokens is None:
        model_limits = {
            "o4-mini-2025-04-16": 100000,
            "gpt-4o": 128000,
            "gpt-4o-mini": 128000,
            "gpt-3.5-turbo": 16385,
        }
        max_tokens = model_limits.get(model, 4000)

    # Leave buffer for model output
    chunk_size = max_tokens - 2000

    chunks = []
    for i in range(0, len(tokens), chunk_size - overlap):
        chunk = tokens[i:i + chunk_size]
        chunks.append(enc.decode(chunk))

    return chunks



def clean_sql_code(sql: str) -> str:
    """
    Removes markdown-style triple backticks and 'sql' language tags from LLM output.
    """
    if not sql:
        return ""

    # Remove any ```sql or ``` and trim
    return re.sub(r"```(?:sql)?\n?", "", sql).strip()


def sanitize_for_json(obj):
    """Recursively replace non-JSON-serializable objects."""
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    elif isinstance(obj, np.ndarray):
        return "NDARRAY_EMBEDDING"  # placeholder
    return obj



# Helper to retrieve relevant memory
def get_relevant_memory(state: dict):
    """
    Return relevant heuristics or previous queries for current question.

    - Looks at the last 10 short-term memory entries for keyword overlap.
    - Always adds long-term memory (if available).
    - Handles cases where memory keys may not exist.
    """
    relevant = []
    question = state.get("question", "").lower()

    # Short-term memory: look for past questions with overlapping keywords
    short_term = state.get("short_term_memory", [])

    for past in short_term[-10:]:  # last 10 queries
        keywords = past.get("keywords", [])
        if any(k.lower() in question for k in keywords):
            relevant.append(past)

    # Long-term memory: always available
    long_term = state.get("long_term_memory", [])
    relevant.extend(long_term)

    return relevant

# ---------- Hashing ----------
def hash_question(question: str) -> str:
    return hashlib.sha256(question.strip().encode()).hexdigest()



def embed_text(text: str):
    """Generate embedding vector for semantic search."""
    return np.array(embeddings.embed_query(text), dtype=np.float32)


# ---------- Long-Term Memory ----------
def save_to_db_memory(question: str, result: dict, narrative: str, sql_code: str, keywords: list, data_source='marketing', user_id='global'):
    question_hash = hash_question(question)
    embedding = result.get("embedding")  # can be None or bytes
    keywords_json = json.dumps(keywords or [])
    narrative_text = narrative or ""

    conn = mysql.connector.connect(**MYSQL_MEMORY_CONFIG)
    cursor = conn.cursor()

    # Auto-create table fallback (optional safety)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agent_memory (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id VARCHAR(100),
            question TEXT,
            sql_code TEXT,
            keywords JSON,
            embedding BLOB,
            question_hash VARCHAR(64) UNIQUE,
            narrative TEXT,
            data_source VARCHAR(50) DEFAULT 'marketing',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        INSERT INTO agent_memory (user_id, question, sql_code, keywords, embedding, question_hash, narrative, data_source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            sql_code=%s,
            keywords=%s,
            embedding=%s,
            narrative=%s,
            data_source=%s,
            updated_at=CURRENT_TIMESTAMP
    """, (
        user_id,
        question or "",
        sql_code or "",
        keywords_json,
        embedding,
        question_hash,
        narrative_text,
        data_source,
        # ON DUPLICATE
        sql_code or "",
        keywords_json,
        embedding,
        narrative_text,
        data_source
    ))

    conn.commit()
    cursor.close()
    conn.close()


def load_from_db_memory(question: str, user_id='global', data_source='marketing', top_k=5):
    question_hash = hash_question(question)
    conn = mysql.connector.connect(**MYSQL_MEMORY_CONFIG)
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT question, sql_code, narrative, keywords, embedding
        FROM agent_memory
        WHERE user_id=%s AND data_source=%s
    """, (user_id, data_source))

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    memories = []
    for row in rows:
        emb_bytes = row.get("embedding")
        if emb_bytes is None:
            continue
        emb = np.frombuffer(emb_bytes, dtype=np.float32).reshape(1, -1)
        memories.append({
            "question": row["question"],
            "sql": row["sql_code"],
            "narrative": row["narrative"],
            "keywords": json.loads(row["keywords"] or "[]"),
            "embedding": emb
        })

    return memories[:top_k]
def format_result(columns, rows):
    formatted_rows = []
    for row in rows:
        formatted_rows.append([
            float(v) if isinstance(v, Decimal) else ("N/A" if v is None else v)
            for v in row
        ])
    return {"columns": columns, "rows": formatted_rows}


def create_dataframe(result):
    if not result or not result.get("rows"):
        return "⚠️ No results returned from SQL."
    df = pd.DataFrame(result["rows"], columns=result["columns"])
    return df



def get_db_config(database_name: str):
    config = BASE_MYSQL_CONFIG.copy()
    config["database"] = database_name
    return config


def validate_sql(query: str, database: str) -> bool:
    try:
        db_config = get_db_config(database)
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("EXPLAIN " + query)
        cursor.fetchall()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print("SQL validation failed:", e)
        return False


