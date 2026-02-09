import os
import json
from typing import List, Dict, Any
from pinecone import Pinecone, ServerlessSpec
from openai import OpenAI
from dotenv import load_dotenv
load_dotenv()

# create index in pinecone. With integrated models, you upsert and search with text and have Pinecone generate vectors automatically.

# ==== CONFIG ====
INDEX_NAME = "metadata-schema-index"   # name for Pinecone index
JSON_FILE = "metadata.json"     # your document file

# ==== INIT CLIENTS ====
client = OpenAI()
pc = Pinecone()

# ==== CREATE INDEX IF NOT EXISTS ====
if INDEX_NAME not in pc.list_indexes().names():
    pc.create_index(
        name=INDEX_NAME,
        dimension=1536,   # dimension of text-embedding-3-small
        metric="cosine",
        spec=ServerlessSpec(cloud="aws", region="us-east-1")
    )

index = pc.Index(INDEX_NAME)


# ==== FUNCTION: embed text ====
def embed_texts(texts):
    response = client.embeddings.create(
        input=texts,
        model="text-embedding-3-small"
    )
    return [d.embedding for d in response.data]


# ==== FUNCTION: transform schema into records ====
def schema_to_records(schema):
    records = []
    for table in schema["tables"]:
        text = f"Table: {table['name']}\nColumns: {', '.join(table['columns'])}"
        record = {
            "id": table["name"],
            "text": text,
            "metadata": {
                "table": table["name"],
                "num_columns": len(table["columns"])
            }
        }
        records.append(record)
    return records


# ==== MAIN PIPELINE ====
def upload_schema_to_pinecone():
    # 1. Load schema JSON
    with open(JSON_FILE, "r") as f:
        schema = json.load(f)

    # 2. Convert to records
    records = schema_to_records(schema)

    # 3. Embed and upsert
    vectors = []
    texts = [r["text"] for r in records]
    embeddings = embed_texts(texts)

    for r, vec in zip(records, embeddings):
        vectors.append({
            "id": r["id"],
            "values": vec,
            "metadata": r["metadata"]
        })

    index.upsert(vectors=vectors)
    print(f"✅ Uploaded {len(vectors)} tables into Pinecone index '{INDEX_NAME}'")


if __name__ == "__main__":
    upload_schema_to_pinecone()
