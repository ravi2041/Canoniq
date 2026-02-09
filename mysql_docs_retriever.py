# mysql_docs_retriever.py
import os
from typing import List
from openai import OpenAI
from pinecone import Pinecone
from dotenv import load_dotenv
load_dotenv()

EMBED_MODEL = "text-embedding-3-small"

oaiclient = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
index = pc.Index(os.getenv("MYSQL_DOCS_INDEX", "mysql-84-docs"))

def embed(q: str) -> list[float]:
    resp = oaiclient.embeddings.create(model=EMBED_MODEL, input=[q])
    return resp.data[0].embedding

def get_docs_for_error(error_text: str, top_k: int = 2, min_score: float = 0.8) -> List[dict]:
    """Retrieve docs for an error. Only include docs with score >= min_score."""
    vec = embed(error_text)
    res = index.query(vector=vec, top_k=top_k, include_metadata=True)
    out = []
    for m in res.matches:
        if m.score is not None and m.score >= min_score:
            out.append({
                "title": m.metadata.get("title"),
                "url": m.metadata.get("url"),
                "snippet": m.metadata.get("snippet"),
                "score": m.score
            })
    # If no doc meets threshold, return empty list
    return out

def format_docs_citations(docs: List[dict]) -> str:
    """Format docs into text snippets for passing into LLM."""
    if not docs:
        return "No high-confidence docs found (score < 0.8)."
    lines = []
    for d in docs:
        snippet = d.get("snippet", "")
        score = d.get("score", 0)
        # ✅ include score in formatted text (optional)
        lines.append(f"[Score {score:.2f}] {snippet}")
    return "\n".join(lines)


# if __name__ == "__main__":
#     res = get_docs_for_error("ERROR 1146 (42S02): Table 'test.no_such_table' doesn't exist")
#     for i in res:
#         print(f"{i["score"] }: {i["snippet"]}")
