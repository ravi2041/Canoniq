# mysql_docs_rag.py
import os
import re
import time
import json
import hashlib
import tldextract
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from typing import List, Dict, Iterable, Tuple
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
load_dotenv()
from openai import OpenAI
from pinecone import Pinecone, ServerlessSpec
import PyPDF2

# =========================
# Config
# =========================
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")

# Choose ONE embedding model and matching dimension
EMBED_MODEL = "text-embedding-3-small"  # 1536-dim
DIMENSION = 1536

INDEX_NAME = "mysql-84-docs"            # Pinecone index name
CLOUD = "aws"
REGION = "us-east-1"

# Seed URLs (you can add the full reference manual later)
SEED_URLS = [
    #"https://dev.mysql.com/doc/relnotes/mysql/8.4/en/",
    # Optional, richer reference content (recommended):
    "https://dev.mysql.com/doc/refman/8.4/en/error-message-elements.html",
     "https://dev.mysql.com/doc/refman/8.4/en/error-interfaces.html",
    # "https://dev.mysql.com/doc/refman/8.4/en/mathematical-functions.html",
    # "https://dev.mysql.com/doc/refman/8.4/en/string-functions.html",
    # "https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html",
]

MAX_PAGES = 200         # crawl budget
TIMEOUT = 20            # seconds
CHUNK_SIZE = 1000       # characters
CHUNK_OVERLAP = 200     # characters
BATCH_SIZE = 64         # embedding + upsert batch

ALLOWED_NETLOC = urlparse(SEED_URLS[0]).netloc
ALLOWED_REGISTERED_DOMAIN = ".".join(
    [tldextract.extract(SEED_URLS[0]).domain, tldextract.extract(SEED_URLS[0]).suffix]
)

HEADERS = {
    "User-Agent": "DocsRAGBot/1.0 (+https://example.com)"
}

# =========================
# Clients
# =========================
oaiclient = OpenAI(api_key=OPENAI_API_KEY)
pc = Pinecone(api_key=PINECONE_API_KEY)

def get_or_create_index(name: str, dim: int):
    existing = [i["name"] for i in pc.list_indexes()]
    if name not in existing:
        pc.create_index(
            name=name,
            dimension=dim,
            metric="cosine",
            spec=ServerlessSpec(cloud=CLOUD, region=REGION),
        )
    return pc.Index(name)

index = get_or_create_index(INDEX_NAME, DIMENSION)

# # =========================
# # Utilities
# # =========================
def clean_text(txt: str) -> str:
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt

# def is_same_site(url: str) -> bool:
#     parsed = urlparse(url)
#     if not parsed.netloc:
#         return True  # relative link
#     # stay within same registered domain (dev.mysql.com)
#     ext = tldextract.extract(parsed.netloc)
#     registered = f"{ext.domain}.{ext.suffix}"
#     return registered == ALLOWED_REGISTERED_DOMAIN
#
# def absolutize(base: str, href: str) -> str:
#     return urljoin(base, href)

# def extract_links_and_text(url: str) -> Tuple[str, List[str]]:
#     """Return (clean_text_of_page, absolute_links[])"""
#     resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
#     resp.raise_for_status()
#     soup = BeautifulSoup(resp.text, "html.parser")
#
#     # Remove non-content
#     for tag in soup(["script", "style", "nav", "header", "footer"]):
#         tag.decompose()
#
#     # Extract title
#     title = soup.title.get_text(strip=True) if soup.title else ""
#
#     # Extract main text
#     text = soup.get_text(separator="\n")
#     text = f"{title}\n\n{text}"
#     text = clean_text(text)
#
#     # Extract links
#     links = []
#     for a in soup.find_all("a", href=True):
#         href = a["href"]
#         abs_url = absolutize(url, href)
#         if is_same_site(abs_url):
#             links.append(abs_url)
#
#     # Dedup links
#     links = list(dict.fromkeys(links))
#     return text, links

def chunk_text(text: str, size: int, overlap: int) -> List[str]:
    chunks = []
    start = 0
    while start < len(text):
        end = min(start + size, len(text))
        chunks.append(text[start:end])
        if end == len(text):
            break
        start = max(0, end - overlap)
    return chunks

def deterministic_id(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:40]

@retry(wait=wait_exponential(multiplier=1, min=1, max=20), stop=stop_after_attempt(6))
def embed_batch(texts: List[str]) -> List[List[float]]:
    resp = oaiclient.embeddings.create(model=EMBED_MODEL, input=texts)
    return [d.embedding for d in resp.data]

def batched(iterable: List, n: int) -> Iterable[List]:
    for i in range(0, len(iterable), n):
        yield iterable[i:i+n]


# =========================
# PDF Upload → Chunk → Upsert
# =========================
def upload_pdf_pages_to_pinecone(pdf_path: str, page_ranges: List[Tuple[int, int]]):
    """
    Upload selected pages from a PDF to Pinecone.

    Args:
        pdf_path: Path to PDF file
        page_ranges: List of tuples [(start1, end1), (start2, end2)] inclusive, 1-indexed
    """
    # Step 1: Read selected pages
    pdf_text = []
    with open(pdf_path, "rb") as f:
        reader = PyPDF2.PdfReader(f)
        total_pages = len(reader.pages)
        print(f"PDF has {total_pages} pages")

        for start, end in page_ranges:
            # Clamp the ranges within available pages
            start = max(1, start)
            end = min(end, total_pages)
            for page_num in range(start, end + 1):
                page = reader.pages[page_num - 1]  # PyPDF2 is 0-indexed
                text = page.extract_text()
                if text:
                    pdf_text.append((page_num, text))

    # Step 2: Chunk pages and prepare vectors
    vectors = []
    for page_num, text in pdf_text:
        chunks = chunk_text(text, CHUNK_SIZE, CHUNK_OVERLAP)
        for idx, chunk in enumerate(chunks):
            rec_id = deterministic_id(f"{pdf_path}::page{page_num}::chunk{idx}:::{chunk[:64]}")
            vectors.append({
                "id": rec_id,
                "text": chunk,
                "metadata": {
                    "source": os.path.basename(pdf_path),
                    "page": page_num,
                    "title": f"{os.path.basename(pdf_path)} page {page_num}",
                    "snippet": chunk[:1000]
                }
            })

    # Step 3: Embed and upsert in batches with progress
    total_vectors = len(vectors)
    processed = 0
    for i, chunk in enumerate(batched(vectors, BATCH_SIZE), 1):
        texts = [v["text"] for v in chunk]
        embs = embed_batch(texts)
        payload = []
        for v, e in zip(chunk, embs):
            payload.append({
                "id": v["id"],
                "values": e,
                "metadata": v["metadata"]
            })
        index.upsert(vectors=payload)
        processed += len(chunk)
        print(
            f"Batch {i}: upserted {len(chunk)} vectors | Total processed: {processed}/{total_vectors} ({processed / total_vectors * 100:.2f}%)")

    print(f"✅ Uploaded {total_vectors} chunks from PDF '{pdf_path}' to Pinecone")

# =========================
# Retrieval for error messages
# =========================
def retrieve_mysql_help(query: str, top_k: int = 5) -> List[Dict]:
    emb = embed_batch([query])[0]
    res = index.query(vector=emb, top_k=top_k, include_metadata=True)
    # Pinecone new SDK returns an object with .matches
    matches = []
    for m in res.matches:
        matches.append({
            "id": m.id,
            "score": m.score,
            "url": m.metadata.get("url"),
            "title": m.metadata.get("title"),
            "text": m.metadata.get("snippet")  # store snippets if needed in metadata
        })
    return matches

def retrieve_text_snippets(query: str, top_k: int = 5) -> List[str]:
    # If you want actual text in metadata, store it in metadata at upsert time (shorten!)
    # Alternatively, store short "snippet" fields instead of the full chunk.
    emb = embed_batch([query])[0]
    res = index.query(vector=emb, top_k=top_k, include_metadata=True)
    snippets = []
    for i, m in enumerate(res.matches, 1):
        title = m.metadata.get("title", "MySQL 8.4 Doc")
        url = m.metadata.get("url", "")
        snippet = m.metadata.get("snippet", "")
        formatted = f"[Doc {i}] {title}\nSource: {url}\nSnippet:\n{snippet}\n"
        snippets.append(formatted)
    return snippets

if __name__ == "__main__":
    # Example: upload pages 1-5 and 10-12 from a PDF
    pdf_file = "./mysql_doc/mysql-errors-8.0-en.a4.pdf"
    page_ranges = [(1, 2)]
    upload_pdf_pages_to_pinecone(pdf_file, page_ranges)