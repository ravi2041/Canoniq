import json
import numpy as np
import pandas as pd
from helper_fucntions.helper_functions import create_dataframe, save_to_db_memory
from chains.get_narrative_chain import run_narrative_with_chunking
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from helper_fucntions.recommendation_result_format import format_recommendations


embeddings = OpenAIEmbeddings(model="text-embedding-3-small")


# -------------------------------------------------
# Helper: safe markdown conversion
# -------------------------------------------------
def safe_to_markdown(data):
    """Safely convert DataFrame, list, or string into markdown table text."""
    import pandas as pd

    if data is None:
        return ""
    if isinstance(data, pd.DataFrame):
        try:
            return data.to_markdown(index=False)
        except Exception:
            return data.to_string(index=False)
    if isinstance(data, list):
        try:
            df = pd.DataFrame(data)
            return df.to_markdown(index=False)
        except Exception:
            return str(data)
    return str(data)


# -------------------------------------------------
# Keyword Extraction
# -------------------------------------------------
def extract_keywords(state: dict) -> dict:
    question = state.get("question", "")
    sql = state.get("sql", "")

    prompt = f"""
    Extract 5-10 important keywords from the following question and SQL query.
    Question: {question}
    SQL: {sql}
    Return them as a valid Python list of strings (e.g., ["facebook", "campaigns", "performance"]).
    """

    keyword_model = ChatOpenAI(model="o4-mini-2025-04-16", temperature=1)
    response = keyword_model.invoke(prompt)
    raw_text = response.content.strip()

    try:
        keywords = eval(raw_text)
        if not isinstance(keywords, list):
            keywords = [str(raw_text)]
    except Exception:
        keywords = [str(raw_text)]

    return {"keywords": keywords}


# -------------------------------------------------
# Memory management
# -------------------------------------------------
def update_memory(state: dict) -> dict:
    """Update short-term and long-term memory with the latest query and insights."""
    if "short_term_memory" not in state:
        state["short_term_memory"] = []
    if "long_term_memory" not in state:
        state["long_term_memory"] = []

    entry = {
        "question": state.get("question", ""),
        "sql": state.get("sql", ""),
        "narrative": state.get("narrative", ""),
        "keywords": state.get("keywords", []),
    }
    state["short_term_memory"].append(entry)

    # Keep only last 20
    state["short_term_memory"] = state["short_term_memory"][-20:]

    # Example heuristic: add to long-term if marked important
    if "important" in state.get("question", "").lower():
        state["long_term_memory"].append(entry)

    return state


# -------------------------------------------------
# Summarization Step
# -------------------------------------------------
def summarize_step(state: dict):
    """
    Generates narrative + recommendations + memory update + embeddings.
    Safe for any SQL result type (string, list, or dataframe).
    """
    # Handle unfixable error
    if not state.get("result") and state.get("unfixable_error"):
        state["narrative"] = state.get("user_friendly_error")
        return state

    # Ensure keywords exist
    if not state.get("keywords"):
        extracted = extract_keywords(state)
        state["keywords"] = extracted.get("keywords", [])

    # Convert SQL result → DataFrame (robustly)
    rows = create_dataframe(state.get("result"))
    total_output = safe_to_markdown(rows)

    # --- Narrative + Recommendation ---
    summary = run_narrative_with_chunking(
        question=state["question"],
        result_table=total_output,
        model="o4-mini-2025-04-16",
    )

    # Narrative text
    state["narrative"] = summary.get("summary") if isinstance(summary, dict) else str(summary)

    # --- Recommendations (robust) ---
    recs = []
    if isinstance(summary, dict) and "recommendation" in summary:
        try:
            recs = format_recommendations(summary["recommendation"])
        except Exception:
            recs = []
    state["recommendation"] = recs

    # --- Summary block (structured) ---
    if isinstance(summary, dict) and "summary" in summary:
        s = summary["summary"]
        if isinstance(s, str):
            state["summary"] = {"key_findings": [s]}
        elif isinstance(s, dict):
            state["summary"] = s
        else:
            state["summary"] = {"key_findings": [str(s)]}
    else:
        state["summary"] = {"key_findings": [state.get("narrative", "")]}

    # --- Update short-term memory ---
    state = update_memory(state)

    # --- Compute embeddings ---
    text_for_embedding = f"{state['question']} {state.get('narrative','')}"
    embedding_vector = embeddings.embed_query(text_for_embedding)

    # --- Optional: Persist to DB (safe json dump) ---
    try:
        save_to_db_memory(
            question=state["question"],
            result={"embedding": np.array(embedding_vector, dtype=np.float32).tobytes()},
            narrative=state.get("narrative", ""),
            sql_code=state.get("sql", ""),
            keywords=json.dumps(state.get("keywords", [])),
            user_id="global",
        )
    except Exception as e:
        print(f"[WARN] Could not save memory: {e}")

    return state
