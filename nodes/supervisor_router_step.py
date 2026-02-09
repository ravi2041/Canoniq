# nodes/supervisor_router_step.py
from chains.router_chain import router_chain
from core.state import AgentState
import json

# Map chain type → database
CHAIN_DB_MAP = {
    "marketing": "marketing",
    "shopify": "shopify",
    "ga4": "shopify",
    "combined": "shopify",
    "data_quality": "marketing",  # use marketing DB by default for DQ checks
}

def supervisor_router_step(state: AgentState) -> dict:
    """
    Classify the user question and store routing info in state.
    Decides whether to use marketing/shopify/ga4/combined/data_quality path.
    """
    question = state.get("question", "")
    metadata = state.get("metadata", {})

    if not question or not metadata:
        return {"error": "Missing question or metadata for routing."}

    # --- Heuristic short-circuit for Data Quality ---
    # if user explicitly asks for "data quality", "anomaly", "naming check", etc.
    dq_keywords = ["data quality", "anomaly", "naming", "schema check", "validation", "integrity"]
    if any(k in question.lower() for k in dq_keywords):
        chain_type = "data_quality"
        target_db = CHAIN_DB_MAP.get(chain_type, "marketing")
        print(f"🔍 Router override → chain_type: {chain_type}, target_db: {target_db}")
        return {"chain_type": chain_type, "target_db": target_db}

    # --- Otherwise, use LLM router classification ---
    router_result = router_chain.invoke({
        "question": question,
        "metadata": json.dumps(metadata, indent=2),
    })

    chain_type = router_result.get("chain_type", "marketing")
    target_db = CHAIN_DB_MAP.get(chain_type, "marketing")

    print(f"🔍 chain_type: {chain_type}")
    print(f"🗄️ target_db: {target_db}")

    return {"chain_type": chain_type, "target_db": target_db}
