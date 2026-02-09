# app/main.py

from typing import Annotated, List, Dict, Any
from dotenv import load_dotenv
import mysql.connector
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langgraph.graph import StateGraph, END
from langchain_core.agents import AgentFinish
from langgraph.types import interrupt, Command
from sklearn.metrics.pairwise import cosine_similarity

# --- Imports from your project ---
from helper_fucntions.helper_functions import (
    load_metadata,
    load_shopify_metadata,
    load_from_db_memory,
)
from core.state import AgentState
from log_files.observability import new_run_id, app_log, save_json_artifact

# --- Existing Node Imports ---
from nodes.keypoints_step import keypoints_step
from nodes.supervisor_router_step import supervisor_router_step
from nodes.marketing_sql_step import marketing_sql_step
from nodes.shopify_sql_step import shopify_sql_step
from nodes.ga4_sql_step import ga4_sql_step
from nodes.combined_sql_step import combined_sql_step
from nodes.validate_step import validate_step
from nodes.fix_sql_step import fix_sql_step
from nodes.execute_sql_step import execute_sql_step
from nodes.human_checkpoint_step import human_checkpoint_node
from nodes.summarize_step import summarize_step
from nodes.graph_suggesstion_step import graph_suggestion_step
from nodes.report_error_step import report_error_step
from nodes.generate_critic_step import critic_sql_step

# --- New Import for DQ ---
from chains.data_quality_chain import data_quality_chain

# --- Conditional edge logic ---
from nodes.conditional_edge.should_continue_after_execute import (
    should_continue_after_execute,
    should_continue,
)

load_dotenv()
embeddings = OpenAIEmbeddings(model="text-embedding-3-small")

# ---------------------------------------------------------------------
# Router logic: decide which SQL or DQ chain to use
# ---------------------------------------------------------------------
def which_chain_router(state: AgentState) -> str:
    chain_type = state.get("chain_type", "marketing")
    if chain_type == "shopify":
        return "generate_sql_shopify"
    elif chain_type == "ga4":
        return "generate_sql_ga4"
    elif chain_type == "combined":
        return "generate_sql_combined"
    elif chain_type == "data_quality":  # ✅ Added
        return "collect_dq_dimensions_step"  # entry point of DQ chain
    else:
        return "generate_sql_marketing"


# ---------------------------------------------------------------------
# Graph Definition
# ---------------------------------------------------------------------
graph = StateGraph(AgentState)

# --- Core NLQ Nodes ---
graph.add_node("keypoints", keypoints_step)
graph.add_node("supervisor_router", supervisor_router_step)
graph.add_node("generate_sql_marketing", marketing_sql_step)
graph.add_node("generate_sql_shopify", shopify_sql_step)
graph.add_node("generate_sql_ga4", ga4_sql_step)
graph.add_node("generate_sql_combined", combined_sql_step)
graph.add_node("validate_sql", validate_step)
graph.add_node("fix_sql", fix_sql_step)
graph.add_node("execute_sql", execute_sql_step)
graph.add_node("human_checkpoint", human_checkpoint_node)
graph.add_node("summarize", summarize_step)
graph.add_node("graph_suggestion", graph_suggestion_step)
graph.add_node("report_error", report_error_step)

# ✅ Integrate Data Quality Chain Nodes
data_quality_chain(graph)

# ---------------------------------------------------------------------
# Edges
# ---------------------------------------------------------------------
graph.set_entry_point("keypoints")
graph.add_edge("keypoints", "supervisor_router")
graph.add_conditional_edges("supervisor_router", which_chain_router)

# --- SQL & Report Generation path ---
graph.add_edge("generate_sql_marketing", "validate_sql")
graph.add_edge("generate_sql_shopify", "validate_sql")
graph.add_edge("generate_sql_ga4", "validate_sql")
graph.add_edge("generate_sql_combined", "validate_sql")

graph.add_conditional_edges("validate_sql", should_continue)
graph.add_edge("fix_sql", "validate_sql")
graph.add_edge("validate_sql", "execute_sql")
graph.add_conditional_edges("execute_sql", should_continue_after_execute)
graph.add_edge("human_checkpoint", "summarize")
graph.add_edge("execute_sql", "summarize")
graph.add_edge("summarize", "graph_suggestion")
graph.add_edge("graph_suggestion", END)
graph.add_edge("report_error", END)

# Compile full graph
agent_executor = graph.compile()


# ---------------------------------------------------------------------
# Main Entry Function
# ---------------------------------------------------------------------
def run_langgraph_agent(
    question: str,
    user_id: str = "global",
    chat_history: list | None = None,
    chain_type: str | None = None,   # e.g. "data_quality"
    dq_feedback: dict | None = None, # 👈 NEW
):
    """
    Run the LangGraph agent (NLQ or Data Quality, depending on chain_type).
    """
    if not isinstance(chat_history, list):
        chat_history = []

    run_id = new_run_id("agent")

    # base state
    state = AgentState(
        question=question,
        metadata={
            "marketing": load_metadata(),
            "shopify": load_shopify_metadata(),
        },
        chat_history=chat_history,
        run_id=run_id,
    )

    # tell router which branch to take
    if chain_type:
        state["chain_type"] = chain_type

    # 👇 pass user validations / corrections to the graph
    if dq_feedback:
        # the DQ nodes (ai_update_dq_memory_step) can read this
        state["dq_feedback"] = dq_feedback

    # logging
    app_log("agent_start", run_id=run_id, question=question, user_id=user_id)
    save_json_artifact(
        run_id,
        "input_state",
        {
            "question": question,
            "user_id": user_id,
            "chat_history_len": len(chat_history),
            "chain_type": chain_type,
        },
    )

    # load long-term memory (same as before)
    db_memories = load_from_db_memory(question, user_id=user_id)
    if db_memories:
        state["long_term_memory"] = db_memories

    # run graph
    final = agent_executor.invoke(state)

    # save artifact
    save_json_artifact(
        run_id,
        "final_state_summary",
        {
            "sql": final.get("sql"),
            "error": final.get("error"),
            "fix_status": final.get("fix_status"),
            "has_result": bool(final.get("result")),
            "has_narrative": bool(final.get("narrative")),
            "keypoints_present": bool(final.get("keypoints")),
        },
    )
    app_log("agent_end", run_id=run_id, status="ok" if not final.get("error") else "error")

    # choose assistant msg
    assistant_msg = (
        final.get("summary")
        or final.get("narrative")
        or final.get("user_friendly_error")
        or "I generated the analysis."
    )

    updated_chat_history = chat_history + [
        {"role": "user", "content": question},
        {"role": "assistant", "content": assistant_msg},
    ]

    # return with DQ fields so Streamlit can render
    return {
        "sql": final.get("sql"),
        "result": final.get("result"),
        "narrative": final.get("narrative"),
        "summary": final.get("summary"),
        "recommendation": final.get("recommendation"),
        "keypoints": final.get("keypoints"),
        "error": final.get("error"),
        "fix_status": final.get("fix_status"),
        "mysql_doc": final.get("mysql_doc"),
        "user_friendly_error": final.get("user_friendly_error"),
        "used_memory": db_memories,
        "chart_suggestions": final.get("chart_suggestions"),
        "chat_history": updated_chat_history,

        # 👇 DQ stuff
        "dq_dimensions": final.get("dq_dimensions"),
        "dq_ai_findings": final.get("dq_ai_findings"),
        "dq_final_summary": final.get("dq_final_summary"),
        "dq_pattern_memory": final.get("dq_pattern_memory"),
        "dq_ingestion_anomalies": final.get("dq_ingestion_anomalies"),
    }

# ---------------------------------------------------------------------
# Example Usage
# ---------------------------------------------------------------------
if __name__ == "__main__":
    # # Example 1: Regular NLQ query
    # output = run_langgraph_agent(
    #     "compare campaign performance across all platforms. Include metrics that you think are useful for comparison."
    # )
    # print("OUTPUT (NLQ):\n", output)

    # Example 2: Run Data Quality Check
    dq_output = run_langgraph_agent(
        "check data quality for all campaign, placement and creative names.",
        chain_type="data_quality",
    )
    print("\nOUTPUT (DATA QUALITY):\n", dq_output)
