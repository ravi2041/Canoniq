from typing import Annotated, Any, Dict, List, Optional
from typing_extensions import TypedDict
from langgraph.graph.message import add_messages
import pandas as pd

class ChartSuggestion(TypedDict):
    chart_type: str
    x_axis: str
    y_axis: str
    group_by: Optional[str]
    title: str
    description: str


class AgentState(TypedDict, total=False):
    # ----------------- CORE CONTEXT -----------------
    question: str
    chat_history: List[dict]
    run_id: Dict[str, Any]
    chain_type: str
    plan: Dict[str, Any]
    target_db: str
    semantics: Dict[str, Any]
    keypoints: Dict[str, Any]
    content: str
    bandit: Dict[str, Any]
    messages: Annotated[List, add_messages]

    # ----------------- SQL PIPELINE -----------------
    sql: str
    result: Dict[str, Any]
    metadata: Dict[str, Any]
    marketing_formulae: Dict[str, Any]
    narrative: str
    summary: Dict[str, Any]
    recommendation: str
    error: str
    mysql_doc: str
    fix_status: str
    unfixable_error: str
    user_friendly_error: Annotated[str, "multiple values allowed"]
    keywords: Annotated[List[str], "multiple values allowed"] = []
    chart_suggestions: Annotated[List[ChartSuggestion], "list of chart config dicts"] = []

    # ----------------- MEMORY -----------------
    short_term_memory: List[dict] = []    # recent queries + results
    long_term_memory: List[dict] = []     # persistent heuristics/rules
    semantic_memory: List[dict] = []      # embeddings or knowledge summaries

    # ----------------- AI-DRIVEN DATA QUALITY -----------------
    dq_dimensions: Dict[str, List[str]]            # collected dimension values
    dq_ai_findings: Dict[str, Any]                 # AI anomaly results
    dq_last_checked: str                           # timestamp of last check
    dq_feedback: Dict[str, Any]                    # user feedback
    dq_pattern_memory: Dict[str, Any]              # stored learned pattern memory
    dq_memory_updated_at: str                      # timestamp of memory update
    dq_memory_summary: str                         # summary of memory change
    dq_final_summary: str                          # final report text for display/export
    dq_ingestion_anomalies: str
    dq_last_summarized_at:str
    dq_metadata:str