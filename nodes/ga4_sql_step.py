from core.state import AgentState
from nodes.generate_sql_step import generate_sql_step
from app.observability.observability import app_log

def ga4_sql_step(state: AgentState) -> dict:
    """
    Shopify SQL generation step. Sets active_chain and target_db to 'shopify',
    then delegates to generate_sql_step.
    """
    run_id = state.get("run_id")
    state["active_chain"] = "ga4"
    state["target_db"] = "shopify"

    app_log("ga4_sql_step_invoked", run_id=run_id, question=state.get("question", ""))

    return generate_sql_step(state)
