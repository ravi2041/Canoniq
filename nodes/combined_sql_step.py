from core.state import AgentState
from nodes.generate_sql_step import generate_sql_step
from app.observability.observability import app_log

def combined_sql_step(state: AgentState) -> dict:
    """
    SQL generation step. Sets active_chain to combined and target_db to 'shopify',
    then delegates to generate_sql_step.
    """
    run_id = state.get("run_id")
    state["active_chain"] = "combined"
    state["target_db"] = "shopify"

    app_log("combined_sql_step_invoked", run_id=run_id, question=state.get("question", ""))

    return generate_sql_step(state)