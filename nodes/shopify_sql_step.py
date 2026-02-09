from core.state import AgentState
from nodes.generate_sql_step import generate_sql_step
from log_files.observability import app_log

def shopify_sql_step(state: AgentState) -> dict:
    """
    Shopify SQL generation step. Sets active_chain and target_db to 'shopify',
    then delegates to generate_sql_step.
    """
    run_id = state.get("run_id")
    state["active_chain"] = "shopify"
    state["target_db"] = "shopify"

    app_log("shopify_sql_step_invoked", run_id=run_id, question=state.get("question", ""))

    return generate_sql_step(state)
