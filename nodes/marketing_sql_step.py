from core.state import AgentState
from nodes.generate_sql_step import generate_sql_step


def marketing_sql_step(state: AgentState) -> dict:
    state["active_chain"] = "marketing"
    state["target_db"] = "marketing"
    return generate_sql_step(state)