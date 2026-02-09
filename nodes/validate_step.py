from typing import Dict, Any
from helper_fucntions.helper_functions import validate_sql
from app.observability.observability import (
    app_log,
    save_text_artifact,
    time_block,
)

def validate_step(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate the SQL query against the selected database.
    Logs success/failure and stores artifacts.
    Updates state with error (if any) and sql_valid flag.
    """
    run_id = state.get("run_id")
    database = state.get("target_db")  # Expected to be set via routing/supervisor
    sql_query = state.get("sql", "").strip()

    if not database or not sql_query:
        msg = "Missing database or SQL query for validation."
        if run_id:
            save_text_artifact(run_id, "validate_sql_error", msg)
        app_log("validate_sql_missing_input", run_id=run_id, database=database)
        state["error"] = msg
        state["sql_valid"] = False
        return state

    with time_block("validate_sql", run_id, {"sql_len": len(sql_query)}):
        try:
            is_valid = validate_sql(sql_query, database)
        except Exception as e:
            msg = f"Validation exception: {type(e).__name__}: {str(e)[:800]}"
            if run_id:
                save_text_artifact(run_id, "validate_sql_exception", msg)
            app_log("validate_sql_exception", run_id=run_id, message=msg)
            state["error"] = msg
            state["sql_valid"] = False
            return state

        if not is_valid:
            err_msg = "SQL validation failed"
            if run_id:
                save_text_artifact(run_id, "validate_sql_error", err_msg)
            app_log("validate_sql_failed", run_id=run_id)
            state["error"] = err_msg
            state["sql_valid"] = False
        else:
            app_log("validate_sql_success", run_id=run_id)
            state["error"] = ""
            state["sql_valid"] = True

    return state
