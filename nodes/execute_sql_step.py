from typing import Dict, Any
from log_files.observability import time_block, app_log, save_text_artifact, save_json_artifact
from helper_fucntions.sql_runner import run_sql_on_mysql
from helper_fucntions.helper_functions import format_result

MAX_SAFE_ROWS = 10_000
RESULT_SAMPLE_ROWS = 5  # how many rows to store in artifacts for inspection


def execute_sql_step(state: Dict[str, Any]) -> Dict[str, Any]:
    if state.get("unfixable_error"):
        return state

    run_id = state.get("run_id")
    sql = (state.get("sql") or "").strip()
    database = state.get("target_db","shopify")
    if not sql:
        return {"error": "No SQL to execute"}

    # save the sql we are about to run
    if run_id:
        save_text_artifact(run_id, "executed_sql", sql, suffix=".sql")


    with time_block("execute_sql", run_id, {"sql_len": len(sql)}):
        try:
            result = run_sql_on_mysql(sql,database)
        except Exception as e:
            msg = f"SQL execution exception: {type(e).__name__}: {str(e)[:500]}"
            app_log("sql_runtime_exception", run_id=run_id, message=msg)
            return {"error": msg}

    # handle errors returned by your sql runner
    if isinstance(result, dict) and "error" in result:
        err = str(result.get("error", "Unknown SQL runtime error"))[:2000]
        app_log("sql_runtime_error", run_id=run_id, error=err)
        if run_id:
            save_text_artifact(run_id, "sql_runtime_error", err, suffix=".txt")
        return {"error": err}

    # format the result into your normalized shape
    try:
        formatted = format_result(result["columns"], result["rows"])
    except Exception as e:
        msg = f"Result formatting error: {type(e).__name__}: {str(e)[:500]}"
        app_log("result_format_error", run_id=run_id, message=msg)
        if run_id:
            save_text_artifact(run_id, "result_format_error", msg, suffix=".txt")
        return {"error": msg}

    row_count = len(formatted.get("rows", []))

    # store a small sample for debugging
    if run_id:
        sample = {
            "columns": formatted.get("columns", []),
            "sample_rows": (formatted.get("rows", [])[:RESULT_SAMPLE_ROWS] if formatted.get("rows") else [])
        }
        save_json_artifact(run_id, "sql_result_sample", sample)
        app_log("sql_executed", run_id=run_id, row_count=row_count)

    # checkpoint for very large results (do not attach full result to state)
    if row_count > MAX_SAFE_ROWS:
        return {"row_count": row_count, "needs_checkpoint": True}

    # normal successful path
    return {"result": formatted, "row_count": row_count}

