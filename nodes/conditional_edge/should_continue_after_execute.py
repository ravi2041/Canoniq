from log_files.observability import app_log


def should_continue_after_execute(state):
    """
    Decide what to do after SQL execution:
    - If error exists, retry fix_sql.
    - If result too large (needs_checkpoint), route to human_checkpoint.
    - Else go to summarize.
    """
    run_id = state.get("run_id")

    if state.get("error"):
        app_log("routing_decision", run_id=run_id, next_node="fix_sql", reason="SQL execution error detected")
        return "fix_sql"

    if state.get("needs_checkpoint"):
        app_log("routing_decision", run_id=run_id, next_node="human_checkpoint", reason="Large result set")
        return "human_checkpoint"

    app_log("routing_decision", run_id=run_id, next_node="summarize", reason="SQL executed successfully")
    return "summarize"


def should_continue(state):
    """
    Decide what to do after SQL validation or fix attempt:
    - If marked as fixed, execute again.
    - If unfixable, go to error report.
    - If still error but not fixed, retry fix_sql.
    - Else assume valid and execute.
    """
    run_id = state.get("run_id")
    fix_status = state.get("fix_status", "").lower()
    error = state.get("error", "")

    if fix_status == "fixed":
        app_log("routing_decision", run_id=run_id, next_node="execute_sql", reason="Fix successful")
        return "execute_sql"

    if fix_status == "unfixable":
        app_log("routing_decision", run_id=run_id, next_node="report_error", reason="SQL unfixable")
        return "report_error"

    if error:
        app_log("routing_decision", run_id=run_id, next_node="fix_sql", reason="SQL still has errors")
        return "fix_sql"

    app_log("routing_decision", run_id=run_id, next_node="execute_sql", reason="SQL is valid")
    return "execute_sql"
