from langchain_core.messages import HumanMessage
from mysql_docs_retriever import get_docs_for_error, format_docs_citations
from chains.get_fix_chain import get_fix_chain
from helper_fucntions.helper_functions import validate_sql
from log_files.observability import app_log, save_text_artifact


def fix_sql_step(state):
    run_id = state.get("run_id")

    # ✅ Exit early if there's no error
    if not state.get("error"):
        app_log("fix_sql_skipped", run_id=run_id, reason="No error in state")
        return state

    # ✅ Defensive check for missing SQL
    if "sql" not in state or not state["sql"].strip():
        app_log("fix_sql_skipped", run_id=run_id, reason="Missing SQL in state")
        state["user_friendly_error"] = "No SQL found to fix after initial generation step."
        return state

    # ✅ Convert error messages to plain string
    def extract_error_text(error_list):
        return "\n".join(
            e.content if isinstance(e, HumanMessage) else str(e) for e in error_list
        )

    error_text = extract_error_text(state["error"])

    # ✅ Get MySQL documentation if not already loaded
    # if not state.get("mysql_doc"):
    #     docs = get_docs_for_error(error_text, top_k=2)
    #     state["mysql_doc"] = format_docs_citations(docs)

    fix_chain = get_fix_chain()
    max_retries = 2
    fixed = False
    target_db = state.get("target_db")

    # ✅ Defensive check for missing DB context
    if not target_db:
        state["fix_status"] = "Missing database context"
        state["user_friendly_error"] = "Could not determine which database to validate against."
        app_log("fix_sql_failed", run_id=run_id, reason="Missing database context")
        return state

    for attempt in range(max_retries):
        try:
            fixed_sql = fix_chain.invoke({
                "question": state["question"],
                "sql": state["sql"],
                "error": error_text
                #"mysql_docs": state["mysql_doc"]
            })

            if run_id:
                save_text_artifact(run_id, f"fix_sql_attempt_{attempt+1}", fixed_sql, suffix=".sql")

            state["sql"] = fixed_sql

            if validate_sql(fixed_sql, database=target_db):
                state["error"] = []
                state["fix_status"] = f"✅ Fixed on attempt {attempt + 1}"
                app_log("fix_sql_success", run_id=run_id, attempt=attempt + 1)
                fixed = True
                break

        except Exception as e:
            msg = f"Fix attempt {attempt + 1} failed: {type(e).__name__}: {str(e)}"
            app_log("fix_sql_exception", run_id=run_id, message=msg)
            state["fix_status"] = msg

    if not fixed:
        state["fix_status"] = "❌ Unfixable after 2 attempts"
        state["unfixable_error"] = error_text
        state["user_friendly_error"] = (
            f"❌ Could not fix SQL automatically.\n\nError:\n{error_text}\n\n"
            f"Please check table names, group by expressions, or reserved keywords.\n\n"
            f"📚 Related documentation:\n{state.get('mysql_doc', '')}"
        )
        app_log("fix_sql_unfixable", run_id=run_id, error=error_text)

    return state
