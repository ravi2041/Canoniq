from langgraph.types import interrupt
from helper_fucntions.sql_runner import run_sql_on_mysql
from helper_fucntions.helper_functions import format_result
from chains.get_fix_chain import get_fix_chain

def human_checkpoint_node(state: dict):
    decision = interrupt({
        "question": state["question"],
        "row_count": state.get("row_count"),
        "sql": state.get("sql"),
    })

    target_db = state.get("target_db")  # ✅ get database from state
    if not target_db:
        state["error"] = "Missing database context for human checkpoint"
        return state

    if decision == "proceed":
        result = run_sql_on_mysql(state["sql"], database=target_db)
        state["result"] = format_result(result["columns"], result["rows"])

    elif decision == "aggregate":
        fix_chain = get_fix_chain()
        new_sql = fix_chain.invoke({
            "question": state["question"],
            "sql": state["sql"],
            "error": f"Query returned {state['row_count']:,} rows, too large",
            "mysql_docs": state.get("mysql_doc", "")
        })
        state["sql"] = new_sql
        result = run_sql_on_mysql(new_sql, database=target_db)
        state["result"] = format_result(result["columns"], result["rows"])

    elif decision == "cancel":
        state["error"] = "Query cancelled by user"

    return state
