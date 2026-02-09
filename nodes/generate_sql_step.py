from typing import Dict, Any
import json

from chains.combined_sql_chain import combined_sql_chain
from chains.shopify_sql_chain import shopify_sql_chain
from chains.ga4_sql_chain import ga4_sql_chain
from chains.marketing_sql_chain import marketing_sql_chain
from reserve_keywords import MYSQL_RESERVED_KEYWORDS
from helper_fucntions.helper_functions import (
    load_metadata,
    load_shopify_metadata, clean_sql_code,
)

from core.state import AgentState
from log_files.observability import (
    time_block,
    app_log,
    save_json_artifact,
    save_text_artifact,
)



reserved_str = ", ".join(MYSQL_RESERVED_KEYWORDS)

def generate_sql_step(state: AgentState) -> Dict[str, Any]:
    """
    LangGraph node: Generates SQL from question and metadata based on chain_type.
    Logs all inputs/outputs/errors using app_log and saves artifacts.
    """
    run_id = state.get("run_id")
    question = state.get("question", "").strip()
    chain_type = state.get("chain_type", "marketing")  # default fallback
    print(chain_type)

    app_log("generate_sql_step_start", run_id=run_id, question=question, chain_type=chain_type)

    # 🧠 Load metadata dynamically
    try:
        if chain_type == "shopify":
            metadata = load_shopify_metadata()
        elif chain_type == "ga4":
            metadata = load_shopify_metadata()
        elif chain_type == "combined":
            metadata = load_shopify_metadata()
        else:
            metadata = load_metadata()

        print("🔍 chain_type:", chain_type)
        #print("📦 Sample metadata tables:", list(metadata.keys())[:5])

    except Exception as e:
        msg = f"Metadata loading failed: {type(e).__name__}: {str(e)}"
        app_log("generate_sql_metadata_load_fail", run_id=run_id, error=msg)
        return {"error": msg}

    # ✅ Basic checks
    if not question or not metadata:
        err = "Missing question or metadata for SQL generation."
        app_log("generate_sql_missing_input", run_id=run_id)
        return {"error": err}

       # ⏱️ Time & Execute SQL generation
    with time_block("generate_sql", run_id, {"question_len": len(question), "chain_type": chain_type}):
        try:
            # (Optional) also persist raw inputs
            if run_id:
                save_json_artifact(run_id, "sql_generation_inputs", {
                    "question": question,
                    "metadata_keys": list(metadata.keys()) if isinstance(metadata, dict) else "unknown",
                })
            # Chain routing
            if chain_type == "shopify":
                chain = shopify_sql_chain(metadata)
            elif chain_type == "ga4":
                chain = ga4_sql_chain(metadata)
            elif chain_type == "combined":
                chain = combined_sql_chain(metadata)
            else:
                chain = marketing_sql_chain(metadata)

            output = chain.invoke({
                "metadata": json.dumps(metadata),
                "question": question,
                "reserved_str": reserved_str,
            })

        except Exception as e:
            msg = f"SQL generation exception: {type(e).__name__}: {str(e)[:500]}"
            app_log("generate_sql_exception", run_id=run_id, error=msg)
            if run_id:
                save_text_artifact(run_id, "generate_sql_error", msg, suffix=".txt")
            return {"error": msg}

    # 🧾 Parse Output
    sql_text = output.get("sql") if isinstance(output, dict) else None
    sql_text = clean_sql_code(sql_text)
    if not sql_text or not sql_text.strip():
        err = "SQL generation returned empty or invalid output."
        app_log("generate_sql_empty", run_id=run_id)
        if run_id:
            save_text_artifact(run_id, "generate_sql_empty", err)
        return {"error": err}

    # 💾 Save final SQL
    if run_id:
        save_text_artifact(run_id, "generated_sql_raw", sql_text, suffix=".sql")
        app_log("generate_sql_success", run_id=run_id, sql_len=len(sql_text))

    return {"sql": sql_text}
