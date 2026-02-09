# critic_step.py
import re
from typing import Dict, Any
from chains.get_critic_chain import get_critic_chain, make_critic_inputs
from app.observability.observability import time_block, app_log, save_text_artifact

def _extract_final_sql(text: str) -> str:
    # Preferred: XML tags
    m = re.search(r"<final_sql>\s*(.*?)\s*</final_sql>", text, flags=re.DOTALL|re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # Fallbacks: code fences
    for pat in (r"```sql\s*(.*?)\s*```", r"```mysql\s*(.*?)\s*```", r"```\s*(.*?)\s*```"):
        m = re.search(pat, text, flags=re.DOTALL|re.IGNORECASE)
        if m:
            return m.group(1).strip()
    # Last resort: from first SQL verb
    m = re.search(r"(?is)\b(SELECT|WITH|INSERT|UPDATE|DELETE)\b.*", text)
    return m.group(0).strip() if m else text.strip()

def critic_sql_step(state: Dict[str, Any]) -> Dict[str, Any]:
    run_id = state.get("run_id")
    """
    LangGraph node: runs the critic chain on state['sql'] and writes corrected SQL back.
    Expects:
      - state['question']
      - state['metadata'] or state['schema']
      - state['sql'] (candidate from generate step)
    """
    sql_candidate = (state.get("sql") or "").strip()
    if not sql_candidate:
        return state  # nothing to review

    with time_block("critic_sql_step", run_id, {"sql_len": len(sql_candidate)}):
        question = state.get("question") or state.get("input", "")
        schema = state.get("metadata") or state.get("schema") or ""

        chain = get_critic_chain()
        inputs = make_critic_inputs(question, schema, sql_candidate)
        resp = chain.invoke(inputs)

        # LangChain ChatModels return a Message with `.content`
        content = getattr(resp, "content", str(resp))
        final_sql = _extract_final_sql(content)

        updates: Dict[str, Any] = {}
        if final_sql and final_sql != sql_candidate:
            updates["sql"] = final_sql  # ✅ only write if actually changed

        # If you have messages declared with a reducer (e.g., add_messages), you can also append:
        # if "messages" in state:
        #     updates.setdefault("messages", [])
        #     updates["messages"].append({"role": "critic", "content": content})
            # artifacts
        save_text_artifact(run_id, "sql_before_critic", sql_candidate, ".sql")
        save_text_artifact(run_id, "sql_after_critic", final_sql or sql_candidate, ".sql")
        app_log("critic_done", run_id=run_id, changed=bool(final_sql and final_sql != sql_candidate))
        return updates  # ✅ minimal dict; no full state return
