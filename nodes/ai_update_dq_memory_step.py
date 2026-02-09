# nodes/ai_update_dq_memory_step.py
import os, json
from datetime import datetime
from core.state import AgentState
from chains.ai_dq_feedback_chain import ai_dq_feedback_chain
from log_files.observability import app_log

MEMORY_FILE = "dq_pattern_memory.json"

def ai_update_dq_memory_step(state: AgentState) -> dict:
    """
    Node: Update the stored pattern memory based on user feedback.
    Feedback structure example in state["dq_feedback"]:
    {
        "accepted_patterns": ["Brand_Product_Year"],
        "rejected_patterns": ["Incomplete_Name"],
        "comments": "These new product codes are valid."
    }
    """
    run_id = state.get("run_id")

    # load existing memory
    if os.path.exists(MEMORY_FILE):
        with open(MEMORY_FILE, "r", encoding="utf-8") as f:
            current_memory = json.load(f)
    else:
        current_memory = {}

    user_feedback = state.get("dq_feedback", {})

    if not user_feedback:
        app_log("ai_update_dq_memory_step_skip", run_id=run_id, reason="No feedback found.")
        return state

    chain = ai_dq_feedback_chain()
    result = chain.invoke({
        "current_memory": json.dumps(current_memory, indent=2),
        "user_feedback": json.dumps(user_feedback, indent=2)
    })

    # Write updated memory to file
    updated_memory = result.get("updated_memory", current_memory)
    with open(MEMORY_FILE, "w", encoding="utf-8") as f:
        json.dump(updated_memory, f, indent=2)

    state["dq_pattern_memory"] = updated_memory
    state["dq_memory_updated_at"] = datetime.utcnow().isoformat()
    state["dq_memory_summary"] = result.get("summary", "No summary returned")

    app_log("ai_update_dq_memory_step_done", run_id=run_id, summary=result.get("summary"))
    print("🧠 Pattern memory updated successfully.")
    return state
