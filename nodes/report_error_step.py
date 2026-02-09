
def report_error_step(state):
    state["user_friendly_error"] = state.get("user_friendly_error") or state.get("error") or "Unknown error"
    print("🛑 SQL could not be fixed. User-friendly message:\n", state["user_friendly_error"])
    return state
