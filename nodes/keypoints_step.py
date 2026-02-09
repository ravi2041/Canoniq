# keypoints_step.py
from typing import Dict, Any
from chains import keypoints_chain
from app.observability.observability import time_block, app_log, save_json_artifact

def keypoints_step(state: Dict[str, Any]) -> Dict[str, Any]:

    run_id = state.get("run_id")

    """LangGraph node: extracts key points from the user question."""
    question = state.get("question") or state.get("input", "")
    if not question:
        return state

    with time_block("keypoints_step", run_id, {"question_len": len(question)}):
        chain = keypoints_chain.get_keypoints_chain()
        out = chain.invoke({"question": question})  # dict via JsonOutputFunctionsParser
        # Minimal hardening
        bullets = out.get("bullets", []) if isinstance(out, dict) else []
        kp = {
            "bullets": bullets[:8],
            "metrics": out.get("metrics", []),
            "filters": out.get("filters", []),
            "time_window": out.get("time_window"),
            "platform_hints": out.get("platform_hints", []),
            "group_bys": out.get("group_bys", []),
            "output_pref": out.get("output_pref", "unknown")
        }
        save_json_artifact(run_id, "keypoints",kp)
        app_log("keypoints_extracted", run_id=run_id, bullets=len(kp["bullets"]))
        return {"keypoints":kp}
