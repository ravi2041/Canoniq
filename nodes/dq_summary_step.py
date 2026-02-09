# nodes/dq_summary_step.py
from datetime import datetime, timezone
from core.state import AgentState
from app.observability.observability import app_log


def dq_summary_step(state: AgentState) -> dict:
    """
    Build a human-friendly DQ summary from the AI findings.
    This MUST be defensive because LLMs can return a mix of strings and dicts.
    """
    run_id = state.get("run_id")
    findings_payload = state.get("dq_ai_findings") or {}

    summary_text: list[str] = []

    # top-level summary from the LLM, if any
    top_summary = findings_payload.get("summary") if isinstance(findings_payload, dict) else None
    if top_summary:
        summary_text.append(f"Overall: {top_summary}")
        summary_text.append("")

    findings = []
    if isinstance(findings_payload, dict):
        findings = findings_payload.get("findings") or []

    if findings:
        summary_text.append("Details:")
        for f in findings:
            # f can be a dict or a string
            if isinstance(f, str):
                # just append the raw string
                summary_text.append(f"- {f}")
                continue

            if not isinstance(f, dict):
                continue

            dim = f.get("dimension", "unknown dimension")
            platform = f.get("platform")
            pattern = f.get("pattern_observed")

            if platform:
                summary_text.append(f"- Dimension: **{dim}** (platform: {platform})")
            else:
                summary_text.append(f"- Dimension: **{dim}**")

            if pattern:
                summary_text.append(f"  - learned pattern: `{pattern}`")

            anomalies = f.get("anomalies") or []
            if anomalies:
                summary_text.append(f"  - anomalies found: {len(anomalies)}")
                for a in anomalies:
                    if not isinstance(a, dict):
                        continue
                    val = a.get("value", "<empty>")
                    reason = a.get("reason", "")
                    suggestion = a.get("suggested_action")
                    summary_text.append(f"    • {val} → {reason}")
                    if suggestion:
                        summary_text.append(f"      ↳ suggestion: {suggestion}")
            else:
                summary_text.append("  - no anomalies for this dimension.")
    else:
        summary_text.append(
            f"- {f.get('dimension', '?')} anomalies found in **{f.get('table_name', 'unknown_table')}** "
            f"(Platform: {f.get('platform', 'N/A')})"
        )

    final_summary_str = "\n".join(summary_text)

    # write back to state
    state["dq_final_summary"] = final_summary_str
    state["dq_last_summarized_at"] = datetime.now(timezone.utc).isoformat()

    app_log("dq_summary_step_done", run_id=run_id)

    # also return state (LangGraph style)
    return state
