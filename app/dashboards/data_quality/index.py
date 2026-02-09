# app/dashboards/data_quality/index.py
import streamlit as st
from ...agents.nlq_agent import run_langgraph_agent


# ------------------ helpers ------------------ #
def _get_feedback_store():
    if "dq_feedback" not in st.session_state:
        st.session_state["dq_feedback"] = {}
    return st.session_state["dq_feedback"]


def _paginate_list(items, page_size=20, key_prefix="page"):
    """simple manual pagination for lists (uses st.rerun, not experimental)"""
    total = len(items)
    if total == 0:
        return [], 0, 0

    page_key = f"{key_prefix}_page"
    if page_key not in st.session_state:
        st.session_state[page_key] = 0

    page = st.session_state[page_key]
    start = page * page_size
    end = start + page_size
    page_items = items[start:end]

    # controls
    cols = st.columns(3)
    with cols[0]:
        if st.button("Prev", key=f"{key_prefix}_prev") and page > 0:
            st.session_state[page_key] -= 1
            st.rerun()
    with cols[1]:
        st.write(f"Page {page+1} / {max(1, (total - 1)//page_size + 1)}")
    with cols[2]:
        if st.button("Next", key=f"{key_prefix}_next") and end < total:
            st.session_state[page_key] += 1
            st.rerun()

    return page_items, page, total


def _render_dimension_block(dq_dims: dict):
    """nicely show dimensions at the bottom in an expander"""
    with st.expander("📦 Dimensions collected (per source)", expanded=False):
        if not dq_dims:
            st.warning("No dimensions were collected. Check DB connection or metadata.")
            return

        st.write("These are the raw unique names we pulled from your tables. Each row keeps table + column info.")
        for dim_name, values in dq_dims.items():
            st.markdown(f"### {dim_name.title()} ({len(values)} unique)")
            # values is a list of dicts: {"value": "...", "table": "...", "column": "..."}
            # paginate per dimension
            page_items, _, _ = _paginate_list(values, page_size=15, key_prefix=f"{dim_name}_dim")

            # make it readable
            if page_items:
                # turn list of dicts into 3 columns
                # we only show these 3: value, table, column
                table_data = {
                    "Value": [v.get("value", "") for v in page_items],
                    "Table": [v.get("table", "") for v in page_items],
                    "Column": [v.get("column", "") for v in page_items],
                }
                st.table(table_data)
            else:
                st.write("No values.")


# ------------------ main renderer ------------------ #
def render():
    st.header("🛡️ Data Quality")
    st.write("Run data-quality checks on campaign / placement / creative / site names, and confirm anomalies.")
    st.caption("✅ = this name is OK / teach the system; ❌ = this name is wrong / keep flagging it.")

    fb_store = _get_feedback_store()

    # run the DQ flow
    if st.button("Run data quality check"):
        resp = run_langgraph_agent(
            "run data quality",
            chain_type="data_quality",
            chat_history=[],
            dq_feedback=fb_store,
        )
        st.session_state["dq_last_response"] = resp

    # show last response if present
    resp = st.session_state.get("dq_last_response")
    if not resp:
        st.info("Click **Run data quality check** to analyze current data.")
        return

    dq_dims = resp.get("dq_dimensions") or {}
    dq_findings = resp.get("dq_ai_findings") or {}
    dq_summary_backend = resp.get("dq_final_summary") or ""
    dq_memory = resp.get("dq_pattern_memory") or {}

    # 1) 🧠 AI findings (TOP)
    st.subheader("🧠 AI findings (review & correct)")

    findings_list = dq_findings.get("findings") if isinstance(dq_findings, dict) else []

    # top-level summary (from model)
    if isinstance(dq_findings, dict) and dq_findings.get("summary"):
        st.info(dq_findings["summary"])

    if not findings_list:
        st.write("No issues detected.")
    else:
        # flatten anomalies into rows: dimension, platform, value, reason, suggestion
        rows = []
        for f in findings_list:
            if not isinstance(f, dict):
                continue
            dim = f.get("dimension", "unknown")
            platform = f.get("platform", "Unknown")
            table_name = f.get("table_name", "unknown_table")
            pattern = f.get("pattern_observed", "")
            anomalies = f.get("anomalies") or []
            for a in anomalies:
                if not isinstance(a, dict):
                    continue
                rows.append({
                    "dimension": dim,
                    "platform": platform,
                    "table_name": table_name,
                    "pattern": pattern,
                    "value": a.get("value", "<empty>"),
                    "reason": a.get("reason", ""),
                    "suggested_action": a.get("suggested_action", ""),
                })

        if not rows:
            st.write("No anomalies to review.")
        else:
            # pagination for anomalies
            page_rows, _, _ = _paginate_list(rows, page_size=10, key_prefix="anomalies")

            # header row
            header_cols = st.columns([0.8, 0.9, 1.0, 1.0, 1.2, 1.2, 0.7])
            header_cols[0].markdown("**Dimension**")
            header_cols[1].markdown("**Platform**")
            header_cols[2].markdown("**Table**")
            header_cols[3].markdown("**Pattern**")
            header_cols[4].markdown("**Value**")
            header_cols[5].markdown("**Reason**")
            header_cols[6].markdown("**Action**")

            for idx, r in enumerate(page_rows):
                c1, c2, c3, c4, c5, c6, c7 = st.columns([0.8, 0.9, 1.0, 1.0, 1.2, 1.2, 0.7])
                c1.write(r["dimension"])
                c2.write(r["platform"])
                c3.write(r["table_name"])
                c4.write(r["pattern"] if r["pattern"] else "—")
                c5.write(r["value"])
                c6.write(r["reason"])
                finding_id = f"{r['dimension']}:{r['platform']}:{r['table_name']}:{r['value']}"
                with c7:
                    valid_key = f"valid_{finding_id}_{idx}"
                    invalid_key = f"invalid_{finding_id}_{idx}"
                    col_btn1, col_btn2 = st.columns(2)
                    with col_btn1:
                        if st.button("✅", key=valid_key):
                            fb_store[finding_id] = {
                                "dimension": r["dimension"],
                                "platform": r["platform"],
                                "table_name": r["table_name"],
                                "value": r["value"],
                                "status": "valid",
                                "reason": r["reason"],
                            }
                            st.success("ok")
                    with col_btn2:
                        if st.button("❌", key=invalid_key):
                            fb_store[finding_id] = {
                                "dimension": r["dimension"],
                                "platform": r["platform"],
                                "table_name": r["table_name"],
                                "value": r["value"],
                                "status": "invalid",
                                "reason": r["reason"],
                                "suggested_action": r["suggested_action"],
                            }
                            st.warning("flagged")

    # 2) 🧾 Data Quality Summary (single)
    st.subheader("🧾 Data Quality Summary")
    if dq_summary_backend:
        st.markdown(dq_summary_backend)
    else:
        st.write("No summary was generated.")

    # 3) 📦 Dimensions (BOTTOM, EXPANDABLE)
    _render_dimension_block(dq_dims)

    # 4) debug panels
    with st.expander("🧠 Learned naming memory (debug)"):
        st.json(dq_memory)

    with st.expander("📝 Your validations this session"):
        st.json(st.session_state.get("dq_feedback", {}))
