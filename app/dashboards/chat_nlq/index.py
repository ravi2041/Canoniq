import time
import json
import streamlit as st
import pandas as pd
import plotly.express as px

from ...agents.nlq_agent import run_langgraph_agent  # your backend


# --------- helpers ---------
def format_for_chat(payload):
    if not payload:
        return "Done."

    if isinstance(payload, str):
        return payload

    if isinstance(payload, dict):
        kf = payload.get("key_findings")
        if isinstance(kf, list) and kf:
            lines = ["Key findings:"]
            for item in kf[:4]:
                lines.append(f"- {item}")
            return "\n".join(lines)
        for k, v in payload.items():
            if isinstance(v, list) and v:
                lines = [f"{k.replace('_', ' ').title()}:"]
                lines += [f"- {x}" for x in v[:4]]
                return "\n".join(lines)
        return str(payload)

    if isinstance(payload, list):
        return "\n".join(f"- {x}" for x in payload[:4])

    return str(payload)


def result_to_df_from_agent(result) -> pd.DataFrame:
    if result is None:
        return pd.DataFrame()

    if isinstance(result, list):
        if len(result) == 0:
            return pd.DataFrame()
        if isinstance(result[0], dict):
            return pd.DataFrame(result)
        return pd.DataFrame(result)

    if (
        isinstance(result, dict)
        and "columns" in result
        and "rows" in result
        and result.get("columns") == ["columns", "rows"]
    ):
        outer_rows = result.get("rows") or []
        if not outer_rows:
            return pd.DataFrame()
        headers = outer_rows[0][0]
        data_rows = outer_rows[0][1]
        return pd.DataFrame(data_rows, columns=headers)

    if isinstance(result, dict) and "columns" in result and "rows" in result:
        return pd.DataFrame(result["rows"], columns=result["columns"])

    if isinstance(result, dict):
        return pd.DataFrame([result])

    return pd.DataFrame()


def clean_df_for_streamlit(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
    return df


def render_keypoints_block(resp: dict):
    kp = resp.get("keypoints") or {}
    bullets = kp.get("bullets") or []
    metrics = kp.get("metrics") or []
    filters = kp.get("filters") or []
    time_window = kp.get("time_window")
    group_bys = kp.get("group_bys") or []
    platform_hints = kp.get("platform_hints") or []

    if not bullets and not any([metrics, filters, time_window, group_bys, platform_hints]):
        return

    with st.expander("🧠 What I understood", expanded=True):
        for b in bullets[:8]:
            st.markdown(f"- {b}")

        cols = st.columns(3)
        with cols[0]:
            if metrics:
                st.caption("**Metrics**")
                st.write(", ".join(metrics))
            if group_bys:
                st.caption("**Group by**")
                st.write(", ".join(group_bys))
        with cols[1]:
            if filters:
                st.caption("**Filters**")
                st.write(", ".join(filters))
            if time_window:
                st.caption("**Time window**")
                st.write(time_window)
        with cols[2]:
            if platform_hints:
                st.caption("**Platforms**")
                st.write(", ".join(platform_hints))


def render_summary_block(summary):
    if not summary:
        return
    if isinstance(summary, str):
        st.subheader("📊 Key Findings")
        st.markdown(f"- {summary}")
        return

    st.subheader("📊 Key Findings")
    for f in summary.get("key_findings", []):
        st.markdown(f"- {f}")

    if summary.get("drivers"):
        st.subheader("🔎 Drivers")
        for d in summary["drivers"]:
            st.markdown(f"- {d}")

    if summary.get("limitations"):
        st.subheader("⚠️ Limitations")
        for l in summary["limitations"]:
            st.markdown(f"- {l}")


# --------- main ---------
def render():
    st.markdown("## 🤖 NLQ Conversational Analytics")
    st.caption("Chat with your data. Ask, then choose to see the table or charts.")

    # session init
    if "chat_messages" not in st.session_state:
        st.session_state.chat_messages = []
    if "responses" not in st.session_state:
        st.session_state.responses = []
    if "answer_view_state" not in st.session_state:
        st.session_state.answer_view_state = {}

    col_chat, col_resp = st.columns([0.4, 0.6], gap="large")

    # ---------------- LEFT (chat) ----------------
    with col_chat:
        st.markdown("### 💬 Conversation")

        with st.container():
            for msg in st.session_state.chat_messages:
                role = "🧑‍💻 You" if msg["role"] == "user" else "🤖 Assistant"
                bg = "#f0f0f5" if msg["role"] == "user" else "#e8f4fd"
                content = msg.get("content", "")
                if not isinstance(content, str):
                    content = str(content)
                content_html = content.replace("\n", "<br>")
                st.markdown(
                    f"""
                    <div style="background-color:{bg};
                                border-radius:10px;
                                padding:8px 10px;
                                margin-bottom:6px;">
                        <b>{role}:</b><br>{content_html}
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

        st.markdown("---")
        with st.form(key="chat_form", clear_on_submit=True):
            user_q = st.text_input(
                "Ask a new question",
                placeholder="e.g. campaign performance by platform last 30 days"
            )
            submitted = st.form_submit_button("🚀 Submit")

        if submitted and user_q.strip():
            # 1) add user msg
            st.session_state.chat_messages.append({"role": "user", "content": user_q})

            # 2) progress + call backend
            progress_placeholder = st.empty()
            progress_bar = progress_placeholder.progress(0)
            status_placeholder = st.empty()
            start = time.time()
            with st.spinner("Running NLQ agent..."):
                for pct in (20, 40, 60):
                    progress_bar.progress(pct)
                    status_placeholder.info(f"⏱️ Processing... {pct}%")
                    time.sleep(0.15)

                agent_resp = run_langgraph_agent(
                    question=user_q,
                    user_id="demo",
                    chat_history=st.session_state.chat_messages,
                )

            end = time.time()
            elapsed = round(end - start, 2)

            # 3) assistant bubble (short) – use keypoints or summary
            assistant_payload = (
                agent_resp.get("keypoints")
                or agent_resp.get("summary")
                or agent_resp.get("user_friendly_error")
            )
            bubble_text = format_for_chat(assistant_payload)
            st.session_state.chat_messages.append({"role": "assistant", "content": bubble_text})

            # 4) store full response
            st.session_state.responses.append(agent_resp)
            idx = len(st.session_state.responses) - 1
            st.session_state.answer_view_state[idx] = {"show_table": False, "show_chart": False}

            progress_bar.progress(100)
            status_placeholder.success(f"✅ Done in {elapsed}s")
            st.rerun()

    # ---------------- RIGHT (results) ----------------
    with col_resp:
        st.markdown("### 📊 Results")

        if not st.session_state.responses:
            st.info("Ask something on the left to see results here.")
            return

        # 👇 NEW: show newest first
        # enumerate to keep original index for view state and button keys
        indexed_responses = list(enumerate(st.session_state.responses))
        for idx, resp in reversed(indexed_responses):
            st.markdown(f"#### 🧠 Answer {idx + 1}")

            # keypoints
            # render_keypoints_block(resp)

            # summary / narrative
            summary = resp.get("summary") or resp.get("narrative")
            if summary:
                render_summary_block(summary)

            st.markdown(
                "<p style='color:#666;font-size:0.85rem;'>Want to see the underlying data or charts? Use the buttons below.</p>",
                unsafe_allow_html=True,
            )

            # view state init
            if idx not in st.session_state.answer_view_state:
                st.session_state.answer_view_state[idx] = {"show_table": False, "show_chart": False}
            view_state = st.session_state.answer_view_state[idx]

            result = resp.get("result")
            charts = resp.get("chart_suggestions") or []

            c1, c2 = st.columns(2)
            with c1:
                if result:
                    if st.button("📋 Show table", key=f"show_table_{idx}"):
                        view_state["show_table"] = not view_state["show_table"]
                        st.session_state.answer_view_state[idx] = view_state
                        st.rerun()
            with c2:
                if charts and result:
                    if st.button("📈 Show chart", key=f"show_chart_{idx}"):
                        view_state["show_chart"] = not view_state["show_chart"]
                        st.session_state.answer_view_state[idx] = view_state
                        st.rerun()

            # table
            if view_state["show_table"] and result:
                df = result_to_df_from_agent(result)
                df = clean_df_for_streamlit(df)
                st.dataframe(df, use_container_width=True)

            # charts
            if view_state["show_chart"] and charts and result:
                df = result_to_df_from_agent(result)
                df = clean_df_for_streamlit(df)
                if not df.empty:
                    st.markdown("**Charts:**")
                    for c in charts:
                        x = c.get("x") or c.get("x_axis")
                        y = c.get("y") or c.get("y_axis")
                        title = c.get("title", "")
                        ctype = c.get("type", "bar")
                        # handle chart axes safely
                        if isinstance(x, list) and len(x) > 0:
                            x = x[0]
                        if isinstance(y, list) and len(y) > 0:
                            y = y[0]

                        if not (isinstance(x, str) and isinstance(y, str)):
                            continue
                        if x not in df.columns or y not in df.columns:
                            continue

                        if ctype == "line":
                            fig = px.line(df, x=x, y=y, title=title)
                        else:
                            fig = px.bar(df, x=x, y=y, title=title)
                        st.plotly_chart(fig, use_container_width=True)

            st.markdown("---")

        # style
        st.markdown(
            """
            <style>
            .stTextInput > div > div > input {
                border-radius: 8px;
                border: 1px solid #cccccc;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
