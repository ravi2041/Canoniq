import json, hashlib, time, re
from collections import Counter
from typing import List, Dict, Optional
import streamlit as st
import pandas as pd

from app.charts.chart_renderer import render_chart_suggestions
from ...agents.nlq_agent import run_langgraph_agent
from ...data.db import read_sql
from ...utils.recommendation_format import format_recommendations


# ------------------ Helpers ------------------
def result_to_df(result: dict) -> pd.DataFrame:
    if "result" in result and "rows" in result["result"]:
        return pd.DataFrame(result["result"]["rows"], columns=result["result"]["columns"])
    return pd.DataFrame()


def render_keypoints(result: dict):
    kp = result.get("keypoints") or {}
    bullets = kp.get("bullets") or []
    metrics = kp.get("metrics") or []
    filters = kp.get("filters") or []
    time_window = kp.get("time_window")
    platform_hints = kp.get("platform_hints") or []
    group_bys = kp.get("group_bys") or []
    output_pref = kp.get("output_pref") or "unknown"

    if not bullets and not any([metrics, filters, time_window, platform_hints, group_bys]):
        return

    with st.expander("🧠 Key Points (understood from your question)", expanded=True):
        if bullets:
            for b in bullets[:8]:
                st.markdown(f"- {b}")

        cols = st.columns(3)
        with cols[0]:
            if metrics: st.caption("**Metrics**"); st.write(", ".join(metrics))
            if group_bys: st.caption("**Group by**"); st.write(", ".join(group_bys))
        with cols[1]:
            if filters: st.caption("**Filters**"); st.write(", ".join(filters))
            if time_window: st.caption("**Time window**"); st.write(time_window)
        with cols[2]:
            if platform_hints: st.caption("**Platforms (hints)**"); st.write(", ".join(platform_hints))
            if output_pref and output_pref != "unknown":
                st.caption("**Output preference**"); st.write(output_pref)


# ------- Suggestions (pull from agent_memory) -------
def tokenize(text: str) -> List[str]:
    return re.findall(r"[a-zA-Z0-9_]+", text.lower()) if text else []


def score_related(seed_tokens: List[str], kw_list: List[str]) -> float:
    if not seed_tokens or not kw_list:
        return 0.0
    s = set(seed_tokens)
    k = set([k.lower() for k in kw_list])
    return len(s & k) / (len(s) + 1e-9)


def generate_templates_from_keywords(top_kw: List[str]) -> List[str]:
    suggestions = []
    for k in top_kw:
        suggestions += [
            f"Show {k} performance by month",
            f"Top campaigns for {k} last quarter",
            f"Compare {k} across platforms",
            f"Trend of CTR for {k}",
            f"ROI breakdown for {k} by campaign",
        ]
    # de-dupe preserve order
    seen, deduped = set(), []
    for s in suggestions:
        if s not in seen:
            seen.add(s)
            deduped.append(s)
    return deduped


@st.cache_data(ttl=60)
def build_suggestions(user_id: str = "global", seed_question: Optional[str] = None, limit: int = 8) -> List[str]:
    # Uses your existing agent_memory table
    sql = """
        SELECT question, keywords
        FROM agent_memory
        WHERE user_id=%s
        ORDER BY updated_at DESC
        LIMIT 200
    """
    df = read_sql(sql, [user_id])
    rows = df.to_dict("records")

    for r in rows:
        try:
            r["keywords"] = json.loads(r.get("keywords") or "[]")
        except Exception:
            r["keywords"] = []

    if not rows:
        return [
            "CTR trend by month in 2025",
            "Compare Facebook vs Google spend in Q2",
            "ROI by campaign last quarter",
            "Top 10 campaigns by conversions",
            "Spend and CPA weekly trend for Facebook",
            "Impressions vs Clicks by platform",
            "Best performing placements this year",
            "Campaign performance by device type",
        ][:limit]

    if seed_question:
        seed_tokens = tokenize(seed_question)
        scored = []
        for r in rows:
            q = (r.get("question") or "").strip()
            if not q:
                continue
            score = max(
                score_related(seed_tokens, r.get("keywords") or []),
                score_related(seed_tokens, tokenize(q)),
            )
            if score > 0:
                scored.append((q, score))
        scored.sort(key=lambda x: x[1], reverse=True)
        related_qs, seen_q = [], set()
        for q, _ in scored:
            if q not in seen_q:
                seen_q.add(q)
                related_qs.append(q)
            if len(related_qs) >= limit // 2:
                break
        # top keywords → templates
        all_kw = [k for r in rows for k in (r.get("keywords") or [])]
        top_kw = [k for k, _ in Counter(all_kw).most_common(5)]
        templates = generate_templates_from_keywords(top_kw)
        return (related_qs + templates)[:limit]

    all_kw = [k for r in rows for k in (r.get("keywords") or [])]
    if not all_kw:
        return [r["question"] for r in rows if r.get("question")][:limit]
    top_kw = [k for k, _ in Counter(all_kw).most_common(4)]
    return generate_templates_from_keywords(top_kw)[:limit]


def render_summary(summary):
    if not summary:
        st.info("No summary available.")
        return

    if isinstance(summary, str):
        st.markdown(f"- {summary}")  # fallback
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


def format_recs_for_table(recs):
    """Convert list of recs into table-compatible rows."""
    table_rows = []
    for i, r in enumerate(recs, 1):
        table_rows.append({
            "No.": i,
            "Title": r.get("title", ""),
            "Action": r.get("action", {}).get("verb", ""),
            "Why": r.get("why", ""),
            "Expected Impact": f"{r['expected_impact']['direction'].title()} ({r['expected_impact']['magnitude']}) on {r['expected_impact']['dimension']}",
            "Confidence": r.get("confidence", "").title(),
            "Metrics Cited": ", ".join(m.get("formatted", "") for m in r.get("metrics_cited", [])),
            "Reasoning": "\n- " + "\n- ".join(r.get("reasoning", [])) if r.get("reasoning") else "",
        })
    return pd.DataFrame(table_rows)


# ---------- Streamlit UI ----------
def show_recommendations(recs):
    st.subheader("📋 Recommendations Table")
    table_df = format_recs_for_table(recs)
    st.dataframe(table_df, use_container_width=True)


def render(df_placeholder: pd.DataFrame = None, side_panel=None):
    """
    NLQ Analytics main page.

    side_panel:
        - If provided (e.g. main.py passes the right-hand column),
          history & session tiles are rendered there.
        - If None, falls back to st.sidebar (for backward compatibility).
    """
    st.subheader("🔎 NLQ Analytics")

    # Session state
    if "query_cache" not in st.session_state:
        st.session_state["query_cache"] = {}
    if "history" not in st.session_state:
        st.session_state["history"] = []
    if "selected_question" not in st.session_state:
        st.session_state["selected_question"] = None

    # ------------- RIGHT-HAND HISTORY PANEL -------------
    panel = side_panel or st.sidebar

    panel.subheader("Your history (agent memory)")
    mem_rows = build_suggestions(user_id="global", limit=30)
    for q in mem_rows:
        label = q if len(q) <= 80 else q[:77] + "..."
        if panel.button(label, key=f"mem_{hashlib.sha256(q.encode()).hexdigest()[:8]}"):
            st.session_state["selected_question"] = q

    panel.markdown("---")
    panel.subheader("Session history")
    for q in st.session_state["history"]:
        if panel.button(q, key=f"hist_{hashlib.sha256(q.encode()).hexdigest()[:8]}"):
            st.session_state["selected_question"] = q
    if panel.button("🏠 Home / Clear Selection"):
        st.session_state["selected_question"] = None

    # ------------- MAIN INPUT + INLINE SUGGESTIONS -------------
    col_input, col_suggest = st.columns([2, 1])

    with col_input:
        question = st.text_input("Ask a question", placeholder="e.g., CTR trend by month in 2025")
        seed = question.strip() if question else None
        suggestions = build_suggestions(user_id="global", seed_question=seed, limit=8)

    with col_suggest:
        st.markdown("### Try asking:")
        for s in suggestions:
            if st.button(s, key=f"sugges_{hashlib.sha256(s.encode()).hexdigest()[:8]}"):
                st.session_state["selected_question"] = s

    st.markdown("---")
    active_question = st.session_state["selected_question"] or question

    def _save_to_cache(q: str, res: dict):
        st.session_state["query_cache"][hashlib.sha256(q.strip().encode()).hexdigest()] = res

    def _load_from_cache(q: str):
        return st.session_state["query_cache"].get(hashlib.sha256(q.strip().encode()).hexdigest())

    # Submit flow
    if st.button("Submit") and question:
        if question not in st.session_state["history"]:
            st.session_state["history"].insert(0, question)
        active_question = question

    # ------------- EXECUTION & DISPLAY -------------
    if active_question:
        cached = _load_from_cache(active_question)
        if cached:
            st.success("Loaded from cache ✅")
            render_keypoints(cached)

            if cached.get("narrative") or cached.get("summary"):
                st.subheader("Campaign Performance Insights")
                summary = cached.get("summary")
                if summary:
                    render_summary(summary)

            recs = format_recommendations(cached.get("recommendation", []))
            if recs:
                show_recommendations(recs)

            df = result_to_df(cached)
            if not df.empty:
                num_cols = df.select_dtypes(include="number").columns
                if len(num_cols) >= 1:
                    st.subheader("Quick Chart")
                    render_chart_suggestions(cached)

        else:
            progress_text = st.empty()
            progress_bar = st.progress(0)
            for i in range(0, 101, 5):
                progress_text.text(f"Running your query... {i}%")
                progress_bar.progress(i)
                time.sleep(0.02)

            with st.spinner("Executing your query with LangGraph..."):
                result = run_langgraph_agent(active_question)

            _save_to_cache(active_question, result)
            progress_text.empty()
            progress_bar.empty()
            st.success("Query executed and stored ✅")

            render_keypoints(result)

            if result.get("narrative") or result.get("summary"):
                st.subheader("Campaign Performance Insights")
                summary = result.get("summary")
                if summary:
                    render_summary(summary)

            recs = format_recommendations(result.get("recommendation", []))
            if recs:
                show_recommendations(recs)

            df = result_to_df(result)
            if not df.empty:
                num_cols = df.select_dtypes(include="number").columns
                if len(num_cols) >= 1:
                    render_chart_suggestions(result)
