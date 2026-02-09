# app/dashboards/homepage/index.py
import streamlit as st

def render():
    # Section heading under global header
    st.subheader("What would you like to do today?")
    st.caption("Pick an entry point – ask a question, open dashboards, or manage data quality.")

    # ---------- CUSTOM CSS FOR BUBBLES ----------
    st.markdown(
        """
        <style>
        .bubble-card {
            width: 100%;
            max-width: 260px;
            margin: 0 auto 1.5rem auto;
            border-radius: 18px;
            padding: 1.4rem 1.2rem;
            text-align: left;
            background: rgba(15,23,42,0.02);
            box-shadow: 0 10px 25px rgba(15,23,42,0.08);
            border: 1px solid rgba(148,163,184,0.25);
            transition: all 0.18s ease-in-out;
            cursor: default;
            display: flex;
            flex-direction: column;
            gap: 0.4rem;
        }

        .bubble-card:hover {
            transform: translateY(-4px);
            box-shadow: 0 14px 32px rgba(15,23,42,0.15);
            border-color: rgba(59,130,246,0.65);
        }

        .bubble-icon {
            font-size: 1.7rem;
        }

        .bubble-title {
            font-weight: 600;
            font-size: 1.00rem;
        }

        .bubble-subtitle {
            font-size: 0.85rem;
            opacity: 0.85;
            line-height: 1.4;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    bubbles = [
        {
            "icon": "💬",
            "title": "Ask a Question",
            "subtitle": "Type a natural-language question and let the agent generate SQL, charts, and insights.",
        },
        {
            "icon": "📊",
            "title": "Live Dashboards",
            "subtitle": "Explore prebuilt dashboards for marketing, Shopify, and GA4 performance.",
        },
        {
            "icon": "🧬",
            "title": "Insight Engine",
            "subtitle": "Auto-detect anomalies, trends, and key drivers for your KPIs.",
        },
        {
            "icon": "🛍️",
            "title": "Shopify Analytics",
            "subtitle": "Deep-dive into orders, cohorts, attribution, and LTV for your store.",
        },
        {
            "icon": "🧹",
            "title": "Data Quality & Naming",
            "subtitle": "Scan for broken mappings, naming issues, and metadata gaps across platforms.",
        },
        {
            "icon": "🧱",
            "title": "Metadata Builder",
            "subtitle": "Let AI build and update table metadata, mappings, and semantic models.",
        },
    ]

    row1_cols = st.columns(3, gap="large")
    for col, bubble in zip(row1_cols, bubbles[0:3]):
        with col:
            st.markdown(
                f"""
                <div class="bubble-card">
                    <div class="bubble-icon">{bubble["icon"]}</div>
                    <div class="bubble-title">{bubble["title"]}</div>
                    <div class="bubble-subtitle">{bubble["subtitle"]}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    row2_cols = st.columns(3, gap="large")
    for col, bubble in zip(row2_cols, bubbles[3:6]):
        with col:
            st.markdown(
                f"""
                <div class="bubble-card">
                    <div class="bubble-icon">{bubble["icon"]}</div>
                    <div class="bubble-title">{bubble["title"]}</div>
                    <div class="bubble-subtitle">{bubble["subtitle"]}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown("---")
    st.subheader("Session notes")
    st.caption("Use this area to jot down ideas, TODOs, or experiment notes while you use Clarity AI.")

    notes = st.text_area("Scratchpad", value="", height=120)
    st.session_state["home_notes"] = notes
