"""
decision_monitor.py
-------------------
Streamlit dashboard to visualize AI recommendation confidence and actions.
Integrated with the main Marketing Analytics app.
"""

import os
import sys
import streamlit as st
import pandas as pd
import plotly.express as px

# --- Ensure project root is in sys.path ---
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from nodes.recommend_step import recommend_step
from ai.decision_engine.trainer import train_bandit


# =========================================================
# 🧠 Render Function — Called from app/main.py
# =========================================================
def render():
    """Render the AI Decision Confidence & Action Monitor dashboard."""
    st.header("🧠 AI Decision Confidence & Action Monitor")
    st.caption(
        "Monitor how the self-learning bandit recommends creative optimizations "
        "and explore model confidence across platforms and campaigns."
    )

    # -----------------------------------------------------------------
    # Step 1: Train or load model
    # -----------------------------------------------------------------
    try:
        with st.spinner("Training contextual bandit..."):
            bandit, encoder, feature_names = train_bandit(days=14)
        state = {"bandit": bandit, "encoder": encoder, "feature_names": feature_names}

        with st.spinner("Generating recommendations..."):
            new_state = recommend_step(state)

        recs = pd.DataFrame(new_state.get("recommendations", []))
        if recs.empty:
            st.warning("⚠️ No recommendations generated. Check data availability.")
            return

    except Exception as e:
        st.error(f"❌ Error during recommendation generation: {e}")
        st.stop()

    # -----------------------------------------------------------------
    # Step 2: Visualizations
    # -----------------------------------------------------------------
    st.markdown("### 📊 Model Output Overview")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Confidence Distribution")
        fig_conf = px.histogram(
            recs,
            x="confidence",
            nbins=20,
            color_discrete_sequence=["#4C78A8"],
            title="Model Confidence Levels",
        )
        fig_conf.update_layout(xaxis_title="Confidence", yaxis_title="Count", bargap=0.1)
        st.plotly_chart(fig_conf, use_container_width=True)

    with col2:
        st.subheader("Recommended Action Mix")
        action_counts = recs["recommended_action"].value_counts().reset_index()
        action_counts.columns = ["action", "count"]
        fig_actions = px.bar(
            action_counts,
            x="action",
            y="count",
            color="action",
            color_discrete_map={"boost": "#2ca02c", "keep": "#1f77b4", "pause": "#d62728"},
            title="Action Distribution",
        )
        fig_actions.update_layout(xaxis_title="Action", yaxis_title="Count")
        st.plotly_chart(fig_actions, use_container_width=True)

    # -----------------------------------------------------------------
    # Step 3: Recommendation Table
    # -----------------------------------------------------------------
    st.subheader("Recommendation Details")
    st.dataframe(
        recs[
            [
                "campaign_id",
                "creative_id",
                "platform",
                "recommended_action",
                "confidence",
                "expected_reward",
                "reasoning",
            ]
        ],
        use_container_width=True,
        height=420,
    )

    # -----------------------------------------------------------------
    # Step 4: Summary Insights
    # -----------------------------------------------------------------
    st.markdown("### 🔍 Quick Insights")

    avg_conf = recs["confidence"].mean()
    low_conf = (recs["confidence"] < 0.5).mean() * 100
    boost_rate = (recs["recommended_action"] == "boost").mean() * 100

    st.markdown(
        f"""
        - **Average Model Confidence:** {avg_conf:.2f}  
        - **Low Confidence (<0.5) Recommendations:** {low_conf:.1f}%  
        - **Boost Recommendations:** {boost_rate:.1f}%  
        """
    )

    st.success("✅ Dashboard generated successfully.")

    # -----------------------------------------------------------------
    # Step 5: Optional — Add a refresh button
    # -----------------------------------------------------------------
    if st.button("🔁 Refresh Recommendations"):
        st.rerun()


# =========================================================
# Local Debug Run (optional)
# =========================================================
if __name__ == "__main__":
    st.set_page_config(page_title="🧠 AI Decision Monitor", layout="wide")
    render()
