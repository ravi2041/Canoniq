"""
recommend_step.py
-----------------
LangGraph node to generate optimization recommendations
based on trained bandit model and recent campaign data.

Now enhanced with context features (platform, placement, campaign)
for more accurate and explainable decisions.
"""

import numpy as np
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helper_fucntions.data_loader import fetch_performance_data
from helper_fucntions.feature_builder import build_context_features
from ai.decision_engine.bandit import BayesianLinTS
from ai.decision_engine.trainer import train_bandit


def recommend_step(state: dict) -> dict:
    """
    Recommends actions (boost / pause / keep) for campaigns or creatives
    based on the current trained bandit model and recent data.

    Args:
        state (dict): Agent state dictionary (LangGraph-compatible).
            Must contain:
              - 'bandit': trained BayesianLinTS model
              - 'encoder' (optional): context encoder from training
              - 'feature_names' (optional): feature column list

    Returns:
        dict: Updated state with recommendations added.
    """

    print("\n🧠 Running recommend_step...")

    # ---------------------------------------------------------------------
    # 1️⃣ Load recent performance data
    # ---------------------------------------------------------------------
    df = fetch_performance_data(days=7)
    if df.empty:
        print("⚠️ No data found for recommendations.")
        return {"recommendations": []}

    numeric_features = ["ctr", "cvr", "cpc", "cpa", "impressions", "cost"]
    categorical_features = ["platform", "placement", "campaign"]

    # Drop incomplete rows
    df = df.dropna(subset=numeric_features)

    # ---------------------------------------------------------------------
    # 2️⃣ Load or initialize Bandit model
    # ---------------------------------------------------------------------
    bandit = state.get("bandit")
    encoder = state.get("encoder")
    feature_names = state.get("feature_names")

    if bandit is None:
        print("⚠️ No trained bandit found in state. Training fresh model...")
        bandit, encoder, feature_names = train_bandit(days=14)
        state["bandit"] = bandit
        state["encoder"] = encoder
        state["feature_names"] = feature_names

    # ---------------------------------------------------------------------
    # 3️⃣ Build context-aware feature matrix
    # ---------------------------------------------------------------------
    X, feature_names, _ = build_context_features(df, numeric_features, categorical_features)

    # ---------------------------------------------------------------------
    # 4️⃣ Generate recommendations using Thompson Sampling
    # ---------------------------------------------------------------------
    actions = ["boost", "keep", "pause"]
    recs = []

    for idx, row in enumerate(X):
        context = np.array(row)
        creative_id = df.iloc[idx].get("creative", f"creative_{idx}")
        campaign_id = df.iloc[idx].get("campaign", "unknown_campaign")
        platform = df.iloc[idx].get("platform", "unknown_platform")

        # Sample reward distribution per possible action
        samples = {a: bandit.sample_action(context) for a in actions}

        # Pick best action with max expected reward
        best_action = max(samples, key=samples.get)

        # Calculate softmax-based confidence
        sample_values = np.array(list(samples.values()), dtype=np.float64)

        # Subtract max for numerical stability
        sample_values -= np.max(sample_values)

        exp_values = np.exp(sample_values)
        confidence = float(exp_values[actions.index(best_action)] / np.sum(exp_values))


        # -----------------------------------------------------------------
        # 5️⃣ Explainable reasoning string
        # -----------------------------------------------------------------
        rec = {
            "campaign_id": campaign_id,
            "creative_id": creative_id,
            "platform": platform,
            "recommended_action": best_action,
            "confidence": round(confidence, 3),
            "expected_reward": round(samples[best_action], 4),
            "reasoning": (
                f"Predicted {best_action.upper()} for creative '{creative_id}' "
                f"on {platform} with {confidence*100:.1f}% confidence "
                f"(expected reward: {samples[best_action]:.3f}). "
                f"Metrics: CTR={df.iloc[idx]['ctr']:.3f}, "
                f"CVR={df.iloc[idx]['cvr']:.3f}, CPA={df.iloc[idx]['cpa']:.2f}."
            ),
        }
        recs.append(rec)

    # ---------------------------------------------------------------------
    # 6️⃣ Store results in state for downstream nodes
    # ---------------------------------------------------------------------
    print(f"✅ Generated {len(recs)} recommendations.")
    state["recommendations"] = recs

    return state


# -------------------------------------------------------------------------
# 🧪 Local test run
# -------------------------------------------------------------------------
if __name__ == "__main__":
    bandit, encoder, feature_names = train_bandit(days=14)
    state = {"bandit": bandit, "encoder": encoder, "feature_names": feature_names}
    new_state = recommend_step(state)

    # Preview top 5 recommendations
    rec_df = pd.DataFrame(new_state["recommendations"])
    print("\n📊 Recommendation Preview:")
    print(rec_df.head())
