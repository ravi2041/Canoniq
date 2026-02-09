"""
trainer.py
-----------
Uses real campaign data to train the bandit model.

Process:
1. Load data via data_loader
2. Build context vectors (numeric + categorical)
3. Use reward function to compute reward signals
4. Update bandit model iteratively
"""

import numbers
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import numpy as np
from helper_fucntions.data_loader import fetch_performance_data
from ai.decision_engine.bandit import BayesianLinTS
from ai.decision_engine.reward import compute_reward
from helper_fucntions.feature_builder import build_context_features


def train_bandit(days: int = 14, alpha: float = 1.0):
    """Train Thompson Sampling model using live contextual data."""
    print(f"\n🧠 Training Bandit on last {days} days of data...")
    df = fetch_performance_data(days)
    if df.empty:
        print("⚠️ No data fetched.")
        return

    # -----------------------------------------------------------------
    # 1️⃣ Define numeric & categorical features
    # -----------------------------------------------------------------
    numeric_features = ["ctr", "cvr", "cpc", "cpa", "impressions", "cost"]
    categorical_features = ["platform", "placement", "campaign"]

    # Drop rows missing key metrics
    df = df.dropna(subset=numeric_features)

    # -----------------------------------------------------------------
    # 2️⃣ Build context vector using feature builder
    # -----------------------------------------------------------------
    X, feature_names, encoder = build_context_features(df, numeric_features, categorical_features)

    print(f"✅ Context vector built with {len(feature_names)} features.")
    print(f"   → Example features: {feature_names[:8]} ...")

    # -----------------------------------------------------------------
    # 3️⃣ Initialize bandit
    # -----------------------------------------------------------------
    bandit = BayesianLinTS(d=X.shape[1], alpha=alpha)

    # -----------------------------------------------------------------
    # 4️⃣ Simulate past experiences for training
    # -----------------------------------------------------------------
    for idx, row in df.iterrows():
        x = X[idx]
        action = "keep"  # assume all historical data are "kept" creatives

        # Create small perturbation for simulated after-scenario
        kpi_before = row.to_dict()
        kpi_after = {}
        for k, v in kpi_before.items():
            if isinstance(v, numbers.Number):
                kpi_after[k] = v * np.random.uniform(0.95, 1.05)
            else:
                kpi_after[k] = v

        reward = compute_reward(kpi_before, kpi_after)
        bandit.update(np.array(x), action, reward)

    print(f"✅ Bandit training completed on {len(df)} records.")
    return bandit, encoder, feature_names


if __name__ == "__main__":
    bandit, encoder, feature_names = train_bandit(days=14)
