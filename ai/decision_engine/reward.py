"""
reward.py
---------
Defines reward function for delivery-based marketing metrics
(impressions, clicks, conversions, CPA, CVR, CTR, CPC, cost).

This reward guides the bandit model toward better efficiency:
- Higher CTR and CVR  → reward increases
- Lower CPC and CPA   → reward increases
- Stable impressions  → small bonus
- Spend spikes        → penalty

The final reward is normalized between -1 and +1.
"""

import numpy as np

# ------------------------------
# Helper Function: Safe Change
# ------------------------------
def safe_change(before: float, after: float) -> float:
    """
    Computes percentage change safely and handles missing or zero data.
    Returns 0 if before is invalid.

    Args:
    -----
    before : float
        Metric value before action (e.g., last 7 days)
    after : float
        Metric value after action (e.g., next 7 days)

    Returns:
    --------
    float : normalized percentage change
    """
    if before <= 0 or np.isnan(before) or np.isnan(after):
        return 0.0
    return (after - before) / before


# ------------------------------
# Core Function: Compute Reward
# ------------------------------
def compute_reward(kpi_before: dict, kpi_after: dict,
                   weights: dict = None) -> float:
    """
    Calculate overall normalized reward for marketing performance metrics.

    Args:
    -----
    kpi_before : dict
        Metrics before the AI action (7-day baseline)
        Example: {"ctr": 0.05, "cvr": 0.01, "cpc": 1.2, "cpa": 40, "impressions": 20000, "cost": 300}
    kpi_after : dict
        Metrics after the AI action (7-day post period)
    weights : dict, optional
        Relative weight for each metric. Should roughly sum to 1.0.

    Returns:
    --------
    float : Reward value between -1 and +1
    """

    # Default weights (you can tune these)
    if weights is None:
        weights = {
            "ctr":  0.35,   # engagement quality
            "cvr":  0.35,   # conversion quality
            "cpc": -0.15,   # cost per click
            "cpa": -0.10,   # cost per acquisition
            "impressions": 0.05  # stability
        }

    reward = 0.0

    # Compute normalized improvement for each metric
    for metric, weight in weights.items():
        before = kpi_before.get(metric, 0)
        after  = kpi_after.get(metric, 0)
        change = safe_change(before, after)

        # For positive metrics (CTR, CVR): higher is better
        # For cost metrics (CPC, CPA): higher is worse, so invert
        if metric in ["cpc", "cpa"]:
            change = -change  # reverse direction for cost metrics

        # Use tanh for normalization (smooths outliers)
        reward_component = weight * np.tanh(change)
        reward += reward_component

    # Small penalty for large cost fluctuation (spend instability)
    cost_before = kpi_before.get("cost", 0)
    cost_after  = kpi_after.get("cost", 0)
    cost_volatility = abs(safe_change(cost_before, cost_after))
    reward -= 0.05 * np.tanh(cost_volatility)

    # Clip to safe range
    reward = float(np.clip(reward, -1, 1))
    return reward


# ------------------------------
# Example Test
# ------------------------------
if __name__ == "__main__":
    before = {
        "ctr": 0.045,
        "cvr": 0.009,
        "cpc": 1.25,
        "cpa": 38.0,
        "impressions": 18000,
        "cost": 290
    }

    after = {
        "ctr": 0.051,
        "cvr": 0.010,
        "cpc": 1.18,
        "cpa": 36.0,
        "impressions": 18500,
        "cost": 300
    }

    r = compute_reward(before, after)
    print(f"Computed Reward: {r:.3f}")
