"""
bandit.py
----------
Bayesian Linear Thompson Sampling (LinTS) implementation
for adaptive decision-making based on contextual features.

This model learns weights for each feature and action pair
to estimate expected reward distributions.
"""

import numpy as np
from collections import defaultdict

class BayesianLinTS:
    def __init__(self, d: int, alpha: float = 1.0):
        """
        Args:
            d (int): number of features in context vector
            alpha (float): exploration factor; higher = more exploration
        """
        self.d = d
        self.alpha = alpha

        # Per-action posterior parameters
        self.A = defaultdict(lambda: np.identity(d))  # covariance matrices
        self.b = defaultdict(lambda: np.zeros((d, 1)))  # reward-weighted feature sums

    def sample_action(self, x: np.ndarray, actions=None) -> float:
        """
        Samples a reward estimate for each action based on the posterior
        and returns the expected reward for that action.

        Args:
            x (np.ndarray): normalized context vector
            actions (list): optional list of actions

        Returns:
            dict or float:
              - If actions is provided → dict[action] = sampled reward
              - If single action context → float (expected reward sample)
        """
        if actions is None:
            # Single action mode
            actions = ["default"]

        rewards = {}
        for a in actions:
            A_inv = np.linalg.inv(self.A[a])
            mu = A_inv @ self.b[a]
            theta_sample = np.random.multivariate_normal(mu.ravel(), self.alpha**2 * A_inv)
            rewards[a] = float(np.dot(theta_sample, x))

        # If only one action asked, return scalar
        if len(actions) == 1:
            return list(rewards.values())[0]

        return rewards

    def update(self, x: np.ndarray, action: str, reward: float):
        """
        Update posterior parameters for a specific action.

        Args:
            x (np.ndarray): context vector
            action (str): action name (e.g., 'boost', 'pause')
            reward (float): observed reward (e.g., uplift)
        """
        x = x.reshape(-1, 1)
        self.A[action] += np.dot(x, x.T)
        self.b[action] += reward * x

    def expected_reward(self, x: np.ndarray, action: str) -> float:
        """
        Compute mean expected reward (without random sampling).

        Args:
            x (np.ndarray): context vector
            action (str): action name

        Returns:
            float: mean reward estimate
        """
        A_inv = np.linalg.inv(self.A[action])
        mu = A_inv @ self.b[action]
        return float(np.dot(mu.T, x))



if __name__ == "__main__":
    # Suppose our context has 4 features (like CTR, CVR, Spend, ROAS)
    bandit = BayesianLinTS(d=4, alpha=1.0)

    # Simulate a context (example campaign data)
    x = np.array([0.5, 0.2, 0.1, 0.7])

    # Step 1: Select an action based on this context
    action, score = bandit.select(x)
    print(f"Chosen action: {action}, predicted reward: {score:.3f}")

    # Step 2: Simulate receiving a reward from that action
    reward = np.random.uniform(0, 1)  # dummy reward
    bandit.update(x, action, reward)