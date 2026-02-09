"""
feature_builder.py
------------------
Utility to transform raw campaign/placement/platform context
into model-ready numeric feature vectors.
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder

def build_context_features(df: pd.DataFrame, numeric_features=None, categorical_features=None):
    """
    Transforms campaign/placement/platform context into numeric model features.

    Args:
        df (pd.DataFrame): raw data with categorical and numeric features.
        numeric_features (list): columns to use as numeric.
        categorical_features (list): columns to encode as categorical.

    Returns:
        tuple: (X, feature_names)
            X: np.ndarray feature matrix ready for model
            feature_names: list of feature names for interpretability
    """
    if numeric_features is None:
        numeric_features = ["ctr", "cvr", "cpc", "cpa", "impressions", "cost"]

    if categorical_features is None:
        categorical_features = ["platform", "placement", "campaign"]

    # Fill missing text values to avoid encoder errors
    df[categorical_features] = df[categorical_features].fillna("unknown")

    # One-hot encode categorical columns
    encoder = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    encoded = encoder.fit_transform(df[categorical_features])
    encoded_cols = encoder.get_feature_names_out(categorical_features)

    # Combine numeric + encoded categorical features
    X_numeric = df[numeric_features].fillna(0).values
    X = np.concatenate([X_numeric, encoded], axis=1)

    # Feature names for debugging & explainability
    feature_names = numeric_features + list(encoded_cols)

    print(f"✅ Context features built: {len(feature_names)} columns")
    return X, feature_names, encoder
