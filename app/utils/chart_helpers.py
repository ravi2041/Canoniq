"""
chart_helpers.py
Reusable helper functions for preparing data before visualization.
Handles common use cases like time-series aggregation, top-N filtering,
category limiting, outlier handling, etc.
"""

import pandas as pd
import yaml,os
import re

# -----------------------------
# Load YAML config
# -----------------------------

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "chart_config.yaml")
with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)

# Defaults
DEFAULTS = CONFIG.get("defaults", {})
CHART_RULES = CONFIG.get("charts", {})

# -----------------------------
# Time-related helpers
# -----------------------------

TIME_HINTS = ("date", "day", "week", "month", "year", "quarter")


def coerce_numeric(series: pd.Series) -> pd.Series:
    """
    Convert a series with potential strings/percentages to numeric.
    Examples:
      '25.5%' -> 0.255
      '1,234' -> 1234
    """
    if series.dtype == object:
        series = series.astype(str).str.replace(",", "", regex=False).str.strip()
        # Convert percentages
        pct_mask = series.str.endswith("%")
        series[pct_mask] = series[pct_mask].str.rstrip("%").astype(float) / 100
    return pd.to_numeric(series, errors="coerce")

def resolve_column_name(df, suggested_name: str):
    """Resolve LLM-suggested column name to actual DataFrame column."""
    if not suggested_name:
        return None

    cols = list(df.columns)

    # 1. Exact case-insensitive match
    for col in cols:
        if col.lower() == suggested_name.lower():
            return col

    # 2. Common synonyms for time fields
    time_map = {
        "month": ["month", "report_month", "month_name", "mo", "date"],
        "date": ["date", "day", "report_date", "ds"],
        "year": ["year", "report_year", "yr"]
    }
    if suggested_name.lower() in time_map:
        for alias in time_map[suggested_name.lower()]:
            for col in cols:
                if alias in col.lower():
                    return col

    # 3. Partial substring match
    for col in cols:
        if suggested_name.lower() in col.lower():
            return col

    return None  # if nothing matched



def is_time_like(col_name: str, series) -> bool:
    if col_name is None:
        return False
    if pd.api.types.is_datetime64_any_dtype(series):
        return True
    return any(h in str(col_name).lower() for h in TIME_HINTS)

def coerce_datetime(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        return series
    return pd.to_datetime(series, errors="coerce", infer_datetime_format=True)

def aggregate_timeseries(df, x, y, group_by=None, freq="MS", max_groups=6):
    out = df.copy()
    out[x] = coerce_datetime(out[x])
    out = out.dropna(subset=[x, y])

    if group_by:
        agg = (out.groupby([pd.Grouper(key=x, freq=freq), group_by], as_index=False)[y].sum())
        top = agg.groupby(group_by)[y].sum().nlargest(max_groups).index
        agg = agg[agg[group_by].isin(top)]
    else:
        agg = (out.groupby(pd.Grouper(key=x, freq=freq), as_index=False)[y].sum())

    return agg.sort_values(x)

# -----------------------------
# Ranking & Top-N helpers
# -----------------------------

def top_n(df, y, n):
    if y in df.columns:
        return df.nlargest(n, y)
    return df.head(n)

def limit_categories(df, col, max_categories, other_label="Other"):
    if col not in df.columns:
        return df
    counts = df[col].value_counts()
    keep = counts.nlargest(max_categories).index
    out = df.copy()
    out[col] = out[col].where(out[col].isin(keep), other_label)
    return out

# -----------------------------
# Outlier handling
# -----------------------------

def clip_outliers(df, col, upper_quantile):
    if col not in df.columns:
        return df
    upper = df[col].quantile(upper_quantile)
    out = df.copy()
    out[col] = out[col].clip(upper=upper)
    return out

# -----------------------------
# Convenience dispatcher
# -----------------------------

def prepare_for_chart(df, chart_type, x, y, group_by=None):
    rules = {**DEFAULTS, **CHART_RULES.get(chart_type, {})}

    df_copy = df.copy()

    # --- Force numeric on y if present ---
    if y and y in df_copy.columns:
        df_copy[y] = coerce_numeric(df_copy[y])

    # --- Continue with your existing logic ---
    if chart_type in ("line", "area") and is_time_like(x, df_copy[x]):
        return aggregate_timeseries(
            df_copy, x, y, group_by,
            freq=rules.get("time_freq", "MS"),
            max_groups=rules.get("max_groups", 6)
        )

    elif chart_type in ("bar", "pie"):
        df_copy = top_n(df_copy, y, rules.get("top_n", 15))
        if group_by:
            df_copy = limit_categories(df_copy, group_by, rules.get("max_categories", 10))
        return df_copy

    elif chart_type == "scatter":
        if y and y in df_copy.columns:
            df_copy[y] = coerce_numeric(df_copy[y])
        return clip_outliers(df_copy, y, rules.get("outlier_quantile", 0.99))

    else:
        return df_copy

