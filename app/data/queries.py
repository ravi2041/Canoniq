import pandas as pd
from .db import read_sql
from .config import *

# Platform → Table map (should be defined in config.py)
# PLATFORM_TABLE_MAP = {"facebook": "facebook_data", "google": "google_data", ...}

# -------------------------------
# Date Range Bounds
# -------------------------------
def get_date_bounds(platforms: list[str]) -> tuple:
    if not platforms:
        return None, None

    date_bounds = []
    for p in platforms:
        table = PLATFORM_TABLE_MAP.get(p.lower())
        if not table:
            continue
        q = f"SELECT MIN({DATE_COL}) AS min_d, MAX({DATE_COL}) AS max_d FROM {table}"
        df = read_sql(q)
        if not df.empty:
            date_bounds.append((pd.to_datetime(df["min_d"][0]), pd.to_datetime(df["max_d"][0])))

    if not date_bounds:
        return None, None

    # Compute global min/max from all platform tables
    min_date = min([d[0] for d in date_bounds]).date()
    max_date = max([d[1] for d in date_bounds]).date()
    return min_date, max_date


# -------------------------------
# Get Distinct Dropdown Values
# -------------------------------
def get_distinct(col: str, platforms: list[str], start=None, end=None, limit=1000):
    allowed_fact_cols = {PLATFORM_COL, CAMPAIGN_COL, CREATIVE_COL, PLACEMENT_COL}
    allowed_cm_cols = {CM_ACTIVITY_NAME}  # New: CM360 filterable fields

    distinct_values = set()

    # For FACT table fields
    if col in allowed_fact_cols:
        for p in platforms:
            table = PLATFORM_TABLE_MAP.get(p.lower())
            if not table:
                continue

            sql = f"SELECT DISTINCT {col} AS v FROM {table}"
            params = []

            if start and end:
                sql += f" WHERE {DATE_COL} BETWEEN %s AND %s"
                params += [start, end]

            sql += " ORDER BY v LIMIT %s"
            params += [limit]

            df = read_sql(sql, params)
            distinct_values.update(df["v"].dropna().astype(str).tolist())

    # For CM360 table fields
    elif col in allowed_cm_cols:
        sql = f"SELECT DISTINCT {col} AS v FROM {CM360_TABLE}"
        params = []

        if start and end:
            sql += f" WHERE {CM_DATE_COL} BETWEEN %s AND %s"
            params += [start, end]

        sql += " ORDER BY v LIMIT %s"
        params += [limit]

        df = read_sql(sql, params)
        distinct_values.update(df["v"].dropna().astype(str).tolist())

    else:
        raise ValueError(f"Unknown column '{col}'. Must be one of {allowed_fact_cols.union(allowed_cm_cols)}")

    return sorted(distinct_values)

# -------------------------------
# SQL WHERE Clause Builder
# -------------------------------
def build_where(start, end, platforms, campaigns, creatives, placements):
    clauses, params = [], []
    if start and end:
        clauses.append(f"{DATE_COL} BETWEEN %s AND %s"); params += [start, end]

    def add_in(col, values):
        if values:
            placeholders = ",".join(["%s"] * len(values))
            clauses.append(f"{col} IN ({placeholders})")
            params.extend(values)

    add_in(PLATFORM_COL, platforms)
    add_in(CAMPAIGN_COL, campaigns)
    add_in(CREATIVE_COL, creatives)
    add_in(PLACEMENT_COL, placements)

    where_sql = " WHERE " + " AND ".join(clauses) if clauses else ""
    return where_sql, params


# -------------------------------
# Get Combined Platform Slice + CM360 Conversions
# -------------------------------
NORM_FACT = f"LOWER(REGEXP_REPLACE(REGEXP_REPLACE({CAMPAIGN_COL}, '[^[:alnum:]]+', ' '), '[[:space:]]+', ' '))"
NORM_CM   = f"LOWER(REGEXP_REPLACE(REGEXP_REPLACE({CM_CAMPAIGN_COL}, '[^[:alnum:]]+', ' '), '[[:space:]]+', ' '))"

def get_platform_slice_with_cm360(start, end, platforms, campaigns, creatives, placements):
    where_sql, params = build_where(start, end, [], campaigns, creatives, placements)

    fact_queries = []
    fact_params = []

    if not platforms:
        raise ValueError("At least one platform must be selected")

    for p in platforms:
        table = PLATFORM_TABLE_MAP.get(p.lower())
        if not table or table == CM360_TABLE:
            continue

        q = f"""
        SELECT
            {DATE_COL} AS date,
            '{p}' AS platform,
            {CAMPAIGN_COL} AS campaign,
            {CREATIVE_COL} AS creative,
            {PLACEMENT_COL} AS placement,
            LOWER(REGEXP_REPLACE(REGEXP_REPLACE({CAMPAIGN_COL}, '[^[:alnum:]]+', ' '), '[[:space:]]+', ' ')) AS norm_campaign,
            SUM(impressions) AS impressions,
            SUM(clicks)      AS clicks,
            SUM(cost)        AS cost
        FROM {table}
        {where_sql}
        GROUP BY 1,2,3,4,5,6
        """
        fact_queries.append(q)
        fact_params += params

    if not fact_queries:
        return pd.DataFrame([])

    unioned_fact_sql = "\nUNION ALL\n".join(fact_queries)

    cm_sql = f"""
    SELECT
        {CM_DATE_COL} AS date,
        {NORM_CM} AS norm_campaign,
        activity_id,
        activity,
        SUM({CM_CONVERSIONS_COL}) AS conversions
    FROM {CM360_TABLE}
    WHERE {CM_DATE_COL} BETWEEN %s AND %s
    GROUP BY 1,2,3,4
    """

    full_sql = f"""
    WITH fact AS (
        {unioned_fact_sql}
    ),
    cm AS (
        {cm_sql}
    )
    SELECT
        f.date, f.platform, f.campaign, f.creative, f.placement,activity_id, activity,
        f.impressions, f.clicks, f.cost,
        COALESCE(cm.conversions, 0) AS conversions
    FROM fact f
    LEFT JOIN cm
      ON cm.date = f.date AND cm.norm_campaign = f.norm_campaign
    ORDER BY f.date ASC, f.platform, f.campaign
    """

    return read_sql(full_sql, fact_params + [start, end])
