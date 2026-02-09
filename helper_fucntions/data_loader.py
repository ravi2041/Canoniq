"""
data_loader.py
---------------
Fetch and preprocess campaign performance data from MySQL
for use in the AI decision engine.

Now MySQL-safe and SQLAlchemy-compatible.
"""

import pandas as pd
from sqlalchemy import create_engine
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helper_fucntions.performance_query import query
from config import MYSQL_CONFIG_MARKETING
from sqlalchemy import create_engine
from urllib.parse import quote_plus



def get_engine():
    """Create a SQLAlchemy engine for MySQL with safe credential encoding."""
    user = MYSQL_CONFIG_MARKETING['user']
    password = quote_plus(MYSQL_CONFIG_MARKETING['password'])  # encodes @, %, &, etc.
    host = MYSQL_CONFIG_MARKETING['host']
    port = MYSQL_CONFIG_MARKETING('port', 3306)
    database = MYSQL_CONFIG_MARKETING['database']

    connection_str = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
    return create_engine(connection_str)



def fetch_performance_data(days: int = 14) -> pd.DataFrame:
    """
    Pull aggregated campaign/creative performance metrics for a given lookback window.

    Args:
        days (int): number of days of history to include

    Returns:
        pd.DataFrame: aggregated performance metrics with derived KPIs
    """
 

    engine = get_engine()
    df = pd.read_sql(query, con=engine)

    # Fallback: if any KPI missing, compute in pandas
    for metric, formula in {
        "ctr": lambda d: d["clicks"] / d["impressions"].replace(0, pd.NA),
        "cvr": lambda d: d["conversions"] / d["clicks"].replace(0, pd.NA),
        "cpc": lambda d: d["cost"] / d["clicks"].replace(0, pd.NA),
        "cpa": lambda d: d["cost"] / d["conversions"].replace(0, pd.NA),
    }.items():
        if metric not in df.columns:
            df[metric] = formula(df)

    # Clean up and fill any inf/nan
    df = df.replace([pd.NA, pd.NaT, float("inf"), -float("inf")], 0)
    return df

if __name__ == "__main__":
    df = fetch_performance_data(days=14)
    print(f"✅ Fetched {len(df)} rows of performance data.")
    print(df.head())