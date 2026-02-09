import mysql.connector
import pandas as pd
import streamlit as st

MYSQL_CONFIG_MARKETING = dict(st.secrets["mysql_marketing"])
MYSQL_CONFIG = dict(st.secrets["mysql_shopify"])
MYSQL_MEMORY_CONFIG = dict(st.secrets["mysql_memory"])
BASE_MYSQL_CONFIG = dict(st.secrets["mysql_base"])


def get_conn():
    return mysql.connector.connect(**MYSQL_CONFIG_MARKETING)

def read_sql(sql: str, params=None) -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=params or [])
