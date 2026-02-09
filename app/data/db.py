import mysql.connector
import pandas as pd
from config import MYSQL_CONFIG_MARKETING

def get_conn():
    return mysql.connector.connect(**MYSQL_CONFIG_MARKETING)

def read_sql(sql: str, params=None) -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=params or [])
