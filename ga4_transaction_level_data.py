import pandas as pd
import mysql.connector
from dotenv import load_dotenv
from config import MYSQL_CONFIG
load_dotenv()

# ---- READ CSV ----
csv_path = "C:/Users/Ravi/Downloads/ga4_transaction_level_data.csv"

# read as string so we don't lose things like 1.20228E+17
df = pd.read_csv(
    csv_path,
    dtype={
        "Transaction ID": str,
        "Session campaign": str,
        "Session source": str,
        "Session medium": str,
    }
)

# normalize column names to snake_case
df = df.rename(columns={
    "Transaction ID": "transaction_id",
    "Session campaign": "session_campaign",
    "Session source": "session_source",
    "Session medium": "session_medium",
    "Ecommerce purchases": "ecommerce_purchases",
    "Purchase revenue": "purchase_revenue",
})

# clean numeric columns
df["ecommerce_purchases"] = pd.to_numeric(df["ecommerce_purchases"], errors="coerce").fillna(0).astype(int)
df["purchase_revenue"] = pd.to_numeric(df["purchase_revenue"], errors="coerce").fillna(0.0).round(2)

# ---- INSERT INTO MYSQL ----
conn = mysql.connector.connect(**MYSQL_CONFIG)
cur = conn.cursor()

insert_sql = """
INSERT INTO ga4_transaction_level_data
(transaction_id, session_campaign, session_source, session_medium,
 ecommerce_purchases, purchase_revenue)
VALUES (%s, %s, %s, %s, %s, %s)
"""

rows = []
for _, row in df.iterrows():
    rows.append((
        row.get("transaction_id"),
        row.get("session_campaign"),
        row.get("session_source"),
        row.get("session_medium"),
        int(row.get("ecommerce_purchases", 0)),
        float(row.get("purchase_revenue", 0.0)),
    ))

cur.executemany(insert_sql, rows)
conn.commit()

print(f"Inserted {cur.rowcount} rows.")

cur.close()
conn.close()
