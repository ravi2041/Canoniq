# ga4_etl.py (Updated)
import os, math, hashlib
from datetime import date, timedelta
import pandas as pd
from dotenv import load_dotenv
import mysql.connector
from config import MYSQL_CONFIG
from google.analytics.data import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account

load_dotenv()

PROPERTY_ID = os.getenv("GA4_PROPERTY_ID")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
assert PROPERTY_ID and GOOGLE_APPLICATION_CREDENTIALS, "Set GA4_PROPERTY_ID and GOOGLE_APPLICATION_CREDENTIALS"

client = BetaAnalyticsDataClient()

def ga4_run_report(dimensions, metrics, start_date, end_date, filters=None, page_size=10000):
    dims = [Dimension(name=d) for d in dimensions]
    mets = [Metric(name=m) for m in metrics]
    rows = []
    offset = 0

    while True:
        req = RunReportRequest(
            property=f"properties/{PROPERTY_ID}",
            dimensions=dims,
            metrics=mets,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            limit=page_size,
            offset=offset,
            dimension_filter=filters
        )
        resp = client.run_report(req)

        for r in resp.rows:
            row = {}
            for i, d in enumerate(resp.dimension_headers):
                row[d.name] = r.dimension_values[i].value
            for j, m in enumerate(resp.metric_headers):
                row[m.name] = r.metric_values[j].value
            rows.append(row)

        if len(resp.rows) < page_size:
            break
        offset += page_size

    return pd.DataFrame(rows)

def add_lpq_hash_column(df):
    if "landingPagePlusQueryString" in df.columns:
        df["lpq_hash"] = df["landingPagePlusQueryString"].fillna("").apply(
            lambda x: hashlib.md5(x.encode("utf-8")).hexdigest()[:16]
        )
    return df

def upsert_mysql(df: pd.DataFrame, table: str, pk_cols):
    if df is None or df.empty:
        return 0

    INT_LIKE = {
        "sessions", "activeUsers", "engagedSessions", "newUsers",
        "screenPageViews", "eventCount", "itemsPurchased", "purchaserRate",
        "totalUsers"
    }
    DECIMAL_LIKE = {
        "engagementRate", "bounceRate", "averageSessionDuration",
        "conversions", "totalRevenue", "itemRevenue", "cartToViewRate",
        "entrances", "exits", "pageviews", "views", "purchaseRevenue"
    }

    df = df.copy()
    for c in df.columns:
        if c in INT_LIKE:
            df[c] = pd.to_numeric(df[c], errors="coerce").round().astype("Int64")
        elif c in DECIMAL_LIKE:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        elif c == "date":
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date

    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor()

    cur.execute("SHOW TABLES LIKE %s", (table,))
    exists = cur.fetchone() is not None

    def mysql_type(col):
        if col == "date":
            return "DATE"
        if col in INT_LIKE:
            return "INT"
        if col in DECIMAL_LIKE:
            return "DECIMAL(18,6)"
        return "VARCHAR(191)"

    if not exists:
        cols_def = [f"`{c}` {mysql_type(c)}" for c in df.columns]
        pk_sql = ", ".join(f"`{c}`" for c in pk_cols)
        ddl = f"""
        CREATE TABLE `{table}` (
            {", ".join(cols_def)},
            PRIMARY KEY ({pk_sql})
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;
        """
        cur.execute(ddl)
    else:
        cur.execute(f"SHOW COLUMNS FROM `{table}`")
        existing_cols = {row[0] for row in cur.fetchall()}
        for c in df.columns:
            if c not in existing_cols:
                cur.execute(f"ALTER TABLE `{table}` ADD COLUMN `{c}` {mysql_type(c)}")

    conn.commit()

    cols = list(df.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(f"`{c}`" for c in cols)
    upd = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in cols if c not in pk_cols)

    sql = f"INSERT INTO `{table}` ({col_list}) VALUES ({placeholders})"
    if upd:
        sql += f" ON DUPLICATE KEY UPDATE {upd}"

    data = df.astype(object).where(pd.notna(df), None).to_numpy().tolist()
    if data:
        cur.executemany(sql, data)
        conn.commit()

    cur.close(); conn.close()
    return len(data)

if __name__ == "__main__":
    end = date.today().isoformat()
    start = (date.today() - timedelta(days=180)).isoformat()

    # 1) Acquisition by Default Channel Group
    acq = ga4_run_report(
        dimensions=["date", "sessionDefaultChannelGroup","sessionSource","sessionSourceMedium","sessionCampaignName"],
        metrics=["sessions", "activeUsers", "engagedSessions", "engagementRate", "bounceRate", "conversions", "totalRevenue","returnOnAdSpend",
                 "advertiserAdCost"],
        start_date=start, end_date=end
    )

    upsert_mysql(acq, "ga4_acquisition_channel_daily", pk_cols=["date", "sessionDefaultChannelGroup"])

    land = ga4_run_report(
        dimensions=["date", "landingPagePlusQueryString", "sessionSourceMedium", "sessionCampaignName", "eventName"],
        metrics=["sessions", "activeUsers", "engagedSessions", "averageSessionDuration", "conversions", "totalRevenue",
                 "eventCount", "screenPageViews"],
        start_date=start, end_date=end
    )
    land = add_lpq_hash_column(land)  # 🔥 This line is **essential**
    upsert_mysql(
        land,
        "ga4_landing_daily",
        pk_cols=["date", "lpq_hash", "sessionSourceMedium"]
    )

    # 3) Ecommerce item performance
    # Simplified test
    ecom = ga4_run_report(
        dimensions=["date","itemId", "itemName","itemCategory","itemVariant"],
        metrics=["itemRevenue", "itemsPurchased"],
        start_date=start, end_date=end
    )
    upsert_mysql(ecom, "ga4_item_daily", pk_cols=["date", "itemId"])

    # Ecommerce cart/purchase actions (aggregated)
    ecom_actions = ga4_run_report(
        dimensions=["date","sessionMedium","sessionCampaignName","sessionSource","eventName"],  # Or try ["date", "itemId"] if allowed
        metrics=["addToCarts","cartToViewRate","checkouts","purchaseToViewRate", "averagePurchaseRevenue","ecommercePurchases"],
        start_date=start,
        end_date=end
    )
    upsert_mysql(ecom_actions, "ga4_item_actions_daily", pk_cols=["date"])

    print("\n✅ GA4 ETL run complete.")

