import pandas as pd
import mysql.connector

from config import MYSQL_CONFIG

def load_meta_campaign_to_db(csv_file_path, table_name="meta_campaign_performance"):
    df = pd.read_csv(csv_file_path)

    # Rename columns to match SQL schema
    df.columns = [
        "report_date","campaign_id", "campaign_name","ad_set_id", "ad_set_name","ad_id", "ad_name", "objective", "region",
        "delivery_status", "delivery_level", "attribution_setting", "reach",
        "impressions", "frequency", "amount_spent_nzd", "link_clicks", "cpm",
        "cpc", "ctr_all", "video_plays_25", "reporting_starts", "reporting_ends"
    ]

    # Convert and clean columns
    df["report_date"] = pd.to_datetime(df["report_date"], errors='coerce').dt.date
    df["reporting_starts"] = pd.to_datetime(df["reporting_starts"], errors='coerce').dt.date
    df["reporting_ends"] = pd.to_datetime(df["reporting_ends"], errors='coerce').dt.date

    # Fill numeric fields
    df["campaign_id"] = pd.to_numeric(df["campaign_id"], errors='coerce').fillna(0).astype(int)
    df["ad_set_id"] = pd.to_numeric(df["ad_set_id"], errors='coerce').fillna(0).astype(int)
    df["ad_id"] = pd.to_numeric(df["ad_id"], errors='coerce').fillna(0).astype(int)
    df["reach"] = pd.to_numeric(df["reach"], errors='coerce').fillna(0).astype(int)
    df["impressions"] = pd.to_numeric(df["impressions"], errors='coerce').fillna(0).astype(int)
    df["frequency"] = pd.to_numeric(df["frequency"], errors='coerce').fillna(0.0)
    df["amount_spent_nzd"] = pd.to_numeric(df["amount_spent_nzd"], errors='coerce').fillna(0.0).round(2)
    df["link_clicks"] = pd.to_numeric(df["link_clicks"], errors='coerce').fillna(0).astype(int)
    df["cpm"] = pd.to_numeric(df["cpm"], errors='coerce').fillna(0.0).round(2)
    df["cpc"] = pd.to_numeric(df["cpc"], errors='coerce').fillna(0.0).round(2)
    df["ctr_all"] = pd.to_numeric(df["ctr_all"], errors='coerce').fillna(0.0)
    df["video_plays_25"] = pd.to_numeric(df["video_plays_25"], errors='coerce').fillna(0).astype(int)

    # Fill object columns with None where missing
    obj_cols = [
        "campaign_name", "ad_set_name", "ad_name", "objective", "region",
        "delivery_status", "delivery_level", "attribution_setting"
    ]
    for col in obj_cols:
        df[col] = df[col].fillna(' ')

    # Connect to DB and insert data
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

    for row in df.itertuples(index=False, name=None):
        cursor.execute(insert_sql, row)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Loaded {len(df)} records into `{table_name}`")


def load_meta_funnel_extended(csv_file_path, table_name="meta_funnel_extended"):
    df = pd.read_csv(csv_file_path)

    # Rename columns to match schema
    df.columns = [
        "report_date", "campaign_name", "ad_set_name", "ad_name","campaign_id","ad_set_id","ad_id", "attribution_setting",
        "starts", "ends", "link_clicks", "landing_page_views", "messaging_conversations_started",
        "add_to_cart", "cost_per_add_to_cart", "checkouts_initiated", "purchases",
        "video_plays_25", "video_plays_50", "video_plays_75", "video_plays_100",
        "reporting_starts", "reporting_ends"
    ]

    # Convert date columns
    date_cols = ["report_date", "starts", "ends", "reporting_starts", "reporting_ends"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

    # Numeric columns: fill missing with 0 or 0.0
    int_cols = [
        "campaign_id", "ad_set_id", "ad_id",
        "link_clicks", "landing_page_views", "messaging_conversations_started", "add_to_cart",
        "checkouts_initiated", "purchases", "video_plays_25", "video_plays_50",
        "video_plays_75", "video_plays_100"
    ]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    df["cost_per_add_to_cart"] = pd.to_numeric(df["cost_per_add_to_cart"], errors='coerce').fillna(0.0).round(2)

    # Fill missing text values with None
    text_cols = ["campaign_name", "ad_set_name", "ad_name", "attribution_setting"]
    for col in text_cols:
        df[col] = df[col].fillna('')

    # Connect to DB and insert
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

    for row in df.itertuples(index=False, name=None):
        cursor.execute(insert_sql, row)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Loaded {len(df)} records into `{table_name}`")

# Usage
if __name__ == "__main__":
    load_meta_campaign_to_db("./marketing/Campaign-Level-Performance - meta.csv")
    load_meta_funnel_extended("./marketing/Sales-Funnel - Meta.csv")
