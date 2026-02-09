import pandas as pd
import mysql.connector
from datetime import datetime
from config import MYSQL_CONFIG
import math

# === 1. Read the CSV ===
csv_path = "C:/Users/Ravi/Downloads/Sales-Funnel (1).csv"
df = pd.read_csv(csv_path)

# === 2. Helper to parse dates safely ===
def parse_date(val):
    if pd.isna(val):
        return None
    val = str(val).strip()
    if val.lower() == "ongoing":
        return None
    try:
        return datetime.strptime(val, "%Y-%m-%d").date()
    except ValueError:
        return None

# === 3. Connect to MySQL ===
conn = mysql.connector.connect(**MYSQL_CONFIG)
cursor = conn.cursor()

# === 4. Prepare INSERT statement ===
insert_sql = """
INSERT INTO facebook_ads_performance (
    day,
    campaign_id,
    campaign_name,
    ad_set_id,
    ad_set_name,
    ad_id,
    ad_name,
    age,
    objective,
    attribution_setting,
    starts,
    ends,
    impressions,
    link_clicks,
    website_landing_page_views,
    messaging_conversations_started,
    add_to_cart,
    cost_per_add_to_cart,
    checkouts_initiated,
    purchases,
    amount_spent_nzd,
    cpc,
    cpm,
    ctr,
    video_plays_100,
    reporting_starts,
    reporting_ends
) VALUES (
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s
)
"""

# === 5. Iterate over DataFrame rows and insert ===
for _, row in df.iterrows():
    day = parse_date(row["Day"])
    starts = parse_date(row["Starts"])
    reporting_starts = parse_date(row["Reporting starts"])
    reporting_ends = parse_date(row["Reporting ends"])

    # some columns are float in CSV but logically int
    def to_int(x):
        if pd.isna(x):
            return None
        return int(x)

    def to_float(x):
        if pd.isna(x):
            return None
        return float(x)

    data_tuple = (
        day,
        int(row["Campaign ID"]) if not pd.isna(row["Campaign ID"]) else None,
        row["Campaign name"],
        int(row["Ad set ID"]) if not pd.isna(row["Ad set ID"]) else None,
        row["Ad set name"],
        int(row["Ad ID"]) if not pd.isna(row["Ad ID"]) else None,
        row["Ad name"],
        row["Age"],
        row["Objective"],
        row["Attribution setting"],
        starts,
        row["Ends"] if not pd.isna(row["Ends"]) else None,
        to_int(row["Impressions"]),
        to_int(row["Link clicks"]),
        to_int(row["Website landing page views"]),
        to_int(row["Messaging conversations started"]),
        to_int(row["Add to cart"]),
        to_float(row["Cost per Add to cart"]),
        to_int(row["Checkouts initiated"]),
        to_int(row["Purchases"]),
        to_float(row["Amount spent (NZD)"]),
        to_float(row["CPC (cost per link click)"]),
        to_float(row["CPM (cost per 1,000 impressions)"]),
        to_float(row["CTR (all)"]),
        to_int(row["Video plays at 100%"]),
        reporting_starts,
        reporting_ends,
    )

    cursor.execute(insert_sql, data_tuple)

# === 6. Commit and close ===
conn.commit()
cursor.close()
conn.close()
print("Data inserted successfully.")
