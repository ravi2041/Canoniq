import mysql.connector
import pandas as pd
from datetime import datetime
from config import MYSQL_CONFIG



# ---------- HELPER FUNCTIONS ----------
def parse_nz_like_date(s):
    """
    Your CSV shows dates like 9/10/2025 or 30/09/2025.
    This tries day-first (common outside US).
    """
    if pd.isna(s):
        return None
    return datetime.strptime(s, "%d/%m/%Y").date()

def to_int_safe(x):
    if pd.isna(x):
        return 0
    # remove commas like "1,234"
    return int(str(x).replace(",", "").strip())

def to_decimal_from_percent(x):
    """
    '1.36%' -> 1.36 (not 0.0136, we store the percent value itself)
    """
    if pd.isna(x):
        return 0
    x = str(x).strip()
    if x.endswith("%"):
        x = x[:-1]
    return float(x) if x else 0.0

def to_float_safe(x):
    if pd.isna(x) or x == "":
        return 0.0
    return float(x)

# ---------- MAIN INSERT LOGIC ----------
def main():
    # connect
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor()

    # ---------- 1) Conversions report ----------
    conv_path = "C:/Users/Ravi/Downloads/Conversions Report - Google ads.csv"
    conv_df = pd.read_csv(conv_path)

    # rename to match our SQL columns
    conv_df = conv_df.rename(columns={
        "Day": "day",
        "Campaign": "campaign",
        "Ad group": "ad_group",
        "Search keyword": "search_keyword",
        "Conversion source": "conversion_source",
        "Conversion action": "conversion_action",
        "Conversions": "conversions",
    })

    insert_conv_sql = """
        INSERT INTO google_ads_conversions
        (day, campaign, ad_group, search_keyword, conversion_source, conversion_action, conversions)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    conv_rows = []
    for _, row in conv_df.iterrows():
        conv_rows.append((
            parse_nz_like_date(row["day"]),
            row.get("campaign"),
            row.get("ad_group"),
            row.get("search_keyword"),
            row.get("conversion_source"),
            row.get("conversion_action"),
            to_float_safe(row.get("conversions")),
        ))

    cur.executemany(insert_conv_sql, conv_rows)
    print(f"Inserted {cur.rowcount} rows into google_ads_conversions")

    # ---------- 2) Campaign performance report ----------
    camp_path = "C:/Users/Ravi/Downloads/Campaign performance - google ads.csv"
    camp_df = pd.read_csv(camp_path)

    camp_df = camp_df.rename(columns={
        "Date": "date",
        "Campaign": "campaign",
        "Campaign type": "campaign_type",
        "Clicks": "clicks",
        "Impr.": "impressions",
        "CTR": "ctr",
        "Currency code": "currency_code",
        "Avg. CPC": "avg_cpc",
        "Cost": "cost",
        "Conversions": "conversions",
        "Cost / conv.": "cost_per_conv",
        "Conv. rate": "conv_rate",
        "Orders": "orders",
        "Revenue": "revenue",
    })

    insert_camp_sql = """
        INSERT INTO google_ads_campaign_performance
        (date, campaign, campaign_type, clicks, impressions, ctr, currency_code,
         avg_cpc, cost, conversions, cost_per_conv, conv_rate, orders, revenue)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    camp_rows = []
    for _, row in camp_df.iterrows():
        camp_rows.append((
            parse_nz_like_date(row["date"]),
            row.get("campaign"),
            row.get("campaign_type"),
            int(row["clicks"]) if not pd.isna(row["clicks"]) else 0,
            to_int_safe(row["impressions"]),
            to_decimal_from_percent(row["ctr"]),
            row.get("currency_code"),
            to_float_safe(row.get("avg_cpc")),
            to_float_safe(row.get("cost")),
            to_float_safe(row.get("conversions")),
            to_float_safe(row.get("cost_per_conv")),
            to_decimal_from_percent(row.get("conv_rate")),
            int(row["orders"]) if not pd.isna(row["orders"]) else 0,
            to_float_safe(row.get("revenue")),
        ))

    cur.executemany(insert_camp_sql, camp_rows)
    print(f"Inserted {cur.rowcount} rows into google_ads_campaign_performance")

    # commit and close
    conn.commit()
    cur.close()
    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
