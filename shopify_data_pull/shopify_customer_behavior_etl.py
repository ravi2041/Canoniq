# shopify_behavior_etl.py
import os, json, time, math
from datetime import datetime, timedelta
import requests
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
from datetime import datetime, timedelta, UTC
load_dotenv()
from config import MYSQL_CONFIG

SHOP_URL        = os.getenv("SHOPIFY_SHOP")
API_VERSION     = os.getenv("SHOPIFY_API_VERSION", "2025-07")
ACCESS_TOKEN    = os.getenv("SHOPIFY_ACCESS_TOKEN")


GRAPHQL_URL = f"https://{SHOP_URL}/admin/api/{API_VERSION}/graphql.json"
HEADERS = {"Content-Type": "application/json", "X-Shopify-Access-Token": ACCESS_TOKEN}




# Put this near the top of your file
EXPECTED_COLUMNS = {
    "shopify_order_attribution": [
        "order_id","order_name","created_at",
        "app_name","cj_ready","days_to_conv",
        "first_landing","first_utm_source","first_utm_medium",
        "first_utm_campaign","first_utm_term","first_utm_content",
        "last_landing","last_utm_source","last_utm_medium",
        "last_utm_campaign","last_utm_term","last_utm_content"
    ],
    "shopify_abandoned_checkouts": [
        "ab_checkout_id","checkout_name","created_at","completed_at",
        "abandoned_url","customer_id","customer_email","currency",
        "subtotal","total","line_title","line_variant","sku","qty"
    ],
    "shopify_discount_applications": [
        "order_id","order_name","created_at","currency","app_index",
        "application_typename","allocation_method","target_selection","target_type",
        "value_typename","value","title","description","code"
    ],
    "shopify_discount_allocations": [
        "order_id","line_id","sku","line_name","quantity",
        "alloc_amount","currency",
        "application_typename","allocation_method","target_selection","target_type",
        "value_typename","value","title","description","code"
    ],
    "shopify_subscription_contract_lines": [
        "contract_id","status","created_at","next_billing_date",
        "customer_id","customer_email","line_id","line_title","qty",
        "variant_id","product_id"
    ],
    "shopify_subscription_attempts": [
        "attempt_id","contract_id","created_at","status","amount","currency",
        "next_action","error_code","error_msg"
    ],
}

# put near top of your file
ATTRIB_COLS = [
    "order_id", "order_name", "created_at",
    "app_name", "cj_ready", "days_to_conv",
    "first_landing", "first_utm_source", "first_utm_medium",
    "first_utm_campaign", "first_utm_term", "first_utm_content",
    "last_landing", "last_utm_source", "last_utm_medium",
    "last_utm_campaign", "last_utm_term", "last_utm_content",
]


# ---------- helpers ----------

def _clean_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Drop any bad/unnamed/NaN headers without changing data order."""
    if df is None or df.empty:
        return df
    cols = []
    for c in df.columns:
        if isinstance(c, str):
            s = c.strip()
            if s and s.lower() != "nan":
                cols.append(s)
            else:
                cols.append(None)   # mark for drop
        else:
            # non-string header -> drop
            cols.append(None)
    # Build a mask of valid columns and filter
    mask = [c is not None for c in cols]
    df = df.loc[:, mask].copy()
    # rename the kept columns to their cleaned names
    kept_names = [c for c in cols if c is not None]
    df.columns = kept_names
    return df


def _ts(s):  # parse Shopify ISO to MySQL DATETIME string
    if not s: return None
    # Shopify returns e.g. "2025-08-02T11:30:22Z"
    try: return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
    except: return s

def sanitize_attr_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    # drop bad headers (None / "" / "nan")
    good = []
    for c in df.columns:
        s = "" if c is None else str(c).strip()
        if s and s.lower() != "nan":
            good.append(s)
    df = df.loc[:, good].copy()

    # add any missing expected columns as NULLs (except special handling below)
    for col in ATTRIB_COLS:
        if col not in df.columns:
            df[col] = None

    # special: coerce days_to_conv -> int with NaN/None -> 0
    if "days_to_conv" in df.columns:
        df["days_to_conv"] = pd.to_numeric(df["days_to_conv"], errors="coerce").fillna(0).astype("int64")

    # ensure created_at is MySQL-friendly string (ISO -> 'YYYY-mm-dd HH:MM:SS')
    if "created_at" in df.columns:
        df["created_at"] = df["created_at"].map(_ts)

    # boolean hygiene for cj_ready
    if "cj_ready" in df.columns:
        df["cj_ready"] = df["cj_ready"].map(lambda x: bool(x) if x is not None else None)

    # keep only expected columns in the exact order
    df = df[ATTRIB_COLS]

    # value-level NaN -> None (after we handled days_to_conv)
    df = df.where(pd.notna(df), None)
    return df


def gql(query, variables=None, retries: int = 3, backoff: float = 0.5):
    """Post a GraphQL query with light retry on 429/5xx."""
    payload = {"query": query, "variables": variables or {}}
    for attempt in range(retries):
        r = requests.post(GRAPHQL_URL, headers=HEADERS, json=payload, timeout=60)
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            # Retry on 429/5xx
            if r.status_code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                time.sleep(backoff * (2 ** attempt))
                continue
            raise

        data = r.json()
        if "errors" in data:
            e0 = data["errors"][0]
            path = " → ".join(map(str, e0.get("path") or []))
            msg = e0.get("message") or "GraphQL error"
            raise RuntimeError(f"GraphQL error: {msg} at {path}\n{json.dumps(data['errors'][:2], indent=2)}")
        return data["data"]
    # Fallback (shouldn’t get here)
    raise RuntimeError("GraphQL request failed after retries")

def save_df(df: pd.DataFrame, table: str, pk_cols=None, batch_size: int = 1000):
    if df is None or df.empty:
        return

    # ---- header cleaning (drop None/blank/'nan') ----
    cols = []
    for c in df.columns:
        s = "" if c is None else str(c).strip()
        if s and s.lower() != "nan":
            cols.append(s)
    df = df.loc[:, cols].copy()

    # enforce days_to_conv policy if present
    if "days_to_conv" in df.columns:
        df["days_to_conv"] = pd.to_numeric(df["days_to_conv"], errors="coerce").fillna(0).astype("int64")

    # values: NaN -> None (after coercions)
    df = df.where(pd.notna(df), None)

    conn = mysql.connector.connect(**MYSQL_CONFIG)
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"SHOW COLUMNS FROM `{table}`")
        db_cols = {row["Field"] for row in cur.fetchall()}

        keep_cols = [c for c in df.columns if c in db_cols]
        if not keep_cols:
            raise RuntimeError(
                f"No matching columns to insert for {table}. "
                f"DF cols={list(df.columns)}; DB cols={sorted(db_cols)}"
            )
        df = df[keep_cols]

        cols_sql = ", ".join(f"`{c}`" for c in keep_cols)
        placeholders = ", ".join(["%s"] * len(keep_cols))
        upd_cols = [c for c in keep_cols if not pk_cols or c not in pk_cols]
        update_sql = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in upd_cols]) if upd_cols else ""

        sql = f"INSERT INTO `{table}` ({cols_sql}) VALUES ({placeholders})"
        if update_sql:
            sql += f" ON DUPLICATE KEY UPDATE {update_sql}"

        n = len(df)
        for i in range(0, n, batch_size):
            batch = df.iloc[i:i+batch_size]
            rows = [tuple(r) for r in batch.itertuples(index=False, name=None)]
            if rows:
                cur.executemany(sql, rows)

        conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

# ---------- 1) Abandoned checkouts (inc. recovery) ----------
# Docs: abandonedCheckouts connection + AbandonedCheckout object
# https://shopify.dev/docs/api/admin-graphql/2024-10/queries/abandonedCheckouts
ABANDONED_Q = """
query AbandonedCheckouts($first: Int!, $after: String, $query: String) {
  abandonedCheckouts(first: $first, after: $after, query: $query) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      name
      createdAt
      completedAt
      abandonedCheckoutUrl
      customer { id email firstName lastName state tags }
      subtotalPriceSet { shopMoney { amount currencyCode } }
      totalPriceSet    { shopMoney { amount currencyCode } }
      lineItems(first: 50) {
        nodes {
          title
          quantity
          variantTitle
          sku
        }
      }
    }
  }
}
"""



def _money_from_bag(node):
    if not node or not node.get("shopMoney"):
        return None, None
    m = node["shopMoney"]
    amt = float(m["amount"]) if m.get("amount") is not None else None
    ccy = m.get("currencyCode")
    return amt, ccy

def fetch_abandoned_checkouts(since_iso: str):
    rows, after = [], None
    q = f"created_at:>={since_iso}"
    while True:
        data = gql(ABANDONED_Q, {"first": 100, "after": after, "query": q})
        conn = data["abandonedCheckouts"]
        for n in conn["nodes"]:
            subtotal_amt, subtotal_ccy = _money_from_bag(n.get("subtotalPriceSet"))
            total_amt,    total_ccy    = _money_from_bag(n.get("totalPriceSet"))
            cust = n.get("customer") or {}

            base = {
                "ab_checkout_id": n["id"],
                "checkout_name": n.get("name"),
                "created_at": _ts(n.get("createdAt")),
                "completed_at": _ts(n.get("completedAt")),
                "abandoned_url": n.get("abandonedCheckoutUrl"),
                "customer_id": cust.get("id"),
                "customer_email": cust.get("email"),
                "currency": total_ccy or subtotal_ccy,
                "subtotal": subtotal_amt,
                "total": total_amt,
            }
            for li in (n.get("lineItems") or {}).get("../nodes", []):
                row = base.copy()
                row.update({
                    "line_title":  li.get("title"),
                    "line_variant": li.get("variantTitle"),
                    "sku":         li.get("sku"),
                    "qty":         li.get("quantity"),
                })
                rows.append(row)

        if not conn["pageInfo"]["hasNextPage"]:
            break
        after = conn["pageInfo"]["endCursor"]
        time.sleep(0.2)
    return pd.DataFrame(rows)

# ---------- 2) Orders with Discount usage + allocations ----------
# Docs: DiscountApplication interface on Order
# https://shopify.dev/docs/api/admin-graphql/latest/interfaces/DiscountApplication
ORDERS_DISCOUNTS_Q = """
query OrdersDiscounts($first:Int!, $after:String, $query:String) {
  orders(first:$first, after:$after, query:$query, sortKey:CREATED_AT, reverse:false) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      name
      createdAt
      currencyCode
      customer { id email }
      totalPriceSet { shopMoney { amount currencyCode } }

      # Order-level discount applications (kept if you still want a summary table)
      discountApplications(first: 50) {
        nodes {
          __typename
          allocationMethod
          targetSelection
          targetType
          value {
            __typename
            ... on MoneyV2 { amount currencyCode }
            ... on PricingPercentageValue { percentage }
          }
          ... on ManualDiscountApplication     { title description }
          ... on AutomaticDiscountApplication  { title }
          ... on DiscountCodeApplication       { code }
        }
      }

      # Line items and allocations (now include the discountApplication object per allocation)
      lineItems(first: 100) {
        nodes {
          id
          sku
          name
          quantity
          discountedTotalSet { shopMoney { amount currencyCode } }
          discountAllocations {
            allocatedAmountSet { shopMoney { amount currencyCode } }
            discountApplication {
              __typename
              allocationMethod
              targetSelection
              targetType
              value {
                __typename
                ... on MoneyV2 { amount currencyCode }
                ... on PricingPercentageValue { percentage }
              }
              ... on ManualDiscountApplication     { title description }
              ... on AutomaticDiscountApplication  { title }
              ... on DiscountCodeApplication       { code }
            }
          }
        }
      }
    }
  }
}
"""

def fetch_orders_discounts(since_iso: str):
    rows_app, rows_alloc = [], []
    after, q = None, f"created_at:>={since_iso}"

    while True:
        data = gql(ORDERS_DISCOUNTS_Q, {"first": 100, "after": after, "query": q})
        conn = data["orders"]

        for o in conn["nodes"]:
            oid = o["id"]
            ocurr = o.get("currencyCode")

            # --- order-level applications (optional summary table)
            for i, app in enumerate((o.get("discountApplications") or {}).get("../nodes", [])):
                t = app.get("__typename")
                v = app.get("value") or {}
                vt = v.get("__typename")
                value = (
                    float(v["amount"]) if vt == "MoneyV2" and v.get("amount") is not None
                    else float(v["percentage"]) if vt == "PricingPercentageValue" and v.get("percentage") is not None
                    else None
                )
                title = description = code = None
                if t == "ManualDiscountApplication":
                    title = app.get("title"); description = app.get("description")
                elif t == "AutomaticDiscountApplication":
                    title = app.get("title")
                elif t == "DiscountCodeApplication":
                    code = app.get("code")

                rows_app.append({
                    "order_id": oid,
                    "order_name": o.get("name"),
                    "created_at": _ts(o.get("createdAt")),
                    "currency": ocurr,
                    "app_index": i,  # you can keep a sequential index just for the order-level table
                    "application_typename": t,
                    "allocation_method": app.get("allocationMethod"),
                    "target_selection": app.get("targetSelection"),
                    "target_type": app.get("targetType"),
                    "value_typename": vt,
                    "value": value,
                    "title": title,
                    "description": description,
                    "code": code,
                })

            # --- line-level allocations (use embedded application)
            for li in (o.get("lineItems") or {}).get("../nodes", []):
                for alloc in (li.get("discountAllocations") or []):
                    money = (alloc.get("allocatedAmountSet") or {}).get("shopMoney") or {}
                    app = alloc.get("discountApplication") or {}
                    t = app.get("__typename")
                    v = app.get("value") or {}
                    vt = v.get("__typename")
                    value = (
                        float(v["amount"]) if vt == "MoneyV2" and v.get("amount") is not None
                        else float(v["percentage"]) if vt == "PricingPercentageValue" and v.get("percentage") is not None
                        else None
                    )
                    title = description = code = None
                    if t == "ManualDiscountApplication":
                        title = app.get("title"); description = app.get("description")
                    elif t == "AutomaticDiscountApplication":
                        title = app.get("title")
                    elif t == "DiscountCodeApplication":
                        code = app.get("code")

                    rows_alloc.append({
                        "order_id": oid,
                        "line_id": li.get("id"),
                        "sku": li.get("sku"),
                        "line_name": li.get("name"),
                        "quantity": li.get("quantity"),
                        "alloc_amount": float(money.get("amount")) if money.get("amount") is not None else None,
                        "currency": money.get("currencyCode"),

                        # embedded application summary
                        "application_typename": t,
                        "allocation_method": app.get("allocationMethod"),
                        "target_selection": app.get("targetSelection"),
                        "target_type": app.get("targetType"),
                        "value_typename": vt,
                        "value": value,
                        "title": title,
                        "description": description,
                        "code": code,
                    })

        if not conn["pageInfo"]["hasNextPage"]:
            break
        after = conn["pageInfo"]["endCursor"]
        time.sleep(0.2)

    return pd.DataFrame(rows_app), pd.DataFrame(rows_alloc)

# ---------- 3) Channel attribution (UTMs & journey) ----------
# Docs: CustomerJourneySummary / UTMParameters on Order
# https://shopify.dev/docs/api/admin-graphql/latest/objects/customerjourneysummary
ORDERS_ATTRIB_Q = """
query OrdersAttribution($first:Int!, $after:String, $query:String) {
  orders(first:$first, after:$after, query:$query, sortKey:CREATED_AT) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      name
      createdAt
      app { name }   # keep app info; salesChannel field removed

      customerJourneySummary {
        ready
        daysToConversion
        firstVisit {
          landingPage
          utmParameters { source medium campaign term content }
        }
        lastVisit {
          landingPage
          utmParameters { source medium campaign term content }
        }
      }
    }
  }
}
"""

def fetch_orders_attribution(since_iso: str):
    rows = []
    after, q = None, f"created_at:>={since_iso}"
    print(since_iso)  # verify boundary
    while True:
        data = gql(ORDERS_ATTRIB_Q, {"first": 100, "after": after, "query": q})
        conn = data["orders"]

        for o in conn["nodes"]:
            cj = o.get("customerJourneySummary") or {}
            fv = cj.get("firstVisit") or {}
            lv = cj.get("lastVisit") or {}
            fv_utm = fv.get("utmParameters") or {}
            lv_utm = lv.get("utmParameters") or {}

            rows.append({
                "order_id": o["id"],
                "order_name": o.get("name"),
                "created_at": _ts(o.get("createdAt")),

                # no salesChannel field in this API version
                "app_name": (o.get("app") or {}).get("name"),

                "cj_ready": cj.get("ready"),
                "days_to_conv": cj.get("daysToConversion"),

                "first_landing": fv.get("landingPage"),
                "first_utm_source": fv_utm.get("source"),
                "first_utm_medium": fv_utm.get("medium"),
                "first_utm_campaign": fv_utm.get("campaign"),
                "first_utm_term": fv_utm.get("term"),
                "first_utm_content": fv_utm.get("content"),

                "last_landing": lv.get("landingPage"),
                "last_utm_source": lv_utm.get("source"),
                "last_utm_medium": lv_utm.get("medium"),
                "last_utm_campaign": lv_utm.get("campaign"),
                "last_utm_term": lv_utm.get("term"),
                "last_utm_content": lv_utm.get("content"),
            })

        if not conn["pageInfo"]["hasNextPage"]:
            break
        after = conn["pageInfo"]["endCursor"]
        time.sleep(0.2)

    return pd.DataFrame(rows)

# # ---------- 5) Cohorts (time to repeat) ----------
# # Approach: compute from your Orders table after ingest. Here we reuse Orders we already pull.
#
# def compute_customer_cohorts(orders_df: pd.DataFrame):
#     """
#     Input: orders_df with cols: customer_id, order_id, created_at
#     Output: cohort metrics per customer (days_to_repeat, order_count, first_order_date)
#     """
#     if orders_df is None or orders_df.empty:
#         return pd.DataFrame()
#     df = orders_df[["customer_id", "order_id", "created_at"]].dropna().copy()
#     df["created_at"] = pd.to_datetime(df["created_at"])
#     df = df.sort_values(["customer_id", "created_at"])
#     # compute next purchase delta
#     df["next_purchase_at"] = df.groupby("customer_id")["created_at"].shift(-1)
#     df["days_to_repeat"] = (df["next_purchase_at"] - df["created_at"]).dt.days
#     agg = (df.groupby("customer_id")
#              .agg(first_order=("created_at","min"),
#                   last_order=("created_at","max"),
#                   orders=("order_id","count"),
#                   avg_days_to_repeat=("days_to_repeat","mean"))
#              .reset_index())
#     # round nicely
#     agg["avg_days_to_repeat"] = agg["avg_days_to_repeat"].round(1)
#     return agg


# ---------- RUN ALL ----------
def run_behavior_etl(since_days):
    since = (datetime.now(UTC) - timedelta(days=since_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(datetime.now(UTC))
    print(since)

    # 1) Abandoned checkouts
    df_ab = fetch_abandoned_checkouts(since)
    save_df(df_ab, "shopify_abandoned_checkouts",
            pk_cols=["ab_checkout_id", "sku", "line_title"])

    # 2) Discounts (apps + allocations)
    df_apps, df_allocs = fetch_orders_discounts(since)
    save_df(df_apps, "shopify_discount_applications",
            pk_cols=["order_id", "app_index"])

    if "code" not in df_allocs.columns:
        df_allocs["code"] = ""
    df_allocs["code"] = df_allocs["code"].fillna("")
    save_df(df_allocs, "shopify_discount_allocations",
            pk_cols=["order_id", "line_id", "application_typename", "code"])

    # 3) Attribution
    df_attr = fetch_orders_attribution(since)
    df_attr = sanitize_attr_df(df_attr)
    save_df(df_attr, "shopify_order_attribution", pk_cols=["order_id"])

    print(f"✅ Behavior ETL complete. Rows → abandoned:{len(df_ab)} apps:{len(df_apps)} allocs:{len(df_allocs)} attrib:{len(df_attr)}")
    return {
        "abandoned": df_ab,
        "discount_apps": df_apps,
        "discount_allocs": df_allocs,
        "attribution": df_attr,
    }

if __name__ == "__main__":
    run_behavior_etl(since_days=250)

