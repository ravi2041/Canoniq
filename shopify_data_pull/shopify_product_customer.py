import requests
import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
from config import MYSQL_CONFIG
from datetime import datetime
import json
import time

load_dotenv()

SHOP_URL = os.getenv("SHOPIFY_SHOP")
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-07")
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")

SHOPIFY_GRAPHQL_URL = f"https://{SHOP_URL}/admin/api/{API_VERSION}/graphql.json"
HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": ACCESS_TOKEN
}

# ---------- GraphQL with light retry ----------
def fetch_graphql(entity: str, query: str, retries: int = 3, backoff: float = 0.5):
    payload = {"query": query}
    for i in range(retries):
        r = requests.post(SHOPIFY_GRAPHQL_URL, headers=HEADERS, json=payload, timeout=60)
        try:
            r.raise_for_status()
            data = r.json()
            if "errors" in data:
                # Non-retryable GraphQL errors
                raise RuntimeError(f"GraphQL error for {entity}: {json.dumps(data['errors'][:2], indent=2)}")
            if "data" not in data:
                raise RuntimeError(f"No 'data' in GraphQL response for {entity}: {data}")
            return data
        except (requests.HTTPError, requests.ConnectionError, requests.Timeout) as e:
            if i < retries - 1:
                time.sleep(backoff * (2 ** i))
                continue
            raise RuntimeError(f"Request error while fetching {entity}: {e}") from e

# ---------- Queries ----------
QUERIES = {
    "products": '''{
      products(first: 200) {
        edges {
          node {
            id
            title
            vendor
            status
            createdAt
            totalInventory
            variants(first: 50) {
              edges {
                node {
                  id
                  title
                  price
                  sku
                }
              }
            }
          }
        }
      }
    }''',
    "customers": '''{
      customers(first: 200) {
        nodes {
          id
          firstName
          lastName
          email
          phone
          createdAt
          updatedAt
          numberOfOrders
          state
          amountSpent { amount currencyCode }
          verifiedEmail
          taxExempt
          tags
          defaultAddress {
            id
            address1
            city
            province
            country
            zip
            phone
            provinceCode
            countryCodeV2
          }
        }
      }
    }'''
}

def convert_shopify_datetime(dt_str):
    try:
        return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

# ---------- Flatteners ----------
def flatten_products(data):
    rows = []
    for edge in data["data"]["products"]["edges"]:
        product = edge["node"]
        created_at = convert_shopify_datetime(product.get("createdAt"))
        variants = (product.get("variants") or {}).get("edges") or []
        if not variants:
            # still emit a product row without variant if desired; here we skip
            continue
        for variant_edge in variants:
            variant = variant_edge["node"]
            price_val = variant.get("price")
            try:
                price = float(price_val) if price_val is not None else None
            except Exception:
                price = None
            rows.append({
                "product_id": product.get("id"),
                "sku": variant.get("sku"),
                "product_title": product.get("title"),
                "product_vendor": product.get("vendor"),
                "product_status": product.get("status"),
                "created_at": created_at,
                "total_inventory": product.get("totalInventory"),
                "variant_id": variant.get("id"),
                "variant_title": variant.get("title"),
                "variant_price": price,
            })
    df = pd.DataFrame(rows)
    # NaN -> None for SQL NULL
    return df.where(pd.notna(df), None)

def flatten_customers(data):
    rows = []
    for node in data["data"]["customers"]["nodes"]:
        created_at = convert_shopify_datetime(node.get("createdAt"))
        updated_at = convert_shopify_datetime(node.get("UpdatedAt") or node.get("updatedAt"))
        amt = (node.get("amountSpent") or {}).get("amount")
        try:
            amount_spent = float(amt) if amt is not None else None
        except Exception:
            amount_spent = None

        rows.append({
            "customer_id": node.get("id"),
            "customer_first_name": node.get("firstName"),
            "customer_last_name": node.get("lastName"),
            "customer_email": node.get("email"),
            "customer_phone": node.get("phone"),
            "customer_created_at": created_at,
            "customer_updated_at": updated_at,
            "customer_number_of_orders": node.get("numberOfOrders"),
            "state": node.get("state"),
            "amount_spent": amount_spent,
            "currency": (node.get("amountSpent") or {}).get("currencyCode"),
            "verified_email": node.get("verifiedEmail"),
            "tax_exempt": node.get("taxExempt"),
            "tags": ", ".join(node.get("tags") or []) if node.get("tags") else None,
            # some minimal address fields in case your table includes them
            "default_address_id": (node.get("defaultAddress") or {}).get("id"),
            "default_city": (node.get("defaultAddress") or {}).get("city"),
            "default_country": (node.get("defaultAddress") or {}).get("country"),
        })
    df = pd.DataFrame(rows)
    return df.where(pd.notna(df), None)

# ---------- Robust MySQL upsert ----------
def save_to_mysql_upsert(df: pd.DataFrame, table_name: str, pk_cols=None, batch_size: int = 1000):
    """
    Upsert df into MySQL table:
      - trims to DB columns
      - NaN -> NULL
      - INSERT ... ON DUPLICATE KEY UPDATE (non-PK cols)
    """
    if df is None or df.empty:
        print(f"⚠️ No data to save for {table_name}")
        return 0

    # Clean headers
    kept = []
    for c in df.columns:
        s = "" if c is None else str(c).strip()
        if s and s.lower() != "nan":
            kept.append(s)
    df = df.loc[:, kept].copy()
    df = df.where(pd.notna(df), None)

    conn = mysql.connector.connect(**MYSQL_CONFIG)
    rows_written = 0
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"SHOW COLUMNS FROM `{table_name}`")
        db_cols = [row["Field"] for row in cur.fetchall()]
        db_cols_set = set(db_cols)

        # trim to DB columns
        keep_cols = [c for c in df.columns if c in db_cols_set]
        if not keep_cols:
            raise RuntimeError(
                f"No matching columns to insert for {table_name}. "
                f"DF cols={list(df.columns)}; DB cols={db_cols}"
            )
        df = df[keep_cols]

        # Build UPSERT
        cols_sql = ", ".join(f"`{c}`" for c in keep_cols)
        placeholders = ", ".join(["%s"] * len(keep_cols))
        pk_cols = set(pk_cols or [])
        upd_cols = [c for c in keep_cols if c not in pk_cols]
        update_sql = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in upd_cols]) if upd_cols else ""

        sql = f"INSERT INTO `{table_name}` ({cols_sql}) VALUES ({placeholders})"
        if update_sql:
            sql += f" ON DUPLICATE KEY UPDATE {update_sql}"

        # executemany in batches
        n = len(df)
        for i in range(0, n, batch_size):
            batch = df.iloc[i:i+batch_size]
            rows = [tuple(r) for r in batch.itertuples(index=False, name=None)]
            if rows:
                cur.executemany(sql, rows)
                rows_written += len(rows)
        conn.commit()
        return rows_written
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

# ---------- Runner ----------
def run_all():
    print("Fetching Products...")
    products_data = fetch_graphql("products", QUERIES["products"])
    if products_data:
        products_df = flatten_products(products_data)
        # IMPORTANT: set PK columns to avoid overwriting PK field on update
        # Typically variant_id is PRIMARY KEY in shopify_products
        save_to_mysql_upsert(products_df, "shopify_products", pk_cols=["variant_id"])

    print("Fetching Customers...")
    customers_data = fetch_graphql("customers", QUERIES["customers"])
    if customers_data:
        customers_df = flatten_customers(customers_data)
        # Typically customer_id is PRIMARY KEY in shopify_customers
        save_to_mysql_upsert(customers_df, "shopify_customers", pk_cols=["customer_id"])

    print("✅ Data pulled and upserted for products and customers")

if __name__ == "__main__":
    run_all()
