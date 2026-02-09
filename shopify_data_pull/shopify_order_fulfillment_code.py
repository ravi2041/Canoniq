# shopify_orders_etl.py
import requests
import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
from datetime import datetime
from config import MYSQL_CONFIG
import json

load_dotenv()

SHOPIFY_GRAPHQL_URL = f"https://{os.getenv('SHOPIFY_SHOP')}/admin/api/2024-07/graphql.json"
HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": os.getenv("SHOPIFY_ACCESS_TOKEN")
}

FULFILLMENT_QUERY = '''
query FulfillmentShow($id: ID!) {
  fulfillment(id: $id) {
    status
    estimatedDeliveryAt
    trackingInfo(first: 10) {
      company
      number
      url
    }
    originAddress {
      address1
      address2
      city
      countryCode
      provinceCode
      zip
    }
  }
}'''

ORDERS_QUERY = '''
{
  orders(first: 50) {
    edges {
      node {
        id
        name
        createdAt
        totalPriceSet { shopMoney { amount } }

        customer {
          id
          firstName
          lastName
          email
          phone
          createdAt
        }

        fulfillments {
          id
        }

        lineItems(first: 10) {
          edges {
            node {
              title
              quantity
              sku
              variant {
                id
                title
                price
                product {
                  id
                  title
                  vendor
                  status
                }
              }
              originalUnitPriceSet { shopMoney { amount } }
            }
          }
        }
      }
    }
  }
}
'''

def convert_shopify_datetime(dt_str):
    return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")

def fetch_graphql(query, variables=None):
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    response = requests.post(SHOPIFY_GRAPHQL_URL, headers=HEADERS, json=payload)
    response.raise_for_status()
    data = response.json()
    if "errors" in data:
        print("GraphQL Error:", json.dumps(data["errors"], indent=2))
        return None
    return data.get("data")

def fetch_orders_with_fulfillments():
    data = fetch_graphql(ORDERS_QUERY)
    if not data:
        return []

    rows = []

    for edge in data["orders"]["edges"]:
        order = edge["node"]
        customer = order.get("customer", {})
        fulfillments = order.get("fulfillments", [])
        line_items = order.get("lineItems", {}).get("edges", [])

        for f in fulfillments:
            fid = f["id"]
            fdata = fetch_graphql(FULFILLMENT_QUERY, {"id": fid})
            fulfillment_info = fdata["fulfillment"] if fdata else {}

            for item in line_items:
                line = item["node"]
                variant = line.get("variant") or {}
                product = variant.get("product") or {}

                rows.append({
                    "order_id": order["id"],
                    "order_name": order["name"],
                    "order_date": convert_shopify_datetime(order["createdAt"]),
                    "order_total_price": float(order["totalPriceSet"]["shopMoney"]["amount"]),

                    "customer_id": customer.get("id"),
                    "customer_first_name": customer.get("firstName"),
                    "customer_last_name": customer.get("lastName"),
                    "customer_email": customer.get("email"),
                    "customer_phone": customer.get("phone"),
                    "customer_created_date": convert_shopify_datetime(customer["createdAt"]) if customer.get("createdAt") else None,

                    "fulfillment_id": fid,
                    "fulfillment_status": fulfillment_info.get("status"),
                    "estimated_delivery_date": fulfillment_info.get("estimatedDeliveryAt"),
                    "tracking_info": json.dumps(fulfillment_info.get("trackingInfo", [])),
                    "origin_address": json.dumps({
                        "address1": fulfillment_info.get("address1"),
                        "address2": fulfillment_info.get("address2")
                    }),

                    "product_title": product.get("title"),
                    "product_vendor": product.get("vendor"),
                    "product_status": product.get("status"),
                    "variant_id": variant.get("id"),
                    "variant_title": variant.get("title"),
                    "variant_price": float(variant.get("price")) if variant.get("price") else 0.0,
                    "sku": line.get("sku"),
                    "quantity": line.get("quantity"),
                    "unit_price": float(line["originalUnitPriceSet"]["shopMoney"]["amount"]) if line.get("originalUnitPriceSet") else None
                })

    return pd.DataFrame(rows)

def save_to_mysql(df, table_name):
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    columns = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    # Convert NaN to None for SQL compatibility
    df = df.where(pd.notnull(df), None)

    for row in df.itertuples(index=False):
        try:
            cursor.execute(insert_sql, tuple(row))
        except Exception as e:
            print(f"❌ Error inserting row {row}: {e}")
    conn.commit()
    cursor.close()
    conn.close()




def run_all():
    print("Fetching Orders and Fulfillments...")
    df = fetch_orders_with_fulfillments()
    if not df.empty:
        save_to_mysql(df, "shopify_orders_fulfillments")
        print("✅ Orders and Fulfillments saved to DB")
    else:
        print("⚠️ No order/fulfillment data fetched")

if __name__ == "__main__":
    run_all()
