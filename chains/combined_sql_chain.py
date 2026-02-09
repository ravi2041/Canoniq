from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain.output_parsers.openai_functions import JsonOutputFunctionsParser
from reserve_keywords import MYSQL_RESERVED_KEYWORDS


sql_function = {
    "name": "generate_sql_query",
    "description": "Generate a valid SQL query based on user question and database metadata. Include GROUP BY, ORDER BY, and reasoning when applicable.",
    "parameters": {
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": "A valid MySQL 8.4 SQL query reflecting the user's question, with correct filtering, joining, grouping, and ordering."
            },
            "group_by": {
                "type": "array",
                "description": "Optional list of columns the query groups by (for aggregations).",
                "items": {"type": "string"}
            },
            "order_by": {
                "description": "Optional ordering instructions. Can be an array of {field, dir} objects, or strings like 'conversions desc'.",
                "oneOf": [
                    {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "field": {"type": "string", "description": "Column or alias to order by"},
                                "dir": {
                                    "type": "string",
                                    "enum": ["asc", "desc"],
                                    "default": "desc",
                                    "description": "Sort direction"
                                }
                            },
                            "required": ["field"]
                        }
                    },
                    {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Each item like 'field desc' or 'field asc'."
                    },
                    {
                        "type": "string",
                        "description": "Single space-delimited instruction like 'revenue desc'."
                    }
                ]
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "description": "Optional LIMIT to restrict rows returned."
            },
            "target_tables": {
                "type": "array",
                "items": {"type": "string"},
                "description": "List of tables used in the SQL query."
            },
            "used_columns": {
                "type": "array",
                "items": {"type": "string"},
                "description": "List of column names used in SELECT, WHERE, GROUP BY, or ORDER BY."
            },
            "reasoning": {
                "type": "string",
                "description": "Explanation of how the query was constructed from the question and metadata."
            }
        },
        "required": ["sql"]
    }
}


reserved_str = ", ".join(MYSQL_RESERVED_KEYWORDS)


def combined_sql_chain(metadata: dict):
    prompt = ChatPromptTemplate.from_messages([
        ("system",
         """
         ### 📌 Objective:
                You are an analytics agent tasked with generating SQL from natural language business questions. The available data sources include:
                - **Google Analytics 4 (GA4)**: Session-based web behavior data
                - **Shopify**: Customer, orders, fulfillment, and product data
                
                
                Your job is to:
                - Correctly infer the relevant **GA4 or Shopify** tables
                - Join data across them where logical (e.g., campaigns to orders)
                - Prioritize metrics that **match the user's question intent**
                 IMPORTANT NOTE:
                 Avoid these reserved keywords in your sql generation and output: specially "of"
                 {reserved_str}
                 Wrap any reserved keyword used as an identifier in backticks: e.g., `order`, `group`, `select`, etc.
                

                
                ### 🧭 GA4 Tables Overview (Web Behavior & Revenue)
                | Table | What It Contains |
                |------------------------------|----------------------------------------------------------------------------------|
                | `ga4_acquisition_channel_daily` | Channel attribution (source/medium/campaign), sessions, conversions, revenue |
                | `ga4_cart_purchase_actions_daily` | Cart actions (add to cart, checkout, purchase), event-level ecom data |
                | `ga4_item_actions_daily` | Item-level cart & purchase actions per campaign/source |
                | `ga4_item_daily` | Product performance: revenue, purchases, add-to-carts |
                | `ga4_landing_daily` | Landing pages, sessions, engagement, conversions, screen views |
                

                
                ### 🛒 Shopify Tables Overview (Transaction & Customer Info)
                | Table | What It Contains |
                |----------------------------|----------------------------------------------------------------------------------|
                | `shopify_orders_fulfillments` | Orders with customer + fulfillment + product details |
                | `shopify_customers` | Customer metadata: email, phone, # of orders, amount spent |
                | `shopify_products` | Product catalog, inventory, price, status |
                | `shopify_discount_applications` | Order-level applied discounts |
                | `shopify_abandoned_checkouts` | Unfinished purchases and related cart info |
                | `shopify_order_attribution` | UTM source/medium/campaign for first & last click on order |
                

                ### 🔁 Mapping GA4 to Shopify (Common Use Cases)
                | Use Case | GA4 Table | Shopify Table | Join Key or Strategy |
                |----------------------------------------------|--------------------------------|----------------------------------|--------------------------------------|
                | Attribution of purchase to campaign | `ga4_acquisition_channel_daily` | `shopify_order_attribution` | Join via `sessionCampaignName ~ campaign` |
                | Product revenue breakdown by platform | `ga4_item_daily` | `shopify_orders_fulfillments` | Join by `itemId ~ product_id` (if mapped) |
                | Cart abandonment tracking | `ga4_cart_purchase_actions_daily` | `shopify_abandoned_checkouts` | Time-based matching / SKU matching |
                | Landing page → Order conversion | `ga4_landing_daily` | `shopify_order_attribution` | Match `landingPage` to `first_landing` |
                | Avg order value vs session engagement | `ga4_acquisition_channel_daily` | `shopify_orders_fulfillments` | Compare metrics by channel/campaign |
                | Customer re-engagement | `ga4_acquisition_channel_daily` | `shopify_customers` | Match via email if available |
                
                
                ### 🧠 Prompt Rules to Follow:
                - 🟢 Use **GA4 tables** when question is about sessions, website traffic, bounce, engagement, or campaigns
                - 🟢 Use **Shopify tables** when the question is about products, orders, fulfillment, discount, or customers
                - 🧩 Use **both** if the question bridges attribution and revenue, e.g., "Which campaign generated the highest revenue last month?"
                - ⏳ Time-based aggregation always uses `date` column
                - 🧪 Avoid double counting: always group by `sessionCampaignName` or `product_id` or `order_id` as needed
                
                ### ✅ Example Interpretations
                | Question | Strategy |
                |----------|----------|
                | "Which campaign drove most revenue last month?" | Use GA4 (`ga4_acquisition_channel_daily`) for campaign + revenue OR join with Shopify `shopify_order_attribution` + `shopify_orders_fulfillments` |
                | "How many add-to-carts per item?" | Use `ga4_item_actions_daily` or `shopify_abandoned_checkouts` depending on whether web or checkout view |
                | "What’s the AOV by source/medium?" | Shopify `orders_fulfillments` joined with `order_attribution` on `order_id` |
                | "Which landing pages resulted in most sales?" | GA4 `ga4_landing_daily` joined with `order_attribution.first_landing` |
                

                ### 🎯 Output Format Reminder
                Ensure SQL outputs:
                - Select only needed columns
                - Group by appropriate keys (`campaign`, `product_id`, `date`, etc.)
                - Join only on mapped fields (`sessionCampaignName`, `itemId`, `order_id`, `sku`)
         """
        ),
        ("human", "Metadata:\n{metadata}\n\nUser Question:\n{question}")
    ])

    llm = ChatOpenAI(model="o4-mini-2025-04-16", temperature=1).bind(
        functions=[sql_function], function_call={"name": "generate_sql_query"})

    return prompt | llm | JsonOutputFunctionsParser()
