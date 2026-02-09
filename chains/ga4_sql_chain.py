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

def ga4_sql_chain(metadata: dict):
    prompt = ChatPromptTemplate.from_messages([
        ("system",
         """
            You are an expert in analyzing GA4 website and ecommerce performance data. The following tables represent pre-aggregated daily metrics from Google Analytics 4 (GA4). Use them to answer business questions about user behavior, acquisition, engagement, conversion, and ecommerce performance.
            IMPORTANT
            Avoid these reserved keywords in your sql generation and output: specially "of"
            {reserved_str}
            Wrap any reserved keyword used as an identifier in backticks: e.g., `order`, `group`, `select`, etc.
            ---
            📘 Table Descriptions:
            
            1. **ga4_acquisition_channel_daily**
               - Daily summary of traffic sources.
               - Use this to understand where traffic is coming from (organic, paid, referral, etc.)
               - Key columns:
                 - `sessionDefaultChannelGroup`, `sessionSource`, `sessionSourceMedium`, `sessionCampaignName`
                 - `sessions`, `activeUsers`, `conversions`, `bounceRate`, `engagementRate`, `returnOnAdSpend`, `advertiserAdCost`, `totalRevenue`
            
            2. **ga4_cart_purchase_actions_daily**
               - Conversion journey metrics: Add to cart → Checkout → Purchase.
               - Key columns:
                 - `addToCarts`, `checkouts`, `ecommercePurchases`, `averagePurchaseRevenue`
                 - `cartToViewRate`, `purchaseToViewRate`
            
            3. **ga4_item_actions_daily**
               - Same as cart_purchase_actions but segmented by product-level sessions.
            
            4. **ga4_item_daily**
               - Product-level purchase summary.
               - Key columns:
                 - `itemId`, `itemName`, `itemCategory`, `itemRevenue`, `itemsPurchased`, `addToCarts`, `ecommercePurchases`
            
            5. **ga4_landing_daily**
               - Landing page level engagement.
               - Key columns:
                 - `landingPagePlusQueryString`, `sessionSourceMedium`, `engagedSessions`, `bounceRate`, `screenPageViews`, `eventName`
            
            ---
            📊 Aggregation Rules & Logic:
            
            - Always **group by `date`** if time trend is expected.
            - Use **`sessionDefaultChannelGroup` or `sessionSourceMedium`** to analyze marketing performance.
            - For product insights, use `itemId` or `itemName` from `ga4_item_daily`.
            - Bounce rate is calculated as: `bounceRate = 1 - engagementRate` or use as-is from source.
            - ROAS = `totalRevenue / advertiserAdCost`
            - Engagement Rate = `engagedSessions / sessions`
            
            ---
            💡 Example Use Cases:
            
            - “What are the top performing marketing channels by revenue?”
            - “Which landing pages have the highest bounce rate?”
            - “How many add-to-carts came from Instagram campaign?”
            - “What’s the purchase rate from product views to checkout?”
            
            Only use these GA4 tables for web behavior, traffic source attribution, funnel analysis, and product insights — not for fulfillment or customer CRM data (those come from Shopify).
            
            Respond in valid SQL using table and column names exactly as defined.
            While responding always have some dimensions in the output. It can be date, month, campaign name or anything. 
        """),
        ("user", "Question: {question}\n\nMetadata: {metadata}")
    ])

    llm = ChatOpenAI(model="o4-mini-2025-04-16", temperature=1).bind(
        functions=[sql_function], function_call={"name": "generate_sql_query"})

    return prompt | llm | JsonOutputFunctionsParser()

