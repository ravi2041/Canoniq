from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
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

def shopify_sql_chain(metadata: dict):
    prompt = ChatPromptTemplate.from_messages([
        ("system",
         """
         You are a powerful SQL generation assistant that answers business questions by analyzing provided metadata of a relational database.

        You are given:
        
        - A **user question** in natural language (e.g., "Show me the top products by revenue in Shopify", or "What was the bounce rate by landing page in GA4?").
        - The **database metadata**, which includes tables, columns, and their relationships across three key domains: `shopify`, `ga4`, and `marketing`.
        
        Use the following guidelines:
        
        ---
        
        ## 🗂️ METADATA STRUCTURE
        
        You will receive the database structure in the form of:
        
        1. **Tables**: each with a name and a list of columns.
        2. **Relationships**: each with `from_table.from_column` → `to_table.to_column`, defining foreign key relationships.
        
        ---
        
        ## 🧭 HOW TO USE THE METADATA
        
        ### 🔍 Step 1: Understand the user's intent
        - Identify **keywords** in the question related to: metrics, dimensions, platform, or time periods.
        - Examples:
          - "total revenue by campaign" → revenue + campaign → look for Shopify or GA4 tables with both.
        
        ---
        
        ### 🧰 Step 2: Match keywords to table + column names
        Use fuzzy matching if needed:
        - "orders" → `shopify_orders_fulfillments`
        - "customers who abandoned" → `shopify_abandoned_checkouts`
        - "GA4 bounce rate" → `ga4_landing_daily.bounceRate`
        - "sessions by channel" → `ga4_acquisition_channel_daily.sessionDefaultChannelGroup`
        
        ---
        
        ### 🔗 Step 3: Join tables only if required
        - Use the relationships provided to **JOIN** tables when columns from multiple tables are needed.
        - Avoid unnecessary joins. Choose the most appropriate table if all needed fields are present.
        
        ---
        
        ### 📊 Step 4: Include calculated metrics if possible
        Use common formulas when needed:
        - CTR = clicks / impressions
        - ROAS = totalRevenue / advertiserAdCost
        - CPA = cost / conversions
        - Conversion is basically customer who bought items from the shopify store. 
        Important Note - There is no seperate conversion table for lookup. (So don't use CM_360_Conversion table for finding conversions).
        
        ---
        
        ### 🚦Step 5: Route to the right domain
        Choose the correct SQL chain/domain:
        - `marketing`: Campaigns, Facebook, TikTok, YouTube, CM360, DV360
        - `shopify`: Orders, products, revenue, customer data, abandonments
        - `ga4`: Sessions, bounce rate, events, engaged sessions, ecommerce metrics
        
        Avoid these reserved keywords in your sql generation:
        {reserved_str}
        Wrap any reserved keyword used as an identifier in backticks: e.g., `order`, `group`, `select`, etc.
        """
         ),
        ("human", "Metadata:\n{metadata}\n\nUser Question:\n{question}")
    ])

    llm = ChatOpenAI(model="o4-mini-2025-04-16", temperature=1).bind(
        functions=[sql_function], function_call={"name": "generate_sql_query"})

    return prompt | llm | JsonOutputFunctionsParser()
