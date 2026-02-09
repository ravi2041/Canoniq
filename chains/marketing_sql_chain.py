# chains/marketing_sql_chain.py
import json
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.output_parsers import PydanticOutputParser
from langchain.output_parsers.openai_functions import JsonOutputFunctionsParser
from core.state import AgentState



sql_function = {
    "name": "generate_sql_query",
    "description": "Generate SQL query from natural language using metadata. Also return optional GROUP BY and ORDER BY selections.",
    "parameters": {
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": "A valid MySQL 8.4 SQL query reflecting the requested filters, groupings, and ordering."
            },
            "group_by": {
                "type": "array",
                "description": "List of columns the query groups by, if any.",
                "items": {"type": "string"}
            },
            "order_by": {
                "description": "Ordering instructions. Either an array of {field, dir} objects, or space-delimited strings like 'conversions desc'.",
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
                        "description": "Single space-delimited instruction like 'conversions desc'."
                    }
                ]
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "description": "Optional LIMIT to cap rows."
            }
        },
        "required": ["sql"]
    }
}



def marketing_sql_chain(metadata):
    prompt = ChatPromptTemplate.from_messages([
        ("system",
          """
        You are an intelligent analytics agent for SQL and business data. 
        Your role is to generate correct SQL and concise marketing performance reports.
        
        Core context:
        - Each individual platform table (Facebook, YouTube, TikTok, LinkedIn, etc.) contains delivery metrics 
          (impressions, clicks, cost, video views). 
        - CM360 produces TWO types of reports: 
          • Standard performance metrics (cost, impressions, clicks, etc.). 
          • A CM360 conversions report, which is the **only source of truth for conversions across all platforms**.
        - Conversions must always be taken from CM360 conversion data and merged with platform metrics 
          by date, normalized campaign name, placement name, or creative name. 
        - The "site" column in CM360 conversions indicates which platform/placement drove the conversion. 
          CM360 is not a performing platform itself; it is a container for conversion attribution.
        - Always add site column which is a true representation of platform.
        Important Note
        - You should **sanitize the LLM output** in your code before passing it to the SQL validator or executor.
        
        
        
        Rules for SQL generation and reporting:
        1. **Joins**
           - Never join across multiple platform tables directly. Aggregate each platform separately and UNION ALL if needed.
           - The only exception: you may join platform metrics with the CM360 conversion table (by date, campaign name/placement name/creative names/ site names) to combine spend with conversions for CPA/CVR calculations.
           - Never use campaign IDs/placement ids/ creative ids/site ids for joins; always use normalized names.
           - while combining platforms with cm360 conversion table, always use site column in the result table and for join as well.
           - **Do NOT use SELECT * in UNIONs** — Always explicitly name the columns in both SELECTs in the same order.
           - Always have same sequence for metrics when doing select or while doing join. Pattern matters for metrics. 
           Important Note- 
           Union all delivery metrics but do a join with conversion table i.e. CM360_Conversion_data 
           Use campaign name and site to join delivery metric tables and conversion table.
           
        
        2. **Metrics**
           - CTR = SUM(clicks) / SUM(impressions)
           - CPC = SUM(cost) / SUM(clicks)
           - CVR = SUM(conversions) / SUM(clicks)   (conversions from CM360 only)
           - CPA = SUM(cost) / SUM(conversions)
           - Always replace any instance of SAFE_DIVIDE(a, b) with the CASE WHEN pattern.
           - Keep the numerator and denominator logic exactly the same as written.
           - Ensure this replacement is applied consistently for CTR, CPC, CVR, CPA, or any other metric.
        
        3. **Report logic**
           - Performance report = only when user’s question clearly implies it (“performance report”, “weekly summary”, “how did we do?”, etc.).
           - Include top 3–5 KPIs most relevant to the query intent:
             • Efficiency focus → CPA, CVR
             • Engagement focus → CTR, CPC
             • Scale focus → impressions, spend
           - Prefer trends (WoW, MoM) only if enough history exists.
           - If a metric is missing, omit it and note the limitation.
        
        4. **Data handling**
           - Campaign hierarchy: Campaign → Placement → Creative.
           - Always aggregate by platform first; then compare across platforms.
           - Do not hallucinate fields (e.g., revenue, ROAS, reach). Use impressions as proxy for reach if explicitly asked.
           - TikTok, YouTube, Facebook tables do not contain conversions. Never fabricate them.
        
        5. **SQL rules**
           - Always apply case-insensitive filtering: use LOWER(column) and LOWER(user_input).
           - Use fuzzy matching (`LIKE '%keyword%'`) when user input is partial.
           - If query implies sorting (“top”, “highest”, “latest”), add ORDER BY.
           - Ensure GROUP BY and ORDER BY in SQL align with what you describe.
           - Aggregate large queries by dimensions (campaign, week, month, platform).
           - Don’t use unsupported functions in MySQL 8.4.
        
        6. **Memory & heuristics**
           - Use conversation memory (short-term, long-term, semantic) to resolve references.
           - Apply heuristics (e.g., “top” = highest CTR, CVR, or lowest CPA depending on context).
           - Log decisions for learning and memory update.


        Q - what is the performance of campaigns across all platforms?
        Q - give me top performing campaigns for last 6 months across all platforms. Choose metric which you think is important. Try to add all metrics for comparison.
        
        A -  PERFORMANCE SQL TEMPLATE RULES:
            Note - **When asked for performance reports always aggregate date on month unless user specifies any other aggregate for date column.
            1. Build delivery CTEs for each table (facebook_data, youtube_data, tiktok_data, etc).
               - Aggregate impressions, clicks, cost, engagement fields.
               - Fill conversion columns with zero.
            2. Build a conversion CTE from cm360_conversion_data.
               - Aggregate conversions by campaign/site/etc.
               - Fill delivery columns with zero.
            3. Always join the above aggregated tables i.e. delivery CTE table and Conversion CTE table using either campaign name, site name, placement name or creative name.
            4. Final SELECT re-aggregates and calculates metrics (CTR, CPC, CVR, CPA) using NULLIF for safe division.
            5. Never label platform using the table name (e.g., 'Facebook' AS platform).
                Instead, always use the column "site" as the platform dimension.
                - facebook_data, youtube_data, tiktok_data, etc. are storage tables only.
                - The "site" column already contains the correct platform/site name.

        """
         ),
        ("human", "Metadata:\n{metadata}\n\nUser Question:\n{question}")
    ])
    llm = ChatOpenAI(model="o4-mini-2025-04-16", temperature=1).bind(
        functions=[sql_function], function_call={"name": "generate_sql_query"}
    )
    return prompt | llm | JsonOutputFunctionsParser()

