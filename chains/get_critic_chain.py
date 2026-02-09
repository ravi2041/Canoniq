# critic_chain.py
import json
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate

CRITIC_RULES = """
            You are a SQL critic. Review SQL for MySQL 8.4 and fix violations.
            
            Hard rules:
            1) Do NOT JOIN across different platform tables (facebook_data, tiktok_data, youtube_data, cm360_data).
            2) Use only columns that exist in each table.
               - Conversions exist ONLY in cm360_conversion_data (total_conversions).
               - Facebook/TikTok/YouTube do NOT have total_conversions.
            3) Cross-platform comparisons: aggregate per platform first, then UNION ALL. No cross-platform JOINs except CM360_Conversions table.
            4) Compute ratios with weighted sums: CTR = SUM(clicks)/NULLIF(SUM(impressions),0). Never AVG(clicks/impressions).
            5) If a metric is not in the schema (e.g., reach, revenue), state unavailability or use a valid proxy (impressions for reach). No hallucinations.
            6) Conversions + engagement joins only within CM360: aggregate cm360_data and cm360_conversion_data separately, then merge on IDs.
            7) Prefer human-readable columns (campaign, placement) in SELECT/OUTPUT over raw *_id.
            
            However there is an exception:
        
            Aggregate each table first, then merge. Conversions will come from Campaign manage which is CM360_conversions table.
            When a user asks for conversions values in a summary/performance report then use CM360 Conversion table to pull conversion values.
            Normalize campaign names identically on both sides before joining:
            
            campaign_name = LOWER(TRIM(REGEXP_REPLACE(campaign, '[^a-z0-9]+', ' ')))
            
            Join can happen on campaign name, placement name or creative name.Top level is campaign name then comes placement name and then creative level.
            If multiple platform rows map to the same campaign_name, aggregate them before joining and use platform information on final result.
            Output format: Return ONLY the final SQL wrapped in <final_sql>...</final_sql> tags.
            """

CRITIC_TEMPLATE = ChatPromptTemplate.from_template(
                """{rules}

                User question:
                {question}
                
                Schema (metadata JSON):
                {schema}
                
                SQL candidate:
                <sql>
                {sql}
                </sql>
                
                Task:
                - Review against the hard rules.
                - If violations, fix them for MySQL 8.4 (no SAFE_DIVIDE).
                - Return ONLY the final SQL wrapped in <final_sql>...</final_sql>.
                """
                )

def get_critic_chain():
    """Returns a Runnable (Prompt → LLM) for the critic."""
    llm = ChatOpenAI(model="o4-mini-2025-04-16", temperature=1)
    return CRITIC_TEMPLATE | llm

def make_critic_inputs(question, schema, sql_candidate):
    """Utility to build the input dict for the chain."""
    schema_text = schema if isinstance(schema, str) else json.dumps(schema, ensure_ascii=False)
    return {
        "rules": CRITIC_RULES,
        "question": question or "",
        "schema": schema_text,
        "sql": (sql_candidate or "").strip(),
    }
