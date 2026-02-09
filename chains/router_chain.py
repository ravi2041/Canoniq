# chains/router_chain.py
from langchain.prompts import ChatPromptTemplate
from langchain.output_parsers.openai_functions import JsonOutputFunctionsParser
from langchain_openai import ChatOpenAI

router_prompt = ChatPromptTemplate.from_messages([
    ("system", """
        You are an AI router for query classification. Choose exactly one:
        - "marketing": campaigns, ad spend, impressions, platform performance (FB/TikTok/YouTube/CM360/DV360).
        - "shopify": orders, line items, customers, fulfillments, products, variants, SKU-level revenue/quantity.
        - "ga4": sessions, engagement, events, landing pages, item-level web analytics (itemRevenue, itemsPurchased, addToCarts).
        - "combined": user wants cross-source comparison/join (e.g. Shopify vs GA4).
        - "data_quality": user is asking to validate data, find naming issues, anomalies, ingestion gaps, schema mismatches, or "check data quality".

        If the user says things like:
        - "check anomalies", "validate campaigns", "find naming issues", "data quality", "DQ", "check if all platforms are reporting", "missing data"
          → choose "data_quality".

        Decision rules (schema-aware):
        1) GA4 terms → "ga4".
        2) Shopify words (orders, fulfillment, variant, SKU, product) → "shopify".
        3) Cross-source compare → "combined".
        4) Explicit "data quality"/"validate data"/"naming" → "data_quality".
        5) Else → "marketing".
    """),
    ("user", "Question: {question}\n\nMetadata: {metadata}")
])

function_schema = {
    "name": "classify_sql_chain",
    "description": "Classify chain type based on question.",
    "parameters": {
        "type": "object",
        "properties": {
            "chain_type": {
                "type": "string",
                "enum": ["marketing", "shopify", "ga4", "combined", "data_quality"]
            }
        },
        "required": ["chain_type"]
    }
}

router_chain = (
    router_prompt
    | ChatOpenAI(
        model="o4-mini-2025-04-16",
        reasoning_effort="high"
      ).bind(
        functions=[function_schema],
        function_call={"name": "classify_sql_chain"}
      )
    | JsonOutputFunctionsParser()
)
