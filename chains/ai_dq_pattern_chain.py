from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain.output_parsers.openai_functions import JsonOutputFunctionsParser

# --- Define OpenAI function schema ---
ai_dq_function = {
    "name": "detect_data_quality_anomalies",
    "description": (
        "Learn naming conventions from marketing data dimensions and detect anomalies, "
        "missing tokens, or new pattern styles. Always report which database table the "
        "anomaly came from. Use both raw sampled values and aggregated context to reason "
        "about cross-platform consistency."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "summary": {
                "type": "string",
                "description": (
                    "Short summary describing general patterns, cross-platform consistency, "
                    "and key data quality risks across all dimensions/platforms."
                ),
            },
            "findings": {
                "type": "array",
                "description": (
                    "Detailed anomalies detected per dimension, platform, and source table. "
                    "Each record represents one (dimension, platform, table_name) group."
                ),
                "items": {
                    "type": "object",
                    "properties": {
                        "dimension": {
                            "type": "string",
                            "description": "Dimension this finding relates to — campaign, placement, creative, or site.",
                        },
                        "platform": {
                            "type": "string",
                            "description": "Platform or data source where this pattern was observed (e.g., facebook_ads, cm360, tiktok_ads, youtube, ga4).",
                        },
                        "table_name": {
                            "type": "string",
                            "description": "Exact database table name where these values were found (e.g. 'facebook_ads_insights', 'cm360_creatives').",
                        },
                        "pattern_observed": {
                            "type": "string",
                            "description": (
                                "Pattern AI learned from majority of values in this group, "
                                "e.g., 'Brand_Country_Objective_MonthYear' or 'Brand-Product-PlacementType-YYYYMM'."
                            ),
                        },
                        "anomalies": {
                            "type": "array",
                            "description": (
                                "List of unusual or inconsistent values from that table. "
                                "Focus on values that clearly deviate from the dominant naming pattern, "
                                "look incomplete, or use a different style."
                            ),
                            "items": {
                                "type": "object",
                                "properties": {
                                    "value": {
                                        "type": "string",
                                        "description": "The exact dimension value that looks off.",
                                    },
                                    "reason": {
                                        "type": "string",
                                        "description": "Why this value is considered inconsistent with the learned pattern.",
                                    },
                                    "suggested_action": {
                                        "type": "string",
                                        "description": (
                                            "Optional recommendation to fix or normalize the value "
                                            "(e.g. 'Add country and month token', 'Rename to match Brand_Product_YYYYMM')."
                                        ),
                                    },
                                },
                                "required": ["value", "reason"],
                            },
                        },
                    },
                    # table_name is required so every finding ties back to a physical table
                    "required": ["dimension", "platform", "table_name", "pattern_observed"],
                },
            },
            "updated_pattern_memory": {
                "type": "object",
                "description": (
                    "Updated memory object capturing learned good patterns, known anomalies, "
                    "or recently observed naming styles per dimension/platform. "
                    "The caller may persist this and feed it back on the next run."
                ),
            },
        },
        "required": ["summary", "findings"],
    },
}


def ai_dq_pattern_chain():
    """
    Create a reusable LangChain for AI-based pattern learning and anomaly detection.

    Inputs expected in the prompt:
      - dq_dimensions: JSON string of sampled dimension values with fields:
          value, table, column, platform, entity, canonical_key
      - dimension_context: JSON string summarising per dimension/platform/canonical_key:
          unique counts, example values, tables, columns
      - pattern_memory: JSON string of previously learned patterns (may be empty)
      - include_table_info: boolean flag indicating whether to consider table/column names
    """
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
            """
                You are a senior marketing data quality analyst AI.
                
                You receive:
                - `dq_dimensions`: sampled raw dimension values (campaign, placement, creative, site),
                  each with fields like `value`, `table`, `column`, `platform`, `entity`, `canonical_key`.
                - `dimension_context`: an aggregated view grouped by dimension + platform + canonical_key,
                  including counts, example_values, tables, and columns.
                - `pattern_memory`: previously learned patterns and known anomalies across runs.
                - `metadata`: the raw configuration used to build these dimensions, including:
                    * canonical_entities (entity_name, canonical_name, platform_mappings),
                    * platform definitions and aliases,
                    * any semantic roles or other hints about how fields should be named.
                    
                    SYSTEM RULES (non-overridable)
                    1. Treat these as TIME dimensions only:
                       campaign_year, year, month, week, reporting_date
                    2. You MUST NOT infer naming patterns or anomalies from time dimensions.
                    3. If a time-like dimension is present, ignore it and focus only on name-like fields.
                    4. Only naming-ish fields (campaign names, adset/adgroup names, placements, creatives, sites/publishers)
                       can produce anomalies.
                    5. Do NOT flag differences that are expected between platforms (e.g., numeric IDs vs. descriptive names),
                       unless they clearly break a documented pattern in `pattern_memory`.
                    
                    YOUR TASKS
                    1. For each dimension (campaign, placement, creative, site) and platform:
                       - Learn the dominant naming pattern using `dq_dimensions` and `dimension_context`.
                       - Use canonical_key and entity to understand what the field represents
                         (e.g., creative vs campaign vs adset).
                    2. Detect:
                       - Values that are clearly incomplete (missing brand/product/country/date/objective when most others have them).
                       - Values that follow a completely different style than the majority.
                       - New naming styles or product codes that appear only in a few rows.
                    3. Use `pattern_memory`:
                       - If a pattern is already known as valid for a dimension+platform (e.g., stored in memory),
                         avoid flagging it as anomalous.
                       - Extend `updated_pattern_memory` with any new stable patterns you infer
                         (e.g., new standard templates or frequently-seen tokens).
                    4. Always group findings by:
                       - dimension (campaign, placement, creative, site),
                       - platform (e.g. facebook_ads, cm360, tiktok_ads),
                       - table_name (physical DB table from the `table` field).
                    5. If multiple tables share the same pattern, you may either:
                       - create separate findings per table_name, OR
                       - create separate entries for each table_name with the same pattern_observed.
                    6. Be conservative:
                       - Prefer fewer, high-confidence anomalies over noisy ones.
                       - If a value could reasonably match a pattern with minor variation, do NOT flag it.
                    7. Return results in the structured JSON format defined in the function schema.
                    8. campaign_year is not similar to campaign name. Do not use time-only fields for pattern learning or anomaly detection.
                                    """,
                                ),
                    (
                     "human",
                        """
                            Here is the sampled raw dimension data (per dimension/platform):
                            
                            `dq_dimensions`:
                            {dq_dimensions}
                            
                            Here is the aggregated context per dimension/platform/canonical_key:
                            
                            `dimension_context`:
                            {dimension_context}
                            
                            Here is the previous pattern memory (may be empty):
                            
                            `pattern_memory`:
                            {pattern_memory}
                            
                            Here is the raw metadata configuration for dimensions and platforms:
                            
                            `metadata`:
                            {metadata}
                            
                            The caller also provided this flag:
                            include_table_info = {include_table_info}
                            
                            Using ALL of the above, learn naming patterns and detect anomalies.
                            Remember to:
                            - Always include `platform` and `table_name` in each finding.
                            - Use canonical_key and entity to understand the semantic meaning of the field.
                            - Use `metadata` and `pattern_memory` together to understand what is expected,
                              and avoid flagging patterns already defined as valid.
                            - Use `updated_pattern_memory` to store any newly inferred stable patterns.
                            
                            Return your answer by calling the `detect_data_quality_anomalies` function.
                        """,
            ),
        ]
    )

    llm = ChatOpenAI(
        model="o4-mini-2025-04-16",
        temperature=1.0,  # a bit lower for more stable, repeatable patterns
    ).bind(
        functions=[ai_dq_function],
        function_call={"name": "detect_data_quality_anomalies"},
    )

    return prompt | llm | JsonOutputFunctionsParser()
