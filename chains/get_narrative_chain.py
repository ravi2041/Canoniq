from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.output_parsers.openai_functions import JsonOutputFunctionsParser
from helper_fucntions.helper_functions import chunk_text, count_tokens
import json


def get_narrative_chain():
    narrative_function = {
        "name": "generate_narrative",
        "description": "Summarize SQL output and recommend next actions to improve performance.",
        "parameters": {
            "type": "object",
            "properties": {
                "summary": {
                    "type": "object",
                    "properties": {
                        "key_findings": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Bullet points summarizing what happened (trends, deltas, drivers)."
                        },
                        "drivers": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Bullet points explaining causes behind performance."
                        },
                        "limitations": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Any caveats, missing data, or constraints."
                        }
                    },
                    "required": ["key_findings"]
                },
                "recommendation": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "title": {"type": "string"},
                            "action": {"type": "string"},
                            "why": {"type": "string"},
                            "expected_impact": {"type": "string"},
                            "confidence": {
                                "type": "string",
                                "enum": ["low", "medium", "high"]
                            },
                            "metrics_cited": {
                                "type": "array",
                                "items": {"type": "string"}
                            },
                            "reasoning": {
                                "type": "array",
                                "items": {"type": "string"}
                            }
                        },
                        "required": ["title", "action", "why"]
                    },
                    "minItems": 3,
                    "maxItems": 5
                },
                "assumptions": {
                    "type": "array",
                    "items": {"type": "string"}
                }
            },
            "required": ["summary", "recommendation"]
        }
    }

    prompt = ChatPromptTemplate.from_messages([
        ("system",
         """
            You are a performance marketing analyst AND recommender.

            Use ONLY the provided inputs (SQL result table, question, and optional goals/policies). 
            
            Output goals:
             - Always format output cleanly and consistently. 
            1) Narrative Summary
                - Write a structured summary (bullet points, not long paragraphs). 
            ### Summary Guidelines
                - Provide findings as bullets under **Key Findings**.
                - If there are clear reasons behind results, list them under **Drivers**.
                - If there are caveats (e.g., missing conversions), list under **Limitations**.
                - Keep bullets concise, 1 line each, exec-level clarity.
                
            2)  Recommendation:
                  • Present each recommendation as a numbered list (1, 2, 3…).
                  • For each item, bold the `title`, then show fields in a bullet list:
                      - **Action:** <short imperative>
                      - **Why:** <driver finding>
                      - **Expected impact:** <direction + magnitude>
                      - **Confidence:** <low/medium/high>
                      - **Metrics cited:** comma-separated exact figures
                      - **Reason: for providing such recommendation (what made you take this decision. justify your reasoning)
            
            
            Formatting & style:
            - Executive tone, no fluff, no platform-UI jargon. 
            - Refer to metrics precisely (e.g., CTR, CPC, CVR, CPA). 
            - When conversions are unavailable for a platform, keep recommendations to engagement/efficiency (e.g., CTR/CPC) and state the limitation.
            
        """
         ),
        ("human",
         "Question:\n{question}\nResult Table:\n{result_table}\nSummary:")
    ])
    llm = ChatOpenAI(model="o4-mini-2025-04-16",reasoning_effort="medium").bind(
        functions=[narrative_function], function_call={"name": "generate_narrative"}
    )
    return prompt | llm | JsonOutputFunctionsParser()

def safe_list(val):
    if isinstance(val, list):
        return val
    elif isinstance(val, dict):
        return [val]  # wrap single dict
    elif val is None:
        return []
    else:
        return [str(val)]  # fallback: stringify unexpected types


def run_narrative_with_chunking(question: str, result_table: str, model: str = "o4-mini-2025-04-16"):
    """Run narrative chain with automatic token counting + chunking + final merge."""
    total_tokens = count_tokens(question + result_table, model=model)

    # If input fits in context → run once
    if total_tokens < 100000:  # safe buffer
        chain = get_narrative_chain()
        return chain.invoke({"question": question, "result_table": result_table})

    # Otherwise → chunk the table
    outputs = []
    chunks = chunk_text(result_table, model=model)
    for i, chunk in enumerate(chunks, 1):
        chain = get_narrative_chain()
        result = chain.invoke({
            "question": f"{question} (Chunk {i}/{len(chunks)})",
            "result_table": chunk
        })
        outputs.append(result)

    # --- Merge step ---
    merged_summary = "\n\n".join([
        (
            o.get("summary", "")
            if isinstance(o.get("summary"), str)
            else json.dumps(o.get("summary"), indent=2)  # fallback to readable JSON
        )
        for o in outputs
    ])
    merged_recs = [item for o in outputs for item in safe_list(o.get("recommendation", []))]
    merged_assumptions = [item for o in outputs for item in safe_list(o.get("assumptions", []))]

    # Now ask the LLM to create a polished, unified output
    chain = get_narrative_chain()
    final_result = chain.invoke({
        "question": f"Unify and polish these partial narratives for the query: {question}",
        "result_table": f"""
        --- Summaries from chunks ---
        {merged_summary}

        --- Recommendation from chunks ---
        {json.dumps(merged_recs, indent=2)}

        --- Assumptions from chunks ---
        {json.dumps(merged_assumptions, indent=2)}
        """
    })

    return final_result
