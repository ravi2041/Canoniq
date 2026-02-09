# keypoints_chain.py
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.output_parsers.openai_functions import JsonOutputFunctionsParser

KEYPOINTS_FUNC = {
    "name": "extract_keypoints",
    "description": "Extract key points from a user analytics question.",
    "parameters": {
        "type": "object",
        "properties": {
            "bullets": {
                "type": "array",
                "items": {"type": "string"},
                "description": "3-8 concise bullet points summarizing intent."
            },
            "metrics": {"type": "array", "items": {"type": "string"}},
            "filters": {"type": "array", "items": {"type": "string"}},
            "time_window": {"type": "string", "description": "e.g., 'Q1 2025', 'last 30 days', or None"},
            "platform_hints": {"type": "array", "items": {"type": "string"}},
            "group_bys": {"type": "array", "items": {"type": "string"}},
            "output_pref": {"type": "string", "description": "table|chart|summary|mixed|unknown"}
        },
        "required": ["bullets"]
    }
}

KEYPOINTS_TEMPLATE = ChatPromptTemplate.from_messages([
    ("system",
     "You extract salient intent from marketing analytics questions. "
     "Only use details present in the question; do not invent metrics like revenue or reach. "
     "Prefer schema terms: impressions, clicks, cost, total_conversions, video_25/50/75/100, etc. "
     "Infer likely time windows only if explicitly stated (e.g., 'Jan 2025', 'last month'). "
     "Map vague phrases to safe hints (e.g., 'YouTube' → platform_hints=['youtube_data'])."),
    ("human",
     "Question:\n{question}\n\n"
     "Return a structured JSON with bullets (3-8), metrics, filters, time_window, platform_hints, "
     "group_bys, and output_pref.")
])

def get_keypoints_chain():
    llm = ChatOpenAI(model="o4-mini-2025-04-16", reasoning_effort="low").bind(
        functions=[KEYPOINTS_FUNC],
        function_call={"name": "extract_keypoints"}
    )
    return KEYPOINTS_TEMPLATE | llm | JsonOutputFunctionsParser()