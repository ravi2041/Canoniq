# chains/ai_dq_feedback_chain.py
import json
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain.output_parsers.openai_functions import JsonOutputFunctionsParser

update_feedback_function = {
    "name": "update_pattern_memory",
    "description": "Update the stored pattern memory based on user feedback on detected anomalies.",
    "parameters": {
        "type": "object",
        "properties": {
            "updated_memory": {
                "type": "object",
                "description": (
                    "The updated dq_pattern_memory object after incorporating feedback. "
                    "Contains accepted patterns and deprecated/ignored ones."
                )
            },
            "summary": {
                "type": "string",
                "description": "Summary of changes made to the pattern memory."
            }
        },
        "required": ["updated_memory", "summary"]
    }
}


def ai_dq_feedback_chain():
    """
    Chain to refine or update pattern memory (dq_pattern_memory.json)
    based on user feedback.
    """
    prompt = ChatPromptTemplate.from_messages([
        ("system", """
        You maintain an evolving knowledge base of naming patterns (dq_pattern_memory).
        Users review anomalies found by AI and either accept them as valid (new pattern)
        or reject them (true anomaly).

        Task:
        - Merge feedback with existing memory.
        - Add new valid patterns or synonyms if user accepted.
        - Remove or deprecate patterns marked invalid.
        - Keep output compact and structured.
        """),
        ("human", """
        Current dq_pattern_memory:
        {current_memory}

        New user feedback:
        {user_feedback}

        Return updated memory and a short summary.
        """)
    ])

    llm = ChatOpenAI(model="o4-mini-2025-04-16", temperature=1).bind(
        functions=[update_feedback_function],
        function_call={"name": "update_pattern_memory"}
    )

    return prompt | llm | JsonOutputFunctionsParser()
