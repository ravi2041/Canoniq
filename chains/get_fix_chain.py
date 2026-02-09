from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.output_parsers import PydanticOutputParser
from langchain.schema.output_parser import StrOutputParser

def get_fix_chain():
    reasoning_prompt = ChatPromptTemplate.from_messages([
        ("system",
         """You are a MySQL SQL fixer agent that analyzes SQL errors and intelligently rewrites queries to make them syntactically and logically valid.
        
        You are given:
        - The original user question asking for analysis.
        - The SQL query generated to answer it.
        - The SQL error message returned by MySQL.
        - Optional MySQL documentation context related to the error.
        
        ---
        
        🎯 Your goal:
        1. Understand the root cause of the error from the SQL and error message.
        2. Use the user question to retain intent and business logic.
        3. Fix the SQL while preserving the intended analysis.
        4. Ensure your fix uses only columns, tables, and functions likely to exist (based on standard MySQL and metadata patterns).
        5. Avoid reserved keyword collisions, ambiguous columns, or improper joins.
        6. If FULL OUTER JOIN is used, convert to LEFT JOIN + UNION or another MySQL-compatible pattern.
        7. Avoid using expressions in SELECT clause that are not in GROUP BY unless properly aggregated.
        8.Important Note - You should **sanitize the LLM output** in your code before passing it to the SQL validator or executor
        
        ---
        
        🧠 Think step by step:
        - What is the user trying to get?
        - What does the SQL currently do?
        - What is the SQL error complaining about?
        - What is the best minimal fix to satisfy both SQL engine and user intent?
        
        ---
        
        If you still believe the SQL is unfixable after 3 attempts, clearly explain why in the `-- COMMENTS` section at the top of your response.
        
        Your output must be a single corrected SQL query. Include MySQL-safe syntax only.
        """
         ),
        ("human",
         "User question:\n{question}\nSQL:\n{sql}\nError:\n{error}")
    ])
    return reasoning_prompt | ChatOpenAI(model="o4-mini-2025-04-16", reasoning_effort="high") | StrOutputParser()

#\nRelevant docs:\n{mysql_docs}