from dotenv import load_dotenv
import json

from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema.output_parser import StrOutputParser
from langchain.tools import tool
from config import MYSQL_CONFIG
import mysql.connector
from langchain.output_parsers.openai_functions import JsonOutputFunctionsParser
from langchain.schema.runnable import Runnable
from helper_fucntions.sql_runner import run_sql_on_mysql
from decimal import Decimal
from tabulate import tabulate

load_dotenv()


@tool
def validate_sql(query: str) -> bool:
    """Checks SQL syntax by using EXPLAIN on a MySQL connection."""

    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute("EXPLAIN " + query)
        cursor.fetchall()  # fetch all rows to clear the result set
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ SQL validation failed:\n{e}")
        return False


def load_metadata(filepath="./metadata.json"):
    with open(filepath, "r") as f:
        return json.load(f)


def format_result(columns, rows):
    formatted_rows = []

    for row in rows:
        formatted_row = []
        for value in row:
            if isinstance(value, Decimal):
                formatted_row.append(float(value))  # or str(value) if needed
            elif value is None:
                formatted_row.append("N/A")
            else:
                formatted_row.append(value)
        formatted_rows.append(formatted_row)

    return tabulate(formatted_rows, headers=columns, tablefmt="grid")  # options: "grid", "pretty", "pipe"


def clean_row(row):
    return [
        float(item) if isinstance(item, Decimal) else ("N/A" if item is None else item)
        for item in row
    ]

sql_function = {
    "name": "generate_sql_query",
    "description": "Generate SQL query from natural language using metadata",
    "parameters": {
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": "A valid SQL query that answers the user's question"
            }
        },
        "required": ["sql"]
    }
}

narrative_function = {
    "name": "generate_narrative",
    "description": "Generate a plain-language summary of SQL query results",
    "parameters": {
        "type": "object",
        "properties": {
            "summary": {
                "type": "string",
                "description": "Narrative summary of SQL output"
            }
        },
        "required": ["summary"]
    }
}

def get_function_call_chain(metadata: dict) -> Runnable:
    prompt = ChatPromptTemplate.from_messages([
        ("system",
         "You are a helpful data assistant. Use the provided metadata to write a valid SQL query.\n"
         "The database contains Shopify ecommerce data for Pacific Mart.\n\n"
         "Table `shopify_customers` contains customer details like name, email, phone, address, and total spend.\n"
         "Table `shopify_order_fulfillments` includes shipping status, tracking info, origin address, and delivery estimates for fulfilled orders.\n"
         "Table `shopify_products` includes product catalog data including title, variants, vendor, and inventory.\n\n"
         "Table ` meta_campaign_performance ` includes campaign performance metrics such as objective, reach, impressions, frequency, link_clicks,CTR .\n\n"
         "Table ` meta_funnel_extended ` includes campaign performance metrics such as add to cart, checkouts, purchases, landing page views,video views.\n\n"         
         "Use table relationships (e.g., orders to customers or orders to fulfillments) to join relevant data as needed."
         ),
        ("human", "Metadata:\n{metadata}\n\nUser Question:\n{question}")
    ])

    llm = ChatOpenAI(model="gpt-4o-2024-08-06", temperature=0).bind(
        functions=[sql_function],
        function_call={"name": "generate_sql_query"}
    )

    return prompt | llm | JsonOutputFunctionsParser()

MAX_RETRIES = 3

def reason_and_retry(question: str, metadata: dict, failed_sql: str, error: str) -> str:
    """
    Ask the LLM to fix the SQL query based on the error.
    """
    reasoning_prompt = ChatPromptTemplate.from_messages([
        ("system", "You are an expert SQL fixer who analyzes faulty SQL queries, identifies the root cause of errors, and rewrites them into correct, production-ready SQL code."),
        ("human",
         "User question:\n{question}\n\n"
         "The SQL generated was:\n{sql}\n\n"
         "But it failed with this error:\n{error}\n\n"
         "Fix the SQL and return only the corrected SQL.")
    ])

    fixer_chain = reasoning_prompt | ChatOpenAI(model="gpt-4", temperature=0) | StrOutputParser()

    fixed_sql = fixer_chain.invoke({
        "question": question,
        "sql": failed_sql,
        "error": error
    })

    return fixed_sql.strip()

# Step 2: Narrative generation chain
def get_narrative_chain() -> Runnable:
    prompt = ChatPromptTemplate.from_messages([
        (
            "system",
            """You are a data analyst helping a marketing team understand the performance of their campaigns.
    Given a SQL result table and a business question, provide a clear, concise, and meaningful summary that includes:

    - Overall performance and trends
    - Key metrics that stand out (e.g., high spend, low ROI, CTR changes)
    - Comparisons across campaigns or time periods
    - Any anomalies or notable recommendations

    Avoid simply listing values or describing table structure.
    Focus on insights that help stakeholders make decisions.
    Use plain language that is easy to understand for non-technical users."""
        ),
        (
            "human",
            """Question:
    {question}

    Result Table:
    {result_table}

    Summary:"""
        )
    ])

    llm = ChatOpenAI(model="gpt-4.1-nano-2025-04-14", temperature=0).bind(
        functions=[narrative_function],
        function_call={"name": "generate_narrative"}
    )

    return prompt | llm | JsonOutputFunctionsParser()


def nlq_pipeline_mysql(user_question: str):
    metadata = load_metadata("metadata.json")
    sql_chain = get_function_call_chain(metadata)

    sql = sql_chain.invoke({"metadata": json.dumps(metadata), "question": user_question})["sql"]
    print("🧠 Initial SQL:\n", sql)

    retries = 0
    while retries <= MAX_RETRIES:
        validation_result = validate_sql.run(sql)
        print(f"🔍 Validation (Attempt {retries + 1}):", validation_result)

        if validation_result:
            break

        if retries == MAX_RETRIES:
            raise Exception("❌ Failed to generate valid SQL after multiple attempts.")

        # Retry with error reasoning
        error_msg = "SQL failed validation (syntax or structure error)"
        sql = reason_and_retry(user_question, metadata, sql, error_msg)
        print("🔁 Retried SQL:\n", sql)
        retries += 1

    # Execute on MySQL
    result = run_sql_on_mysql(sql)

    if "error" in result:
        print("❌ Query Execution Error:", result["error"])
        return {"sql": sql, "error": result["error"], "validation": False}

    # Format and display result
    formatted_output = format_result(result["columns"], result["rows"])
    print("📊 Query Result:\n", formatted_output)

    # Clean the rows
    cleaned_rows = [clean_row(row) for row in result['rows']]
    row_dicts = [dict(zip(result['columns'], row)) for row in cleaned_rows]

    # Step 4: Generate narrative
    narrative_chain = get_narrative_chain()
    narrative_response = narrative_chain.invoke({
        "question": user_question,
        "result_table": result
    })
    print("📢 Narrative:\n", narrative_response["summary"])

    return {
        "sql": sql,
        "validation": True,
        "result": result,
        "narrative": narrative_response["summary"]
    }

# Example usage
if __name__ == "__main__":
    nlq_pipeline_mysql("give me list of customers who has purchased 1 product and price is more than $100. Give me full name, order date, product information and customer information")



