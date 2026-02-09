# nodes/graph_suggestion_step.py

from chains.graph_suggestion_chain import get_graph_suggestion_chain
from helper_fucntions.helper_functions import create_dataframe
import pandas as pd


def graph_suggestion_step(state):
    """
    Step in LangGraph pipeline that suggests chart types
    based on the SQL result table.

    Uses schema-aware prompt to ensure only approved charts
    are suggested (bar, stacked_bar, grouped_bar, line, area, stacked_area,
    dual_axis, scatter, bubble, heatmap, pie, waterfall, bullet, word_cloud, table).
    """

    if not state.get("result"):
        state["chart_suggestions"] = []
        return state

    # --- Build dataframe from SQL result ---
    df = create_dataframe(state["result"])

    # --- Detect numeric vs categorical columns ---
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    categorical_cols = [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c])]

    # --- Prepare sample rows for LLM (limit for token efficiency) ---
    sample_rows = df.head(50).to_dict(orient="records")
    columns = df.columns.tolist()

    # --- Invoke suggestion chain with context ---
    suggestion_chain = get_graph_suggestion_chain()
    suggestions = suggestion_chain.invoke({
        "columns": columns,
        "numeric_columns": numeric_cols,
        "categorical_columns": categorical_cols,
        "sample_rows": sample_rows
    })

    # --- Extract charts safely ---
    charts = suggestions.get("charts", [])
    state["chart_suggestions"] = charts

    return state
