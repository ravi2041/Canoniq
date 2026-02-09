# graph_suggestion_chain.py

from langchain.prompts import ChatPromptTemplate
from langchain.output_parsers.openai_functions import JsonOutputFunctionsParser
from langchain_openai import ChatOpenAI

graph_suggestion_func = {
    "name": "suggest_graphs",
    "description": (
        "Decide what charts to generate based on the result table from SQL. "
        "Use fields and data types to infer best chart types. "
        "Support stacked, grouped, dual-axis, or multiple metrics for y-axis."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "charts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "chart_type": {
                            "type": "string",
                            "enum": [
                                "bar",
                                "stacked_bar",
                                "grouped_bar",
                                "line",
                                "area",
                                "stacked_area",
                                "pie",
                                "scatter",
                                "bubble",
                                "heatmap",
                                "dual_axis",
                                "waterfall",
                                "bullet",
                                "word_cloud",
                                "table"
                            ]
                        },
                        # x_axis should usually be one field (dimension or time)
                        "x_axis": {
                            "type": "string",
                            "description": "Dimension or time column for x-axis"
                        },
                        # y_axis can now be one OR multiple metrics
                        "y_axis": {
                            "oneOf": [
                                {"type": "string"},
                                {"type": "array", "items": {"type": "string"}}
                            ],
                            "description": "Metric(s) to plot on y-axis"
                        },
                        # group_by is optional, used for stacked/grouped series
                        "group_by": {
                            "type": "string",
                            "description": "Optional grouping field (e.g. platform, site)"
                        },
                        "title": {
                            "type": "string",
                            "description": "Chart title"
                        },
                        "description": {
                            "type": "string",
                            "description": "Business-friendly explanation of the chart"
                        }
                    },
                    "required": ["chart_type", "x_axis", "y_axis"]
                }
            }
        },
        "required": ["charts"]
    }
}

graph_suggestion_prompt = ChatPromptTemplate.from_messages([
    ("system", """
        You are a data visualization assistant.
        Given a SQL result table with columns and sample rows, your task is to suggest 3–4 charts that would best help a business user understand the result data.
        
        When suggesting charts:
        - Prioritize meaningful business metrics (conversions, CTR, CPA, ROAS, full video plays).
        - De-prioritize less relevant or intermediate metrics (e.g., video_25, video_50, raw IDs).
        - Always choose the most interpretable metric if multiple options exist.
        - Avoid suggesting charts that only show vanity metrics (e.g., impressions alone, unless in context).
        - Favor charts that combine primary business metrics with dimensions (campaign, platform, date).
        - Always suggest 3–4 different charts if data allows (mix trends, comparisons, ratios, distributions).

        
        ✅ Allowed Chart Types
        You may only suggest from the following charts:
        Bar Chart
        Stacked Bar Chart
        Grouped Bar Chart
        Area Chart / Stacked Area Chart
        Dual Axis Chart (bars for volume, lines for ratios)
        Bubble Chart
        Pie Chart (only when <8 categories)
        Bullet Chart
        Line Chart
        Waterfall Chart
        
        📊 Chart Selection Rules
        
        Bar Chart → 1 categorical + 1 metric (or multiple metrics). Use horizontal for long labels.
        Stacked Bar / Grouped Bar → 1 categorical + 1 metric, with an additional dimension as group.
        Area / Stacked Area → 1 time dimension + 1 metric (stacked if grouping exists).
        Dual Axis Chart → 1 categorical/time dimension + ≥2 metrics, where volume metrics (impressions, clicks, sales, cost) go on bars (left y-axis) and rate metrics (CTR, CPC, CPA, CVR) go on lines (right y-axis).
        Pie Chart → 1 categorical dimension + 1 metric, only if <8 unique categories.
        Bullet Chart → 1 metric compared against a target/benchmark.
        Line Chart → 1 time dimension + 1 metric, optional grouping for multiple series.
        Waterfall Chart → 1 categorical dimension that represents sequential changes in a metric.
        
        ⚠️ Limitations
        Do not suggest charts outside the approved list.
        Do not suggest Pie Charts for >8 categories.
        Line/Area charts must have a time dimension.
        
        📦 Output Format

        Return suggestions as a list of JSON objects with the following fields:
        chart_type: one of the approved chart types
        x_axis: the dimension for the x-axis
        y_axis: the metric(s) for the y-axis
        group_by: optional grouping column
        title: short, descriptive chart title
        description: one-sentence business explanation of what the chart shows
        
    """),
    ("human", "Columns: {columns}\n\nSample Rows:\n{sample_rows}")
])

def get_graph_suggestion_chain():
    llm = ChatOpenAI(model="o4-mini-2025-04-16", temperature=1).bind(
        functions=[graph_suggestion_func],
        function_call={"name": "suggest_graphs"}
    )
    return graph_suggestion_prompt | llm | JsonOutputFunctionsParser()
