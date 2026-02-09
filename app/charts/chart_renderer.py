from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import difflib
from app.utils.chart_helpers import prepare_for_chart, coerce_numeric


def truncate_labels(values, max_len=12):
    """
    Truncate labels for axis/legend display but keep originals in hover.
    """
    truncated = []
    for v in values:
        v_str = str(v)
        truncated.append(v_str[:max_len] + "..." if len(v_str) > max_len else v_str)
    return truncated


def style_chart(fig, title: str, max_legend_items: int = 5):
    """Apply consistent styling + truncate labels but hide cluttered legends."""
    fig.update_layout(
        title=dict(
            text=title,
            x=0.5,
            xanchor="center",
            font=dict(size=16, family="Arial Black", color="black"),
        ),
        xaxis=dict(
            showline=True,
            linecolor="black",
            title_font=dict(size=14, family="Arial Black", color="black"),
            tickfont=dict(size=12, color="black"),
        ),
        yaxis=dict(
            showline=True,
            linecolor="black",
            title_font=dict(size=14, family="Arial Black", color="black"),
            tickfont=dict(size=12, color="black"),
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=12, family="Arial Black", color="black"),
        ),
        margin=dict(l=40, r=20, t=60, b=100),
        hovermode="closest"
    )

    # 🔑 Hide cluttered legends if too many categories
    try:
        for trace in fig.data:
            if hasattr(trace, "labels"):
                if len(set(trace.labels)) > max_legend_items:
                    fig.update_layout(showlegend=False)
                    break
    except Exception:
        pass

    return fig



def rank_dataframe(df: pd.DataFrame, metrics: list, top_n: int = 10):
    """
    Rank rows in a dataframe based on multiple metrics.
    Normalizes metrics and computes a combined score.
    """
    df_ranked = df.copy()

    if not metrics:
        return df_ranked.head(top_n), df_ranked.tail(top_n)

    # Normalize each metric (0-1 scaling) → higher = better
    for m in metrics:
        if m not in df_ranked.columns:
            continue
        series = pd.to_numeric(df_ranked[m], errors="coerce")
        if series.nunique() <= 1:
            continue
        df_ranked[f"norm_{m}"] = (series - series.min()) / (series.max() - series.min())

    norm_cols = [c for c in df_ranked.columns if c.startswith("norm_")]
    if not norm_cols:
        return df_ranked.head(top_n), df_ranked.tail(top_n)

    df_ranked["score"] = df_ranked[norm_cols].mean(axis=1)

    top_df = df_ranked.sort_values("score", ascending=False).head(top_n)
    bottom_df = df_ranked.sort_values("score", ascending=True).head(top_n)

    return top_df, bottom_df



# --- Column resolver ---
def resolve_column_name(df: pd.DataFrame, suggested_name: str) -> Optional[str]:
    """Robustly map LLM-suggested column name to actual df column."""
    if not suggested_name or df.empty:
        return None

    cols = list(df.columns)
    suggested = suggested_name.strip().lower()

    # 1. Exact match (case-insensitive)
    for col in cols:
        if col.lower() == suggested:
            return col

    # 2. Substring match
    for col in cols:
        if suggested in col.lower():
            return col

    # 3. Synonym mapping
    synonyms = {
        "month": ["date", "month_name", "period", "month_id"],
        "date": ["day", "time", "period"],
        "campaign": ["adset", "ad_group", "line_item"],
        "impressions": ["imprs", "views"],
        "clicks": ["taps", "interactions"],
        "conversions": ["purchases", "orders", "signups"],
        "cost": ["spend", "media_cost"]
    }
    if suggested in synonyms:
        for syn in synonyms[suggested]:
            for col in cols:
                if syn in col.lower():
                    return col

    # 4. Fuzzy match (close string)
    matches = difflib.get_close_matches(suggested, [c.lower() for c in cols], n=1, cutoff=0.6)
    if matches:
        for col in cols:
            if col.lower() == matches[0]:
                return col

    # 5. Fallback: return first column (at least avoids crash)
    return cols[0] if cols else None

def choose_dimension(df: pd.DataFrame, min_unique: int = 2, max_unique: int = 10) -> str:
    """
    Select the best dimension column based on priority rules + unique value check.

    Priority order: campaign > site > placement > creative > month > date.
    Must have between `min_unique` and `max_unique` unique values to qualify.
    """
    if df.empty:
        return None

    priority = ["campaign", "site", "placement", "creative", "month", "date"]
    cols_lower = {c.lower(): c for c in df.columns}

    # Check priority with uniqueness condition
    for p in priority:
        if p in cols_lower:
            col = cols_lower[p]
            unique_vals = df[col].dropna().unique()
            if min_unique <= len(unique_vals) <= max_unique:
                return col

    # Fallback: find any non-numeric column with valid unique count
    for c in df.columns:
        if not pd.api.types.is_numeric_dtype(df[c]):
            unique_vals = df[c].dropna().unique()
            if min_unique <= len(unique_vals) <= max_unique:
                return c

    # Absolute fallback: first column
    return df.columns[0]


def cap_categories(df, x, y, group_by=None, top_n=10):
    """
    Reduce categories to Top-N by metric, bucket rest into 'Other'.
    Works for x-axis categories and group_by legends.
    """
    if df.empty or not y or y not in df.columns:
        return df

    # Cap x-axis categories
    if x and x in df.columns:
        temp_x = df.groupby(x, dropna=False)[y].sum().reset_index()
        top_x = temp_x.nlargest(top_n, y)[x].tolist()
        df[x] = df[x].where(df[x].isin(top_x), "Other")

    # Cap legend categories
    if group_by and group_by in df.columns:
        temp_g = df.groupby(group_by, dropna=False)[y].sum().reset_index()
        top_g = temp_g.nlargest(top_n, y)[group_by].tolist()
        df[group_by] = df[group_by].where(df[group_by].isin(top_g), "Other")

    return df


def render_chart_suggestions(result: dict):
    """Render chart suggestions from LLM results using Streamlit + Plotly + AgGrid."""

    if not result.get("chart_suggestions"):
        st.info("No chart suggestions available.")
        return

    try:
        # --- Convert LLM result into DataFrame ---
        df = pd.DataFrame(result["result"]["rows"], columns=result["result"]["columns"])

        # --- Interactive AgGrid Table ---
        st.subheader("📊 Interactive Data Table")
        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_default_column(sortable=True, filter=True, resizable=True)
        gb.configure_pagination(enabled=True, paginationPageSize=10)
        gb.configure_grid_options(enableRangeSelection=True, enableCellTextSelection=True, enableClipboard=True)
        gb.configure_side_bar()
        grid_response = AgGrid(
            df,
            gridOptions=gb.build(),
            update_mode=GridUpdateMode.MODEL_CHANGED,
            theme="alpine",
            fit_columns_on_grid_load=True,
        )
        df_sorted = pd.DataFrame(grid_response["data"])

        chart_suggestions = result.get("chart_suggestions", [])[:8]

        # --- Ranking setup ---
        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        dim_cols = [c for c in df.columns if c not in numeric_cols]
        dimension_col = choose_dimension(df)

        top_df, bottom_df = rank_dataframe(df, numeric_cols, top_n=10)

        # --- Show Top Performers ---
        st.subheader("🏆 Top Performers")
        for metric in numeric_cols[:1]:
            if metric in top_df.columns:
                sorted_top = top_df.sort_values(metric, ascending=False).head(10)
                fig = px.bar(  # <<< dimension on x-axis
                    sorted_top,
                    x=dimension_col, y=metric, color=None,
                    title=f"Top 10 {dimension_col} by {metric}"
                )
                fig = style_chart(fig, f"Top 10 {dimension_col} by {metric}")
                st.plotly_chart(fig, use_container_width=True)

        # # --- Show Underperformers ---
        # st.subheader("⚠️ Underperformers")
        # for metric in numeric_cols[:1]:
        #     if metric in bottom_df.columns:
        #         sorted_bottom = bottom_df.sort_values(metric, ascending=True).head(10)
        #         fig = px.bar(  # <<< dimension on x-axis
        #             sorted_bottom,
        #             x=dimension_col, y=metric, color=None,
        #             title=f"Bottom 10 {dimension_col} by {metric}"
        #         )
        #         fig = style_chart(fig, f"Bottom 10 {dimension_col} by {metric}")
        #         st.plotly_chart(fig, use_container_width=True)

        # --- Regular Chart Suggestions ---
        st.subheader("Other Suggested Charts")

        for i in range(0, len(chart_suggestions), 2):
            cols = st.columns(2)

            for j in range(2):
                if i + j >= len(chart_suggestions):
                    break

                chart = chart_suggestions[i + j]
                chart_type = str(chart.get("chart_type", "bar")).lower()
                x = chart.get("x_axis")
                y_raw = chart.get("y_axis")
                group_by = chart.get("group_by") or None
                title = chart.get("title", "Chart")
                description = chart.get("description", "")

                try:
                    # --- Normalize y-axis into list ---
                    if isinstance(y_raw, str):
                        y_list = [v.strip() for v in y_raw.split(",") if v.strip()]
                    elif isinstance(y_raw, list):
                        y_list = [v.strip() for v in y_raw if v]
                    else:
                        y_list = []

                    # --- Resolve column names safely ---
                    x = resolve_column_name(df_sorted, x)
                    y_list = [resolve_column_name(df_sorted, y) for y in y_list if resolve_column_name(df_sorted, y)]
                    group_by = resolve_column_name(df_sorted, group_by)

                    base_y = y_list[0] if y_list else None
                    plot_df = prepare_for_chart(df_sorted, chart_type, x, base_y, group_by)

                    # NEW: Cap categories for both x-axis and legend
                    plot_df = cap_categories(plot_df, x, base_y, group_by, top_n=10)


                    valid_y = [col for col in y_list if col in plot_df.columns]
                    for col in valid_y:
                        plot_df[col] = coerce_numeric(plot_df[col])

                    if len(valid_y) > 1 and chart_type in ("line", "area", "bar", "stacked_bar", "grouped_bar"):
                        id_vars = [c for c in [x, group_by] if c and c in plot_df.columns]
                        plot_df = plot_df.melt(
                            id_vars=id_vars,
                            value_vars=valid_y,
                            var_name="metric",
                            value_name="value",
                        )
                        y, color = "value", "metric"
                    else:
                        y = valid_y[0] if valid_y else None
                        color = group_by if group_by in plot_df.columns else None

                    # --- Cap categories to Top-N to avoid clutter ---
                    if chart_type in ("bar", "stacked_bar", "grouped_bar", "line", "area", "stacked_area", "pie"):
                        plot_df = cap_categories(plot_df, x, y, group_by, top_n=10)
                    # --- Chart rendering ---
                    if chart_type == "bar":
                        fig = px.bar(plot_df.nlargest(10, y) if y else plot_df,
                                     x=x, y=y, color=color)  # <<< dimension on x
                    elif chart_type == "stacked_bar":
                        fig = px.bar(plot_df, x=x, y=y, color=group_by, barmode="stack")
                    elif chart_type == "grouped_bar":
                        fig = px.bar(plot_df, x=x, y=y, color=group_by, barmode="group")
                    elif chart_type == "line":
                        fig = px.line(plot_df, x=x, y=y, color=color)
                        fig.update_traces(mode="lines+markers")
                    elif chart_type == "area":
                        fig = px.area(plot_df, x=x, y=y, color=color)
                        fig.update_traces(mode="lines")
                    elif chart_type == "stacked_area":
                        fig = px.area(plot_df, x=x, y=y, color=group_by, groupnorm="fraction")
                    elif chart_type == "dual_axis" and len(valid_y) >= 2:
                        fig = go.Figure()
                        fig.add_trace(go.Bar(x=plot_df[x], y=plot_df[valid_y[0]], name=valid_y[0], yaxis="y1"))
                        for m in valid_y[1:]:
                            fig.add_trace(
                                go.Scatter(x=plot_df[x], y=plot_df[m], name=m, mode="lines+markers", yaxis="y2")
                            )
                        fig.update_layout(
                            yaxis=dict(title=valid_y[0], side="left"),
                            yaxis2=dict(title=" / ".join(valid_y[1:]), overlaying="y", side="right"),
                        )
                    elif chart_type == "pie":
                        fig = px.pie(plot_df, names=x, values=y)
                    elif chart_type == "waterfall":
                        fig = go.Figure(go.Waterfall(x=plot_df[x], y=plot_df[y]))
                    elif chart_type == "bullet" and len(valid_y) >= 2:
                        fig = go.Figure()
                        fig.add_trace(go.Bar(x=plot_df[x], y=plot_df[valid_y[0]], name="Actual"))
                        fig.add_trace(go.Scatter(x=plot_df[x], y=plot_df[valid_y[1]],
                                                 mode="markers", marker_symbol="line-ns-open", name="Target"))
                    else:
                        st.warning(f"⚠️ Chart type '{chart_type}' not implemented. Skipping.")
                        continue
                    # Hide cluttered legends if still too many categories
                    if group_by and plot_df[group_by].nunique() > 5:
                        fig.update_layout(showlegend=False)
                        st.caption("⚠️ Legend hidden (too many categories). Use hover to inspect details.")


                    fig = style_chart(fig, title)

                    with cols[j]:
                        st.subheader(f"**{title}**")
                        if description:
                            st.caption(description)
                        st.plotly_chart(fig, use_container_width=True)

                except Exception as e:
                    st.error(f"Failed to render {chart_type} chart: {e}")
                    st.code(f"x: {x}, y: {y_raw}, group_by: {group_by}")

    except Exception as e:
        st.error(f"Chart rendering failed: {e}")
