import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt


def coerce_numeric(series):
    """Convert a pandas Series to numeric, forcing errors to NaN."""
    return pd.to_numeric(series, errors="coerce")


# ---------------- CHART BUILDERS ---------------- #

def build_bar_chart(df, x, y, color=None):
    """Horizontal bar chart (Top 10)."""
    if y in df.columns:
        df[y] = coerce_numeric(df[y])
    if x in df.columns:
        df[x] = df[x].astype(str)
    if y in df.columns and pd.api.types.is_numeric_dtype(df[y]):
        df = df.nlargest(10, y)

    fig = px.bar(df, x=y, y=x, color=color, orientation="h")
    fig.update_layout(
        yaxis=dict(autorange="reversed"),
        xaxis=dict(title=y, tickformat=",.0f"),
        margin=dict(l=120, r=20, t=40, b=60),
    )
    return fig


def build_stacked_bar_chart(df, x, y, group_by):
    return px.bar(df, x=x, y=y, color=group_by, barmode="stack")


def build_grouped_bar_chart(df, x, y, group_by):
    return px.bar(df, x=x, y=y, color=group_by, barmode="group")


def build_line_chart(df, x, y, color=None):
    fig = px.line(df, x=x, y=y, color=color)
    fig.update_traces(mode="lines+markers")
    return fig


def build_area_chart(df, x, y, color=None):
    fig = px.area(df, x=x, y=y, color=color)
    fig.update_traces(mode="lines")
    return fig


def build_stacked_area_chart(df, x, y, group_by):
    return px.area(df, x=x, y=y, color=group_by, groupnorm="fraction")


def build_dual_axis_chart(df, x, metrics):
    """Bar for first metric, line(s) for others."""
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df[x], y=df[metrics[0]], name=metrics[0], yaxis="y1"))

    for m in metrics[1:]:
        fig.add_trace(go.Scatter(x=df[x], y=df[m], name=m,
                                 mode="lines+markers", yaxis="y2"))

    fig.update_layout(
        xaxis=dict(title=x),
        yaxis=dict(title=metrics[0], side="left"),
        yaxis2=dict(title=" / ".join(metrics[1:]), overlaying="y", side="right"),
        barmode="group",
    )
    return fig


def build_scatter_chart(df, x, y, group_by=None):
    fig = px.scatter(df, x=x, y=y, color=group_by)
    fig.update_traces(mode="markers")
    return fig


def build_bubble_chart(df, x, y, size, group_by=None):
    return px.scatter(df, x=x, y=y, size=size, color=group_by)


def build_heatmap_chart(df, x, y, metric):
    return px.density_heatmap(df, x=x, y=y, z=metric)


def build_pie_chart(df, names, values):
    return px.pie(df, names=names, values=values)


def build_waterfall_chart(df, x, y):
    return go.Figure(go.Waterfall(
        x=df[x],
        y=coerce_numeric(df[y]),
        connector={"line": {"color": "rgb(63, 63, 63)"}}
    ))


def build_bullet_chart(df, actual, target, label_col=None):
    """Bullet chart using bar vs. reference line."""
    fig = go.Figure()

    if label_col:
        labels = df[label_col]
    else:
        labels = ["Metric"]

    fig.add_trace(go.Bar(
        x=df[actual], y=labels,
        orientation="h", name="Actual"
    ))

    fig.add_trace(go.Scatter(
        x=df[target], y=labels,
        mode="markers", marker_symbol="line-ns-open",
        marker_line_width=2, marker_size=20,
        name="Target"
    ))
    return fig


def build_table_chart(df):
    return go.Figure(data=[go.Table(
        header=dict(values=list(df.columns)),
        cells=dict(values=[df[col] for col in df.columns])
    )])
