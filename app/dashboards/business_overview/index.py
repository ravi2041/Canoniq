# app/dashboards/executive_dashboard.py

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from app.data.db import get_conn
from app.dashboards.business_overview.executive_query_sql import query
from app.ui.layout import app_header, apply_theme
# from app.ui.filters import global_filters   # only keep if you actually use it elsewhere


# --------------------------
# Cached data loader (snapshot-based)
# --------------------------
@st.cache_data(show_spinner="Loading executive dashboard data…", ttl=3600)
def load_data(refresh_key: int) -> pd.DataFrame:
    """
    Load data for the executive dashboard.

    - Uses the snapshot query (business_overview_snapshot) from `query`.
    - `refresh_key` is a dummy parameter used to invalidate the cache when the user
      clicks the 'Reload data' button.
    - ttl=3600 means cache auto-expires after 1 hour as a safety net.
    """
    conn = get_conn()
    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty:
        return df

    # Derived metrics
    df["ctr"] = df["clicks"] / df["impressions"].replace(0, pd.NA)
    df["cpc"] = df["cost"] / df["clicks"].replace(0, pd.NA)
    df["cpm"] = df["cost"] / (df["impressions"].replace(0, pd.NA) / 1000)
    df["cpa"] = df["cost"] / df["conversions"].replace(0, pd.NA)

    # Ensure date is datetime for charts
    df["date"] = pd.to_datetime(df["date"])

    return df


# --------------------------
# Helper for styling charts
# --------------------------
def style_chart(fig, x_title, y_title, y2_title=None):
    fig.update_layout(
        title=dict(x=0.5, font=dict(size=18, family="Arial Black", color="black")),
        xaxis=dict(
            title=x_title,
            showline=True,
            linecolor="black",
            title_font=dict(size=14, family="Arial", color="black"),
            tickfont=dict(size=12),
        ),
        yaxis=dict(
            title=y_title,
            showline=True,
            linecolor="black",
            title_font=dict(size=14, family="Arial", color="black"),
            tickfont=dict(size=12),
        ),
        legend=dict(font=dict(size=12)),
        hovermode="x unified"
    )
    if y2_title:
        fig.update_layout(
            yaxis2=dict(
                title=y2_title,
                overlaying="y",
                side="right",
                title_font=dict(size=14, family="Arial", color="black"),
                tickfont=dict(size=12),
            )
        )
    return fig


# --------------------------
# Dashboard Render Function
# --------------------------
def render(filter_container=None):
    """
    Render the Executive / Business Overview dashboard.

    If `filter_container` is provided, all filters are rendered into that container
    (e.g. the RIGHT sidebar column from main.py). Otherwise, they fall back to st.sidebar.
    """
    # IMPORTANT: no st.set_page_config here; it's handled in main.py
    #st.title("📊 Executive Dashboard")



    # Decide where filters live
    panel = filter_container or st.sidebar

    # --------------------------
    # Reload / cache control
    # --------------------------
    if "exec_refresh_key" not in st.session_state:
        st.session_state.exec_refresh_key = 0

    reload_col, info_col = st.columns([1, 5])
    with reload_col:
        if st.button("🔄 Reload data from DB"):
            # bump the key => invalidates cache for load_data
            st.session_state.exec_refresh_key += 1

    with info_col:
        st.caption(
            f"Snapshot version: {st.session_state.exec_refresh_key} "
            "(click reload to pull latest snapshot from the database)"
        )

    # Load data from cached snapshot
    df = load_data(st.session_state.exec_refresh_key)

    if df.empty:
        st.warning("No data found in snapshot table / query result.")
        return

    # --- Filters (now in panel, not st.sidebar) ---
    panel.header("Executive Filters")

    advertisers = sorted(df["advertiser"].dropna().unique())
    sites = sorted(df["site"].dropna().unique())
    campaigns = sorted(df["campaign"].dropna().unique())
    creatives = sorted(df["creative"].dropna().unique())
    placements = sorted(df["placement"].dropna().unique())
    activities = sorted(df["activity_name"].dropna().unique())

    selected_site = panel.selectbox(
        "Site", ["All"] + sites, index=0, key="site_filter"
    )
    selected_campaign = panel.selectbox(
        "Campaign", ["All"] + campaigns, index=0, key="campaign_filter"
    )
    selected_creative = panel.selectbox(
        "Creative", ["All"] + creatives, index=0, key="creative_filter"
    )
    selected_placement = panel.selectbox(
        "Placement", ["All"] + placements, index=0, key="placement_filter"
    )
    selected_activity = panel.selectbox(
        "Activity Name", ["All"] + activities, index=0, key="activity_filter"
    )

    # --- Date Range Filter for Dataset ---
    min_date, max_date = df["date"].min(), df["date"].max()
    start_date, end_date = panel.date_input(
        "Filter Date Range", [min_date, max_date], key="filter_date_range"
    )

    # --- Apply filters ---
    filtered_df = df.copy()
    if selected_site != "All":
        filtered_df = filtered_df[filtered_df["site"] == selected_site]
    if selected_campaign != "All":
        filtered_df = filtered_df[filtered_df["campaign"] == selected_campaign]
    if selected_creative != "All":
        filtered_df = filtered_df[filtered_df["creative"] == selected_creative]
    if selected_placement != "All":
        filtered_df = filtered_df[filtered_df["placement"] == selected_placement]
    if selected_activity != "All":
        filtered_df = filtered_df[filtered_df["activity_name"] == selected_activity]

    filtered_df = filtered_df[
        filtered_df["date"].between(pd.to_datetime(start_date), pd.to_datetime(end_date))
    ]

    if filtered_df.empty:
        st.warning("No data after applying filters and date range.")
        return

    # --- KPI Comparison Mode ---
    panel.subheader("KPI Comparison Mode")
    compare_option = panel.radio(
        "Comparison Mode",
        ["Last 7 Days vs Previous 7 Days",
         "Last 30 Days vs Previous 30 Days",
         "Custom Range"],
        index=0,
        key="compare_mode"
    )

    today = filtered_df["date"].max()

    if compare_option == "Last 7 Days vs Previous 7 Days":
        latest_period = filtered_df[filtered_df["date"] > (today - pd.Timedelta(days=7))]
        previous_period = filtered_df[
            (filtered_df["date"] <= (today - pd.Timedelta(days=7))) &
            (filtered_df["date"] > (today - pd.Timedelta(days=14)))
        ]

    elif compare_option == "Last 30 Days vs Previous 30 Days":
        latest_period = filtered_df[filtered_df["date"] > (today - pd.Timedelta(days=30))]
        previous_period = filtered_df[
            (filtered_df["date"] <= (today - pd.Timedelta(days=30))) &
            (filtered_df["date"] > (today - pd.Timedelta(days=60)))
        ]

    elif compare_option == "Custom Range":

        latest_start = panel.date_input(
            "Latest Period Start", today - pd.Timedelta(days=7), key="latest_start"
        )

        latest_end = panel.date_input(
            "Latest Period End", today, key="latest_end"
        )

        # Ensure Latest Start <= Latest End
        if latest_start > latest_end:
            st.error("⚠️ Latest Period Start must be before Latest Period End")
            return

        # Compute number of days in latest period
        latest_days = (pd.to_datetime(latest_end) - pd.to_datetime(latest_start)).days + 1

        # Let user choose where comparison period ends
        compare_end = panel.date_input(
            "Comparison End", today - pd.Timedelta(days=latest_days), key="compare_end"
        )

        compare_start = compare_end - pd.Timedelta(days=latest_days - 1)

        # Show helper text with exact ranges (fine to keep in main area)
        st.info(
            f"📊 Comparing **{latest_start.strftime('%Y-%m-%d')} → {latest_end.strftime('%Y-%m-%d')}** "
            f"(latest {latest_days} days) vs **{compare_start.strftime('%Y-%m-%d')} → {compare_end.strftime('%Y-%m-%d')}** "
            f"(previous {latest_days} days)"
        )

        # Filter data for both periods
        latest_period = filtered_df[
            filtered_df["date"].between(pd.to_datetime(latest_start), pd.to_datetime(latest_end))
        ]

        previous_period = filtered_df[
            filtered_df["date"].between(pd.to_datetime(compare_start), pd.to_datetime(compare_end))
        ]

    # --- KPI Cards (Comparison) ---
    st.subheader("Key Metrics – Comparison")

    latest_df = latest_period.sum(numeric_only=True)
    previous_df = previous_period.sum(numeric_only=True)

    def compute_derived(row):
        row["ctr"] = row["clicks"] / row["impressions"] if row["impressions"] else 0
        row["cpc"] = row["cost"] / row["clicks"] if row["clicks"] else 0
        row["cpm"] = row["cost"] / (row["impressions"] / 1000) if row["impressions"] else 0
        row["cpa"] = row["cost"] / row["conversions"] if row["conversions"] else 0
        return row

    latest = compute_derived(latest_df.copy())
    previous = compute_derived(previous_df.copy())

    # KPI Cards
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("Media Cost", f"${latest['cost']:,.0f}",
                f"{(latest['cost'] - previous['cost']) / previous['cost']:.1%}" if previous['cost'] else "N/A")
    col2.metric("Impressions", f"{latest['impressions'] / 1e6:.2f}M",
                f"{(latest['impressions'] - previous['impressions']) / previous['impressions']:.1%}"
                if previous['impressions'] else "N/A")
    col3.metric("Clicks", f"{latest['clicks']:,.0f}",
                f"{(latest['clicks'] - previous['clicks']) / previous['clicks']:.1%}"
                if previous['clicks'] else "N/A")
    col4.metric("Conversions", f"{latest['conversions']:,.0f}",
                f"{(latest['conversions'] - previous['conversions']) / previous['conversions']:.1%}"
                if previous['conversions'] else "N/A")
    col5.metric("CTR", f"{latest['ctr']:.2%}",
                f"{(latest['ctr'] - previous['ctr']) / previous['ctr']:.1%}" if previous['ctr'] else "N/A")
    col6.metric("CPC", f"${latest['cpc']:.2f}",
                f"{(latest['cpc'] - previous['cpc']) / previous['cpc']:.1%}" if previous['cpc'] else "N/A")

    st.markdown("---")

    # --- Charts ---
    st.subheader("Spend & Performance Trends")
    c1, c2 = st.columns(2)

    grouped = filtered_df.groupby("date").sum(numeric_only=True).reset_index()
    grouped["ctr"] = grouped["clicks"] / grouped["impressions"].replace(0, pd.NA)
    grouped["cpc"] = grouped["cost"] / grouped["clicks"].replace(0, pd.NA)
    grouped["cpm"] = grouped["cost"] / (grouped["impressions"].replace(0, pd.NA) / 1000)
    grouped["cpa"] = grouped["cost"] / grouped["conversions"].replace(0, pd.NA)

    with c1:
        fig = px.line(grouped, x="date", y="cost", title="Media Cost Over Time")
        fig = style_chart(fig, "Date", "Cost")
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=grouped["date"], y=grouped["impressions"], name="Impressions"))
        fig.add_trace(go.Scatter(x=grouped["date"], y=grouped["ctr"], name="CTR",
                                 mode="lines+markers", yaxis="y2"))
        fig = style_chart(fig, "Date", "Impressions", "CTR")
        st.plotly_chart(fig, use_container_width=True)

    # --- Conversions Trend ---
    st.subheader("Conversions by Date")
    fig = px.bar(grouped, x="date", y="conversions", text_auto=True, title="Conversions Over Time")
    fig = style_chart(fig, "Date", "Conversions")
    st.plotly_chart(fig, use_container_width=True)

    # --- Breakdown Section ---
    st.subheader("Platform / Campaign / Creative Insights")
    col1, col2 = st.columns(2)

    with col1:
        fig = px.bar(
            filtered_df.groupby("site").sum(numeric_only=True).reset_index(),
            x="site", y="conversions",
            title="Conversions by Platform", text_auto=True,
            hover_data={"site": True, "conversions": ":,.0f"}
        )
        fig = style_chart(fig, "Platform", "Conversions")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        campaign_grouped = filtered_df.groupby("campaign").sum(numeric_only=True).reset_index()
        fig = px.bar(
            campaign_grouped, x="campaign", y="cost",
            title="Spend by Campaign",
            hover_data={"campaign": True, "cost": ":,.0f"}
        )
        # hide long campaign labels, show only on hover
        fig.update_xaxes(showticklabels=False)
        fig = style_chart(fig, "Campaign", "Cost")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Top Creatives by Clicks")
    creative_rank = (filtered_df.groupby("creative").sum(numeric_only=True)
                     .reset_index()
                     .nlargest(10, "clicks"))
    fig = px.bar(
        creative_rank, x="creative", y="clicks",
        title="Top 10 Creatives by Clicks",
        hover_data={"creative": True, "clicks": ":,.0f"}
    )
    fig.update_xaxes(showticklabels=False)  # hide long names
    fig = style_chart(fig, "Creative", "Clicks")
    st.plotly_chart(fig, use_container_width=True)

    # --- Summary Table ---
    st.markdown("---")
    st.subheader("📋 Summary Table")

    summary_table = (
        filtered_df.groupby(
            ["advertiser", "site", "campaign", "creative", "placement", "activity_name"]
        )
        .agg({
            "impressions": "sum",
            "clicks": "sum",
            "conversions": "sum",
            "cost": "sum"
        })
        .reset_index()
    )

    summary_table["ctr"] = summary_table["clicks"] / summary_table["impressions"].replace(0, pd.NA)
    summary_table["cpc"] = summary_table["cost"] / summary_table["clicks"].replace(0, pd.NA)
    summary_table["cpm"] = summary_table["cost"] / (summary_table["impressions"].replace(0, pd.NA) / 1000)
    summary_table["cpa"] = summary_table["cost"] / summary_table["conversions"].replace(0, pd.NA)

    summary_table = summary_table.round({"ctr": 4, "cpc": 2, "cpm": 2, "cpa": 2, "cost": 2})

    st.dataframe(summary_table, use_container_width=True, hide_index=True)
