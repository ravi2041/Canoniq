import streamlit as st
from ..data import queries
from ..data.config import (
    PLATFORM_COL, CAMPAIGN_COL, CREATIVE_COL, PLACEMENT_COL,
    PLATFORM_TABLE_MAP, CM_ACTIVITY_NAME
)


def global_filters(prefix: str = "global", container=None):
    """
    Render global filters inside the given container (column, expander, etc.).
    If no container is provided, fall back to st.sidebar for backward compatibility.
    """
    c = container or st.sidebar  # where to render

    c.header("Filters")

    # ---- Platform filter (always first) ----
    all_platforms = list(PLATFORM_TABLE_MAP.keys())
    platforms = c.multiselect(
        "Platform",
        all_platforms,
        default=all_platforms,
        placeholder="All",
        key=f"{prefix}_platform",
    )

    if not platforms:
        c.warning("Please select at least one platform.")
        st.stop()

    # ---- Date range based on selected platforms ----
    min_d, max_d = queries.get_date_bounds(platforms)
    if not (min_d and max_d):
        c.error("No date range found for selected platforms.")
        st.stop()

    date_range = c.date_input(
        "Date range",
        value=(min_d, max_d),
        min_value=min_d,
        max_value=max_d,
        key=f"{prefix}_date",
    )

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start, end = date_range
    else:
        c.error("Please select a valid date range.")
        st.stop()

    # ---- More filters inside an expander (compact) ----
    with c.expander("More filters", expanded=True):
        campaigns = queries.get_distinct(CAMPAIGN_COL, platforms, start, end)
        creatives = queries.get_distinct(CREATIVE_COL, platforms, start, end)
        placements = queries.get_distinct(PLACEMENT_COL, platforms, start, end)
        activity_name = queries.get_distinct(CM_ACTIVITY_NAME, platforms, start, end)

        selected_campaigns = st.multiselect(
            "Campaign",
            campaigns,
            placeholder="All",
            key=f"{prefix}_campaign",
        )
        selected_creatives = st.multiselect(
            "Creative",
            creatives,
            placeholder="All",
            key=f"{prefix}_creative",
        )
        selected_placements = st.multiselect(
            "Placement",
            placements,
            placeholder="All",
            key=f"{prefix}_placement",
        )
        selected_activity_name = st.multiselect(
            "Activity Name",
            activity_name,
            placeholder="All",
            key=f"{prefix}_activity",
        )

    return dict(
        start=start,
        end=end,
        platforms=platforms,
        campaigns=selected_campaigns,
        creatives=selected_creatives,
        placements=selected_placements,
        activity_name=selected_activity_name,
    )
