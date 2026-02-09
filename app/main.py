# app/main.py
import os, sys
import streamlit as st

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PARENT = os.path.abspath(os.path.join(ROOT, ".."))
for p in (PARENT, ROOT):
    if p not in sys.path:
        sys.path.insert(0, p)

from app.data.queries import get_platform_slice_with_cm360
from app.routing import get_page
from app.ui.filters import global_filters
from app.ui.layout import apply_theme, app_header, sidebar_brand, page_section_title
from app.dashboards.business_overview import index as business
from app.dashboards.homepage import index as home
from app.dashboards.nlq_analytics import index as nlq

# ---------- PAGE CONFIG ----------
st.set_page_config(
    page_title="CanonIQ – Marketing Analytics",
    layout="wide",
)

# ---------- LOAD THEME ----------
if "theme" not in st.session_state:
    st.session_state["theme"] = "light"
apply_theme()

# ---------- NAVIGATION ----------
PAGES = [
    "Homepage",
    "Business Overview",
    "Media Assistant",
    "NLQ Analytics",
    "NLQ Chat (beta)",
    "Data Quality",
]

with st.sidebar:
    sidebar_brand()

    page_name = st.radio(
        "Navigation",
        options=PAGES,
        index=0,
    )

    st.markdown(
        "<div style='font-size:0.75rem; color:#6b7280; margin-top:1.5rem;'>"
        "v0.1 • Internal prototype"
        "</div>",
        unsafe_allow_html=True,
    )

# ---------- GLOBAL HEADER (FULL WIDTH, WITH THEME TOGGLE) ----------
# small spacer so header never touches the Streamlit top bar
st.markdown("<div style='margin-top:0.75rem;'></div>", unsafe_allow_html=True)

# Brand + theme (NO section label here)


# Decide section label based on current page
SECTION_LABELS = {
    "Homepage": "Homepage",
    "Business Overview": "Executive Dashboard",
    "Media Assistant": "Media Assistant",
    "NLQ Analytics": "NLQ Analytics",
    "NLQ Chat (beta)": "NLQ Chat (beta)",
    "Data Quality": "Data Quality Monitor",
}

section_label = SECTION_LABELS.get(page_name, page_name)

app_header(
    section_label=section_label,
    product_name="CanonIQ",
    company_logo="D:/Analytics_AI/app/ui/images/clarity labs logo.jpg",
)

# This is the ONLY place we render the middle title
#page_section_title(section_label)

st.markdown("---")

# ---------- LAYOUT: CONTENT + RIGHT FILTERS ----------
filters = None
content_col, filter_col = st.columns([0.80, 0.20], gap="large")


# RIGHT: Filters panel
with filter_col:
    if page_name == "Media Assistant":
        # Global filters for Media Assistant, rendered in the right column
        filters = global_filters(prefix="media", container=filter_col)

    elif page_name == "Business Overview":
        # Filters for Business Overview are rendered inside the dashboard
        # via the filter_container argument, so nothing to draw here.
        st.empty()

    else:
        st.empty()

# CENTER: Page content
with content_col:
    # ---- Routing ----
    if page_name == "Media Assistant":
        if filters is None:
            st.info("Use the filters on the right to load media performance.")
        else:
            df = get_platform_slice_with_cm360(
                filters["start"],
                filters["end"],
                filters["platforms"],
                filters["campaigns"],
                filters["creatives"],
                filters["placements"],
            )
            render = get_page(page_name)
            render(df)

    elif page_name == "NLQ Analytics":


        nlq.render(side_panel=filter_col)

    elif page_name == "Business Overview":
        # Business Overview dashboard with its own filters

        business.render(filter_container=filter_col)

    elif page_name == "Homepage":

        home.render()

    else:
        render = get_page(page_name)
        render()
