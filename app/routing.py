# app/routing.py
from typing import Callable
import importlib

# ======================================================
# 🧭 Page Registry
# ======================================================
# Each entry maps a page name (as shown in the UI) to its module and render function.
PAGES: dict[str, dict] = {
    "Homepage": {
        "module": "app.dashboards.homepage.index",
        "func": "render",
    },
    "Business Overview": {
        "module": "app.dashboards.business_overview.index",
        "func": "render",
    },
    "Media Assistant": {
        "module": "app.dashboards.media_assistant.index",
        "func": "render",
    },
    "NLQ Analytics": {
        "module": "app.dashboards.nlq_analytics.index",
        "func": "render",
    },
    "Decision Monitor": {  # ✅ match the label used in main.py sidebar
        "module": "app.dashboards.decision_monitor",
        "func": "render",
    },
    "NLQ Chat (beta)": {
        "module": "app.dashboards.chat_nlq.index",
        "func": "render",
    },
    "Data Quality": {
        "module": "app.dashboards.data_quality.index",
        "func": "render",
    },
}


# ======================================================
# 🔄 Dynamic Page Loader
# ======================================================
def get_page(name: str) -> Callable:
    """
    Dynamically imports and returns the `render()` function
    for the given dashboard name.

    Args:
        name (str): Human-readable page name from the navigation sidebar.

    Returns:
        Callable: The render() function for the corresponding module.
    """
    if name not in PAGES:
        raise ValueError(f"❌ Page '{name}' not found in routing table.")
    spec = PAGES[name]
    mod = importlib.import_module(spec["module"])
    return getattr(mod, spec["func"])
