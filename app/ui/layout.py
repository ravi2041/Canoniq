# app/ui/layout.py
from pathlib import Path
import streamlit as st

BASE_DIR = Path(__file__).resolve().parents[1]   # app/


def _find_css(theme: str) -> Path | None:
    """
    Locate CSS inside: app/assets/styles/light.css or dark.css
    """
    css_path = BASE_DIR / "assets" / "styles" / f"{theme}.css"
    return css_path if css_path.exists() else None


def apply_theme() -> None:
    """Inject theme CSS based on session_state['theme']."""
    theme = st.session_state.get("theme", "light")
    css_path = _find_css(theme)

    if css_path is None:
        return  # No CSS found → do nothing

    with open(css_path, "r", encoding="utf-8") as f:
        css = f.read()

    st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)


def sidebar_brand() -> None:
    """App brand block inside left navigation."""
    st.markdown(
        """
        <div style="
            display:flex;
            align-items:center;
            gap:0.6rem;
            margin-bottom:1.4rem;
        ">
            <div style="font-size:1.7rem;">🧠</div>
            <div style="display:flex;flex-direction:column;">
                <span style="font-weight:600;font-size:1.05rem;">
                    CanonIQ
                </span>
                <span style="font-size:0.80rem;color:#9ca3af;">
                    Marketing copilot for busy analysts
                </span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def theme_toggle_top_right() -> None:
    """Light/Dark switch shown in the top-right corner of header."""
    current = st.session_state.get("theme", "light")
    new = st.radio(
        "Theme",
        options=["light", "dark"],
        index=0 if current == "light" else 1,
        horizontal=True,
        label_visibility="collapsed",
    )
    if new != current:
        st.session_state["theme"] = new
        st.rerun()


def app_header(
    section_label: str,
    product_name: str = "CanonIQ",
    company_logo: str | None = None,   # path to image OR None
) -> None:
    """
    Header layout:

    LEFT  : logo (max width 150px) if file exists, otherwise product_name text
    CENTER: section/dashboard title
    RIGHT : theme toggle
    """

    col_left, col_center, col_right = st.columns([1.6, 2, 1])

    # ---------- LEFT COLUMN ----------
    with col_left:
        logo_rendered = False

        if company_logo:
            logo_path = Path(company_logo)

            # Allow passing relative paths like "ui/images/logo.jpg"
            if not logo_path.is_absolute():
                logo_path = BASE_DIR / logo_path

            if logo_path.exists():
                # Show ONLY the image, with controlled size
                st.image(str(logo_path), width=150)
                logo_rendered = True

        if not logo_rendered:
            # Fallback: show just the product name
            st.markdown(
                f"""
                <div style="display:flex;align-items:center;height:100%;">
                    <span style="font-size:1.25rem;font-weight:600;color:#1f2937;">
                        {product_name}
                    </span>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # ---------- CENTER COLUMN ----------
    with col_center:
        st.markdown(
            f"""
            <div style="
                display:flex;
                justify-content:center;
                align-items:center;
                height:100%;
            ">
                <span style="font-size:1.45rem;font-weight:650;color:#374151;">
                    {section_label}
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # ---------- RIGHT COLUMN (theme toggle) ----------
    with col_right:
        st.markdown(
            """
            <div style="
                display:flex;
                justify-content:flex-end;
                align-items:center;
                padding-right:0.5rem;
            ">
            """,
            unsafe_allow_html=True,
        )
        theme_toggle_top_right()
        st.markdown("</div>", unsafe_allow_html=True)


def page_section_title(section_label: str) -> None:
    """Centered dashboard title between navigation and filters."""
    st.markdown(
        f"""
        <div style="
            text-align:center;
            font-size:3.3rem;
            font-weight:600;
            margin:0;
            padding:0.4rem 0 0.8rem 0;
            color:#374151;
        ">
            {section_label}
        </div>
        """,
        unsafe_allow_html=True,
    )
