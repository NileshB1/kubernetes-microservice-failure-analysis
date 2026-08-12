
# Streamlit Dashboard - Entry Point


from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

# Allow `streamlit run modules/dashboard.py` from a checkout that has not
# been pip-installed
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules.settings import get_settings  # noqa: E402
from modules.ui import data  # noqa: E402
from modules.ui.components import status_pill  # noqa: E402
from modules.ui.pages import anomalies, operations, overview, propagation, scalability, services  # noqa: E402
from modules.ui.theme import inject_theme  # noqa: E402

st.set_page_config(
    page_title="Microservice Failure Analysis",
    # Shortcode rather than a literal glyph: keeps this file plain ASCII
    # while Streamlit still renders a real favicon.
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "about": (
            "Distributed analysis of Kubernetes microservice logs for failure "
            "detection - cross-service propagation (RQ1), abnormal failure "
            "patterns (RQ2), and Spark scalability (RQ3)."
        )
    },
)

PAGES = {
    "Overview": overview.render,
    "RQ1 - Propagation": propagation.render,
    "RQ2 - Anomalies": anomalies.render,
    "RQ3 - Scalability": scalability.render,
    "Service explorer": services.render,
    "Operations": operations.render,
}


def _sidebar() -> tuple[str, tuple[str, ...]]:
    """
    Render the sidebar and return the chosen page and service filter.

    """
    with st.sidebar:
        st.markdown(
            '<p style="font-size:0.72rem;font-weight:600;letter-spacing:0.08em;'
            'text-transform:uppercase;color:var(--ink-muted);margin:0 0 0.15rem">'
            "Kubernetes telemetry</p>"
            '<p style="font-size:1.05rem;font-weight:660;letter-spacing:-0.01em;'
            'color:var(--ink-primary);margin:0 0 1rem">Failure analysis</p>',
            unsafe_allow_html=True,
        )

        page = st.radio("View", list(PAGES), label_visibility="collapsed")

        st.divider()

        selected: tuple[str, ...] = ()
        if data.has_data("processed_telemetry"):
            names = data.service_names()
            if names:
                chosen = st.multiselect(
                    "Filter services",
                    names,
                    default=[],
                    placeholder="All services",
                    help="Scopes every panel that is per-service. " "Leave empty to include all.",
                )
                selected = tuple(chosen)
                st.caption(
                    f"Showing {len(selected)} of {len(names)} services"
                    if selected
                    else f"Showing all {len(names)} services"
                )

        st.divider()

        health = data.db_health()
        if health.get("connected"):
            st.markdown(
                status_pill("good", health["backend"]) + " connected",
                unsafe_allow_html=True,
            )
            st.caption(f"{health['total_rows']:,} rows across {len(health['tables'])} tables")
        else:
            st.markdown(status_pill("critical", "Database down"), unsafe_allow_html=True)
            st.caption(f"Could not reach {health.get('backend')} at {health.get('location')}.")

        if st.button("Refresh data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        st.caption(
            "Theme follows your Streamlit setting - change it under the "
            "three-dot menu -> Settings."
        )

    return page, selected


def main() -> None:
    palette = inject_theme()

    # Surface configuration problems once, at the top, rather than letting
    # them appear as confusing per-panel failures further down.
    problems = get_settings().problems()
    if problems:
        st.warning("Configuration needs attention:\n\n" + "\n".join(f"- {problem}" for problem in problems))

    page, selected_services = _sidebar()
    PAGES[page](palette, selected_services)

    st.divider()
    st.caption(
        "Distributed Analysis of Kubernetes Microservice Logs for Failure Detection - "
        "Data Intensive & Scalable Systems"
    )

#main
if __name__ == "__main__":
    main()
