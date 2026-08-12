
# Service explorer - one service at a time


from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from modules.ui import data
from modules.ui.components import (
    chart, compact_number,
    empty_state, masthead, section,stat_tiles,
    status_pill,
)
from modules.ui.theme import Palette


def render(pal: Palette, services: tuple[str, ...]) -> None:
    masthead(
        "Service explorer", "Everything the pipeline knows about a single service: its traffic, its "
        "reliability, who it takes down, and who takes it down.",
        eyebrow="Operational view",
    )

    if not data.has_data("processed_telemetry"):
        empty_state(
            "No processed telemetry yet",
            "Run module 2 (preprocessing), or start in demo mode with " "python run_streamlit.py --local",
        )
        return

    all_services = data.service_names()
    if not all_services:
        empty_state("No services found", "processed_telemetry contains no service names.")
        return

    # The sidebar filter narrows the choices; it does not remove the picker,
    # because this page is inherently about one service.
    choices = [s for s in all_services if not services or s in services] or all_services
    selected = st.selectbox("Service", choices, key="service_explorer_choice")

    _headline(pal, selected)
    _traffic(pal, selected)
    _relationships(pal, selected)


def _headline(pal: Palette, service: str) -> None:
    health = data.service_health_table((service,))
    if health.empty:
        empty_state("No data for this service", f"No telemetry rows for {service}.")
        return

    row = health.iloc[0]
    requests = int(row["requests"])
    failures = int(row["failures"] or 0)
    error_rate = failures / requests * 100 if requests else 0.0

    if error_rate < 1:
        level, wording = "good", "within budget"
    elif error_rate < 5:
        level, wording = "warning", "above 1% budget"
    elif error_rate < 15:
        level, wording = "serious", "sustained failures"
    else:
        level, wording = "critical", "critical failure rate"

    anomalies = data.anomaly_summary((service,)) if data.has_data("anomaly_scores") else {}

    stat_tiles(
        [
            {"label": "Requests", "value": compact_number(requests), "note": f"{failures:,} failed"},
            {
                "label": "Error rate", "value": f"{error_rate:.2f}",
                "unit": "%",
                "note": wording, "status": level,
            },
            {
                "label": "Mean latency",
                "value": f"{float(row['avg_latency_ms']):.0f}",
                "unit": "ms", "note": f"CPU {float(row['avg_cpu_mcores']):.0f} mcores",
            },
            {
                "label": "Anomalous buckets",
                "value": compact_number(anomalies.get("overall", 0)),
                "note": (
                    f"of {compact_number(anomalies.get('total_buckets', 0))} analysed"
                    if anomalies
                    else "run failure detection (RQ2)"
                ),
            },
        ]
    )


def _traffic(pal: Palette, service: str) -> None:
    section("Traffic shape", "Hourly request volume and the share that failed.")

    hourly = data.error_rate_by_hour((service,))
    if hourly.empty:
        empty_state("No hourly data", "hour_of_day is not populated for this service.")
        return

    frame = hourly.copy()
    frame["failures"] = frame["failures"].fillna(0)
    frame["successes"] = frame["requests"] - frame["failures"]

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=frame["hour_of_day"],  y=frame["successes"],
            name="Succeeded",  marker=dict(color=pal.slot(0), line=dict(width=2, color=pal.surface)),
            hovertemplate="Hour %{x}<br>%{y:,} succeeded<extra></extra>",
        )
    )
    fig.add_trace(
        go.Bar(
            x=frame["hour_of_day"],  y=frame["failures"],
            name="Failed",  marker=dict(color=pal.critical, line=dict(width=2, color=pal.surface)),
            hovertemplate="Hour %{x}<br>%{y:,} failed<extra></extra>",
        )
    )
    fig.update_layout(
        title=f"{service} - requests by hour", barmode="stack", bargap=0.3, hovermode="x unified"
    )
    fig.update_xaxes(title="Hour of day", dtick=2)
    fig.update_yaxes(title="Requests")
    chart(fig, pal, data=frame, height=340, download_name=f"{service}_hourly.csv")


def _relationships(pal: Palette, service: str) -> None:
    if not data.has_data("cross_service_pairs"):
        return

    section(
        "Blast radius", "Downstream services whose failures coincide with this one's (what it takes "
        "down), and upstream services whose failures coincide with this one's "
        "(what takes it down).",
    )

    pairs = data.propagation_pairs()
    if pairs.empty:
        empty_state("No propagation data", "cross_service_pairs is empty.")
        return

    downstream = (
        pairs[pairs["caller_service"] == service]
        .nlargest(10, "propagation_score")
        .sort_values("propagation_score")
    )
    upstream = (
        pairs[pairs["callee_service"] == service]
        .nlargest(10, "propagation_score")
        .sort_values("propagation_score")
    )

    left, right = st.columns(2, gap="medium")

    with left:
        if downstream.empty:
            st.markdown(
                status_pill("good", "No downstream impact")
                + f" &nbsp;{service} is not a caller in any recorded pair.",
                unsafe_allow_html=True,
            )
        else:
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=downstream["propagation_score"],  y=downstream["callee_service"],
                    orientation="h",     marker=dict(color=pal.slot(1), line=dict(width=0)),
                    text=[f"{v:.2f}" for v in downstream["propagation_score"]],
                    textposition="outside",
                    textfont=dict(color=pal.ink_secondary, size=11),
                    cliponaxis=False,
                    hovertemplate="%{y}<br>propagation score %{x:.3f}<extra></extra>"
                )
            )
            fig.update_layout(title=f"{service} -> downstream", bargap=0.3)
            fig.update_xaxes(title="Propagation score", range=[0, 1.08])
            fig.update_yaxes(title=None)
            chart(
                fig, pal,
                data=downstream.round(4), height=max(280, 28 * len(downstream) + 90),
                download_name=f"{service}_downstream.csv", show_legend=False
            )

    with right:
        if upstream.empty:
            st.markdown(
                status_pill("good", "No upstream exposure")
                + f" &nbsp;{service} is not a callee in any recorded pair.",
                unsafe_allow_html=True,
            )
        else:
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=upstream["propagation_score"], y=upstream["caller_service"],
                    orientation="h", marker=dict(color=pal.slot(6), line=dict(width=0)),
                    text=[f"{v:.2f}" for v in upstream["propagation_score"]],
                    textposition="outside", textfont=dict(color=pal.ink_secondary, size=11),
                    cliponaxis=False, hovertemplate="%{y}<br>propagation score %{x:.3f}<extra></extra>",
                )
            )
            fig.update_layout(title=f"upstream -> {service}", bargap=0.3)
            fig.update_xaxes(title="Propagation score", range=[0, 1.08])
            fig.update_yaxes(title=None)
            chart(
                fig, pal, data=upstream.round(4),
                height=max(280, 28 * len(upstream) + 90),
                download_name=f"{service}_upstream.csv",   show_legend=False
            )
