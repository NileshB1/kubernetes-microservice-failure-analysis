
# Page: Overview - the state of the estate in one screen

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from modules.ui import data
from modules.ui.components import (
    chart,
    compact_number,
    empty_state,
    masthead,
    section,
    stat_tiles,
    status_pill,
)
from modules.ui.theme import Palette

#latency buckets are ordered, not nominal, so they get a fixed order and
# an ordinal ramp rather than eight identity hues
LATENCY_ORDER = ["low", "medium", "high", "critical"]


def _error_rate_status(error_rate: float | None) -> tuple[str, str]:
    """Map an error rate onto a reserved status level and its wording."""
    if error_rate is None:
        return "neutral", "no data"
    if error_rate < 1:
        return "good", "within budget"
    if error_rate < 5:
        return "warning", "above 1% budget"
    if error_rate < 15:
        return "serious", "sustained failures"
    return "critical", "estate-wide failure"


def render(pal: Palette, services: tuple[str, ...]) -> None:
    masthead(
        "Microservice reliability overview",
        "Distributed analysis of Kubernetes telemetry: how failures spread between "
        "services, which behaviour is genuinely abnormal, and how the Spark pipeline "
        "scales with data volume.",
        eyebrow="Data Intensive & Scalable Systems",
    )

    if not data.has_data("processed_telemetry"):
        empty_state(
            "No processed telemetry yet",
            "The overview reads the processed_telemetry table, which module 2 "
            "(preprocessing) writes. Run the pipeline from the Operations page, or "
            "start the dashboard in demo mode with: python run_streamlit.py --local",
        )
        return

    summary=data.telemetry_summary(services)
    anomalies=data.anomaly_summary(services) if data.has_data("anomaly_scores") else {}

    level, wording = _error_rate_status(summary["error_rate"])
    error_rate_text = f"{summary['error_rate']:.2f}" if summary["error_rate"] is not None else "-"
    latency_text = f"{summary['avg_latency']:.0f}" if summary["avg_latency"] is not None else "-"

    stat_tiles(
        [
            {
                "label": "Requests analysed", "value": compact_number(summary["total_records"]),
                "note": f"{compact_number(summary['traces'])} distinct traces",
            },
            {  "label": "Error rate", "value": error_rate_text,
                "unit": "%", "note": wording,
                "status": level,
            },
            {
                "label": "Mean response time", "value": latency_text, "unit": "ms",
                "note": f"across {summary['services']} services",
            },
            {
                "label": "Multi-signal anomalies",
                "value": compact_number(anomalies.get("overall", 0)),
                "note": (
                    f"{anomalies['anomaly_rate']:.1f}% of {compact_number(anomalies['total_buckets'])} buckets"
                    if anomalies.get("anomaly_rate") is not None
                    else "run failure detection (RQ2)"
                ),
            },
        ]
    )
    _traffic_and_failures(pal,services)
    _service_leaderboard(pal,services)
    _outcome_and_latency(pal,services)


def _traffic_and_failures(pal: Palette, services: tuple[str, ...]) -> None:
    section(
        "Traffic and failures by hour",
        "Request volume against failures for the same hour. Two measures of very "
        "different magnitude share one time axis, so they are drawn as two panels "
        "rather than two y-scales on one plot.",
    )

    hourly = data.error_rate_by_hour(services)
    if hourly.empty:
        empty_state("No hourly data", "processed_telemetry has no hour_of_day values.")
        return

    hourly = hourly.copy()
    hourly["failures"] = hourly["failures"].fillna(0)
    hourly["error_rate_pct"] = (hourly["failures"] / hourly["requests"].replace(0, pd.NA) * 100).astype(float)

    left, right = st.columns(2, gap="medium")

    with left:
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=hourly["hour_of_day"], y=hourly["requests"],
                name="Requests", marker=dict(color=pal.slot(0), line=dict(width=0)),
                hovertemplate="Hour %{x}<br>%{y:,} requests<extra></extra>",
            )
        )
        fig.update_layout(title="Requests per hour", bargap=0.35)
        fig.update_xaxes(title="Hour of day", dtick=3)
        fig.update_yaxes(title="Requests")
        chart(
            fig, pal, data=hourly[["hour_of_day", "requests"]],
            download_name="requests_by_hour.csv", show_legend=False
        )

    with right:
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=hourly["hour_of_day"],  y=hourly["error_rate_pct"],
                mode="lines+markers", name="Error rate",
                line=dict(color=pal.critical, width=2),
                marker=dict(size=8, color=pal.critical, line=dict(width=2, color=pal.surface)),
                hovertemplate="Hour %{x}<br>%{y:.2f}% failed<extra></extra>")
        )
        fig.update_layout(title="Error rate per hour", hovermode="x unified")
        fig.update_xaxes(title="Hour of day", dtick=3)
        fig.update_yaxes(title="Failed requests (%)", rangemode="tozero")
        chart(
            fig, pal,
            data=hourly[["hour_of_day", "failures", "error_rate_pct"]].round(3),
            download_name="error_rate_by_hour.csv", show_legend=False
        )


def _service_leaderboard(pal: Palette, services: tuple[str, ...]) -> None:
    section(
        "Where the failures are",
        "Services ranked by failed requests. One series, one colour - bar length "
        "already encodes the magnitude, so hue is left free to carry meaning elsewhere.",
    )

    health = data.service_health_table(services)
    if health.empty:
        empty_state("No service data", "No rows matched the current service filter.")
        return

    health = health.copy()
    health["failures"] = health["failures"].fillna(0).astype(int)
    health["error_rate_pct"] = (
        (health["failures"] / health["requests"].replace(0, pd.NA) * 100).astype(float).round(2)
    )
    health["avg_latency_ms"] = health["avg_latency_ms"].astype(float).round(1)

    top = health.nlargest(12, "failures").sort_values("failures")

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=top["failures"], y=top["service_name"],
            orientation="h", marker=dict(color=pal.slot(0), line=dict(width=0)),
            # Direct-label the bar ends: the value is readable without hovering.
            text=[f"{v:,}" for v in top["failures"]],
            textposition="outside", textfont=dict(color=pal.ink_secondary, size=11),
            cliponaxis=False, hovertemplate="%{y}<br>%{x:,} failed requests<extra></extra>",
        )
    )
    fig.update_layout(title="Failed requests by service", bargap=0.3)
    fig.update_xaxes(title="Failed requests")
    fig.update_yaxes(title=None)
    chart(fig, pal,
        data=health, height=max(320, 26 * len(top) + 90),
        download_name="service_health.csv", table_label="View all services",
        show_legend=False)


def _outcome_and_latency(pal: Palette, services: tuple[str, ...]) -> None:
    left, right = st.columns(2, gap="medium")

    with left:
        section("Response outcomes", "How every request ended.")
        outcomes = data.error_categories(services)
        if outcomes.empty:
            empty_state("No outcome data", "error_category is not populated.")
        else:
            # Outcome is a status, not an identity: reserved status colours,
            # each paired with its own written label on the axis.
            outcome_colours = {
                "success": pal.good, "client_error": pal.warning,
                "server_error": pal.critical }
            ordered = outcomes.sort_values("requests")
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=ordered["requests"],  y=ordered["error_category"],
                    orientation="h",
                    marker=dict(
                        color=[outcome_colours.get(c, pal.ink_muted) for c in ordered["error_category"]],
                        line=dict(width=0),
                    ),
                    text=[f"{v:,}" for v in ordered["requests"]],
                    textposition="outside", textfont=dict(color=pal.ink_secondary, size=11),
                    cliponaxis=False,  hovertemplate="%{y}<br>%{x:,} requests<extra></extra>",
                )
            )
            fig.update_layout(title="Requests by outcome", bargap=0.4)
            fig.update_xaxes(title="Requests")
            fig.update_yaxes(title=None)
            chart(
                fig, pal, data=outcomes, height=280, download_name="response_outcomes.csv", show_legend=False
            )

    with right:
        section("Latency profile", "Requests grouped into ordered latency bands.")
        buckets = data.latency_distribution(services)
        if buckets.empty:
            empty_state("No latency buckets", "latency_bucket is not populated.")
        else:
            present = [b for b in LATENCY_ORDER if b in set(buckets["latency_bucket"])]
            ordered = buckets.set_index("latency_bucket").reindex(present).reset_index()
            # Ordered bands get an ordinal ramp, stepped so the lightest
            # value still separates from the surface.
            ramp = [pal.sequential[3], pal.sequential[6], pal.sequential[8], pal.sequential[10]]
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=ordered["latency_bucket"], y=ordered["requests"],
                    marker=dict(color=ramp[: len(ordered)], line=dict(width=0)),
                    text=[f"{v:,}" for v in ordered["requests"]],
                    textposition="outside",  textfont=dict(color=pal.ink_secondary, size=11),
                    cliponaxis=False,  hovertemplate="%{x} latency<br>%{y:,} requests<extra></extra>"
                )
            )
            fig.update_layout(title="Requests by latency band", bargap=0.4)
            fig.update_xaxes(title=None)
            fig.update_yaxes(title="Requests")
            chart(fig, pal, data=ordered,
                height=280, download_name="latency_distribution.csv",
                show_legend=False)

    worst = data.service_health_table(services)
    if not worst.empty:
        worst = worst.copy()
        worst["failures"] = worst["failures"].fillna(0)
        worst["error_rate_pct"] = (worst["failures"] / worst["requests"].replace(0, pd.NA) * 100).astype(
            float
        )
        top_row = worst.nlargest(1, "error_rate_pct").iloc[0]
        level, _ = _error_rate_status(float(top_row["error_rate_pct"]))
        st.markdown(f"{status_pill(level, 'Highest error rate')} "
            f"**{top_row['service_name']}** - {top_row['error_rate_pct']:.1f}% of "
            f"{int(top_row['requests']):,} requests failed.",
            unsafe_allow_html=True)
