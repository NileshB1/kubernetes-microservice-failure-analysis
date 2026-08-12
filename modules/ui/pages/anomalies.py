
# Page: RQ2 - Abnormal failure pattern detection


from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from modules.ui import data
from modules.ui.components import chart, compact_number, empty_state, masthead, section, stat_tiles
from modules.ui.theme import Palette

# Human wording for the pattern labels the clustering step emits.
PATTERN_LABELS = {"full_failure": "Full failure (all three signals)",
    "cascading_failure": "Cascading failure (errors + latency)",
    "resource_exhaustion": "Resource exhaustion (latency + resources)",
    "error_resource_link": "Error/resource link",
    "error_surge": "Error surge", "latency_spike": "Latency spike",
    "resource_pressure": "Resource pressure"}


def render(pal: Palette, services: tuple[str, ...]):
    masthead(
        "Abnormal failure patterns",
        "Every service-time bucket is tested against three independent signals: error "
        "rate, latency, and resource use. A bucket tripping two or more is treated as a "
        "genuine anomaly rather than ordinary noise.",
        eyebrow="Research question 2", badge="RQ2",
    )

    if not data.has_data("anomaly_scores"):
        empty_state(
            "No anomaly analysis yet",
            "This page reads anomaly_scores and failure_patterns, written by module 4 "
            "(failure_detection). Run it from the Operations page, or start in demo "
            "mode with python run_streamlit.py --local",
        )
        return

    summary = data.anomaly_summary(services)
    rate = summary.get("anomaly_rate")

    stat_tiles(
        [{
                "label": "Buckets analysed", "value": compact_number(summary.get("total_buckets", 0)),
                "note": "service x time windows"
            },
            {
                "label": "Confirmed anomalies", "value": compact_number(summary.get("overall", 0)),
                "note": f"{rate:.1f}% of all buckets" if rate is not None else "-",
                "status": "good" if (rate or 0) < 5 else ("warning" if (rate or 0) < 15 else "serious"),
            },
            {
                "label": "Error-rate signal",  "value": compact_number(summary.get("error_signal", 0)),
                "note": "buckets flagged"
            },
            {
                "label": "Latency signal", "value": compact_number(summary.get("latency_signal", 0)),
                "note": "buckets flagged"
            }])

    _timeline(pal, services)
    _ranking_and_signals(pal, services, summary)
    _regression_vs_chronic(pal, services)
    _patterns(pal, services)


def _timeline(pal: Palette, services: tuple[str, ...]):
    section(
        "When the system was in trouble", "Flagged buckets over time, one panel per signal. Four detectors overlaid on "
        "one axis cross each other constantly and read as noise; stacked panels "
        "sharing an x-axis keep every series legible and still line the spikes up.",
    )

    timeline = data.anomaly_timeline(services)
    if timeline.empty:
        empty_state("No timeline data", "anomaly_scores has no time buckets.")
        return

    frame = timeline.copy()
    frame["time_bucket"] = pd.to_datetime(frame["time_bucket"], errors="coerce")
    frame = frame.dropna(subset=["time_bucket"]).sort_values("time_bucket")
    if frame.empty:
        empty_state("No timeline data", "time_bucket values could not be parsed.")
        return

    series_spec = [
        ("error_signal", "Error rate", pal.slot(0)), ("latency_signal", "Latency", pal.slot(1)),
        ("resource_signal", "Resources", pal.slot(2)),
        ("multi_signal", "Multi-signal (confirmed)", pal.slot(7))
    ]

    fig = make_subplots(
        rows=len(series_spec), cols=1,
        shared_xaxes=True, vertical_spacing=0.06,
        subplot_titles=[label for _, label, _ in series_spec],
    )

    # A shared y-range keeps the panels honest: a spike that looks taller
    # really is taller, rather than each panel rescaling to its own max.
    y_max = max(1, int(frame[[column for column, _, _ in series_spec]].to_numpy().max()))

    for row, (column, label, colour) in enumerate(series_spec, start=1):
        fig.add_trace(
            go.Scatter(
                x=frame["time_bucket"],
                y=frame[column], mode="lines",
                name=label, line=dict(color=colour, width=1.75),
                fill="tozeroy", fillcolor=_translucent(colour),
                hovertemplate=f"{label}: %{{y:,}}<extra></extra>",
            ),
            row=row, col=1)
        fig.update_yaxes(range=[0, y_max * 1.1], row=row, col=1, title=None)

    fig.update_annotations(font=dict(size=11, color=pal.ink_secondary))
    # Each panel is named by its subplot title, so the figure has no
    # overall title. Note: never write update_layout(title=None) - that
    # emits an empty title object that Plotly.js draws as 'undefined'.
    fig.update_layout(hovermode="x unified")
    fig.update_xaxes(title=None)
    chart(fig, pal, data=frame,
        height=420,  download_name="anomaly_timeline.csv",
        show_legend=False)


def _translucent(hex_colour: str, alpha: float = 0.16) -> str:
    """Return the same hue as a low-opacity fill under its own line."""
    raw = hex_colour.lstrip("#")
    red, green, blue = (int(raw[i : i + 2], 16) for i in (0, 2, 4))
    return f"rgba({red},{green},{blue},{alpha})"


def _ranking_and_signals(pal: Palette, services: tuple[str, ...], summary: dict) -> None:
    left, right = st.columns([3, 2], gap="medium")

    with left:
        section("Worst-affected services", "Ranked by confirmed multi-signal anomalies.")
        ranked = data.anomalies_by_service(services, limit=12)
        if ranked.empty:
            empty_state(
                "No confirmed anomalies", "No bucket tripped two or more signals. Lower "
                "anomaly_zscore_threshold in config.yaml to widen detection.")
        else:
            frame = ranked.sort_values("anomaly_count")
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=frame["anomaly_count"], y=frame["service_name"],
                    orientation="h",  marker=dict(color=pal.slot(7), line=dict(width=0)),
                    text=[f"{v:,}" for v in frame["anomaly_count"]],
                    textposition="outside",  textfont=dict(color=pal.ink_secondary, size=11),
                    cliponaxis=False,
                    hovertemplate="%{y}<br>%{x:,} confirmed anomalies<extra></extra>",
                )
            )
            fig.update_layout(title="Confirmed anomalies by service", bargap=0.3)
            fig.update_xaxes(title="Anomalous buckets")
            fig.update_yaxes(title=None)
            chart(fig, pal,
                data=ranked, height=max(320, 28 * len(frame) + 90),
                download_name="anomalies_by_service.csv",
                show_legend=False)

    with right:
        section("Signal contribution", "How often each detector fired.")
        signals = pd.DataFrame(
            {
                "signal": ["Error rate", "Latency", "Resources"],
                "buckets": [
                    summary.get("error_signal", 0),  summary.get("latency_signal", 0),
                    summary.get("resource_signal", 0)
                ],
            }
        )
        if signals["buckets"].sum() == 0:
            empty_state("No signals fired", "No detector flagged any bucket.")
        else:
            ordered = signals.sort_values("buckets")
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=ordered["buckets"], y=ordered["signal"],
                    orientation="h",
                    marker=dict(
                        color=[pal.slot(2), pal.slot(1), pal.slot(0)][: len(ordered)], line=dict(width=0)
                    ),
                    text=[f"{v:,}" for v in ordered["buckets"]],
                    textposition="outside", textfont=dict(color=pal.ink_secondary, size=11),
                    cliponaxis=False, hovertemplate="%{y}<br>%{x:,} buckets<extra></extra>"
                )
            )
            fig.update_layout(title="Buckets flagged per signal", bargap=0.4)
            fig.update_xaxes(title="Buckets")
            fig.update_yaxes(title=None)
            chart(
                fig, pal, data=signals, height=280, download_name="signal_contribution.csv", show_legend=False
            )


def _regression_vs_chronic(pal: Palette, services: tuple[str, ...]) -> None:
    section(
        "Regressing or chronically bad?",
        "A z-score flag means a service deviated from its own baseline - something "
        "changed. An SLO breach means it exceeded an absolute ceiling regardless of "
        "history. A service that is always broken never deviates from itself, so only "
        "the SLO signal can see it.",
    )

    breaches = data.slo_breach_summary(services)
    if breaches.empty:
        empty_state("No breach data", "anomaly_scores has no SLO breach columns populated.")
        return

    frame = breaches.copy()
    for column in ("error_slo_breaches", "latency_slo_breaches", "overall_anomalies"):
        frame[column] = frame[column].fillna(0).astype(int)
    frame["slo_breaches"] = frame["error_slo_breaches"] + frame["latency_slo_breaches"]
    frame = frame[frame["slo_breaches"] > 0].nlargest(12, "slo_breaches").sort_values("slo_breaches")

    if frame.empty:
        st.markdown(
            "No service breached an absolute SLO ceiling - every flag came from a "
            "deviation against the service's own baseline."
        )
        return

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=frame["error_slo_breaches"],  y=frame["service_name"],
            orientation="h", name="Error-rate SLO breach",
            marker=dict(color=pal.slot(0), line=dict(width=2, color=pal.surface)),
            hovertemplate="%{y}<br>%{x:,} error-rate breaches<extra></extra>",
        )
    )
    fig.add_trace(
        go.Bar(
            x=frame["latency_slo_breaches"],
            y=frame["service_name"], orientation="h", name="Latency SLO breach",
            marker=dict(color=pal.slot(1), line=dict(width=2, color=pal.surface)),
            hovertemplate="%{y}<br>%{x:,} latency breaches<extra></extra>",
        )
    )
    fig.update_layout(title="SLO breaches by service", barmode="stack", bargap=0.3)
    fig.update_xaxes(title="Buckets breaching an SLO")
    fig.update_yaxes(title=None)
    chart(
        fig, pal, data=breaches,
        height=max(320, 28 * len(frame) + 110),
        download_name="slo_breaches.csv",  table_label="View all services")


def _patterns(pal: Palette, services: tuple[str, ...]):
    section(
        "Failure pattern mix",
        "Anomalies grouped by which combination of signals fired. Seven ordered "
        "categories of close magnitude, so a ranked bar chart rather than a pie.")

    patterns = data.failure_patterns(services)
    if patterns.empty:
        empty_state("No patterns recorded", "failure_patterns is empty.")
        return

    frame = patterns.copy()
    frame["label"] = frame["pattern_type"].map(PATTERN_LABELS).fillna(frame["pattern_type"])
    frame["avg_severity"] = frame["avg_severity"].astype(float).round(2)
    frame = frame.sort_values("occurrences")

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=frame["occurrences"], y=frame["label"],
            orientation="h",
            marker=dict(color=pal.slot(6), line=dict(width=0)),
            text=[f"{v:,}" for v in frame["occurrences"]],
            textposition="outside",
            textfont=dict(color=pal.ink_secondary, size=11),
            cliponaxis=False, customdata=frame[["avg_severity"]],
            hovertemplate="%{y}<br>%{x:,} occurrences<br>"
            "mean severity %{customdata[0]:.2f} of 3<extra></extra>",
        )
    )
    fig.update_layout(title="Occurrences by failure pattern", bargap=0.35)
    fig.update_xaxes(title="Occurrences")
    fig.update_yaxes(title=None)
    chart(
        fig, pal, data=data.failure_patterns_detail(services),
        height=max(320, 32 * len(frame) + 90),
        download_name="failure_patterns.csv",
        table_label="View per-service detail", show_legend=False)
