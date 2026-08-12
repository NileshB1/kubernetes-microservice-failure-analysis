
# Page: RQ3 - Spark scalability


from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from modules.ui import data
from modules.ui.components import chart, compact_number, duration, empty_state, masthead, section, stat_tiles
from modules.ui.theme import Palette

OPERATIONS = [
    ("groupby_agg_sec", "GroupBy + aggregate", 0), ("window_fn_sec", "Window function", 1),
    ("join_sec", "Join", 2), ("shuffle_sec", "Full shuffle", 3),
]


def render(pal: Palette, services: tuple[str, ...]) -> None:
    del services  # Benchmarks are pipeline-wide; the service filter does not apply.

    masthead(
        "Spark scalability",  "The same workload replayed at increasing data volumes. Perfect scaling means "
        "runtime grows exactly in proportion to the data; anything steeper is "
        "coordination overhead.", eyebrow="Research question 3",
        badge="RQ3",
    )

    if not data.has_data("scalability_metrics"):
        empty_state(
            "No benchmark runs yet",  "This page reads scalability_metrics, written by module 5 "
            "(scalability_analysis). Run it from the Operations page, or start in "
            "demo mode with python run_streamlit.py --local",
        )
        return

    by_size = data.scalability_by_size()
    if by_size.empty:
        empty_state("No benchmark runs yet", "scalability_metrics returned no rows.")
        return

    frame = data.scalability_efficiency(by_size)
    largest = frame.iloc[-1]
    peak_throughput = float(frame["throughput"].max())
    final_efficiency = float(largest["efficiency"])

    # Efficiency above 1 is super-linear: the baseline run was dominated by
    # fixed start-up cost, which the larger runs amortise away. Reporting that
    # as "near-linear" would hide the fact that the baseline is the outlier.
    if final_efficiency > 1.1:
        efficiency_status, efficiency_note = "good", "super-linear - overhead amortised"
    elif final_efficiency >= 0.9:
        efficiency_status, efficiency_note = "good", "near-linear scaling"
    elif final_efficiency >= 0.7:
        efficiency_status, efficiency_note = "warning", "sub-linear, acceptable"
    elif final_efficiency >= 0.5:
        efficiency_status, efficiency_note = "serious", "overhead dominating"
    else:
        efficiency_status, efficiency_note = "critical", "scaling has broken down"

    stat_tiles(
        [
            {
                "label": "Data sizes tested", "value": str(len(frame)),
                "note": f"{int(frame['repetitions'].sum())} benchmark runs",
            },
            {
                "label": "Largest run", "value": compact_number(float(largest["data_size"])),
                "unit": "rows", "note": f"completed in {duration(float(largest['total_sec']))}",
            },
            {
                "label": "Peak throughput",  "value": compact_number(peak_throughput),
                "unit": "rows/s", "note": "best sustained rate"
            },
            {
                "label": "Efficiency at scale", "value": f"{final_efficiency:.2f}",
                "note": efficiency_note,  "status": efficiency_status
            },
        ]
    )

    _runtime_and_throughput(pal, frame)
    _scaling_behaviour(pal, frame)
    _operation_breakdown(pal, frame)


def _runtime_and_throughput(pal: Palette, frame) -> None:
    section(
        "Runtime and throughput", "Both axes are logarithmic: on a log-log plot, perfectly linear scaling is a "
        "straight line, so curvature is the signal."
    )

    left, right = st.columns(2, gap="medium")

    with left:
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=frame["data_size"],  y=frame["total_sec"],
                mode="lines+markers",    name="Measured",
                line=dict(color=pal.slot(0), width=2),
                marker=dict(size=9, color=pal.slot(0), line=dict(width=2, color=pal.surface)),
                hovertemplate="%{x:,.0f} rows<br>%{y:.2f}s<extra></extra>",
            )
        )
        # Reference: what the runtime would be if cost grew exactly with data.
        baseline_size = float(frame["data_size"].iloc[0])
        baseline_time = float(frame["total_sec"].iloc[0])
        fig.add_trace(
            go.Scatter(
                x=frame["data_size"],
                y=frame["data_size"] / baseline_size * baseline_time,
                mode="lines",  name="Perfectly linear",
                line=dict(color=pal.ink_muted, width=1.5),
                hovertemplate="linear reference: %{y:.2f}s<extra></extra>",
            )
        )
        fig.update_layout(title="Total runtime vs data size", hovermode="x unified")
        # dtick=1 on a log axis labels one tick per decade; without it Plotly
        # prints every minor tick and the axis turns into a wall of digits.
        fig.update_xaxes(type="log", title="Rows", dtick=1)
        fig.update_yaxes(type="log", title="Seconds", dtick=1)
        chart(fig, pal, data=frame[["data_size", "total_sec"]].round(3), download_name="runtime_by_size.csv")

    with right:
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=frame["data_size"],  y=frame["throughput"],
                mode="lines+markers",  name="Throughput",
                line=dict(color=pal.slot(2), width=2),
                marker=dict(size=9, color=pal.slot(2), line=dict(width=2, color=pal.surface)),
                hovertemplate="%{x:,.0f} rows<br>%{y:,.0f} rows/s<extra></extra>",
            )
        )
        fig.update_layout(title="Throughput vs data size", hovermode="x unified")
        fig.update_xaxes(type="log", title="Rows")
        fig.update_yaxes(title="Rows per second", rangemode="tozero")
        chart(
            fig, pal,
            data=frame[["data_size", "throughput"]].round(1),
            download_name="throughput_by_size.csv",  show_legend=False
        )


def _scaling_behaviour(pal: Palette, frame) -> None:
    section(
        "Scaling behaviour",
        "Left: how much longer the job takes as the data grows, against the ideal. "
        "Right: the resulting efficiency, where 1.0 is perfect.",
    )

    left, right = st.columns(2, gap="medium")

    with left:
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=frame["size_ratio"],  y=frame["time_ratio"],
                mode="lines+markers", name="Measured time growth",
                line=dict(color=pal.slot(0), width=2),
                marker=dict(size=9, color=pal.slot(0), line=dict(width=2, color=pal.surface)),
                hovertemplate="%{x:.0f}x data<br>%{y:.2f}x time<extra></extra>",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=frame["size_ratio"], y=frame["size_ratio"],
                mode="lines",  name="Ideal (1x time per 1x data)",  line=dict(color=pal.ink_muted, width=1.5),
                hovertemplate="ideal: %{y:.2f}x<extra></extra>",
            )
        )
        fig.update_layout(title="Time growth vs data growth", hovermode="x unified")
        fig.update_xaxes(title="Data size (x baseline)")
        fig.update_yaxes(title="Runtime (x baseline)", rangemode="tozero")
        chart(fig, pal, data=frame[["size_ratio", "time_ratio"]].round(3), download_name="time_growth.csv")

    with right:
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=frame["size_ratio"], y=frame["efficiency"],
                mode="lines+markers",  name="Efficiency",
                line=dict(color=pal.slot(1), width=2),
                marker=dict(size=9, color=pal.slot(1), line=dict(width=2, color=pal.surface)),
                hovertemplate="%{x:.0f}x data<br>efficiency %{y:.3f}<extra></extra>",
            )
        )
        fig.add_hline(
            y=1.0, line=dict(color=pal.ink_muted, width=1.5),
            annotation_text="perfect",  annotation_position="top left",
            annotation_font=dict(color=pal.ink_muted, size=11),
        )
        fig.update_layout(title="Scaling efficiency", hovermode="x unified")
        fig.update_xaxes(title="Data size (x baseline)")
        fig.update_yaxes(title="Efficiency", rangemode="tozero")
        chart(
            fig, pal, data=frame[["size_ratio", "efficiency"]].round(4),
            download_name="scaling_efficiency.csv", show_legend=False
        )


def _operation_breakdown(pal: Palette, frame) -> None:
    section(
        "Where the time goes", "Runtime split by Spark operation. Shuffle-heavy stages are what usually "
        "pull efficiency away from linear."
    )

    fig = go.Figure()
    for column, label, slot in OPERATIONS:
        fig.add_trace(
            go.Bar(
                x=[f"{int(v):,}" for v in frame["data_size"]],
                y=frame[column],  name=label,
                # A surface-coloured line separates stacked segments without
                # drawing a border around each mark.
                marker=dict(color=pal.slot(slot), line=dict(width=2, color=pal.surface)),
                hovertemplate=f"{label}<br>%{{y:.2f}}s<extra></extra>",
            )
        )

    fig.update_layout(title="Runtime by operation", barmode="stack", bargap=0.4, hovermode="x unified")
    fig.update_xaxes(title="Rows", type="category")
    fig.update_yaxes(title="Seconds")
    chart(
        fig, pal,  data=data.scalability_raw().round(3),
        height=380, download_name="scalability_runs.csv",
        table_label="View every benchmark run",
    )
