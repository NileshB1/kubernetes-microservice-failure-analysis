
# Page: RQ1- Cross-service failure propagation


from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from modules.ui import data
from modules.ui.components import chart, compact_number, empty_state, masthead, section, stat_tiles
from modules.ui.theme import Palette, diverging_scale, sequential_scale

# A heatmap past this many services becomes an unreadable pixel grid; the
# ranked table below it stays exact for the full set.
MAX_HEATMAP_SERVICES = 14


def render(pal: Palette, services: tuple[str, ...]) -> None:
    masthead(
        "Cross-service failure propagation",
        "Failures rarely stay where they start. These panels trace how an error in "
        "one service turns into an error in the services that depend on it.",
        eyebrow="Research question 1",
        badge="RQ1",
    )

    if not data.has_data("cross_service_pairs"):
        empty_state(
            "No propagation analysis yet",
            "This page reads cross_service_pairs, propagation_chains and "
            "error_correlations, all written by module 3 (cross_service_analysis). "
            "Run it from the Operations page, or start in demo mode with "
            "python run_streamlit.py --local",
        )
        return

    pairs = data.propagation_pairs()
    chains = data.propagation_chains(limit=12)
    correlations = data.error_correlations(limit=12)

    strong = pairs[pairs["propagation_score"] >= 0.5] if not pairs.empty else pairs
    lag = data.propagation_lag_profile()
    median_lag = float(lag["propagation_lag_sec"].median()) if not lag.empty else None

    stat_tiles(
        [
            {
                "label": "Service pairs",
                "value": compact_number(len(pairs)),
                "note": "with an observed call relationship",
            },
            {
                "label": "Strong propagation",
                "value": compact_number(len(strong)),
                "note": "score >= 0.5 - most callee errors co-occur",
                "status": "serious" if len(strong) else "good",
            },
            {
                "label": "Cascades observed",
                "value": compact_number(int(chains["chain_count"].sum()) if not chains.empty else 0),
                "note": "source -> target failure sequences",
            },
            {
                "label": "Median spread time",
                "value": f"{median_lag:.1f}" if median_lag is not None else "-",
                "unit": "s",
                "note": "failure to downstream failure",
            },
        ]
    )

    _heatmap(pal, pairs)
    _chains_and_correlations(pal, chains, correlations)
    _ranked_pairs(pairs)


def _heatmap(pal: Palette, pairs) -> None:
    section(
        "Propagation score matrix",
        "Each cell is the share of a callee's failures that coincided with a failure "
        "in its caller. Magnitude, so a single hue from light to dark - a rainbow "
        "would invent categories that aren't in the data.",
    )

    if pairs.empty:
        empty_state("No propagation pairs", "cross_service_pairs returned no rows.")
        return

    matrix = pairs.pivot_table(
        index="callee_service",
        columns="caller_service",
        values="propagation_score",
        aggfunc="max",
        fill_value=0.0,
    )

    if matrix.shape[0] > MAX_HEATMAP_SERVICES or matrix.shape[1] > MAX_HEATMAP_SERVICES:
        # Keep the services that actually carry signal rather than
        # shrinking every cell until none of them can be read.
        top_callees = matrix.max(axis=1).nlargest(MAX_HEATMAP_SERVICES).index
        top_callers = matrix.max(axis=0).nlargest(MAX_HEATMAP_SERVICES).index
        matrix = matrix.loc[top_callees, top_callers]
        st.caption(
            f"Showing the {MAX_HEATMAP_SERVICES} highest-signal services on each axis. "
            "The ranked table below covers every pair."
        )

    fig = go.Figure(
        go.Heatmap(
            z=matrix.values,  x=list(matrix.columns),
            y=list(matrix.index),
            colorscale=sequential_scale(pal), zmin=0,  zmax=1,
            xgap=2,   ygap=2, 
            colorbar=dict(
                title=dict(text="Score", font=dict(color=pal.ink_secondary, size=11)),
                tickfont=dict(color=pal.ink_muted, size=10),   outlinewidth=0,
                thickness=10,  len=0.85
            ),
            hovertemplate="%{x} -> %{y}<br>propagation score %{z:.3f}<extra></extra>",
        )
    )
    fig.update_layout(title="Caller -> callee propagation score")
    fig.update_xaxes(title="Caller (upstream)", tickangle=-40, showgrid=False)
    fig.update_yaxes(title="Callee (downstream)", showgrid=False)

    height = max(360, 26 * len(matrix.index) + 150)
    chart(
        fig,  pal,
        data=pairs.round(4),
        height=height, download_name="propagation_pairs.csv",
        table_label="View all pairs",  show_legend=False
    )


def _chains_and_correlations(pal: Palette, chains, correlations) -> None:
    left, right = st.columns(2, gap="medium")

    with left:
        section("Most frequent cascades", "Ordered by how often the sequence was observed.")
        if chains.empty:
            empty_state("No cascades recorded", "propagation_chains is empty.")
        else:
            frame = chains.copy()
            frame["path"] = frame["source_service"] + "  ->  " + frame["target_service"]
            frame["avg_lag_sec"] = frame["avg_lag_sec"].astype(float).round(2)
            frame = frame.sort_values("chain_count")

            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=frame["chain_count"],  y=frame["path"],
                    orientation="h", marker=dict(color=pal.slot(1), line=dict(width=0)),
                    text=[f"{v:,}" for v in frame["chain_count"]],
                    textposition="outside",  textfont=dict(color=pal.ink_secondary, size=11),
                    cliponaxis=False, customdata=frame[["avg_lag_sec"]],
                    hovertemplate="%{y}<br>%{x:,} cascades<br>"
                    "mean lag %{customdata[0]:.2f}s<extra></extra>",
                )
            )
            fig.update_layout(title="Observed failure cascades", bargap=0.3)
            fig.update_xaxes(title="Times observed")
            fig.update_yaxes(title=None)
            chart(
                fig,  pal, data=chains.round(3),
                height=max(320, 30 * len(frame) + 90),
                download_name="propagation_chains.csv", show_legend=False
            )

    with right:
        section("Error-rate correlation",
            "Whether two services fail at the same time. Polarity, so two opposed "
            "hues around a neutral midpoint - grey means no relationship.",
        )
        if correlations.empty:
            empty_state("No correlations found",
                "No service pair cleared the correlation threshold with enough "
                "overlapping minutes. Tune min_correlation_threshold in config.yaml.",
            )
        else:
            frame = correlations.copy()
            frame["pair"] = frame["service_a"] + "  <->  " + frame["service_b"]
            frame["error_correlation"] = frame["error_correlation"].astype(float)
            frame = frame.sort_values("error_correlation")

            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=frame["error_correlation"],
                    y=frame["pair"],
                    orientation="h",
                    marker=dict(
                        color=frame["error_correlation"],  colorscale=diverging_scale(pal),
                        cmin=-1,  cmax=1,
                        line=dict(width=0),
                        colorbar=dict(
                            title=dict(text="r", font=dict(color=pal.ink_secondary, size=11)),
                            tickfont=dict(color=pal.ink_muted, size=10),   outlinewidth=0,
                            thickness=10,  len=0.85
                        ),
                    ),
                    customdata=frame[["sample_size"]],
                    hovertemplate="%{y}<br>r = %{x:.3f}<br>"
                    "%{customdata[0]:,} shared minutes<extra></extra>",
                )
            )
            fig.update_layout(title="Pearson correlation of error rates", bargap=0.3)
            fig.update_xaxes(
                title="Correlation (r)", range=[-1.05, 1.05],
                zeroline=True,
                zerolinecolor=pal.axis,  zerolinewidth=1
            )
            fig.update_yaxes(title=None)
            chart(
                fig,  pal,  data=correlations.round(4),
                height=max(320, 30 * len(frame) + 90),
                download_name="error_correlations.csv", show_legend=False)


def _ranked_pairs(pairs) -> None:
    section("Every propagation pair", "Sorted by propagation score. Exact values, exportable.")
    if pairs.empty:
        return

    frame = pairs.copy()
    frame["propagation_score"] = frame["propagation_score"].astype(float).round(4)
    frame["avg_callee_latency_ms"] = frame["avg_callee_latency_ms"].astype(float).round(1)
    frame = frame.rename(
        columns={
            "caller_service": "Caller", "callee_service": "Callee",
            "call_count": "Calls", "caller_error_count": "Caller errors",
            "callee_error_count": "Callee errors",
            "co_failure_count": "Co-failures",  "avg_callee_latency_ms": "Callee latency (ms)",
            "propagation_score": "Score"  }
    )

    st.dataframe(
        frame,  use_container_width=True,
        hide_index=True,
        column_config={
            "Score": st.column_config.ProgressColumn(
                "Score", min_value=0.0,  max_value=1.0,
                format="%.3f",
                help="Share of callee failures that coincided with a caller failure",
            ),
        },
    )
    st.download_button(
        "Download all pairs (CSV)", frame.to_csv(index=False).encode("utf-8"),
        file_name="cross_service_pairs.csv",   mime="text/csv",
    )
