
# UI Components - the pieces every page is assembled from


from __future__ import annotations

import html
from collections.abc import Iterable
from typing import Literal

import pandas as pd
import streamlit as st

from modules.ui.theme import Palette, apply_chart_theme

StatusLevel = Literal["good", "warning", "serious", "critical", "neutral"]

# Icon + label pairing: a status is never communicated by colour alone
STATUS_ICONS: dict[StatusLevel, str] = {
    "good": "+", "warning": "!", "serious": "!!", "critical": "X",
    "neutral": "-" }


# Formatting

def compact_number(value: float | int | None, *, decimals: int = 1) -> str:
    """Render a number the way a KPI tile should: short, but never wrong."""
    if value is None or pd.isna(value):
        return "-"
    value = float(value)
    for threshold, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(value) >= threshold:
            return f"{value / threshold:.{decimals}f}{suffix}"
    if value == int(value):
        return f"{int(value):,}"
    return f"{value:,.{decimals}f}"


def duration(seconds: float | None) -> str:
    """Render a duration in the largest unit that keeps it readable."""
    if seconds is None or pd.isna(seconds):
        return "-"
    seconds = float(seconds)
    if seconds < 1:
        return f"{seconds * 1000:.0f} ms"
    if seconds < 90:
        return f"{seconds:.1f} s"
    if seconds < 5400:
        return f"{seconds / 60:.1f} min"
    return f"{seconds / 3600:.1f} h"


def bytes_human(num: float | int | None) -> str:
    """Render a byte count in binary units."""
    if num is None or pd.isna(num):
        return "-"
    num = float(num)
    for unit in ("B", "KB", "MB", "GB"):
        if abs(num) < 1024:
            return f"{num:.1f} {unit}"
        num /= 1024
    return f"{num:.1f} TB"


# Page furniture

def masthead(title: str, subtitle: str, *, eyebrow: str | None = None, badge: str | None = None) -> None:
    """The heading block every page opens with."""
    parts = ['<div class="dash-masthead"><div>']
    if eyebrow:
        parts.append(f'<p class="dash-eyebrow">{html.escape(eyebrow)}</p>')
    badge_html = f'<span class="rq-badge">{html.escape(badge)}</span>' if badge else ""
    parts.append(f'<h1 class="dash-title">{badge_html}{html.escape(title)}</h1>')
    parts.append(f'<p class="dash-subtitle">{html.escape(subtitle)}</p>')
    parts.append("</div></div>")
    st.markdown("".join(parts), unsafe_allow_html=True)


def section(title: str, help_text: str | None = None) -> None:
    """A labelled band that groups related panels."""
    body = f'<div class="section-head"><p class="section-title">{html.escape(title)}</p>'
    if help_text:
        body += f'<p class="section-help">{html.escape(help_text)}</p>'
    body += "</div>"
    st.markdown(body, unsafe_allow_html=True)


def stat_tiles(tiles: Iterable[dict]) -> None:
    """
    Render a row of KPI tiles
    """
    cells = []
    for tile in tiles:
        unit = tile.get("unit")
        unit_html = f'<span class="unit">{html.escape(unit)}</span>' if unit else ""
        note = tile.get("note")
        status: StatusLevel | None = tile.get("status")

        if note and status:
            icon = STATUS_ICONS[status]
            note_html = (
                f'<div class="stat-note"><span class="pill pill-{status}">'
                f"{icon} {html.escape(note)}</span></div>"
            )
        elif note:
            note_html = f'<div class="stat-note">{html.escape(note)}</div>'
        else:
            note_html = '<div class="stat-note">&nbsp;</div>'

        cells.append(
            '<div class="stat-tile">'
            f'<div class="stat-label">{html.escape(str(tile["label"]))}</div>'
            f'<div class="stat-value">{html.escape(str(tile["value"]))}{unit_html}</div>'
            f"{note_html}"
            "</div>"
        )
    st.markdown(f'<div class="stat-grid">{"".join(cells)}</div>', unsafe_allow_html=True)


def status_pill(level: StatusLevel, label: str) -> str:
    """Return the HTML for an inline status pill (icon + label + colour)."""
    return f'<span class="pill pill-{level}">{STATUS_ICONS[level]} ' f"{html.escape(label)}</span>"


def empty_state(title: str, body: str):
    """
    Shown wherever a panel has no data
    """
    st.markdown(
        f'<div class="empty-state"><div class="empty-title">{html.escape(title)}</div>'
        f'<div class="empty-body">{html.escape(body)}</div></div>',
        unsafe_allow_html=True,
    )


# Charts

def chart(
    fig, pal: Palette,
    *,  data: pd.DataFrame, height: int = 340,
    table_label: str = "View data", download_name: str,
    show_legend: bool,
):
    """
    Render a themed chart with its table-view twin
    """
    apply_chart_theme(fig, pal, height=height, show_legend=show_legend)
    st.plotly_chart(
        fig,
        use_container_width=True,
        config={"displayModeBar": False, "scrollZoom": False })

    if data is None or data.empty:
        return

    with st.expander(table_label):
        st.dataframe(data, use_container_width=True, hide_index=True)
        if download_name:
            st.download_button(
                "Download CSV",   data.to_csv(index=False).encode("utf-8"),
                file_name=download_name,
                mime="text/csv", use_container_width=True,
                key=f"dl-{download_name}",
            )
