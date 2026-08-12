# ============================================================
# Design System - Tokens, CSS, and Plotly Templates
# ============================================================
# One palette, defined once, consumed by both the CSS layer and every
# chart. Charts never hard-code a hex value; they ask for a role
# ("series slot 3", "status: critical") and get the step that was
# validated for the active surface.
#
# The categorical order is a colour-vision-deficiency safety mechanism,
# not a decoration: the eight hues were validated in this exact order so
# that adjacent slots stay distinguishable under protanopia, deuteranopia
# and tritanopia, on both the light and the dark surface. Reordering them
# or generating a ninth silently breaks that guarantee.
#
# Light-mode slots 3, 4 and 5 sit below 3:1 against the light surface, so
# every chart that can use them also ships a table view - that is the
# documented relief for the contrast warning, and it is why each chart
# card carries a "Data" expander.
# ============================================================

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import streamlit as st

ThemeName = Literal["light", "dark"]


# ------------------------------------------------------------
# Palette
# ------------------------------------------------------------
@dataclass(frozen=True)
class Palette:
    """Every colour the dashboard is allowed to draw, for one surface."""

    name: ThemeName

    # Surfaces & ink
    surface: str
    page: str
    raised: str
    ink_primary: str
    ink_secondary: str
    ink_muted: str
    grid: str
    axis: str
    border: str

    # Categorical series - fixed order, never cycled
    series: tuple[str, ...]

    # Sequential ramp (single hue, light -> dark)
    sequential: tuple[str, ...]

    # Diverging poles + neutral midpoint
    diverging_low: str
    diverging_mid: str
    diverging_high: str

    # Status - reserved, never reused as a series colour
    good: str
    warning: str
    serious: str
    critical: str

    accent: str

    def slot(self, index: int) -> str:
        """
        Return categorical slot `index` (0-based).

        Raises past the eighth slot rather than cycling: a ninth generated
        hue is indistinguishable from an existing one under CVD. Callers
        with more than eight categories must fold the tail into "Other"
        or facet into small multiples.
        """
        if not 0 <= index < len(self.series):
            raise IndexError(
                f"Categorical slot {index} is out of range; the palette has "
                f"{len(self.series)} validated slots. Fold the tail into "
                f"'Other' or use small multiples instead of a 9th hue."
            )
        return self.series[index]


LIGHT = Palette(
    name="light",
    surface="#fcfcfb",
    page="#f9f9f7",
    raised="#ffffff",
    ink_primary="#0b0b0b",
    ink_secondary="#52514e",
    ink_muted="#898781",
    grid="#e1e0d9",
    axis="#c3c2b7",
    border="rgba(11,11,11,0.10)",
    series=("#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"),
    sequential=(
        "#cde2fb",
        "#b7d3f6",
        "#9ec5f4",
        "#86b6ef",
        "#6da7ec",
        "#5598e7",
        "#3987e5",
        "#2a78d6",
        "#256abf",
        "#1c5cab",
        "#184f95",
        "#104281",
        "#0d366b",
    ),
    diverging_low="#2a78d6",
    diverging_mid="#f0efec",
    diverging_high="#e34948",
    good="#0ca30c",
    warning="#fab219",
    serious="#ec835a",
    critical="#d03b3b",
    accent="#2a78d6",
)

DARK = Palette(
    name="dark",
    surface="#1a1a19",
    page="#0d0d0d",
    raised="#232322",
    ink_primary="#ffffff",
    ink_secondary="#c3c2b7",
    ink_muted="#898781",
    grid="#2c2c2a",
    axis="#383835",
    border="rgba(255,255,255,0.10)",
    series=("#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"),
    sequential=(
        "#cde2fb",
        "#b7d3f6",
        "#9ec5f4",
        "#86b6ef",
        "#6da7ec",
        "#5598e7",
        "#3987e5",
        "#2a78d6",
        "#256abf",
        "#1c5cab",
        "#184f95",
        "#104281",
        "#0d366b",
    ),
    diverging_low="#3987e5",
    diverging_mid="#383835",
    diverging_high="#e66767",
    good="#0ca30c",
    warning="#fab219",
    serious="#ec835a",
    critical="#d03b3b",
    accent="#3987e5",
)

FONT_STACK = 'system-ui, -apple-system, "Segoe UI", sans-serif'


def active_theme() -> ThemeName:
    """
    Return the theme the browser is actually rendering.

    Streamlit reports the viewer's resolved theme, so the palette follows
    the user's own light/dark choice instead of guessing. If the runtime
    cannot tell us (older Streamlit, bare script run), dark is the
    documented default in .streamlit/config.toml.
    """
    try:
        reported = st.context.theme.type
    except Exception:  # noqa: BLE001 - theme detection must never break a page
        return "dark"
    return "light" if reported == "light" else "dark"


def palette() -> Palette:
    """Return the palette validated for the currently active surface."""
    return LIGHT if active_theme() == "light" else DARK


def sequential_scale(pal: Palette) -> list[list]:
    """Plotly-format single-hue ramp for continuous magnitude."""
    steps = pal.sequential
    return [[i / (len(steps) - 1), colour] for i, colour in enumerate(steps)]


def diverging_scale(pal: Palette) -> list[list]:
    """
    Plotly-format diverging ramp: two opposed hues around a neutral middle.

    The midpoint is grey on purpose - a hue there would read as a third
    category rather than as "no signal".
    """
    return [
        [0.0, pal.diverging_low],
        [0.5, pal.diverging_mid],
        [1.0, pal.diverging_high],
    ]


# ------------------------------------------------------------
# CSS layer
# ------------------------------------------------------------
def _css(pal: Palette) -> str:
    """Application chrome styled from the same tokens the charts use."""
    return f"""
<style>
  :root {{
    --surface: {pal.surface};
    --page: {pal.page};
    --raised: {pal.raised};
    --ink-primary: {pal.ink_primary};
    --ink-secondary: {pal.ink_secondary};
    --ink-muted: {pal.ink_muted};
    --border: {pal.border};
    --grid: {pal.grid};
    --accent: {pal.accent};
    --good: {pal.good};
    --warning: {pal.warning};
    --serious: {pal.serious};
    --critical: {pal.critical};
    --radius: 12px;
    --space-1: 0.25rem;
    --space-2: 0.5rem;
    --space-3: 0.75rem;
    --space-4: 1rem;
    --space-6: 1.5rem;
  }}

  html, body, [class*="st-"], button, input, select, textarea {{
    font-family: {FONT_STACK};
  }}

  .block-container {{
    padding-top: 2.2rem;
    padding-bottom: 3rem;
    max-width: 1480px;
  }}

  /* ---- Page masthead ---- */
  .dash-masthead {{
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: var(--space-6);
    flex-wrap: wrap;
    margin-bottom: var(--space-6);
  }}
  .dash-title {{
    font-size: 1.65rem;
    font-weight: 680;
    letter-spacing: -0.02em;
    color: var(--ink-primary);
    margin: 0 0 var(--space-2) 0;
    line-height: 1.2;
  }}
  .dash-subtitle {{
    font-size: 0.95rem;
    color: var(--ink-secondary);
    margin: 0;
    max-width: 68ch;
    line-height: 1.5;
  }}
  .dash-eyebrow {{
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--ink-muted);
    margin: 0 0 var(--space-2) 0;
  }}

  /* ---- Stat tiles ---- */
  .stat-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
    gap: var(--space-4);
    margin-bottom: var(--space-2);
  }}
  .stat-tile {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: var(--space-4) var(--space-4) var(--space-3);
    display: flex;
    flex-direction: column;
    gap: var(--space-2);
    min-width: 0;
  }}
  .stat-label {{
    font-size: 0.75rem;
    font-weight: 600;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: var(--ink-muted);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }}
  .stat-value {{
    /* Proportional figures: tabular-nums makes a large standalone
       number look loosely spaced. */
    font-size: 2rem;
    font-weight: 640;
    line-height: 1.05;
    letter-spacing: -0.025em;
    color: var(--ink-primary);
  }}
  .stat-value .unit {{
    font-size: 0.95rem;
    font-weight: 500;
    color: var(--ink-secondary);
    margin-left: 0.15em;
  }}
  .stat-note {{
    font-size: 0.78rem;
    color: var(--ink-secondary);
    line-height: 1.35;
  }}

  /* ---- Status pills: icon + label, never colour alone ---- */
  .pill {{
    display: inline-flex;
    align-items: center;
    gap: 0.4em;
    padding: 0.15rem 0.6rem;
    border-radius: 999px;
    font-size: 0.75rem;
    font-weight: 600;
    border: 1px solid var(--border);
    white-space: nowrap;
  }}
  .pill-good     {{ color: var(--good); }}
  .pill-warning  {{ color: var(--warning); }}
  .pill-serious  {{ color: var(--serious); }}
  .pill-critical {{ color: var(--critical); }}
  .pill-neutral  {{ color: var(--ink-secondary); }}

  /* ---- Section headers ---- */
  .section-head {{
    margin: var(--space-6) 0 var(--space-3) 0;
  }}
  .section-title {{
    font-size: 1.05rem;
    font-weight: 640;
    letter-spacing: -0.01em;
    color: var(--ink-primary);
    margin: 0;
  }}
  .section-help {{
    font-size: 0.84rem;
    color: var(--ink-secondary);
    margin: var(--space-1) 0 0 0;
    max-width: 78ch;
    line-height: 1.5;
  }}

  /* ---- Research-question badges ---- */
  .rq-badge {{
    display: inline-block;
    padding: 0.1rem 0.55rem;
    border-radius: 6px;
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 0.04em;
    border: 1px solid var(--border);
    color: var(--ink-secondary);
    margin-right: 0.5rem;
    vertical-align: middle;
  }}

  /* ---- Empty states ---- */
  .empty-state {{
    border: 1px dashed var(--border);
    border-radius: var(--radius);
    padding: var(--space-6);
    text-align: center;
    color: var(--ink-secondary);
    background: var(--surface);
  }}
  .empty-state .empty-title {{
    font-weight: 620;
    color: var(--ink-primary);
    margin-bottom: var(--space-2);
    font-size: 0.95rem;
  }}
  .empty-state .empty-body {{
    font-size: 0.85rem;
    line-height: 1.55;
    max-width: 56ch;
    margin: 0 auto;
  }}

  /* ---- Streamlit chrome ---- */
  [data-testid="stSidebar"] {{
    border-right: 1px solid var(--border);
  }}
  [data-testid="stSidebar"] .block-container {{ padding-top: 1.5rem; }}

  div[data-testid="stDataFrame"] {{
    border: 1px solid var(--border);
    border-radius: 10px;
    overflow: hidden;
  }}

  .stTabs [data-baseweb="tab-list"] {{ gap: 2px; }}

  /* Hold the previous render during a refetch instead of flashing a
     skeleton, which would jump the layout. */
  [data-testid="stElementContainer"]:has(.stSpinner) {{ opacity: 0.72; }}

  hr {{ border-color: var(--border); }}

  /* Numbers that line up vertically get equal-width digits. */
  div[data-testid="stDataFrame"] td {{ font-variant-numeric: tabular-nums; }}
</style>
"""


def inject_theme() -> Palette:
    """
    Push the CSS layer for the active theme and return its palette.

    Call once per page render, before anything draws.
    """
    pal = palette()
    st.markdown(_css(pal), unsafe_allow_html=True)
    return pal


# ------------------------------------------------------------
# Plotly template
# ------------------------------------------------------------
def apply_chart_theme(fig, pal: Palette, *, height: int = 340, show_legend: bool | None = None):
    """
    Apply the shared chart chrome to a Plotly figure.

    Recessive hairline grid, no chart-surface fill fighting the card, ink
    from the text tokens (never a series colour), and a unified hover
    layer. Height includes room for the axis band so the card never grows
    its own scrollbar.
    """
    # The title sits at the very top of the paper and the legend on its own
    # band just below it. Both live in the top margin, so that margin has to
    # be tall enough for two rows whenever a legend is drawn - otherwise the
    # legend entries print straight through the title.
    has_legend = show_legend is not False
    top_margin = 62 if has_legend else 34

    fig.update_layout(
        template=None,
        font=dict(family=FONT_STACK, size=12, color=pal.ink_secondary),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=8, r=8, t=top_margin, b=8),
        height=height + (top_margin - 34),
        hoverlabel=dict(
            bgcolor=pal.raised,
            bordercolor=pal.border,
            font=dict(family=FONT_STACK, size=12, color=pal.ink_primary),
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.01,
            xanchor="left",
            x=0,
            font=dict(color=pal.ink_secondary, size=11),
            bgcolor="rgba(0,0,0,0)",
        ),
        colorway=list(pal.series),
    )

    # Style the title only when there is one. A title dict without `text`
    # serialises with the key absent, and Plotly.js then draws the literal
    # string "undefined" - which is what a small-multiples figure, whose
    # panels carry their own names, would otherwise show.
    if not fig.layout.title.text:
        # An empty title object (what update_layout(title=None) leaves
        # behind) renders as the literal string "undefined". An empty
        # string renders as nothing.
        fig.update_layout(title_text="")
    else:
        fig.update_layout(
            title=dict(
                font=dict(color=pal.ink_primary, size=13),
                x=0,
                xanchor="left",
                y=1.0,
                yanchor="top",
                pad=dict(t=0, b=0),
            )
        )
    if show_legend is not None:
        fig.update_layout(showlegend=show_legend)

    axis_style = dict(
        gridcolor=pal.grid,
        griddash="solid",
        zeroline=False,
        linecolor=pal.axis,
        tickfont=dict(color=pal.ink_muted, size=11),
        title_font=dict(color=pal.ink_secondary, size=11),
        automargin=True,
    )
    fig.update_xaxes(**axis_style, showgrid=False)
    fig.update_yaxes(**axis_style, showgrid=True)
    return fig
