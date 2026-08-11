# ============================================================
# Streamlit Dashboard — End-to-End Interactive UI
# ============================================================
# Run: streamlit run modules/dashboard.py --server.port 8501
#
# Features:
#   - Overview tab with architecture & key metrics
#   - RQ1: Cross-service propagation heatmap & chains
#   - RQ2: Anomaly detection time series & patterns
#   - RQ3: Scalability curves & efficiency analysis
#   - Sidebar: pipeline controls, config, data refresh
# ============================================================

import os
import sys
import time
import logging
import subprocess
import threading
from queue import Queue

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
from dotenv import load_dotenv

from modules.report_generator import generate_report_bytes
from modules.db_adapter import (
    run_query,
    get_table_names,
    get_table_row_count,
    check_connection,
    DB_MODE,
)

load_dotenv()

# ============================================================
# Page Config
# ============================================================
st.set_page_config(
    page_title="K8s Microservice Failure Analysis",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# Theme System — Dark / Light Mode
# ============================================================
# Streamlit doesn't natively support runtime theme switching, so we use
# CSS custom properties plus a small JS snippet that reads our toggle
# state and sets a data-theme attribute on <body>.

# Initialise theme in session state (default: dark)
if "theme" not in st.session_state:
    st.session_state.theme = "dark"

THEME_CSS = """
<style>
    /* ============================================================
       CSS Custom Properties — Light & Dark Palettes
       ============================================================ */
    :root,
    [data-theme="dark"] {
        --bg-primary: #0f172a;
        --bg-secondary: #1e293b;
        --bg-card: #1e293b;
        --bg-card-hover: #334155;
        --bg-input: #1e293b;
        --border-color: #334155;
        --border-light: #1e293b;
        --text-primary: #f1f5f9;
        --text-secondary: #94a3b8;
        --text-muted: #64748b;
        --accent-primary: #818cf8;
        --accent-secondary: #a78bfa;
        --accent-gradient: linear-gradient(135deg, #818cf8 0%, #a78bfa 100%);
        --success: #34d399;
        --warning: #fbbf24;
        --danger: #f87171;
        --info: #60a5fa;
        --metric-bg: linear-gradient(135deg, #818cf815 0%, #a78bfa15 100%);
        --shadow: 0 1px 3px rgba(0,0,0,0.4);
        --scrollbar-thumb: #475569;
        --scrollbar-track: #1e293b;
        --log-bg: #111827;
        --log-border: #374151;
        --log-text: #d1d5db;
        --log-timestamp: #6b7280;
        --badge-rq1-bg: #422006;
        --badge-rq1-text: #fcd34d;
        --badge-rq2-bg: #1e3a5f;
        --badge-rq2-text: #93c5fd;
        --badge-rq3-bg: #064e3b;
        --badge-rq3-text: #6ee7b7;
    }

    [data-theme="light"] {
        --bg-primary: #ffffff;
        --bg-secondary: #f8fafc;
        --bg-card: #ffffff;
        --bg-card-hover: #f1f5f9;
        --bg-input: #ffffff;
        --border-color: #e2e8f0;
        --border-light: #f1f5f9;
        --text-primary: #0f172a;
        --text-secondary: #475569;
        --text-muted: #94a3b8;
        --accent-primary: #667eea;
        --accent-secondary: #764ba2;
        --accent-gradient: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        --success: #10b981;
        --warning: #f59e0b;
        --danger: #ef4444;
        --info: #3b82f6;
        --metric-bg: linear-gradient(135deg, #667eea10 0%, #764ba210 100%);
        --shadow: 0 1px 3px rgba(0,0,0,0.08);
        --scrollbar-thumb: #cbd5e1;
        --scrollbar-track: #f1f5f9;
        --log-bg: #f8fafc;
        --log-border: #e2e8f0;
        --log-text: #1e293b;
        --log-timestamp: #94a3b8;
        --badge-rq1-bg: #fef3c7;
        --badge-rq1-text: #92400e;
        --badge-rq2-bg: #dbeafe;
        --badge-rq2-text: #1e40af;
        --badge-rq3-bg: #d1fae5;
        --badge-rq3-text: #065f46;
    }

    /* ============================================================
       Global Styles
       ============================================================ */
    .stApp {
        background-color: var(--bg-primary);
    }

    /* Scrollbar */
    ::-webkit-scrollbar { width: 8px; }
    ::-webkit-scrollbar-track { background: var(--scrollbar-track); }
    ::-webkit-scrollbar-thumb {
        background: var(--scrollbar-thumb);
        border-radius: 4px;
    }

    /* Streamlit component overrides */
    .stMetric {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 12px !important;
        padding: 1rem !important;
        box-shadow: var(--shadow);
    }
    .stMetric label {
        color: var(--text-secondary) !important;
    }
    .stMetric [data-testid="stMetricValue"] {
        color: var(--accent-primary) !important;
    }

    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        background: var(--bg-secondary);
        border-radius: 12px;
        padding: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        color: var(--text-secondary) !important;
        border-radius: 8px !important;
        padding: 8px 16px !important;
    }
    .stTabs [aria-selected="true"] {
        background: var(--accent-primary) !important;
        color: #ffffff !important;
    }

    .stButton > button {
        border-radius: 8px !important;
        font-weight: 600 !important;
        transition: all 0.2s ease;
    }
    .stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: var(--shadow);
    }

    .stSelectbox > div > div {
        background: var(--bg-input) !important;
        border-color: var(--border-color) !important;
    }

    .stTextInput > div > div > input {
        background: var(--bg-input) !important;
        border-color: var(--border-color) !important;
        color: var(--text-primary) !important;
    }

    .stDataFrame {
        border: 1px solid var(--border-color) !important;
        border-radius: 8px !important;
    }

    .stProgress > div > div {
        background: var(--accent-primary) !important;
    }

    .stAlert {
        border-radius: 8px !important;
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: var(--bg-secondary);
        border-right: 1px solid var(--border-color);
    }
    [data-testid="stSidebar"] .stMarkdown {
        color: var(--text-primary);
    }

    /* ============================================================
       Custom Component Classes
       ============================================================ */
    .main-header {
        font-size: 2.5rem;
        font-weight: 800;
        background: var(--accent-gradient);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.1rem;
        color: var(--text-secondary);
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background: var(--metric-bg);
        border: 1px solid var(--border-color);
        border-radius: 12px;
        padding: 1.25rem;
        text-align: center;
        box-shadow: var(--shadow);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    }
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: var(--accent-primary);
    }
    .metric-label {
        font-size: 0.85rem;
        color: var(--text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .rq-badge {
        display: inline-block;
        padding: 0.15rem 0.6rem;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 600;
    }
    .rq1 { background: var(--badge-rq1-bg); color: var(--badge-rq1-text); }
    .rq2 { background: var(--badge-rq2-bg); color: var(--badge-rq2-text); }
    .rq3 { background: var(--badge-rq3-bg); color: var(--badge-rq3-text); }

    /* Theme toggle switch */
    .theme-toggle-wrapper {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 10px;
        padding: 6px 0;
    }
    .theme-toggle-label {
        font-size: 18px;
        line-height: 1;
        transition: opacity 0.3s ease;
    }
    .theme-toggle-switch {
        position: relative;
        display: inline-block;
        width: 52px;
        height: 28px;
        cursor: pointer;
    }
    .theme-toggle-switch input {
        opacity: 0;
        width: 0;
        height: 0;
    }
    .theme-toggle-slider {
        position: absolute;
        top: 0; left: 0; right: 0; bottom: 0;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 28px;
        transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: inset 0 1px 3px rgba(0,0,0,0.2);
    }
    .theme-toggle-slider:before {
        content: "";
        position: absolute;
        height: 22px;
        width: 22px;
        left: 3px;
        bottom: 3px;
        background: #ffffff;
        border-radius: 50%;
        transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: 0 2px 6px rgba(0,0,0,0.25);
        z-index: 1;
    }
    .theme-toggle-switch input:checked + .theme-toggle-slider {
        background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
        box-shadow: inset 0 1px 3px rgba(0,0,0,0.4);
    }
    .theme-toggle-switch input:checked + .theme-toggle-slider:before {
        transform: translateX(24px);
        box-shadow: 0 2px 8px rgba(0,0,0,0.35);
    }
    .theme-toggle-slider .toggle-icon {
        position: absolute;
        top: 50%;
        transform: translateY(-50%);
        font-size: 13px;
        line-height: 1;
        transition: opacity 0.3s ease;
        z-index: 0;
    }
    .toggle-icon-sun {
        left: 6px;
        opacity: 0.8;
    }
    .toggle-icon-moon {
        right: 6px;
        opacity: 0.6;
    }
    input:checked ~ .theme-toggle-slider .toggle-icon-sun { opacity: 0.3; }
    input:checked ~ .theme-toggle-slider .toggle-icon-moon { opacity: 0.9; }

    /* Card component */
    .card {
        background: var(--bg-card);
        border: 1px solid var(--border-color);
        border-radius: 12px;
        padding: 1.25rem;
        box-shadow: var(--shadow);
    }

    /* Section divider */
    .section-divider {
        border: none;
        border-top: 1px solid var(--border-color);
        margin: 1.5rem 0;
    }
</style>
"""

# JavaScript: apply data-theme from sessionStorage on page load
THEME_JS = """
<script>
(function() {
    var theme = sessionStorage.getItem('dashboard-theme') || 'dark';
    document.body.setAttribute('data-theme', theme);
    var observer = new MutationObserver(function() {
        document.body.setAttribute('data-theme',
            sessionStorage.getItem('dashboard-theme') || 'dark');
    });
    observer.observe(document.body, { attributes: true });

    // Keyboard shortcut: Ctrl+Shift+T toggles dark/light mode
    document.addEventListener('keydown', function(e) {
        if (e.ctrlKey && e.shiftKey && e.key === 'T') {
            e.preventDefault();
            // Find the Streamlit toggle checkbox by its label text
            var labels = document.querySelectorAll('[data-testid="stWidgetLabel"]');
            for (var i = 0; i < labels.length; i++) {
                if (labels[i].textContent.indexOf('Dark mode') !== -1) {
                    var cb = labels[i].querySelector('input[type="checkbox"]');
                    if (cb) {
                        cb.click();
                        // Also update theme immediately for instant visual feedback
                        var newTheme = cb.checked ? 'dark' : 'light';
                        sessionStorage.setItem('dashboard-theme', newTheme);
                        document.body.setAttribute('data-theme', newTheme);
                    }
                    break;
                }
            }
        }
    });
})();
</script>
"""

# Inject CSS and JS
st.markdown(THEME_CSS + THEME_JS, unsafe_allow_html=True)

# ============================================================
# Database Connection (via db_adapter — PG or SQLite)
# ============================================================
@st.cache_data(ttl=30)
def query_db(query: str) -> pd.DataFrame:
    """Run a SQL query via the adapter and return a DataFrame."""
    try:
        return run_query(query)
    except Exception as e:
        st.warning(f"Query failed: {e}")
        return pd.DataFrame()


# ============================================================
# Pipeline Runner (subprocess)
# ============================================================

PIPELINE_MODULES = {
    "Full Pipeline (all 6 modules)": os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "scripts", "run_pipeline.sh",
    ),
    "1. Ingestion": "modules.ingestion",
    "2. Preprocessing": "modules.preprocessing",
    "3. Cross-Service Analysis (RQ1)": "modules.cross_service_analysis",
    "4. Failure Detection (RQ2)": "modules.failure_detection",
    "5. Scalability Analysis (RQ3)": "modules.scalability_analysis",
    "6. Visualization": "modules.visualization",
}


def _enqueue_output(pipe, queue):
    """Read subprocess output line-by-line into a queue (runs in thread)."""
    for line in iter(pipe.readline, ""):
        queue.put(line)
    pipe.close()


def run_pipeline_module(module_key: str):
    """
    Run a pipeline module via subprocess and stream output.

    Uses st.status for a live-expanding output block.
    Output is streamed line-by-line via a queue + thread.
    """
    target = PIPELINE_MODULES[module_key]

    if target.endswith(".sh"):
        cmd = ["bash", target]
    else:
        cmd = [sys.executable, "-m", target]

    with st.status(f"Running: {module_key}", expanded=True) as status:
        output_container = st.empty()
        all_output = []

        try:
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"

            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=env,
                cwd=os.getenv("APP_ROOT", os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            )

            q = Queue()
            t = threading.Thread(target=_enqueue_output, args=(proc.stdout, q), daemon=True)
            t.start()

            # Stream output while process runs
            placeholder = st.empty()
            while proc.poll() is None or not q.empty():
                try:
                    line = q.get(timeout=0.1)
                    all_output.append(line)
                    # Show last 30 lines to avoid flooding
                    visible = "".join(all_output[-30:])
                    placeholder.code(visible, language="bash")
                except Exception:
                    pass

            t.join(timeout=2)
            proc.wait()

            # Final output flush
            final_output = "".join(all_output)
            placeholder.code(final_output, language="bash")

            if proc.returncode == 0:
                status.update(label=f"✓ {module_key} — completed", state="complete")
            else:
                status.update(
                    label=f"✗ {module_key} — exit code {proc.returncode}",
                    state="error",
                )

            # Clear caches so new data appears on next refresh
            st.cache_data.clear()
            st.toast(f"{module_key} finished (code {proc.returncode})", icon="✅" if proc.returncode == 0 else "❌")

        except FileNotFoundError:
            status.update(label=f"✗ Module not found: {target}", state="error")
            st.error(f"Could not find module: {target}")
        except Exception as e:
            status.update(label=f"✗ Error: {e}", state="error")
            st.error(str(e))


# ============================================================
# Sidebar — Controls
# ============================================================
def render_sidebar():
    """Render the sidebar with pipeline controls and info."""
    with st.sidebar:
        st.image("https://streamlit.io/images/brand/streamlit-mark-color.svg", width=60)

        # ---- Theme Toggle ----
        st.markdown("### 🎨 Theme")
        current_theme = st.session_state.get("theme", "dark")
        is_dark = current_theme == "dark"

        new_is_dark = st.toggle(
            "Dark mode", value=is_dark, key="theme_toggle",
            help="Toggle between dark and light theme (Ctrl+Shift+T)",
        )
        if new_is_dark != is_dark:
            new_theme = "dark" if new_is_dark else "light"
            st.session_state.theme = new_theme
            # Apply theme immediately via JS (persists in sessionStorage)
            st.markdown(
                f"<script>sessionStorage.setItem('dashboard-theme','{new_theme}');"
                f"document.body.setAttribute('data-theme','{new_theme}');</script>",
                unsafe_allow_html=True,
            )
            st.rerun()

        st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)
        st.markdown("## 🎛️ Controls")

        # Refresh / cache
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Refresh", use_container_width=True):
                st.cache_data.clear()
                st.rerun()
        with col2:
            if st.button("🧹 Clear", use_container_width=True):
                st.cache_resource.clear()
                st.cache_data.clear()
                st.rerun()

        # Pipeline runner
        st.markdown("---")
        st.markdown("### 🚀 Run Pipeline")

        module_choice = st.selectbox(
            "Select module",
            list(PIPELINE_MODULES.keys()),
            index=0,
            label_visibility="collapsed",
        )

        run_col1, run_col2 = st.columns([3, 1])
        with run_col1:
            if st.button("▶️ Execute", use_container_width=True, type="primary"):
                run_pipeline_module(module_choice)
                st.rerun()

        # Placeholder generator
        st.markdown("---")
        st.markdown("### 🎨 Placeholders")
        if st.button("🖼️ Regenerate Placeholders", use_container_width=True):
            with st.status("Generating placeholder plots...", expanded=True) as status:
                cmd = [sys.executable, "scripts/generate_placeholders.py"]
                result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.getenv("APP_ROOT", os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), timeout=30)
                st.code(result.stdout, language="bash")
                if result.returncode == 0:
                    status.update(label="Placeholders generated", state="complete")
                    st.toast("Placeholders regenerated", icon="🎨")
                else:
                    status.update(label=f"Failed (exit {result.returncode})", state="error")
                    st.error(result.stderr)

        # PDF Report download (PostgreSQL only)
        st.markdown("---")
        st.markdown("### 📄 Report")
        if DB_MODE == "sqlite":
            st.caption("⚠️ PDF reports require PostgreSQL. Switch with `--local` off.")
        elif st.button("📥 Generate PDF Report", use_container_width=True, type="secondary"):
            with st.status("Generating PDF report...", expanded=True) as status:
                try:
                    pdf_bytes = generate_report_bytes()
                    status.update(label=f"Report ready ({len(pdf_bytes):,} bytes)", state="complete")
                    st.toast("PDF report generated!", icon="📄")
                    st.session_state["report_pdf_bytes"] = pdf_bytes
                    st.session_state["report_ready"] = True
                except Exception as e:
                    status.update(label=f"Failed: {e}", state="error")
                    st.error(str(e))
                    st.session_state["report_ready"] = False

        # Show download button after generation
        if st.session_state.get("report_ready"):
            st.download_button(
                label="⬇️ Download Report PDF",
                data=st.session_state["report_pdf_bytes"],
                file_name="k8s_microservice_failure_analysis_report.pdf",
                mime="application/pdf",
                use_container_width=True,
            )

        # Config
        st.markdown("---")
        st.markdown("### ⚙️ Configuration")
        st.markdown(f"""
        - **Spark**: `{os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077')}`
        - **MinIO**: `{os.getenv('MINIO_ENDPOINT', 'http://minio:9000')}`
        - **Postgres**: `{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}`
        - **Data rows**: `{os.getenv('SAMPLE_DATA_ROWS', '100K')}`
        """)

        # DB Status
        st.markdown("---")
        st.markdown("### 💾 Database Status")
        db_ok = check_connection()
        if db_ok:
            backend_label = "SQLite" if DB_MODE == "sqlite" else "PostgreSQL"
            st.success(f"Connected ✓ ({backend_label})")
            table_names = get_table_names()
            for tname in table_names:
                cnt = get_table_row_count(tname)
                st.caption(f"📋 `{tname}` — {cnt:,} rows")
        else:
            st.error("Disconnected ✗")
            backend_label = "SQLite" if DB_MODE == "sqlite" else "PostgreSQL"
            st.caption(f"Could not connect to {backend_label}.")

        st.markdown("---")
        st.caption("Academic Project — Data Intensive & Scalable Systems")


# ============================================================
# Overview Tab
# ============================================================
def render_overview():
    """Render the overview/landing tab."""
    st.markdown('<p class="main-header">Distributed Analysis of K8s Microservice Logs</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Failure Detection · Cross-Service Propagation · Spark Scalability</p>', unsafe_allow_html=True)

    # Key metrics row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_processed = query_db("SELECT COUNT(*) as cnt FROM processed_telemetry")
        val = f"{total_processed.iloc[0,0]:,}" if not total_processed.empty else "—"
        st.markdown(f'<div class="metric-card"><div class="metric-value">{val}</div><div class="metric-label">Records Processed</div></div>', unsafe_allow_html=True)

    with col2:
        total_anomalies = query_db("SELECT COUNT(*) as cnt FROM anomaly_scores WHERE is_anomaly_overall = 1")
        val = f"{total_anomalies.iloc[0,0]:,}" if not total_anomalies.empty else "—"
        st.markdown(f'<div class="metric-card"><div class="metric-value">{val}</div><div class="metric-label">Anomalies Detected</div></div>', unsafe_allow_html=True)

    with col3:
        prop_pairs = query_db("SELECT COUNT(*) as cnt FROM cross_service_pairs WHERE propagation_score > 0")
        val = f"{prop_pairs.iloc[0,0]:,}" if not prop_pairs.empty else "—"
        st.markdown(f'<div class="metric-card"><div class="metric-value">{val}</div><div class="metric-label">Propagation Pairs</div></div>', unsafe_allow_html=True)

    with col4:
        scale_tests = query_db("SELECT COUNT(DISTINCT data_size) as cnt FROM scalability_metrics")
        val = f"{scale_tests.iloc[0,0]}" if not scale_tests.empty else "—"
        st.markdown(f'<div class="metric-card"><div class="metric-value">{val}</div><div class="metric-label">Scale Tests Run</div></div>', unsafe_allow_html=True)

    st.markdown("---")

    # Architecture diagram (text-based)
    col_left, col_right = st.columns([1, 1])

    with col_left:
        st.markdown("### 🏗️ Architecture")
        st.code("""
Dataset (CSV)
    ↓
MinIO Blob Storage  ←  PostgreSQL (Results)
    ↓                        ↑
Apache Spark Cluster      JDBC
├─ Master                  │
└─ Workers × N             │
    ↓                       │
┌───────────────────────────┘
│ Preprocessing → Cross-Service (RQ1)
│              → Failure Detection (RQ2)
│              → Scalability (RQ3)
│              → Visualization
└─ Streamlit Dashboard
        """, language=None)

        st.markdown("### 📋 Research Questions")
        st.markdown("""
        <span class="rq-badge rq1">RQ1</span> Cross-service failure propagation<br>
        <span class="rq-badge rq2">RQ2</span> Abnormal failure pattern detection<br>
        <span class="rq-badge rq3">RQ3</span> Spark scalability analysis
        """, unsafe_allow_html=True)

    with col_right:
        st.markdown("### 📊 Pipeline Progress")

        progress = _scan_pipeline_progress()
        completed_count = sum(1 for s in progress.values() if s == "completed")
        running_count = sum(1 for s in progress.values() if s == "running")

        # Overall progress bar
        overall_pct = completed_count / 6
        if running_count > 0:
            # Estimate partial progress for running module
            overall_pct += 0.08  # small bump for in-progress
        st.progress(min(overall_pct, 1.0), text=f"{completed_count}/6 modules completed")

        # Per-module step indicators
        status_icons = {
            "completed": "✅",
            "running":   "🔄",
            "pending":   "⏳",
        }
        status_labels = {
            "completed": "completed",
            "running":   "**running...**",
            "pending":   "pending",
        }

        for label, _marker, _mod in MODULE_STEPS:
            mod_num = int(label[0])
            s = progress[mod_num]
            icon = status_icons[s]
            lbl = status_labels[s]
            st.markdown(f"{icon} {label} — *{lbl}*")

        # Quick stats
        st.markdown("### 🔍 Quick Stats")
        error_rate = query_db("""
            SELECT ROUND(100.0 * SUM(is_failure) / COUNT(*), 2) as pct
            FROM processed_telemetry
        """)
        if not error_rate.empty and error_rate.iloc[0,0] is not None:
            st.metric("Overall Error Rate", f"{error_rate.iloc[0,0]}%")

        top_failing = query_db("""
            SELECT service_name, COUNT(*) as errors
            FROM processed_telemetry
            WHERE is_failure = 1
            GROUP BY service_name
            ORDER BY errors DESC LIMIT 5
        """)
        if not top_failing.empty:
            st.markdown("**Top Failing Services:**")
            st.dataframe(top_failing, hide_index=True, use_container_width=True)

    # ---- Placeholder Image Gallery ----
    st.markdown("---")
    st.markdown("### 🖼️ Results Preview")

    output_dir = os.environ.get("OUTPUT_DIR", "/output")
    gallery = [
        ("rq1_propagation_heatmap.png", "RQ1: Propagation Heatmap"),
        ("rq2_anomaly_timeseries.png", "RQ2: Anomaly Time Series"),
        ("rq2_failure_patterns.png", "RQ2: Failure Patterns"),
        ("rq3_scaling_curves.png", "RQ3: Scaling Curves"),
        ("rq3_scalability_efficiency.png", "RQ3: Efficiency"),
        ("dashboard_summary.png", "Dashboard Summary"),
    ]

    # Two rows of 3 images
    for row_start in (0, 3):
        cols = st.columns(3)
        for i, (filename, caption) in enumerate(gallery[row_start:row_start + 3]):
            path = os.path.join(output_dir, filename)
            if os.path.exists(path):
                cols[i].image(path, caption=caption, use_container_width=True)
            else:
                cols[i].info(f"{caption}\n\n(placeholder not found)")


# ============================================================
# RQ1 Tab — Cross-Service Failure Propagation
# ============================================================
def render_rq1():
    """Render RQ1: Cross-service failure propagation analysis."""
    st.markdown("## <span class='rq-badge rq1'>RQ1</span> Cross-Service Failure Propagation", unsafe_allow_html=True)
    st.markdown("*How can distributed analysis identify cross-service failure propagation?*")

    # Propagation heatmap
    st.markdown("### 🔥 Propagation Score Heatmap")
    prop_data = query_db("""
        SELECT caller_service, callee_service, propagation_score
        FROM cross_service_pairs
        WHERE propagation_score > 0
    """)

    if not prop_data.empty:
        pivot = prop_data.pivot_table(
            index="callee_service", columns="caller_service",
            values="propagation_score", fill_value=0
        )
        fig = px.imshow(
            pivot.values,
            x=pivot.columns, y=pivot.index,
            color_continuous_scale="YlOrRd",
            text_auto=".2f",
            aspect="auto",
            title="Service-to-Service Failure Propagation",
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No propagation data yet. Run the pipeline first.")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### ⛓️ Top Propagation Chains")
        chains = query_db("""
            SELECT source_service, target_service,
                   COUNT(*) as chain_count,
                   ROUND(AVG(propagation_lag_sec), 2) as avg_lag_sec
            FROM propagation_chains
            GROUP BY source_service, target_service
            ORDER BY chain_count DESC LIMIT 10
        """)
        if not chains.empty:
            fig = go.Figure(data=[
                go.Bar(
                    x=chains["chain_count"],
                    y=chains["source_service"] + " → " + chains["target_service"],
                    orientation="h",
                    marker_color="orangered",
                    text=chains["chain_count"],
                    textposition="outside",
                )
            ])
            fig.update_layout(height=400, xaxis_title="Chain Count", margin=dict(l=200))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No propagation chains yet.")

    with col2:
        st.markdown("### 🔗 Error Correlation Matrix")
        corr_data = query_db("""
            SELECT service_a, service_b, error_correlation
            FROM error_correlations
            ORDER BY ABS(error_correlation) DESC
        """)
        if not corr_data.empty:
            top10 = corr_data.head(10)
            fig = px.bar(
                top10,
                x="error_correlation",
                y=top10["service_a"] + " ↔ " + top10["service_b"],
                orientation="h",
                color="error_correlation",
                color_continuous_scale="RdBu",
                title="Top Service Error Correlations",
            )
            fig.update_layout(height=400, margin=dict(l=200))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No correlation data yet.")

    # Raw data table
    st.markdown("### 📋 Propagation Details")
    top_pairs = query_db("""
        SELECT caller_service, callee_service, call_count,
               caller_error_count, callee_error_count,
               co_failure_count, ROUND(propagation_score, 4) as score
        FROM cross_service_pairs
        ORDER BY propagation_score DESC LIMIT 20
    """)
    if not top_pairs.empty:
        st.dataframe(top_pairs, hide_index=True, use_container_width=True)


# ============================================================
# RQ2 Tab — Abnormal Failure Pattern Detection
# ============================================================
def render_rq2():
    """Render RQ2: Anomaly detection and failure patterns."""
    st.markdown("## <span class='rq-badge rq2'>RQ2</span> Abnormal Failure Pattern Detection", unsafe_allow_html=True)
    st.markdown("*How effectively can distributed processing identify abnormal failure patterns?*")

    # Anomaly overview metrics
    metrics = query_db("""
        SELECT
            COUNT(*) as total_buckets,
            SUM(is_anomaly_overall) as total_anomalies,
            ROUND(100.0 * SUM(is_anomaly_overall) / COUNT(*), 2) as anomaly_rate,
            SUM(is_anomaly_error) as error_anomalies,
            SUM(is_anomaly_latency) as latency_anomalies,
            SUM(is_anomaly_resource) as resource_anomalies
        FROM anomaly_scores
    """)

    col1, col2, col3, col4 = st.columns(4)
    if not metrics.empty:
        m = metrics.iloc[0]
        with col1:
            st.metric("Total Anomalies", f"{m['total_anomalies']:,}")
        with col2:
            st.metric("Anomaly Rate", f"{m['anomaly_rate']}%")
        with col3:
            st.metric("Error Anomalies", f"{m['error_anomalies']:,}")
        with col4:
            st.metric("Latency Anomalies", f"{m['latency_anomalies']:,}")

    # Anomaly per service bar chart
    st.markdown("### 📊 Anomalies per Service")
    svc_anomalies = query_db("""
        SELECT service_name, COUNT(*) as anomaly_count
        FROM anomaly_scores WHERE is_anomaly_overall = 1
        GROUP BY service_name ORDER BY anomaly_count DESC
    """)
    if not svc_anomalies.empty:
        fig = px.bar(
            svc_anomalies, x="service_name", y="anomaly_count",
            color="anomaly_count", color_continuous_scale="Reds",
            title="Multi-Signal Anomalies by Service",
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 🧩 Failure Pattern Distribution")
        patterns = query_db("""
            SELECT pattern_type, SUM(occurrence_count) as total
            FROM failure_patterns
            GROUP BY pattern_type ORDER BY total DESC
        """)
        if not patterns.empty:
            fig = px.pie(
                patterns, values="total", names="pattern_type",
                color_discrete_sequence=px.colors.qualitative.Set2,
                title="Pattern Type Breakdown",
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No pattern data yet.")

    with col2:
        st.markdown("### 🔴 Anomaly Signal Breakdown")
        if not metrics.empty:
            signals = pd.DataFrame({
                "Signal": ["Error Rate", "Latency", "Resource"],
                "Count": [m["error_anomalies"], m["latency_anomalies"], m["resource_anomalies"]],
            })
            fig = px.bar(
                signals, x="Signal", y="Count", color="Signal",
                color_discrete_map={"Error Rate": "#ef4444", "Latency": "#f59e0b", "Resource": "#3b82f6"},
                title="Anomalies by Signal Type",
            )
            st.plotly_chart(fig, use_container_width=True)

    # Pattern details table
    st.markdown("### 📋 Failure Pattern Details")
    pattern_detail = query_db("""
        SELECT service_name, pattern_type, occurrence_count,
               ROUND(avg_severity, 2) as avg_severity
        FROM failure_patterns
        ORDER BY occurrence_count DESC LIMIT 15
    """)
    if not pattern_detail.empty:
        st.dataframe(pattern_detail, hide_index=True, use_container_width=True)


# ============================================================
# RQ3 Tab — Spark Scalability
# ============================================================
def render_rq3():
    """Render RQ3: Spark scalability analysis."""
    st.markdown("## <span class='rq-badge rq3'>RQ3</span> Spark Scalability Analysis", unsafe_allow_html=True)
    st.markdown("*How does distributed Spark processing scale with increasing data volumes?*")

    scale_data = query_db("""
        SELECT data_size,
               ROUND(AVG(total_sec), 2) as avg_time_sec,
               ROUND(AVG(throughput_rows_per_sec), 0) as avg_throughput,
               ROUND(AVG(speedup_vs_baseline), 3) as avg_speedup
        FROM scalability_metrics
        GROUP BY data_size ORDER BY data_size
    """)

    if scale_data.empty:
        st.info("No scalability data yet. Run the pipeline first.")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        sizes_tested = len(scale_data)
        st.metric("Data Sizes Tested", sizes_tested)
    with col2:
        max_rows = scale_data["data_size"].max()
        st.metric("Max Data Size", f"{max_rows:,.0f} rows")
    with col3:
        max_throughput = scale_data["avg_throughput"].max()
        st.metric("Max Throughput", f"{max_throughput:,.0f} rows/s")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### ⏱️ Execution Time vs Data Size")
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Scatter(x=scale_data["data_size"], y=scale_data["avg_time_sec"],
                       mode="lines+markers", name="Time (s)",
                       line=dict(color="#667eea", width=3), marker=dict(size=10)),
            secondary_y=False,
        )
        fig.update_xaxes(type="log", title="Data Size (rows)")
        fig.update_yaxes(title_text="Execution Time (s)", secondary_y=False)
        fig.update_layout(height=450, title="Execution Time Scaling (log-log)")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### 📈 Throughput vs Data Size")
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=scale_data["data_size"], y=scale_data["avg_throughput"],
            mode="lines+markers", name="Throughput",
            line=dict(color="#10b981", width=3), marker=dict(size=10),
            fill="tozeroy", fillcolor="rgba(16,185,129,0.1)",
        ))
        fig.update_xaxes(type="log", title="Data Size (rows)")
        fig.update_yaxes(title="Throughput (rows/s)")
        fig.update_layout(height=450, title="Throughput Scaling")
        st.plotly_chart(fig, use_container_width=True)

    # Scalability efficiency
    st.markdown("### ⚡ Scalability Efficiency")
    baseline_time = scale_data["avg_time_sec"].iloc[0]
    baseline_rows = scale_data["data_size"].iloc[0]

    efficiency_data = []
    for _, row in scale_data.iterrows():
        ratio = row["data_size"] / baseline_rows
        speedup = baseline_time / row["avg_time_sec"] if row["avg_time_sec"] > 0 else 0
        efficiency = speedup / ratio if ratio > 0 else 0
        efficiency_data.append({
            "Data Size": row["data_size"],
            "Ratio": ratio,
            "Speedup": round(speedup, 3),
            "Efficiency": round(efficiency, 3),
        })
    eff_df = pd.DataFrame(efficiency_data)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(x=eff_df["Ratio"], y=eff_df["Speedup"],
                   mode="lines+markers", name="Actual Speed-up",
                   line=dict(color="#667eea", width=3), marker=dict(size=10)),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(x=eff_df["Ratio"], y=eff_df["Ratio"],
                   mode="lines", name="Ideal Linear",
                   line=dict(color="gray", dash="dash", width=2)),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(x=eff_df["Ratio"], y=eff_df["Efficiency"],
                   mode="lines+markers", name="Efficiency",
                   line=dict(color="#ef4444", width=2, dash="dot"), marker=dict(size=10, symbol="square")),
        secondary_y=True,
    )
    fig.update_xaxes(title="Data Size Ratio (× baseline)")
    fig.update_yaxes(title_text="Speed-up Factor", secondary_y=False)
    fig.update_yaxes(title_text="Efficiency", secondary_y=True, range=[0, 1.3])
    fig.update_layout(height=450, title="Speed-up & Scalability Efficiency")
    st.plotly_chart(fig, use_container_width=True)

    # Operation breakdown
    st.markdown("### 🔬 Operation-Level Breakdown")
    op_data = query_db("""
        SELECT data_size,
               ROUND(AVG(groupby_agg_sec), 2) as groupby_agg,
               ROUND(AVG(window_fn_sec), 2) as window_fn,
               ROUND(AVG(join_sec), 2) as join_op,
               ROUND(AVG(shuffle_sec), 2) as shuffle
        FROM scalability_metrics
        GROUP BY data_size ORDER BY data_size
    """)
    if not op_data.empty:
        op_melted = op_data.melt(
            id_vars=["data_size"],
            value_vars=["groupby_agg", "window_fn", "join_op", "shuffle"],
            var_name="Operation", value_name="Time (s)"
        )
        fig = px.bar(
            op_melted, x="data_size", y="Time (s)", color="Operation",
            barmode="group", title="Operation Time by Data Size",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig.update_xaxes(type="category", title="Data Size (rows)")
        st.plotly_chart(fig, use_container_width=True)

    # Raw data
    st.markdown("### 📋 Raw Benchmark Data")
    st.dataframe(scale_data, hide_index=True, use_container_width=True)


# ============================================================
# Spark Metrics Tab
# ============================================================

SPARK_UI = os.getenv("SPARK_MASTER_HOST", "spark-master")
SPARK_API_BASE = f"http://{SPARK_UI}:8080/api/v1"


def _fetch_spark(path: str, timeout: int = 5) -> dict | list | None:
    """Fetch from Spark REST API, returning parsed JSON or None on failure."""
    try:
        r = requests.get(f"{SPARK_API_BASE}{path}", timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def render_spark_metrics():
    """Render real-time Spark cluster metrics from the REST API."""
    st.markdown("## 🖥️ Spark Cluster — Real-Time Metrics")
    st.caption(f"API: {SPARK_API_BASE}")

    # Auto-refresh toggle
    auto_refresh = st.checkbox("🔄 Auto-refresh (5s)", value=True, key="spark_autorefresh")

    # Fetch cluster overview
    apps = _fetch_spark("/applications")
    master_state = _fetch_spark("/applications?status=running", timeout=3)

    if apps is None:
        st.warning("⚠️ Unable to reach Spark Master API. Is Spark running?")
        st.caption(f"Trying: {SPARK_API_BASE}/applications")
        return

    # ---- Row 1: Cluster Summary ----
    running_apps = [a for a in apps if a.get("state") == "RUNNING"] if isinstance(apps, list) else []
    completed_apps = [a for a in apps if a.get("state") == "FINISHED"] if isinstance(apps, list) else []

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Applications", len(apps) if isinstance(apps, list) else "—")
    with col2:
        st.metric("Running", len(running_apps))
    with col3:
        st.metric("Completed", len(completed_apps))
    with col4:
        cores = sum(a.get("cores", 0) for a in running_apps)
        st.metric("Cores in Use", cores)

    st.markdown("---")

    # ---- Row 2: Running Applications ----
    if running_apps:
        st.markdown("### 🟢 Running Applications")
        for app in running_apps:
            app_id = app.get("id", "")
            app_name = app.get("name", "Unknown")
            duration = app.get("duration", 0) / 1000  # ms → s

            with st.expander(f"📱 {app_name} ({app_id[:20]}...)", expanded=True):
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    st.metric("Duration", f"{duration:.0f}s")
                with c2:
                    st.metric("Cores", app.get("cores", "—"))
                with c3:
                    mem_per_exec = app.get("memoryPerExecutorMB", 0)
                    st.metric("Mem/Executor", f"{mem_per_exec} MB")
                with c4:
                    spark_user = app.get("sparkUser", "—")
                    st.metric("User", spark_user)

                # ---- Jobs ----
                jobs = _fetch_spark(f"/applications/{app_id}/jobs")
                if jobs and isinstance(jobs, list):
                    st.markdown("**Jobs**")
                    job_rows = []
                    for j in jobs[:10]:
                        job_id = j.get("jobId", "—")
                        status = j.get("status", "—")
                        num_tasks = j.get("numTasks", 0)
                        completed = j.get("numCompletedTasks", 0)
                        job_rows.append({
                            "Job ID": job_id,
                            "Status": status,
                            "Progress": f"{completed}/{num_tasks}",
                            "Pct": round(100 * completed / max(num_tasks, 1), 1),
                        })
                    if job_rows:
                        job_df = pd.DataFrame(job_rows)
                        st.dataframe(job_df, hide_index=True, use_container_width=True)

                        # Progress bars
                        for jr in job_rows:
                            col_bar, col_pct = st.columns([4, 1])
                            with col_bar:
                                st.progress(jr["Pct"] / 100, text=f"Job {jr['Job ID']} — {jr['Status']}")
                            with col_pct:
                                st.caption(f"{jr['Pct']}%")

                # ---- Stages ----
                stages = _fetch_spark(f"/applications/{app_id}/stages")
                if stages and isinstance(stages, list):
                    st.markdown("**Stages**")
                    stage_rows = []
                    for s in stages[:8]:
                        stage_rows.append({
                            "Stage ID": s.get("stageId", "—"),
                            "Status": s.get("status", "—"),
                            "Tasks": f"{s.get('numCompleteTasks', 0)}/{s.get('numTasks', 0)}",
                            "Input": _fmt_bytes(s.get("inputBytes", 0)),
                            "Output": _fmt_bytes(s.get("outputBytes", 0)),
                        })
                    if stage_rows:
                        st.dataframe(pd.DataFrame(stage_rows), hide_index=True, use_container_width=True)

                # ---- Executors ----
                executors = _fetch_spark(f"/applications/{app_id}/allexecutors")
                if executors and isinstance(executors, list):
                    st.markdown("**Executors**")
                    exec_rows = []
                    total_mem_used = 0
                    total_mem_max = 0
                    for e in executors:
                        mem_used = e.get("memoryUsed", 0)
                        mem_max = e.get("maxMemory", 0)
                        total_mem_used += mem_used
                        total_mem_max += mem_max
                        exec_rows.append({
                            "ID": e.get("id", "—")[:12],
                            "Host": e.get("hostPort", "—").split(":")[0][:20],
                            "Cores": e.get("totalCores", 0),
                            "Tasks": f"{e.get('completedTasks', 0)}/{e.get('totalTasks', 0)}",
                            "Memory": _fmt_bytes(mem_used),
                        })
                    if exec_rows:
                        st.dataframe(pd.DataFrame(exec_rows), hide_index=True, use_container_width=True)

                        # Memory usage gauge
                        if total_mem_max > 0:
                            mem_pct = total_mem_used / total_mem_max
                            st.markdown(f"**Cluster Memory:** {_fmt_bytes(total_mem_used)} / {_fmt_bytes(total_mem_max)}")
                            st.progress(min(mem_pct, 1.0))
    else:
        st.info("No running applications. Start a pipeline module to see live metrics.")

    # ---- Completed Apps Summary ----
    if completed_apps:
        st.markdown("---")
        st.markdown("### ✅ Recently Completed")
        comp_rows = []
        for a in completed_apps[-5:]:
            comp_rows.append({
                "Name": a.get("name", "—")[:40],
                "Duration": f"{a.get('duration', 0) / 1000:.0f}s",
                "Cores": a.get("cores", "—"),
                "State": a.get("state", "—"),
            })
        st.dataframe(pd.DataFrame(comp_rows), hide_index=True, use_container_width=True)

    # Auto-refresh
    if auto_refresh:
        time.sleep(5)
        st.rerun()


def _fmt_bytes(n: int) -> str:
    """Format bytes to human-readable string."""
    if n == 0:
        return "0 B"
    for unit in ["B", "KB", "MB", "GB"]:
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


# ============================================================
# Pipeline Progress Scanner (log-based)
# ============================================================

MODULE_STEPS = [
    ("1. Ingestion",           "MODULE 1", "ingestion"),
    ("2. Preprocessing",       "MODULE 2", "preprocessing"),
    ("3. Cross-Service (RQ1)", "MODULE 3", "cross_service_analysis"),
    ("4. Failure Detect (RQ2)","MODULE 4", "failure_detection"),
    ("5. Scalability (RQ3)",   "MODULE 5", "scalability"),
    ("6. Visualization",       "MODULE 6", "visualization"),
]


def _scan_pipeline_progress() -> dict[int, str]:
    """
    Scan the pipeline log file to determine each module's status.

    Detects:
      - "running":  module banner found but no completion message
      - "completed": "✓ Module X complete" found
      - "pending":   no banner found yet

    Returns dict: {module_number: status_string}
    """
    import re

    status = {i: "pending" for i in range(1, 7)}

    try:
        with open(LOG_FILE_PATH, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except FileNotFoundError:
        return status

    if not content.strip():
        return status

    for i in range(1, 7):
        # Check for completion first (more definitive)
        if f"Module {i} complete" in content:
            status[i] = "completed"
        # Check if module has started (MODULE X in log)
        elif f"MODULE {i}" in content:
            # If we see the module banner, it's either running or completed
            # Completion is already checked above, so this means still running
            status[i] = "running"

    # Edge case: if modules 5-6 aren't started but 4 just completed,
    # module 4 should still show completed.
    # The above logic handles this correctly.

    return status


# ============================================================
# Pipeline Log Viewer Tab
# ============================================================

LOG_FILE_PATH = "/output/pipeline.log"

# Log-level colour map for HTML rendering
LOG_COLORS = {
    "DEBUG":    "#9ca3af",  # gray
    "INFO":     "#e5e7eb",  # light gray
    "WARNING":  "#fbbf24",  # amber
    "ERROR":    "#ef4444",  # red
    "CRITICAL": "#dc2626",  # bold red (same colour, heavier weight)
}


def _parse_log_line(line: str) -> tuple[str, str, str] | None:
    """
    Parse a log line into (timestamp_prefix, level, message).

    Expected format:
      2024-01-15 10:23:45.123 [INFO ] ingestion: Generating data...

    Returns None if the line doesn't match the expected format.
    """
    import re
    m = re.match(
        r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d{3})\s+\[(DEBUG|INFO |WARN |ERROR|CRIT)\]\s+(.+)",
        line,
    )
    if m:
        ts = m.group(1)
        level_raw = m.group(2).strip()
        # Normalise WARN → WARNING
        if level_raw == "WARN":
            level_raw = "WARNING"
        return ts, level_raw, m.group(3)
    return None


def _highlight_text(text: str, keyword: str) -> str:
    """
    Wrap case-insensitive matches of `keyword` in `<mark>` tags.
    Preserves the case of the original text. Already HTML-escaped input expected.
    """
    import re
    pattern = re.compile(re.escape(keyword), re.IGNORECASE)
    return pattern.sub(lambda m: f"<mark>{m.group(0)}</mark>", text)


def render_logs():
    """Render the live pipeline log viewer tab."""
    st.markdown("## 📋 Pipeline Log Viewer")
    st.caption(f"Watching: `{LOG_FILE_PATH}`")

    # ---- Controls Row ----
    c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 1, 1])

    with c1:
        auto_refresh = st.checkbox("🔄 Auto-refresh (3s)", value=True, key="log_autorefresh")

    with c2:
        level_filter = st.selectbox(
            "Min level",
            ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            index=0,
            key="log_level_filter",
            label_visibility="collapsed",
        )

    with c3:
        num_lines = st.selectbox(
            "Lines",
            [50, 100, 200, 500, 1000, "All"],
            index=1,
            key="log_num_lines",
            label_visibility="collapsed",
        )

    with c4:
        if st.button("🗑️ Clear", use_container_width=True, key="log_clear"):
            try:
                open(LOG_FILE_PATH, "w").close()
                st.toast("Log file cleared", icon="🗑️")
            except OSError:
                st.warning("Could not clear log file.")

    with c5:
        st.download_button(
            label="⬇️",
            data=_read_log_file(0, "All"),
            file_name="pipeline.log",
            mime="text/plain",
            use_container_width=True,
            key="log_download",
        )

    # ---- Search ----
    col_srch, col_hl = st.columns([4, 1])
    with col_srch:
        search_query = st.text_input(
            "🔍 Search",
            placeholder="Filter lines containing...",
            key="log_search",
            label_visibility="collapsed",
        )
    with col_hl:
        highlight = st.checkbox("💡 Highlight", value=True, key="log_highlight")

    # ---- File Stats ----
    try:
        stat = os.stat(LOG_FILE_PATH)
        from datetime import datetime
        modified = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        total_lines = 0
        with open(LOG_FILE_PATH, "r", encoding="utf-8", errors="replace") as f:
            for _ in f:
                total_lines += 1

        col_s1, col_s2, col_s3 = st.columns(3)
        with col_s1:
            st.caption(f"📄 {total_lines:,} lines")
        with col_s2:
            st.caption(f"💾 {_fmt_bytes(stat.st_size)}")
        with col_s3:
            st.caption(f"🕐 {modified}")
    except FileNotFoundError:
        st.info("📭 Log file not found. The pipeline hasn't written any logs yet.\n\n"
                "Run a pipeline module or start the full pipeline to generate logs.")
        return
    except OSError as e:
        st.warning(f"⚠️ Could not read log file: {e}")
        return

    # ---- Log Content ----
    log_text = _read_log_file(level_filter, num_lines, search=search_query)

    if not log_text.strip():
        msg = f"No log entries at {level_filter}+ level"
        if search_query:
            msg += f" matching '{search_query}'"
        st.info(msg + ". Try adjusting the filters.")
    else:
        # Render with colour-coded HTML
        html_lines = []
        level_priority = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}
        min_priority = level_priority.get(level_filter, 0)

        for line in log_text.split("\n"):
            parsed = _parse_log_line(line)
            if parsed:
                ts, level, msg = parsed
                if level_priority.get(level, 0) >= min_priority:
                    color = LOG_COLORS.get(level, "#e5e7eb")
                    weight = "font-weight:bold;" if level in ("ERROR", "CRITICAL") else ""
                    escaped = msg.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                    # Apply highlight if enabled
                    if highlight and search_query.strip():
                        escaped = _highlight_text(escaped, search_query.strip())
                    html_lines.append(
                        f'<span style="color:var(--log-timestamp);font-family:monospace;font-size:12px">{ts}</span> '
                        f'<span style="color:{color};font-family:monospace;font-size:12px;{weight}">'
                        f'[{level:5s}]</span> '
                        f'<span style="color:var(--log-text);font-family:monospace;font-size:12px">{escaped}</span>'
                    )
                else:
                    # Below threshold — show dimmed
                    html_lines.append(
                        f'<span style="color:var(--text-muted);font-family:monospace;font-size:12px;opacity:0.5">'
                        f'{line.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")}</span>'
                    )
            else:
                # Unparseable line — show dim
                html_lines.append(
                    f'<span style="color:var(--text-muted);font-family:monospace;font-size:12px;opacity:0.4">'
                    f'{line.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")}</span>'
                )

        # Wrap in a scrollable container
        html = (
            '<div style="background:var(--log-bg);border:1px solid var(--log-border);'
            'border-radius:8px;padding:12px;max-height:70vh;overflow-y:auto;'
            'font-family:monospace;line-height:1.6;white-space:pre-wrap;'
            'word-break:break-all">\n'
            + "\n".join(html_lines)
            + "\n</div>"
        )
        st.markdown(html, unsafe_allow_html=True)

        # Scroll-to-bottom helper
        st.markdown(
            "<script>"
            "var el = window.parent.document.querySelector('[data-testid=\"stMarkdownContainer\"] div');"
            "if(el) el.scrollTop = el.scrollHeight;"
            "</script>",
            unsafe_allow_html=True,
        )

    # Auto-refresh
    if auto_refresh:
        time.sleep(3)
        st.rerun()


def _read_log_file(level_filter: str | int = 0, num_lines: str | int = 100, search: str = "") -> str:
    """
    Read the pipeline log file, optionally filtering by level, line count, and keyword.

    Args:
        level_filter: Minimum log level (DEBUG=0, INFO=1, ..., or string name).
        num_lines:   Number of lines to return, or "All".
        search:      Case-insensitive keyword to filter lines by content.

    Returns the filtered log content as a single string.
    """
    try:
        with open(LOG_FILE_PATH, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except FileNotFoundError:
        return ""

    if not lines:
        return ""

    # Limit to last N lines
    if num_lines != "All":
        lines = lines[-int(num_lines):]

    # If level_filter is an int (priority), convert to name
    if isinstance(level_filter, int):
        priority_map = {0: "DEBUG", 1: "INFO", 2: "WARNING", 3: "ERROR", 4: "CRITICAL"}
        level_filter = priority_map.get(level_filter, "DEBUG")

    # Build level priority map
    level_priority = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}
    min_priority = level_priority.get(level_filter, 0)

    search_lower = search.strip().lower()
    filtered = []
    for line in lines:
        stripped = line.rstrip("\n")
        # Apply keyword filter first (before level filter for accuracy)
        if search_lower and search_lower not in stripped.lower():
            continue
        parsed = _parse_log_line(stripped)
        if parsed:
            _, level, _ = parsed
            if level_priority.get(level, 0) >= min_priority:
                filtered.append(stripped)
        else:
            # Unparseable line — always include if it matches search
            filtered.append(stripped)

    return "\n".join(filtered)
def main():
    render_sidebar()

    # Tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "🏠 Overview",
        "🔗 RQ1 · Propagation",
        "🔴 RQ2 · Anomalies",
        "⚡ RQ3 · Scalability",
        "🖥️ Spark Cluster",
        "📋 Logs",
    ])

    with tab1:
        render_overview()
    with tab2:
        render_rq1()
    with tab3:
        render_rq2()
    with tab4:
        render_rq3()
    with tab5:
        render_spark_metrics()
    with tab6:
        render_logs()

    # Footer
    st.markdown("---")
    st.caption(
        "Distributed Analysis of Kubernetes Microservice Logs for Failure Detection · "
        "Academic Project · Data Intensive & Scalable Systems"
    )


if __name__ == "__main__":
    main()
