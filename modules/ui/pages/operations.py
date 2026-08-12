
# Page: Operations - run the pipeline, watch the cluster, read the logs


from __future__ import annotations

import html
import os
import re

import subprocess
import sys
import threading


from datetime import datetime
from pathlib import Path
from queue import Empty, Queue

import pandas as pd
import requests
import streamlit as st

from modules.settings import PROJECT_ROOT, get_settings
from modules.ui import data
from modules.ui.components import (
    bytes_human, compact_number,
    empty_state, masthead,section,
    stat_tiles,  status_pill
)
from modules.ui.theme import Palette

PIPELINE_TARGETS: dict[str, str] = {
    "Full pipeline (all six modules)": "scripts/run_pipeline.sh",
    "1 - Ingestion": "modules.ingestion",
    "2 - Preprocessing": "modules.preprocessing",
    "3 - Cross-service analysis (RQ1)": "modules.cross_service_analysis",
    "4 - Failure detection (RQ2)": "modules.failure_detection",
    "5 - Scalability analysis (RQ3)": "modules.scalability_analysis",
    "6 - Visualization": "modules.visualization"
}

# Which table each module fills, so the page can report what has actually run.
MODULE_OUTPUTS: list[tuple[str, str]] = [
    ("1 - Ingestion", "raw_telemetry"),  ("2 - Preprocessing", "processed_telemetry"),
    ("3 - Cross-service (RQ1)", "cross_service_pairs"),
    ("4 - Failure detection (RQ2)", "anomaly_scores"), ("5 - Scalability (RQ3)", "scalability_metrics")
]

LOG_LINE = re.compile(
    r"(?P<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d{3})\s+"
    r"\[(?P<level>DEBUG|INFO|WARN|WARNING|ERROR|CRIT|CRITICAL)\s*\]\s+(?P<msg>.*)"
)
LEVEL_ORDER = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}

# Bound how much of a log file is parsed in one render, so a runaway log
# cannot wedge the page.
MAX_LOG_LINES = 2000


def render(pal: Palette, services: tuple[str, ...]) -> None:
    del services  # Operations are pipeline-wide.

    masthead(
        "Operations",
        "Run pipeline stages, watch the Spark cluster, read the pipeline log, and " "export the findings.",
        eyebrow="Control plane",
    )

    tab_run, tab_cluster, tab_logs, tab_report, tab_config = st.tabs(
        ["Pipeline", "Spark cluster", "Logs", "Report", "Configuration"]
    )

    with tab_run:
        _pipeline_tab(pal)
    with tab_cluster:
        _cluster_tab(pal)
    with tab_logs:
        _logs_tab(pal)
    with tab_report:
        _report_tab()
    with tab_config:
        _config_tab()



# Report

def _report_tab() -> None:
    section(
        "Analysis report",
        "A PDF covering all three research questions, generated from whatever the "
        "pipeline has written so far.",
    )

    if not data.has_data("processed_telemetry"):
        empty_state(
            "Nothing to report yet",
            "The report is built from the results tables. Run the pipeline first, or "
            "start in demo mode with python run_streamlit.py --local",
        )
        return

    if st.button("Generate PDF report", type="primary"):
        with st.status("Building report", expanded=False) as status:
            try:
                # Imported here rather than at module scope: fpdf2 is only
                # needed when someone actually asks for a report, and a
                # missing optional dependency should not break the page.
                from modules.report_generator import generate_report_bytes

                st.session_state["report_pdf"] = generate_report_bytes()
                status.update(
                    label=f"Report ready ({len(st.session_state['report_pdf']):,} bytes)",
                    state="complete",
                )
            except Exception as exc:  # noqa: BLE001 - surfaced to the user below
                st.session_state.pop("report_pdf", None)
                status.update(label="Report generation failed", state="error")
                st.error(str(exc))

    if st.session_state.get("report_pdf"):
        st.download_button(
            "Download report (PDF)",
            st.session_state["report_pdf"],
            file_name="microservice_failure_analysis.pdf",
            mime="application/pdf",
        )



# Pipeline

def _pipeline_tab(pal: Palette) -> None:
    section("Pipeline state", "Which stages have written their output table.")

    health = data.db_health()
    counts = health.get("tables", {})

    rows = []
    for label, table in MODULE_OUTPUTS:
        count = int(counts.get(table, 0))
        rows.append(
            {  "Stage": label,  "Output table": table,
                "Status": "complete" if count else "not run",     "Rows": count,
            }
        )
    st.dataframe(
        pd.DataFrame(rows),  use_container_width=True,
        hide_index=True,  column_config={"Rows": st.column_config.NumberColumn("Rows", format="%d")},
    )

    completed = sum(1 for r in rows if r["Rows"] > 0)
    st.progress(completed / len(rows), text=f"{completed} of {len(rows)} stages have output")

    section("Run a stage", "Runs in a subprocess; output streams below as it arrives.")

    choice = st.selectbox("Stage", list(PIPELINE_TARGETS), key="pipeline_target")
    col_run, col_note = st.columns([1, 3])
    with col_run:
        launch = st.button("Run stage", type="primary", use_container_width=True)
    with col_note:
        st.caption( "The full pipeline needs Spark, MinIO and PostgreSQL running. "
            "In SQLite demo mode these stages are not available."
        )

    if launch:
        _run_stage(choice)


def _run_stage(choice: str) -> None:
    """Run a pipeline stage, streaming its output into an expanding status block."""
    target = PIPELINE_TARGETS[choice]
    if target.endswith(".sh"):
        command = ["bash", str(PROJECT_ROOT / target)]
    else:
        command = [sys.executable, "-m", target]

    with st.status(f"Running {choice}", expanded=True) as status:
        output_slot = st.empty()
        collected: list[str] = []

        try:
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"

            process = subprocess.Popen(
                command,   stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  text=True,
                bufsize=1,   env=env,
                cwd=str(PROJECT_ROOT) )
        except FileNotFoundError:
            status.update(label=f"Could not launch {choice}", state="error")
            st.error(f"Command not found: {' '.join(command)}")
            return
        except OSError as exc:
            status.update(label=f"Could not launch {choice}", state="error")
            st.error(str(exc))
            return

        queue: Queue[str] = Queue()

        def pump(stream, sink: Queue) -> None:
            for line in iter(stream.readline, ""):
                sink.put(line)
            stream.close()

        reader = threading.Thread(target=pump, args=(process.stdout, queue), daemon=True)
        reader.start()

        while process.poll() is None or not queue.empty():
            try:
                collected.append(queue.get(timeout=0.1))
            except Empty:
                continue
            output_slot.code("".join(collected[-40:]), language="bash")

        reader.join(timeout=2)
        exit_code = process.wait()
        output_slot.code("".join(collected[-200:]) or "(no output)", language="bash")

        # New rows may exist now, so every cached read must be invalidated.
        st.cache_data.clear()

        if exit_code == 0:
            status.update(label=f"{choice} completed", state="complete")
        else:
            status.update(label=f"{choice} failed (exit {exit_code})", state="error")



# Spark cluster

def _fetch_spark(path: str, timeout: float = 4.0):
    """GET a Spark REST endpoint, returning parsed JSON or None if unreachable."""
    base = get_settings().spark.rest_api_base
    try:
        response = requests.get(f"{base}{path}", timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.RequestException:
        return None


def _cluster_tab(pal: Palette) -> None:
    settings = get_settings()
    section("Spark cluster", f"Live metrics from {settings.spark.rest_api_base}")

    if st.button("Refresh cluster metrics", key="spark_refresh"):
        st.rerun()

    applications = _fetch_spark("/applications")
    if applications is None:
        empty_state(
            "Spark master unreachable",
            f"No response from {settings.spark.rest_api_base}. This is expected in "
            "SQLite demo mode. Start the cluster with: docker compose up -d spark-master",
        )
        return

    if not isinstance(applications, list):
        empty_state("Unexpected response", "The Spark REST API returned an unrecognised payload.")
        return

    running = [a for a in applications if a.get("state") == "RUNNING"]
    finished = [a for a in applications if a.get("state") == "FINISHED"]

    stat_tiles(
        [
            {"label": "Applications", "value": str(len(applications)), "note": "known to the master"},
            {
                "label": "Running", "value": str(len(running)),
                "note": "active now","status": "good" if running else "neutral",
            },
            {"label": "Completed", "value": str(len(finished)), "note": "finished this session"},
            {
                "label": "Cores in use", "value": str(sum(a.get("cores", 0) for a in running)),
                "note": "across running applications",
            },
        ]
    )

    if not running:
        st.markdown(
            status_pill("neutral", "Idle")
            + " &nbsp;No application is running. Start a pipeline stage to see live job progress.",
            unsafe_allow_html=True,
        )
        return

    for app in running:
        app_id = app.get("id", "")
        with st.expander(f"{app.get('name', 'Application')} - {app_id}", expanded=True):
            _render_application(pal, app, app_id)


def _render_application(pal: Palette, app: dict, app_id: str) -> None:
    stat_tiles(
        [
            {   "label": "Runtime", "value": f"{app.get('duration', 0) / 1000:.0f}",
                "unit": "s",  "note": "since submission",
            },
            {"label": "Cores", "value": str(app.get("cores", "-")), "note": "allocated"},
            {
                "label": "Memory / executor",
                "value": str(app.get("memoryPerExecutorMB", "-")),
                "unit": "MB",  "note": "requested",
            },
            {"label": "Submitted by", "value": str(app.get("sparkUser", "-")), "note": "Spark user"},
        ]
    )

    jobs = _fetch_spark(f"/applications/{app_id}/jobs")
    if isinstance(jobs, list) and jobs:
        frame = pd.DataFrame(
            [
                {
                    "Job": job.get("jobId"),  "Status": job.get("status"),
                    "Tasks": f"{job.get('numCompletedTasks', 0)}/{job.get('numTasks', 0)}",
                    "Progress": (
                        job.get("numCompletedTasks", 0) / job["numTasks"] if job.get("numTasks") else 0.0
                    ),
                }
                for job in jobs[:12]
            ]
        )
        st.dataframe(
            frame,     use_container_width=True,
            hide_index=True, column_config={
                "Progress": st.column_config.ProgressColumn(
                    "Progress", min_value=0.0, max_value=1.0, format="%.0f%%"
                )
            },
        )

    executors = _fetch_spark(f"/applications/{app_id}/allexecutors")
    if isinstance(executors, list) and executors:
        frame = pd.DataFrame(
            [
                {
                    "Executor": ex.get("id"),  "Host": str(ex.get("hostPort", "")).split(":")[0],
                    "Cores": ex.get("totalCores", 0),   "Tasks": f"{ex.get('completedTasks', 0)}/{ex.get('totalTasks', 0)}",
                    "Memory used": bytes_human(ex.get("memoryUsed", 0))
                }
                for ex in executors
            ]
        )
        st.dataframe(frame, use_container_width=True, hide_index=True)



# Logs

def _log_path() -> Path:
    return Path(get_settings().app.log_file)


def _logs_tab(pal: Palette) -> None:
    path = _log_path()
    section("Pipeline log", f"Reading {path}")

    if not path.exists():
        empty_state(
            "No log file yet",  f"Nothing has been written to {path}. Run a pipeline stage from the "
            "Pipeline tab to produce output."
        )
        return

    try:
        stat = path.stat()
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        empty_state("Log file unreadable", str(exc))
        return

    truncated = len(lines) > MAX_LOG_LINES
    if truncated:
        lines = lines[-MAX_LOG_LINES:]

    controls = st.columns([2, 2, 4, 2])
    with controls[0]:
        min_level = st.selectbox("Minimum level", list(LEVEL_ORDER), index=1, key="log_level")
    with controls[1]:
        tail = st.selectbox("Show last", [100, 250, 500, 1000, MAX_LOG_LINES], index=1, key="log_tail")
    with controls[2]:
        search = st.text_input("Filter", placeholder="Substring to match", key="log_search")
    with controls[3]:
        st.download_button(
            "Download log",
            "\n".join(lines).encode("utf-8"),
            file_name="pipeline.log",
            mime="text/plain",
            use_container_width=True,
        )

    st.caption(
        f"{len(lines):,} lines - {bytes_human(stat.st_size)} - last written "
        f"{datetime.fromtimestamp(stat.st_mtime):%Y-%m-%d %H:%M:%S}"
        + ("  -  showing the most recent portion of a larger file" if truncated else "")
    )

    rendered = _render_log(pal, lines[-int(tail) :], min_level, search.strip())
    if not rendered:
        st.info(f"No lines at {min_level} or above" + (f' matching "{search}".' if search else "."))


def _render_log(pal: Palette, lines: list[str], min_level: str, search: str) -> int:
    """Render matching log lines as coloured monospace rows. Returns the count."""
    threshold = LEVEL_ORDER[min_level]
    level_colours = {
        "DEBUG": pal.ink_muted, "INFO": pal.ink_secondary,
        "WARNING": pal.warning,  "ERROR": pal.critical,
        "CRITICAL": pal.critical
    }

    rows: list[str] = []
    for line in lines:
        if search and search.lower() not in line.lower():
            continue

        match = LOG_LINE.match(line)
        if not match:
            # Unparsed output (Spark banners, tracebacks) is still worth
            # showing, just recessively.
            if threshold <= LEVEL_ORDER["INFO"]:
                rows.append(
                    f'<span style="color:{pal.ink_muted};opacity:0.65">' f"{html.escape(line)}</span>"
                )
            continue

        level = match.group("level").upper()
        level = {"WARN": "WARNING", "CRIT": "CRITICAL"}.get(level, level)
        if LEVEL_ORDER.get(level, 1) < threshold:
            continue

        weight = "font-weight:600;" if level in ("ERROR", "CRITICAL") else ""
        rows.append(
            f'<span style="color:{pal.ink_muted}">{match.group("ts")}</span> '
            f'<span style="color:{level_colours[level]};{weight}">{level:<8}</span>'
            f'<span style="color:{pal.ink_secondary}">{html.escape(match.group("msg"))}</span>'
        )

    if not rows:
        return 0

    st.markdown(
        f'<div style="background:{pal.page};border:1px solid {pal.border};'
        "border-radius:10px;padding:12px;max-height:60vh;overflow:auto;"
        "font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;"
        'line-height:1.65;white-space:pre-wrap;word-break:break-word">' + "<br>".join(rows) + "</div>",
        unsafe_allow_html=True,
    )
    return len(rows)



# Configuration

def _config_tab():
    settings = get_settings()

    section("Effective configuration", "Resolved from the environment. Secrets are never shown.")
    st.dataframe(
        pd.DataFrame([{"Setting": k, "Value": v} for k, v in settings.describe().items()]),
        use_container_width=True,
        hide_index=True,
    )

    problems = settings.problems()
    if problems:
        st.warning("Configuration problems:\n\n" + "\n".join(f"- {p}" for p in problems))
    else:
        st.markdown(
            status_pill("good", "Configuration valid") + " &nbsp;No problems detected.",
            unsafe_allow_html=True,
        )

    section("Database", "Backend, reachability, and row counts per table.")
    health = data.db_health()
    if not health.get("connected"):
        st.markdown(
            status_pill("critical", "Disconnected")
            + f" &nbsp;Could not reach {health.get('backend')} at {health.get('location')}",
            unsafe_allow_html=True
        )
        return

    st.markdown(
        status_pill("good", "Connected") + f" &nbsp;{health['backend']} - {health['location']} - "
        f"{compact_number(health['total_rows'])} rows total",
        unsafe_allow_html=True
    )
    tables = health.get("tables", {})
    if tables:
        st.dataframe(
            pd.DataFrame([{"Table": name, "Rows": count} for name, count in sorted(tables.items())]),
            use_container_width=True,  hide_index=True
        )
