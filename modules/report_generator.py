# ============================================================
# Report Generator - PDF Summary of All RQ Findings
# ============================================================
# Generates a publication-quality PDF report from the analysis results.
# Used by the dashboard's Operations page.
#
# Every query here is ANSI SQL that PostgreSQL and SQLite execute
# identically, and the connection comes from db_adapter - so the report
# works in the zero-dependency SQLite demo mode as well as against a
# populated PostgreSQL instance.
#
# Dependencies: fpdf2
# ============================================================

from datetime import datetime
from textwrap import shorten

from dotenv import load_dotenv
from fpdf import FPDF

from modules.db_adapter import get_connection
from modules.shared_utils import setup_logging

load_dotenv()
logger = setup_logging("report_generator")


# ============================================================
# Database Helpers
# ============================================================
def _get_conn():
    """Open a connection to whichever backend DB_MODE selects."""
    return get_connection()


def _fetch(conn, query: str, params=None) -> list[tuple]:
    """Run a query and return all rows."""
    # Closed explicitly rather than with `with`: sqlite3 cursors do not
    # implement the context-manager protocol, and this has to work on
    # both backends.
    cur = conn.cursor()
    try:
        cur.execute(query, params or ())
        return cur.fetchall()
    finally:
        cur.close()


def _fetch_one(conn, query: str, params=None) -> tuple | None:
    """Run a query and return the first row."""
    cur = conn.cursor()
    try:
        cur.execute(query, params or ())
        return cur.fetchone()
    finally:
        cur.close()


# ============================================================
# PDF Report Class
# ============================================================
# The built-in PDF fonts are Latin-1 only, so typographic characters and
# arrows abort rendering rather than degrading. Transliterate them to
# ASCII equivalents that carry the same meaning.
_ASCII_FALLBACKS = str.maketrans(
    {
        "—": "-",
        "–": "-",
        "‑": "-",
        "•": "-",
        "·": "-",
        "→": "->",
        "←": "<-",
        "↔": "<->",
        "⟶": "->",
        "≥": ">=",
        "≤": "<=",
        "≈": "~",
        "±": "+/-",
        "×": "x",
        "÷": "/",
        "✓": "[ok]",
        "✔": "[ok]",
        "✗": "[x]",
        "✘": "[x]",
        "'": "'",
        '"': '"',
        "…": "...",
        "°": "deg",
        "†": "+",
        "‰": "o/oo",
    }
)


class AnalysisReport(FPDF):
    """Custom PDF report with header/footer and structured sections."""

    def __init__(self):
        super().__init__(orientation="P", unit="mm", format="A4")
        self.set_auto_page_break(auto=True, margin=20)
        # Built-in fonts only - no TTF file has to ship with the image.
        self.set_title("K8s Microservice Failure Analysis - Research Report")

    def normalize_text(self, text):
        """
        Make every string safe for the Latin-1 built-in fonts.

        Applied at the single point all text passes through, so it covers
        both our own copy and values read from the database - a service
        name containing anything unusual degrades to '?' instead of
        failing the whole report.
        """
        if isinstance(text, str):
            text = text.translate(_ASCII_FALLBACKS)
            text = text.encode("latin-1", "replace").decode("latin-1")
        return super().normalize_text(text)

    # ----------------------------------------------------------
    # Header / Footer
    # ----------------------------------------------------------
    def header(self):
        if self.page_no() > 1:
            self.set_font("Helvetica", "I", 8)
            self.set_text_color(128, 128, 128)
            self.cell(0, 5, "Distributed Analysis of K8s Microservice Logs - Academic Report", align="C")
            self.ln(8)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    # ----------------------------------------------------------
    # Building Blocks
    # ----------------------------------------------------------
    def section_title(self, title: str):
        """Add a coloured section title bar."""
        self.set_fill_color(102, 126, 234)  # #667eea
        self.set_text_color(255, 255, 255)
        self.set_font("Helvetica", "B", 14)
        self.cell(0, 10, f"  {title}", fill=True, new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(0, 0, 0)
        self.ln(4)

    def sub_title(self, title: str):
        """Add a subsection title."""
        self.set_font("Helvetica", "B", 12)
        self.set_text_color(102, 126, 234)
        self.cell(0, 8, title, new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(0, 0, 0)
        self.ln(2)

    def body_text(self, text: str):
        """Add body text with word wrapping."""
        self.set_font("Helvetica", "", 10)
        self.multi_cell(0, 5, text)
        self.ln(2)

    def metric_row(self, metrics: list[tuple[str, str]], cols: int = 3):
        """Draw metric cards in a row. Each metric is (label, value)."""
        col_w = (self.w - self.l_margin - self.r_margin) / cols
        start_x = self.get_x()
        y = self.get_y()

        for i, (label, value) in enumerate(metrics):
            x = start_x + i * col_w
            self.set_xy(x, y)
            # Value
            self.set_font("Helvetica", "B", 14)
            self.set_text_color(102, 126, 234)
            self.cell(col_w, 8, value, align="C")
            # Label
            self.set_xy(x, y + 8)
            self.set_font("Helvetica", "", 8)
            self.set_text_color(128, 128, 128)
            self.cell(col_w, 5, label, align="C")

        self.set_text_color(0, 0, 0)
        self.set_y(y + 16)

    def simple_table(self, headers: list[str], rows: list[tuple], col_widths: list[float] = None):
        """Draw a simple table with header row and alternating row colours."""
        if not rows:
            self.body_text("(no data available)")
            return

        if col_widths is None:
            usable = self.w - self.l_margin - self.r_margin
            col_widths = [usable / len(headers)] * len(headers)

        # Header
        self.set_font("Helvetica", "B", 9)
        self.set_fill_color(102, 126, 234)
        self.set_text_color(255, 255, 255)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 7, f" {h}", border=0, fill=True)
        self.ln()

        # Rows
        self.set_text_color(0, 0, 0)
        for row_idx, row in enumerate(rows):
            if row_idx % 2 == 0:
                self.set_fill_color(245, 245, 250)
            else:
                self.set_fill_color(255, 255, 255)

            self.set_font("Helvetica", "", 9)
            max_h = 6
            for i, cell in enumerate(row):
                text = str(cell) if cell is not None else "-"
                text = shorten(text, width=30, placeholder="...")
                self.cell(col_widths[i], max_h, f" {text}", fill=True)
            self.ln()

        self.ln(3)

    # ----------------------------------------------------------
    # Cover Page
    # ----------------------------------------------------------
    def cover_page(self):
        """Render the report cover page."""
        self.add_page()
        self.ln(30)

        # Title
        self.set_font("Helvetica", "B", 26)
        self.set_text_color(102, 126, 234)
        self.multi_cell(
            0, 12, "Distributed Analysis of\nKubernetes Microservice Logs\nfor Failure Detection", align="C"
        )
        self.ln(8)

        # Subtitle
        self.set_font("Helvetica", "", 14)
        self.set_text_color(100, 100, 100)
        self.cell(0, 8, "Academic Research Report", align="C", new_x="LMARGIN", new_y="NEXT")
        self.ln(6)

        self.set_font("Helvetica", "", 11)
        self.set_text_color(128, 128, 128)
        self.cell(0, 6, "Module: Data Intensive & Scalable Systems", align="C", new_x="LMARGIN", new_y="NEXT")
        self.ln(20)

        # Divider
        self.set_draw_color(102, 126, 234)
        self.set_line_width(0.5)
        x_center = self.w / 2
        self.line(x_center - 30, self.get_y(), x_center + 30, self.get_y())
        self.ln(12)

        # Metadata
        self.set_text_color(0, 0, 0)
        self.set_font("Helvetica", "", 11)
        self.cell(
            0,
            7,
            f"Generated: {datetime.now().strftime('%B %d, %Y at %H:%M')}",
            align="C",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.cell(
            0,
            7,
            "Technology: Apache Spark 3.5 - MinIO - PostgreSQL - Python",
            align="C",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.ln(20)

        # Research Questions
        self.set_font("Helvetica", "B", 12)
        self.cell(0, 8, "Research Questions", align="C", new_x="LMARGIN", new_y="NEXT")
        self.ln(4)
        self.set_font("Helvetica", "", 10)
        rqs = [
            "RQ1: How can distributed analysis identify cross-service failure propagation?",
            "RQ2: How effectively can distributed processing identify abnormal failure patterns?",
            "RQ3: How does distributed Spark processing scale with increasing data volumes?",
        ]
        for rq in rqs:
            self.cell(0, 6, rq, align="C", new_x="LMARGIN", new_y="NEXT")


# ============================================================
# Report Builder - queries DB and populates PDF sections
# ============================================================
def generate_report_bytes() -> bytes:
    """
    Query all analysis results from PostgreSQL and build a PDF report.

    Returns:
        PDF file content as bytes (suitable for Streamlit download_button).
    """
    conn = _get_conn()
    try:
        pdf = AnalysisReport()
        pdf.alias_nb_pages()

        # ---- Cover Page ----
        pdf.cover_page()

        # ---- Executive Summary ----
        pdf.add_page()
        pdf.section_title("Executive Summary")

        # Key metrics
        proc_cnt = _fetch_one(conn, "SELECT COUNT(*) FROM processed_telemetry")
        anom_cnt = _fetch_one(conn, "SELECT COUNT(*) FROM anomaly_scores WHERE is_anomaly_overall = 1")
        prop_cnt = _fetch_one(conn, "SELECT COUNT(*) FROM cross_service_pairs WHERE propagation_score > 0")
        scale_cnt = _fetch_one(conn, "SELECT COUNT(DISTINCT data_size) FROM scalability_metrics")

        rec = proc_cnt[0] if proc_cnt else 0
        anom = anom_cnt[0] if anom_cnt else 0
        prop = prop_cnt[0] if prop_cnt else 0
        scal = scale_cnt[0] if scale_cnt else 0

        pdf.metric_row(
            [
                ("Records Processed", f"{rec:,}"),
                ("Anomalies Detected", f"{anom:,}"),
                ("Propagation Pairs", str(prop)),
                ("Scale Tests Run", str(scal)),
            ],
            cols=4,
        )

        pdf.ln(4)

        error_rate = _fetch_one(
            conn,
            """
            SELECT ROUND(100.0 * SUM(is_failure) / NULLIF(COUNT(*),0), 2)
            FROM processed_telemetry
        """,
        )
        anomaly_rate = _fetch_one(
            conn,
            """
            SELECT ROUND(100.0 * SUM(is_anomaly_overall) / NULLIF(COUNT(*),0), 2)
            FROM anomaly_scores
        """,
        )

        summary_text = (
            f"This report presents findings from a distributed analysis of Kubernetes "
            f"microservice telemetry using Apache Spark. The pipeline processed "
            f"{rec:,} records across 20 microservices, detecting {anom:,} multi-signal "
            f"anomalies and {prop} significant cross-service failure propagation pairs."
        )

        if error_rate and error_rate[0] is not None:
            summary_text += f" The overall error rate was {error_rate[0]}%."
        if anomaly_rate and anomaly_rate[0] is not None:
            summary_text += f" {anomaly_rate[0]}% of time buckets exhibited anomalous behaviour."

        pdf.body_text(summary_text)

        # ---- RQ1: Cross-Service Failure Propagation ----
        pdf.add_page()
        pdf.section_title("RQ1: Cross-Service Failure Propagation")
        pdf.body_text(
            "Research Question: How can distributed analysis identify cross-service "
            "failure propagation that is difficult to detect from individual microservice logs?"
        )

        # Top propagation pairs
        pdf.sub_title("Top Propagation Paths by Score")
        top_props = _fetch(
            conn,
            """
            SELECT caller_service, callee_service,
                   ROUND(propagation_score, 4) as score,
                   call_count, co_failure_count
            FROM cross_service_pairs
            WHERE propagation_score > 0
            ORDER BY propagation_score DESC
            LIMIT 10
        """,
        )
        pdf.simple_table(
            ["Caller Service", "Callee Service", "Score", "Calls", "Co-Failures"],
            top_props,
            [42, 42, 22, 22, 30],
        )

        # Propagation chains
        pdf.sub_title("Top Propagation Chains")
        chains = _fetch(
            conn,
            """
            SELECT source_service, target_service,
                   COUNT(*) as cnt,
                   ROUND(AVG(propagation_lag_sec), 2) as avg_lag
            FROM propagation_chains
            GROUP BY source_service, target_service
            ORDER BY cnt DESC LIMIT 8
        """,
        )
        pdf.simple_table(
            ["Source Service", "Target Service", "Chains", "Avg Lag (s)"],
            chains,
            [45, 45, 28, 30],
        )

        # Error correlations
        pdf.sub_title("Significant Error Correlations")
        corrs = _fetch(
            conn,
            """
            SELECT service_a, service_b,
                   ROUND(error_correlation, 4) as corr,
                   sample_size
            FROM error_correlations
            ORDER BY ABS(error_correlation) DESC
            LIMIT 8
        """,
        )
        pdf.simple_table(
            ["Service A", "Service B", "Correlation", "Sample Size"],
            corrs,
            [45, 45, 32, 30],
        )

        pdf.body_text(
            "Finding: Cross-service failure propagation was detected through "
            "parent-child span relationships in distributed traces. Services with "
            "high propagation scores indicate strong evidence of cascading failures."
        )

        # ---- RQ2: Abnormal Failure Pattern Detection ----
        pdf.add_page()
        pdf.section_title("RQ2: Abnormal Failure Pattern Detection")
        pdf.body_text(
            "Research Question: How effectively can distributed processing identify "
            "abnormal failure patterns across concurrent Kubernetes microservice workloads?"
        )

        # Anomaly summary
        anom_summary = _fetch_one(
            conn,
            """
            SELECT COUNT(*),
                   COALESCE(SUM(is_anomaly_overall), 0),
                   COALESCE(ROUND(100.0 * SUM(is_anomaly_overall) / NULLIF(COUNT(*), 0), 2), 0),
                   COALESCE(SUM(is_anomaly_error), 0),
                   COALESCE(SUM(is_anomaly_latency), 0),
                   COALESCE(SUM(is_anomaly_resource), 0)
            FROM anomaly_scores
        """,
        )

        if anom_summary:
            pdf.metric_row(
                [
                    ("Total Anomalies", f"{anom_summary[1]:,}"),
                    ("Anomaly Rate", f"{anom_summary[2]}%"),
                    ("Error Signal", f"{anom_summary[3]:,}"),
                    ("Latency Signal", f"{anom_summary[4]:,}"),
                ],
                cols=4,
            )

        pdf.ln(2)

        # Top anomalous services
        pdf.sub_title("Most Anomalous Services")
        top_svcs = _fetch(
            conn,
            """
            SELECT service_name, COUNT(*) as cnt
            FROM anomaly_scores WHERE is_anomaly_overall = 1
            GROUP BY service_name ORDER BY cnt DESC LIMIT 10
        """,
        )
        pdf.simple_table(
            ["Service", "Anomaly Count"],
            top_svcs,
            [80, 55],
        )

        # Failure patterns
        pdf.sub_title("Failure Pattern Distribution")
        patterns = _fetch(
            conn,
            """
            SELECT pattern_type,
                   SUM(occurrence_count) as total,
                   ROUND(AVG(avg_severity), 2) as avg_sev
            FROM failure_patterns
            GROUP BY pattern_type ORDER BY total DESC
        """,
        )
        pdf.simple_table(
            ["Pattern Type", "Occurrences", "Avg Severity"],
            patterns,
            [70, 40, 38],
        )

        pdf.body_text(
            "Finding: Z-score based anomaly detection (threshold=3.0) successfully "
            "identified abnormal patterns across error rates, latency, and resource "
            "usage dimensions. Multi-signal anomalies (2+ signals) provide the "
            "strongest evidence of genuine failure conditions."
        )

        # ---- RQ3: Spark Scalability ----
        pdf.add_page()
        pdf.section_title("RQ3: Spark Scalability Analysis")
        pdf.body_text(
            "Research Question: How does distributed Spark processing scale when "
            "analysing increasing volumes of Kubernetes microservice telemetry?"
        )

        # Scalability table
        scale_rows = _fetch(
            conn,
            """
            SELECT data_size,
                   ROUND(AVG(total_sec), 2) as avg_time,
                   ROUND(AVG(throughput_rows_per_sec), 0) as throughput,
                   ROUND(AVG(speedup_vs_baseline), 3) as speedup
            FROM scalability_metrics
            GROUP BY data_size ORDER BY data_size
        """,
        )

        if scale_rows:
            pdf.sub_title("Execution Time vs Data Size")
            pdf.simple_table(
                ["Data Size (rows)", "Avg Time (s)", "Throughput (rows/s)", "Speed-up"],
                scale_rows,
                [42, 36, 40, 36],
            )

            # Compute efficiency
            if len(scale_rows) > 1:
                baseline = scale_rows[0][1]
                baseline_rows = scale_rows[0][0]

                pdf.ln(2)
                pdf.sub_title("Scalability Efficiency")
                eff_rows = []
                for row in scale_rows:
                    ratio = row[0] / baseline_rows
                    speedup = baseline / row[1] if row[1] > 0 else 0
                    efficiency = speedup / ratio if ratio > 0 else 0
                    eff_rows.append((f"{row[0]:,}", f"{ratio:.0f}x", f"{speedup:.3f}", f"{efficiency:.1%}"))
                pdf.simple_table(
                    ["Data Size", "Ratio", "Speed-up", "Efficiency"],
                    eff_rows,
                    [42, 30, 36, 36],
                )

            # Operation breakdown
            op_rows = _fetch(
                conn,
                """
                SELECT data_size,
                       ROUND(AVG(groupby_agg_sec), 2),
                       ROUND(AVG(window_fn_sec), 2),
                       ROUND(AVG(join_sec), 2),
                       ROUND(AVG(shuffle_sec), 2)
                FROM scalability_metrics
                GROUP BY data_size ORDER BY data_size
                LIMIT 5
            """,
            )
            if op_rows:
                pdf.sub_title("Operation-Level Breakdown (seconds)")
                pdf.simple_table(
                    ["Data Size", "GroupBy", "Window", "Join", "Shuffle"],
                    op_rows,
                    [30, 28, 28, 28, 28],
                )

        # Scaling characteristic
        char_row = _fetch_one(
            conn,
            """
            SELECT ROUND(AVG(speedup_vs_baseline), 3)
            FROM scalability_metrics
            WHERE data_size = (SELECT MAX(data_size) FROM scalability_metrics)
        """,
        )
        pdf.ln(2)
        char_text = "Finding: The Spark pipeline demonstrated scalable distributed processing behaviour. "
        if char_row and char_row[0]:
            char_text += f"At the largest data size, the measured speed-up factor was {char_row[0]}x."
        pdf.body_text(char_text)

        # ---- Methodology ----
        pdf.add_page()
        pdf.section_title("Methodology & Architecture")
        pdf.body_text(
            "The analysis pipeline follows a six-module architecture:\n\n"
            "  1. Data Ingestion: CSV telemetry uploaded to MinIO blob storage\n"
            "  2. Preprocessing: Spark ETL with cleaning, joining, and feature engineering\n"
            "  3. Cross-Service Analysis (RQ1): Self-join on span relationships\n"
            "  4. Failure Detection (RQ2): Z-score anomaly detection across 3 signal types\n"
            "  5. Scalability Analysis (RQ3): Timed benchmarks across data sizes\n"
            "  6. Visualization: Publication-quality plots from PostgreSQL results\n\n"
            "Technology Stack: Apache Spark 3.5, MinIO (S3-compatible), PostgreSQL 16, "
            "Python 3.11, Docker Compose orchestration."
        )

        pdf.body_text(
            "Cluster Configuration: 4 Spark worker nodes, 2 cores each, "
            "2 GB executor/driver memory, 8 shuffle partitions. "
            "Dataset: 1M rows of synthetic microservice telemetry across 20 services."
        )

        # ---- Key Findings Summary ----
        pdf.section_title("Key Findings")
        findings = [
            "RQ1: Cross-service failure propagation was quantitatively measured through "
            "propagation scores derived from parent-child span relationships. "
            "Services with high propagation scores represent critical failure cascades.",
            "RQ2: Z-score anomaly detection (threshold 3sigma) effectively identified abnormal "
            "behaviour across error rate, latency, and resource dimensions. "
            "Multi-signal anomalies (2+ signals) provide the strongest failure indicators.",
            "RQ3: Spark demonstrated scalable processing across data volumes from "
            "100K to 1M+ rows. Throughput and execution time scaling curves provide "
            "quantitative evidence of distributed processing efficiency.",
        ]
        for f_text in findings:
            pdf.set_font("Helvetica", "", 10)
            pdf.set_fill_color(245, 245, 250)
            pdf.multi_cell(0, 6, f"  -  {f_text}", fill=True)
            pdf.ln(2)

        # Generate PDF bytes
        return pdf.output()

    finally:
        conn.close()


# ============================================================
# Standalone entry point (for testing)
# ============================================================
if __name__ == "__main__":
    from modules.settings import get_settings

    pdf_bytes = generate_report_bytes()
    output_path = get_settings().app.output_dir / "analysis_report.pdf"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(pdf_bytes)
    logger.info(f"Report written to {output_path} ({len(pdf_bytes):,} bytes)")
