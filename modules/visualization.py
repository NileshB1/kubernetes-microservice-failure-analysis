# ============================================================
# Module 6: Results & Visualization
# ============================================================
# Purpose:
#   Reads analysis results from PostgreSQL and produces
#   publication-quality visualizations to support findings
#   for all three research questions.
#
# Inputs:
#   - PostgreSQL tables: anomaly_scores, cross_service_pairs,
#     propagation_chains, scalability_metrics
#   - config.yaml visualization parameters
#
# Outputs:
#   - PNG plots in /output/ directory
#     - rq1_propagation_heatmap.png
#     - rq1_propagation_sankey.png
#     - rq2_anomaly_timeseries.png
#     - rq2_failure_patterns.png
#     - rq3_scaling_curves.png
#     - rq3_efficiency.png
#     - dashboard_summary.png
#
# Main Functions:
#   - plot_propagation_heatmap()        → Service×service propagation matrix
#   - plot_failure_propagation_sankey()  → Propagation flow diagram
#   - plot_anomaly_timeseries()          → Error rate + anomaly overlay
#   - plot_failure_pattern_distribution() → Pattern type breakdown
#   - plot_scaling_curves()              → Data size vs execution time
#   - plot_scalability_efficiency()      → Speed-up & efficiency curves
#   - generate_dashboard()               → Combined summary figure
#
# RQ Contribution:
#   - RQ1: Propagation visualizations
#   - RQ2: Anomaly detection evidence
#   - RQ3: Scalability curves
# ============================================================

import os
import logging
from typing import Optional

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend for Docker
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import yaml
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

from modules.shared_utils import setup_logging

load_dotenv()
logger = setup_logging("visualization")


def load_config(config_path: str = "config/config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_db_connection():
    """Create a PostgreSQL connection."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "sparkuser"),
        password=os.getenv("POSTGRES_PASSWORD", "sparkpass"),
        dbname=os.getenv("POSTGRES_DB", "microservice_analysis"),
    )


# ============================================================
# Plot Settings
# ============================================================
def setup_plot_style():
    """Apply consistent plot styling."""
    sns.set_style("whitegrid")
    plt.rcParams.update({
        "figure.dpi": 150,
        "font.size": 12,
        "axes.titlesize": 16,
        "axes.labelsize": 13,
        "figure.figsize": (12, 8),
    })


# ============================================================
# RQ1: Propagation Heatmap
# ============================================================
def plot_propagation_heatmap(output_dir: str = "/output") -> str:
    """
    Create a heatmap of cross-service failure propagation scores.
    X-axis = caller service, Y-axis = callee service.
    """
    logger.info("Plotting RQ1: Cross-service propagation heatmap...")
    setup_plot_style()

    conn = get_db_connection()
    try:
        query = """
            SELECT caller_service, callee_service, propagation_score
            FROM cross_service_pairs
            WHERE propagation_score > 0
            ORDER BY propagation_score DESC
        """
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    if df.empty:
        logger.warning("No cross-service data for heatmap. Generating placeholder.")
        return _save_empty_plot("rq1_propagation_heatmap", "No propagation data", output_dir)

    # Pivot to matrix form
    pivot = df.pivot_table(
        index="callee_service",
        columns="caller_service",
        values="propagation_score",
        aggfunc="mean",
        fill_value=0,
    )

    fig, ax = plt.subplots(figsize=(14, 10))
    sns.heatmap(
        pivot,
        annot=True,
        fmt=".2f",
        cmap="YlOrRd",
        cbar_kws={"label": "Propagation Score"},
        ax=ax,
        linewidths=0.5,
    )
    ax.set_title("RQ1: Cross-Service Failure Propagation Heatmap", fontweight="bold")
    ax.set_xlabel("Caller Service")
    ax.set_ylabel("Callee Service")
    plt.xticks(rotation=45, ha="right")
    plt.yticks(rotation=0)
    plt.tight_layout()

    path = os.path.join(output_dir, "rq1_propagation_heatmap.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"  ✓ Saved: {path}")
    return path


# ============================================================
# RQ2: Anomaly Time Series
# ============================================================
def plot_anomaly_timeseries(output_dir: str = "/output") -> str:
    """
    Plot error rate over time for top services with anomaly highlights.
    """
    logger.info("Plotting RQ2: Anomaly time series...")
    setup_plot_style()

    conn = get_db_connection()
    try:
        # Get top 4 most anomalous services
        top_services = pd.read_sql("""
            SELECT service_name, COUNT(*) AS anomaly_count
            FROM anomaly_scores
            WHERE is_anomaly_overall = 1
            GROUP BY service_name
            ORDER BY anomaly_count DESC
            LIMIT 4
        """, conn)

        if top_services.empty:
            logger.warning("No anomaly data. Generating placeholder.")
            return _save_empty_plot("rq2_anomaly_timeseries", "No anomaly data", output_dir)

        service_list = top_services["service_name"].tolist()
        placeholders = ", ".join([f"'{s}'" for s in service_list])

        anomaly_data = pd.read_sql(f"""
            SELECT service_name, time_bucket, is_anomaly_overall, anomaly_score
            FROM anomaly_scores
            WHERE service_name IN ({placeholders})
            ORDER BY time_bucket
        """, conn)
    finally:
        conn.close()

    # Parse time buckets
    anomaly_data["time"] = pd.to_datetime(anomaly_data["time_bucket"])
    anomaly_data = anomaly_data.sort_values("time")

    fig, axes = plt.subplots(2, 2, figsize=(16, 12), sharex=True)
    axes = axes.flatten()

    for i, service in enumerate(service_list):
        ax = axes[i]
        svc_data = anomaly_data[anomaly_data["service_name"] == service]

        normal = svc_data[svc_data["is_anomaly_overall"] == 0]
        anomalous = svc_data[svc_data["is_anomaly_overall"] == 1]

        ax.scatter(normal["time"], [0] * len(normal), c="green", alpha=0.3, s=10, label="Normal")
        ax.scatter(anomalous["time"], anomalous["anomaly_score"], c="red", alpha=0.7, s=30, label="Anomaly")
        ax.set_title(f"{service}")
        ax.set_ylabel("Anomaly Score")
        ax.legend(loc="upper right", fontsize=9)
        ax.grid(True, alpha=0.3)

    fig.suptitle("RQ2: Anomaly Detection Time Series — Top 4 Services", fontweight="bold", fontsize=18)
    fig.supxlabel("Time Bucket")
    plt.tight_layout()

    path = os.path.join(output_dir, "rq2_anomaly_timeseries.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"  ✓ Saved: {path}")
    return path


# ============================================================
# RQ2: Failure Pattern Distribution
# ============================================================
def plot_failure_pattern_distribution(output_dir: str = "/output") -> str:
    """Bar chart of failure pattern types across services."""
    logger.info("Plotting RQ2: Failure pattern distribution...")
    setup_plot_style()

    conn = get_db_connection()
    try:
        df = pd.read_sql("""
            SELECT pattern_type, SUM(occurrence_count) AS total_occurrences
            FROM failure_patterns
            GROUP BY pattern_type
            ORDER BY total_occurrences DESC
        """, conn)
    finally:
        conn.close()

    if df.empty:
        return _save_empty_plot("rq2_failure_patterns", "No pattern data", output_dir)

    fig, ax = plt.subplots(figsize=(12, 6))
    colors = sns.color_palette("Set2", len(df))
    bars = ax.barh(df["pattern_type"], df["total_occurrences"], color=colors, edgecolor="white")

    for bar, val in zip(bars, df["total_occurrences"]):
        ax.text(bar.get_width() + max(df["total_occurrences"]) * 0.01,
                bar.get_y() + bar.get_height() / 2,
                f"{int(val):,}", va="center", fontsize=10)

    ax.set_title("RQ2: Failure Pattern Type Distribution", fontweight="bold")
    ax.set_xlabel("Total Occurrences")
    ax.set_ylabel("Pattern Type")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    plt.tight_layout()

    path = os.path.join(output_dir, "rq2_failure_patterns.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"  ✓ Saved: {path}")
    return path


# ============================================================
# RQ3: Scaling Curves
# ============================================================
def plot_scaling_curves(output_dir: str = "/output") -> str:
    """
    Plot execution time vs data size (log-log) and throughput curves.
    """
    logger.info("Plotting RQ3: Scaling curves...")
    setup_plot_style()

    conn = get_db_connection()
    try:
        df = pd.read_sql("""
            SELECT data_size, total_sec, throughput_rows_per_sec, speedup_vs_baseline
            FROM scalability_metrics
            ORDER BY data_size
        """, conn)
    finally:
        conn.close()

    if df.empty:
        return _save_empty_plot("rq3_scaling_curves", "No scalability data", output_dir)

    # Aggregate
    agg = df.groupby("data_size").agg({
        "total_sec": ["mean", "std"],
        "throughput_rows_per_sec": "mean",
    }).reset_index()
    agg.columns = ["data_size", "avg_time", "std_time", "avg_throughput"]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    # Plot 1: Execution time vs data size (log-log)
    ax1.errorbar(agg["data_size"], agg["avg_time"], yerr=agg["std_time"],
                 fmt="o-", capsize=5, markersize=8, linewidth=2.5, color="#2c7bb6")
    ax1.set_xscale("log")
    ax1.set_yscale("log")
    ax1.set_xlabel("Data Size (rows)")
    ax1.set_ylabel("Execution Time (seconds)")
    ax1.set_title("Execution Time vs Data Size (log-log)")
    ax1.grid(True, alpha=0.3, which="both")
    for _, row in agg.iterrows():
        ax1.annotate(f"{row['avg_time']:.2f}s",
                     (row["data_size"], row["avg_time"]),
                     textcoords="offset points", xytext=(0, 10),
                     fontsize=9, ha="center")

    # Plot 2: Throughput vs data size
    ax2.plot(agg["data_size"], agg["avg_throughput"], "o-", markersize=8,
             linewidth=2.5, color="#d7191c")
    ax2.set_xscale("log")
    ax2.set_xlabel("Data Size (rows)")
    ax2.set_ylabel("Throughput (rows/sec)")
    ax2.set_title("Throughput vs Data Size")
    ax2.grid(True, alpha=0.3, which="both")
    for _, row in agg.iterrows():
        ax2.annotate(f"{row['avg_throughput']:,.0f}",
                     (row["data_size"], row["avg_throughput"]),
                     textcoords="offset points", xytext=(0, 10),
                     fontsize=9, ha="center")

    fig.suptitle("RQ3: Spark Scalability Analysis", fontweight="bold", fontsize=16)
    plt.tight_layout()

    path = os.path.join(output_dir, "rq3_scaling_curves.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"  ✓ Saved: {path}")
    return path


# ============================================================
# RQ3: Efficiency Plot
# ============================================================
def plot_scalability_efficiency(output_dir: str = "/output") -> str:
    """Plot speed-up and scalability efficiency curves."""
    logger.info("Plotting RQ3: Scalability efficiency...")
    setup_plot_style()

    conn = get_db_connection()
    try:
        df = pd.read_sql("""
            SELECT data_size, speedup_vs_baseline, data_size AS size_ratio
            FROM scalability_metrics
            ORDER BY data_size
        """, conn)
    finally:
        conn.close()

    if df.empty:
        return _save_empty_plot("rq3_efficiency", "No data", output_dir)

    agg = df.groupby("data_size")["speedup_vs_baseline"].mean().reset_index()
    sizes = agg["data_size"].values
    baseline = sizes[0]
    data_ratios = sizes / baseline
    speedups = agg["speedup_vs_baseline"].values
    efficiencies = speedups / data_ratios

    fig, ax1 = plt.subplots(figsize=(10, 6))

    ax1.plot(data_ratios, speedups, "o-", color="#2c7bb6", linewidth=2.5,
             markersize=10, label="Actual Speed-up")
    ax1.plot(data_ratios, data_ratios, "--", color="gray", linewidth=1.5,
             label="Ideal Linear Speed-up")
    ax1.set_xlabel("Data Size Ratio (× baseline)")
    ax1.set_ylabel("Speed-up Factor", color="#2c7bb6")
    ax1.tick_params(axis="y", labelcolor="#2c7bb6")

    ax2 = ax1.twinx()
    ax2.plot(data_ratios, efficiencies, "s--", color="#d7191c", linewidth=2,
             markersize=10, label="Efficiency")
    ax2.set_ylabel("Scalability Efficiency", color="#d7191c")
    ax2.set_ylim(0, 1.2)
    ax2.tick_params(axis="y", labelcolor="#d7191c")
    ax2.axhline(y=1.0, color="gray", linestyle=":", alpha=0.5)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    ax1.set_title("RQ3: Speed-up & Scalability Efficiency", fontweight="bold")
    ax1.grid(True, alpha=0.3)
    plt.tight_layout()

    path = os.path.join(output_dir, "rq3_scalability_efficiency.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"  ✓ Saved: {path}")
    return path


# ============================================================
# Combined Dashboard
# ============================================================
def generate_dashboard(output_dir: str = "/output") -> str:
    """Generate a combined summary dashboard of all key findings."""
    logger.info("Generating summary dashboard...")
    setup_plot_style()

    fig = plt.figure(figsize=(20, 16))
    fig.suptitle("Distributed Analysis of Kubernetes Microservice Logs — Summary Dashboard",
                 fontweight="bold", fontsize=20, y=1.01)

    # Collect findings from all RQs
    conn = get_db_connection()
    try:
        # RQ1: Top propagation pairs
        prop_df = pd.read_sql("""
            SELECT caller_service, callee_service, propagation_score
            FROM cross_service_pairs
            ORDER BY propagation_score DESC LIMIT 5
        """, conn)

        # RQ2: Anomaly summary
        anomaly_summary = pd.read_sql("""
            SELECT
                COUNT(*) AS total_buckets,
                SUM(is_anomaly_overall) AS total_anomalies
            FROM anomaly_scores
        """, conn)

        # RQ3: Scaling summary
        scale_summary = pd.read_sql("""
            SELECT data_size, AVG(total_sec) AS avg_time_sec
            FROM scalability_metrics
            GROUP BY data_size ORDER BY data_size
        """, conn)
    finally:
        conn.close()

    # --- Subplot 1: RQ1 Propagation Summary ---
    ax1 = fig.add_subplot(2, 3, 1)
    if not prop_df.empty:
        ax1.barh(
            prop_df["caller_service"] + " → " + prop_df["callee_service"],
            prop_df["propagation_score"],
            color=sns.color_palette("YlOrRd", len(prop_df)),
        )
    ax1.set_title("RQ1: Top Propagation Paths")
    ax1.set_xlabel("Propagation Score")

    # --- Subplot 2: RQ2 Anomaly Rate ---
    ax2 = fig.add_subplot(2, 3, 2)
    if not anomaly_summary.empty:
        total = anomaly_summary["total_buckets"].iloc[0]
        anomalies = anomaly_summary["total_anomalies"].iloc[0]
        normal = total - anomalies
        ax2.pie([normal, anomalies], labels=["Normal", "Anomalous"],
                autopct="%1.1f%%", colors=["#66c2a5", "#fc8d62"],
                explode=(0, 0.05))
    ax2.set_title("RQ2: Anomaly Detection Summary")

    # --- Subplot 3: RQ3 Scaling ---
    ax3 = fig.add_subplot(2, 3, 3)
    if not scale_summary.empty:
        ax3.plot(scale_summary["data_size"], scale_summary["avg_time_sec"],
                 "o-", linewidth=2.5, markersize=8)
        ax3.set_xscale("log")
        ax3.set_ylabel("Time (s)")
        ax3.set_title("RQ3: Execution Time Scaling")

    # --- Subplot 4: Failure Patterns ---
    ax4 = fig.add_subplot(2, 3, 4)
    try:
        conn = get_db_connection()
        pattern_df = pd.read_sql("""
            SELECT pattern_type, SUM(occurrence_count) AS total
            FROM failure_patterns
            GROUP BY pattern_type ORDER BY total DESC
        """, conn)
        conn.close()
        if not pattern_df.empty:
            ax4.bar(pattern_df["pattern_type"], pattern_df["total"],
                    color=sns.color_palette("Set2", len(pattern_df)))
            ax4.set_title("Failure Pattern Distribution")
            ax4.tick_params(axis="x", rotation=30)
    except Exception:
        ax4.text(0.5, 0.5, "No pattern data", ha="center", va="center")

    # --- Subplot 5: Key Metrics Text ---
    ax5 = fig.add_subplot(2, 3, 5)
    ax5.axis("off")
    metrics_text = "KEY METRICS\n\n"
    if not scale_summary.empty:
        baseline = scale_summary["avg_time_sec"].iloc[0]
        largest = scale_summary["avg_time_sec"].iloc[-1]
        speedup = baseline / largest if largest > 0 else 0
        metrics_text += f"Data Range: {scale_summary['data_size'].iloc[0]:,} → {scale_summary['data_size'].iloc[-1]:,} rows\n"
        metrics_text += f"Time Range: {baseline:.2f}s → {largest:.2f}s\n"
        metrics_text += f"Speed-up: {speedup:.2f}×\n\n"
    if not anomaly_summary.empty:
        rate = 100 * anomalies / max(total, 1)
        metrics_text += f"Anomaly Rate: {rate:.2f}%\n"
        metrics_text += f"Total Anomalies: {anomalies:,}\n"
    ax5.text(0.1, 0.9, metrics_text, transform=ax5.transAxes,
             fontsize=13, fontfamily="monospace", verticalalignment="top")

    # --- Subplot 6: Architecture ---
    ax6 = fig.add_subplot(2, 3, 6)
    ax6.axis("off")
    arch_text = (
        "ARCHITECTURE\n\n"
        "Dataset → MinIO (Blob)\n"
        "    ↓\n"
        "PySpark Cluster\n"
        "  ├─ Spark Master\n"
        "  ├─ Spark Workers (×N)\n"
        "    ↓\n"
        "Preprocessing → Analysis\n"
        "    ↓\n"
        "PostgreSQL (Results)\n"
        "    ↓\n"
        "Visualization (Matplotlib)"
    )
    ax6.text(0.1, 0.9, arch_text, transform=ax6.transAxes,
             fontsize=11, fontfamily="monospace", verticalalignment="top")

    plt.tight_layout()
    path = os.path.join(output_dir, "dashboard_summary.png")
    fig.savefig(path, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    logger.info(f"  ✓ Saved: {path}")
    return path


# ============================================================
# Helpers
# ============================================================
def _save_empty_plot(name: str, message: str, output_dir: str) -> str:
    """Generate a placeholder plot when no data is available."""
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.text(0.5, 0.5, f"{message}\n({name})", ha="center", va="center",
            fontsize=14, transform=ax.transAxes)
    ax.set_title(name.replace("_", " ").title())
    ax.axis("off")
    path = os.path.join(output_dir, f"{name}.png")
    fig.savefig(path, dpi=100, bbox_inches="tight")
    plt.close(fig)
    return path


# ============================================================
# Main Entry Point
# ============================================================
def run_visualization(output_dir: str = "/output") -> list[str]:
    """
    Generate all plots for the three research questions.

    Returns list of generated file paths.
    """
    logger.info("=" * 60)
    logger.info("MODULE 6: RESULTS & VISUALIZATION")
    logger.info("=" * 60)

    os.makedirs(output_dir, exist_ok=True)
    paths = []

    # RQ1 plots
    paths.append(plot_propagation_heatmap(output_dir))

    # RQ2 plots
    paths.append(plot_anomaly_timeseries(output_dir))
    paths.append(plot_failure_pattern_distribution(output_dir))

    # RQ3 plots
    paths.append(plot_scaling_curves(output_dir))
    paths.append(plot_scalability_efficiency(output_dir))

    # Dashboard
    paths.append(generate_dashboard(output_dir))

    logger.info(f"\nAll {len(paths)} plots generated in {output_dir}:")
    for p in paths:
        logger.info(f"  {p}")

    return paths


if __name__ == "__main__":
    run_visualization()
