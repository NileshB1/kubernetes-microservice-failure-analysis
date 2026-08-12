
# Placeholder Plot Generator

import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

import numpy as np

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

plt.rcParams.update({"figure.dpi": 120, "font.size": 12})


def _save(fig, name):
    path = os.path.join(OUTPUT_DIR, name)
    fig.savefig(path, dpi=120, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  [OK] {path}")


def make_rq1_propagation_heatmap():
    """Placeholder: cross-service propagation heatmap."""
    services = [
        "frontend", "auth-svc", "user-svc", "order-svc",
        "payment-svc", "inventory-svc",  "notif-svc",   "shipping-svc",
        "catalog-svc", "cart-svc",
    ]
    n = len(services)
    np.random.seed(42)
    data = np.random.rand(n, n) * 0.4
    np.fill_diagonal(data, 0)

    fig, ax = plt.subplots(figsize=(12, 9))
    im = ax.imshow(data, cmap="YlOrRd", aspect="auto", vmin=0, vmax=0.4)
    ax.set_xticks(range(n))
    ax.set_xticklabels(services, rotation=45, ha="right", fontsize=9)
    ax.set_yticks(range(n))
    ax.set_yticklabels(services, fontsize=9)
    ax.set_title("RQ1: Cross-Service Failure Propagation Heatmap", fontweight="bold", fontsize=14)
    ax.set_xlabel("Caller Service")
    ax.set_ylabel("Callee Service")
    for i in range(n):
        for j in range(n):
            if data[i, j] > 0.05:
                ax.text(j, i, f"{data[i,j]:.2f}", ha="center", va="center", fontsize=7)
    plt.colorbar(im, ax=ax, label="Propagation Score")
    fig.tight_layout()
    _save(fig, "rq1_propagation_heatmap.png")


def make_rq2_anomaly_timeseries():
    """Placeholder: anomaly time series for top services."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    services = ["auth-service", "frontend", "payment-service", "user-service"]
    np.random.seed(7)
    t = np.arange(24)

    for ax, svc in zip(axes.flatten(), services, strict=False):
        normal = np.random.normal(0.05, 0.02, 24)
        anomaly_idx = np.random.choice(24, size=3, replace=False)
        for idx in anomaly_idx:
            normal[idx] += np.random.uniform(0.15, 0.25)
        ax.plot(t, normal, "o-", color="#2c7bb6", markersize=6, alpha=0.7, label="Error Rate")
        ax.scatter(t[anomaly_idx], normal[anomaly_idx], color="red", s=80, zorder=5, label="Anomaly")
        ax.set_title(svc, fontsize=11)
        ax.set_ylabel("Error Rate")
        ax.legend(fontsize=8, loc="upper right")
        ax.grid(True, alpha=0.3)
    fig.suptitle("RQ2: Anomaly Detection Time Series", fontweight="bold", fontsize=16)
    fig.tight_layout()
    _save(fig, "rq2_anomaly_timeseries.png")


def make_rq2_failure_patterns():
    """Placeholder: failure pattern distribution bar chart."""
    patterns = [
        "cascading_failure",  "error_surge",
        "latency_spike",  "resource_pressure",
        "full_failure",  "resource_exhaustion",
    ]
    counts = [145, 98, 72, 53, 28, 19]
    colors = ["#fc8d62", "#66c2a5", "#8da0cb", "#e78ac3", "#a6d854", "#ffd92f"]

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.barh(patterns, counts, color=colors, edgecolor="white", height=0.6)
    for bar, val in zip(bars, counts, strict=False):
        ax.text(bar.get_width() + 2, bar.get_y() + bar.get_height() / 2, str(val), va="center", fontsize=10)
    ax.set_title("RQ2: Failure Pattern Type Distribution", fontweight="bold", fontsize=14)
    ax.set_xlabel("Occurrences")
    ax.invert_yaxis()
    fig.tight_layout()
    _save(fig, "rq2_failure_patterns.png")


def make_rq3_scaling_curves():
    """Placeholder: execution time and throughput scaling."""
    sizes = [1e5, 5e5, 1e6, 5e6, 1e7]
    times = [2.3, 9.8, 21.5, 98.0, 210.0]
    throughput = [s / t for s, t in zip(sizes, times, strict=False)]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    ax1.loglog(sizes, times, "o-", color="#2c7bb6", linewidth=2.5, markersize=8)
    ax1.set_xlabel("Data Size (rows)")
    ax1.set_ylabel("Time (s)")
    ax1.set_title("Execution Time vs Data Size")
    ax1.grid(True, alpha=0.3, which="both")
    for s, t in zip(sizes, times, strict=False):
        ax1.annotate(f"{t}s", (s, t), textcoords="offset points", xytext=(0, 10), fontsize=9)

    ax2.semilogx(sizes, throughput, "o-", color="#d7191c", linewidth=2.5, markersize=8)
    ax2.set_xlabel("Data Size (rows)")
    ax2.set_ylabel("Throughput (rows/s)")
    ax2.set_title("Throughput Scaling")
    ax2.grid(True, alpha=0.3, which="both")
    for s, tp in zip(sizes, throughput, strict=False):
        ax2.annotate(f"{tp:,.0f}", (s, tp), textcoords="offset points", xytext=(0, 10), fontsize=9)

    fig.suptitle("RQ3: Spark Scalability Analysis", fontweight="bold", fontsize=14)
    fig.tight_layout()
    _save(fig, "rq3_scaling_curves.png")


def make_rq3_efficiency():
    """Placeholder: speed-up and efficiency curves."""
    ratios = np.array([1, 5, 10, 50, 100])
    speedup = np.array([1.0, 4.2, 7.5, 18.0, 28.0])
    efficiency = speedup / ratios

    fig, ax1 = plt.subplots(figsize=(9, 5))
    ax1.plot(ratios, speedup, "o-", color="#2c7bb6", linewidth=2.5, markersize=10, label="Actual Speed-up")
    ax1.plot(ratios, ratios, "--", color="gray", linewidth=1.5, label="Ideal Linear")
    ax1.set_xlabel("Data Size Ratio (x baseline)")
    ax1.set_ylabel("Speed-up", color="#2c7bb6")
    ax1.tick_params(axis="y", labelcolor="#2c7bb6")

    ax2 = ax1.twinx()
    ax2.plot(ratios, efficiency, "s--", color="#d7191c", linewidth=2, markersize=10, label="Efficiency")
    ax2.set_ylabel("Efficiency", color="#d7191c")
    ax2.set_ylim(0, 1.2)
    ax2.tick_params(axis="y", labelcolor="#d7191c")
    ax2.axhline(y=1.0, color="gray", linestyle=":", alpha=0.5)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    ax1.set_title("RQ3: Speed-up & Scalability Efficiency", fontweight="bold", fontsize=14)
    ax1.grid(True, alpha=0.3)
    fig.tight_layout()
    _save(fig, "rq3_scalability_efficiency.png")


def make_dashboard_summary():
    """Placeholder: combined summary dashboard."""
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle("Dashboard Summary - Placeholder", fontweight="bold", fontsize=18)

    # Subplot 1
    ax = axes[0, 0]
    ax.barh(
        ["frontend->auth", "auth->user", "user->payment", "payment->inventory"],
        [0.42, 0.35, 0.28, 0.19],
        color=plt.cm.YlOrRd([0.9, 0.7, 0.5, 0.3]),
    )
    ax.set_title("RQ1: Propagation Paths")

    # Subplot 2
    ax = axes[0, 1]
    ax.pie(
        [82, 18], labels=["Normal", "Anomalous"],
        autopct="%1.1f%%",  colors=["#66c2a5", "#fc8d62"],   explode=(0, 0.05) )
    ax.set_title("RQ2: Anomaly Rate")

    # Subplot 3
    ax = axes[0, 2]
    sizes = [1e5, 5e5, 1e6, 5e6, 1e7]
    times = [2.3, 9.8, 21.5, 98.0, 210.0]
    ax.loglog(sizes, times, "o-", linewidth=2)
    ax.set_title("RQ3: Scaling")
    ax.set_xlabel("Rows")

    # Subplot 4
    ax = axes[1, 0]
    patterns = ["cascading", "error", "latency", "resource", "full"]
    ax.bar(patterns, [145, 98, 72, 53, 28], color=plt.cm.Set2.colors)
    ax.set_title("Failure Patterns")
    ax.tick_params(axis="x", rotation=30)

    # Subplot 5
    ax = axes[1, 1]
    ax.axis("off")
    ax.text(
        0.5, 0.5,
        "Pipeline Ready\n\nRun the pipeline to see\nlive results.",
        ha="center", va="center", fontsize=14, transform=ax.transAxes,
    )

    # Subplot 6
    ax = axes[1, 2]
    ax.axis("off")
    ax.text(
        0.5,   0.5,  "Dataset -> MinIO -> Spark -> Postgres -> Dashboard",
        ha="center", va="center",  fontsize=12,   transform=ax.transAxes,
        fontfamily="monospace"
    )

    fig.tight_layout()
    _save(fig, "dashboard_summary.png")

#main
if __name__ == "__main__":
    print("Generating placeholder plots....")
    make_rq1_propagation_heatmap()
    make_rq2_anomaly_timeseries()
    make_rq2_failure_patterns()
    make_rq3_scaling_curves()
    make_rq3_efficiency()
    make_dashboard_summary()
    print(f"\n Done  {len(os.listdir(OUTPUT_DIR))} files in {OUTPUT_DIR}")
