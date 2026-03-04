#!/usr/bin/env python3.11
"""
plot_comparison.py — Generate comparison plots and tables across schedulers.

Usage:
  python plot_comparison.py \
      --swarm-dir runs/swarm-mesh-30-500 \
      --greedy-dir runs/baseline-greedy-30-500 \
      --rr-dir runs/baseline-rr-30-500 \
      --random-dir runs/baseline-random-30-500 \
      --output-dir runs/comparison

Reads all_jobs.csv from each directory and produces:
  1. Scheduling Latency CDF
  2. Scheduling Latency Box Plot
  3. Makespan Bar Chart
  4. Job Completion Over Time (cumulative)
  5. Load Balance (per-agent job count + Jain's fairness index)
  6. Summary Statistics Table (also saved as CSV)
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


# ── Helpers ────────────────────────────────────────────────────────

def load_jobs_csv(csv_path: str) -> pd.DataFrame:
    """Load an all_jobs.csv file and return a DataFrame with numeric columns."""
    df = pd.read_csv(csv_path)
    for col in ["submitted_at", "selection_started_at", "assigned_at",
                 "started_at", "completed_at", "scheduling_latency", "reasoning_time"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def jains_fairness(values: np.ndarray) -> float:
    """Compute Jain's Fairness Index for a set of values."""
    n = len(values)
    if n == 0:
        return 0.0
    s = np.sum(values)
    ss = np.sum(values ** 2)
    if ss == 0:
        return 1.0
    return (s ** 2) / (n * ss)


def compute_stats(df: pd.DataFrame) -> dict:
    """Compute summary statistics for a scheduler's job DataFrame."""
    lat = df["scheduling_latency"].dropna()
    completed = df[df["completed_at"] > 0]

    if len(completed) > 0:
        makespan = completed["completed_at"].max() - df["submitted_at"].min()
    else:
        makespan = 0.0

    throughput = len(completed) / makespan if makespan > 0 else 0.0

    # Load balance from leader_id distribution
    leader_counts = df["leader_id"].value_counts().values
    fairness = jains_fairness(leader_counts) if len(leader_counts) > 0 else 0.0

    return {
        "jobs_total": len(df),
        "jobs_completed": len(completed),
        "mean_latency": round(lat.mean(), 3) if len(lat) > 0 else 0,
        "median_latency": round(lat.median(), 3) if len(lat) > 0 else 0,
        "p95_latency": round(lat.quantile(0.95), 3) if len(lat) > 0 else 0,
        "p99_latency": round(lat.quantile(0.99), 3) if len(lat) > 0 else 0,
        "makespan": round(makespan, 2),
        "throughput_jps": round(throughput, 2),
        "fairness_jain": round(fairness, 4),
    }


# ── Plot Functions ─────────────────────────────────────────────────

def plot_latency_cdf(data: dict[str, pd.DataFrame], output_dir: str):
    """1. Scheduling Latency CDF — one line per scheduler."""
    fig, ax = plt.subplots(figsize=(8, 5))

    for label, df in data.items():
        lat = df["scheduling_latency"].dropna().sort_values()
        if len(lat) == 0:
            continue
        cdf = np.arange(1, len(lat) + 1) / len(lat)
        ax.plot(lat.values, cdf, label=label, linewidth=1.5)

    ax.set_xlabel("Scheduling Latency (s)")
    ax.set_ylabel("CDF")
    ax.set_title("Scheduling Latency CDF")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, "latency_cdf.pdf"), dpi=150)
    fig.savefig(os.path.join(output_dir, "latency_cdf.png"), dpi=150)
    plt.close(fig)


def plot_latency_boxplot(data: dict[str, pd.DataFrame], output_dir: str):
    """2. Scheduling Latency Box Plot — median, P25, P75, whiskers to P5/P95."""
    fig, ax = plt.subplots(figsize=(8, 5))

    labels = []
    latencies = []
    for label, df in data.items():
        lat = df["scheduling_latency"].dropna()
        if len(lat) > 0:
            labels.append(label)
            latencies.append(lat.values)

    bp = ax.boxplot(latencies, labels=labels, showfliers=False, patch_artist=True,
                    whis=[5, 95])
    colors = ["#4C72B0", "#DD8452", "#55A868", "#C44E52"]
    for i, patch in enumerate(bp["boxes"]):
        patch.set_facecolor(colors[i % len(colors)])
        patch.set_alpha(0.7)

    ax.set_ylabel("Scheduling Latency (s)")
    ax.set_title("Scheduling Latency Distribution")
    ax.grid(True, alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, "latency_boxplot.pdf"), dpi=150)
    fig.savefig(os.path.join(output_dir, "latency_boxplot.png"), dpi=150)
    plt.close(fig)


def plot_makespan_bar(stats: dict[str, dict], output_dir: str):
    """3. Makespan Bar Chart — total time first→last job."""
    fig, ax = plt.subplots(figsize=(8, 5))

    labels = list(stats.keys())
    makespans = [stats[l]["makespan"] for l in labels]
    colors = ["#4C72B0", "#DD8452", "#55A868", "#C44E52"]

    bars = ax.bar(labels, makespans, color=colors[:len(labels)], alpha=0.8, edgecolor="black")
    for bar, val in zip(bars, makespans):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                f"{val:.1f}s", ha="center", va="bottom", fontsize=10)

    ax.set_ylabel("Makespan (s)")
    ax.set_title("Total Makespan Comparison")
    ax.grid(True, alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, "makespan_bar.pdf"), dpi=150)
    fig.savefig(os.path.join(output_dir, "makespan_bar.png"), dpi=150)
    plt.close(fig)


def plot_completion_over_time(data: dict[str, pd.DataFrame], output_dir: str):
    """4. Cumulative Job Completion Over Time."""
    fig, ax = plt.subplots(figsize=(8, 5))

    for label, df in data.items():
        completed = df[df["completed_at"] > 0].copy()
        if len(completed) == 0:
            continue
        t_min = df["submitted_at"].min()
        elapsed = (completed["completed_at"] - t_min).sort_values()
        cumulative = np.arange(1, len(elapsed) + 1)
        ax.plot(elapsed.values, cumulative, label=label, linewidth=1.5)

    ax.set_xlabel("Elapsed Time (s)")
    ax.set_ylabel("Cumulative Completed Jobs")
    ax.set_title("Job Completion Over Time")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, "completion_over_time.pdf"), dpi=150)
    fig.savefig(os.path.join(output_dir, "completion_over_time.png"), dpi=150)
    plt.close(fig)


def plot_load_balance(data: dict[str, pd.DataFrame], stats: dict[str, dict], output_dir: str):
    """5. Load Balance — per-agent job count distribution + Jain's fairness index."""
    n_schedulers = len(data)
    fig, axes = plt.subplots(1, n_schedulers, figsize=(4 * n_schedulers, 5), sharey=True)
    if n_schedulers == 1:
        axes = [axes]

    colors = ["#4C72B0", "#DD8452", "#55A868", "#C44E52"]

    for i, (label, df) in enumerate(data.items()):
        ax = axes[i]
        counts = df["leader_id"].value_counts().sort_index()
        agents = counts.index.astype(int)
        ax.bar(range(len(agents)), counts.values, color=colors[i % len(colors)], alpha=0.7)
        ax.set_xlabel("Agent")
        ax.set_title(f"{label}\nJain={stats[label]['fairness_jain']:.3f}")
        if i == 0:
            ax.set_ylabel("Jobs Assigned")
        ax.grid(True, alpha=0.3, axis="y")

    fig.suptitle("Load Balance Across Agents", fontsize=13, y=1.02)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, "load_balance.pdf"), dpi=150, bbox_inches="tight")
    fig.savefig(os.path.join(output_dir, "load_balance.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)


def save_summary_table(stats: dict[str, dict], output_dir: str):
    """6. Summary Statistics Table — saved as CSV and printed to console."""
    rows = []
    for label, s in stats.items():
        rows.append({
            "Scheduler": label,
            "Jobs Total": s["jobs_total"],
            "Jobs Completed": s["jobs_completed"],
            "Mean Latency (s)": s["mean_latency"],
            "Median Latency (s)": s["median_latency"],
            "P95 Latency (s)": s["p95_latency"],
            "P99 Latency (s)": s["p99_latency"],
            "Makespan (s)": s["makespan"],
            "Throughput (jobs/s)": s["throughput_jps"],
            "Jain Fairness": s["fairness_jain"],
        })

    df = pd.DataFrame(rows)

    # Save CSV (for LaTeX import)
    csv_path = os.path.join(output_dir, "summary_statistics.csv")
    df.to_csv(csv_path, index=False)

    # Save JSON
    json_path = os.path.join(output_dir, "summary_statistics.json")
    with open(json_path, "w") as f:
        json.dump(stats, f, indent=2)

    # Print table
    print("\n" + "=" * 80)
    print("SUMMARY STATISTICS")
    print("=" * 80)
    print(df.to_string(index=False))
    print("=" * 80)
    print(f"\nSaved to: {csv_path}")


# ── Main ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Compare scheduler results from all_jobs.csv files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--swarm-dir", type=str, help="SWARM+ run directory")
    parser.add_argument("--greedy-dir", type=str, help="Greedy baseline run directory")
    parser.add_argument("--rr-dir", type=str, help="Round-Robin baseline run directory")
    parser.add_argument("--random-dir", type=str, help="Random baseline run directory")
    parser.add_argument("--output-dir", type=str, required=True, help="Output directory for plots")

    # Generic --dirs option for arbitrary number of schedulers
    parser.add_argument(
        "--dirs", nargs="*", metavar="LABEL=PATH",
        help='Additional scheduler dirs as LABEL=PATH pairs (e.g., "MyScheduler=runs/my-run")',
    )

    args = parser.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    # Collect all scheduler data
    data: dict[str, pd.DataFrame] = {}

    named_dirs = [
        ("SWARM+", args.swarm_dir),
        ("Greedy", args.greedy_dir),
        ("Round-Robin", args.rr_dir),
        ("Random", args.random_dir),
    ]

    for label, run_dir in named_dirs:
        if run_dir is None:
            continue
        csv_path = os.path.join(run_dir, "all_jobs.csv")
        if not os.path.exists(csv_path):
            print(f"WARNING: {csv_path} not found, skipping {label}")
            continue
        data[label] = load_jobs_csv(csv_path)
        print(f"Loaded {label}: {len(data[label])} jobs from {csv_path}")

    # Handle generic --dirs
    if args.dirs:
        for entry in args.dirs:
            if "=" not in entry:
                print(f"WARNING: Skipping malformed --dirs entry: {entry} (expected LABEL=PATH)")
                continue
            label, run_dir = entry.split("=", 1)
            csv_path = os.path.join(run_dir, "all_jobs.csv")
            if not os.path.exists(csv_path):
                print(f"WARNING: {csv_path} not found, skipping {label}")
                continue
            data[label] = load_jobs_csv(csv_path)
            print(f"Loaded {label}: {len(data[label])} jobs from {csv_path}")

    if not data:
        print("ERROR: No data loaded. Provide at least one --*-dir argument.")
        sys.exit(1)

    # Compute statistics
    stats = {label: compute_stats(df) for label, df in data.items()}

    # Generate all plots
    print(f"\nGenerating plots in {args.output_dir}/")

    plot_latency_cdf(data, args.output_dir)
    print("  - latency_cdf.pdf")

    plot_latency_boxplot(data, args.output_dir)
    print("  - latency_boxplot.pdf")

    plot_makespan_bar(stats, args.output_dir)
    print("  - makespan_bar.pdf")

    plot_completion_over_time(data, args.output_dir)
    print("  - completion_over_time.pdf")

    plot_load_balance(data, stats, args.output_dir)
    print("  - load_balance.pdf")

    save_summary_table(stats, args.output_dir)


if __name__ == "__main__":
    main()
