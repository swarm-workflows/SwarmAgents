#!/usr/bin/env python3
"""
Plot Multi-Armed Bandit (MAB) results from hierarchical job scheduling runs.

Usage:
    # From Redis (after a run)
    python -m plotting.mab --db-host localhost --output-dir runs/mab-test-001

    # From saved CSV/JSON files
    python -m plotting.mab --from-csv --output-dir runs/mab-test-001

    # Compare MAB vs baseline
    python -m plotting.mab --compare runs/baseline-001 runs/mab-test-001
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from typing import Any, Dict, List, Optional

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd

from plotting.data import (
    load_metrics_from_redis,
    load_metrics_from_file,
    load_jobs_from_csv,
    load_jobs_from_redis,
)


def extract_mab_data(metrics: Dict[int, dict]) -> Dict[str, Any]:
    """Extract MAB-specific data from agent metrics."""
    mab_data = {
        "agents_with_mab": [],
        "arm_stats": defaultdict(lambda: {"q_values": [], "successes": 0, "failures": 0, "pulls": 0}),
        "selections": defaultdict(int),
        "rewards_over_time": defaultdict(list),
    }

    for agent_id, m in metrics.items():
        if "mab_stats" not in m:
            continue

        mab_data["agents_with_mab"].append(agent_id)
        mab_stats = m["mab_stats"]

        # Extract arm statistics
        arms = mab_stats.get("arms", {})
        for arm_id, arm_info in arms.items():
            arm_id = int(arm_id)
            mab_data["arm_stats"][arm_id]["q_values"].append(arm_info.get("q_value", 0))
            mab_data["arm_stats"][arm_id]["successes"] += arm_info.get("successes", 0)
            mab_data["arm_stats"][arm_id]["failures"] += arm_info.get("failures", 0)
            mab_data["arm_stats"][arm_id]["pulls"] += arm_info.get("pull_count", 0)

        # Extract selection counts
        selections = m.get("mab_selections", {})
        for group_id, count in selections.items():
            mab_data["selections"][int(group_id)] += count

        # Extract reward history
        rewards = m.get("mab_rewards", {})
        for group_id, reward_list in rewards.items():
            mab_data["rewards_over_time"][int(group_id)].extend(reward_list)

    return mab_data


def plot_q_values(mab_data: Dict[str, Any], output_dir: str):
    """Plot final Q-values per group (arm)."""
    arm_stats = mab_data["arm_stats"]
    if not arm_stats:
        print("No MAB arm stats found.")
        return

    groups = sorted(arm_stats.keys())
    q_values = [np.mean(arm_stats[g]["q_values"]) if arm_stats[g]["q_values"] else 0 for g in groups]
    q_stds = [np.std(arm_stats[g]["q_values"]) if len(arm_stats[g]["q_values"]) > 1 else 0 for g in groups]

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(range(len(groups)), q_values, yerr=q_stds, capsize=5, color='steelblue', edgecolor='black')

    ax.set_xlabel("Child Group ID", fontsize=12)
    ax.set_ylabel("Q-Value (mean across coordinators)", fontsize=12)
    ax.set_title("MAB Learned Q-Values per Child Group", fontsize=14)
    ax.set_xticks(range(len(groups)))
    ax.set_xticklabels([str(g) for g in groups])
    ax.axhline(y=0, color='gray', linestyle='--', linewidth=0.8)

    # Add value labels on bars
    for bar, val in zip(bars, q_values):
        height = bar.get_height()
        ax.annotate(f'{val:.2f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=9)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "mab_q_values.png"), dpi=150)
    plt.close()
    print(f"Saved: {output_dir}/mab_q_values.png")


def plot_selection_distribution(mab_data: Dict[str, Any], output_dir: str):
    """Plot how many times each group was selected."""
    selections = mab_data["selections"]
    if not selections:
        print("No MAB selection data found.")
        return

    groups = sorted(selections.keys())
    counts = [selections[g] for g in groups]
    total = sum(counts)
    percentages = [c / total * 100 if total > 0 else 0 for c in counts]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    # Bar chart
    colors = plt.cm.Set2(np.linspace(0, 1, len(groups)))
    bars = ax1.bar(range(len(groups)), counts, color=colors, edgecolor='black')
    ax1.set_xlabel("Child Group ID", fontsize=12)
    ax1.set_ylabel("Selection Count", fontsize=12)
    ax1.set_title("MAB Group Selection Counts", fontsize=14)
    ax1.set_xticks(range(len(groups)))
    ax1.set_xticklabels([str(g) for g in groups])

    for bar, cnt in zip(bars, counts):
        ax1.annotate(f'{cnt}',
                     xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                     xytext=(0, 3),
                     textcoords="offset points",
                     ha='center', va='bottom', fontsize=9)

    # Pie chart
    ax2.pie(counts, labels=[f"Group {g}" for g in groups], autopct='%1.1f%%',
            colors=colors, startangle=90)
    ax2.set_title("Selection Distribution", fontsize=14)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "mab_selections.png"), dpi=150)
    plt.close()
    print(f"Saved: {output_dir}/mab_selections.png")


def plot_success_failure_rates(mab_data: Dict[str, Any], output_dir: str):
    """Plot success/failure rates per group."""
    arm_stats = mab_data["arm_stats"]
    if not arm_stats:
        print("No MAB arm stats found.")
        return

    groups = sorted(arm_stats.keys())
    successes = [arm_stats[g]["successes"] for g in groups]
    failures = [arm_stats[g]["failures"] for g in groups]
    totals = [s + f for s, f in zip(successes, failures)]
    success_rates = [s / t * 100 if t > 0 else 0 for s, t in zip(successes, totals)]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    # Stacked bar chart
    x = np.arange(len(groups))
    width = 0.6
    ax1.bar(x, successes, width, label='Successes', color='green', edgecolor='black')
    ax1.bar(x, failures, width, bottom=successes, label='Failures', color='red', edgecolor='black')
    ax1.set_xlabel("Child Group ID", fontsize=12)
    ax1.set_ylabel("Job Count", fontsize=12)
    ax1.set_title("Success vs Failure per Group", fontsize=14)
    ax1.set_xticks(x)
    ax1.set_xticklabels([str(g) for g in groups])
    ax1.legend()

    # Success rate bar chart
    colors = ['green' if r >= 70 else 'orange' if r >= 50 else 'red' for r in success_rates]
    bars = ax2.bar(x, success_rates, width, color=colors, edgecolor='black')
    ax2.set_xlabel("Child Group ID", fontsize=12)
    ax2.set_ylabel("Success Rate (%)", fontsize=12)
    ax2.set_title("Success Rate per Group", fontsize=14)
    ax2.set_xticks(x)
    ax2.set_xticklabels([str(g) for g in groups])
    ax2.set_ylim(0, 105)
    ax2.axhline(y=50, color='gray', linestyle='--', linewidth=0.8, label='50% threshold')

    for bar, rate in zip(bars, success_rates):
        ax2.annotate(f'{rate:.1f}%',
                     xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                     xytext=(0, 3),
                     textcoords="offset points",
                     ha='center', va='bottom', fontsize=9)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "mab_success_rates.png"), dpi=150)
    plt.close()
    print(f"Saved: {output_dir}/mab_success_rates.png")


def plot_rewards_over_time(mab_data: Dict[str, Any], output_dir: str):
    """Plot cumulative rewards over time per group."""
    rewards = mab_data["rewards_over_time"]
    if not rewards:
        print("No MAB reward history found.")
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    colors = plt.cm.tab10(np.linspace(0, 1, len(rewards)))

    for i, (group_id, reward_list) in enumerate(sorted(rewards.items())):
        if not reward_list:
            continue

        # Sort by timestamp
        sorted_rewards = sorted(reward_list, key=lambda x: x[0])
        timestamps = [r[0] for r in sorted_rewards]
        reward_values = [r[1] for r in sorted_rewards]

        # Normalize timestamps to start from 0
        if timestamps:
            t0 = timestamps[0]
            timestamps = [t - t0 for t in timestamps]

        # Cumulative reward
        cumulative = np.cumsum(reward_values)

        ax1.plot(timestamps, cumulative, label=f"Group {group_id}", color=colors[i], linewidth=2)

        # Running average (window=20)
        if len(reward_values) >= 20:
            running_avg = pd.Series(reward_values).rolling(window=20).mean()
            ax2.plot(timestamps, running_avg, label=f"Group {group_id}", color=colors[i], linewidth=2)

    ax1.set_xlabel("Time (seconds)", fontsize=12)
    ax1.set_ylabel("Cumulative Reward", fontsize=12)
    ax1.set_title("Cumulative Reward per Group", fontsize=14)
    ax1.legend()
    ax1.axhline(y=0, color='gray', linestyle='--', linewidth=0.8)

    ax2.set_xlabel("Time (seconds)", fontsize=12)
    ax2.set_ylabel("Running Average Reward (window=20)", fontsize=12)
    ax2.set_title("Reward Trend per Group", fontsize=14)
    ax2.legend()
    ax2.axhline(y=0, color='gray', linestyle='--', linewidth=0.8)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "mab_rewards_over_time.png"), dpi=150)
    plt.close()
    print(f"Saved: {output_dir}/mab_rewards_over_time.png")


def plot_job_exit_status(jobs_df: pd.DataFrame, output_dir: str):
    """Plot job success/failure distribution."""
    if jobs_df.empty or 'exit_status' not in jobs_df.columns:
        print("No job exit_status data found.")
        return

    success_count = (jobs_df['exit_status'] == 0).sum()
    failure_count = (jobs_df['exit_status'] != 0).sum()
    total = len(jobs_df)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    # Pie chart
    ax1.pie([success_count, failure_count],
            labels=['Success', 'Failure'],
            autopct='%1.1f%%',
            colors=['green', 'red'],
            startangle=90,
            explode=(0.02, 0.02))
    ax1.set_title(f"Job Outcomes (n={total})", fontsize=14)

    # Bar chart
    ax2.bar(['Success', 'Failure'], [success_count, failure_count],
            color=['green', 'red'], edgecolor='black')
    ax2.set_ylabel("Count", fontsize=12)
    ax2.set_title("Job Exit Status Distribution", fontsize=14)

    for i, (label, count) in enumerate([('Success', success_count), ('Failure', failure_count)]):
        ax2.annotate(f'{count}',
                     xy=(i, count),
                     xytext=(0, 3),
                     textcoords="offset points",
                     ha='center', va='bottom', fontsize=11)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "job_exit_status.png"), dpi=150)
    plt.close()
    print(f"Saved: {output_dir}/job_exit_status.png")


def compare_runs(run_dirs: List[str], output_dir: str):
    """Compare multiple runs (e.g., baseline vs MAB)."""
    run_data = []

    for run_dir in run_dirs:
        metrics = load_metrics_from_file(os.path.join(run_dir, "metrics.json"))
        jobs_df = load_jobs_from_csv(run_dir)

        run_name = os.path.basename(run_dir)
        mab_enabled = any("mab_stats" in m for m in metrics.values())

        if not jobs_df.empty and 'exit_status' in jobs_df.columns:
            success_count = (jobs_df['exit_status'] == 0).sum()
            total = len(jobs_df)
            success_rate = success_count / total * 100 if total > 0 else 0
        else:
            success_rate = 0
            total = 0

        run_data.append({
            "name": run_name,
            "mab_enabled": mab_enabled,
            "success_rate": success_rate,
            "total_jobs": total,
        })

    if not run_data:
        print("No run data to compare.")
        return

    # Plot comparison
    fig, ax = plt.subplots(figsize=(10, 6))

    names = [d["name"] for d in run_data]
    success_rates = [d["success_rate"] for d in run_data]
    colors = ['steelblue' if d["mab_enabled"] else 'gray' for d in run_data]

    bars = ax.bar(range(len(names)), success_rates, color=colors, edgecolor='black')

    ax.set_xlabel("Run", fontsize=12)
    ax.set_ylabel("Job Success Rate (%)", fontsize=12)
    ax.set_title("Comparison: Job Success Rate Across Runs", fontsize=14)
    ax.set_xticks(range(len(names)))
    ax.set_xticklabels(names, rotation=45, ha='right')
    ax.set_ylim(0, 105)

    # Legend
    mab_patch = mpatches.Patch(color='steelblue', label='MAB Enabled')
    baseline_patch = mpatches.Patch(color='gray', label='MAB Disabled')
    ax.legend(handles=[mab_patch, baseline_patch])

    for bar, rate, data in zip(bars, success_rates, run_data):
        ax.annotate(f'{rate:.1f}%\n(n={data["total_jobs"]})',
                    xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=9)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "mab_comparison.png"), dpi=150)
    plt.close()
    print(f"Saved: {output_dir}/mab_comparison.png")


def print_mab_summary(mab_data: Dict[str, Any]):
    """Print a text summary of MAB results."""
    print("\n" + "=" * 60)
    print("MAB RESULTS SUMMARY")
    print("=" * 60)

    if not mab_data["agents_with_mab"]:
        print("No MAB data found. Make sure mab.enabled=true in config.")
        return

    print(f"Agents with MAB: {sorted(mab_data['agents_with_mab'])}")

    print("\nPer-Group Statistics:")
    print("-" * 60)
    print(f"{'Group':<8} {'Q-Value':<12} {'Successes':<12} {'Failures':<12} {'Success %':<12}")
    print("-" * 60)

    for group_id in sorted(mab_data["arm_stats"].keys()):
        stats = mab_data["arm_stats"][group_id]
        q_val = np.mean(stats["q_values"]) if stats["q_values"] else 0
        succ = stats["successes"]
        fail = stats["failures"]
        total = succ + fail
        rate = succ / total * 100 if total > 0 else 0
        print(f"{group_id:<8} {q_val:<12.3f} {succ:<12} {fail:<12} {rate:<12.1f}")

    print("\nSelection Distribution:")
    print("-" * 60)
    total_selections = sum(mab_data["selections"].values())
    for group_id in sorted(mab_data["selections"].keys()):
        count = mab_data["selections"][group_id]
        pct = count / total_selections * 100 if total_selections > 0 else 0
        print(f"  Group {group_id}: {count} selections ({pct:.1f}%)")

    print("=" * 60 + "\n")


# ---------------------------------------------------------------------------
# Offline full-Redis-dump mode (contextual bandit evaluation — see
# docs/CONTEXTUAL_BANDIT_DESIGN.md section 8 and evaluation/scenario_*/)
# ---------------------------------------------------------------------------

def load_full_dump(path: str) -> Dict[str, Any]:
    """Load a full Redis dump produced by evaluation/scenario_a/extract or the
    inline dump snippets: {"jobs": {key: record}, "metrics": {key: value}}.

    Returns time-ordered L0 delegation rows plus any persisted mab:* states.
    """
    data = json.load(open(path))
    rows = []
    for key, d in data.get("jobs", {}).items():
        if not key.startswith("job:0:"):
            continue
        group = int(key.split(":")[2])
        job_type = d.get("job_type") or "?"
        ts_dict = d.get("assigned_at") or d.get("submitted_at") or {}
        if isinstance(ts_dict, dict) and ts_dict:
            ts = min(float(v) for v in ts_dict.values())
        else:
            ts = float(ts_dict) if ts_dict else 0.0
        rows.append({
            "ts": ts,
            "group": group,
            "job_type": job_type,
            "resource_class": job_type.split("_")[0],
            "complete": d.get("state") == 8,
            "success": int(d.get("exit_status") or 0) == 0,
        })
    rows.sort(key=lambda r: r["ts"])
    mab_states = {k.split(":")[1]: v for k, v in data.get("metrics", {}).items()
                  if k.startswith("mab:")}
    return {"rows": rows, "mab_states": mab_states}


def load_events(path: str) -> List[Dict[str, Any]]:
    """Parse an events file of '<epoch_seconds> <label>' lines."""
    events = []
    with open(path) as fh:
        for line in fh:
            parts = line.split(None, 1)
            if len(parts) == 2:
                events.append({"ts": float(parts[0]), "label": parts[1].strip()})
    return events


def _event_indices(rows: List[dict], events: List[dict]) -> List[Dict[str, Any]]:
    out = []
    for ev in events or []:
        idx = sum(1 for r in rows if r["ts"] < ev["ts"])
        if 0 < idx < len(rows):
            out.append({"idx": idx, "label": ev["label"]})
    return out


def plot_dump_timeline(rows: List[dict], output_dir: str,
                       events: Optional[List[dict]] = None, window: int = 25):
    """Rolling success rate and per-group delegation share over job order,
    with event markers (e.g. failure flip, group join)."""
    n = len(rows)
    if n < window:
        print("Too few rows for timeline plot")
        return
    succ = np.array([1.0 if r["success"] else 0.0 for r in rows])
    kernel = np.ones(window) / window
    rolling = np.convolve(succ, kernel, mode="valid")
    x = np.arange(window - 1, n)

    groups = sorted({r["group"] for r in rows})
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 7), sharex=True)
    ax1.plot(x, rolling, lw=2)
    ax1.set_ylabel(f"success rate (rolling {window})")
    ax1.set_ylim(0, 1.05)
    ax1.grid(alpha=0.3)

    for g in groups:
        share = np.array([1.0 if r["group"] == g else 0.0 for r in rows])
        ax2.plot(x, np.convolve(share, kernel, mode="valid"),
                 lw=1.5, label=f"group {g}")
    ax2.set_ylabel(f"delegation share (rolling {window})")
    ax2.set_xlabel("job (delegation order)")
    ax2.legend(ncol=len(groups), fontsize=8)
    ax2.grid(alpha=0.3)

    for ev in _event_indices(rows, events or []):
        for ax in (ax1, ax2):
            ax.axvline(ev["idx"], color="red", ls="--", alpha=0.7)
        ax1.annotate(ev["label"], (ev["idx"], 1.0), fontsize=8,
                     rotation=90, va="top", color="red")

    fig.suptitle("Delegation timeline")
    fig.tight_layout()
    out = os.path.join(output_dir, "mab_dump_timeline.png")
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Saved: {out}")


def plot_dump_group_outcomes(rows: List[dict], output_dir: str):
    """Success rate and volume per (resource class, group) — routing quality."""
    groups = sorted({r["group"] for r in rows})
    classes = sorted({r["resource_class"] for r in rows})
    rates = np.zeros((len(classes), len(groups)))
    counts = np.zeros((len(classes), len(groups)), dtype=int)
    for r in rows:
        i, j = classes.index(r["resource_class"]), groups.index(r["group"])
        counts[i][j] += 1
        rates[i][j] += 1 if r["success"] else 0
    with np.errstate(invalid="ignore"):
        rates = np.where(counts > 0, rates / np.maximum(counts, 1), np.nan)

    fig, ax = plt.subplots(figsize=(1.6 * len(groups) + 2, 1.2 * len(classes) + 2))
    im = ax.imshow(rates, vmin=0, vmax=1, cmap="RdYlGn", aspect="auto")
    ax.set_xticks(range(len(groups)), [f"group {g}" for g in groups])
    ax.set_yticks(range(len(classes)), classes)
    for i in range(len(classes)):
        for j in range(len(groups)):
            if counts[i][j]:
                ax.text(j, i, f"{rates[i][j]:.0%}\n(n={counts[i][j]})",
                        ha="center", va="center", fontsize=9)
    fig.colorbar(im, label="success rate")
    ax.set_title("Success rate by job class x delegated group")
    fig.tight_layout()
    out = os.path.join(output_dir, "mab_dump_group_outcomes.png")
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Saved: {out}")


def plot_dump_theta(mab_states: Dict[str, dict], output_dir: str,
                    job_types: Optional[List[str]] = None):
    """Learned LinUCB weights (theta = A^-1 b) per coordinator, labeled with
    feature names when the job-type vocabulary is supplied."""
    for agent_id, state in mab_states.items():
        if state.get("A_inv") is None:
            continue
        theta = np.array(state["A_inv"]) @ np.array(state["b"])
        names = [f"f{i}" for i in range(len(theta))]
        if job_types is not None:
            try:
                from swarm.rl.context import ContextExtractor
                ext = ContextExtractor({"job_types": job_types})
                if ext.dim == len(theta):
                    names = ext.feature_names
            except ImportError:
                pass
        order = np.argsort(np.abs(theta))
        fig, ax = plt.subplots(figsize=(8, 0.3 * len(theta) + 2))
        colors = ["tab:green" if theta[i] >= 0 else "tab:red" for i in order]
        ax.barh([names[i] for i in order], [theta[i] for i in order], color=colors)
        ax.set_xlabel("theta (feature weight)")
        ax.set_title(f"LinUCB learned weights — coordinator {agent_id}")
        ax.grid(alpha=0.3, axis="x")
        fig.tight_layout()
        out = os.path.join(output_dir, f"mab_dump_theta_agent{agent_id}.png")
        fig.savefig(out, dpi=150)
        plt.close(fig)
        print(f"Saved: {out}")


def plot_from_dump(dump_path: str, output_dir: str,
                   events_path: Optional[str] = None,
                   job_types: Optional[List[str]] = None):
    dump = load_full_dump(dump_path)
    rows = dump["rows"]
    print(f"Loaded {len(rows)} L0 delegation records, "
          f"{len(dump['mab_states'])} persisted policy state(s)")
    events = load_events(events_path) if events_path else []
    os.makedirs(output_dir, exist_ok=True)
    plot_dump_timeline(rows, output_dir, events=events)
    plot_dump_group_outcomes(rows, output_dir)
    plot_dump_theta(dump["mab_states"], output_dir, job_types=job_types)


def parse_args():
    parser = argparse.ArgumentParser(description="Plot MAB results from hierarchical scheduling runs")
    parser.add_argument("--db-host", type=str, help="Redis host")
    parser.add_argument("--db-port", type=int, default=6379, help="Redis port")
    parser.add_argument("--output-dir", type=str, default=".", help="Output directory for plots")
    parser.add_argument("--from-csv", action="store_true", help="Load from CSV/JSON files instead of Redis")
    parser.add_argument("--compare", nargs="+", metavar="RUN_DIR", help="Compare multiple run directories")
    parser.add_argument("--no-plots", action="store_true", help="Print summary only, no plots")
    parser.add_argument("--dump", type=str,
                        help="Offline mode: plot from a full Redis dump JSON "
                             "(see evaluation/scenario_a/extract_scenario_a.py)")
    parser.add_argument("--events", type=str,
                        help="Optional events file ('<epoch> <label>' lines) "
                             "to mark on dump timelines")
    parser.add_argument("--job-types", type=str,
                        help="Comma-separated job-type vocabulary used in "
                             "mab.context.job_types (labels theta features)")
    return parser.parse_args()


def main():
    args = parse_args()

    # Offline dump mode (contextual bandit evaluation)
    if args.dump:
        job_types = args.job_types.split(",") if args.job_types else None
        plot_from_dump(args.dump, args.output_dir,
                       events_path=args.events, job_types=job_types)
        return

    # Comparison mode
    if args.compare:
        os.makedirs(args.output_dir, exist_ok=True)
        compare_runs(args.compare, args.output_dir)
        return

    # Single run mode
    if args.from_csv:
        metrics = load_metrics_from_file(os.path.join(args.output_dir, "metrics.json"))
        jobs_df = load_jobs_from_csv(args.output_dir)
    else:
        if not args.db_host:
            print("Error: --db-host required when not using --from-csv")
            sys.exit(1)
        metrics = load_metrics_from_redis(args.db_host, args.db_port)
        jobs_df = load_jobs_from_redis(args.db_host, args.db_port)

    if not metrics:
        print("No metrics data found.")
        sys.exit(1)

    mab_data = extract_mab_data(metrics)
    print_mab_summary(mab_data)

    if args.no_plots:
        return

    os.makedirs(args.output_dir, exist_ok=True)

    # Generate plots
    plot_q_values(mab_data, args.output_dir)
    plot_selection_distribution(mab_data, args.output_dir)
    plot_success_failure_rates(mab_data, args.output_dir)
    plot_rewards_over_time(mab_data, args.output_dir)
    plot_job_exit_status(jobs_df, args.output_dir)

    print(f"\nAll plots saved to: {args.output_dir}/")


if __name__ == "__main__":
    main()
