import csv
import json
from typing import Any

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import argparse
import os
import numpy as np

import redis

from swarm.database.repository import Repository
from swarm.models.job import Job, ObjectState
from collections import defaultdict

from typing import Set, Dict, List, Tuple

def collect_restarted_job_ids(output_dir: str, repo: Repository | None = None) -> Set[int]:
    """
    Return the set of job_ids that experienced >=1 restart.

    Prefers Redis metrics; falls back to misc_<agent_id>.json files in output_dir.
    """
    restarted: set[int] = set()

    # Try Redis first
    if repo is not None:
        metrics = load_metrics_from_repo(repo)
        if metrics:
            for _agent_id, m in metrics.items():
                for job_id, cnt in (m.get("restarts", {}) or {}).items():
                    try:
                        if int(cnt) > 0:
                            restarted.add(int(job_id))
                    except Exception:
                        continue
            if restarted:
                return restarted

    # Fallback: disk (misc_*.json)
    for fname in os.listdir(output_dir):
        if fname.startswith("misc_") and fname.endswith(".json"):
            try:
                with open(os.path.join(output_dir, fname), "r") as f:
                    data = json.load(f)
            except Exception:
                continue
            for job_id, cnt in (data.get("restarts", {}) or {}).items():
                try:
                    if int(cnt) > 0:
                        restarted.add(int(job_id))
                except Exception:
                    continue

    return restarted

def load_metrics_from_repo(repo: Repository) -> dict[int, dict]:
    """
    Load metrics objects from Redis and normalize their shape.
    Expected schema per agent:
      {
        "id": <int>,
        "restarts": {job_id: count, ...} or {},
        "conflicts": {job_id: count, ...} or {},
        "idle_time": [floats] or [],
        "load_trace": [[timestamp, load], ...] or []
      }
    Returns: {agent_id: metrics_dict}
    """
    metrics_by_agent: dict[int, dict] = {}
    try:
        raw = repo.get_all_objects(key_prefix="metrics", level=None)
    except Exception:
        raw = []

    if not raw:
        return {}

    for entry in raw:
        # entry can be dict or a serialized json string
        if isinstance(entry, str):
            try:
                entry = json.loads(entry)
            except Exception:
                continue
        if not isinstance(entry, dict):
            continue
        agent_id = entry.get("id")
        if agent_id is None:
            # Some repos return keys; try to sniff common patterns
            # e.g., {"key":"metrics:1","value":{"id":1,...}}
            if "value" in entry and isinstance(entry["value"], dict):
                agent_id = entry["value"].get("id")
                entry = entry["value"]
        if agent_id is None:
            continue

        # normalize fields
        entry.setdefault("restarts", {})
        entry.setdefault("conflicts", {})
        entry.setdefault("idle_time", [])
        entry.setdefault("load_trace", [])
        metrics_by_agent[int(agent_id)] = entry

    return metrics_by_agent

def save_metrics(metrics: dict[int, dict], path: str):
    # Save combined CSV with leader_id
    with open(path, 'w', newline='') as f:
        json.dump(metrics, f, indent=2)


def save_agents(agents: list[Any], path: str):
    # Save combined CSV with leader_id
    with open(path, 'w', newline='') as f:
        json.dump(agents, f, indent=2)

def save_jobs(jobs: list[Any], path: str, level: int | None = None):
    """
    Save jobs to CSV with level-specific timestamps.

    Args:
        jobs: List of job objects or dicts
        path: Output CSV path
        level: Hierarchy level (0, 1, etc.). If None, uses last timestamp (backward compatible)
    """
    detailed_latency = []
    for job_data in jobs:
        if isinstance(job_data, dict):
            job = Job()
            job.from_dict(job_data)
        else:
            job = job_data

        job_id = job.job_id
        leader_id = getattr(job, "leader_id", None)
        if leader_id is None:
            continue
        if job.state not in [ObjectState.READY, ObjectState.RUNNING, ObjectState.COMPLETE]:
            continue

        # Ensure numeric/float for reasoning_time (0.0 if missing)
        reasoning_time = getattr(job, "reasoning_time", None)
        try:
            reasoning_time = float(reasoning_time) if reasoning_time is not None else 0.0
        except Exception:
            reasoning_time = 0.0

        # Extract timestamps for the specified level
        submitted_at_dict = job.submitted_at_dict
        selection_started_at_dict = job.selection_started_at_dict
        assigned_at_dict = job.assigned_at_dict
        started_at_dict = job.started_at_dict
        completed_at_dict = job.completed_at_dict

        # Choose timestamp based on level
        if level is not None:
            # Use timestamps at the specified level (if available)
            submitted_at = submitted_at_dict.get(level, submitted_at_dict.get(0, 0))
            selection_started_at = selection_started_at_dict.get(level, 0)
            assigned_at = assigned_at_dict.get(level, 0)
            started_at = started_at_dict.get(level, started_at_dict.get(0, 0))
            completed_at = completed_at_dict.get(level, completed_at_dict.get(0, 0))
        else:
            # Backward compatible: use last timestamps (max values)
            submitted_at = submitted_at_dict.get(0, 0) if submitted_at_dict else 0
            selection_started_at = max(selection_started_at_dict.values()) if selection_started_at_dict else 0
            assigned_at = max(assigned_at_dict.values()) if assigned_at_dict else 0
            started_at = max(started_at_dict.values()) if started_at_dict else 0
            completed_at = max(completed_at_dict.values()) if completed_at_dict else 0

        # Calculate scheduling latency for this level
        scheduling_latency = assigned_at - submitted_at if assigned_at and submitted_at else 0

        # IMPORTANT: leader_id before reasoning_time to match header
        detailed_latency.append([
            job_id,
            submitted_at,
            selection_started_at,
            assigned_at,
            started_at,
            completed_at,
            job.exit_status if job.exit_status is not None else 0,
            leader_id,
            reasoning_time,
            scheduling_latency,
        ])

    with open(path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'job_id', 'submitted_at', 'selection_started_at', 'assigned_at',
            'started_at', 'completed_at', 'exit_status', 'leader_id', 'reasoning_time', 'scheduling_latency',
        ])
        writer.writerows(detailed_latency)

def plot_agent_loads_from_dir(run_dir):
    agent_data = {}
    mean_loads = {}
    max_loads = {}

    # Read all agent_loads_*.csv files
    for filename in sorted(os.listdir(run_dir)):
        if filename.startswith("agent_loads_") and filename.endswith(".csv"):
            file_path = os.path.join(run_dir, filename)
            df = pd.read_csv(file_path)

            # Skip empty files
            if df.empty or "agent_id" not in df.columns:
                print(f"Skipping empty or malformed file: {filename}")
                continue

            agent_id = df["agent_id"].iloc[0]
            df["relative_time"] = df["timestamp"] - df["timestamp"].min()
            agent_data[agent_id] = df
            mean_loads[agent_id] = df["load"].mean()
            max_loads[agent_id] = df["load"].max()

    # Plot load over time
    plt.figure(figsize=(12, 6))
    for agent_id, df in agent_data.items():
        plt.plot(df["relative_time"], df["load"], label=f"Agent {agent_id}")
    plt.xlabel("Time (s since start)")
    plt.ylabel("Load")
    plt.title("Agent Load Over Time")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(run_dir, "agent_loads_over_time.png"))
    print(f"Saved: {os.path.join(run_dir, 'agent_loads_over_time.png')}")

    # Plot mean and max loads
    fig, ax = plt.subplots(figsize=(10, 5))
    x = list(mean_loads.keys())
    mean_vals = [mean_loads[k] for k in x]
    max_vals = [max_loads[k] for k in x]

    width = 0.35
    ax.bar([i - width/2 for i in range(len(x))], mean_vals, width, label='Mean Load')
    ax.bar([i + width/2 for i in range(len(x))], max_vals, width, label='Max Load')

    ax.set_xticks(range(len(x)))
    ax.set_xticklabels([f"Agent {aid}" for aid in x])
    ax.set_ylabel("Load")
    ax.set_title("Mean and Max Load per Agent")
    ax.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(run_dir, "agent_loads_summary.png"))
    print(f"Saved: {os.path.join(run_dir, 'agent_loads_summary.png')}")

def plot_agent_loads(output_dir: str, repo: Repository | None = None, save_csv: bool = False, skip_plots: bool = False):
    """
    Prefer Redis metrics (load_trace per agent).
    Fallback to plot_agent_loads_from_dir(output_dir) if Redis has nothing.

    Args:
        output_dir: Directory to save outputs
        repo: Repository instance for fetching metrics from Redis
        save_csv: If True, export load data to CSV files
        skip_plots: If True, skip plot generation
    """
    if repo is None:
        return plot_agent_loads_from_dir(output_dir)

    metrics = load_metrics_from_repo(repo)
    if not metrics:
        return plot_agent_loads_from_dir(output_dir)

    save_metrics(path=f"{output_dir}/metrics.json", metrics=metrics)

    agent_data: dict[int, pd.DataFrame] = {}
    mean_loads: dict[int, float] = {}
    max_loads: dict[int, float] = {}

    # Build per-agent DataFrames from load_trace
    for agent_id, m in sorted(metrics.items(), key=lambda x: x[0]):
        trace = m.get("load_trace", []) or []
        if not trace:
            continue
        # Ensure well-formed [ts, load]
        ts = []
        ld = []
        for pair in trace:
            if (isinstance(pair, (list, tuple)) and len(pair) == 2 and
                isinstance(pair[0], (int, float)) and isinstance(pair[1], (int, float))):
                ts.append(float(pair[0]))
                ld.append(float(pair[1]))
        if not ts:
            continue
        df = pd.DataFrame({"timestamp": ts, "load": ld})
        df["relative_time"] = df["timestamp"] - df["timestamp"].min()
        agent_data[agent_id] = df
        mean_loads[agent_id] = float(df["load"].mean())
        max_loads[agent_id] = float(df["load"].max())

    if not agent_data:
        return  # nothing to plot

    # Export CSV if requested
    if save_csv:
        # Export individual agent load traces
        for agent_id, df in agent_data.items():
            csv_path = os.path.join(output_dir, f"agent_{agent_id}_load_trace.csv")
            df.to_csv(csv_path, index=False)

        # Export summary statistics
        summary_data = []
        for agent_id in sorted(mean_loads.keys()):
            summary_data.append({
                "agent_id": agent_id,
                "mean_load": mean_loads[agent_id],
                "max_load": max_loads[agent_id]
            })
        summary_df = pd.DataFrame(summary_data)
        summary_csv = os.path.join(output_dir, "agent_loads_summary.csv")
        summary_df.to_csv(summary_csv, index=False)
        print(f"Saved: {summary_csv}")

    if skip_plots:
        return

    # Plot load over time
    plt.figure(figsize=(12, 6))
    for agent_id, df in agent_data.items():
        plt.plot(df["relative_time"], df["load"], label=f"Agent {agent_id}")
    plt.xlabel("Time (s since start)")
    plt.ylabel("Load")
    plt.title("Agent Load Over Time")
    plt.legend(ncol=2)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "agent_loads_over_time.png"))
    plt.close()

    # Plot mean and max loads
    x = list(sorted(mean_loads.keys()))
    mean_vals = [mean_loads[k] for k in x]
    max_vals = [max_loads[k] for k in x]

    fig, ax = plt.subplots(figsize=(10, 5))
    width = 0.35
    ax.bar([i - width / 2 for i in range(len(x))], mean_vals, width, label='Mean Load')
    ax.bar([i + width / 2 for i in range(len(x))], max_vals, width, label='Max Load')
    ax.set_xticks(range(len(x)))
    ax.set_xticklabels([f"Agent {aid}" for aid in x], rotation=45)
    ax.set_ylabel("Load")
    ax.set_title("Mean and Max Load per Agent")
    ax.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "agent_loads_summary.png"))
    plt.close()


def plot_conflicts_and_restarts_by_agent(output_dir: str, repo: Repository | None = None, save_csv: bool = False, skip_plots: bool = False):
    """
    Prefer Redis metrics; if missing, fall back to misc_<agent_id>.json files under output_dir.

    Produces:
      - conflicts_per_agent.png
      - restarts_per_agent.png

    Args:
        output_dir: Directory to save outputs
        repo: Repository instance for fetching metrics from Redis
        save_csv: If True, export conflicts/restarts data to CSV files
        skip_plots: If True, skip plot generation

    Returns: (agent_conflict_counts, agent_restart_counts)
    """
    agent_conflict_counts: dict[int, int] = {}
    agent_restart_counts: dict[int, int] = {}

    used_redis = False
    if repo is not None:
        metrics = load_metrics_from_repo(repo)
        if metrics:
            save_metrics(path=f"{output_dir}/metrics.json", metrics=metrics)
            used_redis = True
            for agent_id, m in metrics.items():
                conflicts = m.get("conflicts", {}) or {}
                restarts = m.get("restarts", {}) or {}
                agent_conflict_counts[agent_id] = int(sum(int(v) for v in conflicts.values()))
                agent_restart_counts[agent_id] = int(sum(int(v) for v in restarts.values()))

    if not used_redis:
        # Fallback: read misc_<agent_id>.json files from disk
        for fname in os.listdir(output_dir):
            if fname.startswith("misc_") and fname.endswith(".json"):
                try:
                    agent_id = int(fname.split("_")[1].split(".")[0])
                except Exception:
                    continue
                with open(os.path.join(output_dir, fname), "r") as f:
                    data = json.load(f)
                total_conflicts = sum((data.get("conflicts", {}) or {}).values())
                total_restarts = sum((data.get("restarts", {}) or {}).values())
                agent_conflict_counts[agent_id] = int(total_conflicts)
                agent_restart_counts[agent_id] = int(total_restarts)

    # Export CSV if requested
    if save_csv:
        if agent_conflict_counts:
            df_conflicts = pd.DataFrame(
                sorted(agent_conflict_counts.items(), key=lambda x: x[0]),
                columns=["agent_id", "conflicts"]
            )
            csv_path = os.path.join(output_dir, "conflicts_per_agent.csv")
            df_conflicts.to_csv(csv_path, index=False)
            print(f"Saved: {csv_path}")

        if agent_restart_counts:
            df_restarts = pd.DataFrame(
                sorted(agent_restart_counts.items(), key=lambda x: x[0]),
                columns=["agent_id", "restarts"]
            )
            csv_path = os.path.join(output_dir, "restarts_per_agent.csv")
            df_restarts.to_csv(csv_path, index=False)
            print(f"Saved: {csv_path}")

    if skip_plots:
        return agent_conflict_counts, agent_restart_counts

    # Plot (only if we have any)
    if agent_conflict_counts:
        df_conflicts = pd.DataFrame(
            sorted(agent_conflict_counts.items(), key=lambda x: x[0]),
            columns=["agent_id", "conflicts"]
        )
        plt.figure(figsize=(10, 6))
        plt.bar(df_conflicts["agent_id"], df_conflicts["conflicts"])
        plt.xlabel("Agent ID")
        plt.ylabel("Total Conflicts")
        plt.title("Total Conflicts per Agent")
        plt.grid(axis="y")
        plt.savefig(os.path.join(output_dir, "conflicts_per_agent.png"), bbox_inches="tight")
        plt.close()

    if agent_restart_counts:
        df_restarts = pd.DataFrame(
            sorted(agent_restart_counts.items(), key=lambda x: x[0]),
            columns=["agent_id", "restarts"]
        )
        plt.figure(figsize=(10, 6))
        plt.bar(df_restarts["agent_id"], df_restarts["restarts"])
        plt.xlabel("Agent ID")
        plt.ylabel("Total Restarts")
        plt.title("Total Restarts per Agent")
        plt.grid(axis="y")
        plt.savefig(os.path.join(output_dir, "restarts_per_agent.png"), bbox_inches="tight")
        plt.close()

    return agent_conflict_counts, agent_restart_counts


def plot_conflicts_and_restarts(output_dir: str, repo: Repository | None = None, save_csv: bool = False, skip_plots: bool = False):
    """
    Prefer Redis metrics; fallback to reading misc_*.json files.
    Produces:
      - conflicts_per_job.png
      - restarts_per_job.png

    Args:
        output_dir: Directory to save outputs
        repo: Repository instance for fetching metrics from Redis
        save_csv: If True, export conflicts/restarts data to CSV files
        skip_plots: If True, skip plot generation

    Returns: (df_conflicts, df_restarts) which may be empty DataFrames.
    """
    all_conflicts: dict[int, int] = defaultdict(int)
    all_restarts: dict[int, int] = defaultdict(int)

    used_redis = False
    if repo is not None:
        metrics = load_metrics_from_repo(repo)
        if metrics:
            save_metrics(path=f"{output_dir}/metrics.json", metrics=metrics)
            used_redis = True
            for _agent_id, m in metrics.items():
                for job_id, cnt in (m.get("conflicts", {}) or {}).items():
                    try:
                        all_conflicts[int(job_id)] += int(cnt)
                    except Exception:
                        continue
                for job_id, cnt in (m.get("restarts", {}) or {}).items():
                    try:
                        all_restarts[int(job_id)] += int(cnt)
                    except Exception:
                        continue

    if not used_redis:
        # Fallback: disk
        for fname in os.listdir(output_dir):
            if fname.startswith("misc_") and fname.endswith(".json"):
                with open(os.path.join(output_dir, fname), "r") as f:
                    data = json.load(f)
                for job_id, cnt in (data.get("conflicts", {}) or {}).items():
                    try:
                        all_conflicts[int(job_id)] += int(cnt)
                    except Exception:
                        continue
                for job_id, cnt in (data.get("restarts", {}) or {}).items():
                    try:
                        all_restarts[int(job_id)] += int(cnt)
                    except Exception:
                        continue

    df_conflicts = pd.DataFrame(sorted(all_conflicts.items(), key=lambda x: x[0]),
                                columns=["job_id", "conflicts"])
    df_restarts = pd.DataFrame(sorted(all_restarts.items(), key=lambda x: x[0]),
                               columns=["job_id", "restarts"])

    # Export CSV if requested
    if save_csv:
        if not df_conflicts.empty:
            csv_path = os.path.join(output_dir, "conflicts_per_job.csv")
            df_conflicts.to_csv(csv_path, index=False)
            print(f"Saved: {csv_path}")

        if not df_restarts.empty:
            csv_path = os.path.join(output_dir, "restarts_per_job.csv")
            df_restarts.to_csv(csv_path, index=False)
            print(f"Saved: {csv_path}")

    if skip_plots:
        return df_conflicts, df_restarts

    if not df_conflicts.empty:
        plt.figure(figsize=(12, 6))
        plt.bar(df_conflicts["job_id"], df_conflicts["conflicts"])
        plt.xlabel("Job ID")
        plt.ylabel("Conflict Count")
        plt.title("Conflicts per Job (aggregated across agents)")
        plt.grid(axis="y")
        plt.savefig(os.path.join(output_dir, "conflicts_per_job.png"), bbox_inches="tight")
        plt.close()

    if not df_restarts.empty:
        plt.figure(figsize=(12, 6))
        plt.bar(df_restarts["job_id"], df_restarts["restarts"])
        plt.xlabel("Job ID")
        plt.ylabel("Restart Count")
        plt.title("Restarts per Job (aggregated across agents)")
        plt.grid(axis="y")
        plt.savefig(os.path.join(output_dir, "restarts_per_job.png"), bbox_inches="tight")
        plt.close()

    return df_conflicts, df_restarts


def plot_scheduling_latency_and_jobs(run_dir: str,
                                     agent_count: int,
                                     exclude_job_ids: set[int] | None = None,
                                     label_suffix: str = "",
                                     level: int | None = None,
                                     save_csv: bool = False,
                                     skip_plots: bool = False):
    """
    Plot latency, jobs/agent, and histogram for a specific hierarchy level.

    Args:
        run_dir: Output directory containing job CSVs
        agent_count: Number of agents
        exclude_job_ids: Set of job IDs to exclude (e.g., restarted jobs)
        label_suffix: Suffix for output filenames (e.g., '_no_restarts')
        level: Hierarchy level to plot (0, 1, etc.). If None, uses level0_jobs.csv
        save_csv: If True, export scheduling data to CSV files
        skip_plots: If True, skip plot generation
    """
    # Choose CSV file based on level
    if level is not None:
        csv_file = f"{run_dir}/level{level}_jobs.csv"
    else:
        csv_file = f"{run_dir}/level0_jobs.csv"

    if not os.path.exists(csv_file):
        print(f"CSV file not found: {csv_file}")
        return

    df = pd.read_csv(csv_file)

    # Scheduling latency should already be computed in save_jobs, but recompute for safety
    if "scheduling_latency" not in df.columns:
        df["scheduling_latency"] = df["assigned_at"] - df["submitted_at"]

    # Optional filter (e.g., exclude restarted jobs)
    if exclude_job_ids:
        df = df[~df["job_id"].isin(exclude_job_ids)].copy()

    if df.empty:
        print(f"No rows to plot for label_suffix='{label_suffix}'.")
        return

    # Group by leader agent
    grouped = df.groupby("leader_id", dropna=False)

    # Export CSV if requested
    if save_csv:
        # Export scheduling latency per job
        latency_csv = os.path.join(run_dir, f"scheduling_latency_per_job{label_suffix}.csv")
        df[["job_id", "leader_id", "scheduling_latency"]].to_csv(latency_csv, index=False)
        print(f"Saved: {latency_csv}")

        # Export jobs per agent summary
        jobs_per_agent_data = grouped.size().reset_index(name='job_count')
        jobs_csv = os.path.join(run_dir, f"jobs_per_agent{label_suffix}.csv")
        jobs_per_agent_data.to_csv(jobs_csv, index=False)
        print(f"Saved: {jobs_csv}")

        # Export latency statistics
        stats_data = {
            "metric": ["mean_latency", "max_latency", "min_latency", "median_latency", "std_latency"],
            "value": [
                df['scheduling_latency'].mean(),
                df['scheduling_latency'].max(),
                df['scheduling_latency'].min(),
                df['scheduling_latency'].median(),
                df['scheduling_latency'].std()
            ]
        }
        stats_csv = os.path.join(run_dir, f"scheduling_latency_stats{label_suffix}.csv")
        pd.DataFrame(stats_data).to_csv(stats_csv, index=False)
        print(f"Saved: {stats_csv}")

    if skip_plots:
        return

    # Scatter: scheduling latency per job
    plt.figure(figsize=(12, 6))
    for agent_id, group in grouped:
        plt.scatter(group["job_id"], group["scheduling_latency"],
                    label=f"Agent {int(agent_id)}" if pd.notna(agent_id) else "Agent NA",
                    s=12)
    plt.xlabel("Job ID")
    plt.ylabel("Scheduling Latency (s)")
    level_text = f" - Level {level}" if level is not None else ""
    title_extra = " (excluding restarted jobs)" if label_suffix else ""
    plt.title(f"SWARM-MULTI: Scheduling Latency per Job{level_text} "
              f"(Mean: {df['scheduling_latency'].mean():.4f} s / Agents {agent_count}){title_extra}")
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(run_dir, f"selection_latency_per_job{label_suffix}.png"), bbox_inches="tight")
    plt.close()

    # Jobs per agent (bar plot) — for the filtered set
    jobs_per_agent = grouped.size()
    jobs_per_agent = jobs_per_agent.reindex(sorted(df["leader_id"].dropna().unique()))

    plt.figure(figsize=(10, 6))
    plt.bar(jobs_per_agent.index, jobs_per_agent.values)
    plt.xlabel("Agent ID")
    plt.ylabel("Number of Jobs")
    plt.title(f"Jobs per Agent{level_text}{title_extra}")
    plt.grid(axis="y")
    plt.xticks(jobs_per_agent.index)
    plt.savefig(os.path.join(run_dir, f"jobs_per_agent{label_suffix}.png"), bbox_inches="tight")
    plt.close()

    # Histogram of scheduling latency
    plt.figure(figsize=(10, 6))
    plt.hist(df["scheduling_latency"], bins=20, edgecolor='black', alpha=0.7)
    plt.xlabel("Scheduling Latency (s)")
    plt.ylabel("Frequency")
    plt.title(f"Distribution of Scheduling Latency{level_text}{title_extra}")
    plt.grid(axis="y")
    plt.savefig(os.path.join(run_dir, f"scheduling_latency_histogram{label_suffix}.png"), bbox_inches="tight")
    plt.close()

    # Print summary
    print(f"[{('no_restarts' if label_suffix else 'all')}] "
          f"Mean scheduling latency: {df['scheduling_latency'].mean():.4f} s")
    print(f"[{('no_restarts' if label_suffix else 'all')}] "
          f"Max scheduling latency: {df['scheduling_latency'].max():.4f} s")
    print(f"\n[{('no_restarts' if label_suffix else 'all')}] Jobs per agent:")
    for agent_id, count in jobs_per_agent.items():
        print(f"  Agent {int(agent_id)}: {int(count)} jobs")


def plot_reasoning_time(output_dir: str):
    csv_file = f"{output_dir}/all_jobs.csv"
    agents_csv = f"{output_dir}/all_agents.csv"

    if not os.path.exists(csv_file):
        print(f"{csv_file} not found")
        return

    # Check if there are LLM agents before generating reasoning plots
    agent_types, _ = identify_agent_types(agents_csv)
    has_llm_agents = any(agent_type == "llm" for agent_type in agent_types.values())

    if not has_llm_agents:
        print("No LLM agents detected - skipping reasoning time plots")
        return

    df = pd.read_csv(csv_file)
    # Make sure dtypes are right
    df['leader_id'] = pd.to_numeric(df['leader_id'], errors='coerce').astype('Int64')
    if 'reasoning_time' in df.columns:
        df['reasoning_time'] = pd.to_numeric(df['reasoning_time'], errors='coerce').fillna(0.0)

    # Compute scheduling latency if not present
    if "scheduling_latency" not in df.columns:
        df["scheduling_latency"] = df["assigned_at"] - df["submitted_at"]

    # Compute reasoning_time if stamps present
    if "reasoning_time" in df.columns:
        # Save summary CSV
        df[["job_id","leader_id","scheduling_latency","reasoning_time"]].to_csv(
            os.path.join(output_dir, "reasoning_vs_latency.csv"), index=False
        )

        # Scatter: reasoning vs scheduling latency
        plt.figure(figsize=(10,6))
        plt.scatter(df["reasoning_time"], df["scheduling_latency"], s=12, alpha=0.7)
        plt.xlabel("Reasoning Time (s)")
        plt.ylabel("Scheduling Latency (s)")
        plt.title("Reasoning Time vs Scheduling Latency (per job)")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "reasoning_vs_latency_scatter.png"))
        plt.close()

        # Bar: mean reasoning time per agent
        g = df.groupby("leader_id")["reasoning_time"].mean().sort_index()
        plt.figure(figsize=(10,6))
        plt.bar(g.index.astype(int), g.values)
        plt.xlabel("Agent ID")
        plt.ylabel("Mean Reasoning Time (s)")
        plt.title("Mean Reasoning Time per Agent")
        plt.grid(axis="y")
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "mean_reasoning_time_per_agent.png"))
        plt.close()
    else:
        print("Reasoning timestamps not present in all_jobs.csv; nothing to plot.")


def detect_agent_failures(output_dir: str, repo: Repository | None = None) -> tuple[list[dict], dict[int, str]]:
    """
    Detect agent failures and their timestamps, plus host:port mapping for all agents.

    Returns:
        - failures: list of failure events [{"agent_id": int, "failure_time": float, "host": str, "port": int}, ...]
        - agent_mapping: dict mapping agent_id -> "host:port"

    Tries multiple sources:
    1. Redis metrics (agent_failures field)
    2. Redis agent state transitions (ACTIVE -> FAILED)
    3. Agent logs (grepping for failure keywords)
    4. all_agents.csv if state field exists
    """
    failures = []
    agent_mapping = {}

    # Try Redis metrics first - check for agent_failures in metrics
    if repo is not None:
        try:
            metrics = load_metrics_from_repo(repo)
            for agent_id, m in metrics.items():
                agent_failures_data = m.get("failed_agents", {}) or {}
                for failed_agent_id, timestamp in agent_failures_data.items():
                    failures.append({
                        "agent_id": int(failed_agent_id),
                        "failure_time": float(timestamp),
                    })
        except Exception as e:
            print(f"Could not detect failures from Redis metrics: {e}")


    # Deduplicate failures by agent_id (keep first occurrence)
    seen_agents = set()
    unique_failures = []
    for f in failures:
        if f["agent_id"] not in seen_agents:
            seen_agents.add(f["agent_id"])
            unique_failures.append(f)

    return unique_failures, agent_mapping


def collect_reassignments(output_dir: str, repo: Repository | None = None, level: int = 0) -> dict[int, dict]:
    """
    Collect job reassignment data from Redis metrics.

    Returns:
        - reassignments: {job_id: {"from_agent": int, "to_agent": int, "timestamp": float, "reason": str}}
    """
    reassignments = {}

    # Try Redis metrics first
    if repo is not None:
        try:
            metrics = load_metrics_from_repo(repo)
            for agent_id, m in metrics.items():
                # Regular reassignments
                regular_reassignments = m.get("reassignments", {}) or {}
                for job_id, reassignment_info in regular_reassignments.items():
                    if isinstance(reassignment_info, dict):
                        if int(job_id) not in reassignments:
                            job_obj = repo.get(job_id)
                            to_agent = job_obj.get("leader_id", -1)
                            print(f"KOMAl --- {to_agent}")
                            reassignments[int(job_id)] = {
                                "from_agent": reassignment_info.get("failed_agent", -1),
                                "to_agent": to_agent,
                                "timestamp": reassignment_info.get("reassigned_at", 0),
                                "reason": reassignment_info.get("reason", "failure"),
                                "type": "regular"
                            }

                # Delegation reassignments
                delegation_reassignments = m.get("delegation_reassignments", {}) or {}
                for job_id, reassignment_info in delegation_reassignments.items():
                    if isinstance(reassignment_info, dict):
                        # Don't overwrite if already exists
                        if int(job_id) not in reassignments:
                            job_obj = repo.get(job_id)
                            to_agent = job_obj.get("leader_id", -1)
                            print(f"KOMAl --- {to_agent}")
                            reassignments[int(job_id)] = {
                                "from_agent": reassignment_info.get("from_agent", -1),
                                "to_agent": to_agent,  # Current agent took over
                                "timestamp": reassignment_info.get("timestamp", 0),
                                "reason": reassignment_info.get("reason", "delegation"),
                                "type": "delegation"
                            }
        except Exception as e:
            print(f"Could not collect reassignments from Redis: {e}")

    return reassignments


def plot_failure_summary(output_dir: str, repo: Repository | None = None):
    """
    Create summary visualization showing:
    - Total failed agents
    - Total reassigned jobs (regular + delegation)
    - Failed agents by ID
    - Reassigned jobs timeline
    """
    failures, _ = detect_agent_failures(output_dir, repo)
    print("Total failed agents:", len(failures))
    reassignments = collect_reassignments(output_dir, repo)
    print("Total reassigned jobs:", len(reassignments))

    if not failures and not reassignments:
        print("No failure or reassignment data to visualize")
        return

    # Create figure with 4 subplots
    fig = plt.figure(figsize=(16, 12))
    gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)

    # Plot 1: Summary statistics (top left)
    ax1 = fig.add_subplot(gs[0, 0])
    categories = ['Failed\nAgents', 'Reassigned\nJobs']
    values = [len(failures), len(reassignments)]
    colors = ['#e74c3c', '#f39c12']

    bars = ax1.bar(categories, values, color=colors, alpha=0.8, edgecolor='black', linewidth=2)
    ax1.set_ylabel('Count', fontsize=12, fontweight='bold')
    ax1.set_title('Failure Impact Summary', fontsize=14, fontweight='bold')
    ax1.grid(axis='y', alpha=0.3)

    # Add value labels on bars
    for bar, val in zip(bars, values):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(val)}',
                ha='center', va='bottom', fontsize=14, fontweight='bold')

    # Plot 2: Failed agents by ID (top right)
    ax2 = fig.add_subplot(gs[0, 1])
    if failures:
        failed_agent_ids = sorted([f["agent_id"] for f in failures])
        ax2.bar(range(len(failed_agent_ids)), [1]*len(failed_agent_ids),
                color='#e74c3c', alpha=0.8, edgecolor='black', linewidth=1.5)
        ax2.set_xticks(range(len(failed_agent_ids)))
        ax2.set_xticklabels([f'Agent\n{aid}' for aid in failed_agent_ids], fontsize=9)
        ax2.set_ylabel('Failed', fontsize=12, fontweight='bold')
        ax2.set_title(f'Failed Agents (n={len(failed_agent_ids)})', fontsize=14, fontweight='bold')
        ax2.set_ylim(0, 1.5)
        ax2.set_yticks([0, 1])
        ax2.set_yticklabels(['', 'FAILED'])
        ax2.grid(axis='y', alpha=0.3)
    else:
        ax2.text(0.5, 0.5, 'No Agent Failures Detected',
                ha='center', va='center', fontsize=12, transform=ax2.transAxes)
        ax2.set_title('Failed Agents', fontsize=14, fontweight='bold')

    # Plot 3: Reassignments by type (middle left)
    ax3 = fig.add_subplot(gs[1, 0])
    if reassignments:
        reassignment_types = defaultdict(int)
        for job_id, info in reassignments.items():
            reassignment_types[info.get("type", "unknown")] += 1

        types = list(reassignment_types.keys())
        counts = list(reassignment_types.values())
        type_colors = {'regular': '#e67e22', 'delegation': '#3498db', 'unknown': '#95a5a6'}
        bar_colors = [type_colors.get(t, '#95a5a6') for t in types]

        bars = ax3.bar(types, counts, color=bar_colors, alpha=0.8, edgecolor='black', linewidth=1.5)
        ax3.set_ylabel('Count', fontsize=12, fontweight='bold')
        ax3.set_title('Reassignments by Type', fontsize=14, fontweight='bold')
        ax3.grid(axis='y', alpha=0.3)

        # Add value labels
        for bar, val in zip(bars, counts):
            height = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(val)}',
                    ha='center', va='bottom', fontsize=12, fontweight='bold')
    else:
        ax3.text(0.5, 0.5, 'No Job Reassignments Detected',
                ha='center', va='center', fontsize=12, transform=ax3.transAxes)
        ax3.set_title('Reassignments by Type', fontsize=14, fontweight='bold')

    # Plot 4: Reassignment reasons (middle right)
    ax4 = fig.add_subplot(gs[1, 1])
    if reassignments:
        reassignment_reasons = defaultdict(int)
        for job_id, info in reassignments.items():
            reassignment_reasons[info.get("reason", "unknown")] += 1

        reasons = list(reassignment_reasons.keys())
        counts = list(reassignment_reasons.values())
        reason_colors = {'failure': '#e74c3c', 'delegation': '#3498db', 'timeout': '#f39c12', 'unknown': '#95a5a6'}
        bar_colors = [reason_colors.get(r, '#95a5a6') for r in reasons]

        bars = ax4.bar(reasons, counts, color=bar_colors, alpha=0.8, edgecolor='black', linewidth=1.5)
        ax4.set_ylabel('Count', fontsize=12, fontweight='bold')
        ax4.set_title('Reassignment Reasons', fontsize=14, fontweight='bold')
        ax4.grid(axis='y', alpha=0.3)

        # Add value labels
        for bar, val in zip(bars, counts):
            height = bar.get_height()
            ax4.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(val)}',
                    ha='center', va='bottom', fontsize=12, fontweight='bold')
    else:
        ax4.text(0.5, 0.5, 'No Reassignment Data',
                ha='center', va='center', fontsize=12, transform=ax4.transAxes)
        ax4.set_title('Reassignment Reasons', fontsize=14, fontweight='bold')

    # Plot 5: Timeline of failures and reassignments (bottom, spans both columns)
    ax5 = fig.add_subplot(gs[2, :])

    # Get start time from jobs
    csv_file = f"{output_dir}/all_jobs.csv"
    if os.path.exists(csv_file):
        df = pd.read_csv(csv_file)
        if not df.empty and "submitted_at" in df.columns:
            start_time = df["submitted_at"].min()

            # Plot failures
            failure_times = []
            failure_agents = []
            for f in failures:
                if f.get("failure_time"):
                    failure_times.append(f["failure_time"] - start_time)
                    failure_agents.append(f["agent_id"])

            if failure_times:
                ax5.scatter(failure_times, [1]*len(failure_times),
                           s=200, c='#e74c3c', marker='X', alpha=0.8,
                           edgecolors='black', linewidths=2, label='Agent Failures', zorder=3)

                # Add agent ID labels
                for t, aid in zip(failure_times, failure_agents):
                    ax5.annotate(f'A{aid}', (t, 1), xytext=(0, 10),
                               textcoords='offset points', ha='center',
                               fontsize=9, fontweight='bold')

            # Plot reassignments
            reassignment_times = []
            reassignment_jobs = []
            for job_id, info in reassignments.items():
                timestamp = info.get("timestamp", 0)
                if timestamp:
                    reassignment_times.append(timestamp - start_time)
                    reassignment_jobs.append(job_id)

            if reassignment_times:
                ax5.scatter(reassignment_times, [0.5]*len(reassignment_times),
                           s=100, c='#f39c12', marker='o', alpha=0.7,
                           edgecolors='black', linewidths=1.5, label='Job Reassignments', zorder=3)

            ax5.set_xlabel('Time (seconds from start)', fontsize=12, fontweight='bold')
            ax5.set_ylabel('Event Type', fontsize=12, fontweight='bold')
            ax5.set_yticks([0.5, 1])
            ax5.set_yticklabels(['Reassignments', 'Failures'])
            ax5.set_title('Timeline of Failure Events', fontsize=14, fontweight='bold')
            ax5.legend(loc='upper right', fontsize=11)
            ax5.grid(True, alpha=0.3)
            ax5.set_ylim(0, 1.5)
        else:
            ax5.text(0.5, 0.5, 'No timeline data available',
                    ha='center', va='center', fontsize=12, transform=ax5.transAxes)
    else:
        ax5.text(0.5, 0.5, 'No job data available for timeline',
                ha='center', va='center', fontsize=12, transform=ax5.transAxes)

    plt.savefig(os.path.join(output_dir, "failure_summary.png"), dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Saved: {os.path.join(output_dir, 'failure_summary.png')}")
    print(f"  - Failed agents: {len(failures)}")
    print(f"  - Reassigned jobs: {len(reassignments)}")

    # Save detailed data to CSV
    if failures:
        failures_df = pd.DataFrame(failures)
        failures_df.to_csv(os.path.join(output_dir, "failed_agents.csv"), index=False)
        print(f"Saved: {os.path.join(output_dir, 'failed_agents.csv')}")

    if reassignments:
        reassignments_list = [{"job_id": jid, **info} for jid, info in reassignments.items()]
        reassignments_df = pd.DataFrame(reassignments_list)
        reassignments_df.to_csv(os.path.join(output_dir, "reassigned_jobs.csv"), index=False)
        print(f"Saved: {os.path.join(output_dir, 'reassigned_jobs.csv')}")


def plot_timeline_with_failures(output_dir: str, repo: Repository | None = None,
                                  window_size: int = 10):
    """
    Timeline visualization showing:
    - Throughput (jobs/sec) over time
    - Mean latency over time
    - Active agent count over time
    - Vertical markers for agent failures

    Args:
        output_dir: Output directory with all_jobs.csv
        repo: Redis repository for agent state
        window_size: Time window in seconds for throughput calculation
    """
    csv_file = f"{output_dir}/all_jobs.csv"
    if not os.path.exists(csv_file):
        print(f"{csv_file} not found")
        return

    df = pd.read_csv(csv_file)

    # Filter to completed jobs only
    df = df[df["completed_at"].notna()].copy()
    if df.empty:
        print("No completed jobs to plot")
        return

    # Compute scheduling latency
    df["scheduling_latency"] = df["assigned_at"] - df["submitted_at"]

    # Get experiment start/end times
    start_time = df["submitted_at"].min()
    end_time = df["completed_at"].max()
    total_duration = end_time - start_time

    # FIX: Check for sufficient duration
    if total_duration < window_size:
        print(f"Total duration ({total_duration:.1f}s) is less than window size ({window_size}s) - cannot create time bins")
        return

    # Create time windows
    time_bins = list(range(int(start_time), int(end_time) + window_size, window_size))

    # FIX: Ensure we have at least 2 bins
    if len(time_bins) < 2:
        print(f"Insufficient time bins ({len(time_bins)}) - need at least 2 for analysis")
        return

    # Calculate throughput (jobs completed per window)
    df["completion_bin"] = pd.cut(df["completed_at"], bins=time_bins, labels=False, include_lowest=True)
    throughput_series = df.groupby("completion_bin").size()
    throughput_times = [start_time + (i * window_size) + (window_size / 2) for i in throughput_series.index]
    throughput_values = throughput_series.values / window_size  # jobs per second

    # Calculate mean latency per window
    latency_series = df.groupby("completion_bin")["scheduling_latency"].mean()
    latency_times = [start_time + (i * window_size) + (window_size / 2) for i in latency_series.index]
    latency_values = latency_series.values

    # Calculate active agents over time (approximate from job assignments)
    active_agents_series = df.groupby("completion_bin")["leader_id"].nunique()
    active_times = [start_time + (i * window_size) + (window_size / 2) for i in active_agents_series.index]
    active_counts = active_agents_series.values

    # Detect failures
    failures, _ = detect_agent_failures(output_dir, repo)

    # Convert times to relative (seconds from start)
    throughput_times_rel = [t - start_time for t in throughput_times]
    latency_times_rel = [t - start_time for t in latency_times]
    active_times_rel = [t - start_time for t in active_times]

    failure_times_rel = []
    failure_agents = []
    for f in failures:
        if f and f.get("failure_time"):
            failure_times_rel.append(f["failure_time"] - start_time)
            failure_agents.append(f["agent_id"])

    # Create figure with 3 subplots
    fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)

    # Plot 1: Throughput over time
    axes[0].plot(throughput_times_rel, throughput_values, 'b-', linewidth=2, marker='o', markersize=4)
    axes[0].set_ylabel("Throughput (jobs/sec)", fontsize=11)
    axes[0].set_title("System Performance Timeline with Failure Events", fontsize=13, fontweight='bold')
    axes[0].grid(True, alpha=0.3)

    # Mark failures on throughput plot
    for ft, fa in zip(failure_times_rel, failure_agents):
        axes[0].axvline(x=ft, color='red', linestyle='--', alpha=0.7, linewidth=2)
        axes[0].text(ft, axes[0].get_ylim()[1] * 0.95, f'Agent {fa}\nfailed',
                    rotation=0, ha='center', va='top', fontsize=8,
                    bbox=dict(boxstyle='round', facecolor='red', alpha=0.3))

    # Plot 2: Mean Latency over time
    axes[1].plot(latency_times_rel, latency_values, 'g-', linewidth=2, marker='s', markersize=4)
    axes[1].set_ylabel("Mean Latency (sec)", fontsize=11)
    axes[1].grid(True, alpha=0.3)

    # Mark failures on latency plot
    for ft in failure_times_rel:
        axes[1].axvline(x=ft, color='red', linestyle='--', alpha=0.7, linewidth=2)

    # Plot 3: Active agents over time
    axes[2].plot(active_times_rel, active_counts, 'purple', linewidth=2, marker='^', markersize=4)
    axes[2].set_ylabel("Active Agents", fontsize=11)
    axes[2].set_xlabel("Time (seconds from start)", fontsize=11)
    axes[2].grid(True, alpha=0.3)

    # Mark failures on active agents plot
    for ft in failure_times_rel:
        axes[2].axvline(x=ft, color='red', linestyle='--', alpha=0.7, linewidth=2)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "timeline_with_failures.png"), dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Saved: {os.path.join(output_dir, 'timeline_with_failures.png')}")
    if failures:
        print(f"Detected {len(failures)} agent failure(s)")
    else:
        print("No agent failures detected")


def analyze_throughput_degradation(output_dir: str, repo: Repository | None = None,
                                     window_size: int = 10,
                                     baseline_duration: int = 30):
    """
    Analyze throughput degradation due to failures.

    Calculates:
    - Baseline throughput (first N seconds before any failure)
    - Throughput during failure period
    - Throughput degradation percentage
    - Recovery time (time to return to 95% of baseline)

    Args:
        output_dir: Output directory with all_jobs.csv
        repo: Redis repository for agent state
        window_size: Time window in seconds for throughput calculation
        baseline_duration: Duration in seconds to establish baseline (pre-failure)
    """
    csv_file = f"{output_dir}/all_jobs.csv"
    if not os.path.exists(csv_file):
        print(f"{csv_file} not found")
        return

    df = pd.read_csv(csv_file)
    df = df[df["completed_at"].notna()].copy()

    if df.empty:
        print("No completed jobs for degradation analysis")
        return

    start_time = df["submitted_at"].min()
    end_time = df["completed_at"].max()

    # Detect failures
    failures, _ = detect_agent_failures(output_dir, repo)

    if not failures:
        print("No failures detected - cannot compute degradation metrics")
        # Still compute overall throughput
        total_jobs = len(df)
        duration = end_time - start_time
        overall_throughput = total_jobs / duration if duration > 0 else 0
        print(f"Overall throughput: {overall_throughput:.4f} jobs/sec ({total_jobs} jobs in {duration:.2f}s)")
        return

    # Find first failure time
    failure_times = [f["failure_time"] for f in failures if f and f.get("failure_time")]
    if not failure_times:
        print("Failure times not available")
        return

    first_failure = min(failure_times)

    # Calculate baseline throughput (before first failure)
    baseline_end = min(first_failure, start_time + baseline_duration)
    baseline_df = df[df["completed_at"] <= baseline_end]

    if len(baseline_df) < 5:
        print(f"Insufficient baseline data ({len(baseline_df)} jobs) - using first {baseline_duration}s regardless of failures")
        baseline_df = df[df["completed_at"] <= (start_time + baseline_duration)]

    baseline_duration_actual = baseline_end - start_time
    baseline_throughput = len(baseline_df) / baseline_duration_actual if baseline_duration_actual > 0 else 0

    # Calculate throughput during failure period (failure + 60s)
    failure_period_end = first_failure + 60  # 60 seconds after first failure
    failure_df = df[(df["completed_at"] >= first_failure) & (df["completed_at"] <= failure_period_end)]
    failure_duration = min(failure_period_end - first_failure, end_time - first_failure)
    failure_throughput = len(failure_df) / failure_duration if failure_duration > 0 else 0

    # Calculate degradation
    if baseline_throughput > 0:
        degradation_pct = ((baseline_throughput - failure_throughput) / baseline_throughput) * 100
    else:
        degradation_pct = 0

    # Find recovery time (when throughput returns to 95% of baseline)
    target_throughput = baseline_throughput * 0.95
    recovery_time = None

    # Create sliding windows after failure
    # FIX: Check if we have enough data after failure for binning
    time_after_failure = end_time - first_failure
    if time_after_failure >= window_size:
        time_bins = list(range(int(first_failure), int(end_time) + window_size, window_size))

        # Ensure we have at least 2 bins for pd.cut to work
        if len(time_bins) >= 2:
            df["completion_bin"] = pd.cut(df["completed_at"], bins=time_bins, labels=False, include_lowest=True)

            for bin_idx in sorted(df["completion_bin"].dropna().unique()):
                bin_jobs = df[df["completion_bin"] == bin_idx]
                bin_throughput = len(bin_jobs) / window_size

                if bin_throughput >= target_throughput:
                    bin_start = first_failure + (bin_idx * window_size)
                    recovery_time = bin_start - first_failure
                    break
        else:
            print(f"Insufficient time bins ({len(time_bins)}) for recovery analysis")
    else:
        print(f"Insufficient data after failure ({time_after_failure:.1f}s < {window_size}s) for recovery analysis")

    # Get reassignment data for additional context
    reassignments = collect_reassignments(output_dir, repo)

    # Generate visualization
    fig, axes = plt.subplots(3, 1, figsize=(12, 10))

    # Subplot 1: Throughput comparison bar chart
    categories = ['Baseline\n(pre-failure)', 'During Failure\n(+60s)', 'Degradation']
    values = [baseline_throughput, failure_throughput, degradation_pct]
    colors = ['green', 'orange', 'red']

    axes[0].bar(categories[:2], values[:2], color=colors[:2], alpha=0.7, edgecolor='black')
    axes[0].set_ylabel("Throughput (jobs/sec)", fontsize=11)
    axes[0].set_title("Throughput Degradation Analysis", fontsize=13, fontweight='bold')
    axes[0].grid(axis='y', alpha=0.3)

    # Add value labels on bars
    for i, (cat, val) in enumerate(zip(categories[:2], values[:2])):
        axes[0].text(i, val + 0.02 * max(values[:2]) if max(values[:2]) > 0 else 0.01, f'{val:.3f}',
                    ha='center', va='bottom', fontsize=10, fontweight='bold')

    # Subplot 2: Degradation percentage and recovery
    metrics = []
    metric_values = []

    metrics.append(f'Degradation\n({degradation_pct:.1f}%)')
    metric_values.append(degradation_pct)

    if recovery_time:
        metrics.append(f'Recovery Time\n({recovery_time:.1f}s)')
        metric_values.append(recovery_time)

    axes[1].bar(metrics, metric_values, color=['red', 'blue'][:len(metrics)], alpha=0.7, edgecolor='black')
    axes[1].set_ylabel("Value", fontsize=11)
    axes[1].grid(axis='y', alpha=0.3)

    # Add value labels
    for i, (m, v) in enumerate(zip(metrics, metric_values)):
        axes[1].text(i, v + 0.02 * max(metric_values) if max(metric_values) > 0 else 0.01, f'{v:.2f}',
                    ha='center', va='bottom', fontsize=10, fontweight='bold')

    # Subplot 3: Failure impact metrics
    impact_categories = ['Failed\nAgents', 'Reassigned\nJobs']
    impact_values = [len(failures), len(reassignments)]
    impact_colors = ['#e74c3c', '#f39c12']

    bars = axes[2].bar(impact_categories, impact_values, color=impact_colors, alpha=0.7, edgecolor='black')
    axes[2].set_ylabel("Count", fontsize=11)
    axes[2].set_title("Failure Impact Metrics", fontsize=12, fontweight='bold')
    axes[2].grid(axis='y', alpha=0.3)

    # Add value labels
    for bar, val in zip(bars, impact_values):
        height = bar.get_height()
        axes[2].text(bar.get_x() + bar.get_width()/2., height + 0.02 * max(impact_values) if max(impact_values) > 0 else 0.5,
                    f'{int(val)}',
                    ha='center', va='bottom', fontsize=10, fontweight='bold')

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "throughput_degradation_analysis.png"), dpi=300, bbox_inches="tight")
    plt.close()

    # Save metrics to CSV
    metrics_data = {
        "baseline_throughput_jobs_per_sec": [baseline_throughput],
        "failure_throughput_jobs_per_sec": [failure_throughput],
        "degradation_percentage": [degradation_pct],
        "recovery_time_seconds": [recovery_time if recovery_time else -1],
        "baseline_jobs": [len(baseline_df)],
        "failure_period_jobs": [len(failure_df)],
        "first_failure_time": [first_failure - start_time],
        "num_failed_agents": [len(failures)],
        "num_reassigned_jobs": [len(reassignments)]
    }

    pd.DataFrame(metrics_data).to_csv(
        os.path.join(output_dir, "throughput_degradation_metrics.csv"),
        index=False
    )

    # Print summary
    print("\n" + "="*60)
    print("THROUGHPUT DEGRADATION ANALYSIS")
    print("="*60)
    print(f"Baseline throughput (pre-failure): {baseline_throughput:.4f} jobs/sec ({len(baseline_df)} jobs)")
    print(f"Throughput during failure:         {failure_throughput:.4f} jobs/sec ({len(failure_df)} jobs)")
    print(f"Throughput degradation:            {degradation_pct:.2f}%")
    if recovery_time:
        print(f"Recovery time (to 95% baseline):   {recovery_time:.2f} seconds")
    else:
        print(f"Recovery time (to 95% baseline):   Not achieved within test duration")
    print(f"Number of failed agents:           {len(failures)}")
    print(f"Number of reassigned jobs:         {len(reassignments)}")
    print("="*60)
    print(f"Saved: {os.path.join(output_dir, 'throughput_degradation_analysis.png')}")
    print(f"Saved: {os.path.join(output_dir, 'throughput_degradation_metrics.csv')}")
    print("="*60 + "\n")


def identify_agent_types(agents_csv: str) -> Tuple[Dict[int, str], Dict[int, int]]:
    """
    Identify agent types (LLM vs Resource) and hierarchy levels from agents CSV.

    Returns:
        - agent_types: {agent_id: "LLM" or "Resource"}
        - agent_levels: {agent_id: hierarchy_level}
    """
    agent_types = {}
    agent_levels = {}

    if not os.path.exists(agents_csv):
        print(f"Warning: {agents_csv} not found")
        return agent_types, agent_levels

    try:
        with open(agents_csv, 'r') as f:
            agents = json.load(f)

        for agent in agents:
            agent_id = agent.get("agent_id")
            if agent_id is None:
                continue

            # Try to determine agent type from various fields
            # Method 1: Check for 'agent_type' field (if available)
            agent_type = agent.get("agent_type", None)

            # Method 2: Check topology level (level 0 = top-level LLM, level 1+ = resource agents)
            # For hierarchical topology, top-level agents are cluster leaders (LLM)
            level = agent.get("level", 0)
            agent_levels[agent_id] = level

            if agent_type:
                agent_types[agent_id] = "LLM" if "llm" in agent_type.lower() else "Resource"
            else:
                # Infer from level: level 0 = LLM leaders, level 1 = Resource workers
                agent_types[agent_id] = "LLM" if level == 0 else "Resource"

    except Exception as e:
        print(f"Error reading agent types: {e}")

    return agent_types, agent_levels


def plot_hierarchical_topology(output_dir: str, repo: Repository | None = None):
    """
    Visualize the hierarchical topology structure showing LLM and Resource agents.
    """
    agents_csv = f"{output_dir}/all_agents.csv"
    if not os.path.exists(agents_csv):
        print(f"{agents_csv} not found - skipping topology visualization")
        return

    with open(agents_csv, 'r') as f:
        agents = json.load(f)

    agent_types, agent_levels = identify_agent_types(agents_csv)

    if not agent_types:
        print("No agent type information available")
        return

    # Organize agents by level
    levels = defaultdict(list)
    for agent_id, level in agent_levels.items():
        levels[level].append(agent_id)

    # Create visualization
    fig, ax = plt.subplots(figsize=(14, 8))

    max_level = max(levels.keys()) if levels else 0
    y_positions = {}

    # Plot agents level by level
    for level in sorted(levels.keys()):
        agents_at_level = sorted(levels[level])
        num_agents = len(agents_at_level)

        # Calculate positions
        y = level  # Level 1 (Parent) at top, Level 0 (Children) at bottom
        x_spacing = 12.0 / max(num_agents, 1)
        x_start = (12.0 - x_spacing * num_agents) / 2 + 1

        for i, agent_id in enumerate(agents_at_level):
            x = x_start + i * x_spacing
            y_positions[agent_id] = (x, y)

            # Color based on agent type
            color = '#FF6B6B' if agent_types.get(agent_id) == "LLM" else '#4ECDC4'
            marker = 's' if agent_types.get(agent_id) == "LLM" else 'o'

            ax.scatter(x, y, s=500, c=color, marker=marker, edgecolors='black', linewidths=2, zorder=3)
            ax.text(x, y, str(agent_id), ha='center', va='center', fontsize=9, fontweight='bold', zorder=4)

    # Draw connections (parent-child relationships if available)
    for agent in agents:
        agent_id = agent.get("agent_id")
        parent_id = agent.get("parent")

        if agent_id and parent_id and agent_id in y_positions and parent_id in y_positions:
            x1, y1 = y_positions[parent_id]
            x2, y2 = y_positions[agent_id]
            ax.plot([x1, x2], [y1, y2], 'k-', alpha=0.3, linewidth=1.5, zorder=1)

    # Add legend
    children_patch = mpatches.Patch(color='#FF6B6B', label='Children Agents (Workers)')
    parent_patch = mpatches.Patch(color='#4ECDC4', label='Parent Agents (Top-Level)')
    #ax.legend(handles=[parent_patch, children_patch], loc='upper right', fontsize=11)
    ax.legend(handles=[parent_patch, children_patch], fontsize=11)

    ax.set_xlabel('Agent Distribution', fontsize=12)
    ax.set_ylabel('Hierarchy Level', fontsize=12)
    ax.set_title('Hierarchical Consensus Topology', fontsize=14, fontweight='bold')
    ax.set_yticks(range(max_level + 1))
    ax.set_yticklabels([f'Level {i}' for i in range(max_level + 1)])
    ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "hierarchical_topology.png"), dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {os.path.join(output_dir, 'hierarchical_topology.png')}")


def plot_latency_comparison_by_agent_type(output_dir: str):
    """
    Compare scheduling latency between LLM and Resource agents.
    """
    jobs_csv = f"{output_dir}/all_jobs.csv"
    agents_csv = f"{output_dir}/all_agents.csv"

    if not os.path.exists(jobs_csv) or not os.path.exists(agents_csv):
        print(f"Missing required files for latency comparison")
        return

    df_jobs = pd.read_csv(jobs_csv)
    agent_types, agent_levels = identify_agent_types(agents_csv)

    if not agent_types:
        print("No agent type information - skipping latency comparison")
        return

    # Add agent type to jobs dataframe
    df_jobs['agent_type'] = df_jobs['leader_id'].map(agent_types)
    df_jobs['agent_level'] = df_jobs['leader_id'].map(agent_levels)

    # Calculate scheduling latency
    df_jobs['scheduling_latency'] = df_jobs['assigned_at'] - df_jobs['submitted_at']

    # Filter out invalid rows
    df_jobs = df_jobs[df_jobs['scheduling_latency'].notna()].copy()

    # Group by agent type
    llm_jobs = df_jobs[df_jobs['agent_type'] == 'LLM']
    resource_jobs = df_jobs[df_jobs['agent_type'] == 'Resource']

    # Create comparison plots
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # Plot 1: Box plot comparison
    data_to_plot = [llm_jobs['scheduling_latency'].dropna(), resource_jobs['scheduling_latency'].dropna()]
    labels = [f'LLM Agents\n(n={len(llm_jobs)})', f'Resource Agents\n(n={len(resource_jobs)})']

    bp = axes[0, 0].boxplot(data_to_plot, labels=labels, patch_artist=True, showfliers=False)
    bp['boxes'][0].set_facecolor('#FF6B6B')
    bp['boxes'][1].set_facecolor('#4ECDC4')
    axes[0, 0].set_ylabel('Scheduling Latency (s)', fontsize=11)
    axes[0, 0].set_title('Latency Distribution by Agent Type', fontsize=12, fontweight='bold')
    axes[0, 0].grid(axis='y', alpha=0.3)

    # Plot 2: Histogram comparison
    bins = np.linspace(min(df_jobs['scheduling_latency']), max(df_jobs['scheduling_latency']), 30)
    axes[0, 1].hist(llm_jobs['scheduling_latency'], bins=bins, alpha=0.6, label='LLM Agents', color='#FF6B6B', edgecolor='black')
    axes[0, 1].hist(resource_jobs['scheduling_latency'], bins=bins, alpha=0.6, label='Resource Agents', color='#4ECDC4', edgecolor='black')
    axes[0, 1].set_xlabel('Scheduling Latency (s)', fontsize=11)
    axes[0, 1].set_ylabel('Frequency', fontsize=11)
    axes[0, 1].set_title('Latency Histogram by Agent Type', fontsize=12, fontweight='bold')
    axes[0, 1].legend()
    axes[0, 1].grid(axis='y', alpha=0.3)

    # Plot 3: Mean latency by agent
    mean_latency_by_agent = df_jobs.groupby(['leader_id', 'agent_type'])['scheduling_latency'].mean().reset_index()
    llm_agents = mean_latency_by_agent[mean_latency_by_agent['agent_type'] == 'LLM']
    resource_agents = mean_latency_by_agent[mean_latency_by_agent['agent_type'] == 'Resource']

    if len(llm_agents) > 0:
        axes[1, 0].bar(llm_agents['leader_id'], llm_agents['scheduling_latency'],
                      color='#FF6B6B', label='LLM Agents', alpha=0.8, edgecolor='black')
    if len(resource_agents) > 0:
        axes[1, 0].bar(resource_agents['leader_id'], resource_agents['scheduling_latency'],
                      color='#4ECDC4', label='Resource Agents', alpha=0.8, edgecolor='black')

    axes[1, 0].set_xlabel('Agent ID', fontsize=11)
    axes[1, 0].set_ylabel('Mean Scheduling Latency (s)', fontsize=11)
    axes[1, 0].set_title('Mean Latency per Agent', fontsize=12, fontweight='bold')
    axes[1, 0].legend()
    axes[1, 0].grid(axis='y', alpha=0.3)

    # Plot 4: Statistics table
    stats_data = []

    if len(llm_jobs) > 0:
        stats_data.append([
            'LLM Agents',
            f"{llm_jobs['scheduling_latency'].mean():.4f}",
            f"{llm_jobs['scheduling_latency'].median():.4f}",
            f"{llm_jobs['scheduling_latency'].std():.4f}",
            f"{llm_jobs['scheduling_latency'].min():.4f}",
            f"{llm_jobs['scheduling_latency'].max():.4f}"
        ])

    if len(resource_jobs) > 0:
        stats_data.append([
            'Resource Agents',
            f"{resource_jobs['scheduling_latency'].mean():.4f}",
            f"{resource_jobs['scheduling_latency'].median():.4f}",
            f"{resource_jobs['scheduling_latency'].std():.4f}",
            f"{resource_jobs['scheduling_latency'].min():.4f}",
            f"{resource_jobs['scheduling_latency'].max():.4f}"
        ])

    axes[1, 1].axis('tight')
    axes[1, 1].axis('off')
    table = axes[1, 1].table(cellText=stats_data,
                            colLabels=['Agent Type', 'Mean (s)', 'Median (s)', 'Std Dev (s)', 'Min (s)', 'Max (s)'],
                            cellLoc='center',
                            loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 2)

    # Color header
    for i in range(6):
        table[(0, i)].set_facecolor('#E0E0E0')
        table[(0, i)].set_text_props(weight='bold')

    # Color rows
    if len(stats_data) > 0:
        for i in range(6):
            table[(1, i)].set_facecolor('#FFE5E5')
    if len(stats_data) > 1:
        for i in range(6):
            table[(2, i)].set_facecolor('#E5F9F8')

    axes[1, 1].set_title('Latency Statistics', fontsize=12, fontweight='bold')

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "latency_comparison_by_agent_type.png"), dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {os.path.join(output_dir, 'latency_comparison_by_agent_type.png')}")

    # Save stats to CSV
    if stats_data:
        stats_df = pd.DataFrame(stats_data, columns=['Agent Type', 'Mean (s)', 'Median (s)', 'Std Dev (s)', 'Min (s)', 'Max (s)'])
        stats_df.to_csv(os.path.join(output_dir, "latency_stats_by_agent_type.csv"), index=False)


def plot_reasoning_time_overhead(output_dir: str):
    """
    Analyze and visualize LLM reasoning time overhead vs heuristic-based decision making.
    """
    jobs_csv = f"{output_dir}/all_jobs.csv"
    agents_csv = f"{output_dir}/all_agents.csv"

    if not os.path.exists(jobs_csv):
        print(f"{jobs_csv} not found - skipping reasoning time analysis")
        return

    # Check if there are LLM agents before generating reasoning plots
    agent_types, _ = identify_agent_types(agents_csv)
    has_llm_agents = any(agent_type == "LLM" for agent_type in agent_types.values())

    if not has_llm_agents:
        print("No LLM agents detected - skipping reasoning time overhead analysis")
        return

    df_jobs = pd.read_csv(jobs_csv)

    # Check if reasoning_time column exists
    if 'reasoning_time' not in df_jobs.columns:
        print("No reasoning_time data available - skipping analysis")
        return

    df_jobs['agent_type'] = df_jobs['leader_id'].map(agent_types)
    df_jobs['scheduling_latency'] = df_jobs['assigned_at'] - df_jobs['submitted_at']

    # Filter for valid data
    df_jobs = df_jobs[df_jobs['reasoning_time'].notna() & (df_jobs['reasoning_time'] > 0)].copy()

    if df_jobs.empty:
        print("No reasoning time data available")
        return

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # Plot 1: Reasoning time distribution by agent type
    llm_jobs = df_jobs[df_jobs['agent_type'] == 'LLM']
    resource_jobs = df_jobs[df_jobs['agent_type'] == 'Resource']

    if not llm_jobs.empty:
        axes[0, 0].hist(llm_jobs['reasoning_time'], bins=30, alpha=0.7, color='#FF6B6B',
                       edgecolor='black', label=f'LLM (n={len(llm_jobs)})')
    if not resource_jobs.empty:
        axes[0, 0].hist(resource_jobs['reasoning_time'], bins=30, alpha=0.7, color='#4ECDC4',
                       edgecolor='black', label=f'Resource (n={len(resource_jobs)})')

    axes[0, 0].set_xlabel('Reasoning Time (s)', fontsize=11)
    axes[0, 0].set_ylabel('Frequency', fontsize=11)
    axes[0, 0].set_title('Decision Time Distribution', fontsize=12, fontweight='bold')
    axes[0, 0].legend()
    axes[0, 0].grid(axis='y', alpha=0.3)

    # Plot 2: Reasoning time vs scheduling latency scatter
    if not llm_jobs.empty:
        axes[0, 1].scatter(llm_jobs['reasoning_time'], llm_jobs['scheduling_latency'],
                          alpha=0.6, s=20, c='#FF6B6B', label='LLM Agents')
    if not resource_jobs.empty:
        axes[0, 1].scatter(resource_jobs['reasoning_time'], resource_jobs['scheduling_latency'],
                          alpha=0.6, s=20, c='#4ECDC4', label='Resource Agents')

    axes[0, 1].set_xlabel('Reasoning Time (s)', fontsize=11)
    axes[0, 1].set_ylabel('Scheduling Latency (s)', fontsize=11)
    axes[0, 1].set_title('Reasoning Time vs Total Latency', fontsize=12, fontweight='bold')
    axes[0, 1].legend()
    axes[0, 1].grid(True, alpha=0.3)

    # Plot 3: Mean reasoning time per agent
    mean_reasoning = df_jobs.groupby(['leader_id', 'agent_type'])['reasoning_time'].mean().reset_index()
    llm_mean = mean_reasoning[mean_reasoning['agent_type'] == 'LLM']
    resource_mean = mean_reasoning[mean_reasoning['agent_type'] == 'Resource']

    if not llm_mean.empty:
        axes[1, 0].bar(llm_mean['leader_id'], llm_mean['reasoning_time'],
                      color='#FF6B6B', label='LLM Agents', alpha=0.8, edgecolor='black')
    if not resource_mean.empty:
        axes[1, 0].bar(resource_mean['leader_id'], resource_mean['reasoning_time'],
                      color='#4ECDC4', label='Resource Agents', alpha=0.8, edgecolor='black')

    axes[1, 0].set_xlabel('Agent ID', fontsize=11)
    axes[1, 0].set_ylabel('Mean Reasoning Time (s)', fontsize=11)
    axes[1, 0].set_title('Mean Decision Time per Agent', fontsize=12, fontweight='bold')
    axes[1, 0].legend()
    axes[1, 0].grid(axis='y', alpha=0.3)

    # Plot 4: Overhead percentage
    df_jobs['overhead_pct'] = (df_jobs['reasoning_time'] / df_jobs['scheduling_latency']) * 100
    df_jobs['overhead_pct'] = df_jobs['overhead_pct'].clip(0, 100)  # Cap at 100%

    overhead_by_type = df_jobs.groupby('agent_type')['overhead_pct'].agg(['mean', 'median', 'std']).reset_index()

    if not overhead_by_type.empty:
        x_pos = np.arange(len(overhead_by_type))
        axes[1, 1].bar(x_pos, overhead_by_type['mean'],
                      color=['#FF6B6B' if t == 'LLM' else '#4ECDC4' for t in overhead_by_type['agent_type']],
                      alpha=0.8, edgecolor='black')
        axes[1, 1].errorbar(x_pos, overhead_by_type['mean'], yerr=overhead_by_type['std'],
                           fmt='none', ecolor='black', capsize=5)
        axes[1, 1].set_xticks(x_pos)
        axes[1, 1].set_xticklabels(overhead_by_type['agent_type'])
        axes[1, 1].set_ylabel('Reasoning Overhead (%)', fontsize=11)
        axes[1, 1].set_title('Mean Reasoning Overhead (% of Total Latency)', fontsize=12, fontweight='bold')
        axes[1, 1].grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "reasoning_time_overhead.png"), dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {os.path.join(output_dir, 'reasoning_time_overhead.png')}")


def plot_job_distribution_by_hierarchy(output_dir: str):
    """
    Show job distribution across hierarchy levels.
    """
    jobs_csv = f"{output_dir}/all_jobs.csv"
    agents_csv = f"{output_dir}/all_agents.csv"

    if not os.path.exists(jobs_csv) or not os.path.exists(agents_csv):
        print(f"Missing required files for job distribution")
        return

    df_jobs = pd.read_csv(jobs_csv)
    agent_types, agent_levels = identify_agent_types(agents_csv)

    df_jobs['agent_type'] = df_jobs['leader_id'].map(agent_types)
    df_jobs['agent_level'] = df_jobs['leader_id'].map(agent_levels)

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # Plot 1: Jobs by agent type
    jobs_by_type = df_jobs.groupby('agent_type').size()
    colors = ['#FF6B6B' if t == 'LLM' else '#4ECDC4' for t in jobs_by_type.index]

    axes[0].bar(jobs_by_type.index, jobs_by_type.values, color=colors, alpha=0.8, edgecolor='black')
    axes[0].set_ylabel('Number of Jobs', fontsize=11)
    axes[0].set_title('Job Distribution by Agent Type', fontsize=12, fontweight='bold')
    axes[0].grid(axis='y', alpha=0.3)

    # Add value labels on bars
    for i, (idx, val) in enumerate(jobs_by_type.items()):
        axes[0].text(i, val + max(jobs_by_type.values) * 0.02, str(val),
                    ha='center', va='bottom', fontweight='bold')

    # Plot 2: Jobs by hierarchy level
    jobs_by_level = df_jobs.groupby('agent_level').size().sort_index()

    axes[1].bar(jobs_by_level.index, jobs_by_level.values,
               color=['#FF6B6B' if l == 0 else '#4ECDC4' for l in jobs_by_level.index],
               alpha=0.8, edgecolor='black')
    axes[1].set_xlabel('Hierarchy Level', fontsize=11)
    axes[1].set_ylabel('Number of Jobs', fontsize=11)
    axes[1].set_title('Job Distribution by Hierarchy Level', fontsize=12, fontweight='bold')
    axes[1].set_xticks(jobs_by_level.index)
    axes[1].grid(axis='y', alpha=0.3)

    # Add value labels
    for idx, val in jobs_by_level.items():
        axes[1].text(idx, val + max(jobs_by_level.values) * 0.02, str(val),
                    ha='center', va='bottom', fontweight='bold')

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "job_distribution_by_hierarchy.png"), dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {os.path.join(output_dir, 'job_distribution_by_hierarchy.png')}")


def plot_latency_comparison_by_hierarchy_level(output_dir: str):
    """
    Compare scheduling latency between hierarchy levels.
    Level 2 = Top-level (LLM agents)
    Level 1 = Mid-level
    Level 0 = Bottom-level (worker/resource agents)
    Uses level-specific job entries from Redis (level0_jobs.csv, level1_jobs.csv, level2_jobs.csv).
    """
    level0_csv = f"{output_dir}/level0_jobs.csv"
    level1_csv = f"{output_dir}/level1_jobs.csv"
    level2_csv = f"{output_dir}/level2_jobs.csv"

    # Check if at least one level file exists
    has_files = os.path.exists(level0_csv) or os.path.exists(level1_csv) or os.path.exists(level2_csv)
    if not has_files:
        print(f"Level-specific job files not found - skipping hierarchy level latency comparison")
        return

    # Load dataframes for each level (if they exist)
    df_level0 = pd.read_csv(level0_csv) if os.path.exists(level0_csv) else pd.DataFrame()
    df_level1 = pd.read_csv(level1_csv) if os.path.exists(level1_csv) else pd.DataFrame()
    df_level2 = pd.read_csv(level2_csv) if os.path.exists(level2_csv) else pd.DataFrame()

    # Filter out invalid rows
    if not df_level0.empty:
        df_level0 = df_level0[df_level0['scheduling_latency'].notna()].copy()
    if not df_level1.empty:
        df_level1 = df_level1[df_level1['scheduling_latency'].notna()].copy()
    if not df_level2.empty:
        df_level2 = df_level2[df_level2['scheduling_latency'].notna()].copy()

    if df_level0.empty and df_level1.empty and df_level2.empty:
        print("No valid latency data for hierarchy level comparison")
        return

    # Create comparison plots (3 plots instead of 4)
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # Plot 1: Box plot comparison
    data_to_plot = []
    labels = []
    if not df_level2.empty:
        data_to_plot.append(df_level2['scheduling_latency'].dropna())
        labels.append(f'Level 2 (Top)\n(n={len(df_level2)})')
    if not df_level1.empty:
        data_to_plot.append(df_level1['scheduling_latency'].dropna())
        labels.append(f'Level 1 (Mid)\n(n={len(df_level1)})')
    if not df_level0.empty:
        data_to_plot.append(df_level0['scheduling_latency'].dropna())
        labels.append(f'Level 0 (Bottom)\n(n={len(df_level0)})')

    bp = axes[0, 0].boxplot(data_to_plot, labels=labels, patch_artist=True, showfliers=False)
    colors = ['#9B59B6', '#4ECDC4', '#FF6B6B']  # Level 2: purple, Level 1: cyan, Level 0: red
    for patch, color in zip(bp['boxes'], colors[:len(bp['boxes'])]):
        patch.set_facecolor(color)
    axes[0, 0].set_ylabel('Scheduling Latency (s)', fontsize=11)
    axes[0, 0].set_title('Latency Distribution by Hierarchy Level', fontsize=12, fontweight='bold')
    axes[0, 0].grid(axis='y', alpha=0.3)

    # Plot 2: Histogram comparison
    all_latencies = []
    if not df_level0.empty:
        all_latencies.extend(df_level0['scheduling_latency'].tolist())
    if not df_level1.empty:
        all_latencies.extend(df_level1['scheduling_latency'].tolist())
    if not df_level2.empty:
        all_latencies.extend(df_level2['scheduling_latency'].tolist())

    bins = np.linspace(min(all_latencies), max(all_latencies), 30)
    if not df_level2.empty:
        axes[0, 1].hist(df_level2['scheduling_latency'], bins=bins, alpha=0.5,
                       label='Level 2 (Top)', color='#9B59B6', edgecolor='black')
    if not df_level1.empty:
        axes[0, 1].hist(df_level1['scheduling_latency'], bins=bins, alpha=0.5,
                       label='Level 1 (Mid)', color='#4ECDC4', edgecolor='black')
    if not df_level0.empty:
        axes[0, 1].hist(df_level0['scheduling_latency'], bins=bins, alpha=0.5,
                       label='Level 0 (Bottom)', color='#FF6B6B', edgecolor='black')
    axes[0, 1].set_xlabel('Scheduling Latency (s)', fontsize=11)
    axes[0, 1].set_ylabel('Frequency', fontsize=11)
    axes[0, 1].set_title('Latency Histogram by Hierarchy Level', fontsize=12, fontweight='bold')
    axes[0, 1].legend()
    axes[0, 1].grid(axis='y', alpha=0.3)

    # Plot 3: Mean latency bar chart
    mean_data = []
    level_labels = []
    bar_colors = []

    if not df_level2.empty:
        mean_data.append(df_level2['scheduling_latency'].mean())
        level_labels.append('Level 2\n(Top)')
        bar_colors.append('#9B59B6')

    if not df_level1.empty:
        mean_data.append(df_level1['scheduling_latency'].mean())
        level_labels.append('Level 1\n(Mid)')
        bar_colors.append('#4ECDC4')

    if not df_level0.empty:
        mean_data.append(df_level0['scheduling_latency'].mean())
        level_labels.append('Level 0\n(Bottom)')
        bar_colors.append('#FF6B6B')

    x_pos = np.arange(len(mean_data))
    axes[1, 0].bar(x_pos, mean_data, color=bar_colors, alpha=0.8, edgecolor='black')
    axes[1, 0].set_xticks(x_pos)
    axes[1, 0].set_xticklabels(level_labels)
    axes[1, 0].set_ylabel('Mean Scheduling Latency (s)', fontsize=11)
    axes[1, 0].set_title('Mean Latency Comparison', fontsize=12, fontweight='bold')
    axes[1, 0].grid(axis='y', alpha=0.3)

    # Add value labels on bars
    for i, val in enumerate(mean_data):
        axes[1, 0].text(i, val + max(mean_data) * 0.02, f'{val:.4f}s',
                       ha='center', va='bottom', fontweight='bold')

    # Plot 4: Statistics table
    stats_data = []

    if not df_level2.empty:
        stats_data.append([
            'Level 2 (Top)',
            f"{df_level2['scheduling_latency'].mean():.4f}",
            f"{df_level2['scheduling_latency'].median():.4f}",
            f"{df_level2['scheduling_latency'].std():.4f}",
            f"{df_level2['scheduling_latency'].min():.4f}",
            f"{df_level2['scheduling_latency'].max():.4f}",
            f"{len(df_level2)}"
        ])

    if not df_level1.empty:
        stats_data.append([
            'Level 1 (Mid)',
            f"{df_level1['scheduling_latency'].mean():.4f}",
            f"{df_level1['scheduling_latency'].median():.4f}",
            f"{df_level1['scheduling_latency'].std():.4f}",
            f"{df_level1['scheduling_latency'].min():.4f}",
            f"{df_level1['scheduling_latency'].max():.4f}",
            f"{len(df_level1)}"
        ])

    if not df_level0.empty:
        stats_data.append([
            'Level 0 (Bottom)',
            f"{df_level0['scheduling_latency'].mean():.4f}",
            f"{df_level0['scheduling_latency'].median():.4f}",
            f"{df_level0['scheduling_latency'].std():.4f}",
            f"{df_level0['scheduling_latency'].min():.4f}",
            f"{df_level0['scheduling_latency'].max():.4f}",
            f"{len(df_level0)}"
        ])

    axes[1, 1].axis('tight')
    axes[1, 1].axis('off')
    table = axes[1, 1].table(cellText=stats_data,
                            colLabels=['Level', 'Mean (s)', 'Median (s)', 'Std Dev (s)', 'Min (s)', 'Max (s)', 'Count'],
                            cellLoc='center',
                            loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 2)

    # Color header
    for i in range(7):
        table[(0, i)].set_facecolor('#E0E0E0')
        table[(0, i)].set_text_props(weight='bold')

    # Color rows (in order: Level 2, Level 1, Level 0)
    row_colors = ['#F0E6FA', '#E5F9F8', '#FFE5E5']  # Level 2: light purple, Level 1: light cyan, Level 0: light red
    for row_idx in range(len(stats_data)):
        for col_idx in range(7):
            table[(row_idx + 1, col_idx)].set_facecolor(row_colors[row_idx])

    axes[1, 1].set_title('Latency Statistics by Level', fontsize=12, fontweight='bold')

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "latency_comparison_by_hierarchy_level.png"), dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {os.path.join(output_dir, 'latency_comparison_by_hierarchy_level.png')}")

    # Save stats to CSV
    if stats_data:
        stats_df = pd.DataFrame(stats_data, columns=['Level', 'Mean (s)', 'Median (s)', 'Std Dev (s)', 'Min (s)', 'Max (s)', 'Count'])
        stats_df.to_csv(os.path.join(output_dir, "latency_stats_by_hierarchy_level.csv"), index=False)
        print(f"Saved: {os.path.join(output_dir, 'latency_stats_by_hierarchy_level.csv')}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot scheduling latency, jobs per agent, conflicts/restarts, and loads")
    parser.add_argument("--output_dir", type=str, default=".", help="Directory where plots and CSV files are saved")
    parser.add_argument("--agents", type=int, required=True, help="Number of agents")
    parser.add_argument("--db_host", type=str, required=False, default="localhost", help="Database Host (not required with --from-csv)")
    parser.add_argument("--hierarchical", action="store_true", help="Generate hierarchical topology-specific plots")
    parser.add_argument("--save-csv", action="store_true", help="Export all data to CSV files for later analysis")
    parser.add_argument("--skip-plots", action="store_true", help="Skip plot generation (useful with --save-csv)")
    parser.add_argument("--from-csv", action="store_true", help="Generate plots from existing CSV files without connecting to Redis")
    args = parser.parse_args()

    # Validate argument combinations
    if args.from_csv and args.save_csv:
        print("WARNING: --save-csv is ignored when using --from-csv mode")
        args.save_csv = False

    if args.from_csv and args.skip_plots:
        print("ERROR: Cannot use --from-csv with --skip-plots (nothing to do)")
        exit(1)

    os.makedirs(args.output_dir, exist_ok=True)

    # Create Redis connection only if not using --from-csv mode
    if args.from_csv:
        print("Running in CSV-only mode (no Redis connection)")
        repo = None
    else:
        redis_client = redis.StrictRedis(
            host=args.db_host,
            port=6379,
            decode_responses=True
        )
        repo = Repository(redis_client=redis_client)

    agents_csv_file = f"{args.output_dir}/all_agents.csv"

    # Fetch and save data from Redis (skip if using --from-csv mode)
    if not args.from_csv:
        # For hierarchical topologies, fetch jobs from all levels (0, 1, 2)
        if args.hierarchical:
            level0_jobs = repo.get_all_objects(key_prefix=Repository.KEY_JOB, level=0)
            level1_jobs = repo.get_all_objects(key_prefix=Repository.KEY_JOB, level=1)
            level2_jobs = repo.get_all_objects(key_prefix=Repository.KEY_JOB, level=2)

            # Tag jobs with their hierarchy level for analysis
            for job in level0_jobs:
                job['hierarchy_level'] = 0
            for job in level1_jobs:
                job['hierarchy_level'] = 1
            for job in level2_jobs:
                job['hierarchy_level'] = 2

            # Combine all jobs
            all_jobs = level0_jobs + level1_jobs + level2_jobs

            # Save level-specific CSVs with level-specific timestamps
            save_jobs(jobs=level0_jobs, path=f"{args.output_dir}/level0_jobs.csv", level=0)
            save_jobs(jobs=level1_jobs, path=f"{args.output_dir}/level1_jobs.csv", level=1)
            save_jobs(jobs=level2_jobs, path=f"{args.output_dir}/level2_jobs.csv", level=2)

            # Save combined all_jobs.csv (uses last timestamps for backward compatibility)
            save_jobs(jobs=all_jobs, path=f"{args.output_dir}/all_jobs.csv", level=None)
        else:
            # For non-hierarchical topologies, fetch from level 0 only
            all_jobs = repo.get_all_objects(key_prefix=Repository.KEY_JOB, level=0)
            save_jobs(jobs=all_jobs, path=f"{args.output_dir}/level0_jobs.csv", level=None)
            save_jobs(jobs=all_jobs, path=f"{args.output_dir}/all_jobs.csv", level=None)

        all_agents = repo.get_all_objects(key_prefix=Repository.KEY_AGENT, level=None)
        save_agents(agents=all_agents, path=agents_csv_file)
    else:
        # Verify required CSV files exist when in --from-csv mode
        required_files = [agents_csv_file]
        if args.hierarchical:
            required_files.extend([
                f"{args.output_dir}/level0_jobs.csv",
                f"{args.output_dir}/level1_jobs.csv",
                f"{args.output_dir}/level2_jobs.csv",
                f"{args.output_dir}/all_jobs.csv"
            ])
        else:
            required_files.extend([
                f"{args.output_dir}/level0_jobs.csv",
                f"{args.output_dir}/all_jobs.csv"
            ])

        missing_files = [f for f in required_files if not os.path.exists(f)]
        if missing_files:
            print("ERROR: Missing required CSV files for --from-csv mode:")
            for f in missing_files:
                print(f"  - {f}")
            print("\nPlease run without --from-csv first to generate CSV files.")
            exit(1)

        print(f"Using existing CSV files from {args.output_dir}")

    # Gather restarted jobs once
    restarted_job_ids = collect_restarted_job_ids(args.output_dir, repo)

    # Conflicts/Restarts prefer Redis; fallback to files
    plot_conflicts_and_restarts(args.output_dir, repo, save_csv=args.save_csv, skip_plots=args.skip_plots)
    plot_conflicts_and_restarts_by_agent(args.output_dir, repo, save_csv=args.save_csv, skip_plots=args.skip_plots)

    # Agent loads prefer Redis; fallback to files
    plot_agent_loads(args.output_dir, repo, save_csv=args.save_csv, skip_plots=args.skip_plots)

    # NEW: Failure summary with failed agents and reassigned jobs (visualization #1)
    # Note: This function always saves CSVs (failed_agents.csv, reassigned_jobs.csv)
    if not args.skip_plots:
        plot_failure_summary(args.output_dir, repo)

        # NEW: Timeline with failure events (visualization #2)
        plot_timeline_with_failures(args.output_dir, repo, window_size=10)

        # NEW: Throughput degradation analysis (visualization #3)
        analyze_throughput_degradation(args.output_dir, repo, window_size=10, baseline_duration=30)
    else:
        # Still run failure detection to export CSVs
        failures, _ = detect_agent_failures(args.output_dir, repo)
        reassignments = collect_reassignments(args.output_dir, repo)

        if failures:
            failures_df = pd.DataFrame(failures)
            failures_df.to_csv(os.path.join(args.output_dir, "failed_agents.csv"), index=False)
            print(f"Saved: {os.path.join(args.output_dir, 'failed_agents.csv')}")

        if reassignments:
            reassignments_list = [
                {
                    "job_id": job_id,
                    "original_agent": info.get("original_agent"),
                    "new_agent": info.get("new_agent"),
                    "reassignment_time": info.get("reassignment_time"),
                    "reason": info.get("reason", "agent_failure")
                }
                for job_id, info in reassignments.items()
            ]
            reassignments_df = pd.DataFrame(reassignments_list)
            reassignments_df.to_csv(os.path.join(args.output_dir, "reassigned_jobs.csv"), index=False)
            print(f"Saved: {os.path.join(args.output_dir, 'reassigned_jobs.csv')}")

    # Hierarchical topology-specific plots
    if args.hierarchical:
        print("\n" + "="*60)
        print("HIERARCHICAL TOPOLOGY ANALYSIS")
        print("="*60)
        if not args.skip_plots:
            plot_hierarchical_topology(args.output_dir, repo)

        # Plot latency separately for each level
        print("\nGenerating Level 0 latency plots...")
        plot_scheduling_latency_and_jobs(
            args.output_dir,
            args.agents,
            label_suffix="_level0",
            level=0,
            save_csv=args.save_csv,
            skip_plots=args.skip_plots
        )

        print("Generating Level 1 latency plots...")
        plot_scheduling_latency_and_jobs(
            args.output_dir,
            args.agents,
            label_suffix="_level1",
            level=1,
            save_csv=args.save_csv,
            skip_plots=args.skip_plots
        )

        print("Generating Level 2 latency plots...")
        plot_scheduling_latency_and_jobs(
            args.output_dir,
            args.agents,
            label_suffix="_level2",
            level=2,
            save_csv=args.save_csv,
            skip_plots=args.skip_plots
        )

        # Also generate comparison plots
        if not args.skip_plots:
            print("Generating level comparison plots...")
            plot_latency_comparison_by_hierarchy_level(args.output_dir)

            #plot_latency_comparison_by_agent_type(args.output_dir)
            #plot_reasoning_time_overhead(args.output_dir)
            #plot_job_distribution_by_hierarchy(args.output_dir)
        print("="*60 + "\n")
    else:
        # Scheduling latency & jobs (ALL jobs)
        plot_scheduling_latency_and_jobs(args.output_dir, args.agents, level=0, save_csv=args.save_csv, skip_plots=args.skip_plots)
        if not args.skip_plots:
            plot_reasoning_time(args.output_dir)

        # Scheduling latency & jobs (EXCLUDING restarted jobs)
        if restarted_job_ids:
            plot_scheduling_latency_and_jobs(
                args.output_dir,
                args.agents,
                exclude_job_ids=restarted_job_ids,
                label_suffix="_no_restarts",
                level=0,
                save_csv=args.save_csv,
                skip_plots=args.skip_plots
            )

