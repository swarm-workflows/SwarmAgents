import csv
import json
from typing import Any

import pandas as pd
import matplotlib.pyplot as plt
import argparse
import os

import redis

from swarm.database.repository import Repository
from swarm.models.job import Job, ObjectState
from collections import defaultdict

from typing import Set

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

def save_jobs(jobs: list[Any], path: str):
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

        # IMPORTANT: leader_id before reasoning_time to match header
        detailed_latency.append([
            job_id,
            job.submitted_at if job.submitted_at is not None else 0,
            job.selection_started_at if job.selection_started_at is not None else 0,
            job.assigned_at if job.assigned_at is not None else 0,
            job.started_at if job.started_at is not None else 0,
            job.completed_at if job.completed_at is not None else 0,
            job.exit_status if job.exit_status is not None else 0,
            leader_id,
            reasoning_time,
        ])

    with open(path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'job_id', 'submitted_at', 'selection_started_at', 'assigned_at',
            'started_at', 'completed_at', 'exit_status', 'leader_id', 'reasoning_time'
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

def plot_agent_loads(output_dir: str, repo: Repository | None = None):
    """
    Prefer Redis metrics (load_trace per agent).
    Fallback to plot_agent_loads_from_dir(output_dir) if Redis has nothing.
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


def plot_conflicts_and_restarts_by_agent(output_dir: str, repo: Repository | None = None):
    """
    Prefer Redis metrics; if missing, fall back to misc_<agent_id>.json files under output_dir.

    Produces:
      - conflicts_per_agent.png
      - restarts_per_agent.png

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


def plot_conflicts_and_restarts(output_dir: str, repo: Repository | None = None):
    """
    Prefer Redis metrics; fallback to reading misc_*.json files.
    Produces:
      - conflicts_per_job.png
      - restarts_per_job.png

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
                                     label_suffix: str = ""):
    """
    Plot latency, jobs/agent, and histogram.
    If exclude_job_ids is provided, the dataset is filtered to exclude those jobs.

    label_suffix controls output filenames and plot titles (e.g., '_no_restarts').
    """
    csv_file = f"{run_dir}/all_jobs.csv"
    df = pd.read_csv(csv_file)

    # Compute scheduling latency
    df["scheduling_latency"] = df["assigned_at"] - df["submitted_at"]

    # Optional filter (e.g., exclude restarted jobs)
    if exclude_job_ids:
        df = df[~df["job_id"].isin(exclude_job_ids)].copy()

    if df.empty:
        print(f"No rows to plot for label_suffix='{label_suffix}'.")
        return

    # Group by leader agent
    grouped = df.groupby("leader_id", dropna=False)

    # Scatter: scheduling latency per job
    plt.figure(figsize=(12, 6))
    for agent_id, group in grouped:
        plt.scatter(group["job_id"], group["scheduling_latency"],
                    label=f"Agent {int(agent_id)}" if pd.notna(agent_id) else "Agent NA",
                    s=12)
    plt.xlabel("Job ID")
    plt.ylabel("Scheduling Latency (s)")
    title_extra = " (excluding restarted jobs)" if label_suffix else ""
    plt.title(f"SWARM-MULTI: Scheduling Latency per Job "
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
    plt.title(f"Jobs per Agent{title_extra}")
    plt.grid(axis="y")
    plt.xticks(jobs_per_agent.index)
    plt.savefig(os.path.join(run_dir, f"jobs_per_agent{label_suffix}.png"), bbox_inches="tight")
    plt.close()

    # Histogram of scheduling latency
    plt.figure(figsize=(10, 6))
    plt.hist(df["scheduling_latency"], bins=20, edgecolor='black', alpha=0.7)
    plt.xlabel("Scheduling Latency (s)")
    plt.ylabel("Frequency")
    plt.title(f"Distribution of Scheduling Latency{title_extra}")
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
    if not os.path.exists(csv_file):
        print(f"{csv_file} not found")
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
    1. Redis agent state transitions (ACTIVE -> FAILED)
    2. Agent logs (grepping for failure keywords)
    3. all_agents.csv if state field exists
    """
    failures = []
    agent_mapping = {}

    # Try Redis first - look for agents with FAILED state
    if repo is not None:
        try:
            agents = repo.get_all_objects(key_prefix=Repository.KEY_AGENT, level=None)
            for agent_data in agents:
                if isinstance(agent_data, str):
                    agent_data = json.loads(agent_data)
                if not isinstance(agent_data, dict):
                    continue

                agent_id = agent_data.get("id")
                state = agent_data.get("state", "").upper()
                last_updated = agent_data.get("last_updated", 0)

                # Extract host and port
                host = agent_data.get("host", "unknown")
                port = agent_data.get("port", 0)

                # Build mapping for all agents
                if agent_id is not None:
                    agent_mapping[agent_id] = f"{host}:{port}"

                # Check if agent is in FAILED state
                if state == "FAILED" and last_updated:
                    failures.append({
                        "agent_id": agent_id,
                        "failure_time": float(last_updated),
                        "host": host,
                        "port": port
                    })
        except Exception as e:
            print(f"Could not detect failures from Redis: {e}")

    # Fallback: Parse agent logs for failure indicators
    if not failures and os.path.exists(output_dir):
        import re
        failure_pattern = re.compile(r".*detected as FAILED.*agent[_\s]+(\d+).*", re.IGNORECASE)

        for filename in os.listdir(output_dir):
            if filename.startswith("agent-") and filename.endswith(".log"):
                try:
                    agent_id = int(filename.split("-")[1].split(".")[0])
                except Exception:
                    continue

                log_path = os.path.join(output_dir, filename)
                try:
                    with open(log_path, "r") as f:
                        for line in f:
                            # Look for failure indicators
                            if "FAILED" in line.upper() or "FAILURE" in line.upper():
                                # Try to extract timestamp (assuming format like: 2025-01-15 10:30:45)
                                # For now, we'll mark as failed without precise timestamp
                                failures.append({
                                    "agent_id": agent_id,
                                    "failure_time": None,  # Will need timestamp parsing
                                    "host": "unknown",
                                    "port": 0
                                })
                                break
                except Exception:
                    continue

    return failures, agent_mapping


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

    # Create time windows
    time_bins = list(range(int(start_time), int(end_time) + window_size, window_size))

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
    failures = detect_agent_failures(output_dir, repo)

    # Convert times to relative (seconds from start)
    throughput_times_rel = [t - start_time for t in throughput_times]
    latency_times_rel = [t - start_time for t in latency_times]
    active_times_rel = [t - start_time for t in active_times]

    failure_times_rel = []
    failure_agents = []
    for f in failures:
        if f["failure_time"]:
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
    failures = detect_agent_failures(output_dir, repo)

    if not failures:
        print("No failures detected - cannot compute degradation metrics")
        # Still compute overall throughput
        total_jobs = len(df)
        duration = end_time - start_time
        overall_throughput = total_jobs / duration if duration > 0 else 0
        print(f"Overall throughput: {overall_throughput:.4f} jobs/sec ({total_jobs} jobs in {duration:.2f}s)")
        return

    # Find first failure time
    failure_times = [f["failure_time"] for f in failures if f["failure_time"]]
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
    time_bins = list(range(int(first_failure), int(end_time) + window_size, window_size))
    df["completion_bin"] = pd.cut(df["completed_at"], bins=time_bins, labels=False, include_lowest=True)

    for bin_idx in sorted(df["completion_bin"].dropna().unique()):
        bin_jobs = df[df["completion_bin"] == bin_idx]
        bin_throughput = len(bin_jobs) / window_size

        if bin_throughput >= target_throughput:
            bin_start = first_failure + (bin_idx * window_size)
            recovery_time = bin_start - first_failure
            break

    # Generate visualization
    fig, axes = plt.subplots(2, 1, figsize=(12, 8))

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
        axes[0].text(i, val + 0.02 * max(values[:2]), f'{val:.3f}',
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
        axes[1].text(i, v + 0.02 * max(metric_values), f'{v:.2f}',
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
        "num_failures": [len(failures)]
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
    print(f"Number of agent failures:          {len(failures)}")
    print("="*60)
    print(f"Saved: {os.path.join(output_dir, 'throughput_degradation_analysis.png')}")
    print(f"Saved: {os.path.join(output_dir, 'throughput_degradation_metrics.csv')}")
    print("="*60 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot scheduling latency, jobs per agent, conflicts/restarts, and loads")
    parser.add_argument("--output_dir", type=str, default=".", help="Directory where plots and CSV files are saved")
    parser.add_argument("--agents", type=int, required=True, help="Number of agents")
    parser.add_argument("--db_host", type=str, required=True, help="Database Host")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    redis_client = redis.StrictRedis(
        host=args.db_host,
        port=6379,
        decode_responses=True
    )
    repo = Repository(redis_client=redis_client)

    jobs_csv_file = f"{args.output_dir}/all_jobs.csv"
    agents_csv_file = f"{args.output_dir}/all_agents.csv"
    all_jobs = repo.get_all_objects(key_prefix=Repository.KEY_JOB, level=0)
    all_agents = repo.get_all_objects(key_prefix=Repository.KEY_AGENT, level=None)
    save_jobs(jobs=all_jobs, path=jobs_csv_file)
    save_agents(agents=all_agents, path=agents_csv_file)

    # Gather restarted jobs once
    restarted_job_ids = collect_restarted_job_ids(args.output_dir, repo)

    # Scheduling latency & jobs (ALL jobs)
    plot_scheduling_latency_and_jobs(args.output_dir, args.agents)
    plot_reasoning_time(args.output_dir)

    # Scheduling latency & jobs (EXCLUDING restarted jobs)
    if restarted_job_ids:
        plot_scheduling_latency_and_jobs(
            args.output_dir,
            args.agents,
            exclude_job_ids=restarted_job_ids,
            label_suffix="_no_restarts"
        )

    # Conflicts/Restarts prefer Redis; fallback to files
    plot_conflicts_and_restarts(args.output_dir, repo)
    plot_conflicts_and_restarts_by_agent(args.output_dir, repo)

    # Agent loads prefer Redis; fallback to files
    plot_agent_loads(args.output_dir, repo)

    # NEW: Timeline with failure events (visualization #1)
    plot_timeline_with_failures(args.output_dir, repo, window_size=10)

    # NEW: Throughput degradation analysis (visualization #2)
    analyze_throughput_degradation(args.output_dir, repo, window_size=10, baseline_duration=30)

