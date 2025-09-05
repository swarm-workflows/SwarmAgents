import csv
import json
from typing import Any

import pandas as pd
import matplotlib.pyplot as plt
import argparse
import os

import redis

from swarm.database.repository import Repository
from swarm.models.job import Job, JobState
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
    # Save combined CSV with leader_agent_id
    with open(path, 'w', newline='') as f:
        json.dump(metrics, f, indent=2)


def save_agents(agents: list[Any], path: str):
    # Save combined CSV with leader_agent_id
    with open(path, 'w', newline='') as f:
        json.dump(agents, f, indent=2)

def save_jobs(jobs: list[Any], path: str):
    detailed_latency = []  # For combined output
    for job_data in jobs:
        if isinstance(job_data, dict):
            job = Job()
            job.from_dict(job_data)
        else:
            job = job_data
        job_id = job.job_id
        leader_id = getattr(job, "leader_agent_id", None)
        if leader_id is None:
            continue
        if job.state not in [JobState.READY, JobState.RUNNING, JobState.COMPLETE]:
            continue

        # Store all in one row
        detailed_latency.append([
            job_id,
            job.created_at if job.created_at is not None else 0,
            job.selection_started_at if job.selection_started_at is not None else 0,
            job.selected_by_agent_at if job.selected_by_agent_at is not None else 0,
            job.scheduled_at if job.scheduled_at is not None else 0,
            job.completed_at if job.completed_at is not None else 0,
            job.status if job.status is not None else 0,
            leader_id
        ])

    # Save combined CSV with leader_agent_id
    with open(path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['job_id', 'created_at', 'selection_started_at', 'selected_by_agent_at', 'scheduled_at',
                            'completed_at', 'exit_status', 'leader_agent_id'])
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
    df["scheduling_latency"] = df["selected_by_agent_at"] - df["created_at"]

    # Optional filter (e.g., exclude restarted jobs)
    if exclude_job_ids:
        df = df[~df["job_id"].isin(exclude_job_ids)].copy()

    if df.empty:
        print(f"No rows to plot for label_suffix='{label_suffix}'.")
        return

    # Group by leader agent
    grouped = df.groupby("leader_agent_id", dropna=False)

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
    jobs_per_agent = jobs_per_agent.reindex(sorted(df["leader_agent_id"].dropna().unique()))

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

