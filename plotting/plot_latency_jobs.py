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


def plot_conflicts_and_restarts_by_agent(run_dir: str):
    """
    Parses misc_<agent_id>.json files and plots:
    - Total conflicts per agent
    - Total restarts per agent
    """
    agent_conflict_counts = {}
    agent_restart_counts = {}

    for fname in os.listdir(run_dir):
        if fname.startswith("misc_") and fname.endswith(".json"):
            agent_id = fname.split("_")[1].split(".")[0]
            with open(os.path.join(run_dir, fname), "r") as f:
                data = json.load(f)
                total_conflicts = sum(data.get("conflicts", {}).values())
                total_restarts = sum(data.get("restarts", {}).values())
                agent_conflict_counts[agent_id] = total_conflicts
                agent_restart_counts[agent_id] = total_restarts

    # Conflicts per agent
    if agent_conflict_counts:
        df_conflicts = pd.DataFrame(list(agent_conflict_counts.items()), columns=["agent_id", "conflicts"])
        df_conflicts["agent_id"] = df_conflicts["agent_id"].astype(int)
        df_conflicts = df_conflicts.sort_values("agent_id")

        plt.figure(figsize=(10, 6))
        plt.bar(df_conflicts["agent_id"], df_conflicts["conflicts"], color="salmon")
        plt.xlabel("Agent ID")
        plt.ylabel("Total Conflicts")
        plt.title("Total Conflicts per Agent")
        plt.grid(axis="y")
        plt.savefig(os.path.join(run_dir, "conflicts_per_agent.png"), bbox_inches="tight")
        plt.close()

    # Restarts per agent
    if agent_restart_counts:
        df_restarts = pd.DataFrame(list(agent_restart_counts.items()), columns=["agent_id", "restarts"])
        df_restarts["agent_id"] = df_restarts["agent_id"].astype(int)
        df_restarts = df_restarts.sort_values("agent_id")

        plt.figure(figsize=(10, 6))
        plt.bar(df_restarts["agent_id"], df_restarts["restarts"], color="skyblue")
        plt.xlabel("Agent ID")
        plt.ylabel("Total Restarts")
        plt.title("Total Restarts per Agent")
        plt.grid(axis="y")
        plt.savefig(os.path.join(run_dir, "restarts_per_agent.png"), bbox_inches="tight")
        plt.close()

    return agent_conflict_counts, agent_restart_counts


def plot_conflicts_and_restarts(run_dir: str, repo: Repository):
    all_conflicts = {}
    all_restarts = {}
    all_misc = repo.get_all_objects(key_prefix="metrics", level=None)


    for fname in os.listdir(run_dir):
        if fname.startswith("misc_") and fname.endswith(".json"):
            with open(os.path.join(run_dir, fname), "r") as f:
                data = json.load(f)
                for job_id, count in data.get("conflicts", {}).items():
                    all_conflicts[job_id] = all_conflicts.get(job_id, 0) + count
                for job_id, count in data.get("restarts", {}).items():
                    all_restarts[job_id] = all_restarts.get(job_id, 0) + count

    df_conflicts = pd.DataFrame(list(all_conflicts.items()), columns=["job_id", "conflicts"])
    df_restarts = pd.DataFrame(list(all_restarts.items()), columns=["job_id", "restarts"])

    if not df_conflicts.empty:
        df_conflicts = df_conflicts.sort_values("job_id")
        plt.figure(figsize=(12, 6))
        plt.bar(df_conflicts["job_id"].astype(int), df_conflicts["conflicts"], color='tomato')
        plt.xlabel("Job ID")
        plt.ylabel("Conflict Count")
        plt.title("Conflicts per Job (aggregated across agents)")
        plt.grid(axis="y")
        plt.savefig(os.path.join(run_dir, "conflicts_per_job.png"), bbox_inches="tight")
        plt.close()

    if not df_restarts.empty:
        df_restarts = df_restarts.sort_values("job_id")
        plt.figure(figsize=(12, 6))
        plt.bar(df_restarts["job_id"].astype(int), df_restarts["restarts"], color='steelblue')
        plt.xlabel("Job ID")
        plt.ylabel("Restart Count")
        plt.title("Restarts per Job (aggregated across agents)")
        plt.grid(axis="y")
        plt.savefig(os.path.join(run_dir, "restarts_per_job.png"), bbox_inches="tight")
        plt.close()

    return df_conflicts, df_restarts


def plot_scheduling_latency_and_jobs(run_dir, agent_count, repo):
    csv_file = f"{run_dir}/all_jobs.csv"
    all_jobs = repo.get_all_objects(key_prefix=Repository.KEY_JOB, level=0)

    save_jobs(jobs=all_jobs, path=csv_file)
    df = pd.read_csv(csv_file)

    # Compute scheduling latency
    df["scheduling_latency"] = df["selected_by_agent_at"] - df["created_at"]

    # Group by leader agent
    grouped = df.groupby("leader_agent_id")

    # Plot scheduling latency per job (scatter)
    plt.figure(figsize=(12, 6))
    for agent_id, group in grouped:
        plt.scatter(group["job_id"], group["scheduling_latency"], label=f"Agent {int(agent_id)}", s=12)
    plt.xlabel("Job ID")
    plt.ylabel("Scheduling Latency (s)")
    plt.title(f"SWARM-MULTI: Scheduling Latency per Job "
              f"(Mean: {df['scheduling_latency'].mean():.4f} s / Agents {agent_count})")
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(run_dir, "selection_latency_per_job.png"), bbox_inches="tight")
    plt.close()

    # Jobs per agent (bar plot)
    jobs_per_agent = grouped.size()
    jobs_per_agent = jobs_per_agent.reindex(df["leader_agent_id"].unique())

    plt.figure(figsize=(10, 6))
    plt.bar(jobs_per_agent.index, jobs_per_agent.values)
    plt.xlabel("Agent ID")
    plt.ylabel("Number of Jobs")
    plt.title("Jobs per Agent")
    plt.grid(axis="y")
    plt.xticks(jobs_per_agent.index)
    plt.savefig(os.path.join(run_dir, "jobs_per_agent.png"), bbox_inches="tight")
    plt.close()

    # Histogram of scheduling latency
    plt.figure(figsize=(10, 6))
    plt.hist(df["scheduling_latency"], bins=20, edgecolor='black', alpha=0.7)
    plt.xlabel("Scheduling Latency (s)")
    plt.ylabel("Frequency")
    plt.title("Distribution of Scheduling Latency")
    plt.grid(axis="y")
    plt.savefig(os.path.join(run_dir, "scheduling_latency_histogram.png"), bbox_inches="tight")
    plt.close()

    # Print summary
    print(f"Mean scheduling latency: {df['scheduling_latency'].mean():.4f} s")
    print(f"Max scheduling latency: {df['scheduling_latency'].max():.4f} s")
    print("\nJobs per agent:")
    for agent_id, count in jobs_per_agent.items():
        print(f"  Agent {agent_id}: {count} jobs")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot scheduling latency and jobs per agent")
    parser.add_argument("--output_dir", type=str, default=".", help="Directory where plots and CSV files are saved")
    parser.add_argument("--agents", type=str, required=True, help="Number of agents")
    parser.add_argument("--db_host", type=str, required=True, help="Database Host")

    args = parser.parse_args()

    redis_client = redis.StrictRedis(host=args.db_host,
                                          port=6379,
                                          decode_responses=True)
    repo = Repository(redis_client=redis_client)

    plot_scheduling_latency_and_jobs(args.run_dir, args.agents, repo)
    plot_conflicts_and_restarts(args.run_dir, repo)
    plot_conflicts_and_restarts_by_agent(args.run_dir, repo)
    plot_agent_loads_from_dir(args.run_dir, repo)
