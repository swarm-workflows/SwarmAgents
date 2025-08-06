import json

import pandas as pd
import matplotlib.pyplot as plt
import argparse
import os


def plot_conflicts_and_restarts_by_agent(misc_dir: str, output_dir: str):
    """
    Parses misc_<agent_id>.json files and plots:
    - Total conflicts per agent
    - Total restarts per agent
    """
    agent_conflict_counts = {}
    agent_restart_counts = {}

    for fname in os.listdir(misc_dir):
        if fname.startswith("misc_") and fname.endswith(".json"):
            agent_id = fname.split("_")[1].split(".")[0]
            with open(os.path.join(misc_dir, fname), "r") as f:
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
        plt.savefig(os.path.join(output_dir, "conflicts_per_agent.png"), bbox_inches="tight")
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
        plt.savefig(os.path.join(output_dir, "restarts_per_agent.png"), bbox_inches="tight")
        plt.close()

    return agent_conflict_counts, agent_restart_counts


def plot_conflicts_and_restarts(misc_dir: str, output_dir: str):
    all_conflicts = {}
    all_restarts = {}

    for fname in os.listdir(misc_dir):
        if fname.startswith("misc_") and fname.endswith(".json"):
            with open(os.path.join(misc_dir, fname), "r") as f:
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
        plt.savefig(os.path.join(output_dir, "conflicts_per_job.png"), bbox_inches="tight")
        plt.close()

    if not df_restarts.empty:
        df_restarts = df_restarts.sort_values("job_id")
        plt.figure(figsize=(12, 6))
        plt.bar(df_restarts["job_id"].astype(int), df_restarts["restarts"], color='steelblue')
        plt.xlabel("Job ID")
        plt.ylabel("Restart Count")
        plt.title("Restarts per Job (aggregated across agents)")
        plt.grid(axis="y")
        plt.savefig(os.path.join(output_dir, "restarts_per_job.png"), bbox_inches="tight")
        plt.close()

    return df_conflicts, df_restarts

def plot_scheduling_latency_and_jobs(csv_file, output_dir, agent_count):
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
    plt.savefig(os.path.join(output_dir, "selection_latency_per_job.png"), bbox_inches="tight")
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
    plt.savefig(os.path.join(output_dir, "jobs_per_agent.png"), bbox_inches="tight")
    plt.close()

    # Histogram of scheduling latency
    plt.figure(figsize=(10, 6))
    plt.hist(df["scheduling_latency"], bins=20, edgecolor='black', alpha=0.7)
    plt.xlabel("Scheduling Latency (s)")
    plt.ylabel("Frequency")
    plt.title("Distribution of Scheduling Latency")
    plt.grid(axis="y")
    plt.savefig(os.path.join(output_dir, "scheduling_latency_histogram.png"), bbox_inches="tight")
    plt.close()

    # Print summary
    print(f"Mean scheduling latency: {df['scheduling_latency'].mean():.4f} s")
    print(f"Max scheduling latency: {df['scheduling_latency'].max():.4f} s")
    print("\nJobs per agent:")
    for agent_id, count in jobs_per_agent.items():
        print(f"  Agent {agent_id}: {count} jobs")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot scheduling latency and jobs per agent")
    parser.add_argument("--csv_file", type=str, required=True, help="Path to CSV file")
    parser.add_argument("--run_dir", type=str, default=".", help="Directory where all files are present")
    parser.add_argument("--output_dir", type=str, default=".", help="Directory to save plots")
    parser.add_argument("--agents", type=str, required=True, help="Number of agents")

    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    plot_scheduling_latency_and_jobs(args.csv_file, args.output_dir, args.agents)
    plot_conflicts_and_restarts(args.run_dir, args.output_dir)
    plot_conflicts_and_restarts_by_agent(args.run_dir, args.output_dir)
