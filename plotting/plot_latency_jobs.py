import pandas as pd
import matplotlib.pyplot as plt
import argparse
import os


def plot_scheduling_latency_and_jobs(csv_file, output_dir, agent_count):
    df = pd.read_csv(csv_file)

    # Compute selection latency
    df["scheduling_latency"] = df["selected_by_agent_at"] - df["created_at"]

    # Group by leader agent
    grouped = df.groupby("leader_agent_id")

    # Plot selection latency per job
    plt.figure(figsize=(12, 6))
    for agent_id, group in grouped:
        plt.scatter(group["job_id"], group["scheduling_latency"], label=f"Agent {int(agent_id)}", s=12)
    plt.xlabel("Job ID")
    plt.ylabel("Scheduling Latency (s)")
    plt.title(f"SWARm-MULTI: Scheduling Latency per Job (Mean: {df['scheduling_latency'].mean():.4f} s/ Agents {agent_count})")
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(output_dir, "selection_latency_per_job.png"), bbox_inches="tight")
    plt.close()

    # Jobs per agent (keep original order of IDs as in CSV)
    jobs_per_agent = grouped.size()
    jobs_per_agent = jobs_per_agent.reindex(df["leader_agent_id"].unique())

    # Plot
    plt.figure(figsize=(10, 6))
    plt.bar(jobs_per_agent.index, jobs_per_agent.values)
    plt.xlabel("Agent ID")
    plt.ylabel("Number of Jobs")
    plt.title("Jobs per Agent")
    plt.grid(axis="y")
    plt.xticks(jobs_per_agent.index)  # Keep actual agent IDs as tick labels
    plt.savefig(os.path.join(output_dir, "jobs_per_agent.png"), bbox_inches="tight")
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
    parser.add_argument("--output_dir", type=str, default=".", help="Directory to save plots")
    parser.add_argument("--agents", type=str, required=True, help="Number of agents")

    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    plot_scheduling_latency_and_jobs(args.csv_file, args.output_dir, args.agents)
