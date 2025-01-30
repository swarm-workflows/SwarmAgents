import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import argparse


def main(run_directory: str, number_of_agents: int, algo: str):
    # Initialize dictionaries for data storage
    agent_diff_latencies = {agent_id: [] for agent_id in range(number_of_agents)}
    agent_idle_times = {agent_id: [] for agent_id in range(number_of_agents)}
    agent_job_counts = {agent_id: 0 for agent_id in range(number_of_agents)}

    run_dir = run_directory

    # Read and Process Per-Agent Selection Time and Scheduling Latency
    for agent_id in range(number_of_agents):
        selection_time_file = os.path.join(run_dir, f'selection_time_{agent_id}.csv')
        scheduling_latency_file = os.path.join(run_dir, f'scheduling_latency_{agent_id}.csv')

        if not os.path.exists(selection_time_file) or not os.path.exists(scheduling_latency_file):
            print(f"Skipping Agent {agent_id}: Missing files.")
            continue

        df_selection_time = pd.read_csv(selection_time_file)
        df_scheduling_latency = pd.read_csv(scheduling_latency_file)

        # Check if required columns exist
        if 'selection_time' not in df_selection_time.columns or 'scheduling_latency' not in df_scheduling_latency.columns:
            print(f"Skipping Agent {agent_id}: Missing required columns.")
            continue

        # Track Job Count for Each Agent
        agent_job_counts[agent_id] = len(df_selection_time)

        # Compute Scheduling Latency Difference Per Agent
        if len(df_selection_time) > 0 and len(df_scheduling_latency) > 0:
            diff_latency = df_scheduling_latency['scheduling_latency'].values - df_selection_time['selection_time'].values
            agent_diff_latencies[agent_id] = diff_latency

    # Compute Mean Scheduling Latency Over All Jobs
    all_scheduling_latencies = []
    for agent_id in range(number_of_agents):
        scheduling_latency_file = os.path.join(run_dir, f'scheduling_latency_{agent_id}.csv')
        if os.path.exists(scheduling_latency_file):
            df_scheduling_latency = pd.read_csv(scheduling_latency_file)
            if 'scheduling_latency' in df_scheduling_latency.columns:
                all_scheduling_latencies.extend(df_scheduling_latency["scheduling_latency"].tolist())

    if all_scheduling_latencies:
        mean_scheduling_latency = np.mean(all_scheduling_latencies)
        print(f"Mean Scheduling Latency over All Jobs: {mean_scheduling_latency:.4f} seconds")

    # Read and Store Idle Time Per Agent
    for agent_id in range(number_of_agents):
        idle_time_file = os.path.join(run_dir, f'idle_time_per_agent_{agent_id}.csv')
        if os.path.exists(idle_time_file):
            df_idle_time = pd.read_csv(idle_time_file)
            if 'Idle Time' in df_idle_time.columns:
                agent_idle_times[agent_id] = df_idle_time['Idle Time'].dropna().tolist()

    # Plot Scheduling Latency per Job
    plt.figure(figsize=(12, 6))
    for agent_id in range(number_of_agents):
        scheduling_latency_file = os.path.join(run_dir, f'scheduling_latency_{agent_id}.csv')
        if os.path.exists(scheduling_latency_file):
            df_scheduling_latency = pd.read_csv(scheduling_latency_file)
            if 'job_id' in df_scheduling_latency.columns and 'scheduling_latency' in df_scheduling_latency.columns:
                plt.plot(df_scheduling_latency["job_id"], df_scheduling_latency["scheduling_latency"], marker='o', linestyle='-', label=f'Agent {agent_id}')

    plt.xlabel('Job ID')
    plt.ylabel('Scheduling Latency (seconds)')
    plt.title(f'{algo}: Scheduling Latency per Job')
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(run_dir, 'scheduling_latency_per_job.png'), bbox_inches='tight')
    plt.close()

    # Plot Idle Time per Agent (Multiple Lines)
    plt.figure(figsize=(10, 6))
    for agent_id, idle_times in agent_idle_times.items():
        if idle_times:
            plt.plot(range(len(idle_times)), idle_times, marker='o', linestyle='-', label=f'Agent {agent_id}')

    plt.xlabel('Time Index')
    plt.ylabel('Idle Time (seconds)')
    plt.title(f'{algo}: Idle Time per Agent')
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(run_dir, 'idle_time_per_agent.png'), bbox_inches='tight')
    plt.close()

    # Plot Jobs per Agent (Bar Chart)
    plt.figure(figsize=(10, 6))
    plt.bar(agent_job_counts.keys(), agent_job_counts.values())
    plt.xlabel('Agent ID')
    plt.ylabel('Number of Jobs Selected')
    plt.title(f'{algo}: Jobs per Agent')
    plt.xticks(list(agent_job_counts.keys()))
    plt.grid(axis='y')
    plt.savefig(os.path.join(run_dir, 'jobs_per_agent.png'), bbox_inches='tight')
    plt.close()

    # Print Mean Idle Time per Agent
    for agent_id, idle_times in agent_idle_times.items():
        if idle_times:
            print(f"Agent {agent_id}: Mean Idle Time = {np.mean(idle_times):.4f} seconds")

    # Print Total Jobs Per Agent
    for agent_id, job_count in agent_job_counts.items():
        print(f"Agent {agent_id}: Jobs Handled = {job_count}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Analyze metrics from a single run.')
    parser.add_argument('--run_directory', type=str, required=True, help='Directory where run folder exists')
    parser.add_argument('--number_of_agents', type=int, required=True, help='Number of agents per run')
    parser.add_argument('--algo', type=str, required=True, help='Algorithm: pbft, swarm-single, swarm-multi')

    args = parser.parse_args()

    main(args.run_directory, args.number_of_agents, args.algo.upper())
