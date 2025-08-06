import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import argparse


def plot_latency_from_combined_csv(run_dir: str, latency_csv_name: str, number_of_agents: int, algo: str):
    latency_csv_path = os.path.join(run_dir, latency_csv_name)
    df_latency = pd.read_csv(latency_csv_path)

    required_cols = {'job_id', 'wait_time', 'selection_time', 'scheduling_latency', 'leader_agent_id'}
    if not required_cols.issubset(df_latency.columns):
        raise ValueError(f"Missing columns in {latency_csv_name}: {required_cols - set(df_latency.columns)}")

    df_latency['leader_agent_id'] = pd.to_numeric(df_latency['leader_agent_id'], errors='coerce')
    grouped = df_latency.groupby('leader_agent_id')

    mean_schd = df_latency['scheduling_latency'].mean()
    mean_sel = df_latency['selection_time'].mean()
    # Scheduling Latency
    plt.figure(figsize=(12, 6))
    for agent_id, group in grouped:
        plt.scatter(group['job_id'], group['scheduling_latency'], label=f'Agent {int(agent_id)}')
    plt.xlabel('Job ID')
    plt.ylabel('Scheduling Latency (s)')
    plt.title(f'{algo}: Scheduling Latency per Job (Mean {mean_schd:.4f}/ Mean Consensus {mean_sel:.4f}/ Agents: {number_of_agents})')
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(run_dir, 'scheduling_latency_per_job.png'), bbox_inches='tight')
    plt.close()

    # Wait Time
    plt.figure(figsize=(12, 6))
    for agent_id, group in grouped:
        plt.scatter(group['job_id'], group['wait_time'], label=f'Agent {int(agent_id)}')
    plt.xlabel('Job ID')
    plt.ylabel('Wait Time (s)')
    plt.title(f'{algo}: Wait Time per Job')
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(run_dir, 'wait_time_per_job.png'), bbox_inches='tight')
    plt.close()

    # Selection Time
    plt.figure(figsize=(12, 6))
    for agent_id, group in grouped:
        plt.scatter(group['job_id'], group['selection_time'], label=f'Agent {int(agent_id)}')
    plt.xlabel('Job ID')
    plt.ylabel('Selection Time (s)')
    plt.title(f'{algo}: Selection Time per Job')
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(run_dir, 'selection_time_per_job.png'), bbox_inches='tight')
    plt.close()

    return df_latency['scheduling_latency'].mean(), df_latency['selection_time'].mean()


def plot_latency_from_combined_csv_single_plot(run_dir: str, latency_csv_name: str, number_of_agents: int, algo: str):
    latency_csv_path = os.path.join(run_dir, latency_csv_name)
    df_latency = pd.read_csv(latency_csv_path)

    required_cols = {'job_id', 'wait_time', 'selection_time', 'scheduling_latency', 'leader_agent_id'}
    if not required_cols.issubset(df_latency.columns):
        raise ValueError(f"Missing columns in {latency_csv_name}: {required_cols - set(df_latency.columns)}")

    df_latency['leader_agent_id'] = pd.to_numeric(df_latency['leader_agent_id'], errors='coerce')
    grouped = df_latency.groupby('leader_agent_id')

    mean_schd = df_latency['scheduling_latency'].mean()
    mean_sel = df_latency['selection_time'].mean()

    # Create subplots
    fig, axes = plt.subplots(3, 1, figsize=(14, 15), sharex=True)

    # 1. Scheduling Latency
    for agent_id, group in grouped:
        axes[0].scatter(group['job_id'], group['scheduling_latency'], label=f'Agent {int(agent_id)}', s=10)
    axes[0].set_ylabel('Scheduling Latency (s)')
    axes[0].set_title(f'{algo}: Scheduling Latency per Job (Mean {mean_schd:.4f}/ Mean Consensus {mean_sel:.4f}/ Agents: {number_of_agents})')
    axes[0].grid(True)
    axes[0].legend()

    # 2. Wait Time
    for agent_id, group in grouped:
        axes[1].scatter(group['job_id'], group['wait_time'], label=f'Agent {int(agent_id)}', s=10)
    axes[1].set_ylabel('Wait Time (s)')
    axes[1].set_title(f'{algo}: Wait Time per Job')
    axes[1].grid(True)
    axes[1].legend()

    # 3. Selection Time
    for agent_id, group in grouped:
        axes[2].scatter(group['job_id'], group['selection_time'], label=f'Agent {int(agent_id)}', s=10)
    axes[2].set_xlabel('Job ID')
    axes[2].set_ylabel('Selection Time (s)')
    axes[2].set_title(f'{algo}: Selection Time per Job')
    axes[2].grid(True)
    axes[2].legend()

    plt.tight_layout()
    plt.savefig(os.path.join(run_dir, 'latency_metrics_combined.png'), bbox_inches='tight')
    plt.close()

    return mean_schd, mean_sel


def main(run_directory: str, number_of_agents: int, algo: str):
    agent_idle_times = {agent_id: [] for agent_id in range(1, number_of_agents + 1)}
    agent_job_counts = {}
    run_dir = run_directory

    # Step 1: Plot from latency.csv
    #mean_sched_latency, mean_sel_time = plot_latency_from_combined_csv(run_dir, 'latency.csv', number_of_agents, algo)
    mean_sched_latency, mean_sel_time = plot_latency_from_combined_csv_single_plot(run_dir, 'latency.csv', number_of_agents, algo)
    print(f"Mean Scheduling Latency: {mean_sched_latency:.4f} s")
    print(f"Mean Selection Time: {mean_sel_time:.4f} s")

    # Step 2: Jobs per agent
    for agent_id in range(1, number_of_agents + 1):
        file = os.path.join(run_dir, f'jobs_per_agent_{agent_id}.csv')
        if os.path.exists(file):
            df = pd.read_csv(file)
            for _, row in df.iterrows():
                aid = row.get("Agent ID")
                agent_job_counts[aid] = row.get("Number of Jobs Selected", 0)

    # Step 3: Idle Time per agent
    for agent_id in range(1, number_of_agents + 1):
        file = os.path.join(run_dir, f'idle_time_per_agent_{agent_id}.csv')
        if os.path.exists(file):
            df = pd.read_csv(file)
            if 'Idle Time' in df.columns:
                agent_idle_times[agent_id] = df['Idle Time'].dropna().tolist()

    # Plot: Idle Time
    plt.figure(figsize=(10, 6))
    all_idle = []
    for aid, times in agent_idle_times.items():
        if times:
            plt.scatter(range(len(times)), times, label=f'Agent {aid}')
            all_idle.extend(times)
    if all_idle:
        print(f"Mean Idle Time: {np.mean(all_idle):.4f} s")
    plt.xlabel('Time Index')
    plt.ylabel('Idle Time (s)')
    plt.title(f'{algo}: Idle Time per Agent')
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(run_dir, 'idle_time_per_agent.png'), bbox_inches='tight')
    plt.close()

    # Plot: Jobs per Agent
    plt.figure(figsize=(10, 6))
    plt.bar(agent_job_counts.keys(), agent_job_counts.values())
    plt.xlabel('Agent ID')
    plt.ylabel('Jobs Selected')
    plt.title(f'{algo}: Jobs per Agent')
    plt.grid(axis='y')
    plt.savefig(os.path.join(run_dir, 'jobs_per_agent.png'), bbox_inches='tight')
    plt.close()

    # Print Summary
    for aid, job_count in agent_job_counts.items():
        print(f"Agent {aid}: Jobs Handled = {job_count}")
    for aid, times in agent_idle_times.items():
        if times:
            print(f"Agent {aid}: Mean Idle Time = {np.mean(times):.4f} s")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Analyze metrics from a single run.')
    parser.add_argument('--run_directory', type=str, required=True, help='Directory where run folder exists')
    parser.add_argument('--number_of_agents', type=int, required=True, help='Number of agents per run')
    parser.add_argument('--algo', type=str, required=True, help='Algorithm: pbft, swarm-single, swarm-multi')
    args = parser.parse_args()

    main(args.run_directory, args.number_of_agents, args.algo.upper())
