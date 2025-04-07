import ast
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import argparse


def compare_dicts_list(dict_list):
    reference_dict = dict_list[0]  # Using the first dictionary as a reference
    differences = {}

    for i, current_dict in enumerate(dict_list):
        diff = {}
        all_keys = set(reference_dict.keys()).union(set(current_dict.keys()))

        for key in all_keys:
            val_ref = reference_dict.get(key, None)
            val_cur = current_dict.get(key, None)

            if val_ref != val_cur:
                diff[key] = (val_ref, val_cur)

        if diff:  # If there are differences, store them
            differences[i] = diff

    return differences


def main(run_directory: str, number_of_agents: int, algo: str):
    # Initialize dictionaries for data storage
    agent_diff_latencies = {agent_id: [] for agent_id in range(1, number_of_agents+1)}
    agent_idle_times = {agent_id: [] for agent_id in range(1, number_of_agents+1)}
    agent_job_counts = {}

    run_dir = run_directory

    # Read and Process Per-Agent Selection Time and Scheduling Latency
    for agent_id in range(1, number_of_agents+1):
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

        # Compute Scheduling Latency Difference Per Agent
        if len(df_selection_time) > 0 and len(df_scheduling_latency) > 0:
            diff_latency = df_scheduling_latency['scheduling_latency'].values - df_selection_time['selection_time'].values
            agent_diff_latencies[agent_id] = diff_latency

    # Compute Mean Scheduling Latency Over All Jobs
    all_scheduling_latencies = []
    for agent_id in range(1, number_of_agents+1):
        scheduling_latency_file = os.path.join(run_dir, f'scheduling_latency_{agent_id}.csv')
        if os.path.exists(scheduling_latency_file):
            df_scheduling_latency = pd.read_csv(scheduling_latency_file)
            if 'scheduling_latency' in df_scheduling_latency.columns:
                all_scheduling_latencies.extend(df_scheduling_latency["scheduling_latency"].tolist())

    mean_scheduling_latency = 0
    if all_scheduling_latencies:
        mean_scheduling_latency = np.mean(all_scheduling_latencies)
        print(f"Mean Scheduling Latency over All Jobs: {mean_scheduling_latency:.4f} seconds")

    # Compute Mean Selection Time Latency Over All Jobs
    all_selection_time = []
    for agent_id in range(1, number_of_agents+1):
        selection_time_file = os.path.join(run_dir, f'selection_time_{agent_id}.csv')
        if os.path.exists(selection_time_file):
            df_selection_time = pd.read_csv(selection_time_file)
            if 'selection_time' in df_selection_time.columns:
                all_selection_time.extend(df_selection_time["selection_time"].tolist())

    mean_selection_time = 0
    if all_selection_time:
        mean_selection_time = np.mean(all_selection_time)
        print(f"Mean Selection Time over All Jobs: {mean_selection_time:.4f} seconds")

    job_agent_id_mapping = {}

    # Track Job Count for Each Agent
    # Jobs and corresponding agents which think they may have executed it
    for agent_id in range(1, number_of_agents+1):
        jobs_per_agent_file = os.path.join(run_dir, f'jobs_per_agent_{agent_id}.csv')
        if not os.path.exists(jobs_per_agent_file):
            continue
        df_jobs_per_agent = pd.read_csv(jobs_per_agent_file)
        for index, row in df_jobs_per_agent.iterrows():
            _id = row.get("Agent ID")
            agent_job_counts[_id] = row.get("Number of Jobs Selected", 0)
            jobs = ast.literal_eval(row.get("Jobs", []))
            for j in jobs:
                if j not in job_agent_id_mapping:
                    job_agent_id_mapping[j] = set()
                job_agent_id_mapping[j].add(index)

    # Filter job_agent_id_mapping to include only jobs with more than one agent
    filtered_job_agent_id_mapping = {job: agents for job, agents in job_agent_id_mapping.items() if len(agents) > 1}

    # Prepare data for stacked bar plot with filtered jobs
    filtered_job_ids = list(filtered_job_agent_id_mapping.keys())
    unique_agents = sorted(set(agent for agents in filtered_job_agent_id_mapping.values() for agent in agents))
    filtered_agent_presence = {
        agent: [1 if agent in filtered_job_agent_id_mapping[job] else 0 for job in filtered_job_ids] for agent in
        unique_agents}

    # Plot stacked bars for filtered jobs
    plt.figure(figsize=(15, 6))
    bottom = np.zeros(len(filtered_job_ids))  # Initialize bottom for stacking

    for agent in unique_agents:
        plt.bar(filtered_job_ids, filtered_agent_presence[agent], bottom=bottom, label=f"Agent {agent}")
        bottom += np.array(filtered_agent_presence[agent])  # Update bottom for stacking

    # Formatting
    plt.xlabel("Job ID")
    plt.ylabel("Number of Agents")
    plt.title(f"Stacked Representation of Agents Executing Jobs (Agents: {number_of_agents})")
    plt.xticks(filtered_job_ids)  # Set x-ticks to job IDs
    plt.legend(title="Agent ID", bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small', ncol=2, frameon=True)

    plt.grid(axis='y', linestyle='--', alpha=0.7)

    # Show plot
    plt.savefig(os.path.join(run_dir, 'jobs_agent_mapping.png'))
    plt.close()

    # Read and Store Idle Time Per Agent
    for agent_id in range(1, number_of_agents+1):
        idle_time_file = os.path.join(run_dir, f'idle_time_per_agent_{agent_id}.csv')
        if os.path.exists(idle_time_file):
            df_idle_time = pd.read_csv(idle_time_file)
            if 'Idle Time' in df_idle_time.columns:
                agent_idle_times[agent_id] = df_idle_time['Idle Time'].dropna().tolist()

    # Plot Scheduling Latency per Job
    plt.figure(figsize=(12, 6))
    for agent_id in range(1, number_of_agents+1):
        scheduling_latency_file = os.path.join(run_dir, f'scheduling_latency_{agent_id}.csv')
        if os.path.exists(scheduling_latency_file):
            df_scheduling_latency = pd.read_csv(scheduling_latency_file)
            if 'job_id' in df_scheduling_latency.columns and 'scheduling_latency' in df_scheduling_latency.columns:
                #plt.plot(df_scheduling_latency["job_id"], df_scheduling_latency["scheduling_latency"], marker='o', linestyle='-', label=f'Agent {agent_id}')
                plt.scatter(df_scheduling_latency["job_id"], df_scheduling_latency["scheduling_latency"],
                            label=f'Agent {agent_id}')

    plt.xlabel('Job ID')
    plt.ylabel('Scheduling Latency (seconds)')
    plt.title(f'{algo}: Scheduling Latency per Job (Mean {mean_scheduling_latency:.4f}/ Agents: {number_of_agents})')
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(run_dir, 'scheduling_latency_per_job.png'), bbox_inches='tight')
    plt.close()

    # Plot Idle Time per Agent (Multiple Lines)
    plt.figure(figsize=(10, 6))
    for agent_id, idle_times in agent_idle_times.items():
        if idle_times:
            plt.scatter(range(len(idle_times)), idle_times, label=f'Agent {agent_id}')

    plt.xlabel('Time Index')
    plt.ylabel('Idle Time (seconds)')
    plt.title(f'{algo}: Idle Time per Agent (Agents: {number_of_agents})')
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(run_dir, 'idle_time_per_agent.png'), bbox_inches='tight')
    plt.close()

    # Plot Jobs per Agent (Bar Chart)
    plt.figure(figsize=(10, 6))
    plt.bar(agent_job_counts.keys(), agent_job_counts.values())
    plt.xlabel('Agent ID')
    plt.ylabel('Number of Jobs Selected')
    plt.title(f'{algo}: Jobs per Agent (Agents: {number_of_agents})')
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

    # Plot Selection Latency per Job
    plt.figure(figsize=(12, 6))
    for agent_id in range(1, number_of_agents+1):
        selection_time_file = os.path.join(run_dir, f'selection_time_{agent_id}.csv')
        if os.path.exists(selection_time_file):
            df_selection_time = pd.read_csv(selection_time_file)
            if 'job_id' in df_selection_time.columns and 'selection_time' in df_selection_time.columns:
                #plt.plot(df_selection_time["job_id"], df_selection_time["selection_time"], marker='o', linestyle='-',
                #         label=f'Agent {agent_id}')
                plt.scatter(df_selection_time["job_id"], df_selection_time["selection_time"], label=f'Agent {agent_id}')

    plt.xlabel('Job ID')
    plt.ylabel('Selection Time (seconds)')
    plt.title(f'{algo}: Selection Time per Job (Mean {mean_selection_time:.4f}/ Agents: {number_of_agents})')
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(run_dir, 'selection_time_per_job.png'), bbox_inches='tight')
    plt.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Analyze metrics from a single run.')
    parser.add_argument('--run_directory', type=str, required=True, help='Directory where run folder exists')
    parser.add_argument('--number_of_agents', type=int, required=True, help='Number of agents per run')
    parser.add_argument('--algo', type=str, required=True, help='Algorithm: pbft, swarm-single, swarm-multi')

    args = parser.parse_args()

    main(args.run_directory, args.number_of_agents, args.algo.upper())
