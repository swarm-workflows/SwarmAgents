import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import argparse


def main(run_directory: str, number_of_agents: int, algo: str):
    # Initialize lists to store the data from all runs
    selection_times = []
    scheduling_latencies = []
    wait_times = []
    all_idle_times = []

    # Loop through the directories for each run
    for i in range(100):
        run_dir = os.path.join(run_directory, f'run{i + 1}')

        selection_time_file = os.path.join(run_dir, f'selection_time_0.csv')
        scheduling_latency_file = os.path.join(run_dir, f'scheduling_latency_0.csv')
        wait_time_file = os.path.join(run_dir, f'wait_time_0.csv')

        # Load Selection Time
        if os.path.exists(selection_time_file):
            df_selection_time = pd.read_csv(selection_time_file)
            selection_times.append(df_selection_time['selection_time'].dropna().tolist())

        # Load Scheduling Latency
        if os.path.exists(scheduling_latency_file):
            df_scheduling_latency = pd.read_csv(scheduling_latency_file)
            scheduling_latencies.append(df_scheduling_latency['scheduling_latency'].dropna().tolist())

        # Load Wait Time
        if os.path.exists(wait_time_file):
            df_wait_time = pd.read_csv(wait_time_file)
            wait_times.append(df_wait_time['wait_time'].dropna().tolist())

        # Aggregate Idle Time for all agents in each run
        idle_times_list = []
        idle_times = {}
        for agent_id in range(number_of_agents):
            idle_time_file = os.path.join(run_dir, f'idle_time_per_agent_{agent_id}.csv')
            if os.path.exists(idle_time_file):
                df_idle_time = pd.read_csv(idle_time_file)
                idle_times[agent_id] = df_idle_time['Idle Time'].dropna().tolist()
                idle_times_list.extend(idle_times[agent_id])

        all_idle_times.append(idle_times)

        # Plot Idle Time for each run
        plt.figure(figsize=(12, 8))
        plt.bar(range(len(idle_times_list)), idle_times_list, color='orange')
        plt.xlabel('Agent Index')
        plt.ylabel('Idle Time (seconds)')
        plt.title(f'{algo}: Agents: {number_of_agents} - Idle Time per Agent for Run {i + 1}')
        plt.grid(axis='y', linestyle='--', linewidth=0.5)
        plot_path = os.path.join(run_dir, f'idle_time_per_agent_run_{i + 1}.png')
        plt.savefig(plot_path, bbox_inches='tight')
        plt.close()

        # Plot Idle Time for all agents in a single plot
        plt.figure(figsize=(12, 8))
        for agent_id, idle_time_list in idle_times.items():
            plt.plot(range(len(idle_time_list)), idle_time_list, marker='o', label=f'Agent {agent_id}')

        plt.xlabel('Index')
        plt.ylabel('Idle Time (seconds)')
        plt.title(f'{algo}: Agents: {number_of_agents} - Idle Time per Agent in Run {i + 1}')
        plt.grid(True)
        plt.legend(loc='best')
        plot_path = os.path.join(run_dir, f'idle_time_all_agents_run_{i + 1}.png')
        plt.savefig(plot_path, bbox_inches='tight')
        plt.close()

    # Flatten the lists for overall metrics analysis
    flat_selection_times = [item for sublist in selection_times for item in sublist]
    flat_scheduling_latencies = [item for sublist in scheduling_latencies for item in sublist]
    flat_wait_times = [item for sublist in wait_times for item in sublist]
    flat_idle_times = [item for agent_idle_times in all_idle_times for idle_time_list in agent_idle_times.values() for item in idle_time_list]

    # 1. Box Plot for Selection Time, Scheduling Latency, and Wait Time
    data = {
        'Selection Time': flat_selection_times,
        'Scheduling Latency': flat_scheduling_latencies,
        'Wait Time': flat_wait_times
    }

    df_all_runs = pd.DataFrame(data)

    plt.figure(figsize=(14, 10))
    df_all_runs.boxplot(column=['Selection Time', 'Scheduling Latency', 'Wait Time'])
    plt.title(f'{algo}: Agents: {number_of_agents} - Box Plot for Selection Time, Scheduling Latency, '
              f'and Wait Time Across 100 Runs')
    plt.ylabel('Time Units (seconds)')
    plt.grid(True)
    plt.savefig(os.path.join(run_directory, 'box_plot_overall_metrics.png'), bbox_inches='tight')
    plt.close()

    # 2. Line Plot with Mean/Median and Confidence Intervals for Scheduling Latency
    means = [np.mean(run) for run in scheduling_latencies]
    medians = [np.median(run) for run in scheduling_latencies]
    conf_intervals = [stats.t.interval(0.95, len(run) - 1, loc=np.mean(run), scale=stats.sem(run)) for run in scheduling_latencies]

    ci_lower = [ci[0] for ci in conf_intervals]
    ci_upper = [ci[1] for ci in conf_intervals]

    plt.figure(figsize=(14, 10))
    plt.plot(means, 'b-', label='Mean')
    plt.plot(medians, 'g-', label='Median')
    plt.fill_between(range(len(means)), ci_lower, ci_upper, color='blue', alpha=0.2, label='95% Confidence Interval')
    plt.title(f'{algo}: Agents: {number_of_agents} - Mean/Median and 95% Confidence Interval of '
              f'Scheduling Latency Across 100 Runs')
    plt.xlabel('Run Index')
    plt.ylabel('Scheduling Latency (seconds)')
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(run_directory, 'line_plot_with_ci.png'), bbox_inches='tight')
    plt.close()

    means = [np.mean(run) for run in selection_times]
    medians = [np.median(run) for run in selection_times]
    conf_intervals = [stats.t.interval(0.95, len(run) - 1, loc=np.mean(run), scale=stats.sem(run)) for run in selection_times]

    ci_lower = [ci[0] for ci in conf_intervals]
    ci_upper = [ci[1] for ci in conf_intervals]

    plt.figure(figsize=(14, 10))
    plt.plot(means, 'b-', label='Mean')
    plt.plot(medians, 'g-', label='Median')
    plt.fill_between(range(len(means)), ci_lower, ci_upper, color='blue', alpha=0.2, label='95% Confidence Interval')
    plt.title(f'{algo}: Agents: {number_of_agents} - Mean/Median and 95% Confidence Interval of Selection Time Across 100 Runs')
    plt.xlabel('Run Index')
    plt.ylabel('Selection Time (seconds)')
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(run_directory, 'line_plot_with_ci_selection.png'), bbox_inches='tight')
    plt.close()

    # 3. Scatter Plot with Regression Line for Selection Time vs. Scheduling Latency
    plt.figure(figsize=(14, 10))
    sns.regplot(x=flat_selection_times, y=flat_scheduling_latencies, ci=95)
    plt.title(f'{algo}: Agents: {number_of_agents} - Scatter Plot of Selection Time vs. Scheduling Latency with Regression Line')
    plt.xlabel('Selection Time (seconds)')
    plt.ylabel('Scheduling Latency (seconds)')
    plt.grid(True)
    plt.savefig(os.path.join(run_directory, 'scatter_plot_with_regression.png'), bbox_inches='tight')
    plt.close()

    # 4. Histogram for Frequency Distribution of Wait Time
    plt.figure(figsize=(14, 10))
    plt.hist(flat_wait_times, bins=20, color='blue', alpha=0.7)
    plt.title(f'{algo}: Agents: {number_of_agents} - Frequency Distribution of Wait Times Across 100 Runs')
    plt.xlabel('Wait Time (seconds)')
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.savefig(os.path.join(run_directory, 'histogram_wait_times.png'), bbox_inches='tight')
    plt.close()

    # 5. Histogram for Frequency Distribution of Selection Time
    plt.figure(figsize=(14, 10))
    plt.hist(flat_selection_times, bins=20, color='blue', alpha=0.7)
    plt.title(f'{algo}: Agents: {number_of_agents} - Frequency Distribution of Selection Times Across 100 Runs')
    plt.xlabel('Selection Time (seconds)')
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.savefig(os.path.join(run_directory, 'histogram_selection_times.png'), bbox_inches='tight')
    plt.close()

    # 6. Histogram for Frequency Distribution of Idle Time Across All Runs
    plt.figure(figsize=(14, 10))
    plt.hist(flat_idle_times, bins=20, color='orange', alpha=0.7)
    plt.title(f'{algo}: Agents: {number_of_agents} - Frequency Distribution of Idle Time Across 100 Runs')
    plt.xlabel('Idle Time (seconds)')
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.savefig(os.path.join(run_directory, 'histogram_idle_times.png'), bbox_inches='tight')
    plt.close()

    print('All plots including idle time analysis have been saved successfully.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Analyze metrics from multiple runs.')
    parser.add_argument('--run_directory', type=str, required=True, help='Directory where run folders exist')
    parser.add_argument('--number_of_agents', type=int, required=True, help='Number of agents per run')
    parser.add_argument('--algo', type=str, required=True, help='Algorithm: pbft, swarm-single, swarm-multi')

    args = parser.parse_args()

    main(args.run_directory, args.number_of_agents, args.algo.upper())
