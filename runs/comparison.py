import os
from typing import List, Dict, Any

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import argparse


def compute_metrics(run_directory: str, number_of_agents: int, algo: str):
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

        if not os.path.exists(selection_time_file) or not os.path.exists(scheduling_latency_file) or \
                not os.path.exists(wait_time_file):
            continue

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

    # Flatten the lists for overall metrics analysis
    flat_selection_times = [item for sublist in selection_times for item in sublist]
    flat_scheduling_latencies = [item for sublist in scheduling_latencies for item in sublist]
    flat_wait_times = [item for sublist in wait_times for item in sublist]
    flat_idle_times = [item for agent_idle_times in all_idle_times for idle_time_list in agent_idle_times.values() for item in idle_time_list]

    means = [np.mean(run) for run in scheduling_latencies]
    medians = [np.median(run) for run in scheduling_latencies]
    conf_intervals = [stats.t.interval(0.95, len(run) - 1, loc=np.mean(run), scale=stats.sem(run)) for run in scheduling_latencies]
    ci_lower = [ci[0] for ci in conf_intervals]
    ci_upper = [ci[1] for ci in conf_intervals]

    return {
        'flat_selection_times': flat_selection_times,
        'flat_scheduling_latencies': flat_scheduling_latencies,
        'flat_wait_times': flat_wait_times,
        'flat_idle_times': flat_idle_times,
        'means':  means,
        'medians': medians,
        'ci_lower': ci_lower,
        'ci_upper': ci_upper,
        'algo': algo
    }


def plot_means_median_line(data_list: List[Dict[str, Any]], number_of_agents: int):
    plt.figure(figsize=(14, 10))
    colors = [["b-", "blue"], ["r-", "red"], ["g-", "green"]]

    idx = 0
    for mean_median in data_list:
        plt.plot(mean_median['means'], colors[idx][0], label=f'{mean_median["algo"]}: Mean')
        plt.plot(mean_median['medians'], colors[idx][0], label=f'{mean_median["algo"]}: Median')
        plt.fill_between(range(len(mean_median['means'])), mean_median['ci_lower'], mean_median['ci_upper'],
                         color=colors[idx][1], alpha=0.2,
                         label=f'{mean_median["algo"]}: 95% CI')
        idx += 1

    plt.title(f'Comparison of Scheduling Latency Across Algorithms - {number_of_agents} agents')
    plt.xlabel('Run Index')
    plt.ylabel('Scheduling Latency (seconds)')
    plt.legend()
    plt.grid(True)
    plt.savefig(f'comparison_line_plot_with_ci_{number_of_agents}.png', bbox_inches='tight')
    plt.close()


def plot_histograms(data_list: list, number_of_agents: int, param_name: str, param_pretty_name: str):
    plt.figure(figsize=(20, 14))
    colors = ["blue", "red", "green"]

    idx = 1
    for data in data_list:
        # Histogram for Algorithm 1
        plt.subplot(3, 1, idx)
        plt.hist(data[param_name], bins=20, color=colors[idx-1], alpha=0.7)
        plt.title(f'{data["algo"]}: Frequency Distribution of {param_pretty_name} - {number_of_agents} agents')
        plt.xlabel(f'{param_pretty_name} (seconds)')
        plt.ylabel('Frequency')
        plt.grid(True)
        idx += 1

    plt.suptitle(f'Comparison of {param_pretty_name} Across Algorithms - {number_of_agents} agents')
    plt.savefig(f'comparison_histogram_{param_name}_{number_of_agents}.png', bbox_inches='tight')
    plt.close()


def plot_overlaying_histograms(flat_selection_times_pbft: list, flat_selection_times_swarm_single: list,
                               flat_selection_times_swarm_multi: list, number_of_agents: int):
    plt.figure(figsize=(14, 10))

    # Overlay Histogram
    plt.hist(flat_selection_times_pbft, bins=20, color='blue', alpha=0.5, label='PBFT')
    plt.hist(flat_selection_times_swarm_single, bins=20, color='red', alpha=0.5, label='SWARM-SINGLE')
    plt.hist(flat_selection_times_swarm_multi, bins=20, color='green', alpha=0.5, label='SWARM-MULTI')

    plt.title(f'Comparison of Selection Times Across Algorithms - {number_of_agents} agents')
    plt.xlabel('Selection Time (seconds)')
    plt.ylabel('Frequency')
    plt.legend()
    plt.grid(True)
    plt.savefig(f'comparison_histogram_selection_times_{number_of_agents}.png', bbox_inches='tight')
    plt.close()


def plots(number_of_agents: int):
    data_pbft = compute_metrics(run_directory=f"./pbft/{number_of_agents}/repeated",
                                number_of_agents=number_of_agents, algo="PBFT")
    data_swarm_multi = compute_metrics(run_directory=f"./swarm/multi-jobs/{number_of_agents}/repeated",
                                       number_of_agents=number_of_agents, algo="SWARM-MULTI")
    data_swarm_single = compute_metrics(run_directory=f"./swarm/single-job/{number_of_agents}/repeated",
                                        number_of_agents=number_of_agents, algo="SWARM-SINGLE")
    data_list = [data_pbft, data_swarm_single, data_swarm_multi]
    plot_means_median_line(number_of_agents=number_of_agents, data_list=data_list)
    plot_histograms(number_of_agents=number_of_agents, data_list=data_list, param_name='flat_wait_times',
                    param_pretty_name="Wait Time")
    plot_histograms(number_of_agents=number_of_agents, data_list=data_list, param_name='flat_idle_times',
                    param_pretty_name="Idle Time")
    plot_histograms(number_of_agents=number_of_agents, data_list=data_list, param_name='flat_selection_times',
                    param_pretty_name="Selection Time")
    plot_histograms(number_of_agents=number_of_agents, data_list=data_list, param_name='flat_scheduling_latencies',
                    param_pretty_name="Scheduling Latency")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Compare Metrics across algorithms.')
    parser.add_argument('--number_of_agents', type=int, required=True, help='Number of agents per run')
    args = parser.parse_args()
    plots(number_of_agents=args.number_of_agents)