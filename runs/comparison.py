import os
from typing import List, Dict, Any

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
import argparse
import seaborn as sns

colors = [["#1f77b4", "#aec7e8"], ["#ff7f0e", "#ffbb78"], ["#2ca02c", "#98df8a"]]
figsize=(8, 4)


def compute_metrics(run_directory: str, number_of_agents: int, algo: str):
    # Initialize lists to store the data from all runs
    selection_times = []
    scheduling_latencies = []
    wait_times = []
    all_idle_times = []
    all_idle_times_list = []

    # Loop through the directories for each run
    for i in range(3, 100):
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
        all_idle_times_list.append(idle_times_list)

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

    idle_means = [np.mean(run) for run in all_idle_times_list]
    idle_medians = [np.median(run) for run in all_idle_times_list]
    idle_conf_intervals = [stats.t.interval(0.95, len(run) - 1, loc=np.mean(run), scale=stats.sem(run)) for run in all_idle_times_list]
    idle_ci_lower = [ci[0] for ci in idle_conf_intervals]
    idle_ci_upper = [ci[1] for ci in idle_conf_intervals]

    return {
        'flat_selection_times': flat_selection_times,
        'flat_scheduling_latencies': flat_scheduling_latencies,
        'flat_wait_times': flat_wait_times,
        'flat_idle_times': flat_idle_times,
        'means':  means,
        'medians': medians,
        'ci_lower': ci_lower,
        'ci_upper': ci_upper,
        'idle_means': idle_means,
        'idle_medians': idle_medians,
        'idle_ci_lower': idle_ci_lower,
        'idle_ci_upper': idle_ci_upper,
        'algo': algo
    }


def plot_means_median_idle_box_plots_with_points(data_list: List[Dict[str, Any]], number_of_agents: int):
    data_frames = []
    for d in data_list:
        df = pd.DataFrame({'Idle Time': d['idle_means'], 'Algorithm': d['algo']})
        data_frames.append(df)
    combined_df = pd.concat(data_frames)
    plt.figure(figsize=(10, 6))
    sns.boxplot(x='Algorithm', y='Idle Time', data=combined_df, showmeans=True)
    sns.stripplot(x='Algorithm', y='Idle Time', data=combined_df, color='black', alpha=0.5, jitter=True)
    plt.title(f'Box Plot of Idle Time with Individual Runs - {number_of_agents} agents')
    plt.ylabel('Idle Time (seconds)')
    plt.grid(axis='y', linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.savefig(f'box_plot_idle_time_{number_of_agents}.png')
    plt.close()


def plot_means_median_idle_ci_bars(data_list: List[Dict[str, Any]], number_of_agents: int):
    plt.figure(figsize=(10, 6))
    for idx, d in enumerate(data_list):
        mean_idle = np.mean(d['idle_means'])
        ci_low = np.mean(d['idle_ci_lower'])
        ci_high = np.mean(d['idle_ci_upper'])
        plt.errorbar(idx, mean_idle, yerr=[[mean_idle - ci_low], [ci_high - mean_idle]], fmt='o', capsize=5,
                     label=d['algo'])

    plt.xticks(range(len(data_list)), [d['algo'] for d in data_list])
    plt.ylabel('Mean Idle Time (seconds)')
    plt.title(f'Dot Plot with Confidence Intervals - {number_of_agents} agents')
    plt.grid(axis='y', linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.savefig(f'dot_plot_idle_time_{number_of_agents}.png')
    plt.close()


def plot_means_median_violin(data_list: List[Dict[str, Any]], number_of_agents: int):
    data_frames = []
    for d in data_list:
        df = pd.DataFrame({'Idle Time': d['idle_means'], 'Algorithm': d['algo']})
        data_frames.append(df)
    combined_df = pd.concat(data_frames)

    plt.figure(figsize=(10, 6))
    sns.violinplot(x='Algorithm', y='Idle Time', data=combined_df, scale='width', inner='quartile')
    plt.title(f'Violin Plot of Idle Time Distribution - {number_of_agents} agents')
    plt.ylabel('Idle Time (seconds)')
    plt.grid(axis='y', linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.savefig(f'violin_plot_idle_time_{number_of_agents}.png')
    plt.close()


def print_percentage_improvements(data_list: List[Dict[str, Any]]):
    # Assuming data_list contains 'algo' and 'idle_means'
    algorithms = [d['algo'] for d in data_list]
    means = [np.mean(d['idle_means']) for d in data_list]

    # Use the first algorithm as the baseline
    baseline_mean = means[0]

    print(f"Baseline Algorithm: {algorithms[0]} (Mean Idle Time: {baseline_mean:.2f})")

    # Compute and print percentage improvements
    for algo, mean in zip(algorithms, means):
        improvement = ((baseline_mean - mean) / baseline_mean) * 100
        print(f"{algo}: {improvement:.2f}% improvement over {algorithms[0]} (Mean Idle Time: {mean:.2f})")


def plot_means_median_histogram_error_bars_percent(data_list: List[Dict[str, Any]], number_of_agents: int):
    # Assuming data_list contains 'algo', 'idle_means', 'idle_ci_lower', 'idle_ci_upper'
    algorithms = [d['algo'] for d in data_list]
    means = [np.mean(d['idle_means']) for d in data_list]
    ci_errors = [((np.mean(d['idle_means']) - np.mean(d['idle_ci_lower'])),
                  (np.mean(d['idle_ci_upper']) - np.mean(d['idle_means']))) for d in data_list]

    # Compute percentage improvements relative to the first algorithm (e.g., PBFT as baseline)
    baseline_mean = means[0]
    percentage_improvements = [
        ((baseline_mean - mean) / baseline_mean) * 100 for mean in means
    ]

    # x positions for bars
    x = np.arange(len(algorithms))

    # Create the bar plot
    plt.figure(figsize=(10, 6))
    plt.bar(x, means, yerr=np.array(ci_errors).T, capsize=5, alpha=0.7, tick_label=algorithms)

    # Annotate percentage improvements on top of bars
    for i, (mean, improvement) in enumerate(zip(means, percentage_improvements)):
        plt.text(x[i], mean + max(ci_errors[i]) * 0.1, f'{improvement:.1f}%', ha='center', fontsize=10)

    plt.xlabel('Algorithms')
    plt.ylabel('Mean Idle Time (seconds)')
    plt.title(f'Comparison of Mean Idle Time with Error Bars - {number_of_agents} agents')
    plt.grid(axis='y', linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.savefig(f'histogram_idle_time_with_error_bars_{number_of_agents}_percent.png')
    plt.close()


def plot_means_median_histogram_error_bars(data_list: List[Dict[str, Any]], number_of_agents: int):
    # Assuming data_list contains 'algo', 'idle_means', 'idle_ci_lower', 'idle_ci_upper'
    algorithms = [d['algo'] for d in data_list]
    means = [np.mean(d['idle_means']) for d in data_list]
    ci_errors = [((np.mean(d['idle_means']) - np.mean(d['idle_ci_lower'])),
                  (np.mean(d['idle_ci_upper']) - np.mean(d['idle_means']))) for d in data_list]

    x = np.arange(len(algorithms))  # x positions for bars

    plt.figure(figsize=(10, 6))
    plt.bar(x, means, yerr=np.array(ci_errors).T, capsize=5, alpha=0.7, tick_label=algorithms)
    plt.xlabel('Algorithms')
    plt.ylabel('Mean Idle Time (seconds)')
    plt.title(f'Comparison of Mean Idle Time with Error Bars - {number_of_agents} agents')
    plt.grid(axis='y', linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.savefig(f'histogram_idle_time_with_error_bars_{number_of_agents}.png')
    plt.close()


def plot_means_median_line_idle(data_list: List[Dict[str, Any]], number_of_agents: int):
    plt.figure(figsize=figsize)

    for idx, mean_median in enumerate(data_list):
        # Plot mean line
        plt.plot(mean_median['idle_means'], color=colors[idx][0], label=f'{mean_median["algo"]}: Mean')
        # Plot median points
        plt.plot(mean_median['idle_medians'], linestyle=':', color=colors[idx][0], label=f'{mean_median["algo"]}: Median')
        # CI shading
        plt.fill_between(range(len(mean_median['idle_means'])), mean_median['idle_ci_lower'], mean_median['idle_ci_upper'],
                         color=colors[idx][1], alpha=0.3, label=f'{mean_median["algo"]}: 95% CI')

    #plt.title(f'Comparison of Idle Time Across Algorithms - {number_of_agents} agents')
    plt.xlabel('Run Index')
    plt.ylabel('Idle Time (seconds)')
    #, fontsize='medium'
    plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
    plt.rcParams.update({'font.size': 14})
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f'comparison_line_plot_idle_time_with_ci_{number_of_agents}.png', bbox_inches='tight')
    plt.close()


def plot_means_median_line_percent(data_list: List[Dict[str, Any]], number_of_agents: int):
    plt.figure(figsize=figsize)

    # Calculate mean improvements relative to the first algorithm
    baseline_mean = np.mean(data_list[0]['means'])  # First algorithm as baseline
    improvements = [
        ((baseline_mean - np.mean(d['means'])) / baseline_mean) * 100 if idx > 0 else 0
        for idx, d in enumerate(data_list)
    ]

    for idx, mean_median in enumerate(data_list):
        # Plot mean line
        plt.plot(mean_median['means'], color=colors[idx][0], label=f'{mean_median["algo"]}: Mean ({np.mean(mean_median["means"]):.2f}s, {improvements[idx]:.1f}% improvement)')
        # Plot median points
        plt.plot(mean_median['medians'], linestyle=':', color=colors[idx][0], label=f'{mean_median["algo"]}: Median')
        # CI shading
        plt.fill_between(range(len(mean_median['means'])), mean_median['ci_lower'], mean_median['ci_upper'],
                         color=colors[idx][1], alpha=0.3, label=f'{mean_median["algo"]}: 95% CI')

    # Set labels and legend
    plt.xlabel('Run Index')
    plt.ylabel('Scheduling Latency (seconds)')
    plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
    plt.rcParams.update({'font.size': 14})
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f'comparison_line_plot_with_ci_{number_of_agents}_percent.png', bbox_inches='tight')
    plt.close()


def plot_means_median_line(data_list: List[Dict[str, Any]], number_of_agents: int):
    plt.figure(figsize=figsize)

    for idx, mean_median in enumerate(data_list):
        # Plot mean line
        plt.plot(mean_median['means'], color=colors[idx][0], label=f'{mean_median["algo"]}: Mean')
        # Plot median points
        plt.plot(mean_median['medians'], linestyle=':', color=colors[idx][0], label=f'{mean_median["algo"]}: Median')
        # CI shading
        plt.fill_between(range(len(mean_median['means'])), mean_median['ci_lower'], mean_median['ci_upper'],
                         color=colors[idx][1], alpha=0.3, label=f'{mean_median["algo"]}: 95% CI')

    #plt.title(f'Comparison of Scheduling Latency Across Algorithms - {number_of_agents} agents')
    plt.xlabel('Run Index')
    plt.ylabel('Scheduling Latency (seconds)')
    plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
    plt.rcParams.update({'font.size': 14})
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f'comparison_line_plot_with_ci_{number_of_agents}.png', bbox_inches='tight')
    plt.close()


def plot_histograms(data_list: list, number_of_agents: int, param_name: str, param_pretty_name: str):
    plt.figure(figsize=(20, 14))

    idx = 1
    for data in data_list:
        # Histogram for Algorithm 1
        plt.subplot(3, 1, idx)
        plt.hist(data[param_name], bins=20, color=colors[idx-1][0], alpha=0.7)
        #plt.title(f'{data["algo"]}: Frequency Distribution of {param_pretty_name} - {number_of_agents} agents')
        plt.xlabel(f'{param_pretty_name} (seconds)')
        plt.ylabel('Frequency')
        plt.grid(True)
        idx += 1

    plt.suptitle(f'Comparison of {param_pretty_name} Across Algorithms - {number_of_agents} agents')
    plt.savefig(f'comparison_histogram_{param_name}_{number_of_agents}.png', bbox_inches='tight')
    plt.close()


def plot_overlaying_histograms(flat_selection_times_pbft: list, flat_selection_times_swarm_single: list,
                               flat_selection_times_swarm_multi: list, number_of_agents: int):
    plt.figure(figsize=figsize)

    # Overlay Histogram
    plt.hist(flat_selection_times_pbft, bins=20, color=colors[0], alpha=0.5, label='PBFT')
    plt.hist(flat_selection_times_swarm_single, bins=20, color=colors[1], alpha=0.5, label='SWARM-SINGLE')
    plt.hist(flat_selection_times_swarm_multi, bins=20, color=colors[2], alpha=0.5, label='SWARM-MULTI')

    #plt.title(f'Comparison of Selection Times Across Algorithms - {number_of_agents} agents')
    plt.xlabel('Selection Time (seconds)')
    plt.ylabel('Frequency')
    plt.legend()
    plt.grid(True)
    plt.savefig(f'comparison_histogram_selection_times_{number_of_agents}.png', bbox_inches='tight')
    plt.close()


def plot_combined_ci(data_list: List[Dict[str, Any]], number_of_agents: int):
    # Set up a single figure with two subplots for Scheduling Latency and Idle Time
    fig, axs = plt.subplots(1, 2, figsize=(12, 6), constrained_layout=True)

    # Plot Scheduling Latency (first subplot)
    for idx, mean_median in enumerate(data_list):
        axs[0].plot(mean_median['means'], color=colors[idx][0], label=f'{mean_median["algo"]}: Mean')
        axs[0].plot(mean_median['medians'], linestyle=':', color=colors[idx][0], label=f'{mean_median["algo"]}: Median')
        axs[0].fill_between(range(len(mean_median['means'])), mean_median['ci_lower'], mean_median['ci_upper'],
                            color=colors[idx][1], alpha=0.3, label=f'{mean_median["algo"]}: 95% CI')
    axs[0].set_title('Scheduling Latency')
    axs[0].set_xlabel('Run Index')
    axs[0].set_ylabel('Latency (seconds)')
    #axs[0].legend(loc='upper left', bbox_to_anchor=(1, 1))
    axs[0].grid(True)

    # Plot Idle Time (second subplot)
    for idx, mean_median in enumerate(data_list):
        axs[1].plot(mean_median['idle_means'], color=colors[idx][0], label=f'{mean_median["algo"]}: Mean')
        axs[1].plot(mean_median['idle_medians'], linestyle=':', color=colors[idx][0],
                    label=f'{mean_median["algo"]}: Median')
        axs[1].fill_between(range(len(mean_median['idle_means'])), mean_median['idle_ci_lower'],
                            mean_median['idle_ci_upper'],
                            color=colors[idx][1], alpha=0.3, label=f'{mean_median["algo"]}: 95% CI')
    axs[1].set_title('Idle Time')
    axs[1].set_xlabel('Run Index')
    axs[1].set_ylabel('Idle Time (seconds)')
    axs[1].legend(loc='upper left', bbox_to_anchor=(1, 1))
    axs[1].grid(True)

    # Save the combined figure
    plt.savefig(f'comparison_combined_ci_{number_of_agents}.png', dpi=150, bbox_inches='tight')
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
    plot_means_median_line_percent(number_of_agents=number_of_agents, data_list=data_list)
    plot_means_median_histogram_error_bars(number_of_agents=number_of_agents, data_list=data_list)
    print_percentage_improvements(data_list=data_list)
    plot_histograms(number_of_agents=number_of_agents, data_list=data_list, param_name='flat_wait_times',
                    param_pretty_name="Wait Time")
    plot_histograms(number_of_agents=number_of_agents, data_list=data_list, param_name='flat_idle_times',
                    param_pretty_name="Idle Time")
    plot_histograms(number_of_agents=number_of_agents, data_list=data_list, param_name='flat_selection_times',
                    param_pretty_name="Selection Time")
    plot_histograms(number_of_agents=number_of_agents, data_list=data_list, param_name='flat_scheduling_latencies',
                    param_pretty_name="Scheduling Latency")
    #plot_combined_ci(data_list=data_list, number_of_agents=number_of_agents)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Compare Metrics across algorithms.')
    parser.add_argument('--number_of_agents', type=int, required=True, help='Number of agents per run')
    args = parser.parse_args()
    plots(number_of_agents=args.number_of_agents)