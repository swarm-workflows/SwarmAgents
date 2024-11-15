import os
import pandas as pd
import matplotlib.pyplot as plt

# Define file paths for each algorithm and agent count
raft_file_paths = ['./raft/3/scheduling_latency.csv',
                   './raft/5/scheduling_latency.csv',
                   './raft/10/scheduling_latency.csv']
pbft_file_paths = ['./pbft/3/repeated/run50/scheduling_latency_0.csv',
                   './pbft/5/repeated/run50/scheduling_latency_0.csv',
                   './pbft/10/repeated/run50/scheduling_latency_0.csv']
swarm_single_file_paths = ['./swarm/single-job/3/repeated/run50/scheduling_latency_0.csv',
                           './swarm/single-job/5/repeated/run50/scheduling_latency_0.csv',
                           './swarm/single-job/10/repeated/run50/scheduling_latency_0.csv']
swarm_multiple_file_paths = ['./swarm/multi-jobs/3/repeated/run50/scheduling_latency_0.csv',
                             './swarm/multi-jobs/5/repeated/run50/scheduling_latency_0.csv',
                             './swarm/multi-jobs/10/repeated/run50/scheduling_latency_0.csv']

# Load data for each agent count
data_3_agents = {
    "Raft (3 Agents)": pd.read_csv(raft_file_paths[0]),
    "PBFT (3 Agents)": pd.read_csv(pbft_file_paths[0]),
    "SWARM-SINGLE (3 Agents)": pd.read_csv(swarm_single_file_paths[0]),
    "SWARM-MULTI (3 Agents)": pd.read_csv(swarm_multiple_file_paths[0])
}

data_5_agents = {
    "Raft (5 Agents)": pd.read_csv(raft_file_paths[1]),
    "PBFT (5 Agents)": pd.read_csv(pbft_file_paths[1]),
    "SWARM-SINGLE (5 Agents)": pd.read_csv(swarm_single_file_paths[1]),
    "SWARM-MULTI (5 Agents)": pd.read_csv(swarm_multiple_file_paths[1])
}

data_10_agents = {
    "Raft (10 Agents)": pd.read_csv(raft_file_paths[2]),
    "PBFT (10 Agents)": pd.read_csv(pbft_file_paths[2]),
    "SWARM-SINGLE (10 Agents)": pd.read_csv(swarm_single_file_paths[2]),
    "SWARM-MULTI (10 Agents)": pd.read_csv(swarm_multiple_file_paths[2])
}

# Color mapping for each algorithm
color_map = {
    "PBFT": "#1f77b4",
    "SWARM-SINGLE": "#ff7f0e",
    "SWARM-MULTI": "#2ca02c",
    "Raft": "#d62728"
}

# Set up the figure with two subplots
fig, axs = plt.subplots(2, 1, figsize=(6, 8))  # Smaller figure size
fig = plt.figure(figsize=(8, 4))


# Function to plot all algorithms for a given agent count on a specified subplot axis
def plot_all_algorithms(data, ax):
    for algo, df in data.items():
        # Extract the base algorithm name from the label to match color map
        base_algo = algo.split()[0]
        color = color_map.get(base_algo, 'black')  # Default to black if algo not found
        ax.plot(df.index, df['scheduling_latency'], label=algo, color=color, marker='o', markersize=2)

    ax.set_xlabel('Task Index', fontsize=8)
    ax.set_ylabel('Scheduling Latency (seconds)', fontsize=8)
    ax.grid(True)
    plt.legend(fontsize=7)


# Function to plot all algorithms for a given agent count on a specified subplot axis
def plot_all_algorithms_fig(data):
    for algo, df in data.items():
        # Extract the base algorithm name from the label to match color map
        base_algo = algo.split()[0]
        color = color_map.get(base_algo, 'black')  # Default to black if algo not found
        plt.plot(df.index, df['scheduling_latency'], label=algo, color=color, marker='o', markersize=2)

    plt.xlabel('Task Index', fontsize=8)
    plt.ylabel('Scheduling Latency (seconds)', fontsize=8)
    plt.grid(True)
    plt.legend(fontsize='medium')



if __name__ == '__main__':
    # Plot for 5 agents and 10 agents
    #plot_all_algorithms(data_5_agents, axs[0])
    #plot_all_algorithms(data_10_agents, axs[1])
    plot_all_algorithms_fig(data_10_agents)

    # Adjust layout and save the figure as a single PNG
    plt.tight_layout()  # Adjust layout to fit in a smaller space
    plt.savefig('./scheduling_latency_all_agents.png', dpi=150)  # High DPI for clarity
    plt.close()
