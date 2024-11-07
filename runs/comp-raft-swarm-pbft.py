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

# Set up the figure with three subplots and further reduced figsize
fig, axs = plt.subplots(2, 1, figsize=(6, 8))  # Smaller figure size

# Function to plot all algorithms for a given agent count on a specified subplot axis
def plot_all_algorithms(data, ax):
    for algo, df in data.items():
        ax.plot(df.index, df['scheduling_latency'], label=algo, marker='o', markersize=2)

    ax.set_xlabel('Task Index', fontsize=8)
    ax.set_ylabel('Scheduling Latency (seconds)', fontsize=8)
    ax.grid(True)
    ax.legend(fontsize=7)

if __name__ == '__main__':
    # Plot for 3 agents, 5 agents, and 10 agents
    #plot_all_algorithms(data_3_agents, axs[0])
    plot_all_algorithms(data_5_agents, axs[0])
    plot_all_algorithms(data_10_agents, axs[1])

    # Adjust layout and save the figure as a single PNG
    plt.tight_layout()  # Adjust layout to fit in a smaller space
    plt.savefig('./scheduling_latency_all_agents.png', dpi=150)  # High DPI for clarity
    plt.close()
