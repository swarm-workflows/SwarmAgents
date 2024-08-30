import argparse
import os
import pandas as pd
import matplotlib.pyplot as plt


def plot_scheduling_latency(algo: str, base_dir: str):
    # Define file paths dynamically based on the algorithm and base directory
    file_paths = [
        os.path.join(base_dir, f'3/repeated/run2/scheduling_latency_0.csv'),
        os.path.join(base_dir, f'5/repeated/run2/scheduling_latency_0.csv'),
        os.path.join(base_dir, f'10/repeated/run2/scheduling_latency_0.csv')
    ]

    # Load data from each file
    data1 = pd.read_csv(file_paths[0])
    data2 = pd.read_csv(file_paths[1])
    data3 = pd.read_csv(file_paths[2])

    # Plotting the data
    plt.figure(figsize=(10, 6))

    plt.plot(data1.index, data1['scheduling_latency'], label='Total Agents: 3', marker='o')
    plt.plot(data2.index, data2['scheduling_latency'], label='Total Agents: 5', marker='s')
    plt.plot(data3.index, data3['scheduling_latency'], label='Total Agents: 10', marker='^')

    plt.title(f'{algo.upper()}: Scheduling Latency')
    plt.xlabel('Task Index')
    plt.ylabel('Time Units (seconds)')
    plt.grid(True)
    plt.legend()

    # Save the plot
    plot_path = os.path.join(base_dir, f'overall-scheduling-latency.png')
    plt.savefig(plot_path, bbox_inches='tight')  # bbox_inches='tight' ensures that the entire plot is saved
    plt.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Overll scheduling latency comparison')
    parser.add_argument('--run_directory', type=str, required=True, help='Directory where run folders exist')
    parser.add_argument('--algo', type=str, required=True, help='Algorithm: pbft, swarm-single, swarm-multi')

    args = parser.parse_args()

    plot_scheduling_latency(args.algo, args.run_directory)
