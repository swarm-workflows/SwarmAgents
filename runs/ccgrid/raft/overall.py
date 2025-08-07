import os

import pandas as pd
import matplotlib.pyplot as plt

# Load all three CSV files
file_paths = ['./3/scheduling_latency.csv', './5/scheduling_latency.csv', './10/scheduling_latency.csv']

# Load data from each file
data1 = pd.read_csv(file_paths[0])
data2 = pd.read_csv(file_paths[1])
data3 = pd.read_csv(file_paths[2])

# Plotting the data
plt.figure(figsize=(10, 6))

plt.plot(data1.index, data1['scheduling_latency'], label='Total Agents: 3', marker='o')
plt.plot(data2.index, data2['scheduling_latency'], label='Total Agents: 5', marker='s')
plt.plot(data3.index, data3['scheduling_latency'], label='Total Agents: 10', marker='^')

plt.title('RAFT: Scheduling Latency')
plt.xlabel('Task Index')
plt.ylabel('Time Units (seconds)')
plt.grid(True)
plt.legend()
plot_path = os.path.join("", 'overall-scheduling-latency.png')
plt.savefig(plot_path, bbox_inches='tight')  # bbox_inches='tight' ensures that the entire plot is saved
plt.close()

