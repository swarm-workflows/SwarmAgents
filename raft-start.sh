#!/bin/bash

pkill -f "main-raft.py"
python3.11 kafka_cleanup.py --topic agent
rm -rf raft
mkdir -p raft
# Define the number of agents
num_agents=10

# Launch the Python commands for each agent
for i in $(seq 0 $(($num_agents - 1))); do
    python3.11 main-raft.py $i $num_agents 100 &
    sleep 1
done