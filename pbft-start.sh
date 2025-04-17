#!/bin/bash
# Define the number of agents
num_agents=$1
base_index=0

pkill -f "main.py pbft"
python3 kafka_cleanup.py --topic pbft
rm -rf pbft
rm -rf pbft
mkdir -p pbft pbft

# Launch the Python commands for each agent
for i in $(seq 0 $(($num_agents - 1))); do
    python3 main.py pbft $(($base_index + $i)) 100 $num_agents &
done
