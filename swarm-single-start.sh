#!/bin/bash

pkill -f "main.py swarm-single"
python3 kafka_cleanup.py --topic agent-swarm-single
rm -rf swarm-single
rm -rf swarm-single
mkdir -p swarm-single swarm-single

# Define the number of agents
num_agents=5
base_index=0

# Launch the Python commands for each agent
for i in $(seq 0 $(($num_agents - 1))); do
    python3 main.py swarm-single $(($base_index + $i)) 100 $num_agents &
done
