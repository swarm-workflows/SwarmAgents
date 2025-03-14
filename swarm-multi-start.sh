#!/bin/bash

pkill -f "main.py swarm-multi"
python3.11 kafka_cleanup.py --topic agent-swarm-multi
rm -rf swarm-multi
rm -rf swarm-multi
mkdir -p swarm-multi swarm-multi

# Define the number of agents
num_agents=5
base_index=0

# Launch the Python commands for each agent
for i in $(seq 0 $(($num_agents - 1))); do
    python3.11 main.py swarm-multi $(($base_index + $i)) 100 &
done

