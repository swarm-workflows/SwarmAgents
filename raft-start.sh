#!/bin/bash
num_agents=$1
base_index=0

pkill -f "main-raft.py"
python3 kafka_cleanup.py --topic agent
rm -rf raft
mkdir -p raft


# Launch the Python commands for each agent
for i in $(seq 0 $(($num_agents - 1))); do
    python3 main-raft.py $(($base_index + $i)) $num_agents 100 &
    sleep 1
done