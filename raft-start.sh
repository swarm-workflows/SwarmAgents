#!/bin/bash

# Run the Kafka cleanup script
python kafka_cleanup.py

# Define the number of agents
num_agents=4

num_tasks=100

python main-raft.py 0 $num_agents $num_tasks &
python main-raft.py 1 $num_agents $num_tasks &
python main-raft.py 2 $num_agents $num_tasks &
python main-raft.py 3 $num_agents $num_tasks &