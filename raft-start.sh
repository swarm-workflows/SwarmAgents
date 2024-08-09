#!/bin/bash

# Run the Kafka cleanup script
python3.11 kafka_cleanup.py

# Define the number of agents
num_agents=4

num_tasks=100

python3.11 main-raft.py 1 $num_agents $num_tasks &
python3.11 main-raft.py 2 $num_agents $num_tasks &
#python3.11 main-raft.py 3 $num_agents $num_tasks &
#python3.11 main-raft.py 4 $num_agents $num_tasks &
#python3.11 main-raft.py 5 $num_agents $num_tasks &
#python3.11 main-raft.py 6 $num_agents $num_tasks &
#python3.11 main-raft.py 7 $num_agents $num_tasks &
#python3.11 main-raft.py 8 $num_agents $num_tasks &
#python3.11 main-raft.py 9 $num_agents $num_tasks &