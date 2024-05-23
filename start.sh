#!/bin/bash

# Run the Kafka cleanup script
python kafka_cleanup.py
python task_generator.py

# Define the number of agents
num_agents=5
base_id=0

# Create an array to store PIDs
pids=()

# Loop through the agent IDs and start each one in the background
for agent_id in $(seq 1 $num_agents); do
  ag_id=$((agent_id + base_id))
  python main.py --agent_id $ag_id &
  pids+=($!) # Store the PID of the background process
done

# Save the PIDs to a file
echo "${pids[@]}" > agent_pids.txt

echo "Started $num_agents agents."

