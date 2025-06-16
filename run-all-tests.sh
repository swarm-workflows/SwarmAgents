#!/bin/bash

# Configuration
AGENT_COUNTS=(10 30 40 50 60 70 80 90 100)
TOPOLOGIES=("mesh" "ring")
AGENT_TYPES=("swarm-multi" "redis-swarm-multi")
MAX_RETRIES=3  # Number of repetitions per configuration

# Base output directory
BASE_OUTPUT_DIR="results"

# Loop through all combinations
for agent_type in "${AGENT_TYPES[@]}"; do
    for topology in "${TOPOLOGIES[@]}"; do
        for num_agents in "${AGENT_COUNTS[@]}"; do

            echo "==============================================="
            echo "Running: $agent_type | $topology | Agents: $num_agents"
            echo "==============================================="

            # Build output directory
            output_dir="${BASE_OUTPUT_DIR}/${agent_type}/${topology}/${num_agents}"
            mkdir -p "$output_dir"

            # Export required args to launch-runs.sh
            export NUM_AGENTS="$num_agents"
            export TOPOLOGY="$topology"

            # Run the experiment
            ./launch-runs.sh "$output_dir" "$MAX_RETRIES" "$agent_type"
        done
    done
done

echo "All runs completed!"
