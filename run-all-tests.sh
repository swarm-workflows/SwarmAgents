#!/bin/bash

# Configuration
AGENT_COUNTS=(10)
TOPOLOGIES=("mesh" "ring" "star")
AGENT_TYPES=("swarm-multi")
MAX_RETRIES=1  # Number of repetitions per configuration
TASKS=100
JOBS_PER_PROPSAL=10
DATABASE=localhost

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
            ./launch-runs.sh "$output_dir" "$MAX_RETRIES" "$agent_type" "$TASKS" "$JOBS_PER_PROPSAL" "$num_agents" "$topology" "$DATABASE"
        done
    done
done

echo "All runs completed!"
