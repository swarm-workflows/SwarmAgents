#!/bin/bash

# Check if the required parameters are provided
if [ "$#" -lt 5 ]; then
    echo "Usage: $0 DEST_DIR MAX_RETRIES AGENT_TYPE TASKS_CNT JOBS_PER_PROPOSAL NUM_AGENTS TOPOLOGY DATABASE"
    exit 1
fi

# Assign command line arguments to variables
DEST_DIR="$1"
MAX_RETRIES="$2"
AGENT_TYPE="$3"
TASKS="$4"
JOBS_PER_PROPOSAL="$5"
NUM_AGENTS="$6"
TOPOLOGY="$7"
DATABASE="$8"
SLEEP_INTERVAL=10

# Read env vars for num_agents, topology, database
if [ -z "$NUM_AGENTS" ] || [ -z "$TOPOLOGY" ] || [ -z "$DATABASE" ]; then
    echo "NUM_AGENTS, TOPOLOGY, and DATABASE env vars must be set"
    exit 1
fi

# Function to check if agents are running
are_agents_running() {
    if pgrep -f "main.py $AGENT_TYPE" > /dev/null; then
        return 0
    else
        return 1
    fi
}

# Function to copy results and logs
copy_results_and_logs() {
    local index=$1
    local run_dir="$DEST_DIR/run$index"
    mkdir -p "$run_dir"
    cp $AGENT_TYPE/* "$run_dir/"
}

# Main loop
for ((i=1; i<=MAX_RETRIES; i++))
do
    echo "Starting agents... (Attempt $i)"
    ./"$AGENT_TYPE"-start.sh "$NUM_AGENTS" "$TOPOLOGY" "$DATABASE" "$TASKS" "$JOBS_PER_PROPOSAL"

    sleep $SLEEP_INTERVAL

    while are_agents_running; do
        echo "Agents are running... checking again in $SLEEP_INTERVAL seconds."
        sleep $SLEEP_INTERVAL
    done

    echo "Agents have stopped. Copying logs and restarting..."
    copy_results_and_logs $i
done

echo "Max retries reached. Exiting."
