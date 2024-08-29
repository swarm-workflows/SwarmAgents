#!/bin/bash
# MIT License
#
# Copyright (c) 2024 swarm-workflows

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Author: Komal Thareja(kthare10@renci.org)

# Check if the required parameters are provided
if [ "$#" -lt 3 ]; then
    echo "Usage: $0 DEST_DIR MAX_RETRIES AGENT_TYPE"
    exit 1
fi

# Assign command line arguments to variables
DEST_DIR="$1"       # Directory where results will be copied
MAX_RETRIES="$2"    # Number of times to restart the agents
AGENT_TYPE="$3"
LOG_DIR="logs-$AGENT_TYPE"
RESULTS_DIR="$AGENT_TYPE"
SLEEP_INTERVAL=10   # Time in seconds between checks

# Function to check if agents are running
are_agents_running() {
    if pgrep -f "python3.11 main.py $AGENT_TYPE" > /dev/null; then
        return 0  # Agents are running
    else
        return 1  # Agents are not running
    fi
}

# Function to copy results and logs
copy_results_and_logs() {
    local index=$1
    local run_dir="$DEST_DIR/run$index"
    mkdir -p "$run_dir"
    mv "$LOG_DIR/*.log*" "$RESULTS_DIR/*.png" "$RESULTS_DIR/*.csv" "$run_dir/"
}

# Main loop
for ((i=1; i<=MAX_RETRIES; i++))
do
    echo "Starting agents... (Attempt $i)"
    sh "$AGENT_TYPE-start.sh"

    # Wait for agents to start and then begin polling
    sleep $SLEEP_INTERVAL

    while are_agents_running; do
        echo "Agents are running... checking again in $SLEEP_INTERVAL seconds."
        sleep $SLEEP_INTERVAL
    done

    echo "Agents have stopped. Copying logs and restarting..."
    copy_results_and_logs $i

done

echo "Max retries reached. Exiting."
