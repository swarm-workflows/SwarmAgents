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

# Configurable variables
RES_DIR="runs/swarm/multi-jobs/3/repeated"  # Directory where results will be copied
MAX_RETRIES=5                     # Number of times to restart the agents
SLEEP_INTERVAL=10                 # Time in seconds between checks
START_SCRIPT=swarm-start.sh
AGENT_SCRIPT=main-swarm.py

# Function to check if agents are running
are_agents_running() {
    if pgrep -f "python3.11 $AGENT_SCRIPT" > /dev/null; then
        return 0  # Agents are running
    else
        return 1  # Agents are not running
    fi
}

# Function to copy results and logs
copy_results_and_logs() {
    local index=$1
    local run_dir="$RES_DIR/run$index"
    mkdir -p "$run_dir"
    mv *.log *.png *.csv "$run_dir/"
}

# Main loop
for ((i=1; i<=MAX_RETRIES; i++))
do
    echo "Starting agents... (Attempt $i)"
    sh $START_SCRIPT

    # Wait for agents to start and then begin polling
    sleep $SLEEP_INTERVAL

    while are_agents_running; do
        echo "Agents are running... checking again in $SLEEP_INTERVAL seconds."
        sleep $SLEEP_INTERVAL
    done

    echo "Agents have stopped. Copying logs and restarting..."
    copy_results_and_logs $i

    echo "Cleanup before starting new run"
    rm -rf *.log* *.csv *.png
    python3 kafka_cleanup.py

done

echo "Max retries reached. Exiting."
