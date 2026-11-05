#!/usr/bin/env bash
# Step 5: install Python dependencies on every SWARM node (notebook cell 28):
# requirements.txt, pin protobuf==3.20.3, then requirements again.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

banner "Step 5: Python dependencies on $(swarm_nodes | wc -l | tr -d ' ') SWARM nodes"

install_deps() {
    local n="$1"
    nssh "$n" 'sudo bash -c "cd /root/SwarmAgents && pip3.11 install -r requirements.txt"' \
        && nssh "$n" 'sudo bash -c "cd /root/SwarmAgents && pip3.11 install protobuf==3.20.3"' \
        && nssh "$n" 'sudo bash -c "cd /root/SwarmAgents && pip3.11 install -r requirements.txt"'
}

run_nodes install_deps $(swarm_nodes)
