#!/usr/bin/env bash
# Run the full node-configuration pipeline from the database node.
# Equivalent to notebook cells 15-28 of SWARM-2slice.ipynb, but driven from
# inside the testbed (no laptop->bastion->node SSH needed).
#
# Usage:  ./setup_all.sh [start-step]     e.g. ./setup_all.sh 3  to resume
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
START="${1:-0}"

STEPS=(
    00_check_access.sh
    01_netplan.sh
    02_ssh_mesh.sh
    03_etc_hosts.sh
    04_push_swarm.sh
    05_deps.sh
    06_monitoring.sh
)

for s in "${STEPS[@]}"; do
    num="${s%%_*}"
    if [ "$((10#$num))" -lt "$START" ]; then echo "Skipping $s"; continue; fi
    "$HERE/$s"
done

. "$HERE/lib.sh"
banner "Setup complete"
cat <<EOF
Next (as in the notebook's 'Running SWARM-MULTI Consensus Setup' section):

  sudo bash -c "cd /root/SwarmAgents && docker compose up -d redis"
  sudo bash -c "cd /root/SwarmAgents && ./batch_tests_v2.py --runs 1 --base-out run-h-30-100 \\
      --mode remote --agent-type resource --agents 30 --topology hierarchical \\
      --hierarchical-level1-agent-type resource --jobs 100 --db-host database \\
      --job-interval 120 --jobs-per-interval 1"

Fetch results back to your laptop with:
  scp -r swarm:/root/SwarmAgents/run-h-30-100 .    # or tar first
EOF
