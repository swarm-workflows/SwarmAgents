#!/bin/bash
# Start a baseline worker on a remote host.
# Usage: baseline-worker-start.sh <agent_id> <db_host> [db_port]
#
# Called via SSH from run_baseline_remote.py, or manually:
#   ssh agent-1 'cd /root/SwarmAgents && ./baseline-worker-start.sh 1 10.0.0.1'

set -euo pipefail

AGENT_ID="$1"
DB_HOST="$2"
DB_PORT="${3:-6379}"

cd "$(dirname "$0")"

nohup python3.11 baselines/baseline_worker.py \
    --agent-id "$AGENT_ID" \
    --db-host "$DB_HOST" \
    --db-port "$DB_PORT" \
    </dev/null >"baseline-worker-${AGENT_ID}.log" 2>&1 &
disown

echo $!
