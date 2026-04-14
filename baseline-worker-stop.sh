#!/bin/bash
# Stop baseline workers on all hosts listed in agent_hosts.txt.
# Usage: baseline-worker-stop.sh [agent_hosts_file]
#
# Also sets the Redis shutdown key so workers exit gracefully.

set -euo pipefail

HOSTS_FILE="${1:-agent_hosts.txt}"
DB_HOST="${2:-localhost}"
DB_PORT="${3:-6379}"

if [ ! -f "$HOSTS_FILE" ]; then
    echo "Error: hosts file '$HOSTS_FILE' not found"
    exit 1
fi

# Set Redis shutdown signal
redis-cli -h "$DB_HOST" -p "$DB_PORT" SET baseline:shutdown 1 EX 120 2>/dev/null || true

echo "Stopping baseline workers on all hosts …"
while IFS= read -r host; do
    host=$(echo "$host" | xargs)  # trim whitespace
    [ -z "$host" ] && continue
    [[ "$host" == \#* ]] && continue
    echo "  Stopping workers on $host"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -o BatchMode=yes "$host" \
        "pkill -f 'baseline_worker.py' 2>/dev/null; true"
done < "$HOSTS_FILE"

echo "Done."
