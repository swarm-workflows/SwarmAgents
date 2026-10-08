#!/bin/bash
# Start one centralized-baseline execution worker on this host, detached.
# Usage: baseline-worker-start.sh <python> <agent_id> <args for baselines/baseline_worker.py...>
#
# Called over ssh by baselines/run_baseline_remote.py; prints the PID. The log is
# baseline-worker-<agent_id>.log in the repo directory, which the orchestrator copies back.
set -euo pipefail

PY="$1"; AGENT_ID="$2"; shift 2
cd "$(dirname "$0")"
nohup "$PY" baselines/baseline_worker.py --agent-id "$AGENT_ID" "$@" \
    </dev/null >"baseline-worker-${AGENT_ID}.log" 2>&1 &
disown
echo $!
