#!/bin/bash
# Start one Sparrow-style baseline process (worker or scheduler) on this host, detached.
# Usage: sparrow-node-start.sh <python> <role> <node-id> <args for baselines/sparrow_node.py...>
#
# Called over ssh by baselines/run_sparrow.py; prints the PID. The log is
# sparrow-<role>-<node-id>.log in the repo directory, which the orchestrator copies back.
set -euo pipefail

PY="$1"; ROLE="$2"; NODE="$3"; shift 3
cd "$(dirname "$0")"
nohup "$PY" baselines/sparrow_node.py "$ROLE" "$@" \
    </dev/null >"sparrow-${ROLE}-${NODE}.log" 2>&1 &
disown
echo $!
