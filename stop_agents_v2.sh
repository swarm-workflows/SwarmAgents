#!/bin/bash
# stop_agents_v2.sh — stop SwarmAgents in local or remote mode
#
# Usage:
#   stop_agents_v2.sh --mode local
#   stop_agents_v2.sh --mode remote --agent-hosts-file hosts.txt [--remote-repo-dir /root/SwarmAgents]

set -euo pipefail

MODE="local"
HOSTS_FILE=""
REMOTE_REPO_DIR="/root/SwarmAgents"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)           MODE="$2"; shift 2 ;;
        --agent-hosts-file) HOSTS_FILE="$2"; shift 2 ;;
        --remote-repo-dir)  REMOTE_REPO_DIR="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

stop_local() {
    echo "[stop] Touching shutdown flag …"
    touch shutdown
    echo "[stop] Killing local agent processes …"
    pkill -f 'python3\.11 .*main\.py' 2>/dev/null || true
    pkill -f 'python3\.11 .*job_distributor\.py' 2>/dev/null || true
    echo "[stop] Local agents stopped."
}

stop_remote() {
    if [[ -z "$HOSTS_FILE" ]]; then
        echo "ERROR: --agent-hosts-file is required for remote mode" >&2
        exit 1
    fi
    if [[ ! -f "$HOSTS_FILE" ]]; then
        echo "ERROR: Hosts file not found: $HOSTS_FILE" >&2
        exit 1
    fi

    # Also stop locally (job_distributor often runs on the controller)
    stop_local

    while IFS= read -r host || [[ -n "$host" ]]; do
        host="$(echo "$host" | xargs)"  # trim whitespace
        [[ -z "$host" || "$host" == \#* ]] && continue
        echo "[stop] Stopping agents on $host …"
        ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o BatchMode=yes \
            -o ConnectTimeout=10 "$host" \
            "cd ${REMOTE_REPO_DIR} 2>/dev/null && touch shutdown; pkill -f 'python3\.11 .*main\.py' 2>/dev/null; true" \
            2>/dev/null || echo "[stop] WARN: could not reach $host"
    done < "$HOSTS_FILE"

    echo "[stop] Remote agents stopped."
}

case "$MODE" in
    local)  stop_local  ;;
    remote) stop_remote ;;
    *) echo "ERROR: --mode must be 'local' or 'remote'" >&2; exit 1 ;;
esac
