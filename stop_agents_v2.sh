#!/bin/bash
# stop_agents_v2.sh — stop SwarmAgents in local or remote mode
#
# Usage:
#   stop_agents_v2.sh --mode local
#   stop_agents_v2.sh --mode remote --agent-hosts-file hosts.txt [--remote-repo-dir /root/SwarmAgents]
#
# A stop is not done when the signal has been sent — it is done when the processes are gone.
# Agents flush their metrics to Redis during teardown, so returning early leaves the runner
# reading a half-written metrics set, and leaves agents alive to write into the NEXT run
# (see the smoke-g2-* post-mortem). So every kill here is followed by a bounded wait for the
# process to actually exit, and SIGKILL is the last resort rather than the first move.

set -euo pipefail

MODE="local"
HOSTS_FILE=""
REMOTE_REPO_DIR="/root/SwarmAgents"
# Long enough for on_shutdown's pre-drain metrics save plus a short executor drain
# (runtime.shutdown_drain_timeout_s, 20s by default), not long enough to stall a campaign.
DRAIN_TIMEOUT=45

AGENT_PATTERN='python3\.11 .*main\.py'
# GNU timeout bounds each remote stop. Absent (a stock macOS controller), the ssh keepalive
# options below are the only bound; do not fail the sweep over a missing wrapper.
if command -v timeout >/dev/null 2>&1; then TIMEOUT_CMD=timeout
elif command -v gtimeout >/dev/null 2>&1; then TIMEOUT_CMD=gtimeout
else TIMEOUT_CMD=""; fi
DISTRIBUTOR_PATTERN='python3\.11 .*job_distributor\.py'

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)           MODE="$2"; shift 2 ;;
        --agent-hosts-file) HOSTS_FILE="$2"; shift 2 ;;
        --remote-repo-dir)  REMOTE_REPO_DIR="$2"; shift 2 ;;
        --drain-timeout)    DRAIN_TIMEOUT="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Shell fragment run both locally and (over ssh) remotely: SIGTERM, wait for exit, then SIGKILL.
# Emits "[stop] <host>: ..." lines so a parallel remote sweep stays readable.
_stop_fragment() {
    cat <<'FRAG'
_wait_gone() {
    local pattern="$1" deadline="$2" waited=0
    while pgrep -f "$pattern" >/dev/null 2>&1; do
        if [[ "$waited" -ge "$deadline" ]]; then
            local left
            left="$(pgrep -f "$pattern" | tr '\n' ' ')"
            echo "[stop] $(hostname): WARN still alive after ${deadline}s, sending SIGKILL to: ${left}"
            pkill -9 -f "$pattern" 2>/dev/null || true
            sleep 2
            if pgrep -f "$pattern" >/dev/null 2>&1; then
                echo "[stop] $(hostname): ERROR processes survived SIGKILL: $(pgrep -f "$pattern" | tr '\n' ' ')"
                return 1
            fi
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done
    return 0
}
FRAG
}

stop_local() {
    echo "[stop] Touching shutdown flag …"
    touch shutdown
    echo "[stop] Killing local agent processes …"
    eval "$(_stop_fragment)"
    pkill -f "$DISTRIBUTOR_PATTERN" 2>/dev/null || true
    pkill -f "$AGENT_PATTERN" 2>/dev/null || true
    local rc=0
    _wait_gone "$AGENT_PATTERN" "$DRAIN_TIMEOUT" || rc=1
    _wait_gone "$DISTRIBUTOR_PATTERN" 10 || rc=1
    if [[ "$rc" -eq 0 ]]; then
        echo "[stop] Local agents stopped and confirmed gone."
    else
        echo "[stop] ERROR: local agent processes could not be stopped." >&2
    fi
    return "$rc"
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
    local local_rc=0
    stop_local || local_rc=1

    local remote_cmd
    remote_cmd="$(_stop_fragment)
cd ${REMOTE_REPO_DIR} 2>/dev/null && touch shutdown
pkill -f '${AGENT_PATTERN}' 2>/dev/null || true
_wait_gone '${AGENT_PATTERN}' ${DRAIN_TIMEOUT}"

    # Hosts are sequential over ssh but the waits are not: a 45s drain times 92 hosts is 69
    # minutes of teardown, which is why this fans out instead of looping.
    local status_dir
    status_dir="$(mktemp -d)"
    local hosts=()
    while IFS= read -r host || [[ -n "$host" ]]; do
        host="$(echo "$host" | xargs)"  # trim whitespace
        [[ -z "$host" || "$host" == \#* ]] && continue
        hosts+=("$host")
    done < "$HOSTS_FILE"

    if [[ "${#hosts[@]}" -eq 0 ]]; then
        # An empty hosts file would otherwise sweep nothing and report success, which is the
        # silent no-op this script exists to stop being.
        echo "ERROR: hosts file ${HOSTS_FILE} lists no hosts; no agents were stopped" >&2
        rm -rf "$status_dir"
        return 1
    fi

    echo "[stop] Stopping agents on ${#hosts[@]} host(s), waiting up to ${DRAIN_TIMEOUT}s each …"
    for host in "${hosts[@]}"; do
        (
            # ConnectTimeout alone only bounds the handshake: a connection that stalls after
            # it is established would block the `wait` below forever, hanging a whole campaign
            # on one sick host. ServerAlive* bounds a dead peer, and `timeout` bounds
            # everything else (the drain wait plus slack).
            if ${TIMEOUT_CMD:+$TIMEOUT_CMD $(( DRAIN_TIMEOUT + 30 ))} \
               ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o BatchMode=yes \
                   -o ConnectTimeout=10 -o ServerAliveInterval=5 -o ServerAliveCountMax=3 \
                   "$host" "bash -s" <<< "$remote_cmd" 2>&1; then
                echo ok > "${status_dir}/${host}"
            else
                echo "[stop] WARN: could not confirm agents stopped on ${host}"
                echo fail > "${status_dir}/${host}"
            fi
        ) &
    done
    wait

    local failed=()
    for host in "${hosts[@]}"; do
        if [[ "$(cat "${status_dir}/${host}" 2>/dev/null || echo fail)" != "ok" ]]; then
            failed+=("$host")
        fi
    done
    rm -rf "$status_dir"

    if [[ "${#failed[@]}" -gt 0 ]]; then
        echo "[stop] ERROR: ${#failed[@]} host(s) did not confirm a clean stop: ${failed[*]}" >&2
        echo "[stop] Their agents may still be running and may write metrics into the next run." >&2
        return 1
    fi
    echo "[stop] Remote agents stopped and confirmed gone on all ${#hosts[@]} host(s)."
    return "$local_rc"
}

case "$MODE" in
    local)  stop_local  ;;
    remote) stop_remote ;;
    *) echo "ERROR: --mode must be 'local' or 'remote'" >&2; exit 1 ;;
esac
