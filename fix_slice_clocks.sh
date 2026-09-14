#!/usr/bin/env bash
# Give an already-running slice a common clock. Run from the database node as root.
#
# The slice-build pipeline does this as notebooks/db_node_setup/07_ntp.sh, over the
# bootstrap transport it needs before the root SSH mesh exists. This is the same change
# applied the other way round -- over the root mesh, on a slice that is already up -- which
# is the situation you are in when clocks drift mid-campaign, or when the slice was built
# before the step existed. Both read their chrony configuration from ntp_conf.sh.
#
# Why you would run it. Measured on the 92-node slice 2026-09-14: 66 nodes showed
# `Reach 0` / `LastRx 30d` against every configured NTP source -- a month of free-running --
# and were spread 0.4-1.1s apart. Two nodes on the SAME LAN disagreed by ~930ms. Nothing
# reports this by itself: `chronyc tracking` keeps printing the microsecond offset of its
# last sample, which is a month old. `timedatectl`'s "System clock synchronized" flag was
# the one indicator that identified the bad nodes exactly, so that is what is checked here.
#
# What it costs to ignore: anything comparing a timestamp from one node against another.
# SWARM's headline delegation context age is immune by construction (both terms are taken
# on the coordinator's own clock), but `ctx_age_remote_*` and every cross-node latency are
# biased by the full offset.
#
# Usage:  sudo ./fix_slice_clocks.sh [--check] [host ...]
#           --check   report synchronisation state and change nothing
#           host ...  default: every agent-N in agent_hosts.txt, else agent-1..agent-92
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$HERE/notebooks/db_node_setup/ntp_conf.sh"

CHECK_ONLY=0
[ "${1:-}" = "--check" ] && { CHECK_ONLY=1; shift; }

if [ "$#" -gt 0 ]; then
    HOSTS=("$@")
elif [ -f "$HERE/agent_hosts.txt" ]; then
    mapfile -t HOSTS < "$HERE/agent_hosts.txt"
else
    mapfile -t HOSTS < <(seq 1 92 | sed 's/^/agent-/')
fi

SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)
PARALLEL="${PARALLEL:-20}"

# `pgrep -f` and friends over ssh match the ssh command's own argv on the remote side, so
# every probe here reads a value out of a command rather than testing for a string.
sync_state() {
    "${SSH[@]}" "$1" \
        "timedatectl 2>/dev/null | awk -F': *' '/System clock synchronized/{print \$2}'" \
        2>/dev/null || echo unreachable
}

report() {
    local bad=0 total=0 h state
    for h in "${HOSTS[@]}"; do
        [ -n "$h" ] || continue
        total=$((total + 1))
        state="$(sync_state "$h")"
        [ "$state" = "yes" ] || { echo "  [UNSYNCED] $h (${state:-unknown})"; bad=$((bad + 1)); }
    done
    echo "$((total - bad))/$total node(s) synchronised."
    return $((bad > 0))
}

if [ "$CHECK_ONLY" = 1 ]; then
    report
    exit $?
fi

echo "== database node: serving time to the slice =="
printf '%s\n' "$NTP_DB_BODY" > "$NTP_DB_CONF"
systemctl restart chrony
sleep 2

echo "== agents: taking time from the database node =="
# Per-host outcome is written to a file, not returned. These run as background jobs and a
# background job's exit status is discarded by `wait`, so a host that could not be
# configured would otherwise scroll past as one line among ninety-two and the script would
# still exit 0. The clock repair reporting success while leaving nodes unconfigured is the
# failure this tool exists to prevent, so it has to be counted.
STATUS_DIR="$(mktemp -d)"
trap 'rm -rf "$STATUS_DIR"' EXIT

apply_one() {
    local h="$1"
    if printf '%s\n' "$NTP_AGENT_BODY" |
        "${SSH[@]}" "$h" "sudo tee $NTP_AGENT_CONF >/dev/null && sudo systemctl restart chrony" \
        >/dev/null 2>&1
    then
        sleep 5
        "${SSH[@]}" "$h" "sudo chronyc makestep" >/dev/null 2>&1 || true
        echo ok > "$STATUS_DIR/$h"
    else
        echo fail > "$STATUS_DIR/$h"
    fi
}

# A background-job loop rather than `xargs` + `export -f`: SSH is an array, and arrays do
# not survive export into a subshell -- apply_one would silently run with no ssh options.
count=0
for h in "${HOSTS[@]}"; do
    [ -n "$h" ] || continue
    apply_one "$h" &
    count=$((count + 1))
    [ $((count % PARALLEL)) -eq 0 ] && wait
done
wait

applied=0; failed=0
for h in "${HOSTS[@]}"; do
    [ -n "$h" ] || continue
    if [ "$(cat "$STATUS_DIR/$h" 2>/dev/null)" = "ok" ]; then
        applied=$((applied + 1))
    else
        echo "  [NOT CONFIGURED] $h"
        failed=$((failed + 1))
    fi
done
echo "Configured $applied host(s); $failed could not be reached or refused the change."

# chrony needs a poll to select the new source before the flag flips.
echo "== waiting for sources to be selected =="
sleep 15
synced=0
report || synced=1

if [ "$failed" -gt 0 ] || [ "$synced" -ne 0 ]; then
    echo
    echo "Clock repair INCOMPLETE. Nodes left unsynchronised are the ones whose cross-node"
    echo "timestamps stay wrong -- rerun this script, and if a node persists check that it"
    echo "can reach the database node on UDP 123:"
    echo "    chronyc sources   # expect '^* database' or '^+ database', not '^?'"
    exit 1
fi
echo "All nodes synchronised."
