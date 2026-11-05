#!/usr/bin/env bash
# Step 7: give the fleet a common clock.
#
# Why this step exists. Every VM ships with chrony pointed at public NTP pools, and on this
# testbed most sites cannot reach them: measured 2026-09-14, 66 of 92 nodes showed
# `Reach 0` / `LastRx 30d` on every source -- they had not heard from a time server in a
# month and had been free-running ever since. The damage is invisible unless you look for
# it, because `chronyc tracking` keeps reporting the microsecond offset of its LAST sample,
# which is a month old, and that is what makes this worth a script rather than a note:
#
#     agent-75  +1102 ms        agent-10    -6 ms
#     agent-1    +430 ms        agent-70   -51 ms
#     agent-5    -494 ms        agent-46  -177 ms
#
# agent-1 and agent-3 share a LAN and disagreed by ~930 ms. `timedatectl`'s
# "System clock synchronized: no" identifies the bad nodes exactly, so that is what this
# script verifies against -- not chrony's own tracking line, which lies here.
#
# Anything that compares a timestamp taken on one node with one taken on another is wrong
# by that much: the delegation context age (P0-4), inter-node message latencies, and any
# claim about WAN timing. SWARM's own headline context age was made immune to this by
# measuring both terms on one clock, but the end-to-end (`ctx_age_remote_*`) series and
# every cross-node latency still need a synchronised fleet.
#
# The fix is to serve time from the database node over the slice network, which every agent
# can reach by construction. `local stratum 10` keeps it serving even if its own upstream
# goes away, so the fleet stays internally consistent -- which is what these measurements
# actually depend on -- even when it drifts from UTC.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

banner "Step 7: clock synchronisation"

. "$(dirname "${BASH_SOURCE[0]}")/ntp_conf.sh"

# --- database node: serve time to the slice ---------------------------------------------
echo "Configuring $DB_NODE as the slice time server"
printf '%s\n' "$NTP_DB_BODY" | nssh_in "$DB_NODE" \
    "sudo bash -c 'cat > $NTP_DB_CONF && systemctl restart chrony'"

# --- agents: take time from the database node -------------------------------------------
sync_node() {
    local n="$1"
    printf '%s\n' "$NTP_AGENT_BODY" | nssh_in "$n" \
        "sudo bash -c 'cat > $NTP_AGENT_CONF && systemctl restart chrony'" || return 1
    # iburst still needs a moment, and makestep only fires once a source is selected.
    sleep 5
    nssh "$n" "sudo chronyc makestep" >/dev/null 2>&1 || true
}

# `run_nodes` returns non-zero when any node failed, and `set -e` would abort here -- which
# is loud, but it would also skip the verification below, so the operator never learns WHICH
# nodes are still unsynchronised. Carry the failure to the end instead.
apply_failed=0
run_nodes sync_node $(agent_nodes) || apply_failed=1

# --- verify ------------------------------------------------------------------------------
# chronyc's own tracking output is not evidence here (see the header), so check the flag
# that was right the first time.
banner "Step 7: verification"
sleep 10
bad=0
for n in $(agent_nodes); do
    state="$(nssh "$n" "timedatectl 2>/dev/null | awk -F': *' '/System clock synchronized/{print \$2}'" 2>/dev/null || echo "?")"
    if [ "$state" != "yes" ]; then
        echo "  [UNSYNCED] $n (System clock synchronized: ${state:-unknown})"
        bad=$((bad + 1))
    fi
done
total=$(agent_nodes | wc -l | tr -d ' ')
if [ "$bad" -gt 0 ] || [ "$apply_failed" -ne 0 ]; then
    echo
    echo "WARNING: $bad/$total node(s) still unsynchronised."
    echo "  Re-run this step; chrony can need a second poll to select the source."
    echo "  If a node stays unsynced, check it can reach the database node on UDP 123:"
    echo "    chronyc sources   # expect '^* database' or '^+ database', not '^?'"
    exit 1
fi
echo "All $total agent node(s) synchronised to $DB_NODE."
