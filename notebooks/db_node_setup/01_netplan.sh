#!/usr/bin/env bash
# Step 1: upload node_tools and configure the dataplane NICs on every node.
# Replaces notebook cells 15-16: for each NIC row in plan/nics.tsv the
# interface is resolved BY MAC on the node itself, then
# setup-netplan-multihomed.sh applies the static IP + LAN route.
# Both NICs of a node run sequentially (netplan apply is not concurrency-safe
# on one host); nodes run in parallel. Idempotent -- safe to re-run.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

banner "Step 1: netplan dataplane configuration"

configure_node() {
    local n="$1"
    npush_dir "$ROOT_DIR/node_tools" "$n" || return 1
    nssh "$n" "chmod +x node_tools/*.sh" || return 1

    local name mac ipcidr gw lannet ifname
    while IFS=$'\t' read -r name mac ipcidr gw lannet; do
        ifname="$(nssh "$n" "ip -o link | tr 'A-Z' 'a-z' | awk -v m='$mac' '\$0 ~ m {gsub(\":\",\"\",\$2); print \$2; exit}'")" || return 1
        if [ -z "$ifname" ]; then
            echo "ERROR: no interface with MAC $mac on $n" >&2
            return 1
        fi
        echo "[$n] $ifname ($mac) -> $ipcidr via $gw (lan_net $lannet)"
        nssh "$n" "sudo node_tools/setup-netplan-multihomed.sh -i $ifname -a $ipcidr -n $lannet -g $gw" || return 1
    done < <(awk -F'\t' -v n="$n" '$1==n' "$PLAN_DIR/nics.tsv")
}

run_nodes configure_node $(all_nodes)

# Dataplane sanity check. This MUST run on the database node -- FABNetv4
# 10.x addresses are only reachable from inside the testbed, so pinging from
# a laptop would report false failures.
echo
echo "Verifying dataplane (pinging primary IPs from $DB_NODE)..."

awk -F'\t' -v mon="$MONITOR_NODE" '$5=="10.128.0.0/10" && $1!=mon {
    split($3, a, "/"); print $1, a[1]
}' "$PLAN_DIR/nics.tsv" >"$STATE_DIR/primary_ips.txt"

nssh_in "$DB_NODE" "cat > /tmp/primary_ips.txt" <"$STATE_DIR/primary_ips.txt"
nssh_in "$DB_NODE" 'bash -s' <<'REMOTE' | tee "$LOG_DIR/dataplane-ping.log"
fails=0
while read -r name ip; do
    if ping -c1 -W3 "$ip" >/dev/null 2>&1; then
        printf "  %-12s %-15s OK\n" "$name" "$ip"
    else
        printf "  %-12s %-15s NO RESPONSE\n" "$name" "$ip"
        fails=$((fails+1))
    fi
done < /tmp/primary_ips.txt
echo "UNREACHABLE_COUNT=$fails"
REMOTE

fails="$(awk -F= '/^UNREACHABLE_COUNT=/{print $2}' "$LOG_DIR/dataplane-ping.log")"
if [ "${fails:-1}" -eq 0 ]; then
    echo "Dataplane OK."
else
    echo "$fails primary IPs not answering."
    echo "FABNetv4 routes can take ~a minute to converge; re-run this step to recheck."
    exit 1
fi
