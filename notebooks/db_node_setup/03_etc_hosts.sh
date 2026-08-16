#!/usr/bin/env bash
# Step 3: append the SWARM hosts block to /etc/hosts on every node
# (notebook cell 19). Idempotent via BEGIN/END markers.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

banner "Step 3: /etc/hosts"

push_hosts() {
    local n="$1"
    nssh_in "$n" 'sudo bash -c "
if ! grep -q \"# SWARM-HOSTS-BEGIN\" /etc/hosts; then
  { echo \"# SWARM-HOSTS-BEGIN\"; cat; echo \"# SWARM-HOSTS-END\"; } >> /etc/hosts
else
  cat > /dev/null
fi
"' <"$PLAN_DIR/etc_hosts.txt"
}

run_nodes push_hosts $(all_nodes)
echo
echo "Hosts block ($(wc -l <"$PLAN_DIR/etc_hosts.txt" | tr -d ' ') entries):"
sed 's/^/  /' "$PLAN_DIR/etc_hosts.txt"
