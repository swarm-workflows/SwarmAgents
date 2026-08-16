#!/usr/bin/env bash
# Step 2: root SSH mesh (notebook cells 17-18).
# Generates a root keypair on every node, collects the public keys, and
# appends the full set to every node's /root/.ssh/authorized_keys (deduped).
# After this, root@<node> works between all nodes -- push_swarmagents.sh and
# run_test.py --mode remote rely on it.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

banner "Step 2: root SSH mesh"
mkdir -p "$STATE_DIR/pubkeys"

gen_and_collect_key() {
    local n="$1"
    nssh "$n" 'sudo bash -c "test -f /root/.ssh/id_rsa || ssh-keygen -t rsa -N \"\" -f /root/.ssh/id_rsa"' \
        && nssh "$n" "sudo cat /root/.ssh/id_rsa.pub" >"$STATE_DIR/pubkeys/$n.pub" \
        && [ -s "$STATE_DIR/pubkeys/$n.pub" ]
}

run_nodes gen_and_collect_key $(all_nodes)

cat "$STATE_DIR"/pubkeys/*.pub | sort -u >"$STATE_DIR/all_root_keys"
echo "Collected $(wc -l <"$STATE_DIR/all_root_keys" | tr -d ' ') root public keys"

push_keys() {
    local n="$1"
    nssh_in "$n" 'sudo bash -c "
set -e
mkdir -p /root/.ssh
touch /root/.ssh/authorized_keys
cat > /root/.ssh/authorized_keys.__tmp
cat /root/.ssh/authorized_keys /root/.ssh/authorized_keys.__tmp | sort -u > /root/.ssh/authorized_keys.__new
mv /root/.ssh/authorized_keys.__new /root/.ssh/authorized_keys
rm -f /root/.ssh/authorized_keys.__tmp
chmod 700 /root/.ssh
chmod 600 /root/.ssh/authorized_keys
"' <"$STATE_DIR/all_root_keys"
}

run_nodes push_keys $(all_nodes)
