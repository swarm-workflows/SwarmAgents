#!/usr/bin/env bash
# Upload the db-node setup bundle to the database node via your working
# `ssh swarm` alias, then it's all driven from there.
#
# Run from anywhere AFTER gen_inventory.py has written db_node_setup/plan/.
#
# Usage:
#   ./upload_to_db.sh [ssh-host-alias]        # default: swarm
#
# Keys (needed on the db node to bootstrap SSH into the other VMs):
#   SLICE_KEY=/path/to/slice_key      overrides plan/upload.env
#   BASTION_KEY=/path/to/bastion_key  optional; enables bastion fallback for
#                                     nodes whose mgmt IP family the db node
#                                     cannot reach directly (e.g. IPv6-only)
set -euo pipefail

HOST="${1:-swarm}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"       # .../notebooks/db_node_setup
NBDIR="$(dirname "$HERE")"                                  # .../notebooks
REMOTE_DIR="swarm-setup"

[ -f "$HERE/plan/nodes.tsv" ] || { echo "ERROR: $HERE/plan/ missing -- run gen_inventory.py first"; exit 1; }

# Resolve key paths (env overrides plan/upload.env)
SLICE_KEY_PATH=""; BASTION_KEY_PATH=""
[ -f "$HERE/plan/upload.env" ] && . "$HERE/plan/upload.env"
SLICE_KEY="${SLICE_KEY:-$SLICE_KEY_PATH}"
BASTION_KEY="${BASTION_KEY:-$BASTION_KEY_PATH}"
[ -n "$SLICE_KEY" ] && [ -f "$SLICE_KEY" ] || { echo "ERROR: slice key not found ('$SLICE_KEY'); set SLICE_KEY=/path/to/key"; exit 1; }

echo "==> Bundling setup package"
BUNDLE="$(mktemp -t swarm-setup-XXXXXX).tgz"
tar -czf "$BUNDLE" \
    -C "$NBDIR" \
    db_node_setup \
    node_tools \
    push_swarmagents.sh \
    Prometheus_Grafana_Monitor/tools \
    Prometheus_Grafana_Monitor/build

echo "==> Uploading bundle to $HOST:~/$REMOTE_DIR"
ssh "$HOST" "mkdir -p ~/$REMOTE_DIR"
scp -q "$BUNDLE" "$HOST:$REMOTE_DIR/bundle.tgz"
ssh "$HOST" "cd ~/$REMOTE_DIR && tar -xzf bundle.tgz && rm -f bundle.tgz && chmod +x db_node_setup/*.sh node_tools/*.sh push_swarmagents.sh Prometheus_Grafana_Monitor/tools/*.sh"
rm -f "$BUNDLE"

echo "==> Uploading slice key"
ssh "$HOST" "mkdir -p ~/$REMOTE_DIR/db_node_setup/keys && chmod 700 ~/$REMOTE_DIR/db_node_setup/keys"
scp -q "$SLICE_KEY" "$HOST:$REMOTE_DIR/db_node_setup/keys/slice_key"
ssh "$HOST" "chmod 600 ~/$REMOTE_DIR/db_node_setup/keys/slice_key"

if [ -n "$BASTION_KEY" ] && [ -f "$BASTION_KEY" ]; then
    echo "==> Uploading bastion key (fallback transport)"
    scp -q "$BASTION_KEY" "$HOST:$REMOTE_DIR/db_node_setup/keys/bastion_key"
    ssh "$HOST" "chmod 600 ~/$REMOTE_DIR/db_node_setup/keys/bastion_key"
else
    echo "==> No bastion key uploaded (direct mgmt-IP SSH only)"
fi

cat <<EOF

Done. Now on the database node:

    ssh $HOST
    cd ~/$REMOTE_DIR/db_node_setup
    ./setup_all.sh              # or run the numbered steps one at a time

Tip: run it under tmux/screen -- the full setup takes a while.
EOF
