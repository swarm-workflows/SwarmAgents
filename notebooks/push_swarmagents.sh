#!/usr/bin/env bash
# push_and_extract_swarmagents.sh
# Usage: sudo ./push_and_extract_swarmagents.sh <COUNT> [ssh_user] [ssh_key] [branch]
# Example: sudo ./push_and_extract_swarmagents.sh 30 root ~/.ssh/id_rsa 15-resilience-and-perf-improvements

set -euo pipefail

COUNT="${1:?COUNT (number of agents) is required}"
SSH_USER="${2:-root}"
SSH_KEY="${3:-$HOME/.ssh/id_rsa}"
BRANCH="${4:-main}"

REPO_URL="https://github.com/swarm-workflows/SwarmAgents.git"
CLONE_DIR="/root/SwarmAgents"
ARCHIVE_NAME="SwarmAgents.tgz"
SHA_FILE="${ARCHIVE_NAME}.sha256"

log(){ printf '[%(%H:%M:%S)T] %s\n' -1 "$*"; }

# --- Fresh clone locally ---
if [ -d "$CLONE_DIR" ]; then
  log "Removing existing $CLONE_DIR"
  rm -rf "$CLONE_DIR"
fi

log "Cloning $REPO_URL (branch: $BRANCH) to $CLONE_DIR"
sudo git clone -b "$BRANCH" --single-branch "$REPO_URL" "$CLONE_DIR"

# --- Create archive + checksum ---
cd /root
log "Creating archive $ARCHIVE_NAME"
tar -zcf "$ARCHIVE_NAME" "$(basename "$CLONE_DIR")"/
sha256sum "$ARCHIVE_NAME" | tee "$SHA_FILE" >/dev/null

# --- Copy, verify, backup old, extract on each agent (parallel) ---
log "Transferring and extracting on agent-1..agent-$COUNT"
for i in $(seq 1 "$COUNT"); do
  H="agent-$i"
  {
    # Copy files
    scp -i "$SSH_KEY" -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
      "/root/$ARCHIVE_NAME" "$SSH_USER@$H:/root/$ARCHIVE_NAME"
    scp -i "$SSH_KEY" -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
      "/root/$SHA_FILE" "$SSH_USER@$H:/root/$SHA_FILE"

    # Verify checksum, backup old dir (if exists), extract
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "$SSH_USER@$H" bash -s <<'REMOTE_CMDS'
set -euo pipefail
cd /root

# Verify checksum
sha256sum -c SwarmAgents.tgz.sha256

# Backup any existing directory
if [ -d "SwarmAgents" ]; then
  ts="$(date +%Y%m%d-%H%M%S)"
  mv SwarmAgents "SwarmAgents.bak.$ts"
fi

# Extract
tar -xzf SwarmAgents.tgz

# Optional: show brief result
test -f SwarmAgents/README.md && echo "Extracted OK; README present." || echo "Extracted OK."
REMOTE_CMDS

    echo "$H: DONE"
  } &
done
wait

log "All agents updated and extracted."
