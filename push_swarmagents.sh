#!/usr/bin/env bash
# push_swarmagents.sh
# Usage: sudo ./push_swarmagents.sh <COUNT> [ssh_user] [ssh_key] [branch]
# Example: sudo ./push_swarmagents.sh 30 root ~/.ssh/id_rsa 15-resilience-and-perf-improvements

set -euo pipefail

COUNT="${1:?COUNT (number of agents) is required}"
SSH_USER="${2:-root}"
SSH_KEY="${3:-$HOME/.ssh/id_rsa}"
BRANCH="${4:-15-resilience-and-perf-improvements}"

REPO_URL="https://github.com/swarm-workflows/SwarmAgents.git"
CLONE_DIR="/root/SwarmAgents"
ARCHIVE_NAME="SwarmAgents.tgz"

log(){ printf '[%(%H:%M:%S)T] %s\n' -1 "$*"; }

# --- Clone (fresh) ---
if [ -d "$CLONE_DIR" ]; then
  log "Removing existing $CLONE_DIR"
  rm -rf "$CLONE_DIR"
fi

log "Cloning $REPO_URL (branch: $BRANCH) to $CLONE_DIR"
sudo git clone -b "$BRANCH" --single-branch "$REPO_URL" "$CLONE_DIR"

# --- Tar it up ---
cd /root
log "Creating archive $ARCHIVE_NAME"
tar -zcvf "$ARCHIVE_NAME" "$(basename "$CLONE_DIR")"/ >/dev/null

# Optional: checksum for verification
SHA_FILE="${ARCHIVE_NAME}.sha256"
sha256sum "$ARCHIVE_NAME" | tee "$SHA_FILE" >/dev/null

# --- Copy to agents in parallel ---
log "Transferring to agent-1..agent-$COUNT"
for i in $(seq 1 "$COUNT"); do
  H="agent-$i"
  {
    scp -i "$SSH_KEY" -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
      "/root/$ARCHIVE_NAME" "$SSH_USER@$H:/root/$ARCHIVE_NAME"
    scp -i "$SSH_KEY" -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
      "/root/$SHA_FILE" "$SSH_USER@$H:/root/$SHA_FILE"

    # (Optional) verify on remote
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
      "$SSH_USER@$H" "cd /root && sha256sum -c $SHA_FILE && echo '$H: OK'"
  } &
done
wait
log "All transfers complete."

# --- Optional: clean up local clone (uncomment if desired) ---
# rm -rf "$CLONE_DIR"
