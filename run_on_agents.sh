#!/usr/bin/env bash
# run_on_agents.sh
# Run an arbitrary command on agent-1 .. agent-N in parallel.
#
# Usage:
#   ./run_on_agents.sh <COUNT> "<REMOTE_CMD>" [USER] [SSH_KEY] [HOST_PREFIX]
#
# Examples:
#   ./run_on_agents.sh 30 "hostname"
#   ./run_on_agents.sh 30 "sudo apt-get update && sudo apt-get -y install htop" root ~/.ssh/id_rsa
#   ./run_on_agents.sh 30 "sudo sed -i -e '/[[:space:]]databse$/d' -e '/[[:space:]]database$/d' /etc/hosts; echo '10.130.131.17 database' | sudo tee -a /etc/hosts >/dev/null"

set -euo pipefail

COUNT="${1:?COUNT is required, e.g. 30}"
REMOTE_CMD="${2:?REMOTE_CMD is required, wrap it in quotes}"
USER="${3:-ubuntu}"
KEY="${4:-$HOME/.ssh/id_rsa}"
PREFIX="${5:-agent-}"

# Escape the command so it survives SSH/bash quoting safely
ESCAPED_CMD="$(printf '%q' "$REMOTE_CMD")"

echo "Running on ${PREFIX}1..${PREFIX}${COUNT} as ${USER}"
for i in $(seq 1 "$COUNT"); do
  H="${PREFIX}${i}"
  {
    echo "==> $H"
    ssh -i "$KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        "${USER}@${H}" "bash -lc $ESCAPED_CMD"
    echo "<== $H DONE"
  } &
done

wait
echo "All hosts completed."
