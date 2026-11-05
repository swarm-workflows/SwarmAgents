#!/usr/bin/env bash
# Step 4: clone SwarmAgents on the db node and push it to every agent
# (notebook cell 20). Uses the existing push_swarmagents.sh over the root
# SSH mesh + /etc/hosts names set up in steps 2-3.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

banner "Step 4: push SwarmAgents (branch: $BRANCH) to $AGENT_COUNT agents"

# Always executed ON the database node, whether this script is driven from
# the db node itself or from a laptop -- push_swarmagents.sh clones into
# /root and fans out to agent-N over the root mesh built in step 2.
npush "$ROOT_DIR/push_swarmagents.sh" "$DB_NODE" "push_swarmagents.sh"
nssh "$DB_NODE" "chmod +x push_swarmagents.sh && sudo cp push_swarmagents.sh /root/push_swarmagents.sh"

nssh "$DB_NODE" "sudo bash -c 'cd /root && ./push_swarmagents.sh $AGENT_COUNT root /root/.ssh/id_rsa $BRANCH'" \
    2>&1 | tee "$LOG_DIR/push_swarmagents.log"
