#!/usr/bin/env bash
# Run a command on any node in the plan, reusing the SSH transport that
# 00_check_access.sh worked out (direct, or via whichever bastion reaches it).
#
# This is the fablib-free replacement for `node.execute(...)` in the notebooks.
#
# Usage:
#   ./run_on.sh <node> <command...>
#
# Examples:
#   ./run_on.sh database 'sudo bash -c "cd /root/SwarmAgents && docker compose up -d redis"'
#   ./run_on.sh agent-7  'hostname; ip -br addr'
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

NODE="${1:?node name required (e.g. database, agent-3, monitor)}"
shift
[ "$#" -gt 0 ] || { echo "usage: $0 <node> <command...>" >&2; exit 2; }

nssh_in "$NODE" "$@"
