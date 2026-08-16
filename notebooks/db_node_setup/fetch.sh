#!/usr/bin/env bash
# Download a file from a node to the machine running this script, reusing the
# transport cached by 00_check_access.sh.
#
# This is the fablib-free replacement for `node.download_file(...)`.
#
# Usage:
#   ./fetch.sh <node> <remote-path> [local-path]
#
# Example:
#   ./fetch.sh database /tmp/run-h-30-100.tgz ./run-h-30-100.tgz
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

NODE="${1:?node name required}"
REMOTE="${2:?remote path required}"
LOCAL="${3:-$(basename "$2")}"

# `cat` over ssh rather than scp: scp needs its own bastion plumbing and
# stumbles on IPv6 literals, while this reuses the transport we already proved.
nssh "$NODE" "cat '$REMOTE'" >"$LOCAL"
echo "Fetched $NODE:$REMOTE -> $LOCAL ($(wc -c <"$LOCAL" | tr -d ' ') bytes)"
