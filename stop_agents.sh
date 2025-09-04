#!/bin/bash
set -euo pipefail

usage() {
    echo "Usage: $0 <agent_hosts_file>"
    exit 1
}

if [[ $# -lt 1 ]]; then
    usage
fi

agent_hosts_file="$1"

if [[ ! -f "$agent_hosts_file" ]]; then
    echo "Agent hosts file not found: $agent_hosts_file"
    exit 2
fi

while read -r agent_host; do
    if [[ -z "$agent_host" ]]; then continue; fi

    echo "Stopping agents on $agent_host..."
    ssh "$agent_host" "pkill -f 'main.py swarm-multi' || true"

    echo "Tarring swarm-multi directory on $agent_host..."
    ssh "$agent_host" "tar czf /tmp/swarm-multi.tar.gz -C /root/SwarmAgents swarm-multi"

    echo "Transferring tarball from $agent_host..."
    scp "$agent_host:/tmp/swarm-multi.tar.gz" "./swarm-multi_${agent_host}.tar.gz"

    echo "Cleaning up tarball on $agent_host..."
    ssh "$agent_host" "rm -f /tmp/swarm-multi.tar.gz"
done < "$agent_hosts_file"

echo "Done."