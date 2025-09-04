#!/bin/bash
set -u  # Don't exit on error

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

mapfile -t hosts < "$agent_hosts_file"

for agent_host in "${hosts[@]}"; do
    [[ -z "$agent_host" ]] && continue

    echo "Stopping agents on $agent_host..."
    ssh "$agent_host" "touch /root/SwarmAgents/shutdown"
    sleep 5
    ssh "$agent_host" "pkill -f 'main.py swarm-multi' || true" < /dev/null || echo "Warning: SSH to $agent_host failed"

    echo "Tarring swarm-multi directory on $agent_host..."
    ssh "$agent_host" "tar czf /tmp/swarm-multi.tar.gz -C /root/SwarmAgents swarm-multi" < /dev/null || echo "Warning: Tar failed on $agent_host"

    echo "Transferring tarball from $agent_host..."
    scp "$agent_host:/tmp/swarm-multi.tar.gz" "./swarm-multi_${agent_host}.tar.gz" < /dev/null || echo "Warning: SCP failed from $agent_host"

    echo "Cleaning up tarball on $agent_host..."
    ssh "$agent_host" "rm -f /tmp/swarm-multi.tar.gz" < /dev/null || echo "Warning: Cleanup failed on $agent_host"
done

echo "Done."