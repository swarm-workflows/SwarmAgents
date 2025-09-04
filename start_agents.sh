#!/bin/bash
set -euo pipefail

usage() {
    echo "Usage: $0 <num_agents> <topology> <job_cnt> <database> <jobs_per_proposal> <agents_per_host>"
    echo "  num_agents         Number of agents to start"
    echo "  topology           Topology type"
    echo "  job_cnt            Number of jobs"
    echo "  database           Database host"
    echo "  jobs_per_proposal  Jobs per proposal"
    echo "  agents_per_host    Agents per host"
    exit 1
}

if [[ $# -lt 3 ]]; then
    usage
fi

num_agents="$1"; shift
topology="$1"; shift
job_cnt="$1"; shift
database="${1:-localhost}"
jobs_per_proposal="${2:-10}"
agents_per_host="${3:-1}"

rm -rf jobs configs

echo "  Topology: $topology"
echo "  Job count: $job_cnt"
[[ -n "$database" ]] && echo "  Database: $database"

# Compute number of agent hosts
num_hosts=$(( num_agents / agents_per_host ))

# Generate agent hosts file (one entry per host)
rm -f agent_hosts.txt
for ((i=1; i<=num_hosts; i++)); do
    echo "agent-$i" >> agent_hosts.txt
done
python3.11 generate_configs.py "$num_agents" "$jobs_per_proposal" ./config_swarm_multi.yml configs $topology $database $job_cnt --agent-hosts-file agent_hosts.txt

cleanup_cmd="python3.11 cleanup.py --agents $num_agents"
[[ -n "$database" ]] && cleanup_cmd+=" --redis-host $database --cleanup-redis"
eval "$cleanup_cmd"

# Transfer generated configs to each agent host
for ((i=1; i<=num_hosts; i++)); do
    agent_host="agent-$i"
    ssh "${agent_host}" "mkdir -p /root/SwarmAgents/configs"
    ssh "${agent_host}" "rm -rf /root/SwarmAgents/configs/*"

    for ((j=1; j<=agents_per_host; j++)); do
        idx=$(( (i-1)*agents_per_host + j ))   # global agent index
        if [[ $idx -le $num_agents ]]; then
            scp "configs/config_swarm_multi_${idx}.yml" \
                "${agent_host}:/root/SwarmAgents/configs/"
        fi
    done
done

# Start agents on each host using swarm-multi-start.sh with --use-config-dir
for ((i=1; i<=num_hosts; i++)); do
    agent_host="agent-$i"
    ssh "${agent_host}" "cd /root/SwarmAgents && nohup bash swarm-multi-start.sh $agents_per_host $topology $job_cnt $database $jobs_per_proposal --use-config-dir > agent_${i}_start.log 2>&1 &"
done