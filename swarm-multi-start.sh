#!/bin/bash
set -e

# Required argument
num_agents=$1
shift
topology=$1
shift
job_cnt=$1
shift

if [[ -z "$num_agents" ]]; then
    echo "Usage: $0 <num_agents> <topology> <job_cnt> <database> <jobs_per_proposal>"
    exit 1
fi

# Optional arguments (only assigned if passed)
database="$1"
jobs_per_proposal=$2

base_index=0

echo "Starting $num_agents agents with:"
[[ -n "$database" ]] && echo "  Database: $database"

pkill -f "main.py db-swarm-multi" || true
rm -f shutdown

#python3.11 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. swarm/comm/consensus.proto

# Call generate_configs as-is
python3.11 generate_configs.py "$num_agents" "$jobs_per_proposal" ./config_swarm_multi.yml configs $topology $database $job_cnt

# Build cleanup command with optional args only if set
cleanup_cmd="python3.11 cleanup.py --agents $num_agents"
[[ -n "$topic" ]] && cleanup_cmd+=" --topic $topic"
[[ -n "$broker" ]] && cleanup_cmd+=" --broker $broker"
#[[ -n "$database" ]] && cleanup_cmd+=" --etcd-host $database --cleanup-etcd"
[[ -n "$database" ]] && cleanup_cmd+=" --redis-host $database --cleanup-redis"

# Run cleanup
eval "$cleanup_cmd"

# Prepare agent run directory
rm -rf swarm-multi
mkdir -p swarm-multi

# Launch agents
for i in $(seq 0 $(($num_agents - 1))); do
    agent_index=$(($base_index + $i + 1))
    python3.11 main.py swarm-multi "$agent_index" $topology &
done
