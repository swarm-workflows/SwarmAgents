#!/bin/bash
set -euo pipefail

usage() {
    echo "Usage: $0 <num_agents> <topology> <job_cnt> <database> <jobs_per_proposal>"
    echo "  num_agents         Number of agents to start"
    echo "  topology           Topology type"
    echo "  job_cnt            Number of jobs"
    echo "  database           (Optional) Database host"
    echo "  jobs_per_proposal  (Optional) Jobs per proposal"
    exit 1
}

# Check minimum required arguments
if [[ $# -lt 3 ]]; then
    usage
fi

num_agents="$1"; shift
topology="$1"; shift
job_cnt="$1"; shift
database="${1:-}"
jobs_per_proposal="${2:-}"

base_index=0

echo "Starting $num_agents agents with:"
echo "  Topology: $topology"
echo "  Job count: $job_cnt"
[[ -n "$database" ]] && echo "  Database: $database"
[[ -n "$jobs_per_proposal" ]] && echo "  Jobs per proposal: $jobs_per_proposal"

# Clean up previous runs
pkill -f "main.py db-swarm-multi" || true
rm -f shutdown

# Generate configs
python3.13 generate_configs.py "$num_agents" "$jobs_per_proposal" ./config_swarm_multi.yml configs "$topology" "$database" "$job_cnt"

# Build cleanup command
cleanup_cmd="python3.13 cleanup.py --agents $num_agents"
[[ -n "${topic:-}" ]] && cleanup_cmd+=" --topic $topic"
[[ -n "${broker:-}" ]] && cleanup_cmd+=" --broker $broker"
[[ -n "$database" ]] && cleanup_cmd+=" --redis-host $database --cleanup-redis"

# Run cleanup
eval "$cleanup_cmd"

# Prepare agent run directory
rm -rf swarm-multi
mkdir -p swarm-multi

# Trap for cleanup on exit
trap 'pkill -f "main.py swarm-multi" || true' EXIT

# Launch agents
for i in $(seq 0 $((num_agents - 1))); do
    agent_index=$((base_index + i + 1))
    python3.13 main.py swarm-multi "$agent_index" "$topology" &
done
