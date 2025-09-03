#!/bin/bash
set -euo pipefail

usage() {
    echo "Usage: $0 <num_agents> <topology> <job_cnt> <database> <jobs_per_proposal> [--use-config-dir]"
    echo "  num_agents         Number of agents to start"
    echo "  topology           Topology type"
    echo "  job_cnt            Number of jobs"
    echo "  database           (Optional) Database host"
    echo "  jobs_per_proposal  (Optional) Jobs per proposal"
    echo "  --use-config-dir   (Optional) Use configs in ./configs directory"
    exit 1
}

use_config_dir=false

for arg in "$@"; do
    if [[ "$arg" == "--use-config-dir" ]]; then
        use_config_dir=true
        set -- "${@/--use-config-dir/}"
        break
    fi
done

if [[ $# -lt 3 ]]; then
    usage
fi

num_agents="$1"; shift
topology="$1"; shift
job_cnt="$1"; shift
database="${1:-localhost}"
jobs_per_proposal="${2:-10}"

base_index=0

echo "Starting $num_agents agents with:"
echo "  Topology: $topology"
echo "  Job count: $job_cnt"
[[ -n "$database" ]] && echo "  Database: $database"
echo "  Use config dir: $use_config_dir"

pkill -f "main.py swarm-multi" || true
rm -f shutdown

if [[ "$use_config_dir" == false ]]; then
    python3.11 generate_configs.py "$num_agents" "$jobs_per_proposal" ./config_swarm_multi.yml configs $topology $database $job_cnt

    cleanup_cmd="python3.11 cleanup.py --agents $num_agents"
    [[ -n "$database" ]] && cleanup_cmd+=" --redis-host $database --cleanup-redis"
    eval "$cleanup_cmd"
fi

rm -rf swarm-multi
mkdir -p swarm-multi

if [[ "$use_config_dir" == true ]]; then
    config_files=(configs/config_swarm_multi_*.yml)
    for config_file in "${config_files[@]}"; do
        agent_index=$(basename "$config_file" | sed 's/config_swarm_multi_\([0-9]*\)\.yml/\1/')
        python3.11 main.py swarm-multi "$agent_index" "$topology" &
    done
else
    for i in $(seq 0 $(($num_agents - 1))); do
        agent_index=$(($base_index + $i + 1))
        python3.11 main.py swarm-multi "$agent_index" $topology &
    done
fi