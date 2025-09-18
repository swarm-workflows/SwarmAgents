#!/bin/bash
set -euo pipefail

usage() {
    echo "Usage: $0 <num_agents> <topology> <job_cnt> <database> <jobs_per_proposal> [--use-config-dir] [--debug]"
    echo "  num_agents         Number of agents to start"
    echo "  topology           Topology type"
    echo "  job_cnt            Number of jobs"
    echo "  database           (Optional) Database host"
    echo "  jobs_per_proposal  (Optional) Jobs per proposal"
    echo "  --use-config-dir   (Optional) Use configs in ./configs directory"
    echo "  --debug            (Optional) Enable debug metrics"
    exit 1
}

use_config_dir=false
debug=false

# Parse flags
args=()
for arg in "$@"; do
    case "$arg" in
        --use-config-dir)
            use_config_dir=true
            ;;
        --debug)
            debug=true
            ;;
        *)
            args+=("$arg")
            ;;
    esac
done

if [[ $# -lt 3 ]]; then
    usage
fi

set -- "${args[@]}"

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
echo "  Debug: $debug"

pkill -f "main.py swarm-multi" || true
rm -f shutdown

if [[ "$use_config_dir" == false ]]; then
    rm -rf configs jobs
    python3.11 generate_configs.py "$num_agents" "$jobs_per_proposal" ./config_swarm_multi.yml configs $topology $database $job_cnt --dtns

    cleanup_cmd="python3.11 cleanup.py --agents $num_agents"
    [[ -n "$database" ]] && cleanup_cmd+=" --redis-host $database --cleanup-redis"
    eval "$cleanup_cmd"
fi

rm -rf swarm-multi
mkdir -p swarm-multi

# Construct debug flag
debug_flag=""
if [[ "$debug" == true ]]; then
    debug_flag="--debug"
fi

if [[ "$use_config_dir" == true ]]; then
    config_files=(configs/config_swarm_multi_*.yml)
    for config_file in "${config_files[@]}"; do
        agent_index=$(basename "$config_file" | sed 's/config_swarm_multi_\([0-9]*\)\.yml/\1/')
        python3.11 main.py "$agent_index" $debug_flag &
    done
else
    for i in $(seq 0 $(($num_agents - 1))); do
        agent_index=$(($base_index + $i + 1))
        python3.11 main.py "$agent_index" $debug_flag &
    done
fi
