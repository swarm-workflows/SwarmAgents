#!/bin/bash
set -euo pipefail

usage() {
    cat <<EOF
Usage: $0 <agent_type> <num_agents> <topology> <job_cnt> <database> <jobs_per_proposal> [--use-config-dir] [--debug]
  agent_type         Agent kind to start: resource | llm
  num_agents         Number of agents to start
  topology           Topology type
  job_cnt            Number of jobs
  database           (Optional) Database host (default: localhost)
  jobs_per_proposal  (Optional) Jobs per proposal (default: 10)
  --use-config-dir   (Optional) Use configs in ./configs directory instead of regenerating
  --debug            (Optional) Enable debug metrics/logging
EOF
    exit 1
}

use_config_dir=false
debug=false

# Collect flags, keep positionals in args[]
args=()
for arg in "$@"; do
    case "$arg" in
        --use-config-dir) use_config_dir=true ;;
        --debug)          debug=true ;;
        *)                args+=("$arg") ;;
    esac
done

# Need at least: agent_type num_agents topology job_cnt
if [[ ${#args[@]} -lt 4 ]]; then
    usage
fi

set -- "${args[@]}"

agent_type="$1"; shift
num_agents="$1"; shift
topology="$1"; shift
job_cnt="$1"; shift
database="${1:-localhost}"
jobs_per_proposal="${2:-10}"

case "$agent_type" in
  resource|llm) ;;
  *) echo "Error: agent_type must be 'resource' or 'llm'"; usage ;;
esac

base_index=0

echo "Starting $num_agents '$agent_type' agents with:"
echo "  Topology: $topology"
echo "  Job count: $job_cnt"
[[ -n "${database:-}" ]] && echo "  Database: $database"
echo "  Jobs per proposal: $jobs_per_proposal"
echo "  Use config dir: $use_config_dir"
echo "  Debug: $debug"

# Clean previous run
pkill -f "python3\.11 .*main\.py" || true
rm -f shutdown

template_cfg="./config_swarm_multi.yml"
cfg_prefix="config_swarm_multi_"
cfg_glob="configs/${cfg_prefix}*.yml"

# Generate configs unless user asked to use existing
if [[ "$use_config_dir" == false ]]; then
    rm -rf configs jobs
    mkdir -p configs

    python3.11 generate_configs.py \
        "$num_agents" "$jobs_per_proposal" "$template_cfg" configs "$topology" "$database" "$job_cnt" --dtns

    cleanup_cmd=(python3.11 cleanup.py --agents "$num_agents")
    [[ -n "${database:-}" ]] && cleanup_cmd+=(--redis-host "$database" --cleanup-redis)
    "${cleanup_cmd[@]}"
fi

rm -rf swarm-multi
mkdir -p swarm-multi

# Always define arrays to keep set -u happy
declare -a agent_flag debug_flag
agent_flag=(--agent-type "$agent_type")
debug_flag=()
[[ "$debug" == true ]] && debug_flag=(--debug)

if [[ "$use_config_dir" == true ]]; then
    shopt -s nullglob
    config_files=($cfg_glob)
    if (( ${#config_files[@]} == 0 )); then
        echo "No config files found matching ${cfg_glob}. Did you generate them?" >&2
        exit 1
    fi
    for config_file in "${config_files[@]}"; do
        agent_index="$(basename "$config_file" | sed -E "s/${cfg_prefix}([0-9]+)\.yml/\1/")"
        python3.11 main.py "$agent_index" "${agent_flag[@]}" "${debug_flag[@]+"${debug_flag[@]}"}" &
    done
else
    for i in $(seq 0 $((num_agents - 1))); do
        agent_index=$((base_index + i + 1))
        python3.11 main.py "$agent_index" "${agent_flag[@]}" "${debug_flag[@]+"${debug_flag[@]}"}" &
    done
fi

echo "Launched $num_agents '$agent_type' agents."
