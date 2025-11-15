#!/bin/bash
set -euo pipefail

usage() {
    cat <<EOF
Usage: $0 <agent_type> <num_agents> <topology> <job_cnt> [database] [jobs_per_proposal] [--use-config-dir] [--debug] [--groups N] [--group-size M]
  agent_type         Agent kind to start: resource | llm
  num_agents         Number of agents to start
  topology           Topology type (mesh | ring | star | hierarchical)
  job_cnt            Number of jobs
  database           (Optional) Database host (default: localhost)
  jobs_per_proposal  (Optional) Jobs per proposal (default: 10)

  --use-config-dir   (Optional) Use configs in ./configs directory instead of regenerating
  --debug            (Optional) Enable debug metrics/logging

  # NEW (propagated to generate_configs.py for mesh/ring):
  --groups N         Number of independent groups
  --group-size M     Size of each group
EOF
    exit 1
}

use_config_dir=false
debug=false
groups=""
group_size=""

# Collect flags & positionals
pos=()
while (( "$#" )); do
  case "$1" in
    --use-config-dir) use_config_dir=true; shift ;;
    --debug)          debug=true; shift ;;
    --groups)
        [[ $# -lt 2 ]] && { echo "Error: --groups requires a value"; usage; }
        groups="$2"; shift 2 ;;
    --groups=*)
        groups="${1#*=}"; shift ;;
    --group-size)
        [[ $# -lt 2 ]] && { echo "Error: --group-size requires a value"; usage; }
        group_size="$2"; shift 2 ;;
    --group-size=*)
        group_size="${1#*=}"; shift ;;
    -h|--help) usage ;;
    *)
        pos+=("$1"); shift ;;
  esac
done

# Need at least: agent_type num_agents topology job_cnt
if [[ ${#pos[@]} -lt 4 ]]; then
    usage
fi

agent_type="${pos[0]}"
num_agents="${pos[1]}"
topology="${pos[2]}"
job_cnt="${pos[3]}"
database="${pos[4]:-localhost}"
jobs_per_proposal="${pos[5]:-10}"

case "$agent_type" in
  resource|llm) ;;
  *) echo "Error: agent_type must be 'resource' or 'llm'"; usage ;;
esac

base_index=0

echo "Starting $num_agents '$agent_type' agents with:"
echo "  Topology: $topology"
echo "  Job count: $job_cnt"
echo "  Database: $database"
echo "  Jobs per proposal: $jobs_per_proposal"
echo "  Use config dir: $use_config_dir"
echo "  Debug: $debug"
[[ -n "$groups" ]] && echo "  Groups: $groups"
[[ -n "$group_size" ]] && echo "  Group size: $group_size"

# Clean previous run
pkill -f "python3\.11 .*main\.py" || true
rm -f shutdown

template_cfg="./config_swarm_multi.yml"
cfg_prefix="config_swarm_multi_"
cfg_glob="configs/${cfg_prefix}*.yml"

# Generate configs unless user asked to use existing
if [[ "$use_config_dir" == false ]]; then
    echo "Generating configs"
    rm -rf configs jobs
    mkdir -p configs

    # build args for generate_configs.py
    if [[ "$topology" == "hierarchical" ]]; then
      gen_args=(
        "$num_agents" "$jobs_per_proposal" "$template_cfg" configs
        "$topology" "$database" "$job_cnt"
      )
    else
       gen_args=(
        "$num_agents" "$jobs_per_proposal" "$template_cfg" configs
        "$topology" "$database" "$job_cnt" --dtns
      )
    fi
    # propagate grouping flags if set
    [[ -n "$groups" ]] && gen_args+=(--groups "$groups")
    [[ -n "$group_size" ]] && gen_args+=(--group-size "$group_size")

    echo "${gen_args[@]}"
    python3.11 generate_configs.py "${gen_args[@]}"

    cleanup_cmd=(python3.11 cleanup.py --agents "$num_agents")
    [[ -n "${database:-}" ]] && cleanup_cmd+=(--redis-host "$database" --cleanup-redis)
    "${cleanup_cmd[@]}"
fi

rm -rf swarm-multi
mkdir -p swarm-multi

# Always define arrays to keep set -u happy
declare -a debug_flag
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

        # Extract agent_type from config file (default to resource if not specified)
        agent_type_from_config=$(python3.11 -c "import yaml; c=yaml.safe_load(open('$config_file')); print(c.get('agent_type', 'resource'))" 2>/dev/null || echo "resource")

        python3.11 main.py "$agent_index" --agent-type "$agent_type_from_config" "${debug_flag[@]+"${debug_flag[@]}"}" &
    done
else
    # When not using config dir, use the global agent_type parameter
    declare -a agent_flag
    agent_flag=(--agent-type "$agent_type")
    for i in $(seq 0 $((num_agents - 1))); do
        agent_index=$((base_index + i + 1))
        python3.11 main.py "$agent_index" "${agent_flag[@]}" "${debug_flag[@]+"${debug_flag[@]}"}" &
    done
fi

echo "Launched $num_agents '$agent_type' agents."
