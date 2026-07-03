#!/bin/bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BASE_DIR"
source .venv/bin/activate

DB_HOST="localhost"
CONFIG_DIR="configs"
OUT_DIR="${BASE_DIR}/runs/dtn-local"
SHUTDOWN_SECONDS=3600

stop_agents() {
    echo "  Stopping local agents..."
    pkill -f "main.py" 2>/dev/null || true
    sleep 3
}

flush_redis() {
    echo "  Flushing Redis..."
    python3 -c "import redis; redis.StrictRedis(host='${DB_HOST}', port=6379).flushall()"
}

run_one() {
    local run_dir="$1" label="$2"
    echo ""
    echo "--- ${label} -> ${run_dir} ---"
    echo "  $(date)"
    mkdir -p "${run_dir}/logs"
    flush_redis
    stop_agents
    local cmd="python3 run_test.py --mode local --agent-type resource"
    cmd+=" ${TEST_ARGS}"
    cmd+=" --db-host ${DB_HOST}"
    cmd+=" --jobs-per-interval 20 --jobs-per-proposal 10 --runtime 30 --job-interval 0.5"
    cmd+=" --grace-seconds 60 --shutdown-after-seconds ${SHUTDOWN_SECONDS} --fit-all"
    cmd+=" --run-dir ${run_dir} --log-dir ${run_dir}/logs --config-dir ${CONFIG_DIR}"
    cmd+=" --use-config-dir"
    echo "  CMD: ${cmd}"
    eval "${cmd}" 2>&1 | tee "${run_dir}/logs/run_test.stdout.log"
    echo "--- ${label} COMPLETE at $(date) ---"
    sleep 5
}

generate_configs() {
    local topo="$1" agents="$2" jobs="$3" dtn_flag="$4"
    local dtn_args=""
    local job_dtn_args=""
    local extra_args=""
    if [[ "${dtn_flag}" == "true" ]]; then
        dtn_args="--dtns"
        job_dtn_args="--enable-dtns"
        echo "  Generating ${topo} configs WITH DTNs (${agents} agents, ${jobs} jobs)..."
    else
        echo "  Generating ${topo} configs WITHOUT DTNs (${agents} agents, ${jobs} jobs)..."
    fi
    if [[ "${topo}" == "hierarchical" ]]; then
        extra_args="--co-parents 2 --agent-type resource --hierarchical-level1-agent-type resource"
    fi
    python3 generate_configs.py \
        ${agents} 10 ./config_swarm_multi.yml "${CONFIG_DIR}" \
        ${topo} "${DB_HOST}" ${jobs} \
        ${dtn_args} --fit-all ${extra_args}

    rm -rf jobs
    python3 job_generator.py --job-count ${jobs} \
        --agent-profile-path agent_profiles.json --output-dir jobs \
        ${job_dtn_args} --fit-all
}

echo "=== DTN Local Tests ($(date)) ==="

# ============================================================
# Phase 1: Mesh-30 (500 jobs, 2 runs each)
# ============================================================
echo ""
echo "========== PHASE 1: Mesh-30 DTN-Unaware =========="
generate_configs "mesh" 30 500 "false"
TEST_ARGS="--agents 30 --topology mesh --jobs 500"
run_one "${OUT_DIR}/mesh30-no-dtn/run-1" "Mesh-30 No-DTN run 1"
run_one "${OUT_DIR}/mesh30-no-dtn/run-2" "Mesh-30 No-DTN run 2"

echo ""
echo "========== PHASE 2: Mesh-30 DTN-Aware =========="
generate_configs "mesh" 30 500 "true"
run_one "${OUT_DIR}/mesh30-dtn/run-1" "Mesh-30 DTN run 1"
run_one "${OUT_DIR}/mesh30-dtn/run-2" "Mesh-30 DTN run 2"

# ============================================================
# Phase 3: Hier-110 (2188 jobs, 2 runs each)
# ============================================================
echo ""
echo "========== PHASE 3: Hier-110 DTN-Unaware =========="
generate_configs "hierarchical" 110 2188 "false"
TEST_ARGS="--agents 110 --topology hierarchical --jobs 2188 --hierarchical-level1-agent-type resource --co-parents 2"
run_one "${OUT_DIR}/hier110-no-dtn/run-1" "Hier-110 No-DTN run 1"
run_one "${OUT_DIR}/hier110-no-dtn/run-2" "Hier-110 No-DTN run 2"

echo ""
echo "========== PHASE 4: Hier-110 DTN-Aware =========="
generate_configs "hierarchical" 110 2188 "true"
run_one "${OUT_DIR}/hier110-dtn/run-1" "Hier-110 DTN run 1"
run_one "${OUT_DIR}/hier110-dtn/run-2" "Hier-110 DTN run 2"

echo ""
echo "=== ALL DTN LOCAL TESTS COMPLETE at $(date) ==="
