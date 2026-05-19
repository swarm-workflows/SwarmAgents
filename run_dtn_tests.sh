#!/bin/bash
# run_dtn_tests.sh — DTN-aware vs DTN-unaware scheduling evaluation
#
# Evaluates the impact of Data Transfer Node (DTN) connectivity-aware
# scheduling across both mesh and hierarchical topologies:
#   Phase 0: Mesh-30,  500 jobs, DTN-Unaware (5 runs)
#   Phase 1: Mesh-30,  500 jobs, DTN-Aware   (5 runs)
#   Phase 2: Hier-30,  500 jobs, DTN-Unaware (5 runs)
#   Phase 3: Hier-30,  500 jobs, DTN-Aware   (5 runs)
#   Phase 4: Hier-110, 2188 jobs, DTN-Unaware (5 runs)
#   Phase 5: Hier-110, 2188 jobs, DTN-Aware   (5 runs)
#
# NOTE: run_test.py currently skips --dtns for hierarchical topology.
#       This script works around that by calling generate_configs.py and
#       job_generator.py directly with --dtns for hierarchical phases.
#
# Usage:
#   nohup ./run_dtn_tests.sh > runs/dtn-evaluation.log 2>&1 &

set -euo pipefail

# ---- Configuration ----
MODE="remote"
RUNS=5
DB_HOST="database"
AGENTS_PER_HOST=4
AGENT_HOSTS_FILE="/root/agent_hosts_30.txt"
AGENT_HOSTS=$(paste -sd',' "$AGENT_HOSTS_FILE")
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_DIR="configs"
CO_PARENTS=2
SHUTDOWN_SECONDS=10800
OUT_DIR="${BASE_DIR}/runs/dtn-evaluation"

mkdir -p "${OUT_DIR}"

echo "=============================================="
echo "DTN Evaluation Tests"
echo "  Mode: ${MODE}"
echo "  Runs: ${RUNS} per scenario (6 scenarios)"
echo "  DB host: ${DB_HOST}"
echo "  Shutdown: ${SHUTDOWN_SECONDS}s"
echo "  Output: ${OUT_DIR}"
echo "=============================================="

# ---- Helper functions ----
build_cmd() {
    local run_dir="$1"
    local topology="$2"
    local agents="$3"
    local jobs="$4"
    local use_config_dir="$5"
    local extra_args="${6:-}"

    local cmd="python3.11 run_test.py"
    cmd+=" --mode ${MODE}"
    cmd+=" --agent-type resource"
    cmd+=" --agents ${agents}"
    cmd+=" --topology ${topology}"
    cmd+=" --jobs ${jobs}"
    cmd+=" --db-host ${DB_HOST}"
    cmd+=" --jobs-per-interval 20"
    cmd+=" --jobs-per-proposal 10"
    cmd+=" --runtime 30"
    cmd+=" --job-interval 0.5"
    cmd+=" --grace-seconds 60"
    cmd+=" --shutdown-after-seconds ${SHUTDOWN_SECONDS}"
    cmd+=" --fit-all"
    cmd+=" --run-dir ${run_dir}"
    cmd+=" --log-dir ${run_dir}/logs"
    cmd+=" --config-dir ${CONFIG_DIR}"
    cmd+=" --agents-per-host ${AGENTS_PER_HOST}"
    cmd+=" --starter ./swarm-multi-start.sh"
    cmd+=" --agent-hosts ${AGENT_HOSTS}"
    cmd+=" --remote-repo-dir ${BASE_DIR}"
    if [[ "${topology}" == "hierarchical" ]]; then
        cmd+=" --hierarchical-level1-agent-type resource"
        cmd+=" --co-parents ${CO_PARENTS}"
    fi
    if [[ "${use_config_dir}" == "true" ]]; then
        cmd+=" --use-config-dir"
    fi
    if [[ -n "${extra_args}" ]]; then
        cmd+=" ${extra_args}"
    fi
    echo "${cmd}"
}

stop_agents() {
    echo "  Stopping all agents..."
    ./stop_agents_v2.sh --mode remote --agent-hosts-file "${AGENT_HOSTS_FILE}" \
        --remote-repo-dir "${BASE_DIR}" > /dev/null 2>&1 || true
    sleep 5
    for host in $(cat "${AGENT_HOSTS_FILE}"); do
        ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 "${host}" \
            "pkill -9 -f 'main.py' 2>/dev/null" &
    done
    wait
    sleep 3
}

flush_redis() {
    echo "  Flushing Redis..."
    python3.11 -c "import redis; redis.StrictRedis(host='database', port=6379).flushall()"
}

collect_logs() {
    local run_dir="$1"
    echo "  Collecting agent logs..."
    mkdir -p "${run_dir}/agent_logs"
    for host in $(cat "${AGENT_HOSTS_FILE}"); do
        scp -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
            "${host}:/root/SwarmAgents/swarm-multi/agent-*.log" \
            "${run_dir}/agent_logs/" 2>/dev/null || \
        scp -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
            "${host}:/root/SwarmAgents/agent-*.log" \
            "${run_dir}/agent_logs/" 2>/dev/null || true
    done
}

run_test() {
    local run_dir="$1"
    local run_label="$2"
    local topology="$3"
    local agents="$4"
    local jobs="$5"
    local use_config_dir="$6"
    local extra_args="${7:-}"
    echo ""
    echo "--- ${run_label} -> ${run_dir} ---"
    mkdir -p "${run_dir}/logs"

    flush_redis
    stop_agents

    local cmd
    cmd=$(build_cmd "${run_dir}" "${topology}" "${agents}" "${jobs}" "${use_config_dir}" "${extra_args}")
    cd "${BASE_DIR}"
    echo "  CMD: ${cmd}"
    eval "${cmd}" 2>&1 | tee "${run_dir}/logs/run_test.stdout.log"

    collect_logs "${run_dir}"
    echo "--- ${run_label} COMPLETE ---"
    sleep 5
}

# For hierarchical DTN-aware runs, we need to manually generate configs with --dtns
# since run_test.py skips --dtns for hierarchical topology.
generate_hier_dtn_configs() {
    local agents="$1"
    local jobs="$2"
    local hosts_file="$3"
    echo "  Generating hierarchical configs WITH DTNs..."
    python3.11 generate_configs.py \
        "${agents}" 10 \
        ./config_swarm_multi.yml "${CONFIG_DIR}" \
        hierarchical "${DB_HOST}" "${jobs}" \
        --dtns \
        --fit-all \
        --agent-hosts-file "${hosts_file}" \
        --agents-per-host "${AGENTS_PER_HOST}" \
        --co-parents "${CO_PARENTS}" \
        --agent-type resource \
        --hierarchical-level1-agent-type resource

    # Regenerate jobs with DTN dependencies
    echo "  Generating jobs WITH DTN dependencies..."
    rm -rf jobs
    python3.11 job_generator.py \
        --job-count "${jobs}" \
        --agent-profile-path agent_profiles.json \
        --output-dir jobs \
        --enable-dtns \
        --fit-all
}

generate_hier_no_dtn_configs() {
    local agents="$1"
    local jobs="$2"
    local hosts_file="$3"
    echo "  Generating hierarchical configs WITHOUT DTNs..."
    python3.11 generate_configs.py \
        "${agents}" 10 \
        ./config_swarm_multi.yml "${CONFIG_DIR}" \
        hierarchical "${DB_HOST}" "${jobs}" \
        --fit-all \
        --agent-hosts-file "${hosts_file}" \
        --agents-per-host "${AGENTS_PER_HOST}" \
        --co-parents "${CO_PARENTS}" \
        --agent-type resource \
        --hierarchical-level1-agent-type resource

    # Regenerate jobs without DTN dependencies
    echo "  Generating jobs WITHOUT DTN dependencies..."
    rm -rf jobs
    python3.11 job_generator.py \
        --job-count "${jobs}" \
        --agent-profile-path agent_profiles.json \
        --output-dir jobs \
        --fit-all
}

# ============================================================
# PHASE 0: MESH-30, DTN-UNAWARE
# ============================================================
echo ""
echo "=============================================="
echo "Phase 0: Mesh-30, DTN-Unaware — ${RUNS} runs"
echo "=============================================="

for i in $(seq 1 ${RUNS}); do
    if [[ ${i} -eq 1 ]]; then
        run_test "${OUT_DIR}/mesh30-no-dtn/run-${i}" "Mesh-30 No-DTN run ${i}/${RUNS}" \
            "mesh" 30 500 "false"
    else
        run_test "${OUT_DIR}/mesh30-no-dtn/run-${i}" "Mesh-30 No-DTN run ${i}/${RUNS}" \
            "mesh" 30 500 "true"
    fi
done

# ============================================================
# PHASE 1: MESH-30, DTN-AWARE
# ============================================================
echo ""
echo "=============================================="
echo "Phase 1: Mesh-30, DTN-Aware — ${RUNS} runs"
echo "=============================================="

for i in $(seq 1 ${RUNS}); do
    if [[ ${i} -eq 1 ]]; then
        run_test "${OUT_DIR}/mesh30-dtn/run-${i}" "Mesh-30 DTN run ${i}/${RUNS}" \
            "mesh" 30 500 "false"
    else
        run_test "${OUT_DIR}/mesh30-dtn/run-${i}" "Mesh-30 DTN run ${i}/${RUNS}" \
            "mesh" 30 500 "true"
    fi
done

# ============================================================
# PHASE 2: HIER-30, DTN-UNAWARE
# ============================================================
echo ""
echo "=============================================="
echo "Phase 2: Hier-30, DTN-Unaware — ${RUNS} runs"
echo "=============================================="

# Generate configs without DTNs for first run
generate_hier_no_dtn_configs 30 500 "${AGENT_HOSTS_FILE}"

for i in $(seq 1 ${RUNS}); do
    run_test "${OUT_DIR}/hier30-no-dtn/run-${i}" "Hier-30 No-DTN run ${i}/${RUNS}" \
        "hierarchical" 30 500 "true"
done

# ============================================================
# PHASE 3: HIER-30, DTN-AWARE
# ============================================================
echo ""
echo "=============================================="
echo "Phase 3: Hier-30, DTN-Aware — ${RUNS} runs"
echo "=============================================="

# Generate configs WITH DTNs
generate_hier_dtn_configs 30 500 "${AGENT_HOSTS_FILE}"

for i in $(seq 1 ${RUNS}); do
    run_test "${OUT_DIR}/hier30-dtn/run-${i}" "Hier-30 DTN run ${i}/${RUNS}" \
        "hierarchical" 30 500 "true"
done

# ============================================================
# PHASE 4: HIER-110, DTN-UNAWARE
# ============================================================
echo ""
echo "=============================================="
echo "Phase 4: Hier-110, DTN-Unaware — ${RUNS} runs"
echo "=============================================="

generate_hier_no_dtn_configs 110 2188 "${AGENT_HOSTS_FILE}"

for i in $(seq 1 ${RUNS}); do
    run_test "${OUT_DIR}/hier110-no-dtn/run-${i}" "Hier-110 No-DTN run ${i}/${RUNS}" \
        "hierarchical" 110 2188 "true"
done

# ============================================================
# PHASE 5: HIER-110, DTN-AWARE
# ============================================================
echo ""
echo "=============================================="
echo "Phase 5: Hier-110, DTN-Aware — ${RUNS} runs"
echo "=============================================="

generate_hier_dtn_configs 110 2188 "${AGENT_HOSTS_FILE}"

for i in $(seq 1 ${RUNS}); do
    run_test "${OUT_DIR}/hier110-dtn/run-${i}" "Hier-110 DTN run ${i}/${RUNS}" \
        "hierarchical" 110 2188 "true"
done

# ============================================================
# SUMMARY
# ============================================================
echo ""
echo "=============================================="
echo "ALL DTN EVALUATION TESTS COMPLETE"
echo "=============================================="
echo "Results:"
echo "  Mesh-30  No-DTN: ${OUT_DIR}/mesh30-no-dtn/run-{1..${RUNS}}/"
echo "  Mesh-30  DTN:    ${OUT_DIR}/mesh30-dtn/run-{1..${RUNS}}/"
echo "  Hier-30  No-DTN: ${OUT_DIR}/hier30-no-dtn/run-{1..${RUNS}}/"
echo "  Hier-30  DTN:    ${OUT_DIR}/hier30-dtn/run-{1..${RUNS}}/"
echo "  Hier-110 No-DTN: ${OUT_DIR}/hier110-no-dtn/run-{1..${RUNS}}/"
echo "  Hier-110 DTN:    ${OUT_DIR}/hier110-dtn/run-{1..${RUNS}}/"
echo "=============================================="
