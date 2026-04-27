#!/bin/bash
# run_coparent_failure_tests.sh — Batch co-parent failover experiments
# Runs N baseline + N failover tests for statistical significance
#
# Supports both local and remote modes:
#   ./run_coparent_failure_tests.sh --mode local  --runs 5 --db-host localhost
#   ./run_coparent_failure_tests.sh --mode remote --runs 5 --db-host database \
#       --agents-per-host 2 --agent-hosts "agent-1,agent-2,..." --hosts-file agent_hosts_15.txt
set -euo pipefail

# ---- defaults ----
MODE="local"
RUNS=5
AGENTS=30
DB_HOST="localhost"
AGENTS_PER_HOST=1
AGENT_HOSTS=""
HOSTS_FILE=""
KILL_DELAY=60
SHUTDOWN_SECONDS=300
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_DIR="configs"
JOBS_PER_INTERVAL=20
JOBS_PER_PROPOSAL=10

usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --mode MODE              local or remote (default: local)
  --runs N                 Number of runs per phase (default: 5)
  --agents N               Total agents (default: 30)
  --db-host HOST           Redis host (default: localhost)
  --agents-per-host N      Agents per host, remote mode only (default: 1)
  --agent-hosts CSV        Comma-separated host list, remote mode only
  --hosts-file FILE        Hosts file for stop script, remote mode only
  --config-dir DIR         Config directory (default: configs)
  --kill-delay N           Seconds before killing coordinator (default: 60)
  --shutdown-seconds N     Max seconds per run (default: 300)
  --jobs-per-interval N    Jobs fed per interval (default: 20)
  --jobs-per-proposal N    Jobs per consensus proposal (default: 10)
  -h, --help               Show this help
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)               MODE="$2"; shift 2 ;;
        --runs)               RUNS="$2"; shift 2 ;;
        --agents)             AGENTS="$2"; shift 2 ;;
        --db-host)            DB_HOST="$2"; shift 2 ;;
        --agents-per-host)    AGENTS_PER_HOST="$2"; shift 2 ;;
        --agent-hosts)        AGENT_HOSTS="$2"; shift 2 ;;
        --hosts-file)         HOSTS_FILE="$2"; shift 2 ;;
        --config-dir)         CONFIG_DIR="$2"; shift 2 ;;
        --kill-delay)         KILL_DELAY="$2"; shift 2 ;;
        --shutdown-seconds)   SHUTDOWN_SECONDS="$2"; shift 2 ;;
        --jobs-per-interval)  JOBS_PER_INTERVAL="$2"; shift 2 ;;
        --jobs-per-proposal)  JOBS_PER_PROPOSAL="$2"; shift 2 ;;
        -h|--help)            usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

OUT_DIR="${BASE_DIR}/runs/coparent-failure"
mkdir -p "${OUT_DIR}"

# ---- Determine Level-1 agent to kill ----
# In hierarchical topology with 30 agents: Level-1 = agents 26-30
# Agent 26 is co-parent for groups 0 and 4
KILL_AGENT_ID=26

echo "=============================================="
echo "Co-Parent Failure Tests"
echo "  Mode: ${MODE}"
echo "  Runs: ${RUNS} baseline + ${RUNS} failover"
echo "  Agents: ${AGENTS}"
echo "  DB host: ${DB_HOST}"
echo "  Config dir: ${CONFIG_DIR}"
echo "  Output dir: ${OUT_DIR}"
echo "  Kill target: agent ${KILL_AGENT_ID}"
echo "  Kill delay: ${KILL_DELAY}s"
echo "  Shutdown: ${SHUTDOWN_SECONDS}s"
if [[ "${MODE}" == "remote" ]]; then
    echo "  Agents/host: ${AGENTS_PER_HOST}"
    echo "  Hosts file: ${HOSTS_FILE}"
fi
echo "=============================================="

# ---- Build common run_test.py args ----
build_cmd() {
    local run_dir="$1"
    local cmd="python3.11 run_test.py"
    cmd+=" --mode ${MODE}"
    cmd+=" --agent-type resource"
    cmd+=" --agents ${AGENTS}"
    cmd+=" --topology hierarchical"
    cmd+=" --hierarchical-level1-agent-type resource"
    cmd+=" --jobs 500"
    cmd+=" --db-host ${DB_HOST}"
    cmd+=" --jobs-per-interval ${JOBS_PER_INTERVAL}"
    cmd+=" --jobs-per-proposal ${JOBS_PER_PROPOSAL}"
    cmd+=" --runtime 30"
    cmd+=" --job-interval 0.5"
    cmd+=" --grace-seconds 60"
    cmd+=" --shutdown-after-seconds ${SHUTDOWN_SECONDS}"
    cmd+=" --co-parents 2"
    cmd+=" --run-dir ${run_dir}"
    cmd+=" --log-dir ${run_dir}/logs"
    cmd+=" --config-dir ${CONFIG_DIR}"

    if [[ "${MODE}" == "remote" ]]; then
        cmd+=" --agents-per-host ${AGENTS_PER_HOST}"
        cmd+=" --starter ./swarm-multi-start.sh"
        cmd+=" --agent-hosts ${AGENT_HOSTS}"
        cmd+=" --remote-repo-dir ${BASE_DIR}"
    fi
    echo "${cmd}"
}

stop_agents() {
    if [[ "${MODE}" == "remote" && -n "${HOSTS_FILE}" ]]; then
        ./stop_agents_v2.sh --mode remote --agent-hosts-file "${HOSTS_FILE}" > /dev/null 2>&1 || true
    else
        ./stop_agents_v2.sh --mode local > /dev/null 2>&1 || true
    fi
}

kill_coordinator() {
    if [[ "${MODE}" == "local" ]]; then
        # Kill the local process for agent KILL_AGENT_ID
        # Process pattern: "python3.11 main.py 26" or "Python main.py 26"
        ps aux | grep "main.py ${KILL_AGENT_ID}" | grep -v grep | awk '{print $2}' | xargs kill -9 2>/dev/null || true
    else
        # Remote: figure out which host has agent KILL_AGENT_ID
        local host_idx=$(( (KILL_AGENT_ID - 1) / AGENTS_PER_HOST ))
        local kill_host
        kill_host=$(echo "${AGENT_HOSTS}" | tr ',' '\n' | sed -n "$((host_idx + 1))p")
        if [[ -n "${kill_host}" ]]; then
            echo "  Target host: ${kill_host}"
            ssh "${kill_host}" "ps aux | grep 'main.py ${KILL_AGENT_ID}' | grep -v grep | awk '{print \$2}' | xargs -r kill -9" 2>/dev/null || true
        else
            echo "  WARNING: Could not determine host for agent ${KILL_AGENT_ID}"
        fi
    fi
}

# ---- Baseline test ----
run_single_test() {
    local run_dir="$1"
    local run_label="$2"

    echo ""
    echo "--- Starting: ${run_label} -> ${run_dir} ---"

    # Flush Redis
    docker exec redis redis-cli flushall > /dev/null 2>&1

    # Stop any running agents
    cd "${BASE_DIR}"
    stop_agents
    sleep 3

    # Run the test
    local cmd
    cmd=$(build_cmd "${run_dir}")
    cd "${BASE_DIR}"
    eval "${cmd}" 2>&1

    echo "--- Completed: ${run_label} ---"
}

# ---- Failover test ----
run_failover_test() {
    local run_dir="$1"
    local run_label="$2"

    echo ""
    echo "--- Starting: ${run_label} -> ${run_dir} ---"
    mkdir -p "${run_dir}"

    # Flush Redis
    docker exec redis redis-cli flushall > /dev/null 2>&1

    # Stop any running agents
    cd "${BASE_DIR}"
    stop_agents
    sleep 3

    # Start test in background
    local cmd
    cmd=$(build_cmd "${run_dir}")
    cd "${BASE_DIR}"
    eval "${cmd}" > "${run_dir}/test_output.log" 2>&1 &

    local TEST_PID=$!
    echo "  Test PID: ${TEST_PID}"

    echo "  Waiting ${KILL_DELAY}s before killing coordinator agent ${KILL_AGENT_ID}..."
    sleep ${KILL_DELAY}

    echo "  Killing Level-1 coordinator agent ${KILL_AGENT_ID}..."
    kill_coordinator
    echo "  Coordinator killed at $(date)"

    # Wait for test to finish
    echo "  Waiting for test to complete..."
    wait ${TEST_PID} || true

    echo "--- Completed: ${run_label} ---"
}

# ============ BASELINE RUNS ============
echo ""
echo "=============================================="
echo "Phase 1: Baseline runs - no failure"
echo "=============================================="

for i in $(seq 1 ${RUNS}); do
    run_single_test "${OUT_DIR}/baseline/run-${i}" "Baseline run ${i}/${RUNS}"
done

# ============ FAILOVER RUNS ============
echo ""
echo "=============================================="
echo "Phase 2: Failover runs - kill coordinator at ${KILL_DELAY}s"
echo "=============================================="

for i in $(seq 1 ${RUNS}); do
    run_failover_test "${OUT_DIR}/failover/run-${i}" "Failover run ${i}/${RUNS}"
done

echo ""
echo "=============================================="
echo "ALL TESTS COMPLETE"
echo "Results in: ${OUT_DIR}"
echo "=============================================="
