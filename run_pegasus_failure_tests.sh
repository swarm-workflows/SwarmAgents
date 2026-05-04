#!/bin/bash
# run_pegasus_failure_tests.sh — Pegasus workload failure + reselection tests
#
# Runs N baseline + N failure tests with 547 Pegasus jobs on 30 agents.
# Failure tests kill 3 Level-0 agents at t=60s to trigger job reselection.
#
# Usage:
#   # Remote mode (30 VMs):
#   ./run_pegasus_failure_tests.sh --mode remote --runs 10 --db-host database \
#       --agent-hosts "agent-1,...,agent-30"
#
#   # Local mode (single host):
#   ./run_pegasus_failure_tests.sh --mode local --runs 10 --db-host localhost
set -euo pipefail

# ---- defaults ----
MODE="remote"
RUNS=10
AGENTS=30
JOBS=547
DB_HOST="database"
AGENTS_PER_HOST=1
AGENT_HOSTS=""
KILL_DELAY=60
SHUTDOWN_SECONDS=900
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_DIR="configs"
JOBS_PER_INTERVAL=20
JOBS_PER_PROPOSAL=10

# Agents to kill (Level-0 resource agents from different groups)
KILL_AGENT_IDS="5,10,15"

usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --mode MODE              local or remote (default: remote)
  --runs N                 Number of runs per phase (default: 10)
  --agents N               Total agents (default: 30)
  --jobs N                 Number of jobs (default: 547)
  --db-host HOST           Redis host (default: database)
  --agents-per-host N      Agents per host, remote mode only (default: 1)
  --agent-hosts CSV        Comma-separated host list (auto-generated if omitted)
  --config-dir DIR         Config directory (default: configs)
  --kill-agents IDS        Comma-separated agent IDs to kill (default: 5,10,15)
  --kill-delay N           Seconds before killing agents (default: 60)
  --shutdown-seconds N     Max seconds per run (default: 900)
  -h, --help               Show this help
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)               MODE="$2"; shift 2 ;;
        --runs)               RUNS="$2"; shift 2 ;;
        --agents)             AGENTS="$2"; shift 2 ;;
        --jobs)               JOBS="$2"; shift 2 ;;
        --db-host)            DB_HOST="$2"; shift 2 ;;
        --agents-per-host)    AGENTS_PER_HOST="$2"; shift 2 ;;
        --agent-hosts)        AGENT_HOSTS="$2"; shift 2 ;;
        --config-dir)         CONFIG_DIR="$2"; shift 2 ;;
        --kill-agents)        KILL_AGENT_IDS="$2"; shift 2 ;;
        --kill-delay)         KILL_DELAY="$2"; shift 2 ;;
        --shutdown-seconds)   SHUTDOWN_SECONDS="$2"; shift 2 ;;
        -h|--help)            usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

# Auto-generate agent hosts if not provided (remote mode)
if [[ "${MODE}" == "remote" && -z "${AGENT_HOSTS}" ]]; then
    AGENT_HOSTS=$(seq 1 $((AGENTS / AGENTS_PER_HOST)) | sed 's/^/agent-/' | paste -sd,)
fi

OUT_DIR="${BASE_DIR}/runs/pegasus-failure"
mkdir -p "${OUT_DIR}"

echo "=============================================="
echo "Pegasus Failure + Reselection Tests"
echo "  Mode: ${MODE}"
echo "  Runs: ${RUNS} baseline + ${RUNS} failure"
echo "  Agents: ${AGENTS}"
echo "  Jobs: ${JOBS} (Pegasus workload)"
echo "  DB host: ${DB_HOST}"
echo "  Config dir: ${CONFIG_DIR}"
echo "  Output dir: ${OUT_DIR}"
echo "  Kill targets: agents ${KILL_AGENT_IDS}"
echo "  Kill delay: ${KILL_DELAY}s"
echo "  Shutdown: ${SHUTDOWN_SECONDS}s"
if [[ "${MODE}" == "remote" ]]; then
    echo "  Agents/host: ${AGENTS_PER_HOST}"
fi
echo "=============================================="

# ---- Build run_test.py command ----
build_cmd() {
    local run_dir="$1"
    local cmd="python3.11 run_test.py"
    cmd+=" --mode ${MODE}"
    cmd+=" --agent-type resource"
    cmd+=" --agents ${AGENTS}"
    cmd+=" --topology hierarchical"
    cmd+=" --hierarchical-level1-agent-type resource"
    cmd+=" --jobs ${JOBS}"
    cmd+=" --db-host ${DB_HOST}"
    cmd+=" --jobs-per-interval ${JOBS_PER_INTERVAL}"
    cmd+=" --jobs-per-proposal ${JOBS_PER_PROPOSAL}"
    cmd+=" --runtime 30"
    cmd+=" --job-interval 0.5"
    cmd+=" --grace-seconds 60"
    cmd+=" --shutdown-after-seconds ${SHUTDOWN_SECONDS}"
    cmd+=" --fit-all"
    cmd+=" --run-dir ${run_dir}"
    cmd+=" --log-dir ${run_dir}/logs"
    cmd+=" --config-dir ${CONFIG_DIR}"
    cmd+=" --use-config-dir"

    if [[ "${MODE}" == "remote" ]]; then
        cmd+=" --agents-per-host ${AGENTS_PER_HOST}"
        cmd+=" --starter ./swarm-multi-start.sh"
        cmd+=" --agent-hosts ${AGENT_HOSTS}"
        cmd+=" --remote-repo-dir ${BASE_DIR}"
    fi
    echo "${cmd}"
}

stop_agents() {
    if [[ "${MODE}" == "remote" ]]; then
        ./stop_agents_v2.sh > /dev/null 2>&1 || true
        # Also kill on all remote hosts
        for host in $(echo "${AGENT_HOSTS}" | tr ',' '\n'); do
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 "${host}" \
                "pkill -f 'main.py' 2>/dev/null" &
        done
        wait
    else
        ./stop_agents_v2.sh --mode local > /dev/null 2>&1 || true
    fi
}

kill_agents_by_id() {
    local ids="$1"
    echo "  Killing agents: ${ids}"
    for aid in $(echo "${ids}" | tr ',' '\n'); do
        if [[ "${MODE}" == "local" ]]; then
            ps aux | grep "main.py.*${aid}" | grep -v grep | awk '{print $2}' | xargs kill -9 2>/dev/null || true
        else
            # In remote mode with 1 agent per host, agent N is on agent-N
            local host="agent-${aid}"
            echo "    Killing agent ${aid} on ${host}"
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 "${host}" \
                "pkill -9 -f 'main.py' 2>/dev/null" &
        fi
    done
    wait
    echo "  Agents killed at $(date)"
}

collect_agent_logs() {
    local run_dir="$1"
    echo "  Collecting agent logs..."
    mkdir -p "${run_dir}/agent_logs"
    for host in $(echo "${AGENT_HOSTS}" | tr ',' '\n'); do
        scp -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
            "${host}:/root/SwarmAgents/agent-*.log" \
            "${run_dir}/agent_logs/" 2>/dev/null || true
    done
}

# ---- Generate configs once (reused via --use-config-dir) ----
echo ""
echo "Generating configs for ${AGENTS}-agent hierarchical topology..."
cd "${BASE_DIR}"
python3.11 generate_configs.py ${AGENTS} ${JOBS_PER_PROPOSAL} \
    config_swarm_multi.yml ${CONFIG_DIR} hierarchical ${DB_HOST} ${JOBS} \
    --agent-hosts-file agent_hosts.txt --agents-per-host ${AGENTS_PER_HOST} \
    --agent-type resource --hierarchical-level1-agent-type resource --fit-all
echo "Configs generated."

# ============ BASELINE RUNS ============
echo ""
echo "=============================================="
echo "Phase 1: Baseline runs (no failure) — ${RUNS} runs"
echo "=============================================="

for i in $(seq 1 ${RUNS}); do
    run_dir="${OUT_DIR}/baseline/run-${i}"
    echo ""
    echo "--- Baseline run ${i}/${RUNS} -> ${run_dir} ---"
    mkdir -p "${run_dir}/logs"

    # Flush Redis
    docker exec redis redis-cli flushall > /dev/null 2>&1

    # Stop any running agents
    stop_agents
    sleep 3

    # Run the test
    cmd=$(build_cmd "${run_dir}")
    cd "${BASE_DIR}"
    eval "${cmd}" 2>&1 | tee "${run_dir}/logs/run_test.stdout.log"

    # Collect logs from agent VMs
    if [[ "${MODE}" == "remote" ]]; then
        collect_agent_logs "${run_dir}"
    fi

    echo "--- Baseline run ${i}/${RUNS} complete ---"
    sleep 5
done

# ============ FAILURE RUNS ============
echo ""
echo "=============================================="
echo "Phase 2: Failure runs (kill agents ${KILL_AGENT_IDS} at ${KILL_DELAY}s) — ${RUNS} runs"
echo "=============================================="

for i in $(seq 1 ${RUNS}); do
    run_dir="${OUT_DIR}/failure/run-${i}"
    echo ""
    echo "--- Failure run ${i}/${RUNS} -> ${run_dir} ---"
    mkdir -p "${run_dir}/logs"

    # Flush Redis
    docker exec redis redis-cli flushall > /dev/null 2>&1

    # Stop any running agents
    stop_agents
    sleep 3

    # Start test in background
    cmd=$(build_cmd "${run_dir}")
    cd "${BASE_DIR}"
    eval "${cmd}" > "${run_dir}/logs/run_test.stdout.log" 2>&1 &
    TEST_PID=$!
    echo "  Test PID: ${TEST_PID}"

    # Wait, then kill target agents
    echo "  Waiting ${KILL_DELAY}s before killing agents..."
    sleep ${KILL_DELAY}
    kill_agents_by_id "${KILL_AGENT_IDS}"

    # Record kill event
    echo "kill_time=$(date +%s),agents=${KILL_AGENT_IDS},delay=${KILL_DELAY}" \
        > "${run_dir}/kill_event.csv"

    # Wait for test to finish
    echo "  Waiting for test to complete..."
    wait ${TEST_PID} || true

    # Collect logs from agent VMs (including failure/reselection events)
    if [[ "${MODE}" == "remote" ]]; then
        collect_agent_logs "${run_dir}"
    fi

    echo "--- Failure run ${i}/${RUNS} complete ---"
    sleep 5
done

echo ""
echo "=============================================="
echo "ALL TESTS COMPLETE"
echo "Results in: ${OUT_DIR}"
echo "  Baseline: ${OUT_DIR}/baseline/run-{1..${RUNS}}/"
echo "  Failure:  ${OUT_DIR}/failure/run-{1..${RUNS}}/"
echo ""
echo "To analyze reselection metrics:"
echo "  grep -c 'RESTART' ${OUT_DIR}/failure/run-*/agent_logs/agent-*.log"
echo "  grep 'reassign' ${OUT_DIR}/failure/run-*/logs/run_test.stdout.log"
echo "=============================================="
