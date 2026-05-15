#!/bin/bash
# run_hier110_resilience_tests.sh — Hierarchical-110 resilience experiments
#
# Tests SWARM+ resilience at scale with hierarchical topology:
#   Test 1a: Kill 10 L0 agents (1 per group) — distributed multi-group failures
#   Test 1b: Kill 20 L0 agents (2 per group) — heavier distributed failures
#   Test 2:  Kill entire site (all L0 agents + coordinator from one group)
#
# Key design decisions:
#   - Configs and jobs are generated ONCE (first baseline run) and reused
#     across all runs via --use-config-dir, ensuring consistent agent profiles.
#   - Kill targets are chosen DYNAMICALLY at runtime by querying Redis for
#     agents that are actively processing jobs (pick_active_agents.py).
#
# Hier-110 layout: 100 Level-0 agents in 10 groups of 10, plus 10 Level-1 coordinators
#   Group 0: agents 1-10,   coordinator 101
#   Group 1: agents 11-20,  coordinator 102
#   ...
#   Group 9: agents 91-100, coordinator 110
#
# Usage:
#   nohup ./run_hier110_resilience_tests.sh > runs/hier110-resilience.log 2>&1 &

set -euo pipefail

# ---- Configuration ----
MODE="remote"
RUNS=5
AGENTS=110
JOBS=2188
DB_HOST="database"
AGENTS_PER_HOST=4
AGENT_HOSTS_FILE="/root/agent_hosts_30.txt"
AGENT_HOSTS=$(paste -sd',' "$AGENT_HOSTS_FILE")
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_DIR="configs"
JOBS_PER_INTERVAL=20
JOBS_PER_PROPOSAL=10
CO_PARENTS=2
KILL_DELAY=90           # Wait 90s for system to be fully loaded before killing
SHUTDOWN_SECONDS=2700   # 45 min timeout per run
OUT_DIR="${BASE_DIR}/runs/hier110-resilience"

mkdir -p "${OUT_DIR}"

echo "=============================================="
echo "Hier-110 Resilience Tests"
echo "  Mode: ${MODE}"
echo "  Runs: ${RUNS} per scenario (+ ${RUNS} baseline)"
echo "  Agents: ${AGENTS} (100 L0 + 10 L1, co-parents=${CO_PARENTS})"
echo "  Jobs: ${JOBS} (Pegasus workload)"
echo "  DB host: ${DB_HOST}"
echo "  Kill delay: ${KILL_DELAY}s"
echo "  Shutdown: ${SHUTDOWN_SECONDS}s"
echo "  Output: ${OUT_DIR}"
echo "  Kill targets: DYNAMIC (pick_active_agents.py)"
echo "=============================================="

# ---- Helper functions ----
build_cmd() {
    local run_dir="$1"
    local use_config_dir="$2"   # "true" or "false"
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
    cmd+=" --co-parents ${CO_PARENTS}"
    cmd+=" --fit-all"
    cmd+=" --run-dir ${run_dir}"
    cmd+=" --log-dir ${run_dir}/logs"
    cmd+=" --config-dir ${CONFIG_DIR}"
    cmd+=" --agents-per-host ${AGENTS_PER_HOST}"
    cmd+=" --starter ./swarm-multi-start.sh"
    cmd+=" --agent-hosts ${AGENT_HOSTS}"
    cmd+=" --remote-repo-dir ${BASE_DIR}"
    if [[ "${use_config_dir}" == "true" ]]; then
        cmd+=" --use-config-dir"
    fi
    echo "${cmd}"
}

stop_agents() {
    echo "  Stopping all agents..."
    ./stop_agents_v2.sh --mode remote --agent-hosts-file "${AGENT_HOSTS_FILE}" \
        --remote-repo-dir "${BASE_DIR}" > /dev/null 2>&1 || true
    # Allow 5s for graceful shutdown (SIGTERM → save_results → Redis)
    sleep 5
    # Belt and suspenders: force-kill any remaining agents
    for host in $(cat "${AGENT_HOSTS_FILE}"); do
        ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 "${host}" \
            "pkill -9 -f 'main.py' 2>/dev/null" &
    done
    wait
    sleep 3
}

flush_redis() {
    echo "  Flushing Redis..."
    docker exec redis redis-cli flushall > /dev/null 2>&1
}

kill_agents_by_id() {
    local ids="$1"
    local scenario_name="$2"
    echo "  [${scenario_name}] Killing agents: ${ids}"

    for aid in $(echo "${ids}" | tr ',' '\n'); do
        # Calculate which host has this agent (4 agents per host, 0-indexed)
        local host_idx=$(( (aid - 1) / AGENTS_PER_HOST + 1 ))
        local host="agent-${host_idx}"
        echo "    Killing agent ${aid} on ${host}"
        ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 "${host}" \
            "ps aux | grep -P 'main\.py\s+${aid}(\s|$)' | grep -v grep | awk '{print \$2}' | xargs -r kill -9" 2>/dev/null &
    done
    wait
    echo "  Agents killed at $(date '+%Y-%m-%d %H:%M:%S')"
}

collect_logs() {
    local run_dir="$1"
    echo "  Collecting agent logs..."
    mkdir -p "${run_dir}/agent_logs"
    for host in $(cat "${AGENT_HOSTS_FILE}"); do
        scp -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
            "${host}:/root/SwarmAgents/agent-*.log" \
            "${run_dir}/agent_logs/" 2>/dev/null || true
    done
}

# ---- Run a baseline test (no failures) ----
run_baseline() {
    local run_dir="$1"
    local run_label="$2"
    local use_config_dir="$3"   # "true" or "false"
    echo ""
    echo "--- ${run_label} -> ${run_dir} ---"
    mkdir -p "${run_dir}/logs"

    flush_redis
    stop_agents

    local cmd
    cmd=$(build_cmd "${run_dir}" "${use_config_dir}")
    cd "${BASE_DIR}"
    echo "  CMD: ${cmd}"
    eval "${cmd}" 2>&1 | tee "${run_dir}/logs/run_test.stdout.log"

    collect_logs "${run_dir}"
    echo "--- ${run_label} COMPLETE ---"
    sleep 5
}

# ---- Run a failure test ----
run_failure() {
    local run_dir="$1"
    local run_label="$2"
    local scenario_name="$3"
    local pick_args="$4"        # Arguments for pick_active_agents.py

    echo ""
    echo "--- ${run_label} -> ${run_dir} ---"
    mkdir -p "${run_dir}/logs"

    flush_redis
    stop_agents

    # Start test in background (always use --use-config-dir for failure runs)
    local cmd
    cmd=$(build_cmd "${run_dir}" "true")
    cd "${BASE_DIR}"
    echo "  CMD: ${cmd}"
    eval "${cmd}" > "${run_dir}/logs/run_test.stdout.log" 2>&1 &
    local TEST_PID=$!
    echo "  Test PID: ${TEST_PID}"

    # Wait for system to load, then pick active agents and kill them
    echo "  Waiting ${KILL_DELAY}s before killing agents..."
    sleep ${KILL_DELAY}

    # Dynamically pick kill targets from agents that are actively processing jobs
    echo "  Querying Redis for active agents..."
    local kill_ids
    kill_ids=$(python3.11 pick_active_agents.py --db-host "${DB_HOST}" ${pick_args} 2>"${run_dir}/logs/pick_agents.stderr.log")
    echo "  Selected kill targets: ${kill_ids}"

    if [[ -z "${kill_ids}" ]]; then
        echo "  WARNING: No agents selected for killing, skipping kill step"
    else
        kill_agents_by_id "${kill_ids}" "${scenario_name}"
    fi

    # Record kill event
    local num_killed=0
    if [[ -n "${kill_ids}" ]]; then
        num_killed=$(echo "${kill_ids}" | tr ',' '\n' | wc -l | tr -d ' ')
    fi
    cat > "${run_dir}/kill_event.json" <<KILLJSON
{
    "scenario": "${scenario_name}",
    "kill_time_epoch": $(date +%s),
    "kill_time": "$(date '+%Y-%m-%d %H:%M:%S')",
    "kill_delay_s": ${KILL_DELAY},
    "agents_killed": [$(echo "${kill_ids}" | sed 's/,/, /g')],
    "num_killed": ${num_killed},
    "pick_args": "${pick_args}"
}
KILLJSON

    # Wait for test to finish
    echo "  Waiting for test to complete..."
    wait ${TEST_PID} || true

    collect_logs "${run_dir}"
    echo "--- ${run_label} COMPLETE ---"
    sleep 5
}

# ============================================================
# PHASE 0: BASELINE (no failures)
# ============================================================
echo ""
echo "=============================================="
echo "Phase 0: BASELINE — ${RUNS} runs, no failures"
echo "=============================================="

for i in $(seq 1 ${RUNS}); do
    if [[ ${i} -eq 1 ]]; then
        # First run: generate configs and jobs (no --use-config-dir)
        echo "  [Run 1] Generating configs and jobs (one-time setup)"
        run_baseline "${OUT_DIR}/baseline/run-${i}" "Baseline run ${i}/${RUNS}" "false"
    else
        # Subsequent runs: reuse configs and jobs
        run_baseline "${OUT_DIR}/baseline/run-${i}" "Baseline run ${i}/${RUNS}" "true"
    fi
done

# ============================================================
# PHASE 1a: DISTRIBUTED FAILURES — 10 L0 agents (1 per group)
# ============================================================
echo ""
echo "=============================================="
echo "Phase 1a: 10 L0 failures (1/group) — ${RUNS} runs"
echo "  Kill targets: dynamically selected from active agents"
echo "=============================================="

for i in $(seq 1 ${RUNS}); do
    run_failure "${OUT_DIR}/distributed-10/run-${i}" \
        "Distributed-10 run ${i}/${RUNS}" \
        "distributed-10-L0" \
        "--count 10 --distributed"
done

# ============================================================
# PHASE 1b: DISTRIBUTED FAILURES — 20 L0 agents (2 per group)
# ============================================================
echo ""
echo "=============================================="
echo "Phase 1b: 20 L0 failures (2/group) — ${RUNS} runs"
echo "  Kill targets: dynamically selected from active agents"
echo "=============================================="

for i in $(seq 1 ${RUNS}); do
    run_failure "${OUT_DIR}/distributed-20/run-${i}" \
        "Distributed-20 run ${i}/${RUNS}" \
        "distributed-20-L0" \
        "--count 20 --distributed"
done

# ============================================================
# PHASE 2: SITE OUTAGE — entire group + coordinator
# ============================================================
echo ""
echo "=============================================="
echo "Phase 2: Site outage (most-active group + coordinator) — ${RUNS} runs"
echo "  Kill targets: dynamically selected from most-active group"
echo "=============================================="

for i in $(seq 1 ${RUNS}); do
    run_failure "${OUT_DIR}/site-outage/run-${i}" \
        "Site-outage run ${i}/${RUNS}" \
        "site-outage" \
        "--count 10 --site-outage"
done

# ============================================================
# SUMMARY
# ============================================================
echo ""
echo "=============================================="
echo "ALL RESILIENCE TESTS COMPLETE"
echo "=============================================="
echo "Results:"
echo "  Baseline:       ${OUT_DIR}/baseline/run-{1..${RUNS}}/"
echo "  Distributed-10: ${OUT_DIR}/distributed-10/run-{1..${RUNS}}/"
echo "  Distributed-20: ${OUT_DIR}/distributed-20/run-{1..${RUNS}}/"
echo "  Site-outage:    ${OUT_DIR}/site-outage/run-{1..${RUNS}}/"
echo ""
echo "Kill targets used (check kill_event.json in each run dir):"
echo "  cat ${OUT_DIR}/*/run-*/kill_event.json | python3.11 -m json.tool"
echo ""
echo "Quick analysis:"
echo "  grep -c 'RESTART' ${OUT_DIR}/*/run-*/agent_logs/agent-*.log 2>/dev/null | head -20"
echo "  grep 'detected as FAILED' ${OUT_DIR}/*/run-*/agent_logs/agent-*.log 2>/dev/null | head -20"
echo "=============================================="
