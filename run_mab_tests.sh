#!/bin/bash
# run_mab_tests.sh — Multi-Armed Bandit evaluation with Pegasus workloads
#
# Compares MAB delegation strategies in hierarchical topology:
#   Phase 0: Baseline (MAB disabled) — round-robin delegation to all groups
#   Phase 1: Epsilon-Greedy (ε=0.1, decay=0.995)
#   Phase 2: UCB1 (exploration_weight=1.41)
#
# All phases use the same 2188 Pegasus jobs (pre-converted) and Hier-110 layout.
# MAB is toggled by patching config_swarm_multi.yml between phases.
#
# Hier-110 layout: 100 Level-0 agents in 10 groups of 10, plus 10 Level-1 coordinators
#   Group 0: agents 1-10,   coordinator 101
#   ...
#   Group 9: agents 91-100, coordinator 110
#
# Usage:
#   nohup ./run_mab_tests.sh > runs/mab-evaluation.log 2>&1 &

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
CONFIG_FILE="${BASE_DIR}/config_swarm_multi.yml"
JOBS_PER_INTERVAL=20
JOBS_PER_PROPOSAL=10
CO_PARENTS=2
SHUTDOWN_SECONDS=10800   # 3 hour timeout per run
OUT_DIR="${BASE_DIR}/runs/mab-evaluation"

mkdir -p "${OUT_DIR}"

echo "=============================================="
echo "MAB Evaluation Tests (Pegasus Workloads)"
echo "  Mode: ${MODE}"
echo "  Runs: ${RUNS} per scenario (3 scenarios)"
echo "  Agents: ${AGENTS} (100 L0 + 10 L1, co-parents=${CO_PARENTS})"
echo "  Jobs: ${JOBS} (Pegasus workload)"
echo "  DB host: ${DB_HOST}"
echo "  Shutdown: ${SHUTDOWN_SECONDS}s"
echo "  Output: ${OUT_DIR}"
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
    # Allow 5s for graceful shutdown (SIGTERM -> save_results -> Redis)
    sleep 5
    # Force-kill any remaining agents
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

# ---- Config patching functions ----
set_mab_disabled() {
    echo "  Patching config: MAB disabled"
    python3.11 -c "
import yaml
with open('${CONFIG_FILE}') as f:
    cfg = yaml.safe_load(f)
cfg['mab']['enabled'] = False
cfg['mab']['failure_simulation']['enabled'] = False
with open('${CONFIG_FILE}', 'w') as f:
    yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)
"
}

set_mab_epsilon_greedy() {
    echo "  Patching config: MAB epsilon-greedy"
    python3.11 -c "
import yaml
with open('${CONFIG_FILE}') as f:
    cfg = yaml.safe_load(f)
cfg['mab']['enabled'] = True
cfg['mab']['algorithm'] = 'epsilon_greedy'
cfg['mab']['epsilon'] = 0.1
cfg['mab']['epsilon_decay'] = 0.995
cfg['mab']['epsilon_min'] = 0.01
cfg['mab']['top_k'] = 1
cfg['mab']['persist_to_redis'] = True
cfg['mab']['failure_simulation']['enabled'] = False
with open('${CONFIG_FILE}', 'w') as f:
    yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)
"
}

set_mab_ucb1() {
    echo "  Patching config: MAB UCB1"
    python3.11 -c "
import yaml
with open('${CONFIG_FILE}') as f:
    cfg = yaml.safe_load(f)
cfg['mab']['enabled'] = True
cfg['mab']['algorithm'] = 'ucb1'
cfg['mab']['exploration_weight'] = 1.41
cfg['mab']['top_k'] = 1
cfg['mab']['persist_to_redis'] = True
cfg['mab']['failure_simulation']['enabled'] = False
with open('${CONFIG_FILE}', 'w') as f:
    yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)
"
}

# ---- Run a test ----
run_test() {
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

# ---- Save MAB state from Redis after each run ----
save_mab_state() {
    local run_dir="$1"
    echo "  Saving MAB state from Redis..."
    python3.11 -c "
import redis, json, sys
r = redis.StrictRedis(host='database', port=6379, decode_responses=True)
mab_data = {}
for key in r.scan_iter(match='mab:*', count=1000):
    mab_data[key] = json.loads(r.get(key))
metrics_data = {}
for key in r.scan_iter(match='metrics:*', count=1000):
    raw = r.get(key)
    if raw:
        try:
            d = json.loads(raw)
            if 'mab_stats' in d or 'mab_selections' in d or 'mab_rewards' in d:
                metrics_data[key] = {
                    'mab_stats': d.get('mab_stats'),
                    'mab_selections': d.get('mab_selections'),
                    'mab_rewards': d.get('mab_rewards'),
                }
        except Exception:
            pass
output = {'mab_state': mab_data, 'mab_metrics': metrics_data}
with open('${run_dir}/mab_state.json', 'w') as f:
    json.dump(output, f, indent=2)
print(f'  Saved MAB state: {len(mab_data)} bandits, {len(metrics_data)} agent metrics')
" 2>/dev/null || echo "  WARN: Could not save MAB state"
}

# ============================================================
# PHASE 0: BASELINE (MAB disabled)
# ============================================================
echo ""
echo "=============================================="
echo "Phase 0: BASELINE — MAB disabled, ${RUNS} runs"
echo "=============================================="

set_mab_disabled

for i in $(seq 1 ${RUNS}); do
    if [[ ${i} -eq 1 ]]; then
        # First run: generate configs and jobs
        run_test "${OUT_DIR}/baseline/run-${i}" "Baseline run ${i}/${RUNS}" "false"
    else
        # Subsequent runs: reuse configs and jobs
        run_test "${OUT_DIR}/baseline/run-${i}" "Baseline run ${i}/${RUNS}" "true"
    fi
done

# ============================================================
# PHASE 1: EPSILON-GREEDY
# ============================================================
echo ""
echo "=============================================="
echo "Phase 1: EPSILON-GREEDY — ${RUNS} runs"
echo "  epsilon=0.1, decay=0.995, top_k=1"
echo "=============================================="

set_mab_epsilon_greedy

for i in $(seq 1 ${RUNS}); do
    # Regenerate configs on first run (new MAB settings)
    if [[ ${i} -eq 1 ]]; then
        run_test "${OUT_DIR}/epsilon-greedy/run-${i}" "Epsilon-Greedy run ${i}/${RUNS}" "false"
    else
        run_test "${OUT_DIR}/epsilon-greedy/run-${i}" "Epsilon-Greedy run ${i}/${RUNS}" "true"
    fi
    save_mab_state "${OUT_DIR}/epsilon-greedy/run-${i}"
done

# ============================================================
# PHASE 2: UCB1
# ============================================================
echo ""
echo "=============================================="
echo "Phase 2: UCB1 — ${RUNS} runs"
echo "  exploration_weight=1.41, top_k=1"
echo "=============================================="

set_mab_ucb1

for i in $(seq 1 ${RUNS}); do
    if [[ ${i} -eq 1 ]]; then
        run_test "${OUT_DIR}/ucb1/run-${i}" "UCB1 run ${i}/${RUNS}" "false"
    else
        run_test "${OUT_DIR}/ucb1/run-${i}" "UCB1 run ${i}/${RUNS}" "true"
    fi
    save_mab_state "${OUT_DIR}/ucb1/run-${i}"
done

# ============================================================
# SUMMARY
# ============================================================
echo ""
echo "=============================================="
echo "ALL MAB EVALUATION TESTS COMPLETE"
echo "=============================================="
echo "Results:"
echo "  Baseline:       ${OUT_DIR}/baseline/run-{1..${RUNS}}/"
echo "  Epsilon-Greedy: ${OUT_DIR}/epsilon-greedy/run-{1..${RUNS}}/"
echo "  UCB1:           ${OUT_DIR}/ucb1/run-{1..${RUNS}}/"
echo ""
echo "MAB state saved in each run dir as mab_state.json"
echo ""
echo "Quick analysis:"
echo "  python3.11 plot_mab_results.py --db-host ${DB_HOST} --output-dir ${OUT_DIR}/plots"
echo "  python3.11 plot_multi_run_results.py --base-dir ${OUT_DIR} --output-dir ${OUT_DIR}/plots"
echo "=============================================="
