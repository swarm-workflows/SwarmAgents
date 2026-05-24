#!/bin/bash
# run_mab_tests_topk.sh — MAB follow-up: varying top_k with Pegasus workloads
#
# Follow-up to initial MAB evaluation. Initial results showed top_k=1
# creates bottlenecks by funneling all jobs to a single group. This
# experiment tests higher top_k values to find the optimal balance
# between MAB-guided selection and load distribution.
#
# Phases:
#   Phase 0: Epsilon-Greedy, top_k=3 (delegate to best 3 of 10 groups)
#   Phase 1: Epsilon-Greedy, top_k=5 (delegate to best 5 of 10 groups)
#   Phase 2: UCB1, top_k=3
#   Phase 3: UCB1, top_k=5
#
# All phases use 2188 Pegasus jobs and Hier-110 layout.
#
# Usage:
#   nohup ./run_mab_tests_topk.sh > runs/mab-topk.log 2>&1 &

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
OUT_DIR="${BASE_DIR}/runs/mab-topk"

mkdir -p "${OUT_DIR}"

echo "=============================================="
echo "MAB Follow-up: top_k Sweep (Pegasus Workloads)"
echo "  Mode: ${MODE}"
echo "  Runs: ${RUNS} per scenario (4 scenarios)"
echo "  Agents: ${AGENTS} (100 L0 + 10 L1, co-parents=${CO_PARENTS})"
echo "  Jobs: ${JOBS} (Pegasus workload)"
echo "  DB host: ${DB_HOST}"
echo "  Shutdown: ${SHUTDOWN_SECONDS}s"
echo "  Output: ${OUT_DIR}"
echo "=============================================="

# ---- Helper functions ----
build_cmd() {
    local run_dir="$1"
    local use_config_dir="$2"
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

# ---- Config patching ----
set_mab_config() {
    local algorithm="$1"
    local top_k="$2"
    echo "  Patching config: MAB ${algorithm}, top_k=${top_k}"
    python3.11 -c "
import yaml
with open('${CONFIG_FILE}') as f:
    cfg = yaml.safe_load(f)
cfg['mab']['enabled'] = True
cfg['mab']['algorithm'] = '${algorithm}'
cfg['mab']['top_k'] = ${top_k}
cfg['mab']['persist_to_redis'] = True
cfg['mab']['failure_simulation']['enabled'] = False
if '${algorithm}' == 'epsilon_greedy':
    cfg['mab']['epsilon'] = 0.1
    cfg['mab']['epsilon_decay'] = 0.995
    cfg['mab']['epsilon_min'] = 0.01
elif '${algorithm}' == 'ucb1':
    cfg['mab']['exploration_weight'] = 1.41
with open('${CONFIG_FILE}', 'w') as f:
    yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)
"
}

# ---- Run a test ----
run_test() {
    local run_dir="$1"
    local run_label="$2"
    local use_config_dir="$3"
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

# ---- Save MAB state ----
save_mab_state() {
    local run_dir="$1"
    echo "  Saving MAB state from Redis..."
    python3.11 -c "
import redis, json
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
# PHASE 0: EPSILON-GREEDY, top_k=3
# ============================================================
echo ""
echo "=============================================="
echo "Phase 0: EPSILON-GREEDY top_k=3 — ${RUNS} runs"
echo "=============================================="

set_mab_config "epsilon_greedy" 3

for i in $(seq 1 ${RUNS}); do
    if [[ ${i} -eq 1 ]]; then
        run_test "${OUT_DIR}/eps-topk3/run-${i}" "EpsGreedy-topk3 run ${i}/${RUNS}" "false"
    else
        run_test "${OUT_DIR}/eps-topk3/run-${i}" "EpsGreedy-topk3 run ${i}/${RUNS}" "true"
    fi
    save_mab_state "${OUT_DIR}/eps-topk3/run-${i}"
done

# ============================================================
# PHASE 1: EPSILON-GREEDY, top_k=5
# ============================================================
echo ""
echo "=============================================="
echo "Phase 1: EPSILON-GREEDY top_k=5 — ${RUNS} runs"
echo "=============================================="

set_mab_config "epsilon_greedy" 5

for i in $(seq 1 ${RUNS}); do
    if [[ ${i} -eq 1 ]]; then
        run_test "${OUT_DIR}/eps-topk5/run-${i}" "EpsGreedy-topk5 run ${i}/${RUNS}" "false"
    else
        run_test "${OUT_DIR}/eps-topk5/run-${i}" "EpsGreedy-topk5 run ${i}/${RUNS}" "true"
    fi
    save_mab_state "${OUT_DIR}/eps-topk5/run-${i}"
done

# ============================================================
# PHASE 2: UCB1, top_k=3
# ============================================================
echo ""
echo "=============================================="
echo "Phase 2: UCB1 top_k=3 — ${RUNS} runs"
echo "=============================================="

set_mab_config "ucb1" 3

for i in $(seq 1 ${RUNS}); do
    if [[ ${i} -eq 1 ]]; then
        run_test "${OUT_DIR}/ucb1-topk3/run-${i}" "UCB1-topk3 run ${i}/${RUNS}" "false"
    else
        run_test "${OUT_DIR}/ucb1-topk3/run-${i}" "UCB1-topk3 run ${i}/${RUNS}" "true"
    fi
    save_mab_state "${OUT_DIR}/ucb1-topk3/run-${i}"
done

# ============================================================
# PHASE 3: UCB1, top_k=5
# ============================================================
echo ""
echo "=============================================="
echo "Phase 3: UCB1 top_k=5 — ${RUNS} runs"
echo "=============================================="

set_mab_config "ucb1" 5

for i in $(seq 1 ${RUNS}); do
    if [[ ${i} -eq 1 ]]; then
        run_test "${OUT_DIR}/ucb1-topk5/run-${i}" "UCB1-topk5 run ${i}/${RUNS}" "false"
    else
        run_test "${OUT_DIR}/ucb1-topk5/run-${i}" "UCB1-topk5 run ${i}/${RUNS}" "true"
    fi
    save_mab_state "${OUT_DIR}/ucb1-topk5/run-${i}"
done

# ============================================================
# SUMMARY
# ============================================================
echo ""
echo "=============================================="
echo "ALL MAB top_k SWEEP TESTS COMPLETE"
echo "=============================================="
echo "Results:"
echo "  EpsGreedy top_k=3: ${OUT_DIR}/eps-topk3/run-{1..${RUNS}}/"
echo "  EpsGreedy top_k=5: ${OUT_DIR}/eps-topk5/run-{1..${RUNS}}/"
echo "  UCB1 top_k=3:      ${OUT_DIR}/ucb1-topk3/run-{1..${RUNS}}/"
echo "  UCB1 top_k=5:      ${OUT_DIR}/ucb1-topk5/run-{1..${RUNS}}/"
echo ""
echo "Combined with initial results (runs/mab-evaluation/):"
echo "  Baseline (no MAB), EpsGreedy top_k=1, UCB1 top_k=1"
echo "=============================================="
