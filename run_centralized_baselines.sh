#!/usr/bin/env bash
# run_centralized_baselines.sh
# Run all three centralized baseline schedulers (Greedy, Round-Robin, Random)
# with multiple iterations for statistical significance.
#
# Supports two modes:
#   local  — scheduling + execution in a single process (default)
#   remote — scheduling on this host, execution on remote VMs via Redis
#
# Usage:
#   sudo ./run_centralized_baselines.sh [OPTIONS]
#
# Options:
#   --mode          MODE  Execution mode: local or remote (default: local)
#   --agents        N     Number of agents (default: 30)
#   --jobs          N     Total number of jobs (default: 500)
#   --runs          N     Number of iterations per scheduler (default: 10)
#   --db-host       HOST  Redis host (default: localhost)
#   --db-port       PORT  Redis port (default: 6379)
#   --jobs-per-interval N Jobs submitted per interval (default: 10)
#   --base-dir      DIR   Base output directory (default: runs/baselines)
#   --schedulers    LIST  Comma-separated schedulers to run (default: greedy,round-robin,random)
#   --reuse-jobs          Reuse existing jobs/ and agent_profiles.json
#   --no-dtns             Disable DTN generation
#   --timeout       SECS  Max run time per test in seconds (default: 600)
#   --debug               Enable debug logging
#   --agents-per-host N   Agents per remote host (default: 1)
#   --agent-hosts-file F  Hosts file for remote mode (one hostname per line)
#                         If omitted, auto-generated as agent-1..agent-N based on
#                         --agents and --agents-per-host
#   --remote-repo-dir DIR Repo path on remote hosts (default: /root/SwarmAgents)
#   --skip-preflight      Skip SSH preflight checks (remote mode)
#   --worker-timeout SECS Seconds to wait for workers to register (default: 30)
#
# Examples:
#   # Local: all 3 schedulers, 10 runs each, 30 agents, 500 jobs
#   sudo ./run_centralized_baselines.sh
#
#   # Local: quick test, 1 run each, reuse existing jobs
#   sudo ./run_centralized_baselines.sh --runs 1 --reuse-jobs
#
#   # Local: only greedy, 5 runs
#   sudo ./run_centralized_baselines.sh --schedulers greedy --runs 5
#
#   # Remote: provide an explicit hosts file
#   sudo ./run_centralized_baselines.sh --mode remote \
#     --agent-hosts-file agent_hosts.txt --db-host 10.0.0.1 \
#     --agents 30 --jobs 500 --runs 10
#
#   # Remote: auto-generate hosts (30 agents, 1 per host → agent-1..agent-30)
#   sudo ./run_centralized_baselines.sh --mode remote \
#     --db-host database --agents 30 --jobs 500 --runs 10
#
#   # Remote: 30 agents across 10 hosts (3 per host → agent-1..agent-10)
#   sudo ./run_centralized_baselines.sh --mode remote \
#     --db-host database --agents 30 --agents-per-host 3 --runs 10
#
#   # Remote: reuse existing jobs and profiles
#   sudo ./run_centralized_baselines.sh --mode remote \
#     --agent-hosts-file agent_hosts.txt --db-host 10.0.0.1 \
#     --reuse-jobs --runs 5

set -euo pipefail

# ─── Defaults ───────────────────────────────────────────────────────
MODE=local
AGENTS=30
JOBS=500
RUNS=10
DB_HOST=localhost
DB_PORT=6379
JOBS_PER_INTERVAL=10
BASE_DIR="runs/baselines"
SCHEDULERS="greedy,round-robin,random"
REUSE_JOBS=false
NO_DTNS=""
TIMEOUT=600
DEBUG=""
PYTHON=python3.11
AGENTS_PER_HOST=1
AGENT_HOSTS_FILE=""
REMOTE_REPO_DIR="/root/SwarmAgents"
SKIP_PREFLIGHT=""
WORKER_TIMEOUT=30

# ─── Parse arguments ───────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)             MODE="$2";              shift 2 ;;
    --agents)           AGENTS="$2";            shift 2 ;;
    --jobs)             JOBS="$2";              shift 2 ;;
    --runs)             RUNS="$2";              shift 2 ;;
    --db-host)          DB_HOST="$2";           shift 2 ;;
    --db-port)          DB_PORT="$2";           shift 2 ;;
    --jobs-per-interval) JOBS_PER_INTERVAL="$2"; shift 2 ;;
    --base-dir)         BASE_DIR="$2";          shift 2 ;;
    --schedulers)       SCHEDULERS="$2";        shift 2 ;;
    --reuse-jobs)       REUSE_JOBS=true;        shift ;;
    --no-dtns)          NO_DTNS="--no-dtns";    shift ;;
    --timeout)          TIMEOUT="$2";           shift 2 ;;
    --debug)            DEBUG="--debug";        shift ;;
    --python)           PYTHON="$2";            shift 2 ;;
    --agents-per-host)  AGENTS_PER_HOST="$2";   shift 2 ;;
    --agent-hosts-file) AGENT_HOSTS_FILE="$2";  shift 2 ;;
    --remote-repo-dir)  REMOTE_REPO_DIR="$2";   shift 2 ;;
    --skip-preflight)   SKIP_PREFLIGHT="--skip-preflight"; shift ;;
    --worker-timeout)   WORKER_TIMEOUT="$2";    shift 2 ;;
    -h|--help)
      sed -n '2,/^$/p' "$0" | grep '^#' | sed 's/^# \?//'
      exit 0 ;;
    *)
      echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# ─── Validate mode ────────────────────────────────────────────────
if [[ "$MODE" != "local" && "$MODE" != "remote" ]]; then
  echo "ERROR: --mode must be 'local' or 'remote' (got '$MODE')" >&2; exit 1
fi

if [[ "$MODE" == "remote" && -n "$AGENT_HOSTS_FILE" && ! -f "$AGENT_HOSTS_FILE" ]]; then
  echo "ERROR: agent hosts file '$AGENT_HOSTS_FILE' not found" >&2; exit 1
fi

# ─── Resolve paths ─────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

log() { printf '[%(%Y-%m-%d %H:%M:%S)T] %s\n' -1 "$*"; }

# ─── Auto-generate agent_hosts.txt if not provided (remote mode) ──
if [[ "$MODE" == "remote" && -z "$AGENT_HOSTS_FILE" ]]; then
  NUM_HOSTS=$(( (AGENTS + AGENTS_PER_HOST - 1) / AGENTS_PER_HOST ))
  AGENT_HOSTS_FILE="agent_hosts.txt"
  log "Auto-generating $AGENT_HOSTS_FILE: $NUM_HOSTS hosts ($AGENTS agents, $AGENTS_PER_HOST per host)"
  > "$AGENT_HOSTS_FILE"
  for i in $(seq 1 "$NUM_HOSTS"); do
    echo "agent-$i" >> "$AGENT_HOSTS_FILE"
  done
fi

# ─── Verify prerequisites ──────────────────────────────────────────
log "Checking prerequisites..."

if ! command -v "$PYTHON" &>/dev/null; then
  echo "ERROR: $PYTHON not found" >&2; exit 1
fi

# Check Redis connectivity
if ! "$PYTHON" -c "
import redis
r = redis.StrictRedis(host='$DB_HOST', port=$DB_PORT, socket_timeout=5)
r.ping()
print('Redis OK')
" 2>/dev/null; then
  echo "ERROR: Cannot connect to Redis at $DB_HOST:$DB_PORT" >&2
  exit 1
fi

# Check required files
if [[ "$REUSE_JOBS" == true ]]; then
  if [[ ! -f agent_profiles.json ]]; then
    echo "ERROR: --reuse-jobs specified but agent_profiles.json not found" >&2; exit 1
  fi
  if [[ ! -d jobs/ ]]; then
    echo "ERROR: --reuse-jobs specified but jobs/ directory not found" >&2; exit 1
  fi
  REUSE_FLAGS="--use-profiles $(pwd)/agent_profiles.json --use-jobs-dir $(pwd)/jobs/"
else
  REUSE_FLAGS=""
fi

# ─── Print configuration ───────────────────────────────────────────
log "============================================================"
log "  Centralized Baseline Scheduler Tests"
log "============================================================"
log "  Mode:              $MODE"
log "  Python:            $PYTHON"
log "  Agents:            $AGENTS"
log "  Jobs:              $JOBS"
log "  Runs per sched:    $RUNS"
log "  Schedulers:        $SCHEDULERS"
log "  Redis:             $DB_HOST:$DB_PORT"
log "  Jobs/interval:     $JOBS_PER_INTERVAL"
log "  Timeout:           ${TIMEOUT}s"
log "  Output base:       $BASE_DIR"
log "  Reuse jobs:        $REUSE_JOBS"
if [[ "$MODE" == "remote" ]]; then
log "  Hosts file:        $AGENT_HOSTS_FILE"
log "  Agents/host:       $AGENTS_PER_HOST"
log "  Remote repo dir:   $REMOTE_REPO_DIR"
log "  Worker timeout:    ${WORKER_TIMEOUT}s"
fi
log "============================================================"

mkdir -p "$BASE_DIR"

# ─── Run experiments ────────────────────────────────────────────────
IFS=',' read -ra SCHED_LIST <<< "$SCHEDULERS"

TOTAL_TESTS=$(( ${#SCHED_LIST[@]} * RUNS ))
COMPLETED=0
FAILED=0
FAIL_LOG="$BASE_DIR/failures.log"
> "$FAIL_LOG"

for SCHEDULER in "${SCHED_LIST[@]}"; do
  SCHED_DIR="$BASE_DIR/$SCHEDULER"
  mkdir -p "$SCHED_DIR"

  log "────────────────────────────────────────────────────────"
  log "  Scheduler: $SCHEDULER  ($RUNS runs)"
  log "────────────────────────────────────────────────────────"

  for RUN_NUM in $(seq 1 "$RUNS"); do
    RUN_DIR="$SCHED_DIR/run-$RUN_NUM"
    COMPLETED=$((COMPLETED + 1))

    log "[$COMPLETED/$TOTAL_TESTS] $SCHEDULER run $RUN_NUM → $RUN_DIR ($MODE)"

    if [[ "$MODE" == "remote" ]]; then
      REMOTE_FLAGS="--agent-hosts-file $AGENT_HOSTS_FILE --agents-per-host $AGENTS_PER_HOST --remote-repo-dir $REMOTE_REPO_DIR --worker-timeout $WORKER_TIMEOUT"
      [[ -n "$SKIP_PREFLIGHT" ]] && REMOTE_FLAGS="$REMOTE_FLAGS $SKIP_PREFLIGHT"
      RUN_SCRIPT="baselines/run_baseline_remote.py"
    else
      REMOTE_FLAGS=""
      RUN_SCRIPT="baselines/run_baseline.py"
    fi

    if "$PYTHON" "$RUN_SCRIPT" \
        --scheduler "$SCHEDULER" \
        --agents "$AGENTS" \
        --jobs "$JOBS" \
        --db-host "$DB_HOST" \
        --db-port "$DB_PORT" \
        --jobs-per-interval "$JOBS_PER_INTERVAL" \
        --run-dir "$RUN_DIR" \
        --timeout "$TIMEOUT" \
        $NO_DTNS \
        $REUSE_FLAGS \
        $REMOTE_FLAGS \
        $DEBUG \
        2>&1 | tee "$RUN_DIR.log"; then
      log "  ✓ $SCHEDULER run $RUN_NUM completed"
    else
      FAILED=$((FAILED + 1))
      log "  ✗ $SCHEDULER run $RUN_NUM FAILED (see $RUN_DIR.log)"
      echo "$SCHEDULER run-$RUN_NUM" >> "$FAIL_LOG"
    fi

    # Brief pause between runs to let Redis settle
    sleep 2
  done
done

# ─── Summary ────────────────────────────────────────────────────────
log ""
log "============================================================"
log "  ALL BASELINE TESTS COMPLETE"
log "============================================================"
log "  Total tests:  $TOTAL_TESTS"
log "  Succeeded:    $((TOTAL_TESTS - FAILED))"
log "  Failed:       $FAILED"
log "  Results in:   $BASE_DIR/"
log "============================================================"

if [[ "$FAILED" -gt 0 ]]; then
  log "  Failed runs:"
  cat "$FAIL_LOG" | while read -r line; do
    log "    - $line"
  done
fi

# ─── List result files ──────────────────────────────────────────────
log ""
log "Result files:"
for SCHEDULER in "${SCHED_LIST[@]}"; do
  SCHED_DIR="$BASE_DIR/$SCHEDULER"
  CSV_COUNT=$(find "$SCHED_DIR" -name "all_jobs.csv" 2>/dev/null | wc -l)
  log "  $SCHEDULER: $CSV_COUNT run(s) with results"
done

log ""
log "To generate comparison plots, first run SWARM+ distributed tests, then:"
log "  $PYTHON plot_comparison.py \\"
log "    --swarm-dir runs/swarm-mesh-30 \\"
log "    --greedy-dir $BASE_DIR/greedy/run-1 \\"
log "    --output-dir $BASE_DIR/comparison"
