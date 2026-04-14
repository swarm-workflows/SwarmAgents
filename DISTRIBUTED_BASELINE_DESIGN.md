# Distributed Baseline Schedulers with Remote Job Execution

## Context

The centralized baseline schedulers (Greedy, Round-Robin, Random) currently run everything in a single process on the swarm host using `SimulatedAgent` objects and `time.sleep()`. SWARM+ distributes real agent processes across 30 VMs. For a fair comparison, the baselines should also execute jobs on remote VMs — only the **scheduling decision** should remain centralized.

## Architecture

```
 Swarm Host                          Remote VMs (agent-1..agent-30)
┌──────────────────────┐
│ Centralized Scheduler│             ┌─────────────────────┐
│  - Polls PENDING     │──Redis───→  │ baseline_worker.py  │ (agent-1)
│  - Assigns jobs      │             │  - Polls READY jobs │
│  - Sets leader_id +  │             │    where leader_id  │
│    state=READY       │             │    == my_agent_id   │
│  - Tracks capacity   │             │  - Executes (sleep) │
│                      │  ←─Redis──  │  - Sets COMPLETE    │
│  - Monitors COMPLETE │             └─────────────────────┘
│    for progress      │             ┌─────────────────────┐
└──────────────────────┘             │ baseline_worker.py  │ (agent-2)
                                     │  ...                │
                                     └─────────────────────┘
                                      × 30 VMs
```

No new state enum needed — we reuse `ObjectState.READY` (same as SWARM+).
No gRPC — pure Redis coordination (same as SWARM+).

## Files Created

### 1. `baselines/baseline_worker.py` (~120 LOC)
Lightweight worker process that runs on each remote VM.

```
Usage: python3.11 baselines/baseline_worker.py --agent-id 5 --db-host <swarm-ip> [--db-port 6379] [--level 0] [--group 0] [--poll-interval 0.5]
```

**Logic:**
- Connects to Redis
- Polls for jobs where `state == READY` AND `leader_id == agent_id` (filter client-side from `get_all_objects(state=READY)`)
- For each found job: call `job.execute()`, save to Redis as COMPLETE
- Track local capacity via `SimulatedAgent` (loaded from `agent_profiles.json`)
- Exit when signaled via a Redis key (`baseline:shutdown`) or SIGTERM

### 2. `baselines/run_baseline_remote.py` (~200 LOC)
Orchestration script that replaces `run_baseline.py` for distributed mode.

```
Usage: python3.11 baselines/run_baseline_remote.py \
    --scheduler greedy --agents 30 --jobs 500 \
    --db-host localhost --agent-hosts-file agent_hosts.txt \
    --run-dir runs/baselines/greedy/run-1 \
    --use-profiles agent_profiles.json --use-jobs-dir jobs/
```

**Steps:**
1. Clean Redis
2. SSH into each agent host, start `baseline_worker.py` in background (via `nohup`)
3. Wait for all workers to register (heartbeat key in Redis)
4. Start `JobDistributor` thread (push PENDING jobs to Redis)
5. Run centralized scheduling loop:
   - Poll PENDING jobs from Redis
   - Apply scheduling strategy (greedy/RR/random) — reuse existing `assign_jobs()` methods
   - For each assigned job: set `leader_id`, `state=READY`, save to Redis
   - Do NOT execute locally — workers handle execution
   - Monitor COMPLETE count for progress
6. Wait for all jobs to complete (or timeout)
7. Signal workers to stop (set `baseline:shutdown` key in Redis)
8. Collect logs from remote hosts via SCP
9. Save results (`all_jobs.csv`, `metrics.json`)

### 3. `baseline-worker-start.sh` (~15 LOC)
Simple wrapper called via SSH on each remote host:
```bash
#!/bin/bash
cd /root/SwarmAgents
nohup python3.11 baselines/baseline_worker.py \
    --agent-id $1 --db-host $2 --db-port ${3:-6379} \
    > baseline-worker-$1.log 2>&1 &
echo $!  # Return PID for tracking
```

### 4. `baseline-worker-stop.sh` (~10 LOC)
Stop workers on all hosts:
```bash
#!/bin/bash
# Reads agent_hosts.txt, SSH into each, kill baseline_worker.py
```

## Files Modified

### 5. `baselines/scheduler.py` — Refactored for reuse
Scheduling loop supports two modes via a `remote` flag:
- **Local mode** (existing): assign + execute locally (current behavior, kept for quick testing)
- **Remote mode** (new): assign only, write to Redis, let workers execute

When `remote=True`:
- The `_execute_on_agent()` method becomes a no-op (workers handle execution)
- The `run()` loop only assigns jobs (sets `leader_id` + `state=READY` in Redis) and monitors `COMPLETE` count from Redis
- `ThreadPoolExecutor` is not used

### 6. `baselines/agent_sim.py` — Added host field
`SimulatedAgent.from_profile()` reads from the config's `grpc.host` field (already present when configs are generated with `--agent-hosts-file`).

## Key Design Decisions

1. **No new ObjectState** — Use existing `READY` state (value 5). Workers poll for READY jobs with matching `leader_id`.
2. **No gRPC between scheduler and workers** — Redis is the sole communication channel (same as SWARM+).
3. **Worker identifies "my" jobs** by filtering `leader_id == agent_id` from Redis results. The scheduler writes `leader_id` when assigning.
4. **Capacity tracking stays in the scheduler** — The scheduler tracks `SimulatedAgent` capacity (same as now). Workers do a secondary capacity check but trust the scheduler.
5. **`agent_profiles.json` already has host info** when generated with `--agent-hosts-file`. We need to regenerate configs with the actual agent hostnames.
6. **Backward compatible** — `run_baseline.py` (local mode) continues to work unchanged.

## Verification

1. **Unit test**: Run `baseline_worker.py` locally, manually insert a READY job in Redis with matching leader_id, verify it executes and marks COMPLETE
2. **Small-scale remote test**: 3 agents on 3 hosts, 20 jobs, greedy scheduler
3. **Full-scale test**: 30 agents on 30 hosts, 500 jobs, all three schedulers
4. **Comparison**: Run SWARM+ with same 500 jobs on same 30 hosts, compare makespan/fairness/latency
