# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

SwarmAgents is a production PBFT-based distributed job scheduling system where agents reach consensus on job assignments using cost-based selection over gRPC, with Redis persistence.

## Prerequisites

```bash
pip install -r requirements.txt
docker run -d -p 6379:6379 redis   # Required for all distributed tests
```

## Common Commands

### Unit Tests
```bash
python -m pytest tests/                    # All tests: test_bandit.py (MAB), test_snow.py (Snow engine), test_gossip.py (gossip)
python -m pytest tests/test_bandit.py -v   # Single test file
```

### Running Experiments
```bash
# Basic local test
python run_test.py --mode local --agent-type resource --agents 20 --topology mesh --jobs 500 --db-host localhost --run-dir runs/test

# Batch runs with statistical analysis
python batch_tests_v2.py --runs 10 --base-out runs/batch --mode local --agent-type resource --agents 20 --topology mesh --jobs 500 --db-host localhost

# LLM agents (requires OPENAI_API_KEY or LLM_BASE_URL for Ollama)
python run_test.py --mode local --agent-type llm --agents 10 --topology mesh --jobs 200 --db-host localhost --run-dir runs/llm

# Remote mode (requires passwordless SSH, agent_hosts.txt with one host per line)
python run_test.py --mode remote --agents 30 --agents-per-host 5 --topology ring --jobs 1000 --db-host 10.0.0.5 --agent-hosts-file agent_hosts.txt --run-dir runs/remote
```

**Remote test setup (deployment environment):**
- `ssh swarm` connects to the **database node** (runs Redis and orchestrates the run). This is the only host reached from the laptop; every other node in the topology is reached *from* it.
- On that node, become root with `sudo su -` before doing anything else: the code lives at `/root/SwarmAgents` and the passwordless SSH keys to the agent nodes belong to root, so neither is reachable as the login user.
- As root, the database node has **passwordless SSH to every other node in the topology**, which host the agent processes. All hostnames are pre-resolved in `/etc/hosts`, so use them directly — no IPs needed. The current slice is **`agent-1` … `agent-92`** (contiguous, verified 2026-09-09), each with a paired `agent-N-mon` monitoring host on a separate subnet. Earlier revisions of this file said `agent-40`; check `/etc/hosts` rather than trusting a written range.
- Agent ids map to sites in **contiguous blocks sharing a subnet**, so a site outage removes a solid id range: `agent-10`–`agent-18` is PSC, `agent-68`–`agent-74` is AMST. **Sweep reachability before sizing a run.** As of 2026-09-09 18:40 UTC, AMST is back and PSC is still down: **83 of 92 up**, which is still short of the ~90 VMs every rung above Hier-30 needs (90 at 1/VM, 180 at 2/VM, 270 at 3/VM) — PSC's 9 nodes are the whole remaining gap.
  ```bash
  seq 1 92 | xargs -P 40 -I{} bash -c '
    out=$(ssh -o BatchMode=yes -o ConnectTimeout=6 -o StrictHostKeyChecking=accept-new agent-{} hostname 2>&1)
    if [ $? -eq 0 ]; then echo "UP agent-{}";
    elif echo "$out" | grep -qi "host key"; then echo "KEY agent-{}";
    else echo "DOWN agent-{}"; fi'
  ```
  **`StrictHostKeyChecking=accept-new` is not optional here.** A host that returns from a rebuild has a new host key, and plain `ssh` under `BatchMode` then fails with "Host key verification failed" — indistinguishable from a timeout in a pass/fail sweep. That is exactly how AMST was recorded as down while all 7 nodes were up and answering. `accept-new` trusts a first key but still refuses a *changed* one, so a genuinely swapped host reports `KEY` instead of passing silently (which `=no` would do). `run_test.py` and `stop_agents_v2.sh` pass `StrictHostKeyChecking=no` on every hop, so runs themselves were never affected — only the sweep.
- `agent_hosts.txt` lists all agent hostnames, one per line. It is **deleted by `cleanup_between_runs`** unless it is the resolved `--agent-hosts-file`, so it will often be absent on the database node and must be regenerated before a remote run.
- Remote-mode tests are launched from this node with `--db-host database`, e.g.:
```bash
python run_test.py --mode remote --agents 40 --agents-per-host 1 --topology ring --jobs 1000 \
    --db-host database --agent-hosts-file agent_hosts.txt --run-dir runs/remote-test
```

### Job Pipeline
```bash
python generate_configs.py <num_agents> <jobs_per_proposal> <base_config> <output_dir> <topology> <database> <job_cnt>
# Reproducible, comparable fleets: --seed pins the draw, --master-fleet-size makes every smaller
# fleet a strict prefix of the largest rung (flavours are percentages of fleet size, so without it
# the same seed gives agent i a different machine at each size). Generate from a clean state —
# an existing agent_dtns.json is reused through a different RNG path and warns when it is.
python generate_configs.py 30 10 ./config_swarm_multi.yml configs mesh localhost 600 --dtns --seed 42 --master-fleet-size 270
# make_agent_hosts.py builds the hosts file for a remote run. Placement is the ORDER of this
# file (agents are assigned to hosts in contiguous blocks), and agent ids map to sites in
# contiguous blocks, so numeric order clusters each hierarchical group at one site: measured on
# the live slice, 65/79 adjacent pairs same-site sequentially vs 0/79 interleaved. Default is
# interleaved; --order sequential is for a deliberate site-outage test. It also probes with
# StrictHostKeyChecking=accept-new, so a node returning from a rebuild is not misread as down.
python make_agent_hosts.py --count 80 --out agent_hosts.txt --sites-out agent_sites.txt
# Hierarchical only: --groups-per-coordinator G gives each Level-1 coordinator G child groups
# exclusively (default 1). Required for ANY delegation measurement — at 1 a coordinator has a
# single candidate, so the MAB and delegation.policy=llm are both inert. Freed coordinator slots
# become Level-0 agents, so the fleet size is unchanged; two-level hierarchies only.
python generate_configs.py 90 10 ./config_swarm_multi.yml configs hierarchical localhost 600 --seed 42 --groups-per-coordinator 3
# Supported hierarchical fleet sizes: 30, 60, 80, 90, 100, 110, 120, 250, 270, 990, 1000.
# Hier-80 (8 groups of 9 + 8 coordinators) is the stand-in for Hier-90 while PSC is down: it
# keeps group_size 9, the same group shape as Hier-90/270, and fits 83 VMs at 1 agent/VM.
# Anything
# else is refused — a size whose topology does not total the request used to have the overflow
# silently dropped, which for Hier-90 meant a fleet with no coordinators at all.
python job_generator.py --job-count 100 --agent-profile-path agent_profiles.json --output-dir jobs/
python job_distributor.py --redis-host localhost --jobs-dir jobs/ --jobs-per-interval 10
```

**Quantum/hybrid jobs** (see `docs/QUANTUM_HYBRID_DESIGN.md`): `generate_configs.py --quantum-agents-pct 0.25` gives a subset of agents a `quantum_backend`; `job_generator.py --quantum-fraction 0.2 --hybrid-fraction 0.1` emits jobs with a quantum component (one-shot offload or variational classical<->quantum loop). `run_test.py` accepts the same three flags and forwards them. Models in `swarm/models/quantum.py` (`QuantumSpec`/`QuantumBackend`); `Capacities.qubits` carries the allocatable qubit count; feasibility/cost wiring in `resource_agent.py` (`_quantum_feasible`, `qpu` cost weight — default 0.0, classical runs unchanged).

**Phase 2 — data-triggered split scheduling** (`--split-hybrid` on run_test/job_distributor): hybrid jobs decompose into a quantum sub-job (`<id>-q`, measurement producer) and a classical sub-job (`<id>-c`, stream consumer) placed on different agents. `swarm/quantum/measurement_layer.py` = Redis-streams measurement layer; `swarm/quantum/split.py` = split/post-process builders + `split_comm_penalty`. Data predicates (`Job.data_predicate`) gate selection until snapshots exist; consumers keep partial state in `Job.state_data` (persisted per update); one-shot quantum jobs with `post_process` push `<id>-post` classical jobs into the pool (`[POOL_PUSH]` in logs). Config under `quantum:` (comm_penalty_factor, consumer_timeout_s). Note: split + pushed jobs make completed counts exceed submitted counts. Tests: `tests/test_quantum_phase2.py`.

### Utilities
```bash
python dump_db.py --host localhost --type redis          # Inspect Redis state
python kill_agents.py --mode local --count 5 --random    # Simulate agent failures
```

### Visualizations
```bash
# Single-run analysis (latency, conflicts, failures, loads, hierarchical)
python plot_latency_jobs.py --output_dir runs/test --agents 30 --db_host localhost [--hierarchical]

# Multi-run statistical comparison across topologies/scales
python plot_multi_run_results.py --base-dir runs/single-site --output-dir runs/single-site/plots

# Scheduler comparison (SWARM vs baselines)
python plot_comparison.py --swarm-dir runs/swarm --greedy-dir runs/greedy --output-dir runs/comparison

# MAB/hierarchical delegation analysis
python plot_mab_results.py --db-host localhost --output-dir runs/mab-test
```

All plotting lives in the `plotting/` package. Top-level scripts are thin CLI wrappers:

| Module | CLI Wrapper | Purpose |
|--------|-------------|---------|
| `plotting/single_run.py` | `plot_latency_jobs.py` | Single-run analysis |
| `plotting/multi_run.py` | `plot_multi_run_results.py` | Multi-run statistical comparison |
| `plotting/comparison.py` | `plot_comparison.py` | Scheduler comparison (SWARM vs baselines) |
| `plotting/mab.py` | `plot_mab_results.py` | MAB learning curves and delegation |
| `plotting/data.py` | — | Shared data loading/saving (Redis, CSV, JSON) |
| `plotting/stats.py` | — | Shared statistics helpers (Jain's fairness, safe aggregations) |

### Protobuf Compilation (after modifying `swarm/comm/consensus.proto`)
```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. swarm/comm/consensus.proto
```

## Architecture

### Five-Layer Design

1. **Agent Layer** (`swarm/agents/`) — `ResourceAgent` (rule-based), `LLMAgent` (LLM-enhanced), `ColmenaAgent` (Colmena workflow integration). Base class in `agent_grpc.py`.
2. **Consensus Layer** (`swarm/consensus/engine.py`) — Framework-agnostic PBFT-like protocol. Flow: `propose()` → `on_proposal()` (dominance check) → `on_prepare()` → `on_commit()` → quorum triggers `select_job()`. Quorum = `ceil((n+1)/2)`, dynamic based on live agents. An alternative Snow/Avalanche-style engine (`swarm/consensus/gossip_engine.py`, `GossipConsensusEngine`) is drop-in compatible and selected via `consensus.protocol: snow`; it replaces the 3-phase broadcast with k-peer sampling and finalizes exactly-once via Redis `SET NX` (`repository.try_claim_assignment`).
3. **Selection Layer** (`swarm/selection/engine.py`) — Cost matrix computation with LRU caching. `penalties.py` provides live (non-cached) penalty helpers. Selection thresholded via `selection_threshold_pct`.
4. **Communication Layer** (`swarm/comm/`) — gRPC server/client/transport defined in `consensus.proto`.
5. **Data Layer** (`swarm/database/repository.py`) — Redis persistence for jobs, agents, and consensus state with secondary indices by job state.

### Key Integration Pattern

Consensus and selection engines are **decoupled** from agents via adapter classes (`_HostAdapter`, `_TransportAdapter`, `_RouteAdapter`) in `resource_agent.py`. Engines remain framework-agnostic; agents handle all side effects. When modifying consensus logic, changes to `ConsensusEngine` must stay framework-agnostic — side effects belong in adapters.

### Supporting Modules

- `swarm/topology/topology.py` — Ring, Mesh, Star, Hierarchical topologies with neighbor/routing logic
- `swarm/models/` — `job.py`, `capacities.py`, `agent_info.py`, `data_node.py`, `role.py`
- `swarm/rl/` — Multi-Armed Bandit (Epsilon-Greedy, UCB1) for hierarchical delegation
- `swarm/queue/` — Job queue implementations
- `swarm/utils/` — Metrics tracking, thread-safe data structures
- `swarm/membership/swim.py` — SWIM failure detection (Phase 1 of the gossip-consensus migration)
- `swarm/gossip/disseminator.py` — Epidemic state dissemination (Phase 2)
- `swarm/consensus/gossip_engine.py` — Snow/Avalanche consensus engine (Phase 3). See `docs/GOSSIP_CONSENSUS_DESIGN.md`

### Entry Points

- `main.py` — Single agent startup: `main.py <agent_id> [--agent-type resource|llm|colmena] [--config path] [--debug]`
- `run_test.py` — Orchestrates multi-agent experiments (local/remote modes, dynamic agent addition)
- `batch_tests_v2.py` — Repeated runs with statistical analysis

## Key Configuration (`config_swarm_multi.yml`)

- `job_selection.cost_weights` — CPU/RAM/Disk/GPU weights (should sum to ~1.0)
- `job_selection.selection_threshold_pct` — % above min cost for candidate pool (lower = stricter)
- `runtime.jobs_per_proposal` — Batch size for job proposals
- `runtime.peer_expiry_seconds` — Staleness filter for peers read from Redis; 300s. Not the failure detector (heartbeat/SWIM are). The shipped config used to define this key twice, so the effective value was silently 45s; config files are now loaded through `swarm/utils/yaml_strict.py`, which raises on a duplicate key
- `runtime.wall_time_scale` / `wall_time_min_s` / `wall_time_max_s` — Job execution simulation: the simulated sleep is `clamp(wall_time * scale, min, max)`, shipped as scale 1.0 with a 120s cap. `Job.execute()` previously slept a flat 1s for every job, which made makespan, throughput and utilisation meaningless. `scale: 0` restores that legacy behaviour and logs a warning; do not report makespan from such a run
- `runtime.reselection_timeout_s` — Job timeout before reselection (default: 60s)
- `runtime.shutdown_drain_timeout_s` — Teardown's budget for jobs still executing, 20s. `Agent.stop()` runs teardown once and does not return until it has finished, because the SIGTERM handler `os._exit`s as soon as it does (the executor's workers are non-daemon, so `sys.exit` would wait out a whole job); the stop script touches the shutdown flag and signals in one breath, so the periodic thread is usually already inside `save_results` when the signal lands. Metrics are saved **before** this drain and again after it, which is the fix for a defect that made every run's `metrics.json` unreliable: `on_shutdown` used to call `executor.shutdown(wait=True)` first, so an agent holding a job wrote its metrics up to `wall_time_max_s` later — after the runner had already read Redis and plotted. Both P0-1 smoke runs (`smoke-g2-bandit`, `smoke-g2-llm`) reported one agent out of thirty, and the second run's file was 15/16 leftovers from the first. Per-agent load, utilisation, fairness, and the MAB/delegation counts all come from that file
- `mab.algorithm` — "epsilon_greedy" or "ucb1" for hierarchical delegation
- `delegation.policy` (`bandit`/`llm`) + `delegation.top_k` — Who picks the child group a coordinator delegates to. `bandit` (default) is the pre-existing behaviour: the bandit if `mab.enabled`, else every capable group. `llm` puts the model in that seat — it ranks the capable groups from the same `GroupSnapshot` view the contextual bandit sees (headroom, inflight, recent failure and timeout rates) and the top `top_k` are used. Until this existed, an "LLM coordinator" meant a coordinator whose *bid* was LLM-scored; the routing decision the hierarchy actually makes was never the model's. Any failure falls back to `bandit` for that job, and a decision that cannot change the outcome (one candidate, or `top_k` covering them all) spends no inference. A failed decision keeps the configured fan-out (a fallback that widened it would reward failure with the whole subtree) and, with no bandit to ask, picks at random rather than by list order. Only groups the model actually named are credited to it; a short ranking has its tail filled from the candidate order and counted as `filled`. `mean_s` charges every call that reached the model, timeouts included. **Requires a coordinator that actively leads more than one child group, which the shipped hierarchical topology cannot currently produce.** Each Level-1 coordinator is assigned one group, and `--co-parents` does not fix it: leadership goes to the lowest-ID live co-parent, so it concentrates rather than spreads — measured on Hier-30 (5 coordinators, all alive), groups actively led are `[1,1,1,1,1]` at K=1, `[0,1,1,1,2]` at K=2 and `[0,0,0,0,5]` at K=5. With one candidate neither this nor the bandit ever chooses and every delegation is `trivial`. A fan-out covering every candidate is inert the same way, for a config reason rather than a topology one — the key that sets it is `delegation.top_k` under `policy: llm` and `mab.top_k` under `bandit`, which ignores `delegation.top_k` and says so; coordinators log `[DELEGATION] ... can never choose` for both. Pinned by `tests/test_delegation.py`. **Two open limits:** the call is synchronous on the scheduling thread, so a coordinator's delegation rate is capped at 1/latency; and `llm.timeout_seconds` is a per-request timeout, so retries can stack several into one decision — there is no wall-clock deadline yet (P0-3). `top_k: 0` follows `mab.top_k`. Decisions are audited to `llm_score:delegate:*` and counted in `metrics.json` as `llm_delegations`/`llm_delegation_stats`, separately from `mab_selections` — with `llm` the bandit still receives outcomes (with the LLM's selection-time context, so its model stays honest), and a shared counter would read as if it had been choosing
- `llm.provider` — "openai" or "none"; `llm.model` for model selection
- `llm.bid_pacing` (`none`/`fallback_parity`/`uniform`) + `bid_pacing_target_s` / `bid_pacing_quantile` / `bid_pacing_max_s` — Placement is decided by when an agent bids, not what it bids: a failed bid skips inference and returns in ~0s, so 8 LLM-blind agents took 280 of 300 jobs. `fallback_parity` holds a fallback until it has cost what a real bid costs; `uniform` holds every bid to one target so arrival time carries no information. The wait tops a bid up to the target rather than adding to it. Target 0 self-calibrates from observed latencies, falling back to `bid_pacing_bootstrap_s` (default `llm.timeout_seconds`) — an agent whose LLM never succeeds has no observations and would otherwise never pace, which is the whole faulted population. Off by default
- `llm.score_scale` / `tie_break_with_analytic` / `tie_break_ref_cost` — Bid elicitation. A 0-100 rating ties 59-92% of the time, so the bid cannot order agents; `score_scale` widens the range the model is asked for (the bound is in the JSON schema too, since Ollama's structured output drives from it), and `tie_break_with_analytic` orders surviving ties by the analytic cost, quantising the rating to the requested grid and capping the term at half a grid step, so it can never move a rating past a neighbouring one (the cap alone is not enough — scores are floats). Both default to the measured baseline. `[STATS]` reports bid count, distinct values and modal share
- `llm.cost_cache_ttl_s` / `cost_cache_max` / `snow_cost_fallback` — How an LLM agent answers an inbound consensus query. It cannot call the model there (the inbound consumer thread must not block), so it answers from the last verdict it computed for that job. Before this, it answered with the analytic cost, so the LLM priced jobs when proposing and the analytic model priced them when voting. `snow_cost_fallback: yield` (default) abstains on a cache miss; `analytic` is the ablation arm
- `consensus.protocol` — "pbft" (default) or "snow"; Snow tuning under `consensus.snow.{k,alpha,beta,max_rounds,round_timeout_ms,tick_interval_ms}`
- `failure_detection.protocol` — "heartbeat" (default) or "swim" (runs alongside heartbeat)
- `gossip.enabled` / `gossip.fanout` / `gossip.period_ms` / `gossip.state_ttl_s` — Epidemic state dissemination

## Development Guidelines

**Adding new agent types:**
1. Inherit from `Agent` base class in `swarm/agents/agent_grpc.py`
2. Integrate consensus engine via adapter pattern (see `ResourceAgent` in `resource_agent.py`)
3. Implement `compute_job_cost` and `is_job_feasible` methods

**Tuning selection behavior:**
- Adjust cost weights in `config_swarm_multi.yml` first before changing code
- Consider live penalty functions in `swarm/selection/penalties.py`

**Debugging consensus (PBFT):**
1. Set `log-level: DEBUG` in config
2. Check `engine.outgoing`/`engine.incoming` proposal containers and `engine.conflicts`
3. Verify quorum via `calculate_quorum()`
4. Check `save_consensus_votes()` output in Redis

**Debugging consensus (Snow, `consensus.protocol: snow`):**
1. Confirm the engine started: grep logs for `[snow] consensus engine started`; look for `[SNOW_LEADER]`/`[SNOW_PART]` finalization lines
2. `engine.conflicts[job_id]` counts rounds that failed the α-threshold; persistent growth means no candidate is dominating
3. Exactly-once is enforced by `repository.try_claim_assignment` (Redis `SET NX`) — inspect assignment keys with `dump_db.py` to confirm no double-assignment
4. Tune `consensus.snow.{k,alpha,beta}`; enable `gossip` and `failure_detection: swim` so peer cost estimates and the live-peer sample stay fresh

**Agent logs:** `<run-dir>/agent-<id>.log`. Dynamic agent logs: `local_agents_initial_start.log`, `local_agents_dynamic_start.log`

**Run identity and metrics completeness (every run):** `run_test.py` mints a `run_id`, exports it as `SWARM_RUN_ID` (inherited locally, re-exported over ssh for remote agents), and records it in `<run-dir>/run_meta.json`. Agents stamp it on the `metrics:<id>` payload they write to Redis, and the plotting step is passed `--metrics-run-id` so a payload from another run is dropped with a warning instead of being read as this run's — an agent that outlives its run writes its metrics whenever it is finally killed, which can be after the next run has flushed Redis. After stopping the agents the runner waits (`--metrics-wait-seconds`, 120s) for every launched agent id to report, then **exits 3** if any are missing and writes `<run-dir>/metrics_shortfall.json`; `evaluation/collect.py` carries that as `metrics_complete` / `agents_missing_metrics` columns. A failure-injection run that SIGKILLs agents must declare it — `--expect-silent-agents 3,7` (preferred: it also checks *which* agents were silent, so a kill that hit the wrong agent still fails the run, and it warns when the intended kills did not take) or `--allow-missing-metrics N` when the ids are only known after launch, as in `run_hier110_resilience_tests.sh`. A SIGTERM'd agent still flushes and does not need declaring. **Known limitation, dynamic mode:** `swarm-multi-start.sh` pkills every `main.py` on a host before launching, so dynamic agents that wrap onto an occupied host kill the agent already there; its early metrics carry this run's `run_id`, so the completeness gate passes for a fleet that never existed. Give dynamic agents their own hosts until that is fixed. `stop_agents_v2.sh` now waits for each agent process to actually exit (`--drain-timeout`, 45s) before SIGKILL, fans out across hosts in parallel, and exits non-zero if any host could not confirm; `run_test.py` also reaps leftovers **before** flushing Redis at startup.

## Additional Documentation

- `docs/ARCHITECTURE.md` — System architecture, five-layer design, and adapter patterns
- `docs/ROADMAP.md` — Identified improvements and feature roadmap
- `docs/CO_PARENT_USAGE.md` — Multi-parent shared parenting for hierarchical topology
- `docs/HIERARCHICAL_LLM_AGENTS.md` — LLM agents as Level 1 coordinators
- `docs/MAB_README.md` — Multi-Armed Bandit configuration for delegation
- `docs/CONTEXTUAL_BANDIT_DESIGN.md` — Contextual bandit (LinUCB) for delegation, all 4 phases done. Select via `mab.algorithm: linucb`. Deployment-validated (design doc section 8, tooling in `evaluation/scenario_{a,b,c}/`): A — LinUCB 73.4% vs eps 61.9% success; B — discount 0.98 avoids post-flip crash; C — instant vs never rejoin re-adoption, plus dead-group dog-piling / poisoned-window gaps identified. Offline plots: `plot_mab_results.py --dump <redis-dump.json> --events <file>`
- `docs/COMPLEXITY.md` — PBFT message complexity analysis (mesh and hierarchical)
- `docs/GOSSIP_CONSENSUS_DESIGN.md` — Gossip-based consensus stack (SWIM + gossip + Snow). Phases 1-3 implemented, wired, and unit-tested; Phase 4 (hybrid hierarchical) and at-scale evaluation pending
- `docs/DISTRIBUTED_BASELINE_DESIGN.md` — Distributed baseline scheduler design
- `docs/QUANTUM_HYBRID_DESIGN.md` — Hybrid quantum-classical job support (classical/quantum/hybrid taxonomy, QuantumSpec/QuantumBackend models, feasibility/cost integration, Phase 2 split co-scheduling, Phase 3 roadmap)
- `docs/QUANTUM_HYBRID_IMPLEMENTATION.md` — Code-level walkthrough of the quantum support: module map, data-flow, cost formula term by term, cache-signature correctness, measurement layer internals, execution paths, end-to-end job trace, known gotchas
- `docs/ROADMAP.md` — Feature roadmap and identified improvements
