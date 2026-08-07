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
- `ssh swarm` connects to the **database node** (runs Redis and orchestrates the run).
- On that node, switch to the **root** user; the code lives at `/root/SwarmAgents`.
- The database node is configured for **passwordless SSH to `agent-1` … `agent-40`**, which host the agent processes. All hostnames (`database`, `agent-1` … `agent-40`) are pre-resolved in `/etc/hosts`, so use them directly — no IPs needed.
- `agent_hosts.txt` simply lists all agent hostnames, one per line (`agent-1` … `agent-40`).
- Remote-mode tests are launched from this node with `--db-host database`, e.g.:
```bash
python run_test.py --mode remote --agents 40 --agents-per-host 1 --topology ring --jobs 1000 \
    --db-host database --agent-hosts-file agent_hosts.txt --run-dir runs/remote-test
```

### Job Pipeline
```bash
python generate_configs.py <num_agents> <jobs_per_proposal> <base_config> <output_dir> <topology> <database> <job_cnt>
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
- `runtime.peer_expiry_seconds` — Time before marking agent as stale (default: 300s)
- `runtime.reselection_timeout_s` — Job timeout before reselection (default: 60s)
- `mab.algorithm` — "epsilon_greedy" or "ucb1" for hierarchical delegation
- `llm.provider` — "openai" or "none"; `llm.model` for model selection
- `consensus.protocol` — "pbft", "snow", or "hybrid". **The shipped config ships `snow`**, tuned for a 20-agent flat mesh (`k: 10`, `alpha: 0.7`, `beta: 6`, `round_timeout_ms: 300`, `send_workers`/`max_inflight: 16`). Set `pbft` for small clusters where its 3-phase latency wins, or scale `k`/`beta` back toward 15-20 above ~100 agents. Note the *code* fallback when the key is absent is still `pbft`. Full tuning under `consensus.snow.{k,alpha,beta,max_rounds,round_timeout_ms,tick_interval_ms,local_sample_frac,send_workers,send_timeout_ms,max_inflight}`
- `failure_detection.protocol` — "heartbeat" or "swim". **The shipped config ships `swim`** (it keeps Snow's live-peer sample fresh); heartbeat still runs alongside and remains authoritative for job reassignment. Code fallback when absent is `heartbeat`
- `gossip.enabled` / `gossip.fanout` / `gossip.period_ms` / `gossip.state_ttl_s` — Epidemic state dissemination. **The shipped config ships `enabled: true`** so peer `load` reaches Snow cost estimates via the epidemic cache; this also switches the neighbor refresh from per-tick Redis to every 5s (`runtime.neighbor_refresh_full_s`)

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
