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
python -m pytest tests/                    # All tests (currently tests/test_bandit.py)
python -m pytest tests/test_bandit.py -v   # Single test file
```

### Running Experiments
```bash
# Basic local test
python run_test_v2.py --mode local --agent-type resource --agents 20 --topology mesh --jobs 500 --db-host localhost --run-dir runs/test

# Batch runs with statistical analysis
python batch_tests_v2.py --runs 10 --base-out runs/batch --mode local --agent-type resource --agents 20 --topology mesh --jobs 500 --db-host localhost

# LLM agents (requires OPENAI_API_KEY or LLM_BASE_URL for Ollama)
python run_test_v2.py --mode local --agent-type llm --agents 10 --topology mesh --jobs 200 --db-host localhost --run-dir runs/llm

# Remote mode (requires passwordless SSH, agent_hosts.txt with one host per line)
python run_test_v2.py --mode remote --agents 30 --agents-per-host 5 --topology ring --jobs 1000 --db-host 10.0.0.5 --agent-hosts-file agent_hosts.txt --run-dir runs/remote
```

### Job Pipeline
```bash
python generate_configs.py <num_agents> <jobs_per_proposal> <base_config> <output_dir> <topology> <database> <job_cnt>
python job_generator.py --job-count 100 --agent-profile-path agent_profiles.json --output-dir jobs/
python job_distributor.py --redis-host localhost --jobs-dir jobs/ --jobs-per-interval 10
```

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
2. **Consensus Layer** (`swarm/consensus/engine.py`) — Framework-agnostic PBFT-like protocol. Flow: `propose()` → `on_proposal()` (dominance check) → `on_prepare()` → `on_commit()` → quorum triggers `select_job()`. Quorum = `ceil((n+1)/2)`, dynamic based on live agents.
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

### Entry Points

- `main.py` — Single agent startup: `main.py <agent_id> [--agent-type resource|llm|colmena] [--config path] [--debug]`
- `run_test_v2.py` — Orchestrates multi-agent experiments (local/remote modes, dynamic agent addition)
- `batch_tests_v2.py` — Repeated runs with statistical analysis

## Key Configuration (`config_swarm_multi.yml`)

- `job_selection.cost_weights` — CPU/RAM/Disk/GPU weights (should sum to ~1.0)
- `job_selection.selection_threshold_pct` — % above min cost for candidate pool (lower = stricter)
- `runtime.jobs_per_proposal` — Batch size for job proposals
- `runtime.peer_expiry_seconds` — Time before marking agent as stale (default: 300s)
- `runtime.reselection_timeout_s` — Job timeout before reselection (default: 60s)
- `mab.algorithm` — "epsilon_greedy" or "ucb1" for hierarchical delegation
- `llm.provider` — "openai" or "none"; `llm.model` for model selection

## Development Guidelines

**Adding new agent types:**
1. Inherit from `Agent` base class in `swarm/agents/agent_grpc.py`
2. Integrate consensus engine via adapter pattern (see `ResourceAgent` in `resource_agent.py`)
3. Implement `compute_job_cost` and `is_job_feasible` methods

**Tuning selection behavior:**
- Adjust cost weights in `config_swarm_multi.yml` first before changing code
- Consider live penalty functions in `swarm/selection/penalties.py`

**Debugging consensus:**
1. Set `log-level: DEBUG` in config
2. Check `engine.outgoing`/`engine.incoming` proposal containers and `engine.conflicts`
3. Verify quorum via `calculate_quorum()`
4. Check `save_consensus_votes()` output in Redis

**Agent logs:** `<run-dir>/agent-<id>.log`. Dynamic agent logs: `local_agents_initial_start.log`, `local_agents_dynamic_start.log`

## Additional Documentation

- `docs/Architecture.md` — Detailed system architecture and design patterns
- `docs/ROADMAP.md` — Identified improvements and feature roadmap
- `CO_PARENT_USAGE.md` — Multi-parent shared parenting for hierarchical topology
- `HIERARCHICAL_LLM_AGENTS.md` — LLM agents as Level 1 coordinators
- `MAB_README.md` — Multi-Armed Bandit configuration for delegation
- `message_complexity.md` — PBFT message complexity analysis
