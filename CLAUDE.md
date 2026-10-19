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
.venv/bin/python -m pytest tests/          # ALL tests. Use the venv: four files import pydantic_ai
                                           # (test_delegation, test_bid_pacing, test_elicitation,
                                           # test_repository) and under the system python they fail
                                           # to COLLECT — the run reports "9 errors" and passes over
                                           # ~200 tests. On 2026-09-18 that hid eight regressions from
                                           # a default change for a whole day.
.venv/bin/python -m pytest tests/test_bandit.py -v   # Single test file
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
- `ssh swarm` reaches the **database node** only (Redis + orchestration); every other node is reached *from* it. Become root first (`sudo su -`): the code is at `/root/SwarmAgents` and the passwordless SSH keys to the agent nodes are root's.
- Agent hosts are `agent-1` … `agent-92` (contiguous, verified 2026-09-09), each with an `agent-N-mon` host; all resolved in `/etc/hosts`. Check `/etc/hosts`, not a written range. Ids map to sites in contiguous blocks (`agent-10`–`18` PSC, `agent-68`–`74` AMST), so a site outage removes a solid id range. **Sweep reachability before sizing a run** with the loop below, and keep `StrictHostKeyChecking=accept-new`: a rebuilt host has a new key and plain `BatchMode` ssh then reads as DOWN (that is how AMST was recorded down while up); `accept-new` reports a *changed* key as `KEY`.
  ```bash
  seq 1 92 | xargs -P 40 -I{} bash -c '
    out=$(ssh -o BatchMode=yes -o ConnectTimeout=6 -o StrictHostKeyChecking=accept-new agent-{} hostname 2>&1)
    if [ $? -eq 0 ]; then echo "UP agent-{}";
    elif echo "$out" | grep -qi "host key"; then echo "KEY agent-{}";
    else echo "DOWN agent-{}"; fi'
  ```
- **Clocks: check before any run that compares two nodes.** Stock chrony points at unreachable pools; on 2026-09-14, 66 of 92 nodes were free-running 0.4–1.1 s apart and `chronyc tracking` still looked fine. The reliable indicator is `timedatectl`'s `System clock synchronized`. Repair with `./fix_slice_clocks.sh` as root on the database node (`--check` changes nothing). Details: `docs/ENGINEERING_NOTES.md` §1.
- **Level-1 agents default to `resource`** (since 2026-09-18). It is a separate key from `--agent-type`: an all-LLM hierarchy needs `--hierarchical-level1-agent-type llm`, and every host that might get a coordinator then needs `OPENAI_API_KEY`. `tests/test_fleet_sizing.py::TestCoordinatorTypeDefault`.
- `agent_hosts.txt` (one host per line) is **deleted by `cleanup_between_runs`** unless it is the resolved `--agent-hosts-file`; regenerate it before a remote run. Launch from the node with `--db-host database`:
```bash
python run_test.py --mode remote --agents 40 --agents-per-host 1 --topology ring --jobs 1000 \
    --db-host database --agent-hosts-file agent_hosts.txt --run-dir runs/remote-test
```

### Job Pipeline
```bash
python generate_configs.py <num_agents> <jobs_per_proposal> <base_config> <output_dir> <topology> <database> <job_cnt>
# Comparable fleets: --seed pins the draw, --master-fleet-size makes every smaller fleet a prefix of the largest.
python generate_configs.py 30 10 ./config_swarm_multi.yml configs mesh localhost 600 --dtns --seed 42 --master-fleet-size 270
# Identical workload across a ladder: --job-target-agents K, K = the smallest rung's LEAF count (Hier-30: 27).
python generate_configs.py 90 10 ./config_swarm_multi.yml configs hierarchical localhost 600 --dtns --seed 42 --master-fleet-size 270 --job-target-agents 27
# Hosts file for a remote run; its ORDER is the placement. Default interleaved (--order sequential clusters groups by site).
python make_agent_hosts.py --count 80 --out agent_hosts.txt --sites-out agent_sites.txt
# Hierarchical: --groups-per-coordinator G (DEFAULT 2 since 2026-09-18; at 1 every delegation is `trivial`).
python generate_configs.py 90 10 ./config_swarm_multi.yml configs hierarchical localhost 600 --seed 42 --groups-per-coordinator 3
# Supported hierarchical sizes: 30, 60, 80, 90, 100, 110, 120, 250, 270, 990, 1000 (Hier-80 stands in for Hier-90 on 83 VMs). Others are refused.
python job_generator.py --job-count 100 --agent-profile-path agent_profiles.json --output-dir jobs/
python job_distributor.py --redis-host localhost --jobs-dir jobs/ --jobs-per-interval 10
```
Rules behind these flags (prefix fleets, coordinator numbering, fan-out vs `--co-parents`, three-level fleets): `docs/ENGINEERING_NOTES.md` §2.

### Workflows, real execution and staging (full text: `docs/ENGINEERING_NOTES.md` §3)
- **DAG gating is ON by default** (`run_test.py --pegasus-dag-gating`, `--no-pegasus-dag-gating` for an independent replay). The converter's `--dag-gating` emits per-job `data_predicate`s from file edges; it forces `--data-nodes per-file` and the converter defaults are `per-file` + `--dtn-scope job` since 2026-10-06. `--dtn-names dtn1,…` is **non-optional** for an executing corpus. Check `conversion_summary.json` `dag.edges`/`dag.roots`. Completion and output publication are **one Redis write**; the readiness registry is run-scoped. Both selection loops gate on the predicate. Tests: `tests/test_workflow_dag.py`.
- **Execution** (`runtime.execution.mode: real`, default `simulate`; unknown raises) runs only jobs carrying an `execution` block; the real exit status overrides `should_fail`; **a refusal fails the job, never falls back to the sleep**. `ExecutionSpec.refusal_reason()` is the single definition of why a job cannot run (`arguments: None` ≠ `[]`; imageless container; `.sif` on a docker-only host; relative path with no root). `runtime.execution.bundle` points at a converter bundle with a sha256 manifest; `roots.{code,inputs,images}` for hand-authored jobs. Work dir is shared per run; inputs are staged never-overwrite and atomically; the job env drops the agent's secrets. Tests: `tests/test_real_execution.py`, `tests/test_workflow_execution.py`. Apptainer 1.5.3 is on all 92 agents (`setup_apptainer.sh`).
- **Staging** (`runtime.execution.staging.enabled`, shipped **false**; the documented real-workflow path turns it **on**) moves produced files agent-to-agent over a second gRPC port on the consensus host, with a location registry written in the completion's MULTI, eager stage-out to `staging_site.py` before a name is published, `(run, name)` keyed stores, `SWARM_STAGING_TOKEN` auth, and **refusal rather than fallback** on any missing lookup. Requires `SWARM_RUN_ID` (minted by `run_test.py`) and `protobuf>=6.31.1`; re-check that pin after any protoc run. Design: `docs/STAGING_DESIGN.md`; tests: `tests/test_staging.py`.
- `setup_nfs_workflow.sh` is scaffolding only: no published number comes from a shared-mount run. `--check` and setup both exit non-zero on a partial fleet.
- **Quantum/hybrid jobs** (`docs/QUANTUM_HYBRID_DESIGN.md`): `--quantum-agents-pct`, `--quantum-fraction`, `--hybrid-fraction`; `--split-hybrid` splits hybrid jobs into `<id>-q`/`<id>-c` sub-jobs co-scheduled via Redis streams (`swarm/quantum/`), so completed counts exceed submitted. Tests: `tests/test_quantum_phase2.py`.

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

### Defects found by the campaigns and what they changed (full text: `docs/ENGINEERING_NOTES.md` §4)
Each of these made earlier numbers non-citable; read the section before quoting a result from before its date.
- **2026-10-07** Snow claim sweep ran jobs twice on one agent (`duplicate_leader_refusals`); analytic coordinators saw completions only at `delegation_timeout_s`.
- **2026-10-09** PBFT double finalization in one tier: `select_job` persists READY under a not-decided-elsewhere precondition (`lost_leader_races`). Under PBFT both sides of a partition can finalize; say "finalizes twice, executes once".
- **2026-10-10** A reset election is reset everywhere: restarts are persisted (`restarts_persisted`, background writer, never the heartbeat thread), `_forget_decided` clears the local winner, Snow instances are cancelled with a per-job election generation and lock. `tests/test_restart_persisted.py`, `tests/test_election_reset.py`.
- **2026-10-12** One job, one decision: a PBFT COMMIT is binding (`_committed_to`, `commit_lock_holds`, `_resolve_split` by `live_voter_ids`); a decision nobody acts on is re-elected after `runtime.decided_unpersisted_grace_s` (60 s); the Snow initiator re-prices every round (the livelock behind every earlier Snow/Hybrid E1′ number). `tests/test_split_decision.py`.
- **2026-10-12/13** The inbound consumer never waits: `get_object` reports missing from any non-blocking thread (inbound, periodic, fetcher) and a background fetcher replays a **per-job** stash; the failure detector never judges from a stale view; SWIM acks are async; `runtime.fanout_workers` 64 / `fanout_queue` 2048 (16/128 dropped 49k sends). Read `dropped_by_type` beside any message count. `tests/test_inbound_nonblocking.py`.
- **2026-10-12** The shared LinUCB model herded ~90 % of jobs onto one group: `mab.linucb.model: shared|disjoint` and `mab.load_slack`, both off by default; E1′ runs shared vs per-group as scenarios. `tests/test_bandit_herding.py`.
- **2026-10-14** Snow on a far host: late decided-hints finalize (`late_decided_hints`), `send_workers` 64. Job arrival modes: `--arrival poisson --arrival-rate/--arrival-schedule`, seeded, `arrivals.json` per run; `batch` stays default. PBFT: a quorum of commits is a decision and the certificate is resent to the leader (`certificate_finalizes`, `certificates_resent`). `tests/test_commit_certificate.py`.

### Campaign tooling (full text: `docs/ENGINEERING_NOTES.md` §5)
- `--all-dtns` gives every agent every pool DTN name (hierarchical fleets now get DTNs; `--no-dtns` opts out); `--base-config <file>` is staged into the run dir as `config_swarm_multi.yml`.
- `campaign.py <campaign.yml>` runs cells × repeats, resumes from `campaign_state.json`, gates each repeat on ssh/Redis/clocks, classifies attempts (`ok`, `ok_on_cap`, `refused` exit 2 stops the campaign, `metrics_shortfall` 3, `unmeasurable` 4, `timeout`, `failed` retried as `.attempt<k>`), and never lets a run outlive its driver. Run it under tmux/nohup. Example `campaigns/example.yml`.
- `partition.py run` cuts agent↔agent traffic between two site groups with Redis reachable from both, verifies cut and heal on every host, always heals (dead-man timer), exits 3 if the cut was not held as planned; runs as a cell's `companion:`.
- `rtt_matrix.py` measures the host RTT matrix per remote run into `rtt_matrix.json`; `evaluation/rtt.py` bins jobs (lan/regional/continental/transatlantic) and never guesses an unmeasured pair.
- Baselines: `baselines/run_sparrow.py` (batch sampling + late binding through Redis) and the fixed Greedy/Round-Robin/Random in `baselines/scheduler.py` share `baselines/common.py` (level-0 fleet, same placement, `Job.execute()`, `executor_workers`, DAG gate). `run_test.py` archives `agent_profiles.json` per run for `--use-profiles`. No centralized-baseline number from before 2026-10-07 is citable.

### Protobuf Compilation (after modifying `swarm/comm/consensus.proto`)
```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. swarm/comm/consensus.proto
```

## Architecture

### Five-Layer Design

1. **Agent Layer** (`swarm/agents/`) — `ResourceAgent` (rule-based), `LLMAgent` (LLM-enhanced), `ColmenaAgent` (Colmena workflow integration). Base class in `agent_grpc.py`.
2. **Consensus Layer** (`swarm/consensus/engine.py`) — Framework-agnostic PBFT-like protocol: `propose()` → `on_proposal()` → `on_prepare()` → `on_commit()` → quorum triggers `select_job()`; quorum = `ceil((n+1)/2)` over the agent's own `neighbor_map`, floored at 1 (an agent's own votes count since 2026-10-06; no PBFT fault-tolerance number before that is citable). Snow (`gossip_engine.py`, `consensus.protocol: snow`) is drop-in and finalizes exactly-once via Redis `SET NX`. **Exactly-once is a Snow property only**: PBFT writes no claim keys, and both sides of a partition can finalize under PBFT. Never write the safety argument protocol-neutrally. Full text: `docs/ENGINEERING_NOTES.md` §6.
3. **Selection Layer** (`swarm/selection/engine.py`) — Cost matrix computation with LRU caching. `penalties.py` provides live (non-cached) penalty helpers. An agent proposes for a job only when it computes **itself** as the minimum-cost assignee; there is no relative threshold around that (see the `selection_threshold_pct` note below).
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

Every key below has a long history entry in `docs/ENGINEERING_NOTES.md` §7; the line here is the rule.

- `job_selection.cost_weights` — CPU/RAM/Disk/GPU weights (should sum to ~1.0)
- `job_selection.cost_model` (`utilization`/`capacity`) — **`utilization` is the default since 2026-10-16**: a job's cost prices the agent's committed work, (scheduled + backlog + demand) / capacity per resource the job uses, so an idle agent prices as before and a free small agent beats a saturated large one; the analytic load factor is then 1.0. `capacity` (demand / TOTAL capacity × projected-load factor) is every run before that date: it left 54–74 of 81–87 leaves idle in every fc9eb220 cell and, under Snow, the four smallest coordinator groups (`docs/results/e0-e1-fc9eb220/README.md` §9). Backlog travels as `AgentInfo.capacity_backlog` (leaf: selected queue; coordinator: its children's plus delegated-but-unpicked jobs) and on gossip; a batch is assigned job by job so one snapshot cannot hand 20 jobs to one agent, and an agent's open proposals other than the job being priced count as committed (level 0, or `coordinator_cost_matrix: peers` — not under `self`, where every coordinator proposes every job), so those reservations survive Snow re-pricing and peers' votes, and Snow keeps a proposal in `outgoing` until its finalize callbacks have run (`_release_outgoing`; it used to drop it before the CAS, leaving the agent unreserved for those WAN round trips) (stop-time review); the LLM plane keeps the projected factor. Unknown values raise; `cost_model` is in each agent's metrics and a collector column. **Any absolute latency, wait or throughput figure from before 2026-10-16 describes the old model.** `tests/test_cost_model.py`
- ~~`job_selection.selection_threshold_pct`~~ — **removed 2026-09-20; it never worked** and now raises. An agent proposes only when it is its own argmin; there is no replacement knob. `designate_bidder` narrows bidders for LLM agents only.
- `job_selection.connectivity_penalty_factor` — DTN multiplier, 1.0. `local` is not a DTN; `Job.required_dtns()` is the only definition of the required set (two inline copies kept `local` and doubled every workflow cost before 2026-09-20).
- `job_selection.coordinator_cost_matrix` (`self`/`peers`) — `self` (default): every coordinator proposes itself, one decision per coordinator per job; measured as `proposers_per_job_l1`. `peers` is an E5 arm, not a fix; unknown values raise.
- `runtime.jobs_per_proposal` — Batch size for job proposals
- `runtime.peer_expiry_seconds` — Staleness filter for peers read from Redis (300 s), not the failure detector. Loaded via `yaml_strict` (duplicate keys raise). Own-tier eviction is `max(this, failure_threshold_seconds × 1.1 + 1)` so the detector judges before eviction. No F5 re-adoption number before 2026-09-20 is citable.
- `runtime.wall_time_scale` / `wall_time_min_s` / `wall_time_max_s` — simulated sleep `clamp(wall_time × scale, min, max)`, 120 s cap; `scale: 0` is the legacy flat 1 s, do not report makespan from it.
- `runtime.selection_barrier_s` — bounded group-registration wait (default `failure_threshold_seconds`); a timeout logs `[SEL_BARRIER]` and `collect.py` reports `barrier_short_agents` (a validity column).
- `runtime.reselection_timeout_s` — 300 s shipped and code default; `delegation_timeout_s` defaults to twice it.
- `runtime.delegation_exec_grace_s` — how long past `delegation_timeout_s` a coordinator waits for a scheduled child job; defaults to `Job._WALL_TIME_MAX_S`. `schedule_job` persists RUNNING and only `execute_job` writes COMPLETE with the real exit status, so a run ends when jobs finish. No bandit success rate before 2026-09-14 is citable. `tests/test_reward_timing.py`.
- `runtime.shutdown_drain_timeout_s` — 20 s; metrics are saved before and after the drain, since an agent holding a job used to write `metrics.json` after the runner had already read Redis.
- `runtime.decided_unpersisted_grace_s` — 60 s (0 disables); PBFT re-elects a decision whose record stays PENDING past it.
- `runtime.fanout_workers` / `fanout_queue` — transport fan-out pool, 64 / 2048; a full pool drops the send and PBFT never retransmits.
- `mab.algorithm` — `epsilon_greedy`, `ucb1`, `linucb`, `lin_ts`; unknown names and `top_k < 1` raise. `mab.linucb.model: shared|disjoint`, `mab.load_slack` (both off by default).
- `delegation.policy` (`bandit`/`llm`) + `delegation.top_k` — who picks the child group. Needs a coordinator with more than one candidate group (`--groups-per-coordinator`); `--co-parents` concentrates leadership rather than spreading it. `mab.top_k` applies under `bandit`, `delegation.top_k` under `llm`; coordinators log `[DELEGATION] ... can never choose` when inert. LLM decisions are audited as `llm_delegations`, separate from `mab_selections`; the LLM call is synchronous with no wall-clock deadline (P0-3).
- `llm.provider` — "openai" or "none"; `llm.model`. An LLM agent refuses `llm.enabled: false`, `use_for_selection: false` and `provider: none`; `llm.timeout_seconds` is a float.
- `llm.bid_pacing` (`none`/`fallback_parity`/`uniform`) + `bid_pacing_target_s` / `_quantile` / `_max_s` — a failed bid returns in ~0 s and wins placement by arrival time; pacing equalises it. Off by default.
- `llm.score_scale` / `tie_break_with_analytic` / `tie_break_ref_cost` — widen the 0-100 rating (ties 59-92 %) and order surviving ties by analytic cost, capped at half a grid step.
- `llm.cost_cache_ttl_s` / `cost_cache_max` / `snow_cost_fallback` (`yield` default, `analytic` ablation) — an LLM agent answers Snow queries from its last verdict; `yield` is a real abstention since 2026-10-06 and peers answer in proposal units. No Snow number before 2026-10-06 is citable.
- `consensus.protocol` — `pbft` / `snow` / `hybrid`; **shipped `snow`** (`k: 10`, `alpha: 0.7`, `beta: 6`, `round_timeout_ms: 300`, `send_workers: 64`, `max_inflight: 16`, `tie_tolerance: 0.05` — since 2026-10-19 a Snow peer backs itself only when cheaper than the initiator by more than 5 % of its cost, counted as `tolerance_yields`; 0 = strict); code fallback when absent is `pbft`; unknown names raise. `SNOW_DEFAULTS`/`SWIM_DEFAULTS` equal the shipped block (`tests/test_review_2026_10_05_drift.py`).
- `consensus.pbft.counter_propose` — **true by default since 2026-10-19**: a PBFT coordinator under `coordinator_cost_matrix: self` that receives a proposal (or first hears of one via a PREPARE) for a job it has not proposed compares its own `wire_cost_for_job` with the proposal's cost and proposes itself if it dominates, instead of preparing whatever it heard first (`ConsensusEngine._counter_proposal`, via the optional host method `counter_proposal_cost`; never after committing). `false` = every earlier PBFT run, where the coordinator nearest the database won 55–73 % of jobs and one coordinator took 95–99 % of each 1 s batch (`docs/results/e0-e1-f45d9ff1/README.md` §7). Counted as `counter_proposals`; logged `[CON_COUNTER]`. `tests/test_pbft_counter_proposal.py` (delay-ordered bus of real engines)
- `runtime.aggressive_failure_detection` — shipped `false`; on, one slow gRPC probe fails a live peer and its jobs run twice. Heartbeat is the detector in both directions (`_readmit_if_heartbeat_resumed`).
- `failure_detection.protocol` — `heartbeat` / `swim`; shipped `swim`, but **SWIM is advisory**: `consensus_skip_set` is heartbeat's failed set alone, so the peers an agent refuses to talk to and the peers it counts are one set. `tests/test_swim_advisory.py`.
- `gossip.enabled` / `fanout` / `period_ms` / `state_ttl_s` — shipped `enabled: true`; neighbor refresh moves to every 5 s (`runtime.neighbor_refresh_full_s`).
- `instrumentation.decision_log_max` / `textfile_dir` / `textfile_period_s` — per-agent message counts and bytes by type (counted once in `GrpcTransport`; shed sends are `dropped`), `consensus_stats()` on both engines (Snow reports `abandoned`/`finalize_lost`, PBFT reports neither), LLM tokens/latency, delegation decisions. Export needs `textfile_dir` (`--textfile-dir`).

### Metrics and collector (full text: `docs/ENGINEERING_NOTES.md` §8)
- **Collector columns changed 2026-10-06**; re-collect before comparing: `reselection_multiplier` gone (`tier_copies_per_job`, `jobs_restarted`), fan-out ratios divide by jobs offered, `msgs_consensus_sent` counts consensus types only, `consensus_won` is the comparable decision count, `fairness_jain` counts idle agents as zeros, and `aggregate()` emits `<metric>_n` per column — read it.
- **Context age** (`ctx_age_*`) is monotonic on the coordinator; `ctx_age_remote_*` straddles clocks and carries validity columns `ctx_skewed_ages` / `ctx_skew_max_s` / `ctx_unknown_groups`. One record per delegation (`_delegate_child_groups`), exported as `delegation_decisions` and `decisions.csv`.
- **Context error** (T-5): each decision row carries `ctx_view`; `evaluation/context_error.py` reconstructs true in-flight per group and refuses, by reason, any decision it cannot score. `tests/test_context_error.py`.
- **Delegation regret** (P1-1, `evaluation/oracle.py`): scores against the injected failure profile archived in `run_meta.json`; refuses rather than approximates; read `regret_decisions_unscored` first. Design the profile with contrast *within* each coordinator's candidate set. `--validate` on the first run of a regret cell.
- **Designated bidding** (`job_selection.designate_bidder`, LLM agents only): read `designate_forced_share` before `bidders_per_job`.

## Development Guidelines

**Adding new agent types:**
1. Inherit from `Agent` base class in `swarm/agents/agent_grpc.py`
2. Integrate consensus engine via adapter pattern (see `ResourceAgent` in `resource_agent.py`)
3. Implement `compute_job_cost` and `is_job_feasible` methods

**Tuning selection behavior:**
- Adjust cost weights in `config_swarm_multi.yml` first before changing code
- Consider live penalty functions in `swarm/selection/penalties.py`

**Consensus fixes and recovery (full text: `docs/ENGINEERING_NOTES.md` §9):**
- **PBFT stragglers (2026-09-18)**: `_finalized` remembers decisions after the containers clear and `_decided_jobs` makes `is_agreement_achieved` true at finalize; a decision is forgotten only on reset evidence, never elapsed time. No E5/F2 number before this is citable.
- **Review-pass fixes (2026-09-15)**: one COMMIT per proposal (`_commits_sent`), Snow stats under `_stats_lock` with `finalize_errors`, unknown protocol names raise, `batch_tests_v2.py` sizes hosts with `ceil`.
- **Failed-agent job recovery (2026-09-15, retried since 2026-10-06)**: candidates come from Redis (`leader_id`), the Snow claim is released, one reassigner per (job, failure) via `try_claim_reassignment`, peers forget the job; `_resweep_failed_agent_jobs` retries every 30 s with compare-and-delete and WATCHed resets. Coordinator records carry `delegated_groups`; `collect.py` reports `jobs_executed_twice`. No hierarchical failure-injection number before 2026-10-06 is citable.

**Debugging consensus (PBFT):**
1. Set `log-level: DEBUG` in config
2. Check `engine.outgoing`/`engine.incoming` proposal containers and `engine.conflicts`
3. Verify quorum via `calculate_quorum()`
4. Check `save_consensus_votes()` output in Redis

**Debugging consensus (Snow, `consensus.protocol: snow`):**
1. Confirm the engine started: grep logs for `[snow] consensus engine started`; look for `[SNOW_LEADER]`/`[SNOW_PART]` finalization lines
2. `engine.conflicts[job_id]` counts rounds that failed the α-threshold; persistent growth means no candidate is dominating. Bounded at 4096 objects since 2026-09-20, evicting oldest first, matching PBFT — it was unbounded on the engine that runs by default (code review §11)
3. Exactly-once is enforced by `repository.try_claim_assignment` (Redis `SET NX`) — inspect assignment keys with `dump_db.py` to confirm no double-assignment. Since 2026-10-06 they are run-scoped, `assignee:<run_id>:<level>:<group>:<job>` (and `reassign:<run_id>:…`), so a run that skipped the flush cannot inherit claims. This check is **meaningless under PBFT**, which writes no such keys: an empty result there means the audit does not apply, not that nothing was double-assigned
4. Tune `consensus.snow.{k,alpha,beta}`; enable `gossip` and `failure_detection: swim` so peer cost estimates and the live-peer sample stay fresh

**Agent logs:** `<run-dir>/agent-<id>.log`. Dynamic agent logs: `local_agents_initial_start.log`, `local_agents_dynamic_start.log`

**Run identity and metrics completeness (every run; full text: `docs/ENGINEERING_NOTES.md` §9):** `run_test.py` mints `SWARM_RUN_ID`, records it in `run_meta.json`, drops metrics payloads from another run, waits `--metrics-wait-seconds` for every agent and **exits 3** with `metrics_shortfall.json` if any is missing (`--expect-silent-agents` for deliberate kills). It refuses a config whose advertised `grpc.host` is a wildcard or off its placement host, a local `--config-dir` other than `./configs`, and a live leftover agent; the drain writes `drain.json` and exits 4 on an unreadable Redis or a failed distributor; agent start is checked within 3 s; dynamic launches pass `--add`. `stop_agents_v2.sh` waits for exits and fans out in parallel.

## Additional Documentation

- `docs/ENGINEERING_NOTES.md` — the full text of every dated finding and rule summarised above
- `docs/ARCHITECTURE.md` — System architecture, five-layer design, and adapter patterns
- `docs/CONFERENCE_PAPER_PLAN.md` — **the conference paper** (bandit delegation C1 + Snow C3). Local-only, gitignored, venue-independent, self-contained; working deadlines only; its Appendix B is the old Snow-only plan. Shared facts with the FGCS plan are stated in each: **when one changes, change both.** Last tracked revision `ab5ee074` as `CCGRID27_PAPER_PLAN.md`
- `docs/FGCS_EVAL_PLAN.md` — **FGCS journal paper** (31 Jan 2027): LLM plane (C2), composite claim, quantum-hybrid. Local-only, self-contained; review log is Appendix A
- `docs/CODE_REVIEW_2026-10-05.md` — Second full code review (73 findings); items are marked FIXED as they land
- `docs/ROADMAP.md` — Identified improvements and feature roadmap
- `docs/CO_PARENT_USAGE.md` — Multi-parent shared parenting for hierarchical topology
- `docs/HIERARCHICAL_LLM_AGENTS.md` — LLM agents as Level 1 coordinators
- `docs/MAB_README.md` — Multi-Armed Bandit configuration for delegation
- `docs/CONTEXTUAL_BANDIT_DESIGN.md` — Contextual bandit (LinUCB) for delegation, `mab.algorithm: linucb`; deployment-validated scenarios A–C in `evaluation/scenario_{a,b,c}/`
- `docs/COMPLEXITY.md` — PBFT message complexity analysis (mesh and hierarchical)
- `docs/GOSSIP_CONSENSUS_DESIGN.md` — Gossip-based consensus stack (SWIM + gossip + Snow); Phases 1-3 done, Phase 4 pending
- `docs/DISTRIBUTED_BASELINE_DESIGN.md` — Distributed baseline scheduler design (Sparrow in the last section)
- `docs/STAGING_DESIGN.md` — Data staging design, the `(run, name)` store, eager stage-out, measured costs
- `docs/WORKFLOW_EXECUTION.md` — Running a real Pegasus workflow for real; validated soilmoisture results and the limits to read before quoting
- `docs/QUANTUM_HYBRID_DESIGN.md` — Hybrid quantum-classical job support
- `docs/QUANTUM_HYBRID_IMPLEMENTATION.md` — Code-level walkthrough of the quantum support
