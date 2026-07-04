# Contextual Bandit Design for Hierarchical Job Delegation

**Status:** Phases 1–2 implemented. `linucb` is selectable via
`mab.algorithm`: MABManager builds contexts, replays them on delayed
rewards, maintains per-(group, job-type) failure windows, and supports
shaped rewards. Without Phase 3 the group snapshots carry only
manager-owned failure rates (load/capacity features stay at idle
defaults), and `report_outcome` receives no latency/timeout detail from
the agent. Phase 3 (agent wiring) and Phase 4 (evaluation) pending.
**Builds on:** `docs/MAB_README.md` (existing Epsilon-Greedy / UCB1 layer)
**Target modules:** `swarm/rl/bandit.py`, `swarm/rl/mab_manager.py`, `swarm/rl/context.py` (new), `swarm/agents/resource_agent.py`

## Table of Contents

1. [Problem Statement & Motivation](#1-problem-statement--motivation)
2. [Background — Current MAB Layer](#2-background--current-mab-layer)
3. [Gaps Blocking Contextual Learning](#3-gaps-blocking-contextual-learning)
4. [Proposed Design](#4-proposed-design)
5. [Integration with Existing Architecture](#5-integration-with-existing-architecture)
6. [Configuration](#6-configuration)
7. [Pre-Existing Issues Fixed in the Same Pass](#7-pre-existing-issues-fixed-in-the-same-pass)
8. [Evaluation Plan](#8-evaluation-plan)
9. [Implementation Phases](#9-implementation-phases)

---

## 1. Problem Statement & Motivation

In hierarchical topologies, coordinator agents use a Multi-Armed Bandit
(`swarm/rl/`) to pick which child group(s) receive each job. The current
bandit is **context-blind**: each child group has a single scalar Q-value
averaged over every job ever delegated to it, and the `job` argument passed
to `MABManager.select_groups()` is used only for logging.

A context-blind bandit can learn *"group 2 is good on average"* but can never
learn *"group 2 is great for GPU jobs and bad for DTN-heavy jobs."* When the
best group depends on job characteristics — which is the realistic case in a
heterogeneous swarm — the non-contextual policy's cumulative reward is
mathematically capped at the best *single* group's average, while a
per-job-type oracle can do strictly better.

A **contextual bandit** conditions each selection on a feature vector built
from the job and the candidate group, learning the mapping
*(job features, group features) → expected reward*. This directly targets:

- **Heterogeneous job mixes** — CPU-intensive vs memory-heavy vs DTN jobs
  routed to the groups that historically succeed at *that type*.
- **Dynamic agent addition** — a newly joined child group is scored from its
  features (capacity, size, load) immediately instead of starting cold.
- **Non-stationary load** — with a forgetting factor, the model tracks load
  shifts instead of averaging over the whole run.

## 2. Background — Current MAB Layer

### 2.1 Policy Layer (`swarm/rl/bandit.py`)

- `ArmStats` — per-arm counters: `successes`, `failures`, `total_reward`,
  `pull_count`; `q_value = total_reward / pull_count` (lifetime average).
- `BanditPolicy` (ABC) — `select_arm(eligible_arms)`, `update(arm_id, reward)`,
  `get_state()` / `load_state()` for Redis persistence.
- `EpsilonGreedyPolicy` — decaying-epsilon exploration over Q-values.
- `UCB1Policy` — Q-value plus `sqrt(log(total_pulls) / pull_count)` bonus.

Arms are child-group IDs. No feature vector appears anywhere in the interface.

### 2.2 Manager Layer (`swarm/rl/mab_manager.py`)

`MABManager` bridges the policy and the agent hierarchy:

- `select_groups(capable_groups, job, top_k)` — sequentially pulls `top_k`
  arms from the capable set. The `job` parameter is unused except in a debug
  log line.
- `report_outcome(group_id, job_id, success)` — maps success/failure to a
  binary reward (`+1.0` / `-1.0`) and calls `policy.update()`.
- `save_state()` / `load_state()` — JSON round-trip to Redis key
  `mab:{agent_id}`.

### 2.3 Agent Wiring (`swarm/agents/resource_agent.py`)

- **Selection:** `scheduling_main()` filters child groups by DTN capability
  (`_get_child_groups_for_job()`), then calls
  `mab_manager.select_groups(capable_groups, job, top_k)` and delegates by
  writing the job to Redis at `level - 1` for each selected group.
- **Attribution:** `job_delegation_map` records `job_id → group_id`, **only
  when exactly one group was selected** (`len(selected_groups) == 1`).
- **Reward (delayed):** `_monitor_delegated_jobs()` polls child-level job
  state in Redis. On `COMPLETE`, reward is `+1` if `exit_status == 0`, else
  `-1`. On delegation timeout, reward is `-1` and the job is reassigned.
  Rewards therefore arrive **seconds to minutes after selection**.

Eligibility filtering (DTN capability, active leadership) happens *upstream*
of the bandit. This separation is preserved: the contextual bandit ranks
feasible groups; it never overrides feasibility.

## 3. Gaps Blocking Contextual Learning

| # | Gap | Where | Consequence |
|---|-----|-------|-------------|
| 1 | No context in the policy interface | `bandit.py` — `select_arm(eligible_arms)` | Job/group features cannot influence selection |
| 2 | No context replay for delayed rewards | `job_delegation_map` stores only `group_id` | Contextual update needs the feature vector *used at selection time*; it is discarded |
| 3 | Binary ±1 reward | `MABManager.report_outcome()` | Completion latency, timeout-vs-crash, queue wait all collapse into one bit |
| 4 | No forgetting | `ArmStats.q_value` is a lifetime average | Stale estimates under shifting load; hurts existing policies too |
| 5 | Cold start on new arms | Per-arm `ArmStats` | Dynamically added child groups start with zero knowledge |
| 6 | `top_k > 1` produces no learning signal | `resource_agent.py` — delegation map populated only when one group selected | Bandit selects but never updates when `top_k >= 2` |

## 4. Proposed Design

### 4.1 Algorithm — Shared-Model LinUCB

We use **LinUCB with a single shared linear model over per-(job, group)
feature vectors**, rather than the textbook "disjoint" per-arm variant.

For each candidate group `a`, build one feature vector

```
x_a = concat(job_features, group_features(a), [1.0])   # bias term
```

and score all candidates with **one** shared ridge-regression model:

```
score(a) = θᵀ x_a  +  α · sqrt(x_aᵀ A⁻¹ x_a)
           └─ point estimate ─┘  └─ optimism bonus (uncertainty) ─┘

θ = A⁻¹ b        A = γ·A + x xᵀ  (on update, with discount γ)
                 b = γ·b + r x
```

The arm with the highest score wins; ties break randomly (matching existing
policy behavior).

**Why shared-model over disjoint per-arm models:**

1. **Dynamic arms.** Child groups appear and disappear (dynamic agent
   addition, leadership changes). A shared model scores a brand-new group
   immediately from its features; a per-arm model restarts from zero.
2. **Sample efficiency.** All delegations train one model, so patterns like
   *"high-GPU job × group with GPU headroom → high reward"* are learned
   across groups, not per group.
3. **Simpler top-k.** With one model, taking the k highest scores is correct;
   disjoint models complicate sequential selection.

**Interaction features are required.** A linear model over a plain
`concat(job, group)` vector is *additive*: the job part is identical for
every candidate arm, so it shifts all scores equally and cannot influence
*which* arm wins. Job-dependent routing therefore needs explicit
job-x-group interaction terms in `x_a` (Section 4.2): demand-x-headroom fit
products and the group's failure rate *for this job's type*. Without them,
two groups with identical load are indistinguishable regardless of job type
(verified by unit test during Phase 1).

**Incremental inverse via Sherman–Morrison.** With `d ≈ 10–14` features we
maintain `A⁻¹` directly with rank-1 updates:

```
A⁻¹ ← (A⁻¹ − (A⁻¹ x xᵀ A⁻¹) / (1 + xᵀ A⁻¹ x)) / γ
```

O(d²) per update, one small numpy matrix (numpy is already in
`requirements.txt`), no per-selection matrix inversion. Overhead at the
coordinator is negligible relative to Redis round-trips.

**Non-stationarity.** The discount `γ ∈ (0, 1]` (default 0.995) exponentially
down-weights old observations so the model tracks load shifts. `γ = 1.0`
recovers standard LinUCB.

**Linear Thompson Sampling** (`lin_ts`) is the natural second algorithm
behind the same interface — sample `θ̃ ~ N(θ, v² A⁻¹)`, pick
`argmax θ̃ᵀ x_a` — useful as a comparison point in evaluation.

### 4.2 Feature Vector (`swarm/rl/context.py`, new)

A `ContextExtractor` produces a fixed-length vector, all components
normalized to `[0, 1]`, from a `(job, group snapshot)` pair. The coordinator
already sees child state via heartbeats (`children_map`), so no new
communication is required.

**Job features** (shared across candidates):

| Feature | Source | Normalization |
|---------|--------|---------------|
| core, ram, disk, gpu demand | `job.capacities` | divide by configured max caps |
| wall time | `job.wall_time` | `log1p`, clipped at `job_selection.long_job_threshold` |
| DTN requirement count | `job.data_in` / `job.data_out` | divide by max DTNs |
| job type | `job.job_type` | one-hot over configured type list (unknown → all-zero) |

**Group features** (per candidate group `a`):

| Feature | Source | Normalization |
|---------|--------|---------------|
| active child count | `children_map` / active-group tracking | divide by max group size |
| aggregate capacity headroom (cpu/ram/gpu) | children heartbeat capacities | fraction of total |
| in-flight delegations to group | `delegated_jobs` entries for `a` | divide by cap (e.g. 32) |
| recent failure rate | sliding window in `MABManager` (last N outcomes per group) | already in [0, 1] |

**Job x group interaction features** (per candidate group `a` — required
for job-dependent routing, see Section 4.1):

| Feature | Definition |
|---------|------------|
| fit_core / fit_ram / fit_gpu | job demand norm x group headroom (elementwise product) |
| type failure rate | group `a`'s recent failure rate *for this job's type* (sliding window per (group, job type) in `MABManager`; falls back to the aggregate rate for unseen types) |

Plus a constant bias term. `d = 17 + |job_types|` with the Phase 1 layout.

**Schema versioning.** The extractor exposes `schema_version` (hash of
feature names + config). Persisted state carries the version; on mismatch at
load time the state is discarded (a model trained on a different feature
layout is garbage).

### 4.3 Policy Interface Changes (`swarm/rl/bandit.py`)

Backward-compatible signature extension:

```python
class BanditPolicy(ABC):
    @abstractmethod
    def select_arm(self, eligible_arms: List[int],
                   context: Optional[Dict[int, "np.ndarray"]] = None) -> int: ...

    def select_top_k(self, eligible_arms, k, context=None) -> List[int]:
        # default: sequential select_arm + remove (current behavior);
        # LinUCB overrides with top-k-by-score

    def update(self, arm_id: int, reward: float,
               context: Optional["np.ndarray"] = None): ...
```

- `context` maps each eligible arm to its `x_a` vector.
- `EpsilonGreedyPolicy` / `UCB1Policy` ignore `context` — **zero behavior
  change** when `mab.algorithm` stays `epsilon_greedy` or `ucb1`, and A/B
  comparison is a one-line config switch.
- New `LinUCBPolicy(alpha, discount, dim)` implements Section 4.1. It also
  keeps `ArmStats` counters per arm purely for metrics/plotting continuity
  (`get_stats()`, `plot_mab_results.py`).

### 4.4 Delayed-Reward Context Replay (`swarm/rl/mab_manager.py`)

The reward for a delegation arrives minutes later via
`_monitor_delegated_jobs()`. A contextual update must replay the exact
feature vector used at selection time. This is localized inside `MABManager`
so `resource_agent.py` barely changes:

```python
# at selection time
def select_groups(self, capable_groups, job=None, top_k=1):
    contexts = self.context_extractor.build(job, capable_groups, self._group_snapshot())
    selected = self.policy.select_top_k(capable_groups, top_k, contexts)
    for g in selected:
        self._pending[(job.job_id, g)] = PendingSelection(
            context=contexts[g], selected_at=time.time())
    return selected

# at reward time
def report_outcome(self, group_id, job_id, success, latency_s=None):
    pending = self._pending.pop((job_id, group_id), None)
    reward = self._shape_reward(success, latency_s)
    self.policy.update(group_id, reward, context=pending.context if pending else None)
```

`_pending` entries carry a TTL (default: `2 × delegation_timeout_s`) and are
swept periodically so orphaned entries (lost leadership, coordinator
restart) don't leak. In-memory only for Phase 1 — after a coordinator
restart the model persists (Redis) but in-flight selections lose their
context and fall back to a context-free update, which LinUCB simply skips.

### 4.5 Reward Shaping

Replace binary ±1 with a continuous signal (applies to *all* policies,
gated by `mab.reward.shaped`):

```
success:            r = 1 − min(1, completion_latency / delegation_timeout_s)
non-zero exit:      r = −0.5
delegation timeout: r = −1.0
```

`_monitor_delegated_jobs()` already has `delegated_at` and observes the
completion, so `completion_latency` is available at the call site — the only
agent-side change is passing it to `report_outcome()`.

### 4.6 Forgetting for Non-Contextual Policies (cheap independent win)

Add optional `step_size` to `EpsilonGreedyPolicy` / `UCB1Policy`:

```
Q ← Q + step_size · (r − Q)      # exponential recency-weighted average
```

Unset (default) preserves the current lifetime-average behavior. This
addresses Gap 4 even for users who never enable LinUCB.

### 4.7 Persistence

`LinUCBPolicy.get_state()` serializes `A_inv`, `b`, `theta` as nested lists
plus `schema_version`, `alpha`, `discount`, and the per-arm metric counters
— same JSON-to-Redis path (`mab:{agent_id}`) as today. `load_state()`
validates dimension and schema version and discards on mismatch (logged at
WARNING).

## 5. Integration with Existing Architecture

Changes required in `resource_agent.py` are deliberately minimal:

1. **`_init_mab()`** — pass a group-snapshot callable (children/heartbeat
   view) into `MABManager` so the extractor can read group features without
   the manager holding agent internals.
2. **`scheduling_main()`** — unchanged call shape; `select_groups()` already
   receives `job`.
3. **`_monitor_delegated_jobs()`** — pass `latency_s` into
   `report_outcome()`; populate the delegation map for *all* selected groups
   (see Section 7).

Everything else (DTN capability filtering, delegation writes, reassignment,
metrics export) is untouched. The bandit remains a pure ranking layer below
feasibility, consistent with the engine/adapter separation used elsewhere in
the codebase.

## 6. Configuration

Extends the existing `mab:` block in `config_swarm_multi.yml`:

```yaml
mab:
  enabled: true
  algorithm: "linucb"          # epsilon_greedy | ucb1 | linucb | lin_ts
  top_k: 1

  # Existing epsilon-greedy / UCB1 params unchanged; new optional:
  step_size: null              # e.g. 0.05 → recency-weighted Q (null = lifetime avg)

  linucb:
    alpha: 1.0                 # exploration width (UCB bonus multiplier)
    discount: 0.995            # forgetting factor γ; 1.0 = standard LinUCB
    ts_variance: 0.25          # v² for lin_ts posterior sampling

  context:
    job_types: ["compute", "transfer", "dtn"]   # one-hot vocabulary
    max_group_size: 10                          # normalization caps
    max_inflight: 32
    failure_window: 20                          # sliding-window size per group

  reward:
    shaped: true               # latency-shaped reward instead of ±1
    exit_failure: -0.5
    timeout: -1.0

  persist_to_redis: false
  persist_interval_s: 30.0
```

Defaults are chosen so an existing config with `algorithm: epsilon_greedy`
runs byte-identically to today.

## 7. Pre-Existing Issues Fixed in the Same Pass

**`top_k > 1` produces no learning signal.** `job_delegation_map` is only
populated when exactly one group is selected, so with `top_k: 2` the bandit
selects but never updates. Fix: record all selected groups (the pending map
in Section 4.4 is keyed by `(job_id, group_id)`); on completion, credit the
group whose child level completed the job; other selected groups receive no
update by default (`mab.reward.non_winner: null`), configurable to a small
negative value to penalize wasted delegation.

**Sequential top-k with UCB-style policies.** The current loop pulls arms
one at a time without intermediate updates, which double-counts optimism for
UCB1. With the shared LinUCB model, `select_top_k` takes the k highest
scores directly — simpler and correct.

## 8. Evaluation Plan

The existing failure simulator is the ready-made testbed:
`mab.failure_simulation.per_job_type_failure_rates` combined with
`per_agent_failure_rates` creates **group-dependent, job-type-dependent**
outcomes — precisely the regime where contextual beats non-contextual.

**Scenario A — context-dependent failure rates.**
Hierarchical topology, 2–4 child groups. Configure e.g. group 1 to fail 60%
of `dtn` jobs, group 2 to fail 60% of `compute` jobs, mixed job stream.
Compare `epsilon_greedy`, `ucb1`, `linucb` on cumulative reward, delegation
success rate, and per-type routing accuracy. Expected: non-contextual
policies plateau at the best single group's average; LinUCB approaches the
per-type oracle.

**Scenario B — non-stationarity.**
Flip the failure profile mid-run (or add load skew). Compare `discount: 1.0`
vs `0.995`, and `step_size` variants of epsilon-greedy.

**Scenario C — dynamic agent addition.**
Add a child group mid-run (`--dynamic-agents`). Measure time-to-first-use
and regret on the new group for shared-model LinUCB vs per-arm baselines.

**Metrics & plotting.** `plot_mab_results.py` / `plotting/mab.py` extended
with: reward curves per algorithm, per-job-type delegation heatmap, and
learned-θ inspection (feature weights) for interpretability. Raw data flows
through the existing `metrics.json` export (`mab_stats`, `mab_selections`,
`mab_rewards`).

**Unit tests** (extend `tests/test_bandit.py`, 13 tests today):

- Synthetic linear environment: reward `= wᵀx + noise`; assert LinUCB regret
  shrinks and final θ correlates with `w`.
- Sherman–Morrison inverse matches `np.linalg.inv` after N updates.
- Discount: after an abrupt reward flip, `γ < 1` recovers, `γ = 1` lags.
- State round-trip through `get_state()`/`load_state()`; schema-version
  mismatch discards state.
- Context-replay: delayed `report_outcome` updates with the stashed vector;
  TTL sweep drops orphans.
- Backward compatibility: epsilon-greedy/UCB1 results unchanged with
  `context` passed.

## 9. Implementation Phases

| Phase | Scope | Deliverables |
|-------|-------|--------------|
| 1 | Policy + context core | `LinUCBPolicy`, `ContextExtractor`, interface extension, unit tests — **done** |
| 2 | Manager plumbing | Pending-context map, reward shaping, TTL sweep, per-(group, job-type) failure windows feeding `GroupSnapshot`, persistence with schema versioning — **done** |
| 3 | Agent wiring | Group-snapshot callable, latency into `report_outcome`, multi-group attribution (`top_k > 1` fix) |
| 4 | Evaluation | Scenarios A–C via `run_test.py` + failure simulation, plotting extensions, `lin_ts` comparison |

Estimated footprint: ~150 lines in `bandit.py`, ~120 lines in `context.py`
(new), ~60 lines in `mab_manager.py`, minor touches in `resource_agent.py`,
plus tests.
