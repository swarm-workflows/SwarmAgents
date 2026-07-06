# Contextual Bandit Design for Hierarchical Job Delegation

**Status:** Phases 1–3 implemented. `linucb` is fully wired: the agent
provides live group snapshots (children capacity headroom + in-flight
delegations via `snapshots_from_children`), passes completion latency and
timeout detail into `report_outcome`, and credits the group where
completion was observed — `top_k > 1` attribution works and
`job_delegation_map` was removed (superseded by the manager's pending
map). Phase 4 complete: Scenario A — LinUCB beats epsilon-greedy 73.4%
vs 61.9% job success, 69% vs 50% routing (8.1). Scenario B — discount
0.98 avoids the post-flip success crash that discount 1.0 suffers
(68.0% vs 61.1%; 8.2). Scenario C (group outage/rejoin) — LinUCB
re-adopts the recovered group instantly, epsilon-greedy never does
(79.5% vs 74.0%; 8.3, including two design gaps found: dead-group
dog-piling and poisoned-window hysteresis). Offline plotting via
`plot_mab_results.py --dump`.
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
| type failure rate | group `a`'s recent failure rate *for this job's type* (sliding window per (group, job type) in `MABManager`; falls back to the aggregate rate for unseen types). Delegation timeouts are excluded — they are liveness signals, not job-type fit |
| timeout rate | time-decayed per-group delegation-timeout signal (`context.timeout_decay_s` e-folding, ~3 recent timeouts saturate). Fades with wall-clock time even when the arm is never tried — no refresh hysteresis after an outage (Scenario C fix) |

Plus a constant bias term. `d = 18 + |job_types|` with the current layout.

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
Hierarchical topology, 2–4 child groups (use `--co-parents 2+` so each
coordinator leads multiple groups/arms). `per_agent_failure_rates` accepts a
per-job-type dict (`{job_type: rate, "default": rate}`) so failure profiles
can depend on (agent, job type). Configure e.g. group 1's agents to fail 60%
of `dtn` jobs, group 2's to fail 60% of `compute` jobs, mixed job stream.
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

### 8.1 Scenario A Results (2026-07-04, swarm deployment)

Executed on the FABRIC swarm deployment: 30 agents (25 leaves in 5 groups +
5 coordinators), `--co-parents 5` so coordinator 26 is elected active leader
for all 5 groups (5 bandit arms; lowest-ID-alive election makes co-parenting
failover, not load-sharing — with `--co-parents 2` most coordinators lead a
single group and the bandit is bypassed via the `top_k >= len` shortcut).
Workload: 231 Pegasus jobs (`pegasus-data/all_profiles_nodtn.txt` — data
nodes stripped; every DTN job referenced the same `local` site, which
constrained group capability without carrying routing signal). Failure
profiles: groups 0/2/4 fail ram_bound jobs at 0.8, groups 1/3 fail
cpu_bound at 0.8, 0.05 otherwise; workload splits 116 ram / 115 cpu.
Jobs paced at 1/s so delayed rewards train the model online. Tooling in
`evaluation/scenario_a/`.

| Metric (L0 job records) | LinUCB | Epsilon-Greedy |
|---|---|---|
| Job success rate | **73.4%** (234/319) | 61.9% (156/252) |
| Routing accuracy (job type -> non-failing parity) | **69.0%** | 50.0% |
| Routing accuracy, 2nd half | **70.6%** (rising) | 46.0% (flat) |
| Routing by quarter | 47 -> 87 -> 67 -> 74% | 62 -> 46 -> 49 -> 43% |

Epsilon-greedy sits at exactly coin-flip routing, as predicted — a
context-blind bandit cannot condition on job type, and its per-arm averages
locked onto cpu-success-heavy arms, dragging ram jobs into ram-failing
groups (95 of 127 ram jobs misrouted). LinUCB's learning transition is
visible between the first and second quarter (47% -> 87%), and the learned
weights are interpretable: `grp_type_failure_rate` -0.263 (the designed
avoid-groups-that-fail-this-type mechanism) and `grp_inflight` -0.488
(load balancing emerged as secondary behavior).

Operational notes for reruns: (1) enable `mab.persist_to_redis` — agents
are SIGKILLed at run end, so end-of-run `save_results()` metrics are lost;
the 10s persistence interval preserves near-final policy state under
`mab:{agent_id}`. (2) Snapshot Redis (`extract_scenario_a.py`) before the
next run's cleanup flushes it.

### 8.2 Scenario B Results (2026-07-06, swarm deployment)

Same setup as 8.1, but the failure parity **flips mid-run** via
`failure_simulation.phases` (`after_s: 160` ≈ mid-workload at 1 job/s):
even groups switch from failing ram_bound to failing cpu_bound and vice
versa. Two LinUCB runs differing only in `linucb.discount`. Tooling in
`evaluation/scenario_b/`.

| Metric (L0 records, per decile) | discount 1.0 | discount 0.98 |
|---|---|---|
| Pre-flip peak routing / success | 97–100% / 88% | ~74% / 77% |
| Post-flip success trough | **27–33%** (~2 deciles of stale routing) | **≥57%** (no crash) |
| Records to 70% new-parity routing after trough | 44 | n/a — transition smooth |
| Overall job success | 61.1% (203/332) | **68.0%** (240/353) |

Without forgetting (γ=1.0) the model locks in hard, keeps routing by the
stale parity for ~60+ records after the flip (success 27–33%), and only
then swings. With γ=0.98 exploitation is softer pre-flip but the model
tracks the flip almost immediately — success never dips below ~57% and
overall success is +6.9pp. Classic stability-plasticity trade-off,
reproduced on the deployment. Note both runs also adapt through the
per-(group, job-type) failure windows (20 outcomes), which refresh the
`grp_type_failure_rate` input regardless of γ; the discount additionally
controls how fast θ itself un-commits from the stale mapping — the curves
show that effect dominating the recovery shape.

### 8.3 Scenario C Results (2026-07-06, swarm deployment)

Implemented as **group outage and rejoin** (a pure never-seen-group cold
start would require dynamic topology support): groups 0–3 fail all jobs at
0.30, group 4 (agents 21–25) is the superior resource at 0.05. The driver
(`evaluation/scenario_c/scenario_c_driver.sh`) kills group 4's leaves 75s
after launch and restarts them at ~181s (mid-workload);
`delegation_timeout_s: 60` so delegations to the dead group fail fast.

| Metric | LinUCB | Epsilon-Greedy |
|---|---|---|
| Overall job success | **79.5%** | 74.0% |
| Records to first group-4 use after rejoin | **0** | 26 |
| Post-rejoin group-4 share (first quarter) | **62%** | 0% |
| Post-rejoin group-4 share (steady) | 15% (dips to 0 — see below) | ~4% (pure ε-noise; never re-adopts) |

Epsilon-greedy's arm-4 Q-value is poisoned by outage timeouts and can
never recover except by ε-random pulls — re-adoption effectively never
happens. LinUCB re-adopts instantly at rejoin because the context changed
(children reappear, headroom resets) even though the arm identity is the
same.

**Two design gaps surfaced (both algorithms affected, LinUCB more):**

1. **Dead-group dog-piling.** During the outage the bandit routed *toward*
   the dead group (LinUCB rolling share reached ~96%): an empty group looks
   attractive — idle-default snapshot features, empty failure windows
   (falling back to a 0.0 aggregate rate), and `grp_inflight` under-counts
   because delegation-timeout handling removes the `delegated_jobs` entries
   that feed it. Optimism amplifies the effect. The jobs eventually
   completed (many executed from the backlog after rejoin), so *success*
   barely dipped — the damage shows up as latency, not failures.
2. **Poisoned-window hysteresis.** The outage's timeout storm filled group
   4's per-type failure windows with failures; post-rejoin those windows
   only refresh if the arm is tried, so after burning the backlog LinUCB
   avoided the now-best group for ~50 records before uncertainty retried it.

**Fixes — implemented (2026-07-06, post-evaluation):**

1. *Liveness gating* — `scheduling_main` now intersects capable groups with
   `_get_live_child_groups()` (groups holding at least one fresh child
   heartbeat) before the bandit sees them; if every group looks dead the
   ungated list is kept so delegation never stalls. Cold-start optimism for
   genuinely new groups is preserved — they heartbeat.
2. *Time-decayed timeout signal* — delegation timeouts no longer touch the
   per-type failure windows at all (they are liveness, not fit); they feed
   a per-group exponentially-decaying score (`grp_timeout_rate` feature,
   `context.timeout_decay_s`, default 120s e-folding) that fades on its own
   after an outage ends — no refresh hysteresis.
3. *In-flight robustness* — `_build_snapshots` takes the max of the agent's
   delegated-jobs count and the manager's own unresolved pending selections,
   closing the undercount during reassignment churn.

These change the feature schema (dim +1), so previously persisted LinUCB
state is discarded on load by the schema-version check, as designed. A
Scenario C re-run to quantify the fixes is pending.

Additional operational note: `cleanup.py --cleanup-redis` does not clear
`mab:*` keys, so persisted policy state from prior runs leaks into later
dumps — only trust `mab:{agent_id}` for coordinators active in the
current run.

Plotting extensions are implemented (`plot_mab_results.py
--dump/--events/--job-types` offline mode over full Redis dumps: rolling
success + delegation-share timeline with event markers, class-x-group
outcome heatmap, learned-theta chart). `lin_ts` (Linear Thompson Sampling)
is implemented and selectable via `mab.algorithm: lin_ts`
(`linucb.ts_variance`, default 0.25) — it shares LinUCB's model, updates,
persistence, and discounting, replacing the UCB width with posterior
sampling. Remaining open: the deployment `lin_ts` head-to-head, a
Scenario C re-run validating the 8.3 fixes, and batch runs for error bars.

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
| 3 | Agent wiring | Group-snapshot callable, latency into `report_outcome`, multi-group attribution (`top_k > 1` fix) — **done** |
| 4 | Evaluation | Scenarios A–C + plotting extensions — **done** (8.1: LinUCB 73.4% vs eps 61.9%; 8.2: discount 0.98 avoids post-flip crash; 8.3: instant vs never rejoin re-adoption, plus two design gaps found; offline dump plotting in `plot_mab_results.py --dump`). `lin_ts` comparison and batch error bars remain open |

Estimated footprint: ~150 lines in `bandit.py`, ~120 lines in `context.py`
(new), ~60 lines in `mab_manager.py`, minor touches in `resource_agent.py`,
plus tests.
