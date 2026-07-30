# Hybrid Quantum-Classical Jobs — Implementation Walkthrough

Companion to `QUANTUM_HYBRID_DESIGN.md` (the *what and why*). This document
explains *how the code works*: every module touched, the data flow between
them, and the reasoning behind the non-obvious choices. References use
file + method names rather than line numbers.

Contents:

0. [The big picture](#0-the-big-picture)
1. [File map](#1-file-map)
2. [Data models](#2-data-models)
3. [Job model extensions](#3-job-model-extensions)
4. [Agent state and heartbeats](#4-agent-state-and-heartbeats)
5. [Feasibility](#5-feasibility)
6. [Cost model](#6-cost-model)
7. [Selection-cache correctness](#7-selection-cache-correctness)
8. [Measurement data layer](#8-measurement-data-layer)
9. [Splitting hybrid jobs](#9-splitting-hybrid-jobs)
10. [Data-predicate gating](#10-data-predicate-gating)
11. [Execution paths](#11-execution-paths)
12. [Self-expanding pool](#12-self-expanding-pool)
13. [Tooling pipeline](#13-tooling-pipeline)
14. [Configuration reference](#14-configuration-reference)
15. [End-to-end trace of a real job](#15-end-to-end-trace-of-a-real-job)
16. [Tests](#16-tests)
17. [Known limitations and gotchas](#17-known-limitations-and-gotchas)

---

## 0. The big picture

Everything below in one narrative. Jobs come in **three classes**: `classical`
(no quantum component — the existing system, unchanged), `quantum` (a one-shot
circuit offload with a small classical prep component), and `hybrid` (a
variational-style loop of N classical↔quantum iterations).

### Hybrid jobs run in one of two modes

The choice is made at submission time by the distributor flag
`--split-hybrid`; the job file itself is identical.

**Co-located (flag off, Phase 1).** The hybrid job is submitted as *one* job
that needs everything from a single agent: classical capacity for the update
loop *and* a backend with enough qubits. Only backend-owning agents are
feasible; the winner simulates the whole alternating loop in-process
(`Job._execute_quantum`). The measurement layer is never involved. The
consequence — and the resource-utilization complaint that motivates the
runtime paper — is that the agent's QPU is held for the entire job, including
every classical phase in which it is logically idle.

**Split (flag on, Phase 2).** The distributor decomposes the job into two
linked sub-jobs *before* anything enters Redis: `<id>-q` (the quantum spec
plus a prep-sized classical footprint — the measurement **producer**) and
`<id>-c` (the bulky classical demand, no qubits — the stream **consumer**).
The coupling between the halves moves out of process memory and into the
**measurement data layer**: a Redis stream per experiment that the producer
appends snapshot batches to (one per iteration) and the consumer reads
incrementally. The qubits are held only by the producer; the classical work
can land on *any* agent with capacity. The flag therefore chooses *where the
classical↔quantum coupling lives*: inside one process (simple, QPU
monopolized) or through the data layer (two placements, overlap, locality
trade-offs).

### The classical half does NOT wait for the quantum half to finish

Both sub-jobs enter the pool at the same time. The consumer carries a data
predicate — "at least 1 snapshot from experiment X" — and is held back only
until the producer publishes its *first* iteration's measurements. From that
moment the consumer is schedulable; it runs *concurrently* with the producer,
consuming batches as they stream in, and completes only after processing all
of them (`total_snapshots` = producer iterations). Job 37 from the validation
run:

```
37-q (agent 9):  placed → prep → i1 → i2 → i3 → i4 → i5 → done
                              ↓ snapshot 1 published
37-c (agent 7):  gated ......→ placed → u1 → u2 → u3 → u4 → u5 → done
                                                       (+0.86 s after 37-q)
```

That overlap is the "data-triggered" idea: the classical side starts as soon
as enough data *exists*, not when the quantum run *ends*. The one path where
"wait for completion" is accurate is the one-shot **post-processing** push: a
one-shot quantum job publishes its measurements only at completion, then
pushes an `<id>-post` classical job whose predicate is already satisfied.

### The predicate is a gate in front of the scheduler, not a scheduler

There is no second scheduling mechanism. Every job — classical, quantum,
split consumer — travels the same pipeline:

```
pending queue → proposed by an agent → PBFT/Snow consensus → assigned → executed
```

The predicate adds exactly one filter at the entrance: in `selection_main`,
before any costs are computed, jobs whose predicate isn't satisfied are moved
to the back of the pending queue and skipped — still PENDING, still visible,
re-checked every tick via a cached O(1) stream count. The moment the data
exists, the job becomes an utterly ordinary job: agents evaluate feasibility,
compute costs (this is where the cross-site communication penalty enters, as
one multiplier among the usual ones), propose, and run normal consensus.

What the predicate does *not* do: it does not reserve an agent, does not
prioritize the job (once eligible it competes on cost like everything else),
and does not bypass feasibility or quorum. The design reason for gating at
the entrance and nowhere else: readiness is a *global, time-varying* fact
(same answer for every agent), while feasibility and cost are *per-agent,
cacheable* facts — folding readiness into `is_job_feasible` would poison the
per-(job × agent) LRU caches and multiply the Redis checks by the agent count.

### The measurement layer in one paragraph

It is the shared data plane between the halves (the paper's "Quantum
Measurement Data Layer"), built on Redis *streams* rather than pub/sub. It
(1) stores timestamped snapshot batches per experiment, (2) answers the data
predicates that gate scheduling (O(1) `XLEN`, cached), (3) records the
producer's site so consumer placement can be steered toward the data, and
(4) delivers batches to consumers incrementally — with *replay* (a consumer
scheduled after the producer finished reads the full history; pub/sub would
have dropped it) and *cursors* (consumption position is checkpointed in
`Job.state_data`, so a reassigned consumer resumes instead of reprocessing).
Producer, consumer, and schedulers all talk only to the layer — that
decoupling in time, space, and scheduling is what lets the two halves of one
logical job run on different agents without knowing about each other.

The rest of this document walks through the same material bottom-up, module
by module.

---

## 1. File map

| File | Role |
|------|------|
| `swarm/models/quantum.py` | **New.** `QuantumSpec` (job-side requirements) and `QuantumBackend` (resource-side descriptor) |
| `swarm/models/capacities.py` | `qubits` added as a first-class capacity field |
| `swarm/models/job.py` | `quantum` spec, `job_class`, split fields (`sub_role`, `linked_job_id`, `experiment_id`, `data_predicate`, `state_data`), three execution paths |
| `swarm/models/agent_info.py` | `quantum_backend` attached to the peer snapshot (modeled like DTNs) |
| `swarm/quantum/measurement_layer.py` | **New.** Redis-streams measurement layer + data predicates |
| `swarm/quantum/split.py` | **New.** Pure builders: hybrid split, post-processing job, communication penalty |
| `swarm/agents/resource_agent.py` | Config parsing, heartbeat reporting, feasibility, cost terms, predicate gating, execution dispatch, pool push, cache signatures |
| `job_generator.py` | `--quantum-fraction` / `--hybrid-fraction`, spec sampling, feasibility CSV |
| `generate_configs.py` | `--quantum-agents-pct`, backend catalog, per-agent assignment |
| `job_distributor.py` | `--split-hybrid` decomposition at submission time |
| `run_test.py` | Flag pass-through for one-command experiments |
| `tests/test_quantum.py`, `tests/test_quantum_phase2.py` | 41 unit tests |

Nothing in `swarm/consensus/`, `swarm/selection/engine.py`, `swarm/comm/`, or
`swarm/database/` changed. Quantum jobs ride the existing PBFT/Snow consensus,
selection engine, gRPC transport, and Redis repository untouched — only the
*inputs* to selection (feasibility, cost, and which jobs are proposable)
changed, which is exactly the seam the adapter architecture provides.

---

## 2. Data models

### 2.1 The additive / non-additive split

The single most important design decision. Ask of every quantum attribute:
*can you add it across nodes and subtract it on allocation?*

- **Qubit count: yes.** An 80-qubit job on a 133-qubit backend leaves 53
  qubits of headroom, a parent aggregating two children has the sum, and a
  running job's qubits belong in the allocation ledger. So `qubits` became a
  field of `Capacities`, next to `core`/`ram`/`disk`/`gpu`. Every existing
  mechanism then works with **zero special cases**: `__add__`/`__sub__`,
  `negative_fields()` (the feasibility residual check), `can_schedule_job`'s
  ready-queue accounting, hierarchical `total_capacities` and
  `max_child_capacity` aggregation, Redis serialization.
- **CLOPS, gate fidelity, error rate, calibration downtime, architecture,
  gate set: no.** CLOPS is a rate, fidelity/error are probabilities, downtime
  is a schedule property, arch is categorical. These live on
  `QuantumBackend`, attached to `AgentInfo` the same way DTNs are, and are
  consumed explicitly by feasibility and cost.

Backward compatibility falls out of the existing `JSONField` conventions:
`to_dict()` skips zero-valued fields (a classical agent's capacities
serialize exactly as before) and `from_dict(forgiving=True)` ignores unknown
keys (old serialized blobs load cleanly; missing `qubits` defaults to 0).

### 2.2 `QuantumSpec` (job side)

| Field | Meaning |
|-------|---------|
| `qubits`, `circuit_depth` | Circuit shape |
| `shots` | Explicit shot request (0 = derive, see below) |
| `error`, `confidence` | Requested statistical accuracy — the docx's "ERROR, CONFIDENCE determines the necessary number of shots" |
| `clops` | Minimum backend CLOPS (0 = any) |
| `arch` | Preferred architecture: superconducting / ion-trap / neutral-atom / photonic ("" = any) |
| `fidelity` | Minimum gate fidelity (0 = any) |
| `gates` | Required gate set ([] = any) |
| `output_type` | `expectation` or `histogram` |
| `hybrid` | True = continuous classical↔quantum loop |
| `iterations` | Loop rounds (1 for one-shot) |
| `post_process` | One-shot jobs: push a classical post-processing job on completion |

Two derived quantities:

- **`required_shots()`** — explicit `shots` if given; otherwise the binomial
  worst-case bound `n ≥ z² / (4·error²)` with `z` looked up from the
  confidence level (1.645/1.960/2.576 for 90/95/99%). Example: error 0.023 at
  90% → `1.645²/(4·0.023²) ≈ 1279`. Falls back to 1024 when neither is set.
- **`estimated_quantum_time(backend_clops)`** — the CLOPS circuit-runtime
  model: `iterations · shots · depth / CLOPS`. This is how "how long will
  this circuit hold the QPU" enters the cost function, and why a deep circuit
  avoids a 2 kCLOPS ion trap when a 150 kCLOPS superconducting device is
  feasible.

### 2.3 `QuantumBackend` (resource side)

| Field | Meaning |
|-------|---------|
| `name`, `arch`, `qubits`, `clops` | Identity and capability |
| `gate_fidelity`, `error_rate` | Quality |
| `supported_gates` | Native gate set ([] = accepts anything) |
| `calibration_downtime_pct` | Fraction of time unavailable (calibration) |
| `simulator` | True for noisy-simulator backends |

Two methods consumed by the scheduler:

- **`supports(spec)`** — the feasibility gate: qubit count, min CLOPS, arch
  match (only when both sides declare one), fidelity floor, gate-set
  inclusion. `supports(None)` is True — classical jobs are always
  quantum-feasible.
- **`quality_penalty_factor()`** — `error_rate + calibration_downtime_pct`,
  a monotone "how risky/slow is this backend" scalar consumed by the cost
  model. Both terms are probabilities-of-wasted-time (re-execution risk,
  calibration wait), which is why simple addition is defensible.

---

## 3. Job model extensions

### 3.1 Job classes

`Job.quantum` holds an optional `QuantumSpec`. `Job.job_class` derives:

```
quantum is None          -> "classical"
quantum.hybrid is False  -> "quantum"   (one-shot offload)
quantum.hybrid is True   -> "hybrid"    (variational loop)
```

For quantum/hybrid jobs, `capacities`/`wall_time` describe the **classical
component** (compile + state-prep for one-shot; the optimizer loop for
hybrid), matching the docx: "quantum jobs rely on the source classical job
for preparation tasks". The generator mirrors `spec.qubits` into
`capacities.qubits` so the capacity residual check covers qubit headroom.

`classify_job_type()` — the per-type cost tuning keys off `job_type`
substrings (`cpu_bound`, `gpu_bound`, …), so a quantum spec overrides the
resource class to `quantum`/`hybrid`. A hybrid job classifies as e.g.
`hybrid_short_dtn_light`, and the cost function's tuning branch matches on
the `quantum`/`hybrid` substring.

### 3.2 Split-pair fields (Phase 2)

| Field | Meaning |
|-------|---------|
| `sub_role` | `None` (whole job) / `"quantum"` (producer) / `"classical"` (consumer) |
| `linked_job_id` | The other half of a split pair |
| `experiment_id` | Measurement stream this job produces/consumes |
| `data_predicate` | `{experiment_id, min_snapshots, total_snapshots}` — see §10 |
| `state_data` | Stateful-pool state: `snapshots_processed`, `partial_result`, `last_stream_id` |

All serialize through `to_dict()`/`from_dict()` like every other field, so
they survive the Redis round-trips between distributor, agents, and levels of
the hierarchy.

### 3.3 A latent bug fixed on the way

`Job.from_dict()` crashed on `data_in: null` (`job_data.get("data_in", [])`
returns `None` when the key exists with a null value — and generator job
files write exactly that). Fixed with `job_data.get("data_in") or []`.

---

## 4. Agent state and heartbeats

Startup (`ResourceAgent.__init__`):

```python
self.quantum_backend = QuantumBackend.from_dict(self.config.get("quantum_backend") or {})
if self.quantum_backend and not self._capacities.qubits:
    self._capacities.qubits = self.quantum_backend.qubits
```

The backend is the single source of truth for qubit count; mirroring it into
capacities is what makes allocation arithmetic work.

`_agent_info()` builds the `AgentInfo` snapshot that heartbeats/gossip carry:

- **Leaf agents** attach `quantum_backend=self.quantum_backend` directly.
- **Parents (hierarchical)** aggregate children: `total_capacities` sums
  qubits automatically (they're in `Capacities`), `max_child_capacity` gained
  `'qubits'` in its element-wise-max field list, and the parent advertises the
  **best (max-qubit) child backend**. This is deliberately the same
  *optimistic estimate* pattern as `max_child_capacity`, with the same
  documented limitation: the peer may look feasible in aggregate while no
  single child satisfies everything. Delegation monitoring and reassignment
  recover from wrong estimates, exactly as for classical jobs.

---

## 5. Feasibility

`is_job_feasible(job, agent)` gained one check in each of its two paths:

1. **Parent self-check** (iterating actual children): after
   `_has_sufficient_capacity(job, child.capacities)`, also require
   `self._quantum_feasible(job, child.quantum_backend)`.
2. **Peer/leaf path**: after the capacity check against
   `max_child_capacity`-or-`capacities`, require
   `self._quantum_feasible(job, agent.quantum_backend)`.

`_quantum_feasible` is three lines:

```python
spec = job.quantum
if spec is None: return True
return backend is not None and backend.supports(spec)
```

Note the division of labor: **qubit count** is checked twice, and that's
intentional — once *dynamically* through the capacity residual (free qubits
right now, accounting for running jobs) and once *statically* through
`supports()` (does the hardware have that many qubits at all). The remaining
`supports()` checks (CLOPS, arch, fidelity, gates) exist only there.

Result: a quantum job on a classical-only agent is *infeasible*, never
proposed, and can never win consensus — no consensus changes needed.

---

## 6. Cost model

`compute_job_cost(job, total, dtns, backend=None, site=None)` — the two new
parameters are threaded from `_cost_job_on_agent`, which has the full
`AgentInfo`. The final formula:

```
cost = (base_score + bottleneck²) × time_penalty × connectivity_penalty
       × quantum_penalty × comm_penalty × 100
```

New/extended terms, each **inert for classical jobs** (evaluates to its
neutral value):

1. **QPU utilization in the base score.** `qpu_ratio = job.qubits /
   total.qubits` joins the weighted sum with weight
   `job_selection.cost_weights.qpu` (**default 0.0** — a config with no `qpu`
   key produces numerically identical costs to the pre-quantum code) and the
   bottleneck max. The per-job-type tuning branch mirrors the existing ones:
   for `quantum`/`hybrid` job types, `qpu_weight = max(qpu_weight, 0.1) × 1.5`
   and the others shrink ×0.7, then all weights re-normalize to sum 1 — so
   enabling `qpu` doesn't require re-tuning cpu/ram/disk/gpu.
2. **Quantum runtime in the time penalty.** The time penalty now uses
   `effective_wall_time = wall_time + spec.estimated_quantum_time(backend.clops)`.
   This is the mechanism that makes low-CLOPS backends expensive for deep
   circuits *without* a dedicated knob.
3. **Backend quality penalty.**
   `1 + quantum_penalty_factor × (error_rate + calibration_downtime_pct)`.
4. **Communication penalty (split consumers only, §9.3).**
   `split_comm_penalty(factor, total_snapshots, agent.site, producer_site)` —
   1.0 same-site or when either site is unknown; cross-site it grows with
   snapshot volume and saturates at `1 + factor`.

Load balancing across QPUs needs no extra code: a running quantum job's
qubits sit in `capacity_allocations`, raising the agent's load and shrinking
its free capacities, so the next proposal sees a busier, more expensive
agent. This is why the clean validation run placed 17/17 consumer sub-jobs
away from their producers — the producer had just made its own agent look
loaded.

---

## 7. Selection-cache correctness

`SelectionEngine` memoizes feasibility and cost by
`(candidate_key, assignee_key)` (LRU + TTL). Anything that changes the
outcome of feasibility/cost **must** appear in the signatures, or stale
values get served:

- `_job_sig` gained: `capacities.qubits`, a quantum-spec tuple
  `(qubits, depth, required_shots, iterations, hybrid, arch, fidelity, clops)`,
  `sub_role`, and a predicate tuple `(experiment_id, min_snapshots,
  total_snapshots)`.
- `_agent_sig` gained: `capacities.qubits` and a backend tuple
  `(name, arch, qubits, clops, fidelity, error_rate, downtime)`.

One subtlety: the communication penalty depends on the *producer's site*,
which is not in either signature. That would be a staleness bug — except
predicate gating (§10) guarantees a consumer is never costed before its
producer has started, and the producer's site key is written *before* the
first snapshot is published (`execute_producer` calls `announce_producer`
first). Once visible, the producer site never changes, so every cost the
cache ever computes for that job is already based on the final value.

---

## 8. Measurement data layer

`swarm/quantum/measurement_layer.py`, `MeasurementLayer`, built on **Redis
streams** (not pub/sub) for three reasons:

1. **Replay** — a consumer scheduled *after* the producer finished still
   reads everything from `0-0`. Pub/sub would have dropped the data. This is
   what made the 300s-delayed consumer in run 2 recover in 1.1s.
2. **O(1) counting** — `XLEN` implements the snapshot predicate cheaply.
3. **Cursors** — `state_data.last_stream_id` checkpoints consumption, so a
   restarted consumer resumes where it stopped instead of double-counting.

Key layout:

```
measurements:<experiment_id>        stream; 1 entry = 1 snapshot batch
                                    fields: payload (JSON), ts
measurements:<experiment_id>:site   producer's site label (comm penalty)
```

API surface (all thread-safe):

| Method | Redis op | Notes |
|--------|----------|-------|
| `announce_producer(exp, site)` | `SET ... EX ttl` | Writes `""` when the agent has no site, so "producer started" is distinguishable from "no producer" |
| `publish(exp, payload)` | `XADD` + `EXPIRE` + `XLEN` (pipelined) | Returns the new snapshot count |
| `snapshot_count(exp)` | `XLEN` | Cached `predicate_cache_s` (default 1 s) so 20 agents ticking at 2 Hz don't hammer Redis |
| `predicate_satisfied(exp, n)` | — | `snapshot_count >= n` |
| `producer_site(exp)` | `GET` | Cached forever once seen — producers never move mid-experiment |
| `read_from(exp, last_id, block_ms)` | `XREAD BLOCK` | Blocking incremental reads for consumers |

Everything carries `measurement_ttl_s` (default 1 h) so finished experiments
age out of Redis on their own.

The agent constructs one instance in `__init__` from the repository's
existing Redis client — no new connections.

---

## 9. Splitting hybrid jobs

`swarm/quantum/split.py` — deliberately **pure dict→dict functions** (no
Redis, no Job objects), so the distributor, the agents, and the unit tests
all share one implementation.

### 9.1 `split_hybrid_job(job_dict)`

Only splits jobs with `quantum.hybrid == True` that aren't already split;
returns `None` otherwise (the distributor then submits the job whole).

```
original hybrid job "37"
├── "37-q"  sub_role=quantum, linked=37-c, experiment=exp-37
│     capacities: prep footprint (≤1 core, ≤2G ram, ≤5G disk) + the qubits
│     quantum: the full spec (drives producer iterations/shots)
│     wall_time: 20% of the original
└── "37-c"  sub_role=classical, linked=37-q, experiment=exp-37
      capacities: the original classical demand, qubits stripped
      data_predicate: {experiment_id: exp-37, min_snapshots: 1,
                       total_snapshots: iterations}
      wall_time: 80% of the original; inherits data_in/data_out (DTNs)
```

Design points:

- The **quantum sub keeps the whole spec** — feasibility still routes it only
  to agents whose backend `supports()` it, and `iterations` drives how many
  snapshot batches the producer publishes.
- The **classical sub carries no spec** — it must be schedulable on *any*
  agent with classical capacity; its connection to the quantum world is
  entirely through the predicate and the stream.
- `should_fail` stays on the quantum sub only, so failure injection doesn't
  double-fail one logical job.

### 9.2 `build_post_process_job(job_id, experiment_id, output_type)`

A minimal classical consumer (`<id>-post`, prep-sized footprint,
`total_snapshots: 1`) used by the pool push (§12).

### 9.3 `split_comm_penalty(factor, total_snapshots, agent_site, producer_site)`

```
same site / unknown site / factor<=0  ->  1.0
cross-site                            ->  1 + factor · (0.5 + 0.5·min(1, total/50))
```

Cross-site placement costs at least half the factor immediately (any split
pays a latency floor) and grows with snapshot volume, saturating at
`1 + factor` around 50 batches. Sites come from the existing `site` config
label (also used for topology-aware Snow sampling); local runs without site
labels get penalty 1.0 everywhere — the knob only bites on multi-site
deployments like FABRIC.

### 9.4 Placement semantics — sequential, not joint (honest note)

The pair is **not** placed by one joint consensus decision. The quantum sub
(the scarce resource) goes through normal consensus immediately; the
classical sub *exists* in the pool but is predicate-gated until the producer
runs, then places through normal consensus with the comm penalty steering it.
This is a distributed approximation of the paper's joint placement that
matches its classical-shadows example (one-way measurement flow). True
placement-pair consensus and bidirectional VQE parameter feedback are
Phase 3.

---

## 10. Data-predicate gating

In `selection_main`, immediately after pending jobs are pulled and before the
cost matrix is built:

```python
gated = [j for j in pending_jobs if not self._data_predicate_ready(j)]
for job in gated:
    self.queues.pending_queue.move_to_end(job)
pending_jobs = [j for j in pending_jobs if j not in gated]
```

`_data_predicate_ready` returns True for jobs without a predicate, else
`measurement_layer.predicate_satisfied(experiment_id, min_snapshots)`. Gated
jobs stay `PENDING` at the back of the queue — visible in the pool, never
proposed, re-checked next tick (cheap: cached `XLEN`). On a predicate-check
error the job stays gated (fail-closed) rather than being proposed on
possibly-absent data.

Why gate at proposal time rather than in `is_job_feasible`? Feasibility is a
*capability* property (cached per job×agent); readiness is a *global,
time-varying* property (same answer for every agent, changes as data
arrives). Mixing them would poison the feasibility cache and run the Redis
check once per agent instead of once per job.

Every agent gates independently — including hierarchical parents, since the
predicate needs only Redis, which all levels reach.

Empirically the gate does **not** create a proposal thundering-herd: in the
20-agent run all agents' gates opened within the same tick, and the observed
PBFT conflict rate for gated jobs matched ordinary jobs (7 restarts / 242
jobs, of which 1 consumer).

---

## 11. Execution paths

`execute_job` dispatches on `sub_role`:

```python
if job.sub_role == "quantum":
    job.execute_producer(self.measurement_layer, site=self.site)
elif job.sub_role == "classical" and job.data_predicate:
    job.execute_consumer(self.measurement_layer,
                         timeout_s=self.consumer_timeout_s,
                         persist_cb=<save job to Redis>)
else:
    job.execute()          # classical, one-shot quantum, co-located hybrid
```

The Job methods take the layer as a **duck-typed parameter** — models stay
Redis-free, and the unit tests exercise the full producer/consumer logic with
a 40-line in-memory stub.

### 11.1 `execute_producer` (quantum sub-job)

1. `announce_producer(exp, site)` — *before* any snapshot, so the site key
   exists by the time any consumer becomes proposable (§7).
2. Sleep one step (compile/state-prep), then per iteration: sleep one step
   (circuit execution), `publish(exp, {job_id, iteration, shots})`.
3. Record `quantum_time` and `state_data.snapshots_published`; complete.

The whole simulation budget stays ~1 s regardless of iteration count
(`step = 1/(iterations+1)`), matching the classical `execute()` convention.

### 11.2 `execute_consumer` (classical sub-job / post-processing)

The stateful, data-triggered loop:

```python
processed, partial, last_id = state_data (resume support)
while processed < total_snapshots:
    entries = layer.read_from(exp, last_id, block_ms=500)
    if not entries:
        if now - last_data > timeout_s: raise TimeoutError   # producer died
        continue
    for entry_id, payload in entries:
        processed += 1; partial += payload["shots"]           # incremental update
        state_data = {snapshots_processed, partial_result, last_stream_id}
        persist_cb(self)                                      # -> Redis
```

- **Stateful pool**: `partial_result` is the simulated running estimate
  (moment/expectation update); persisting after *every* update means a crash
  loses at most one batch of progress, and a reassigned consumer resumes from
  `last_stream_id`.
- **Failure containment**: only *silence* (`timeout_s` with no new entries,
  default 60 s) fails the job — then the standard exit-status/reassignment
  machinery takes over. A slow producer just makes a slow consumer.
- **Late scheduling is free**: streams replay, so a consumer placed long
  after the producer finished reads the full history in one pass (observed:
  23 batches consumed in 1.1 s after a 300 s scheduling delay).

### 11.3 Co-located hybrid (`_execute_quantum`, Phase 1 path)

An unsplit hybrid job (`--split-hybrid` off) simulates the loop locally:
prep, then per iteration a quantum phase and a classical phase, recording
`quantum_time`. No measurement layer involved.

---

## 12. Self-expanding pool

After a *successful*, *unsplit* job finishes (checked after the
failure-simulation block so injected failures don't spawn successors),
`_maybe_push_post_process` runs for one-shot quantum jobs with
`spec.post_process`:

1. `announce_producer(exp, site)` + `publish(exp, {job_id, shots,
   output_type})` — the completed job's measurements enter the data layer,
   which immediately satisfies the successor's predicate.
2. Build `<id>-post` via `build_post_process_job`, mark it PENDING/submitted,
   `repository.save(...)` at the agent's own level/group.

No discovery plumbing was needed: agents already poll Redis for PENDING jobs
(`_update_pending_jobs`), so the pushed job enters everyone's pending queue
on the next tick and flows through normal selection. Log marker:
`[POOL_PUSH]`.

This is the docx's "quantum agents can push classical jobs to the pool" and
the first concrete instance of the paper's self-expanding DAG: *the successor
job exists only because measurement data now does.* The generator sets
`post_process: true` for one-shot jobs with `output_type: histogram`
(histograms need classical post-processing; expectation values don't).

---

## 13. Tooling pipeline

One command exercises everything (flags forwarded by `run_test.py`):

```bash
python run_test.py --mode local --agents 20 --topology mesh --jobs 200 --db-host localhost \
    --run-dir runs/quantum-test --agent-type resource \
    --quantum-agents-pct 0.25 --quantum-fraction 0.2 --hybrid-fraction 0.1 --split-hybrid
```

- **`generate_configs.py --quantum-agents-pct P`** — assigns backends
  round-robin from `QUANTUM_BACKEND_CATALOG` (aer-sim-64, heron-133,
  aqt-ion-32, neutral-atom-100 — representative, not vendor-measured) to a
  random ⌈P·N⌉ subset of agents. Hierarchical coordinators are excluded
  (leaves execute jobs). Writes `quantum_backend` + `capacities.qubits` into
  the per-agent YAML and into `agent_profiles.json`.
- **`job_generator.py --quantum-fraction / --hybrid-fraction`** — samples the
  job class per job; quantum/hybrid jobs target a random *backend-owning*
  profile and size the spec inside it (qubits ≤ backend qubits, fidelity ≤
  backend fidelity, arch pinned 50% of the time; shots explicit 50% / derived
  from (error, confidence) 50%). `--fit-all` jobs stay classical by
  construction. The feasibility CSV gains `job_class` and `qubits` columns.
- **`job_distributor.py --split-hybrid`** — `_load_jobs_from_file` returns a
  *list*: hybrid files decompose via `split_hybrid_job` into two Jobs, both
  submitted PENDING in the same batch; everything else passes through whole.

---

## 14. Configuration reference

```yaml
job_selection:
  cost_weights:
    qpu: 0.0                    # QPU utilization weight; 0.0 = classical runs unchanged
  quantum_penalty_factor: 1.0   # multiplier on backend (error_rate + downtime)

quantum:                        # Phase 2 runtime (all agents; inert without quantum jobs)
  comm_penalty_factor: 1.0      # cross-site consumer penalty (0 = ignore locality)
  consumer_timeout_s: 60.0      # stream-stall window before a consumer fails
  predicate_cache_s: 1.0        # snapshot-count cache TTL for gating
  measurement_ttl_s: 3600       # Redis TTL for streams / site keys

quantum_backend:                # only on agents that own a QPU / simulator
  name: aer-sim-64
  arch: superconducting         # superconducting | ion-trap | neutral-atom | photonic
  qubits: 64
  clops: 100000
  gate_fidelity: 0.999
  error_rate: 0.001
  calibration_downtime_pct: 0.0
  simulator: true
```

Everything defaults to off/neutral: no `quantum_backend`, no quantum jobs,
`qpu: 0.0` → the system is byte-for-byte the classical scheduler.

---

## 15. End-to-end trace of a real job

Job 37 from the clean 20-agent validation run (2026-07-14, failure sim off):

```
jobs/job_37.json     hybrid · 80 qubits · depth 55 · iterations 5
                     error 0.023 @ 90% -> required_shots() = 1279

distributor          split_hybrid_job -> 37-q, 37-c   (exp-37)

37-q                 feasible only on backend agents with >=80 qubits
                     consensus -> agent 9 (heron-133, 133 qubits)
                     execute_producer: announce site, prep,
                     5 x publish(exp-37, {shots: 1279})

37-c                 in pool, gated (0 snapshots < min 1) ... snapshot 1 lands
                     gate opens on every agent's next tick
                     consensus -> agent 7 (classical; producer's agent looks
                     loaded from the allocation ledger, so consumer goes elsewhere)
                     execute_consumer: 5 incremental updates,
                     partial_result 6395 = 5 x 1279, persisted each step
                     completes 0.86 s after the producer
```

Run-level numbers (200 jobs): 0 failures, 0 timeouts, 0 pending at shutdown;
17/17 split pairs completed, all 17 cross-agent; consumer lag median 1.5 s /
max 4.0 s; 26/26 pushed post-processing jobs completed. Record arithmetic:
200 − 17 hybrids + 34 sub-jobs + 26 pushed = 243.

---

## 16. Tests

`python -m pytest tests/test_quantum.py tests/test_quantum_phase2.py` (41
tests; full suite 157).

- **`test_quantum.py`** (Phase 1): spec/backend serialization round-trips,
  shots-from-error/confidence math, `supports()` per dimension (qubits,
  CLOPS, arch, fidelity, gates), `Capacities.qubits` arithmetic and
  backward-compat, `job_class`/classification, Job round-trips, generator
  targeting and `--fit-all` behavior.
- **`test_quantum_phase2.py`**: split pair structure (ids, links, predicate
  totals, capacity split), non-splittable inputs, sub-job round-trips through
  the Job model, post-process builder, comm-penalty monotonicity/saturation,
  and the full producer/consumer streaming loop against an in-memory stub
  layer — including concurrent producer+consumer, stalled-stream timeout with
  surviving partial state, and per-update `persist_cb`. `MeasurementLayer`
  itself is exercised against `fakeredis` (skipped if not installed; it is
  installed in `SwarmAgents/.venv`).

Heavy agent-level paths (`ResourceAgent` construction needs Redis/gRPC) are
covered by keeping the logic in pure functions the agent merely calls —
`supports`, `split_hybrid_job`, `split_comm_penalty`, the Job execute methods
— plus the live validation runs.

---

## 17. Known limitations and gotchas

1. **Sequential placement, not joint.** See §9.4. The consumer can also
   currently be placed on the producer's agent if that's still cheapest —
   nothing *forces* a split, the allocation ledger just usually prefers one.
2. **Job-count inflation.** `--split-hybrid` (+1 per hybrid) and pool pushes
   (+1 per post-process) make completed counts exceed submitted counts; runs
   shut down on the *submitted* count, so late extras can be left pending
   (observed: pair 39 in run 2). Interpret `all_jobs.csv` accordingly.
3. **`mab.failure_simulation` is independent of `mab.enabled`.**
   `resource_agent` reads the failure-simulation block regardless of whether
   the bandit is on. To stop injected failures you must set
   `mab.failure_simulation.enabled: false`.
4. **Job-level logs don't reach agent logs.** `Job` uses its own `"Job"`
   logger (pre-existing); producer/consumer INFO lines are invisible in
   `agent-<id>.log`. Grep for `[POOL_PUSH]`/`[EXECUTE]` (agent logger) or
   inspect `state_data` in Redis instead.
5. **Comm penalty needs site labels.** Local runs have `site: null`
   everywhere → penalty 1.0. Use `--agent-sites-file` (or FABRIC configs) to
   exercise it.
6. **Post jobs are pushed at the executing agent's level/group.** In mesh
   (level 0/group 0) that's the global pool; in hierarchies the successor
   stays in the pusher's group rather than re-entering at the top.
7. **No re-entry semantics.** The paper's "keeps updating until condition c"
   is implemented as run-to-`total_snapshots` with persisted partial state; a
   *completed* job does not currently reactivate when new data arrives.
8. **Simulated execution.** Producers/consumers sleep and count; no circuits
   run. The OpenQASM/Aer pipeline is Phase 3 — `QuantumSpec.gates`/`arch`
   already carry the metadata it needs.
