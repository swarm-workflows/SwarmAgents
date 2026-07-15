# Hybrid Quantum-Classical Job Support

Design and implementation notes for scheduling jobs with classical and quantum
components in SwarmAgents. For a detailed code-level walkthrough (module by
module, execution flows, cache-correctness reasoning, gotchas), see
`QUANTUM_HYBRID_IMPLEMENTATION.md`. Grounded in two sources:

- *Data-triggered Hybrid Quantum-Classical Runtime* (Herbst & De Maio, April 2026)
  — the runtime model: a Quantum Measurement Data Layer with data predicates, a
  Stateful Job Pool, a self-expanding Dynamic Dependency Graph, and a
  resource-aware heterogeneous scheduler.
- *QBrainstorming* (Q-DISTRI architecture notes) — the concrete job/resource
  taxonomy: classical, quantum, and hybrid jobs; quantum resource attributes
  (CLOPS, gate fidelity, qubit count, error rate, calibration downtime);
  quantum job attributes (qubits, shots, architecture, requested accuracy).

## Job taxonomy

| Class | Representation | Semantics |
|-------|----------------|-----------|
| `classical` | `Job` without a `QuantumSpec` | Existing behavior, unchanged |
| `quantum` | `Job.quantum` set, `hybrid=False` | One-shot circuit offload. The classical component (`capacities`, `wall_time`) models circuit compilation and state preparation ("quantum jobs rely on the source classical job for preparation tasks"); the circuit then runs once on the backend |
| `hybrid` | `Job.quantum` set, `hybrid=True` | Variational-style loop: `iterations` rounds of circuit execution, each producing measurement data that triggers the classical update step. Both components are co-scheduled on one agent (Phase 1) |

`Job.job_class` derives the class; `classify_job_type()` prefixes the job type
with `quantum_`/`hybrid_` so the cost model's per-type weight tuning reacts to
quantum jobs the same way it reacts to `cpu_bound`/`gpu_bound` ones.

## Data model

### `swarm/models/quantum.py`

- **`QuantumSpec`** (job side): `qubits`, `circuit_depth`, `shots`, minimum
  `clops`, preferred `arch`, minimum `fidelity`, target `error`/`confidence`,
  `output_type` (expectation | histogram), `hybrid`, `iterations`, `gates`.
  - `required_shots()` — explicit shots, or derived from the requested
    statistical accuracy: `n >= z²/(4·error²)` (binomial worst case), matching
    the brainstorming doc's "ERROR, CONFIDENCE determines the necessary number
    of shots".
  - `estimated_quantum_time(backend_clops)` — CLOPS circuit-runtime model:
    `iterations · shots · depth / CLOPS`.
- **`QuantumBackend`** (resource side): `name`, `arch`, `qubits`, `clops`,
  `gate_fidelity`, `error_rate`, `supported_gates`,
  `calibration_downtime_pct`, `simulator`.
  - `supports(spec)` — feasibility: qubit count, minimum CLOPS, architecture
    preference, fidelity floor, gate-set inclusion.
  - `quality_penalty_factor()` — `error_rate + calibration_downtime_pct`,
    consumed by the cost model.

### Split between `Capacities` and `QuantumBackend`

The **additive** quantum resource (qubit count) lives in `Capacities.qubits`,
so every existing mechanism — `_has_sufficient_capacity`, allocation
accounting in `can_schedule_job`, hierarchical aggregation
(`total_capacities`, `max_child_capacity`), Redis serialization — covers
qubits with no special cases. **Non-additive quality attributes** (CLOPS is a
rate, fidelity/error are probabilities, downtime is a schedule property) live
on the backend descriptor attached to `AgentInfo`, mirroring how DTNs are
modeled. Old serialized `Capacities`/`AgentInfo` blobs deserialize cleanly
(missing fields default to 0/None).

## Scheduling integration (`resource_agent.py`)

- **Config**: agents read an optional `quantum_backend:` section; the qubit
  count is mirrored into `capacities.qubits` at startup. Classical-only
  deployments need no config change.
- **Heartbeats**: leaf agents include their backend in `AgentInfo`. Parents
  aggregate the *best (max-qubit) child backend* — an optimistic estimate with
  exactly the documented `max_child_capacity` limitation; delegation
  monitoring recovers from wrong estimates.
- **Feasibility** (`is_job_feasible`): classical checks unchanged; jobs with a
  quantum spec additionally require `backend.supports(spec)` — on the child
  being examined (parent self-check) or on the peer's aggregated backend.
- **Cost** (`compute_job_cost`): three additions, all inert for classical jobs:
  1. A `qpu` cost weight (`job_selection.cost_weights.qpu`, default **0.0** —
     classical runs are numerically identical to before) weighs
     `job_qubits / agent_qubits` in the base score and bottleneck penalty.
     Per-type tuning boosts it for `quantum`/`hybrid` job types.
  2. The execution-time penalty uses `wall_time +
     spec.estimated_quantum_time(backend.clops)`, so slow (low-CLOPS) backends
     are penalized for long circuits — this is where an ion trap loses a
     deep-circuit job to a superconducting device despite better fidelity.
  3. A backend-quality penalty `1 + quantum_penalty_factor · (error_rate +
     calibration_downtime_pct)` prices re-execution and calibration-wait risk.
- **Selection caches**: `_job_sig`/`_agent_sig` include the quantum spec and
  backend signatures, so the LRU feasibility/cost caches stay correct.
- **Consensus is untouched** — quantum jobs flow through PBFT/Snow like any
  other job; only feasibility and cost change, which is exactly the
  decoupling the adapter architecture is for.

## Execution simulation (`Job._execute_quantum`)

Hybrid jobs simulate the data-triggered loop: classical prep, then per
iteration a quantum phase (collect `required_shots()` measurements — the shot
predicate) followed by the classical update phase. Total simulated sleep stays
~1s to match classical jobs; the quantum share is recorded in
`Job.quantum_time` and persisted for analysis.

## Tooling

- `generate_configs.py --quantum-agents-pct 0.2` assigns backends (round-robin
  from a catalog of representative simulator/hardware profiles across
  architectures) to a random subset of execution-capable agents, never to
  hierarchical coordinators.
- `job_generator.py --quantum-fraction 0.2 --hybrid-fraction 0.1` generates
  quantum/hybrid jobs sized to fit backend-owning agents; the feasibility CSV
  gains `job_class`/`qubits` columns. `--fit-all` jobs stay classical.
- `job_distributor.py` carries the `quantum` spec into Redis.
- Sample `agent_profiles.json` gives agents 1 and 3 backends (superconducting
  simulator, ion trap).

Example end-to-end run (one command; run_test forwards the quantum flags to
generate_configs.py / job_generator.py / job_distributor.py):

```bash
python run_test.py --mode local --agents 20 --topology mesh --jobs 200 --db-host localhost \
    --run-dir runs/quantum-test --agent-type resource \
    --quantum-agents-pct 0.25 --quantum-fraction 0.2 --hybrid-fraction 0.1 --split-hybrid
```

Omit `--split-hybrid` for Phase 1 behavior (hybrid jobs co-located on one
agent). Manual pipeline equivalent:

```bash
python generate_configs.py 20 20 config_swarm_multi.yml configs mesh localhost 200 --quantum-agents-pct 0.25
python job_generator.py --job-count 200 --agent-profile-path agent_profiles.json \
    --output-dir jobs/ --quantum-fraction 0.2 --hybrid-fraction 0.1
python job_distributor.py --redis-host localhost --jobs-dir jobs/ --jobs-per-interval 10 --split-hybrid
```

Unit tests: `python -m pytest tests/test_quantum.py tests/test_quantum_phase2.py`.

## Phase 2 (implemented): data-triggered split scheduling

Mapped against the runtime paper's components. Enable end-to-end with
`run_test.py ... --split-hybrid` (plus the Phase 1 flags).

1. **Quantum Measurement Data Layer** *(paper: pub-sub measurement
   collection)* — `swarm/quantum/measurement_layer.py`. Redis streams keyed
   by experiment (`measurements:<exp>`), one entry per snapshot batch, with
   TTL, a producer-site key for locality, and cached snapshot counts.
2. **Data predicates** *(paper: "j₂ starts when 2 snapshots are available")*
   — jobs carry `data_predicate: {experiment_id, min_snapshots,
   total_snapshots}`. `selection_main` gates predicate-unsatisfied jobs at
   the back of the pending queue: they exist in the pool but are not
   proposed until the data does.
3. **Split co-scheduling with communication cost** *(brainstorming: "the two
   sub-jobs must be scheduled jointly, considering communication time")* —
   `swarm/quantum/split.py` decomposes a hybrid job into `<id>-q` (quantum
   spec + prep footprint, measurement *producer*) and `<id>-c` (classical
   demand, stream *consumer*, predicate-gated). Enabled via
   `job_distributor.py --split-hybrid`. The consumer's cost includes
   `split_comm_penalty` — cross-site placement pays a penalty scaling with
   snapshot volume (saturating at `1 + quantum.comm_penalty_factor`); site
   labels come from the producer-site key, which predicate gating guarantees
   exists before the consumer is ever costed.
   *Honest scope note*: this is **sequential placement** (quantum first, the
   scarce resource; classical follows, steered by data availability and
   locality), a distributed approximation of the paper's joint placement.
   It matches the paper's classical-shadows example, where measurement data
   flows one way into classical post-processing. True placement-pair
   consensus and bidirectional VQE-style feedback remain future work.
4. **Stateful job pool** *(paper: per-job internal state, update rules)* —
   `Job.state_data` persists `snapshots_processed`, `partial_result`, and
   the stream cursor; consumers run one incremental update per batch and
   persist after each (via `persist_cb` → Redis), so partial results survive
   agent failure. Stream stalls beyond `quantum.consumer_timeout_s` fail the
   job into the existing reassignment machinery.
5. **Self-expanding pool** *(paper: dynamic dependency graph; brainstorming:
   "quantum agents can push classical jobs to the pool")* — after a one-shot
   quantum job with `post_process` completes, the executing agent publishes
   its measurements and pushes `<id>-post` (predicate-gated classical job)
   into the pool (`[POOL_PUSH]` in logs). Agents discover it through the
   normal Redis polling path — the successor job exists only because
   measurement data now does. A generalized workflow agent (dagman-like,
   e.g. moment-order-4 after orders 2–3) is the natural extension.

Config (`quantum:` in `config_swarm_multi.yml`): `comm_penalty_factor`,
`consumer_timeout_s`, `predicate_cache_s`, `measurement_ttl_s`.
Tests: `tests/test_quantum_phase2.py` (split, predicates, streaming
producer/consumer, comm penalty, measurement layer on fakeredis).

Note on job counting: `--split-hybrid` turns each hybrid job into two jobs,
and post-processing pushes add jobs mid-run, so completed-job counts exceed
the submitted count.

## Phase 3 roadmap (not implemented)

1. **Joint placement-pair consensus & VQE feedback** — co-decide both halves
   of a split in one consensus round; add the classical→quantum parameter
   stream for true variational loops.
2. **Generalized workflow agent** — dagman-like materialization of DAG
   successors from data-availability rules (beyond single post-process jobs).
3. **Real execution pipeline** *(brainstorming pipeline option 2)* — OpenQASM
   3.0 as lingua franca, t|ket normalization, noise-model simulators
   (Aer/cuQuantum), then hardware (AQT, LRZ, RPI). `QuantumSpec.gates`/`arch`
   already carry the metadata.
4. **Deadline & failure management** — deadline-aware cost shaping;
   re-execution vs. mitigation policies for noisy backends.
5. **At-scale evaluation** — FABRIC/KISO deployment with site labels driving
   the communication penalty.
