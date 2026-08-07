# Decentralized Job Pool Design

**Status:** proposed (design only — no implementation yet)
**Depends on:** SWIM membership, gossip dissemination, Snow engine (all shipped — see `GOSSIP_CONSENSUS_DESIGN.md`)

## Motivation

Redis is the last centralized piece of the system. Consensus (PBFT or Snow),
selection, and failure detection are already peer-to-peer, but every job still
enters, lives in, and is claimed from a central store. This document proposes
replacing the Redis **control plane** with peer-to-peer mechanisms, phase by
phase, while deliberately keeping Redis (or files) for the **experiment
harness** — metrics, plotting, and test orchestration are observers, not part
of the system under test.

## What Redis does today

| # | Role | Where | Replacement (phase) |
|---|------|-------|---------------------|
| 1 | Job ingestion | `job_distributor.py` → `Repository.save` | Submit-to-any-agent RPC (P1) |
| 2 | Pending-pool discovery | agents poll `state:<level>:<group>:PENDING` via `get_all_objects` | Epidemic job announcements (P2) |
| 3 | Exactly-once finalization | `try_claim_assignment` (`SET NX`), used by Snow | Rendezvous-hash referee (P3) |
| 4 | Job state persistence | `repository.save` per transition, `WATCH` locking, reselection scans | Owner + replica records, epoch fencing (P4) |
| 5 | Agent registry / heartbeats | registry-SET discovery, `save_fast` | SWIM authoritative + profile gossip (P5) |
| 6 | Quantum measurement layer | Redis streams (`swarm/quantum/measurement_layer.py`) | Direct producer→consumer gRPC stream (P5) |
| 7 | Harness: metrics, `dump_db`, dynamic-agent triggers, completion detection | plotting, `run_test.py` | **Stays centralized** (or per-agent files + `collect_logs`) |

## Design principles

- **Control plane vs. harness.** Only roles 1–6 must decentralize. The harness
  may keep Redis; it must simply never be load-bearing for scheduling.
- **Reuse the existing stack.** SWIM provides the live view and failure
  callbacks; the disseminator provides periodic fanout with versioned merge;
  Snow/PBFT stay untouched above the pool.
- **One envelope.** All new messages are new `MessageType` payloads on the
  existing `ConsensusService.SendMessage(ConsensusMessage)` RPC — no new
  services, one new RPC for job submission only.
- **Each phase ships alone** behind a feature flag and is A/B-comparable to the
  Redis baseline with the unchanged test harness.

## Phase 1 — Submit-to-any-agent ingestion

New RPC (the only proto service change in the whole plan):

```proto
service ConsensusService {
  rpc SendMessage(ConsensusMessage) returns (Ack);
  rpc SubmitJob(JobSubmission) returns (SubmitAck);   // NEW
}

message JobSubmission {
  string job_json = 1;   // same schema as jobs/job_*.json
}
message SubmitAck {
  bool accepted = 1;
  uint32 origin_agent = 2;   // agent that took ownership
  string job_id = 3;
}
```

- `job_distributor.py` gains `--target p2p`: instead of writing Redis it
  submits each job to a random live agent (retrying on a different agent if
  refused), at the same paced rate.
- The receiving agent becomes the job's **origin**: it stores the record
  locally (in-memory + append-only journal file for crash recovery), stamps
  `origin_agent` and `epoch = 0`, and is responsible for re-announcing the job
  until it observes an assignment (Phase 3) or a completion (Phase 4).

## Phase 2 — Epidemic pending-pool

Two new gossip payloads, piggybacked on the existing disseminator rounds
(`gossip.fanout` peers every `gossip.period_ms`):

```python
class JobAnnounce(Message):
    entries: List[JobAnnounceEntry]

@dataclass
class JobAnnounceEntry:
    job_id: str
    origin_agent: int
    epoch: int                 # increments on reassignment (Phase 4)
    state: int                 # PENDING while unassigned
    capacities: dict           # core/ram/disk/gpu/qubits digest
    dtn_names: List[str]       # data-locality hints for feasibility
    wall_time: float
    version: int               # per-origin monotonic, for merge

class PoolDigest(Message):     # anti-entropy, piggybacked on SWIM ping/ack
    ids_hash: bytes            # hash of sorted pending job_ids
    count: int
```

- Each agent maintains a **local pending pool** (extends `swarm/queue/`),
  deduped by `job_id`, merged by `(epoch, version)` — same merge discipline the
  disseminator already uses for `AgentStateEntry`.
- On `PoolDigest` mismatch during SWIM ping exchange, peers swap sorted id
  lists and request missing entries — this repairs any announcement a crashed
  or partitioned agent missed.
- The selection engine reads the local pool instead of
  `get_all_objects(state=PENDING)`. Job bodies above the digest fields travel
  once, on first announce; the full JSON is fetched from the origin on demand
  (`JobFetch` / `JobFetchReply` payloads) if a proposal needs it.
- Expected visibility: O(log n) gossip rounds. With defaults (fanout 3,
  period 500 ms), ≈ 1.5–2.5 s at n = 100 — measured against today's 0.5 s
  Redis poll as part of evaluation.

## Phase 3 — Exactly-once assignment (the crux)

PBFT already has quorum finality — on that path the Redis claim is dropped
outright. Snow finalizes on local β-confidence, so it needs a replacement for
the atomic claim.

### 3a. Rendezvous-hash referee (primary design)

```
referee(job_id)   = argmax over live agents a of H(job_id, a)   # HRW hashing
successors(job_id) = next r agents by H(job_id, a), r = 2
```

```python
class ClaimRequest(Message):
    job_id: str
    epoch: int
    claimant: int          # Snow winner requesting the grant
    cost: float            # for deterministic tiebreak

class ClaimGrant(Message):
    job_id: str
    epoch: int
    winner: int            # granted claimant (may differ from requester)
    referee: int
```

- The Snow winner sends `ClaimRequest` to `referee(job_id)`; the referee
  grants the first request per `(job_id, epoch)`, replicates the grant to the
  `r` successors, then replies. One gRPC RTT in the common case — comparable
  to the Redis `SET NX` round-trip it replaces.
- Duplicate requests get the recorded winner back (idempotent); losers abort
  their local finalization, exactly as `try_claim_assignment == False` does
  today.
- **Referee failure:** SWIM's `on_agent_failed` promotes the first successor
  (which holds the replicated grant log for its key range). A claim caught in
  the ambiguity window is retried with the same `(job_id, epoch)` and remains
  idempotent. Membership churn shifts HRW ownership; a short grant history
  (TTL ≈ 2× SWIM detection time) is handed off to the new referee on change.
- **Partitions:** a referee only grants while its SWIM live count ≥ the
  dynamic quorum `ceil((n+1)/2)` — the minority side freezes new claims
  rather than double-granting.

### 3b. Pure Snow finality + conflict repair (research variant)

No referee: rely on β-confidence alone with a deterministic tiebreak
(`(cost, agent_id)` ascending), gossip `AssignmentNotice` post-finalization,
and abort-on-conflict (the tiebreak loser kills its execution if a conflicting
notice arrives within a grace window). Cheaper by one RTT, but exactly-once
becomes probabilistic. Implementing both and quantifying 3b's double-assign
rate vs. α/β and churn is a natural experiment for the Snow/gossip paper line.

## Phase 4 — Replicated job state and failure recovery

- The assigned agent **owns** the job record; every state transition appends
  to a local journal and is replicated (write-behind, piggybacked on gossip
  rounds) to the same `successors(job_id)` set as `StateReplica` entries:

```python
class StateReplica(Message):
    entries: List[JobStateEntry]     # (job_id, epoch, state, owner, version, ts)
```

- **Owner failure:** SWIM `on_agent_failed(owner)` fires at the successors;
  whichever successor is `referee(job_id)` re-announces the job as PENDING
  with `epoch + 1`. Stale writes from a resurrected owner carry the old epoch
  and are rejected everywhere — **epoch fencing replaces the Redis `WATCH`
  optimistic locking** and the `reselection_timeout_s` Redis scans.
- Completion: owner gossips a terminal `JobStateEntry(COMPLETE)`; the origin
  stops re-announcing and releases its journal entry after the entry has been
  visible for `gossip.state_ttl_s`.

## Phase 5 — Registry and measurement layer

- **Membership:** SWIM becomes authoritative (today heartbeat + Redis is
  authoritative and SWIM advisory). Agent capacity/DTN profiles ride the
  existing `GossipState` entries; topology neighbor construction reads the
  SWIM view instead of the registry SET.
- **Quantum split jobs:** the producer/consumer pair is known at co-schedule
  time — replace the Redis stream with a direct gRPC stream between them,
  keeping the existing sequence numbers so `Job.data_predicate` gating is
  unchanged. Snapshot-on-reconnect covers consumer restarts.

## Configuration

```yaml
pool:
  backend: redis            # redis | p2p — global switch, per-phase overrides:
  ingestion: redis          # redis | rpc            (Phase 1)
  discovery: redis          # redis | gossip         (Phase 2)
  claim: redis              # redis | referee | none (Phase 3; none = variant 3b)
  state: redis              # redis | replicated     (Phase 4)
  replicas: 2               # r successors for grants + state
  grant_ttl_s: 30           # referee grant-log handoff window
```

`P2PRepository` implements the existing `Repository` methods that have a
sensible peer-to-peer meaning (`save`, `get`, `get_all_objects(PENDING)`,
`try_claim_assignment`, `get_assignment`) so `resource_agent.py` is mostly
untouched; methods that are harness-only (`delete_all`, dump paths) raise
unless `backend: redis`.

## Evaluation plan

Baseline vs. p2p at each phase, same harness (`run_test.py`, Pegasus replay
via `--pegasus-profiles`, `kill_agents.py` chaos):

| Metric | Target |
|--------|--------|
| Exactly-once violations (3a) | 0, incl. referee kills mid-claim |
| Double-assign rate (3b) | measured vs. α/β/churn — paper result |
| Job loss | 0 with ≤ r simultaneous failures |
| Time-to-visibility (submit → 95% of agents) | ≤ 2× Redis-poll baseline |
| Selection latency | within noise of baseline |
| Messages/agent/s | vs. ~17 Redis ops/agent/s measured at Hier-250 |
| Makespan / completion rate | parity on flat-mesh 10–120 Snow suite |

## Risks

- **Duplicate execution** — referee grants + epoch fencing + abort-before-
  side-effects; the flat-1s execution stub currently masks this class of bug,
  so chaos tests must run with `wall_time_scale` > 0 (ROADMAP item 7).
- **Job loss** — origin journal + re-announce + anti-entropy digests; origin
  death before first announce loses the job unless the submitter retries on
  `SubmitAck` timeout (distributor does).
- **Churn-heavy HRW reshuffling** — grant-log TTL handoff bounds the exposure;
  SWIM suspicion (not just failure) can pre-warm successors.
- **Gossip load at scale** — announcements are digests, bodies fetched once;
  fanout/period knobs and the measured headroom from the scalability phases
  apply.

## Open questions

1. Hierarchical topology: does the pool gossip span all of L0, or per-group
   with parents bridging announcements (matches the Phase-4 hybrid engine)?
2. Journal durability: in-memory + file is fine for experiments; is a
   restart-rejoin (ROADMAP item 14) required before this is production-shaped?
3. Should origin agents apply admission control (refuse when the live pool
   exceeds a bound) to replace the distributor's pacing role?
