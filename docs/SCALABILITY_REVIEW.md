# Critical Scalability & Performance Review (2026-07-02)

> Full-codebase review targeting 100–1000+ agents. Grounded in (a) code inspection of the data,
> runtime, and communication layers, and (b) this month's empirical testbed results (Mesh-120 PBFT
> livelock; Snow driver starvation fixed in `7208ee90`; see `CONSENSUS_SCALING_PLAN.md`,
> `SNOW_DRIVER_REARCH.md`). Findings marked **[verified]** were confirmed by direct code reading;
> others are from subsystem review and should be confirmed with the Phase-0 instrumentation below.

## Executive summary

The system is sound at ≤30 agents but has four structural walls at 100+:

1. **Correctness bug in PBFT message handling** — out-of-order proposals crash the dispatch batch
   and are silently dropped (no `set_pending_*` in ResourceAgent). Likely a root contributor to
   reselection storms.
2. **Serial, blocking PBFT broadcast** — one dead peer costs ~8.7s *per phase* (2s timeout × 4
   retries + backoff), ×3 phases per job. The exact bug class we already fixed for Snow (task #4),
   still present in the PBFT path.
3. **Redis as a polling bus** — every agent SCANs and re-reads full JSON objects every 500ms tick;
   full-object rewrites under WATCH; N sequential GETs where one MGET would do. Single Redis
   saturates well before 1000 agents.
4. **Cache that never hits** — selection cost/feasibility LRU keys include agent versions that bump
   every tick, so the O(jobs × agents) cost matrix is recomputed nearly every iteration.

Everything below is ranked; the plan at the end sequences fixes by (impact ÷ effort), with
measurement first — this month's lesson is that instrumentation (`[SNOW_TIMING]`) found the real
bottleneck after parameter tuning failed.

---

## Findings by layer

### A. Consensus / correctness

| # | Finding | Where | Impact |
|---|---|---|---|
| A1 | **[verified] Missing `set_pending_{proposal,prepare,commit}` in ResourceAgent `_HostAdapter`.** `ConsensusHost` is a Protocol (no defaults); Colmena implements these, ResourceAgent doesn't. When a Proposal/Prepare/Commit arrives before the job is fetched from Redis, `engine.py:104/150/237` raises `AttributeError`; the blanket `except` at `resource_agent.py:608` logs and drops it — **aborting the remaining batch in that message** (`on_proposal` loops proposals). No replay exists. | `swarm/consensus/interfaces.py:44-46`, `engine.py:104,150,237`, `resource_agent.py:608` | Quorum votes silently lost → re-proposal storms; corroborated by the paper's 2.0–2.3× job-log multiplier and PBFT fragility at scale. |
| A2 | **[verified] Unbounded `engine.conflicts` dict** — incremented, never cleaned. | `swarm/consensus/engine.py:50` | Slow memory growth in long runs. |
| A3 | ProposalContainer `prepares`/`commits` lists grow O(N) per proposal per job; fine at 10-agent groups, heavy at flat 100+. | `swarm/consensus/messages/proposal_info.py` | Memory + JSON-payload bloat at scale. |
| A4 | Fixed `reselection_timeout_s` doesn't scale with broadcast latency; when broadcast is slow (B1) jobs recycle every timeout → livelock (observed empirically: Mesh-110/120 PBFT). | `config_swarm_multi.yml` runtime | Thrash/livelock at flat scale. |

### B. Communication layer

| # | Finding | Where | Impact |
|---|---|---|---|
| B1 | **[verified] Serial blocking broadcast with retries.** `broadcast()` iterates peers serially; each `call_unary(timeout=2.0, retries=4)` → ~8.7s worst case per dead/slow peer, ×3 PBFT phases. Snow got a best-effort pool in `7208ee90`; **PBFT/gossip/SWIM sends did not**. | `grpc_transport.py:89-110`, `grpc_client.py:165-181` | The dominant PBFT latency lever; directly explains timeout cascades. |
| B2 | Broadcast doesn't consult the failed-agent/SWIM live set — keeps paying full retry cost for known-dead peers every phase. | `grpc_transport.py:99` loop | Multiplies B1 under churn. |
| B3 | Double serialization: dataclass→dict→`json.dumps`→protobuf string field; parsed back with `json.loads` per message. | `consensus.proto` (payload=string), `grpc_transport.py:73`, `grpc_server.py` | ~2× encode cost + larger wires; CPU tax on the single inbound thread (C1). |
| B4 | Unbounded inbound `queue.Queue()`; no backpressure. Under consensus bursts the queue grows without limit. | `agent_grpc.py:35,228` | OOM risk under storms; hides overload instead of shedding. |
| B5 | Channel pool is good (persistent per-(host,port), keepalives) but has no eviction for departed agents. | `grpc_client.py:65-149` | Minor leak under churn; fine otherwise. |
| B6 | Ring/Star forwarding re-broadcasts (O(N×diameter) messages). Known/intended; just avoid these topologies at scale. | `engine.py:141` | Documented footgun. |

### C. Agent runtime / compute

| # | Finding | Where | Impact |
|---|---|---|---|
| C1 | Single inbound consumer thread does JSON parse + dispatch for every message serially. | `agent_grpc.py:190-235` | Message-processing ceiling; interacts with B3. |
| C2 | **Selection cache likely near-zero hit rate**: cache key includes `assignee_version` (falls back to `updated_at`), which changes every 500ms agent-info save → every tick invalidates every (job, agent) entry → the O(jobs×agents) matrix recomputes from scratch. No hit/miss counters exist to see this. | `selection/engine.py` key fn, `resource_agent.py` selector wiring (~line 328-346) | Selection loop CPU dominates as agents×jobs grow; also GIL pressure on co-hosted agents. |
| C3 | `ThreadSafeDict` = one RLock per dict; `keys()`/`values()` copy under lock. `neighbor_map` is read in selection loop + inbound path + refreshed periodically. | `swarm/utils/thread_safe_dict.py` | Lock contention at high message rates; secondary to C1/C2. |
| C4 | Fixed 0.5s polling everywhere (`on_periodic`, selection loop sleep); no event-driven wakeups. Wastes CPU when idle, adds latency when busy. | `agent_grpc.py:178`, `resource_agent.py` selection loop | Latency floor + density cost (3 agents/host). |
| C5 | Cost function does per-(job,agent) string matching on `job_type` and re-derives DTN penalties; uncached inner loop. | `resource_agent.py:1476-1599` | Multiplies C2's recompute cost. |

### D. Data layer (Redis)

| # | Finding | Where | Impact |
|---|---|---|---|
| D1 | **Discovery via `scan_iter` in the 500ms refresh loop** — every agent SCANs `agent:{level}:{group}:*` (and parents scan children) every tick; redis-py restarts the cursor each call. Keys are *deterministic* (agents known per level:group) — SCAN is unnecessary. | `repository.py:223-226`, `resource_agent.py:1028-1115` | Redis CPU scales O(A × keyspace); the top data-layer cost at 100+. |
| D2 | **Full-object JSON rewrite per save under WATCH/MULTI** — agent-info (2–5KB) fully rewritten every tick; job saves re-serialize whole objects; WATCH retry loop on contended job keys. Note: agent-info keys are single-writer, so WATCH there is pure overhead. | `repository.py:65-117`, `resource_agent.py:1269` | Bandwidth + client CPU + retry storms on hot job keys. |
| D3 | `_update_pending_jobs` does N sequential GETs (one per pending job) though `mget` exists in the same file. | `resource_agent.py:612-625` | Multi-RTT latency per tick, trivially batchable. |
| D4 | `_monitor_delegated_jobs` does per-job-per-child-group GETs, twice on some paths; no pipeline. | `resource_agent.py:689-843` | Hierarchical parents hammer Redis; contributes to coordinator-tier latency (open item). |
| D5 | **[verified] Latent bug: glob pattern passed to `SMEMBERS`** (`state:{level}:*:{state}`) — globs don't work in SMEMBERS; returns empty. Unreached today (callers pass group). | `repository.py:217` | Silent empty results if cross-group queries are ever used. |
| D6 | No TTLs on agent/job keys → stale keys accumulate, slowing SCANs (compounds D1) and requiring manual flush. | `repository.py` save() | Operational + compounds D1. |
| D7 | Good parts to keep: state secondary indices (`sadd`/`srem` on transitions), pipelined `get_all_ids_multi`, `SET NX` assignment claims. | — | Don't re-invent. |

---

## Improvement plan (phased, impact-ordered)

**Principle: instrument → fix → re-measure on the testbed** (Hier-60 hybrid + Mesh-120 are the
established benchmarks with recorded baselines).

### Phase 0 — Correctness + instrumentation (small effort, do first) — ✅ SHIPPED `38ed92d2`

Delivered 2026-07-02, deployed to all 40 agents: `_set_pending_safe` guard in the engine +
`set_pending_*` stash/replay in ResourceAgent (regression tests `tests/test_pbft_pending.py`);
conflicts dict capped; inbound queue bounded (20k) with drop counter; SMEMBERS glob bug fixed;
counters live (selection-cache hit rate, WatchError retries, broadcast latency + `[BCAST_SLOW]`,
per-agent `[STATS]` line every ~30s). Next run's `[STATS]` output decides Phase 1 vs Phase 2 order.
1. **Implement `set_pending_*` in ResourceAgent `_HostAdapter`** (mirror Colmena) + replay pending
   messages when the job arrives in `_update_pending_jobs`. Also make `engine.on_proposal` resilient
   so one bad proposal can't abort the batch. *Expected: fewer lost votes → fewer re-proposals;
   measurable as a drop in the job-log/reselection multiplier.*
2. Fix D5 (raise or iterate groups explicitly). Bound `engine.conflicts` and the inbound queue
   (maxsize + drop-oldest counter, A2/B4).
3. **Counters** (cheap, decisive): selection-cache hit/miss, inbound queue depth, per-broadcast
   latency histogram, Redis WatchError rate, `sends_dropped` (exists for Snow). Export via Metrics.

### Phase 1 — Comm layer: parallel best-effort PBFT fan-out (highest latency leverage)
4. **Parallelize `broadcast()`** with the bounded send-pool pattern already proven for Snow
   (`7208ee90`): concurrent per-peer sends, short timeout, retries in background — never serially
   block a consensus phase on one peer. PBFT still gets its quorum from responses, so per-send
   reliability can be best-effort + one background retry.
5. **Skip known-failed peers** (consult `failed_agents`/SWIM live set) in broadcast.
6. Scale `reselection_timeout_s` with live peer count (or make it adaptive on observed broadcast
   latency) to kill the timeout-storm feedback loop (A4).
   *Verify: Mesh-30/60 PBFT selection latency; whether PBFT-mesh completion extends past the
   30-agent wall (won't fix O(n²) message count, but removes the serial-blocking collapse).*

### Phase 2 — Data layer: stop scanning, start batching
7. **Kill SCAN in hot loops (D1):** membership per level:group is known — construct keys directly
   and `MGET`, or maintain a `members:{level}:{group}` SET updated on join/leave. One command per
   refresh instead of a keyspace scan.
8. **Batch reads:** `_update_pending_jobs` → single MGET (D3); pipeline `_monitor_delegated_jobs`
   (D4 — also directly attacks the open coordinator-tier ~7s item).
9. **Cheapen writes (D2):** drop WATCH for single-writer agent-info keys; move fast-changing fields
   (`load`, `last_updated`, allocations) to a small hash (HSET two fields per tick) and write the
   full object only on real changes. Add TTL = `peer_expiry_seconds` on agent keys (D6).
   *Verify: Redis `INFO stats` ops/sec before/after at Hier-60; DB-node CPU.*

### Phase 3 — Compute: make the cache work, unblock inbound
10. **Fix selection cache keys (C2):** quantize the assignee version (e.g., bucket `updated_at` to
    5–10s, or bump `version` only when capacity/allocations actually change) so entries survive
    ticks. Counters from Phase 0 prove the delta. Cache the `job_type` weight lookup (C5).
11. **Parallel inbound parse (C1/B3):** JSON-parse + `MessageBuilder` in a small pool, dispatch to
    engine on the existing single consumer (keeps engine single-threaded semantics). Longer-term:
    native protobuf fields to remove double serialization (B3) — schema change, do last.
12. Event-driven wakeups where cheap (selection loop already has `pending_event`; use it instead of
    fixed sleeps) (C4).

### Phase 4 — Architecture (already in flight / follow-ups)
13. **Gossip-fed selection state:** Phase-2 gossip dissemination exists but `SelectionEngine` still
    reads Redis; feeding peer load/capacity from the gossip cache removes most of the D1/D2 read
    traffic at the source (the original design intent, `GOSSIP_CONSENSUS_DESIGN.md` §Phase 2).
14. **Coordinator delegation shape** (open item from `SNOW_DRIVER_REARCH.md`): coordinators own
    their group → `peer-decided` CAS instead of β-convergence; revisit co-parent sampling so the
    Snow tier gets real quorums.
15. Keep the hybrid engine + bounded-group guidance (≤10/group, deeper hierarchies) as the scaling
    architecture; flat mesh stays a benchmark, not a deployment mode.

### Sequencing & effort

| Phase | Effort | Expected win | Risk |
|---|---|---|---|
| 0 | 1–2 days | Correctness + visibility; fewer re-proposal storms | Low |
| 1 | 2–3 days | Removes the ~8.7s/dead-peer × 3-phase stall; proven pattern | Low-med (quorum semantics unchanged) |
| 2 | 2–4 days | Redis ops/sec down ~an order of magnitude at 100+ agents | Low |
| 3 | 3–5 days | Selection CPU down (cache actually hits); inbound ceiling lifted | Med (cache-staleness tuning) |
| 4 | ongoing | Architectural headroom to 1000+ | Med-high |

**Benchmarks for every phase:** Hier-60 hybrid (baseline: L0 0.92s / L1 7.14s) and Mesh-120
(Snow 22.06s; PBFT livelock) on the 4-site testbed, plus Redis `INFO stats` and the new counters.
