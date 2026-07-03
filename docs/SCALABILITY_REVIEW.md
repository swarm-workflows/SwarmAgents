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

**First counter readout (Hier-60 hybrid, 2026-07-03) — several review hypotheses falsified:**
- `cache_hit_rate=0.99` at level 0 → **C2 (version-thrashing) is wrong** at this scale; skip the
  Phase-3 cache-key work. `watch_retries=0/724` → D2 contention is not binding at Hier-60.
  `bcast_mean=6ms max=11ms`, no `[BCAST_SLOW]` → B1 only bites with dead/slow peers (resilience,
  not steady-state). Inbound queue empty.
- **Real finding:** coordinator tier showed `snow_sends_dropped=5742` → fixed in `289b769e`
  (count only dispatched sends toward round quorum; pool 12→32). Re-run then exposed the deeper
  pair: **SWIM false-failure flapping under load** (acks processed late behind the single inbound
  consumer → `live_peer_ids()` empties → 23,668 `single-node` self-claims vs 5,320 `peer-decided`)
  and **driver-serialized synchronous Redis claims in `_finalize`** (elapsed mean ~11–16s).
- Post-fix run: **level-0 PBFT 0.17s** (was 0.92s, orig 20.21s); level-1 Snow 8.69s (~unchanged —
  the two findings above are its binding constraints). Next fixes: (A) SWIM probe/suspect timeouts
  for WAN + fall back to `neighbor_map` when the live set is empty; (B) move `_finalize`'s Redis
  claim off the driver tick thread.
1. **Implement `set_pending_*` in ResourceAgent `_HostAdapter`** (mirror Colmena) + replay pending
   messages when the job arrives in `_update_pending_jobs`. Also make `engine.on_proposal` resilient
   so one bad proposal can't abort the batch. *Expected: fewer lost votes → fewer re-proposals;
   measurable as a drop in the job-log/reselection multiplier.*
2. Fix D5 (raise or iterate groups explicitly). Bound `engine.conflicts` and the inbound queue
   (maxsize + drop-oldest counter, A2/B4).
3. **Counters** (cheap, decisive): selection-cache hit/miss, inbound queue depth, per-broadcast
   latency histogram, Redis WatchError rate, `sends_dropped` (exists for Snow). Export via Metrics.

### Phase 1 — Comm layer: parallel best-effort PBFT fan-out — ✅ SHIPPED `85f26208`

Delivered 2026-07-03: `broadcast()` fans out on a bounded pool (fire-and-forget, retries 4→2);
`Agent.broadcast` skips FAILED peers (new `SwimMembership.failed_agents()`, suspects excluded so they
can refute; plus heartbeat `failed_agents`); `bcast_dropped` counter in `[STATS]`. Item 6 (adaptive
reselection timeout) dropped as designed-out — the broadcast-latency → timeout-storm loop no longer
exists. Tests: `tests/test_broadcast.py`.

**Failure-scenario verification (Hier-60 hybrid, 6 of 50 level-0 agents killed at T+150s):**
| Metric | Result |
|---|---|
| Broadcast latency through the kills | **mean 0.000s / max 0.002s**, 0 dropped, 0 `[BCAST_SLOW]` (was ~8.7s per dead peer per phase, ×3 phases) |
| Level-0 PBFT selection under failures | **0.09s** (healthy baseline: 0.10s — failures now cost the worker tier nothing) |
| Level-1 Snow selection under failures | 1.88s (healthy: 0.96s) |
| Completion / recovery | 100% of processed jobs; orphaned jobs reselected; 0 abandons; pipeline clean |

### Phase 1 (original plan, for reference)
4. **Parallelize `broadcast()`** with the bounded send-pool pattern already proven for Snow
   (`7208ee90`): concurrent per-peer sends, short timeout, retries in background — never serially
   block a consensus phase on one peer. PBFT still gets its quorum from responses, so per-send
   reliability can be best-effort + one background retry.
5. **Skip known-failed peers** (consult `failed_agents`/SWIM live set) in broadcast.
6. Scale `reselection_timeout_s` with live peer count (or make it adaptive on observed broadcast
   latency) to kill the timeout-storm feedback loop (A4).
   *Verify: Mesh-30/60 PBFT selection latency; whether PBFT-mesh completion extends past the
   30-agent wall (won't fix O(n²) message count, but removes the serial-blocking collapse).*

### Phase 2 — Data layer — ✅ SHIPPED `3a977f17` + `5ba99454`

Delivered 2026-07-03. D1: `members:{level}:{group}` registry SETs replace per-tick `scan_iter`
(pruned on read; SCAN fallback for migration). D3: `_update_pending_jobs` → one MGET.
D4: `_monitor_delegated_jobs` → one grouped MGET per tick. D2/D6: agent-info via `save_fast`
(single-writer, no WATCH) + TTL (2× peer-expiry). Tests: `tests/test_repository.py` (fakeredis).

**Cascade found by the A/B and fixed in the same phase:** removing Redis latency from the periodic
loop let coordinators feed Snow at full concurrency — per-(job, peer, round) sends hit ~2,900/s
(~5× pool drain), 96k–300k drops, L1 regressed to 58s. Fixed structurally with **per-peer Snow
query/response batching** (`SnowQueryBatch`/`SnowResponseBatch`, one message per peer per tick —
wire demand now ∝ peers, not in-flight decisions).

**A/B verification (Hier-60 hybrid, identical workload, Redis sampled every 10s):**
| Metric | Baseline (pre-Phase-2) | Final (Phase 2 + batching) |
|---|---|---|
| Redis ops/sec (mean) | 13,508 | **1,133 (11.9×)** |
| Redis net input | 0.74 MB/s | 0.11 MB/s |
| Level-0 PBFT selection | 0.17 s | **0.08 s** |
| Level-1 Snow selection | 0.77 s | **0.63 s** |
| Jobs processed in-window | 426 | **1,069 (~2.5×)** — 100% complete |
| snow_sends_dropped | 0 | 0 |

### Phase 2 (original plan, for reference)
7. **Kill SCAN in hot loops (D1):** membership per level:group is known — construct keys directly
   and `MGET`, or maintain a `members:{level}:{group}` SET updated on join/leave. One command per
   refresh instead of a keyspace scan.
8. **Batch reads:** `_update_pending_jobs` → single MGET (D3); pipeline `_monitor_delegated_jobs`
   (D4 — also directly attacks the open coordinator-tier ~7s item).
9. **Cheapen writes (D2):** drop WATCH for single-writer agent-info keys; move fast-changing fields
   (`load`, `last_updated`, allocations) to a small hash (HSET two fields per tick) and write the
   full object only on real changes. Add TTL = `peer_expiry_seconds` on agent keys (D6).
   *Verify: Redis `INFO stats` ops/sec before/after at Hier-60; DB-node CPU.*

### Phase 3 — Compute — ✅ CLOSED (mostly absorbed/falsified by earlier phases)

Disposition 2026-07-03, item by item:
- **Item 10 (cache versioning): no action — hypothesis falsified.** Workers hit 99% because
  `assignee_version` resolves to the stable `version` field (the `updated_at` fallback reads an
  attr that doesn't exist on AgentInfo — accidentally correct). The coordinator tier's low rate
  (0.00–0.43) is cold-cache on once-seen (job, peer) pairs at tiny volume, not thrashing.
- **Item 11 (parallel inbound): already absorbed.** `json.loads` was always parallel (gRPC's
  32-worker pool); the consumer-thread cost collapsed via the local-only query path (`24f4f240`)
  and per-peer batching (`5ba99454`) — inbound queue has been pinned at 0 for four runs.
  Native-protobuf (B3) deferred: wire volume is down ~20× via batching and no measured bottleneck
  remains near serialization; a schema change isn't warranted on current evidence.
- **Item 12 (event-driven wakeups): shipped** (`a49dbc3e` + `2e70dfed`) — `selection_main` was
  already event-driven at the head; the tail waited 0.5s per batch even with a backlog. First
  attempt (skip wait if any PENDING remains) **regressed** — jobs deferred to better-suited peers
  stay locally PENDING, so the loop busy-spun the cost matrix (L0 0.08s→1.44s, throughput halved) —
  caught by per-phase verification. Final gate: skip the wait only when the iteration *proposed*
  and the batch was full. Verified: L0 0.11s, **L1 0.59s (best)**, 1,052 jobs in-window, 100%.

### Phase 3 (original plan, for reference)
10. **Fix selection cache keys (C2):** quantize the assignee version (e.g., bucket `updated_at` to
    5–10s, or bump `version` only when capacity/allocations actually change) so entries survive
    ticks. Counters from Phase 0 prove the delta. Cache the `job_type` weight lookup (C5).
11. **Parallel inbound parse (C1/B3):** JSON-parse + `MessageBuilder` in a small pool, dispatch to
    engine on the existing single consumer (keeps engine single-threaded semantics). Longer-term:
    native protobuf fields to remove double serialization (B3) — schema change, do last.
12. Event-driven wakeups where cheap (selection loop already has `pending_event`; use it instead of
    fixed sleeps) (C4).

### Phase 4 — Architecture — ✅ SHIPPED `0d170ee1` (verification run in flight at time of writing)

Delivered 2026-07-03:
- **Item 13 (gossip-fed selection state): shipped.** With gossip enabled, peer `load` flows to
  `neighbor_map` from the epidemic cache every tick (`_apply_gossip_overlay`; fresher than the old
  0.5s Redis poll), and the full Redis neighbor refresh — every peer's 2–5KB object, O(N) payload
  per agent per tick / O(N²) read bytes cluster-wide, **the genuine 1000-agent wall** — drops to a
  5s default cadence (`runtime.neighbor_refresh_full_s`). Liveness unaffected (SWIM detects; 5s
  staleness ≪ 45–300s expiry). Behavior unchanged when gossip is off.
- **Item 14 (coordinator delegation shape): closed, resolved by evidence.** Since per-peer batching,
  coordinators run genuine consensus — 892 `beta@round=6` commits observed at Hier-60, L1 0.59s;
  the `peer-decided` majority is the designed CAS fast path (first winner propagates), not a
  failure mode. No re-architecture warranted.
- **Item 15 (hybrid + bounded groups as the scaling architecture): standing guidance**, validated
  across every run in this campaign.
- **Verified** (Hier-60, Redis sampled): ops **929/s mean** (was 1,133 post-Phase-2; **14.5×**
  below the 13,508 baseline); L0 0.12s, L1 0.69s, 1,055 jobs in-window, 100% completion —
  selection quality unchanged on gossip-fed load. Note the ops reduction understates the win:
  the eliminated traffic was the O(N)-payload MGETs, which is the term that grows quadratically
  cluster-wide with agent count.

### Scale validation — Hier-110 / Hier-250 on the complete stack (2026-07-03)

Single runs, hybrid + swim + gossip + β=6, weak-scaling workloads (2,188 / 5,000 Pegasus jobs),
4-site testbed (110 = 22 hosts × 5; 250 = 25 hosts × 10; hosts barely loaded at 10/host):

| Tier | Paper (PBFT) selection | Fixed stack (L0 / L1) | Redis ops/s |
|---|---|---|---|
| Hier-110 | 1.09 s | **0.09 / 0.25 s** | 1,984 |
| Hier-250 | **24.53 s** (coordinator O(g²) wall) | **0.03 / 0.32 s (~77×)** | 4,364 |

- Redis load is now **linear at ~17 ops/agent/s** (60→929, 110→1,984, 250→4,364); the old stack
  burned 13,508 at just 60 agents. Projection at 1000 agents: ~17k ops/s — inside a single Redis.
- Zero send drops, zero abandons, queues empty at both tiers; L1 *improves* with more coordinators
  (CAS fast-path propagates faster in larger tiers).
- Caveats: single run per tier; in-window job counts are capped by fixed run windows + multi-minute
  deployment at 250 agents, so latency/health (not throughput) are the metrics of record here.
- Data on the testbed: `runs/scale-hier{110,250}`, `runs/redis_ops_scale-*.csv` (eval-data repo
  keeps `runs/` unversioned by design).

### Phase 4 (original plan, for reference)
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
