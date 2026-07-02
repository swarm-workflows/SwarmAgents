# Consensus Scaling Plan — Hybrid engine + Snow latency fix + topology-aware sampling

> Approved implementation plan (2026-07-01). Companion to `SNOW_GOSSIP_PAPER_PLAN.md` (experiments)
> and `GOSSIP_CONSENSUS_DESIGN.md` (Phase 1–4 design). Tracks the Phase 4 hybrid work + two Snow
> improvements motivated by the 4-site FABRIC testbed results.

## Context

Testbed measurements (2026-07-01, 4-site FABRIC) showed two walls:
- **Flat Mesh-120 (PBFT)** livelocks (~0% completion); **Snow** completes ~100% but at ~57s/selection.
- **Hierarchical (PBFT)** is fast (~1s) but the coordinator tier hits O(g²) (Hier-250 = 24.5s), and
  plain Snow in ≤10-agent groups is ~9s (tuning β/k/timeout did NOT help — the cost is structural,
  not the round count).

Neither pure engine is the endgame. Three improvements the data points to:
1. **Phase 4 Hybrid** — PBFT within small groups (fast) + Snow across the large coordinator tier
   (livelock-free). Removes the coordinator O(g²) wall while keeping PBFT's low latency in-group.
2. **Snow latency fix** — the ~9s is inside the engine: a round only ends on `got >= k` or the full
   `round_timeout`, so groups with fewer than `k` peers (or slow WAN peers) pay the full timeout every
   round; the single driver thread also serializes all in-flight jobs.
3. **Topology-aware sampling** — Snow samples `k` peers uniformly; across 4 sites most queries cross
   the WAN. Sample locality-weighted (mostly same-site) so rounds resolve at LAN latency.

Verified: `self.topology.level` is populated before engine init (`resource_agent.py:250`); each agent
has one immutable level; `neighbor_map` = same-level peers only; the dispatch already guards Snow
messages with `isinstance(self.engine, GossipConsensusEngine)` and Snow has no-op PBFT shims — so
mixed engines across levels are already safe. Peer `host` is in `neighbor_map`; there is no `site` field yet.

## Part A — Phase 4: Hybrid engine per topology level (highest value, cleanest)

When `consensus.protocol: hybrid`, pick engine from `topology.level`: level 0 → `ConsensusEngine`
(PBFT); level ≥ 1 → `GossipConsensusEngine` (Snow).
- `config_swarm_multi.yml`: `protocol: pbft | snow | hybrid` + `hybrid: {level0: pbft, coordinator: snow}`.
- `swarm/agents/resource_agent.py` `__init__` (~229-251): refactor engine construction into
  `_make_engine(protocol_name)`; when `hybrid`, choose by `self.topology.level`. Set `self.consensus_protocol`.
- No dispatch changes (already guarded). Optional: gate swim/gossip startup to Snow-level agents (deferred).

## Part B — Snow latency fix

Root cause: `selection_started_at` stamped at PENDING→PRE_PREPARE just before `propose()`
(`resource_agent.py:1656`); `assigned_at` at finalize→READY (`resource_agent.py:2013`). The gap is
entirely inside `GossipConsensusEngine`.
1. **Round completes early when peers < k.** `_tick` (`gossip_engine.py:269-272`) advances only on
   `got >= self.k` or deadline. With `k` > group size, every round burns the full `round_timeout`.
   Fix: record `state.queried = len(peers)` in `_send_round`; complete when `got >= min(self.k, queried)`
   (or supermajority `ceil(alpha*queried)`).
2. **First query waits a tick** (`propose` sets `round_deadline=now`, sent next tick) — optionally kick inline.
3. **Instrumentation** in `_finalize` (`gossip_engine.py:365-389`): log rounds_used, mean round latency,
   total engine time — validate on testbed, isolate any residual driver-serialization tax.
- Files: `swarm/consensus/gossip_engine.py`; unit test in `tests/test_snow.py` (few-peers round finalizes
  without waiting full timeout).

## Part C — Topology-aware (locality-weighted) peer sampling

Bias Snow's per-round sample toward same-site peers; keep a few cross-site for global convergence.
- `swarm/models/agent_info.py`: add `site` field (serialized as `site`).
- `generate_configs.py` (~686-703): write `site` per agent; source via optional `--agent-sites-file`
  (parallel to `agent_hosts.txt`) or hostname/subnet prefix. Default `site=None` → uniform fallback.
- `swarm/consensus/gossip_engine.py` `_pick_query_peers` (~393): locality-weighted selection; add
  `peer_site`/`my_site` to `_HostAdapter` (`resource_agent.py`) and the `SnowHost` protocol; add
  `consensus.snow.local_sample_frac` (default 0.7). Fall back to uniform when site absent.
- Testbed host→site mapping is derivable (agent-1..40 on per-site subnets in `/etc/hosts`).

## Sequencing & risk

A first (self-contained, biggest win) → B (engine-local, unit-testable) → C (config+model+engine, gated
behind the new `site` field so it's a no-op until sites are provided). All preserve the exactly-once
safety net (Redis `SET NX` in `repository.try_claim_assignment`).

## Verification RESULTS (2026-07-02, 4-site FABRIC testbed)

Code merged (`dc3e708e`, `0a57ef50`) and deployed to all 40 agents + DB node. Raw data:
`swarmplus-evaluation-data/runs/pegasus-workloads/snow-gossip/demo-{hier60-hybrid,mesh120-snowloc}`.

**Part A hybrid — WORKS (engine selection), but no latency win yet.** Hier-60 hybrid, 3 runs:
- Engine selection validated on testbed: level-0 agents log `protocol=hybrid level=0 -> engine=pbft`,
  coordinators `level=1 -> engine=snow`.
- Snow coordinator tier (level-1): **7.68 ± 0.67 s** (≈ pure-Snow 6.49s — the round fix didn't help
  because coordinators finalize `single-node`, `elapsed≈44s`: **single-threaded driver starvation**).
- PBFT worker tier (level-0): **20.21 ± 13.99 s** (vs ~1.0s pure-PBFT baseline — the slow coordinator
  tier backs up delegation and congests the whole pipeline).
- Completion 100%.

**Parts B+C — did NOT help at flat scale; got worse.** Mesh-120 Snow+locality (`local_sample_frac=0.7`), 2 runs:
- level-0 selection: **86.75 ± 12.40 s** vs **56.93 s** uniform baseline — worse.
- Locality IS active (`queried≈12–17`, same-site biased), but rounds **don't converge**: 38–97 rounds
  per decision, finalizing via `peer-decided` (CAS race), never reaching β=20. The round-completion fix
  makes rounds faster → more non-converging churn → higher elapsed. Completion 100%.

**Diagnosis (from the Part B `[SNOW_TIMING]` instrumentation — its real payoff):** Snow's latency is
**not** the round-timeout or WAN locality we targeted. It is (1) **single-threaded driver starvation**
(one daemon pumping all in-flight job states + network sends per tick) and (2) **β-non-convergence under
high contention** (2188 jobs / 120 agents → preference flips → confidence resets → CAS-race fallback).
Correctness held everywhere (100% completion via Redis `SET NX`).

**Redirected next steps:** (a) re-architect the Snow driver — parallel/sharded workers, batched/async
sends; (b) convergence — stabilize per-round preference (sticky sampling, hysteresis) and/or adaptive β;
(c) only then will the round fix + locality sampling pay off. Hybrid engine selection is a keeper.

### Post-fix results (2026-07-02, commit `7208ee90`) — driver + Snowball fixes shipped

Implemented both: non-blocking best-effort sends (bounded pool + `send_besteffort`, short timeout/no
retries) and Snowball sticky preference (`_evaluate_round`, cumulative `d`, `preferred = argmax(d)`).
Deployed to all 40 agents; re-ran on the testbed. See `SNOW_DRIVER_REARCH.md` and data dirs
`snow-gossip/demo-{hier60-hybrid-fixed,mesh120-snowloc-fixed}`.

| Config / metric | Pre-fix | Post-fix |
|---|---|---|
| Hier-60 PBFT worker tier selection | 20.21 s | **0.92 s** |
| Hier-60 `single-node` finalizes (starvation marker) | 3772 | **28** |
| Hier-60 Snow coordinator tier | 7.68 s | 7.14 s |
| **Mesh-120 Snow+locality selection** | 86.75 s | **22.06 ± 1.35 s** (~3.9×) |
| Completion (all) | 100% | 100% |

Driver starvation is eliminated (PBFT tier restored to ~1s; single-node all but gone) and flat-mesh
selection is ~3.9× faster with the convergence + locality changes now that the driver isn't the
bottleneck. Remaining: the Hier-60 Snow *coordinator* tier (~7s) is a delegation-shape issue
(coordinators mostly own their group → `peer-decided` CAS rather than β-convergence), tracked separately.

## Verification (original plan)

1. Unit: `python -m pytest tests/test_snow.py -v` (+ Part B few-peers test).
2. Local smoke: small hierarchical config with `protocol: hybrid`; confirm level-0 log `protocol=pbft`,
   coordinators `[snow] consensus engine started`.
3. Testbed (4-site, `ssh swarm` → `/root/SwarmAgents`, remote, `--db-host database`):
   - A+B: rerun Hier-60 / Hier-110 (jobs_1094 / jobs_2188) with `protocol: hybrid`; compare Snow-tier
     selection vs pure-Snow ~6–9s and PBFT baseline in `swarmplus-evaluation-data/.../hierarchical-60,110`.
   - A+B+C: rerun flat Mesh-120 (jobs_2188) with locality sampling; compare vs recorded 57s, completion ~100%.
   - Reuse `/tmp/analyze_level0.py`, `/tmp/analyze_runs.py`; file raw data under
     `swarmplus-evaluation-data/runs/pegasus-workloads/snow-gossip/`.
4. Correctness: zero double-assignments via `dump_db.py` after each run.
