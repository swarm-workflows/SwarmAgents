# Work Item: Snow Driver Re-architecture (fix driver-thread starvation)

> Follow-up to the 2026-07-02 testbed verification (see `CONSENSUS_SCALING_PLAN.md`). The Phase-4
> hybrid engine works, but Snow's per-decision latency is dominated by driver-thread starvation, not
> the round-timeout/locality addressed in Parts B/C. This item removes that bottleneck.

## Root cause (measured, not hypothesized)

- **Blocking sends:** `GrpcTransport.send` → `client.call_unary(..., timeout=2.0, retries=4)`
  (`swarm/comm/grpc_transport.py:75`). A single slow / WAN / failed peer blocks a send for up to
  **2.0s × 4 = 8s**.
- **Serial, single-threaded driver:** `GossipConsensusEngine._run`/`_tick` is one daemon thread that,
  each 50ms tick, iterates *every* in-flight job state and calls `_send_round` → `_safe_send` inline
  for each of the ~k sampled peers (`gossip_engine.py` `_tick`/`_send_round`).
- **Consequence:** one slow peer stalls the entire driver; jobs queue behind it. Measured on the
  testbed: coordinators finalizing `single-node` with `rounds=1` yet `elapsed≈44s`, and the resulting
  delegation backlog dragged the PBFT worker tier from ~1s to ~20s. This is the real latency source.

## Goals

1. The driver tick never blocks on network I/O.
2. Snow query/response sends are **best-effort** — a dropped query just means that peer abstains this
   round, which the Part B round-completion fix already tolerates.
3. Preserve exactly-once (Redis `SET NX`) and the lock-guarded state machine unchanged.

## Approach (recommended: 1 + 2 first; 3 only if still hot)

1. **Non-blocking sends via a bounded pool.** Give `GossipConsensusEngine` a small
   `ThreadPoolExecutor` (config `consensus.snow.send_workers`, default ~12). `_safe_send` submits the
   send (fire-and-forget) instead of calling inline; drop + count on queue-full. Snow queries are
   one-way — responses already arrive asynchronously via `on_snow_response`, so fire-and-forget is
   the natural model. Tie pool lifecycle to `start()`/`stop()`.
2. **Best-effort send semantics for Snow.** Add a low-latency send path (short timeout, no retries)
   used for `SnowQuery`/`SnowResponse`: `consensus.snow.send_timeout_ms` (default 300), retries 0.
   Route it through a `send_besteffort()` on the transport / `_TransportAdapter`, keeping PBFT's
   reliable send untouched.
3. **If still hot:** (a) **batch per peer** — coalesce all queries destined for the same peer within a
   tick into one message; (b) **shard the driver** — partition `_states` across N worker threads by
   `job_id` hash so state-machine work also parallelizes.

## Files

- `swarm/consensus/gossip_engine.py` — executor + `_safe_send` submit; `start()/stop()` manage it;
  `_tick` logic unchanged. Add a dropped-send counter for observability.
- `swarm/comm/grpc_transport.py` (+ `agent.send` / `_TransportAdapter`) — a best-effort send variant
  (short timeout, no retries) selected for Snow message types.
- `config_swarm_multi.yml` — `consensus.snow.send_workers`, `consensus.snow.send_timeout_ms`.

## Non-goals / linked work

- **β-non-convergence under contention** (mesh: 38–97 rounds, `peer-decided` CAS race) is a *separate*
  problem — track as its own item (sticky/hysteretic per-round sampling, adaptive β). The driver fix
  is a prerequisite: only once the driver isn't starved will convergence behavior be measurable.

## Verification

- **Unit:** a fake transport with an injected slow send (e.g. sleeps) must NOT stall the driver —
  other jobs still finalize within the tick budget (`tests/test_snow.py`).
- **Testbed:** re-run Hier-60 hybrid + Mesh-120 snow. Expect `[SNOW_TIMING]` `elapsed` to drop sharply,
  `single-node` finalizes to disappear (coordinators actually reach peers), Snow-tier selection to fall
  toward ~1–2s, PBFT worker tier back to ~1s, completion stays 100%, zero double-assignments.

## Effort / risk

Medium; localized to the engine + transport. Risk: fire-and-forget failures are silent — mitigate with
a debug log + a dropped-send metric. No change to finalization safety (Redis CAS remains authoritative).

## RESULTS (implemented `7208ee90`, testbed 2026-07-02)

Both fixes shipped: (1) non-blocking best-effort sends (bounded pool + `send_besteffort`), and
(2) Snowball sticky preference (`_evaluate_round` cumulative `d`, `preferred = argmax(d)`). Deployed to
all 40 agents. Hier-60 hybrid re-run vs the pre-fix run:

| Metric | Pre-fix | Post-fix | |
|---|---|---|---|
| PBFT worker tier (level-0) selection | 20.21 s | **0.92 ± 0.50 s** | ✅ back to baseline |
| `single-node` finalizes (coordinator, starvation marker) | 3772 | **28** | ✅ starvation eliminated |
| Snow coordinator tier (level-1) selection | 7.68 s | 7.14 s | ~unchanged (see note) |
| Completion | 100% | 100% | |

Flat **Mesh-120 Snow+locality** (2 runs, where agents have ~119 real peers, so this best exercises the
Snowball convergence fix):

| Metric | Pre-fix | Post-fix | |
|---|---|---|---|
| level-0 selection | 86.75 s | **22.06 ± 1.35 s** | ✅ ~3.9× faster |
| Completion | 100% | 100% | |

**Interpretation:** the driver fix worked as designed — eliminating send-blocking removed the
starvation that had congested delegation and dragged the PBFT worker tier to 20s; it's now ~1s again,
and coordinators reach peers (single-node 3772 → 28). On flat Mesh-120 the combined driver +
Snowball-convergence + locality changes cut selection ~3.9× (86.75s → 22s) at 100% completion — the
regime where agents have enough peers for the convergence fix to bite. The Snow *coordinator* tier in
Hier-60 is still ~7s because each coordinator largely owns its group and finalizes via `peer-decided`
(CAS) rather than β-convergence; the remaining latency there is a hierarchy/delegation-shape question,
not the driver.

**Next:** convergence work item (per-round sticky/hysteretic sampling, adaptive β) now that the driver
is no longer the bottleneck; the round-completion + locality changes should also matter more once
convergence holds.

## FINAL RESULT (2026-07-03): coordinator tier at 0.96s with real consensus

The `[STATS]`/`[SNOW_TIMING]`/`[SNOW_ABANDON]` instrumentation drove five further fix iterations,
each exposing the next bottleneck (full chain in `SCALABILITY_REVIEW.md`):

| Commit | Layer fixed | Symptom that exposed it |
|---|---|---|
| `289b769e` | dropped sends counted toward round quorum; pool 12→32 | `snow_sends_dropped=5742`, deadline-resolved rounds |
| `681e021d` | SWIM empty-live-set false readings (fallback to neighbor_map; WAN timeouts) + finalize moved off the driver thread | 23,668 `single-node` self-claims; late-ack flapping |
| `abb8fd15` | saturated-round retry-per-tick congestion collapse (backoff + `max_inflight` cap) | 2.0M dropped sends, inbound queue pinned, zero finalizes |
| `66c40436` | unconditional `json.dumps` per inbound message; per-send INFO log | queue still pinned at 20k, 722 `[SNOW_ABANDON]` |
| `24f4f240` | **Redis WAN round-trips per SnowQuery on the single inbound consumer** (local-only `my_cost_for_job` + `get_assignment_local`) | sends healthy but responses stale → 100-round abandons |
| (config) | β 20→6, `max_inflight` 16→32 — calibration for a trusted 10-coordinator tier | decisions healthy (68ms) but β=20 × queueing made selection 68s |

**Hier-60 hybrid, final vs history (level CSVs):**

| Tier | Original | Post driver-fix | Final (tuned) |
|---|---|---|---|
| Level-0 PBFT workers | 20.21 s | 0.92 s | **0.10 s** |
| Level-1 Snow coordinators | 7.68 s (fake: single-node self-claims) | 7.14 s | **0.96 s (real consensus)** |

Final run signature: `inbound_q=0, msgs_dropped=0, snow_sends_dropped=0, SNOW_ABANDON=0`;
892 `beta@round=6` genuine convergence commits + 3,454 `peer-decided` fast-path finalizes; 100%
completion of processed jobs; exactly-once preserved throughout (Redis `SET NX`).

**Design lessons encoded:** peer-side query handling must be local-only (no Redis/aggregation on the
inbound consumer); saturation must back off, never retry-per-tick; cap in-flight decisions like PBFT
caps pending elections; an empty failure-detector live set is a signal about the detector, not the
cluster; β should scale with tier size/trust, not Avalanche defaults.
