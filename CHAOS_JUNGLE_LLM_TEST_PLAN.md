# Evaluating Chaos Jungle on SwarmAgents' LLM Scheduling Agents

**Purpose.** Use the [Chaos Jungle](https://swarmourr.github.io/CJ/index.html) (CJ) chaos-engineering
framework to evaluate the resilience of **SwarmAgents' LLM-driven scheduling agents** under
LLM-layer faults, producing figures and hypothesis tests for the Chaos Jungle paper. The focus is
the LLM plane; infrastructure faults appear only as a single composite scenario.

**Status (2026-08-17):** **Phase 0 complete.** A reproducible reference baseline
(`cj-baseline-ref`) is established on a frozen seed-42 fleet with a live DTN connectivity term:
300/300 jobs, 0 fallbacks, 0 failures. CJ is installed and proven to intercept SwarmAgents' LLM
path. Ready to begin Phase 1 (fault sweeps).

| | |
|---|---|
| **Code** | `SwarmAgents` @ `dabe53f0`, branch `chaos` |
| **Testbed** | FABRIC slice: 1 orchestrator (`database`) + 30 agent hosts, 8-core CPU / 7 GB RAM each, no GPU |
| **Chaos Jungle** | v0.1.0, pinned commit `5044939` (see [§6](#6-findings-for-the-cj-maintainers)) |
| **Workload** | Frozen 300-job trace (16 workflows) merged from real Pegasus runs — see [§2.2](#22-workload--a-frozen-reproducible-mixed-pegasus-trace) |

---

## 1. How CJ injects faults into SwarmAgents

SwarmAgents' `LlmAgent` scores every `<job, agent>` pair through **pydantic-ai**, converting the
model's 0–100 score into a scheduling cost (`cost = 100 − score`). On **any** exception it falls
back to the analytical cost model — logged as `[LLM_COST_FALLBACK]`.

CJ injects LLM faults by **spawning a local proxy and redirecting the client's base URL to it**,
then forwarding to the real upstream. Since pydantic-ai honours `OPENAI_BASE_URL` /
`OLLAMA_BASE_URL`, injection needs **no SwarmAgents code changes**:

```
agent (pydantic-ai) --[OLLAMA_BASE_URL]--> CJ fault proxy --> real LLM endpoint
```

The endpoint is resolved as **env var → YAML `llm.base_url` → default**. Env deliberately wins, so
setting `OLLAMA_BASE_URL` on one host redirects *that host's* agent into a fault proxy without
touching any per-agent config. **This is what makes partial-fault studies possible** — fault 10% of
the fleet by starting the proxy on 3 of 30 hosts.

Because faults are injected per host, the natural blast-radius knob is *which hosts* run a proxy.

---

## 2. Environment

### 2.1 Two LLM backends ("arms")

| Arm | Endpoint | Model | Role |
|-----|----------|-------|------|
| **Ollama** (primary) | `127.0.0.1:11434`, per host | `qwen2.5:3b` | All fault sweeps — plain HTTP, no shared contention |
| **Gateway** | FABRIC LiteLLM proxy | `gpt-oss-20b` | Realism spot-check, fault-free only |

**Why fault-inject on Ollama, not the gateway:** a shared endpoint adds load-dependent queueing
that would contaminate baseline-vs-fault deltas — the gateway went from 1.3 s single-call to
5.85 s under 30-agent load. Local Ollama keeps each agent's latency independent.
(An earlier version of this note claimed CJ's proxy could not forward to authenticated-HTTPS
upstreams. That was wrong: the failure was the `/v1` upstream suffix in §6-3, and the proxy
forwards to the HTTPS gateway correctly once given an origin.)

**Gateway access note:** the slice has **no IPv4 default route** (IPv6-only public egress), and the
gateway publishes only an A record — so its public address is unreachable from the slice. It *is*
reachable over the **FABNet dataplane** at `10.141.1.2`; mapping the hostname to that IP in
`/etc/hosts` keeps TLS/SNI valid. The gateway serves 4 reasoning+tool_use models; of these only
`gpt-oss-20b` is fast enough for a per-job scoring loop (1.3 s vs 37–43 s for the others).

### 2.2 Workload — a frozen, reproducible mixed Pegasus trace

Experiments run on a **fixed 300-job trace merged from two extractions**, built by
`build_mixed_trace.py` (seeded, stratified by workflow, so a rebuild is byte-identical):

| Source | Jobs | Contributes |
|--------|------|-------------|
| `cpegasus` | 200 | Per-file **data/DTN nodes** (real filenames + byte sizes) — 6 workflows |
| `pegasus` | 100 | **Workflow & resource diversity** — 10 workflows, heavier tail (up to 8 cores / 46 GB) |

**16 distinct workflows**, 199/300 jobs (66%) carrying data nodes. The `pegasus` extraction predates
the extractor's file-resolution feature, so its jobs have no data nodes — a legitimate second
population (jobs with no data movement), and its source hosts are no longer reachable to re-extract.
`pegasus2` (13,908 jobs) was evaluated and **excluded**: 97% is a single workflow of near-identical
2-second jobs, so it adds volume but almost no signal for LLM scoring.

**DTN names must match the agents' pool, and each job must need only one DTN.** Convert with
`--dtn-names dtn1,…,dtn10 --dtn-scope job`. Two distinct traps:

1. *Names must match.* Without `--dtn-names`, data-nodes keep the raw Pegasus site name (`local`),
   which shares zero names with agent DTNs. `local` is also explicitly excluded from the required-DTN
   set (it means local filesystem, not a transfer node), so the connectivity term silently vanishes.
2. *Scope must be per-job.* Feasibility requires an agent to hold **every** DTN a job references,
   and agents are given only 1–4. Hashing per **file** (`--dtn-scope file`) spread one job's files
   across up to 8 DTNs, making **76/300 jobs unschedulable on any agent**; with a 10-job proposal
   window they head-of-line blocked the queue and stalled a whole run. `--dtn-scope job` puts all of
   a job's files on one DTN — also the realistic model, since workflows stage from one or two sites.

With the frozen trace: every job needs 0 or 1 DTN, **0 infeasible**, and the median job fits **9 of
30** agents — so the connectivity term differentiates agents without making jobs unschedulable.

> **Always verify feasibility offline before launching a run** — intersect each job's required DTNs
> and capacities against `agent_profiles.json`. A stalled run looks identical to a slow one for the
> first hour.

> **Both traps are fixed in `run_test.py`.** `--pegasus-profiles` now converts with
> `--pegasus-data-nodes per-file`, spreads jobs across the *same* `dtn1..dtn10` pool
> `generate_configs.py` gives agents, and uses per-**job** scope — so names match and every job
> needs exactly one DTN, by construction. Override with `--pegasus-dtn-names`.

---

## 3. Validated baselines (fault-free)

All: 30 agents, mesh topology, Snow consensus, 300 Pegasus jobs.

| Run | Model | Fleet / trace | LLM calls | Fallbacks | Failed jobs | Latency (mean) |
|-----|-------|---------------|-----------|-----------|-------------|----------------|
| **`cj-baseline-ref`** ← **reference** | `qwen2.5:3b` | frozen seed-42 fleet, 16-workflow trace, DTN term live, `suspect_timeout_s: 60` | 1064 | **0** | **0** | 9.90 s (p50 9.96 / p95 12.52) |
| `cj-baseline-frozen2` (pre-SWIM-fix) | `qwen2.5:3b` | same fleet/trace, `suspect_timeout_s: 20` | 1096 | 0 | 0 | 10.13 s (p50 10.09 / p95 13.06) |
| `cj-baseline-ollama` (superseded) | `qwen2.5:3b` | random fleet, 6-workflow trace, DTN term inert | 3331 | 0 | 0 | 9.61 s |
| `cj-baseline-gw` (superseded) | `gpt-oss-20b` | random fleet, 6-workflow trace, DTN term inert | 3062 | 0 | 0 | 5.85 s (shared-endpoint contention) |

**`cj-baseline-ref` is the reference** every fault scenario is measured against. `frozen2` is the
same fleet and trace before the SWIM timeout fix and is retained only to show that fix's effect:
suspect-timeout events **52 → 9** and reported failed agents **3 → 0**, while LLM calls (1096 vs
1064), latency (~10 s) and fairness stayed statistically identical — so the two are comparable and
the change altered membership behaviour only. The `random fleet` rows below are superseded.

Characteristics of the reference run:
- **300/300 jobs completed**, 0 infeasible, 0 reassigned, **0 failed agents**; drained in ~11 min.
- **Residual noise: 9 SWIM suspect-timeout events** (down from 52). Not zero — compare fault runs
  against this rate, not against zero.
- **Load is deliberately uneven** — 3 to 85 scoring calls per agent, Jain's fairness 0.71.
  This is the DTN feasibility gate working: the median job is feasible on only 9 of 30 agents.
- **LLM calls dropped 3331 → 1096** versus the inert-DTN run. The ratio (0.33) tracks the
  feasibility ratio (9/30 = 0.30): agents no longer score jobs they cannot run, so infeasible
  pairs never reach the model.

> ⚠ **This run logged 52 SWIM `suspect-timeout` events naming 7 distinct agents (26 of 30 hosts
> reporting) — all false positives.** Every agent ran to completion and 0 jobs were reassigned
> (heartbeat, not SWIM, is authoritative for reassignment). **Now mitigated — see §2.3.** Fault runs
> should still be compared against a re-measured baseline rate rather than against zero.

### 2.3 Isolating inference from the agent (SWIM false positives)

Co-locating inference with the agent starved the agent process of CPU: Ollama saturated all 8 cores
for ~10 s per bid (**worst case 19.2 s**), which sat right at the **20 s** `suspect_timeout_s`, so
healthy-but-busy agents were marked failed. Two mitigations were attempted; **only the second was
kept** — the timeout bump alone gives ample margin (19.2 s worst case against a 60 s window):

1. ~~**Pin Ollama to 6 of 8 cores**~~ — **TRIED AND REVERTED. Do not repeat.** Restricting Ollama to
   a subset of cores (`taskset -c 0-5`, or a systemd `CPUAffinity=0-5` drop-in) **collapsed inference
   from ~0.9 s to 34–69 s per call**; one bid in a re-baseline took **20 minutes**, and host load sat
   at ~7 with *only* ollama running and no agents. Cause: llama.cpp spawns one **busy-waiting** thread
   per core and Ollama exposes **no thread-count env var** (`ollama serve --help`), so the extra
   threads spin against siblings that cannot be scheduled. Restricting CPU without also lowering the
   thread count is actively harmful. Reverted fleet-wide with `/root/cj_ollama_reset.sh`
   (30/30 hosts back to `aff=0-7`, one server each).

   Operational traps found while doing it, which still apply to any Ollama change:
   - **15 of 30 hosts run a systemd `ollama.service`** that respawns the server and wins the port
     race, so `pkill` + `nohup` silently does not stick; and a leftover server can survive
     `systemctl restart` while still holding port 11434. Kill *all* `ollama serve` and `llama-server`
     processes before starting one.
   - The systemd service reads `/usr/share/ollama/.ollama/models`, *not* `/root/.ollama` — a model
     pulled as root is invisible to it (`model not found`). Copy the blobs across
     (`cp -an` + `chown -R ollama:ollama`) rather than re-downloading.
   - **Verify every matching process, not just the first.** `pgrep -f "ollama serve" | head -1` picks
     an arbitrary PID among matches and gave three different answers (20/30, 30/30, 7 unpinned) for
     one unchanged fleet.
2. **Raise `suspect_timeout_s` 20 → 60** in `config_swarm_multi.yml`, above worst-case bid latency.
   Tradeoff: genuine failures now take proportionally longer to detect — relevant if process-kill
   faults are added later (composite X1).

Regenerating the fleet after the config change reproduced **byte-identical** `agent_profiles.json`
(`755eccc2…`), so the timeout propagated to all 30 agent configs without perturbing the fleet — the
reference baseline stays comparable.

**Why not host the model on one shared VM instead?** Considered and rejected for CPU-only hosting:
the fleet currently runs 30 × 8 = **240 cores** of concurrent inference, and the reference run's 1096
bids represent ~3 CPU-hours — hours of wall-clock on a single 8-core VM versus 11 minutes. A shared
endpoint also reintroduces load-dependent queueing that would contaminate baseline-vs-fault deltas,
which is precisely why the gateway arm was rejected for fault work (1.3 s single-call → 5.85 s under
30-agent load). **A GPU inference node would flip this conclusion** — likely ~1 s per bid while
removing agent-host contention entirely — and would keep partial-fault studies working by running
one CJ proxy port per agent on that node.

> **Runtime planning:** at ~9.6 s per bid, one 300-job Ollama run takes roughly 25–30 min. A full
> Tier-1 sweep with repeats is many hours — consider a 100-job trace for wide sweeps.

---

## 4. Metrics, oracles, and what we are actually measuring

The unit of measurement is **the scheduler**, not the model. CJ's
`ChaosRunner.measure(workload, n_baseline, n_fault)` runs a workload under both conditions and
computes the delta; our workload is one full SwarmAgents run.

**Primary (scheduling health)**
- Job **completion rate** and failed/orphaned count
- **Makespan**, P50/P95 job latency
- **Consensus conflicts** and reselections/restarts
- **Load fairness** (Jain's index)
- **Fallback rate** — `[LLM_COST_FALLBACK]` vs `[LLM_COST_COMPLETE]`; *the* key LLM-resilience signal

**Secondary (LLM plane)**
- Per-call latency (`ReasoningTime`), HTTP error mix (429/402/503/504/401)
- Score-distribution shift under semantic faults
- Token cost (gateway arm)

**Correctness invariant (must hold under every fault):** **zero double-assignments** — enforced by
Redis `SET NX` in the Snow engine. Safety must never degrade, only performance.

**Oracles / quality gates**
- `result.passed("completion_rate", threshold=…)` as a CI-style gate per scenario
- CJ's `LLMJudge` on the bidder's `explanation` field — does a corrupted score arrive with a
  confidently-wrong justification? (CJ Layer 10)
- "Graceful degradation" oracle: completion rate stays above threshold *because* fallback engaged

---

## 4b. Scenario harness and first result

Experiments are scripted as **scenario files mirroring Chaos Jungle's own
`LLM_SCENARIOS.md`** — same numbering (S01 latency, S05 unavailable, …) so results are
directly comparable with the framework's catalogue.

```
scenarios/
  helpers.py               health gate, cleanup, per-host injection, metrics, report
  reference_baseline.json  the fault-free reference (§3)
  run_all.py               batch runner (--list); ~15 min per scenario
  api/s01_latency.py       S01  [delay_s] [fraction]
  api/s05_unavailable.py   S05  [fraction]
```
Each scenario takes a **host fraction**, so the same file yields the blast-radius curve
(`s05_unavailable.py 0.25 / 0.5 / 1.0`). Teardown runs in a `finally` — a leaked proxy or
`OLLAMA_BASE_URL` would silently fault every later run. Unlike CJ's scenarios (one LLM
call, seconds), ours is a full 30-agent run, so `run_all.py` is a batch job.

### S05 — LLMUnavailable: the blast-radius curve (2026-08-17)

Same fleet and trace throughout; only the share of hosts whose LLM returns 503 changes.

| faulted hosts | fallback rate | LLM calls OK | **load fairness** | jobs completed | jobs stuck |
|---|---|---|---|---|---|
| 0% (reference) | 0.0% | 1064 | 0.681 | 300 | 0 |
| **25%** (8/30) | 53.9% | 337 | **0.331** | 300 | 0 |
| **50%** (15/30) | 78.7% | 186 | **0.570** | 300 | 0 |
| **100%** (30/30) | 100.0% | 0 | **0.843** | 300 | 0 |

**Headline: a partial LLM outage is far more damaging than a total one.** Load fairness
collapses to **0.331** at 25% — less than half the healthy baseline — then recovers
monotonically as the outage spreads, ending *best* under total failure. Completion never
moves: 300/300 jobs, 0 stuck, 0 agents lost at every point.

**Mechanism** — the LLM-blind agents capture the work:

| outage | faulted agents | healthy agents | ratio |
|---|---|---|---|
| 25% | 8 agents took **280/300 jobs** (35.0 each) | 11 agents took 20 (1.8 each) | **19×** |
| 50% | 15 agents took **292/300 jobs** (19.5 each) | 6 agents took 8 (1.3 each) | **15×** |

A faulted agent gets its 503 and falls back to the analytic cost in ~0 s, while a healthy
agent spends ~10 s producing an LLM bid. In race-to-propose consensus the broken agents win
almost every election, so **8 of 30 agents being LLM-blind is enough to schedule 93% of the
workload analytically** — the healthy majority's LLM reasoning is bought and paid for, then
discarded. This is a gray-failure pattern: the system survives total failure gracefully and is
harmed most by partial failure.

It also explains the disproportionate fallback rate — 26.7% of hosts generate 53.9% of scoring
events, because failing fast lets them cycle the selection loop far more often. **Fallback rate
is a rate over calls, not over agents**, and over-represents fast-failing agents; read it
alongside the per-agent job split.

*Design implication:* if LLM scoring is meant to add value, an agent that has fallen back should
not be able to out-race one that is still reasoning — a fallback penalty or a bid deadline
applied to all agents equally would restore the intended competition.

> **Correction.** An earlier S05 100% run reported here was **not a Chaos Jungle fault**:
> `cj_proxy.py` had never been deployed to the agent hosts, so the proxies never started and the
> agents' fallbacks came from `Connection error` against a dead port rather than an injected
> 503. The graceful-degradation conclusion survived, but the attribution was wrong. The table
> above is the re-run on a harness that verifies the injected fault type, the blast radius, and
> teardown (commit `67d4fa5e`).

### S01 — LLMLatency: +3 s per call (2026-08-18)

| faulted hosts | fallback rate | bid latency mean | **load fairness** | jobs completed |
|---|---|---|---|---|
| 0% (reference) | 0.0% | 9.90 s | 0.681 | 300 |
| **50%** (15/30) | **0.0%** | 10.98 s (+1.08) | **0.849** | 300 |
| **100%** (30/30) | **0.0%** | **12.65 s (+2.75)** | 0.738 | 300 |

The +2.75 s shift at full fleet matches the injected +3 s, so the fault is being measured
correctly. **Latency is absorbed completely**: zero fallbacks, 300/300 jobs, no agent lost.
Scheduling gets slower and nothing else breaks.

*Side effect:* zero fallbacks at +3 s on a ~10 s bid is direct evidence that
`llm.timeout_seconds: 6` is not enforced (SwarmAgents finding 7) — a 12.65 s call should have
breached it.

### S01 vs S05 — it is not slowness that hurts, it is skipping the LLM

Per-agent job capture, faulted vs healthy agents in the same run:

| scenario | faulted agents | healthy agents | ratio |
|---|---|---|---|
| **S01** +3 s on 15/30 (slowed) | 150 jobs (10.0/agent) | 150 jobs (10.0/agent) | **1.00×** |
| **S05** outage on 8/30 (instant-fail) | 280 jobs (35.0/agent) | 20 jobs (1.8/agent) | **19.25×** |

We expected slowed agents to *lose* work, mirroring S05's race dynamic in reverse. They do not —
placement stays exactly even, and fairness actually improves to 0.849.

The difference is one of **regime, not degree**. A +3 s handicap on a ~10 s bid is a ~30%
slowdown: both groups still operate on the same timescale, so neither wins races. An agent whose
LLM is down skips inference entirely and bids in **~0 s** — two orders of magnitude faster. That
categorical gap, not relative slowness, is what lets degraded agents monopolise the workload.

**Design implication (revised).** The problem is not that failed agents are fast; it is that the
**fallback path is orders of magnitude cheaper than the LLM path**, so any agent that errors out
is rewarded with a decisive scheduling advantage. Penalising *slow* agents would not help — S01
shows slowness is already harmless. What is needed is to make a fallback bid cost what an LLM bid
costs, whether by delaying fallback proposals or by applying a bid deadline uniformly.

---

## 5. Experiment matrix

Each row is one CJ `Scenario`, run baseline-vs-fault with n≥5 repeats on a fixed job trace, across
**two topology arms**: flat mesh (30 agents) and hierarchical (LLM agents as Level-1 coordinators,
enabling the *targeted-fault-on-coordinators* story).

### Tier 1 — LLM API faults (CJ Layer 1) — core of the paper
| # | Fault | Sweep | Hypothesis |
|---|-------|-------|------------|
| L1 | `LLMLatency` | 1/3/6/10 s | Latency degrades throughput; fallback absorbs the worst case |
| L2 | `LLMTimeout` | hang 8/15 s | Cancellation + fallback keeps scheduling live |
| L3 | `LLMUnavailable` (503) | 25/50/100% of agents | Full outage ⇒ degrades to pure analytic scheduler, no correctness loss |
| L4 | `LLMRateLimit` (429 after n) | n = 5/20 | Back-off vs fallback; graceful or collapse? |
| L5 | `LLMResponseCorrupt` | truncate/empty/invalid_json | Parse errors ⇒ fallback, no crash, no bad proposals |
| L6 | `LLMBudgetExceeded` (402) | cap mid-run | Cost-cap outage ⇒ fallback |
| L7 | `LLMTokenStarvation` | tiny `max_tokens` | Real truncated output — does score degrade *silently*? |
| L8 | `LLMUnauthorized`/`AuthExpiry` | expiry mid-run | Credential failure ⇒ fallback |

### Tier 2 — Semantic / RAG faults (CJ Layer 4) — the "silent-wrong" story
The HTTP call succeeds and the JSON is valid, but the *content* is wrong — so **fallback never
fires** and consensus itself must absorb a corrupted cost signal. This is the most interesting tier.

| # | Fault | Hypothesis |
|---|-------|------------|
| S1 | `SemanticCorrupt(entity_swap)` | Skewed capacities distort scores; consensus tolerates a minority of poisoned bidders |
| S2 | `SemanticCorrupt(rag_poison)` | Poisoning peer-load context defeats load-aware scoring ⇒ dog-piling, fairness drops |
| S3 | `SemanticCorrupt(inject_distractor)` | Resilience to indirect prompt injection via gossiped peer state |
| S4 | `SemanticCorrupt(context_truncate)` | Degraded but not incorrect scheduling |

**Headline experiment:** sweep the poisoned-agent fraction 0→50% to find the tolerance threshold —
*"SwarmAgents tolerates up to K% semantically-corrupted LLM agents before completion and fairness
degrade."*

### Tier 3 — Composite "bad day" (the single infra touchpoint)
| # | Fault |
|---|-------|
| X1 | `LLMLatency` + `SemanticCorrupt(rag_poison)` on a minority + node loss, simultaneously. Does completion hold, and does double-assignment stay 0? |

### Tier 4 — Dosing strategies
Wrap Tier 1–2 faults in CJ strategies: percentage-based, ramp-up (cleanest degradation curves), and
targeted (coordinators only, hierarchical arm).

### Baselines & ablations
1. **Clean baseline** — done (§3)
2. **Analytic-only** (`resource` agents) — isolates what the LLM adds
3. **LLM with fallback disabled** — exposes true LLM dependence; expect sharp cliffs where the
   default config stays flat. *This is what proves the safety net's value.*
4. **Consensus ablation** — Snow vs PBFT under the same faults

---

## 6. Findings for the CJ maintainers

Full report with reproductions, evidence and suggested fixes:
**[`CHAOS_JUNGLE_FINDINGS.md`](CHAOS_JUNGLE_FINDINGS.md)** — written to be filed upstream.

| # | Severity | Summary |
|---|----------|---------|
| 1 | **Blocker** | `main` / v1.5.0 cannot be imported — `InjectResult` and `ChaosFuzzer` are re-exported by `__init__.py` but defined nowhere. Broken since `21765afb` (2026-07-06); every later commit is docs-only. Pin `5044939…`. |
| 2 | High | The docs site's first install option, `pip install chaos-jungle`, cannot work — not on PyPI (404). The README's `git+https://…` form is correct. |
| 4 | **High** | `chaos-jungle stop` always crashes (`ChaosRunner.attach()` bypasses `__init__`, leaving `_timer` unset), so a session can never be reverted from the CLI and stale `running` rows accumulate. |
| 3 | Medium | `upstream` must be an **origin**: CJ appends the request path, so a `/v1` suffix yields `/v1/v1/…` → 404. The delay still applies, so the fault looks installed while nothing is forwarded — a latency experiment silently becomes an outage experiment. |

**CJ works as advertised once installed from the right commit** — we reproduced a clean
`measure()` delta (1.02 s → 3.08 s under `LLMLatency(delay_s=3)`) and ran a full-fleet
`LLMUnavailable` scenario end to end (§4b).

Three items in earlier drafts were **withdrawn after checking**: a supposed parameter-name drift
in `LLMRateLimit`/`LLMTimeout` (the library and `LLM_SCENARIOS.md` agree; we had confused it with
the separate `intercept.RateLimit`), a supposed broken Quickstart link (it is at the site root and
returns 200), and a supposed HTTPS-forwarding defect (it was our own `/v1` upstream suffix; a
control call bypassing the proxy reproduced the identical error).

## 7. SwarmAgents bugs found and fixed

Full write-up with reproductions and suggested fixes:
**[`SWARMAGENTS_FINDINGS.md`](SWARMAGENTS_FINDINGS.md)**.

Most were **silent** — the system kept running and produced plausible results while a scheduling
term was inert, the fleet differed between runs, or the LLM was never consulted.

| # | Status | Summary |
|---|--------|---------|
| 1 | Fixed `c51b6af9` | Ollama provider unusable — the resolved endpoint was computed then discarded |
| 2 | Fixed `c51b6af9` | Small models fail tool-call schemas; `NativeOutput` is the working mode (3/3 vs 0/3) |
| 3 | Fixed `bf5e2e44` | Job DTN names never matched agent DTNs → connectivity term dead in two baselines |
| 4 | Fixed `bf5e2e44` | Unseeded per-run fleet regeneration made runs incomparable (`--seed`) |
| 5 | Fixed `fb60a3ef` | Per-file DTN spread left 76/300 jobs unschedulable and stalled a run (`--dtn-scope job`) |
| 6 | **Open** | `run_test.py` deletes `agent_hosts.txt` then crashes reading it — use another filename |
| 7 | **Open** | `llm.timeout_seconds` is parsed but never enforced; bids ran 14.9 s, 19.2 s, once 20 min |
| 8 | **Open** | Converter mutates module-level `INSTANCE_FLAVORS` via aliased dicts |
| 9 | **Open** | `Job.execute()` sleeps a flat 1 s, so **makespan is not comparable** to the source workflows |

## 8. Runbook

`ssh chaos` lands on `database` as `ubuntu`; the repo is at `/root/SwarmAgents` (use `sudo`).
`database` has passwordless root SSH to `agent-1 … agent-30`. Python is `python3.11`.

### 8.0 Mandatory pre-run health gate — before EVERY run
**Every host must prove it can infer.** A single host whose Ollama has lost the model still runs, and
its agent silently falls back to analytic cost for the whole run — that contaminated one baseline
with 130 fallbacks from one host (agent-11) before it was caught. Process count and `/api/tags` do
**not** detect this; only a real generate does.
```bash
ssh chaos 'sudo bash -c '"'"'for h in $(cat /root/SwarmAgents/agent_hosts_cj.txt); do (ssh -o StrictHostKeyChecking=no $h "bash /root/fixmodels.sh" 2>/dev/null) & done; wait'"'"'' | awk '{print $2}' | sort | uniq -c
# expect: 30 OK   (fixmodels.sh repairs model visibility if needed, then verifies with a real generate)
```

### 8.1 Mandatory pre-run cleanup — before EVERY run
Leftover agents on *any* host register into the shared Redis and stall the next run at
`[SEL_WAIT] … live≠configured`. Stale per-host logs also inflate the result counters, since they
append across runs.

```bash
ssh chaos 'sudo bash -c '"'"'
for h in $(cat /root/SwarmAgents/agent_hosts_cj.txt); do
  ssh -o StrictHostKeyChecking=no $h "pkill -9 -f main[.]py; rm -f /root/SwarmAgents/swarm-multi/agent-*.log" 2>/dev/null &
done; wait
pkill -9 -f main[.]py; rm -f /root/SwarmAgents/swarm-multi/agent-*.log
docker exec redis redis-cli flushall
echo "dbsize=$(docker exec redis redis-cli dbsize)"
'"'"''
```

### 8.2 Select the LLM arm
```bash
# Ollama (default; used for all fault work)
ssh chaos 'sudo sed -i "s/^  provider:.*/  provider: ollama/; s/^  model:.*/  model: \"qwen2.5:3b\"/" /root/SwarmAgents/config_swarm_multi.yml'

# Gateway (fault-free spot-checks)
ssh chaos 'sudo sed -i "s/^  provider:.*/  provider: openai/; s/^  model:.*/  model: \"gpt-oss-20b\"/" /root/SwarmAgents/config_swarm_multi.yml'
```
Pre-warm Ollama before launching agents, or the first score per agent hits an ~11.5 s cold model
load and falls back once:
```bash
ssh chaos 'sudo bash -c '"'"'for h in $(cat /root/SwarmAgents/agent_hosts_cj.txt); do ssh -o StrictHostKeyChecking=no $h "bash /root/cj_infer_check.sh" & done; wait'"'"''
```

### 8.2b Frozen fleet + trace (do this ONCE, then reuse for every run)

Agent profiles are otherwise **randomly regenerated on every run** — `cleanup_between_runs` deletes
`agent_profiles.json`/`agent_dtns.json` and `generate_configs.py` recreates them with unseeded
`random`. Baseline and fault runs would then use different agent fleets, confounding every delta.
Fix: generate once with `--seed`, then reuse via `--use-config-dir`.

```bash
# (a) Build the mixed trace (byte-identical for a given seed) and convert with matching DTN names
python3 build_mixed_trace.py \
  --cpegasus <cpegasus>/all_runs_jobs_profile.json \
  --pegasus  <pegasus>/profiles/all_runs_jobs_profile.json \
  --n-cpegasus 200 --n-pegasus 100 --seed 42 --output mixed_profile_300.json
python3 pegasus_to_swarm_converter.py --input mixed_profile_300.json --input-type json \
  --output-dir mixed_jobs_300/ --data-nodes per-file --dtn-scope job \
  --dtn-names dtn1,dtn2,dtn3,dtn4,dtn5,dtn6,dtn7,dtn8,dtn9,dtn10

# (b) Generate the frozen fleet ON THE SLICE. Start from a clean state — generate_configs REUSES
#     an existing agent_dtns.json, which silently breaks reproducibility.
ssh chaos 'sudo bash -lc "cd /root/SwarmAgents && rm -rf configs agent_profiles.json agent_dtns.json && \
  python3.11 generate_configs.py 30 10 ./config_swarm_multi.yml /root/SwarmAgents/configs mesh database 300 \
    --dtns --seed 42 --agent-hosts-file agent_hosts_cj.txt --agents-per-host 1 --agent-type llm"'

# (c) Stage the jobs and snapshot everything
ssh chaos 'sudo bash -lc "cd /root/SwarmAgents && rm -rf jobs && mkdir -p jobs && \
  cp mixed_jobs_300/job_*.json jobs/ && \
  tar czf /root/frozen_fleet_seed42.tgz configs jobs agent_profiles.json agent_dtns.json"'
```
Verified: regenerating from a clean state with `--seed 42` reproduces identical `agent_profiles.json`,
`agent_dtns.json`, and all 30 per-agent configs. Restore the snapshot any time with
`tar xzf /root/frozen_fleet_seed42.tgz -C /root/SwarmAgents`.

> The generated artifacts (`mixed_profile_300.json`, `mixed_jobs_300/`, `jobs/`, `configs/`) are
> **not in git** — the repo ignores `*.json` and `jobs/`. They are fully rebuildable from the
> commands above, since both the trace sampling (`--seed 42`) and the fleet (`--seed 42`) are
> deterministic. A copy of the trace is kept outside the repo at
> `pegasus-profiles/mixed-trace/`, and the slice snapshot at `/root/frozen_fleet_seed42.tgz`.

### 8.3 Run
```bash
ssh chaos 'sudo bash -lc "cd /root/SwarmAgents && nohup python3.11 run_test.py \
  --mode remote --agent-type llm --agents 30 --agents-per-host 1 \
  --topology mesh --jobs 300 --db-host database --agent-hosts-file agent_hosts_cj.txt \
  --use-config-dir \
  --jobs-per-interval 30 --stable-seconds 120 --runtime 3000 \
  --generate-plots --run-dir runs/<SCENARIO> > runs_<SCENARIO>.log 2>&1 &"'
```
- **`--use-config-dir` is what makes runs comparable** — it reuses the frozen fleet and staged
  `jobs/` instead of regenerating a fresh random fleet and re-converting. Redis is still flushed.
- Use a **unique `--run-dir` per run**; together with §8.1 this guarantees clean per-run counters.
- Pass `--agent-hosts-file agent_hosts_cj.txt` — **not** `agent_hosts.txt`. run_test deletes the
  literal `agent_hosts.txt` during cleanup and then crashes trying to read it.
- Hierarchical arm: `--topology hierarchical --hierarchical-level1-agent-type llm --groups G --group-size S`.

### 8.4 Read the results
```bash
ssh chaos 'sudo bash -lc "cd /root/SwarmAgents/runs/<SCENARIO> && \
  echo COMPLETE=\$(grep -rho LLM_COST_COMPLETE . | wc -l) \
       FALLBACK=\$(grep -rho LLM_COST_FALLBACK . | wc -l) \
       BIDWON=\$(grep -rho LLM_BID_WON . | wc -l)"'
```
Healthy baseline ⇒ `COMPLETE > 0`, `FALLBACK ≈ 0`. Plots are written into the run dir.

### 8.5 Inject a fault
```bash
# Choose the blast radius: which hosts get a fault proxy (subset ⇒ poisoned-fraction curves)
FAULTED="agent-1 agent-2 agent-3"

ssh chaos 'sudo bash -c '"'"'
for h in '"$FAULTED"'; do
  scp -o StrictHostKeyChecking=no /root/SwarmAgents/cj_proxy.py $h:/root/SwarmAgents/ >/dev/null 2>&1
  ssh -o StrictHostKeyChecking=no $h "cd /root/SwarmAgents && nohup python3.11 cj_proxy.py \
      --fault latency --delay 3.0 --port 18011 --upstream http://127.0.0.1:11434/v1 \
      --base-url-env OLLAMA_BASE_URL > /var/log/cj_proxy.log 2>&1 &
    grep -q OLLAMA_BASE_URL /root/.profile || echo export OLLAMA_BASE_URL=http://127.0.0.1:18011/v1 >> /root/.profile"
done'"'"''
```
Available faults (`cj_proxy.py`, in this repo):

| `--fault` | Options |
|---|---|
| `latency` | `--delay S` |
| `unavailable` | — (503) |
| `timeout` | `--timeout-s S` |
| `ratelimit` | `--after N` |
| `corrupt` | `--mode truncate\|empty\|invalid_json` |
| `semantic` | `--mode entity_swap\|context_truncate\|inject_distractor\|rag_poison` |

Then run §8.1 → §8.3 into a fault-labelled run dir and compare with §8.4.

**Tear down before the next scenario** — otherwise the fault leaks into later runs:
```bash
ssh chaos 'sudo bash -c '"'"'
for h in $(cat /root/SwarmAgents/agent_hosts_cj.txt); do
  ssh -o StrictHostKeyChecking=no $h "pkill -f cj_proxy.py; sed -i /OLLAMA_BASE_URL/d /root/.profile" 2>/dev/null &
done; wait; echo faults-cleared'"'"''
```

### 8.6 Regenerating the Pegasus workload (optional)
```bash
# On the Pegasus submit host (successful runs only):
ssh cpegasus 'python3 pegasus_profile_extractor.py --root /home/cc --output ~/all_runs_jobs_profile.json'
# Relay via laptop (submit host and slice are on different networks), then convert:
ssh chaos 'sudo bash -lc "cd /root/SwarmAgents && python3.11 pegasus_to_swarm_converter.py \
  --input all_runs_jobs_profile.json --input-type json --output-dir pegasus_jobs/ \
  --data-nodes per-file --dtn-names dtn1,dtn2,dtn3,dtn4,dtn5,dtn6,dtn7,dtn8"'
```

### 8.6b Leave the slice idle when you stop

Scenarios call `cleanup()` at their *start*, so a run tidies up after its predecessor but never
after itself — stop a batch and 29 agents and ~1700 Redis keys are left behind, which will stall
the next run at `[SEL_WAIT] live != configured`. When finished:
```bash
ssh chaos 'sudo bash -lc "cd /root/SwarmAgents && python3.11 scenarios/clear_faults.py"'
# expects: clean check ... / idle check: 0 stray agents, 0 Redis keys / slice idle
```

### 8.7 Teardown / secret hygiene
```bash
# Remove the gateway API key from all hosts when finished:
ssh chaos 'sudo bash -c '"'"'for h in $(cat /root/SwarmAgents/agent_hosts_cj.txt); do ssh -o StrictHostKeyChecking=no $h "sed -i /OPENAI_API_KEY/d /root/.profile" & done; wait; sed -i "/OPENAI_API_KEY/d" /root/.profile'"'"''
```

### 8.8 Helper scripts on the slice
| Script | Purpose |
|---|---|
| `/root/cj_install.sh` | Install the pinned CJ commit |
| `/root/cj_ollama_setup.sh` | Install Ollama, pull model, pre-warm |
| `/root/cj_ollama_fastfix.sh` | Repair a broken Ollama install from a local tarball |
| `/root/cj_infer_check.sh` | **Real** inference health check |
| `/root/fixmodels.sh` | Repair model visibility for the running ollama user, then verify (§8.0 gate) |
| `/root/cj_ollama_reset.sh` | Reset Ollama: drop pinning, kill all servers, start one on all cores |
| `/root/cj_verify_fleet.sh` | Verify **all** ollama PIDs pinned + inference OK |
| `/root/SwarmAgents/cj_proxy.py` | Per-host CJ fault proxy driver |
| `scenarios/clear_faults.py` | **Return the slice to idle** — clear faults, stop agents, flush Redis |

---

## 9. Operational gotchas

Each of these cost real debugging time; all are guarded against above.

- **`/api/tags` is not an Ollama health check.** It only proves a model was *pulled*. It reported
  "30/30 ready" while **15 of 30 hosts could not infer at all** — their installs had been truncated
  by a dropped SSH, leaving no `llama-server` binary, so every call returned HTTP 500 and agents
  silently fell back to analytic cost. This invalidated a full baseline run before it was caught.
  **Always health-check with a real `/api/generate` call.**
- **Don't re-download from the internet when a healthy peer has the bits.** 15 concurrent Ollama
  downloads reached 47% in 35 minutes over the shared IPv6 egress. Packaging the CPU runtime from a
  healthy host (2.1 GB → 42 MB after excluding unused CUDA libs) and copying it internally took
  ~2 minutes.
- **`run_test.py` deletes `agent_hosts.txt`** during cleanup, then crashes reading it. Use a
  different filename.
- **Local-mode runs overwrite `agent_hosts.txt`** with `localhost`. Restore from `/etc/hosts`.
- **`pgrep -f <pattern>` matches its own command line** — a check for "no agents running" can report
  phantom processes. Use a bracket pattern like `mai[n].py`.
- **Never restrict Ollama's CPUs without also cutting its thread count** — llama.cpp busy-waits one
  thread per core, so pinning alone made inference 35-70x slower. Ollama has no thread-count env var. See §2.3.
- **Half the fleet runs Ollama under systemd**, which respawns it and ignores a `pkill`+`nohup`
  restart; a leftover server can survive `systemctl restart` still holding the port; and its service
  user reads a different models directory than root. See §2.3.
- **`pgrep -f <pat> | head -1` is not a fleet check** — it picks an arbitrary PID among matches and
  produced three different answers for one unchanged fleet. Walk every matching PID.
- **`timeout_seconds` in the LLM config is not a hard client timeout** — calls of 14.9 s were
  observed without triggering fallback.
- **Agent profiles are random per run unless seeded and reused.** `generate_configs.py` used
  unseeded `random` for capacities, flavors and DTN assignment; a `--seed` flag was added. Seeding
  alone is not enough — `generate_configs` **reuses an existing `agent_dtns.json`** if present,
  taking a different code path and producing different capacities, so always regenerate from a
  clean state.
- **DTN names must match between jobs and agents**, or the connectivity term silently disappears.
  Fixed in `run_test.py` (§2.2); if you call `pegasus_to_swarm_converter.py` directly, pass
  `--dtn-names dtn1,…,dtn10 --dtn-scope job` yourself — its own defaults are still the historical
  per-site naming and per-file scope.
- **A job may not require more DTNs than an agent holds.** Feasibility is all-or-nothing over a
  job's DTNs; per-file spreading deadlocked a run (§2.2).

---

## 10. Next steps

1. **Phase 1 — Tier 1 sweep** on the Ollama arm. Suggested start: **L1 `LLMLatency`** (clean
   dose-response curve) or **L3 `LLMUnavailable`** (sharpest graceful-degradation signal — should
   drive fallback from 0% to 100% and prove the analytic safety net).
2. **Phase 2 — Tier 2 semantic sweep**, including the poisoned-fraction tolerance curve.
3. **Phase 3 — ablations**, especially fallback-disabled.
4. **Phase 4 — composite X1** and hierarchical/targeted-coordinator scenarios.
5. **Report the CJ issues in §6 upstream.**

**Open question for the team:** which figures are must-haves for the paper?

- **A** — Degradation curves: completion rate & P95 latency vs fault severity
- **B** — Fallback rate by fault type (the graceful-degradation headline)
- **C** — Poisoned-agent tolerance threshold (fairness & conflicts)
- **D** — Default vs fallback-disabled (value of the analytic safety net)
- **E** — Composite "bad day", annotated with the zero-double-assignment invariant
