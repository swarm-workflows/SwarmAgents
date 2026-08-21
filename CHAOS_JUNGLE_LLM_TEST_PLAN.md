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

### 2.1b Ollama Cloud — evaluated and rejected as a primary arm (2026-08-19)

Motivated by a real question: local `qwen2.5:3b` produces a degenerate cost signal (59% of bids
are the identical value), so would a frontier model give the scheduler something to work with?
Ollama Cloud (`https://ollama.com/v1`, key in root-only `/root/.ollama_cloud_key`, **never in the
repo or a config**) exposes 19 models including `gpt-oss:20b`, `gpt-oss:120b` and `qwen3.5:397b`.

**Reachability is fine** — unlike the FABRIC gateway, `ollama.com` publishes AAAA records and the
slice reaches it over IPv6 in 0.15 s, no `/etc/hosts` mapping needed.

**Signal quality is genuinely better.** 40 identical `<job, agent>` pairs drawn from the frozen
trace and frozen profiles, through the production system prompt and the same json_schema output
the agents use:

| model | where | latency (mean) | distinct scores /40 | modal share | top-2 share | sd |
|---|---|---|---|---|---|---|
| `qwen2.5:3b` | local | 5.77 s | 15 | 25% | 40% | 24.2 |
| `gpt-oss:20b` | cloud | 2.57 s | 11 | 32% | 57% | 38.2 |
| **`gpt-oss:120b`** | cloud | **1.24 s** | **19** | **18%** | **28%** | 31.0 |

`gpt-oss:120b` discriminates best *and* is 4.6x faster than the local 3B. Note `gpt-oss:20b`
reproduces the coarseness seen in the `cj-baseline-gw` gateway run of the same model — two
independent measurements agreeing that 20B is worse than 3B here.

**But it fails at fleet scale, in two separate ways.** Sweeping concurrency against
`gpt-oss:120b` (the 1.24 s figure above is a *single-client* measurement):

| simultaneous requests | mean latency | slowdown vs sequential | responses |
|---|---|---|---|
| 1 (sequential) | 1.63 s | — | all 200 |
| 4 | 2.27 s | 1.54x | all 200 |
| 8 | 4.03 s | 2.66x | all 200 |
| 16 | 6.24 s | 4.00x | all 200 |
| **30** (fleet size) | 4.10 s (p95 11.72 s) | 2.51x | **13x 429**, 17x 200 |

*First:* **the endpoint serializes.** Latency scales with concurrency — by 16 simultaneous
bidders it is 6.24 s, i.e. *slower than the local 3B it was meant to replace*, and the 4.6x speed
advantage has evaporated. Worse than the raw number, agent i's bid latency now depends on what
agents j≠i are doing. Local per-host Ollama makes bid latency independent by construction; that
independence is exactly what a controlled blast-radius study needs, and it is precisely what §S09
showed decides placement. Coupling it means every agent's outcome depends on fleet-wide load.

*Second:* **at fleet concurrency, 13 of 30 requests are rate-limited** — and a 429 is not benign
here. In `LlmAgent._llm_or_analytic_cost` *any* exception becomes `[LLM_COST_FALLBACK]` and an
analytic cost returned in ~0 s, and S05 established that a ~0 s bid **wins** the race to propose.
A shared quota would hand the workload to whichever agents happened to be throttled, on every
run — reproducing the exact pathology under study as an uncontrolled background fault beneath
whatever CJ is deliberately injecting.

> *Caveat on the 429s — since resolved by measurement, against the prediction.* The sweep above
> fires 30 requests in the same instant, which is the worst case; a real run makes ~1074 calls
> over ~11 minutes. A full 30-agent cloud run produced **2 fallbacks in 1076 calls (0.2%)**, so
> the agents' bidding is spread enough in practice that the rate limit is essentially never hit.
> The predicted "uncontrolled L4 fault under every run" did not happen. The serialization result
> stands and needs no caveat — it reproduces at every concurrency level tested, and it shows up
> in the run as bid-latency p95 (20.75 s cloud vs 12.52 s local) even while the mean improves.

**Measured end to end, the cloud arm is better on every scheduling metric** (§3, `cj-baseline-cloud`).
The health gate is the one place the concurrency ceiling still bites: probing all 30 hosts at
once rate-limits the *health check*, so it runs in batches of 4.

**Not every cloud model is usable at all, regardless of the above.** Two properties have to be
checked before adopting one, because failing either shows up in a run as `[LLM_COST_FALLBACK]`
rather than as an error:

| model | honours `json_schema` | reasons by default | usable |
|---|---|---|---|
| **`gpt-oss:120b`** | **yes** (40/40 parsed) | no | **yes** |
| `gpt-oss:20b` | yes (40/40 parsed) | no | yes, but coarser than the 3B |
| `qwen3.5:397b` | **no** — answers in prose | **yes** — `"content":""`, budget spent in `reasoning` | **no** |

SwarmAgents requires structured output (`NativeOutput(Bid)`, finding 2), so a model that ignores
the schema makes every bid fall back and the LLM arm silently ceases to exist. A reasoning model
is also far too slow for a per-job scoring loop — the same reason §2.1 ruled out three of the
gateway's four models at 37-43 s.

**Verdict (revised 2026-08-20 — the FABRIC gateway is the better remote arm).** Ollama Cloud's
signal quality is the best of any endpoint, but its rate limit makes it unfit as a campaign's
primary: 13 of 30 concurrent requests 429, and a 429 becomes a fallback, which wins the race to
propose. The **FABRIC LiteLLM gateway takes all 30 concurrent with no 429s at all** and has a far
tighter latency tail (p95 7.47 s vs 20.75 s), at the cost of a coarser cost signal — see the
gateway-arm section for the full three-arm comparison.

| use | arm |
|---|---|
| fault campaigns at 30 agents | **FABRIC gateway** (no rate limit, tight tail) — or local Ollama, whose per-host independence is still the cleanest |
| sequential ablations (e.g. score granularity) | Ollama Cloud — cheap, no concurrency, best models |
| "does this survive a frontier model?" | Ollama Cloud `gpt-oss:120b`, small fleet |

Before scaling any cloud usage up, check the account's actual rate limit — the ceiling here was
measured, not looked up, and a paid tier may move it.

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

**`cj-baseline-cloud`** (2026-08-19) is the reference for the **cloud arm** —
`gpt-oss:120b` over Ollama Cloud with local Ollama stopped fleet-wide, same frozen fleet, trace
and gate (§2.1b). Deltas only mean anything within one arm, so cloud scenarios compare against
this and never against `cj-baseline-ref`; point a run at it with
`CJ_REFERENCE=scenarios/reference_cloud.json`.

| metric | `cj-baseline-ref` (local 3B) | **`cj-baseline-cloud`** (120B) |
|---|---|---|
| jobs completed / stuck | 300 / 0 | 300 / 0 |
| fallback rate | 0.0% | 0.2% (2 of 1076) |
| bid latency mean / p95 | 9.90 s / 12.52 s | **7.48 s** / 20.75 s |
| **sched latency mean** | 368.9 s | **235.8 s** (−36%) |
| **load fairness** | 0.681 | **0.809** |
| LLM score mean / sd | 70.4 / 14.8 | 89.5 / 10.2 |
| SWIM false-fails | 9 | **1** |
| low-id capture ratio | 1.59x | **1.14x** |

Two results worth separating. The queue drains 36% faster and fairness rises to 0.809 — but note
*why*: the local arm's figures were dragged down by two starved hosts bidding at 139-250 s
(§S09), and moving inference off the hosts removes that failure mode entirely rather than
improving scheduling per se. The **capture ratio falling from 1.59x to 1.14x** is the more
interesting one: it is independent confirmation of the S09b mechanism. Uniform bid latency
across the fleet flattens the positional skew that no tie-break change could touch, because the
skew was never about ordering — it was about who bids first.

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
  api/s09_semantic.py      S09  [mode] [fraction]
cj_proxy.py                per-host CJ fault proxy (agents reach it via OLLAMA_BASE_URL)
cj_probe.py                per-host semantic-fault probe (prompt tokens, with vs without)
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
monotonically as the outage spreads, ending *best* under total failure.

> **Later correction to the "ending best" half of that.** The recovery to 0.843 at 100% is
> relative to a baseline (0.681) that was itself depressed by heterogeneous host inference. On
> the FABRIC gateway arm, where the fault-free baseline is already 0.849, the same total outage
> takes fairness *down* to 0.795. The U-shape across the blast radius is real; the claim that a
> total outage *improves* fairness is an artefact of a poor baseline. See the gateway-arm
> section. Completion never
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

### S09 — SemanticCorrupt(entity_swap): the silent-wrong tier (2026-08-19)

The first Tier 2 scenario. HTTP and JSON stay intact and the reply parses, so **the fallback
path is unreachable by construction** — there is no exception to catch. Only the *content* of
the request changes, on its way to the model.

Applying CJ's own mutation offline to SwarmAgents' real scheduling prompt shows exactly what
`entity_swap` does to it — one word, in the system prompt:

```
-Score this job for THIS agent (higher = better fit, 0-100).
+Score this job for THIS agent (lower = better fit, 0-100).
```

The JOB/AGENT/PEERS payload is untouched. A poisoned agent therefore **inverts its own bid
polarity** while paying the same ~10 s it always paid — the controlled contrast S01 and S05
could not give us: same timing regime, wrong content.

The model obeys precisely. The modal bid flips to its own complement:

| | modal score | count |
|---|---|---|
| fault-free reference | **75.00** | 626 of 1064 calls |
| poisoned agents (50%) | **25.00** | most common, ahead of 75.00 |

| poisoned hosts | fallback rate | **LLM score mean** | load fairness | sched latency mean | jobs completed | jobs stuck |
|---|---|---|---|---|---|---|
| 0% (reference) | 0.0% | 70.4 | 0.681 | 368.9 s | 300 | 0 |
| **25%** (8/30) | **0.0%** | **63.4** | 0.719 | 356.2 s | 300 | 0 |
| **50%** (15/30) | **0.0%** | **59.9** | 0.675 | 349.2 s | 300 | 0 |
| **100%** (30/30) | **0.0%** | **44.1** | 0.640 | 374.4 s | 300 | 0 |

Within the mixed runs the corruption is sharply localised, and the healthy group reproduces
the fault-free baseline (70.4) to within a point — an internal control that the poisoning did
not leak across the fleet:

| poisoned | poisoned-agent score | healthy-agent score |
|---|---|---|
| 25% | **43.1** | 71.3 |
| 50% | **48.3** | 70.7 |

**Headline: the bids were corrupted and the schedule did not move.** Fallback rate stays at
0.0% (the fault is silent, as designed), and completion, stuck jobs, scheduling latency and
fairness are all flat — but so is *placement itself*. Jobs per agent-id decile barely move
between a clean run and a fully poisoned one:

| run | agents 1-10 | 11-20 | 21-30 |
|---|---|---|---|
| fault-free reference | 153 | 82 | 65 |
| 50% poisoned | 151 | 70 | 79 |
| 100% poisoned | 150 | 89 | 61 |

**Mechanism — placement is decided by a race, not by the bid.** The first explanation we
reached for was the tie-break: 59% of all bids in the clean run are the *identical* value 75.00,
exact ties went to the lowest agent id, and the proposal advertised `cost + self.agent_id`
(a ±30 term on a 0-100 scale). All of that is real and is now fixed — see §S09b — **and fixing
it changed nothing**, which is how we found the actual mechanism.

What decides placement is **how fast an agent produces a bid**. Ranking agents by mean bid
latency against jobs won gives a consistent negative correlation in every run measured, with no
fault present:

| run | spearman(bid latency, jobs won) | slowest / fastest agent |
|---|---|---|
| fault-free reference | −0.38 | 13.0 s / 7.0 s |
| fault-free, tie-break fixed | −0.34 | 249.6 s / 6.5 s |
| S09 100% poisoned | −0.42 | 13.0 s / 7.2 s |
| S09 50% poisoned | −0.64 | 20.0 s / 6.8 s |

The extremes make it plain (`cj-baseline-fixedtb`, 30 agents, 300 jobs, fleet mean 10 jobs):

| agent | mean bid latency | jobs won |
|---|---|---|
| 22 | 249.6 s | **0** |
| 14 | 76.7 s | **0** |
| 8 | **6.5 s** (fastest) | **43** |
| 7 | 6.8 s | 14 |

Agent id looked like the cause only because low-numbered hosts on this slice happen to infer
faster: `corr(agent id, bid latency)` runs +0.13 to +0.35 across runs.

This is **S05's race-to-propose mechanism, present with no fault at all**. There it took a 503
to make an agent bid in ~0 s instead of ~10 s; here ordinary variation in inference speed does a
weaker version of the same thing, continuously. And it explains S09 exactly: `entity_swap`
changes *what* an agent bids, not *when*, so it cannot move an outcome that timing decides.
**The chaos fault did not find a weakness in the scheduler's tolerance; it showed that the
LLM's output has little influence on the scheduler's decision.** That is the finding worth
reporting, and only a semantic fault could produce it — every Tier 1 fault perturbs timing,
which is precisely the channel that works.

> **Method note — a capture ratio is meaningless without its no-fault control.**
>
> | scenario | split at id | measured ratio | fault-free control | verdict |
> |---|---|---|---|---|
> | S09 25% | 8 | 1.56x | 1.86x | **no capture** |
> | S09 50% | 15 | 1.48x | 1.59x | **no capture** |
> | S05 25% | 8 | **19.25x** | 1.86x | real, 10x its control |
>
> The faulted hosts are always the low agent ids, so the positional bias points the same way as
> the effect being measured. `helpers.load_split()` now computes the reference run's ratio at the
> same split point and prints both, so the confound cannot recur. S05's finding stands — but it
> should be quoted against its 1.86x control, not against 1.0.

> **Correction.** An earlier version of this section stated that placement "is dominated by
> agent id". That was inference from code reading, not measurement: the id tie-break and the
> id-laden proposal cost exist and point that way, but removing both left the distribution
> where it was. Agent id was a proxy for host inference speed. The observation — corrupted
> bids, unchanged schedule — held; the explanation did not.

**The SWIM churn has an answer, and it is the same one.**

| run | SWIM false-fails | agents declared failed | most-suspected targets |
|---|---|---|---|
| fault-free reference | 9 | 0 | spread |
| S09 25% / 50% / 100% | 60 / 50 / 6 | 1 / 2 / 0 | spread |
| tie-break fixed, run 1 | 89 | 2 | **22** (13), **14** (11) |
| tie-break fixed, run 2 | 98 | 3 | **22** (7), 24 (7) |

The named targets are exactly the agents bidding at 139-250 s, and exactly the ones that won
**zero** jobs.

The obvious reading — an agent blocked for minutes inside a bid answers its SWIM probes late —
is **wrong, and the S01 sweep is what disproves it.** Injecting +60 s per bid on healthy hosts
produces *no* membership effect: SWIM churn stays at baseline and no agent is declared failed
(see the S01 dose-response section). A blocked bid is therefore not the mechanism.

What these two hosts had that a latency-faulted host does not is **memory starvation**, and a
thrashing host starves the SWIM responder thread itself. The slow bids and the missed probes are
two symptoms of one cause, not one causing the other. **A slow LLM is not a membership risk; a
starved host is.**

#### Root cause of the unplanned fault: memory, not the model (2026-08-19)

Diagnosed after the fact, because it changes what the health gate has to check. With the fleet
idle, **agents 14 and 22 answer a single inference in 0.39 s** — as fast as anyone. They are not
slow hosts. What they are is *full*:

| host | `llama-server` RSS | available RAM | buff/cache | bid latency under load | jobs won |
|---|---|---|---|---|---|
| agent-14 | 7.4 GB | **79 MB** | 192 MB | 77-139 s | **0** |
| agent-22 | 7.4 GB | **102 MB** | 196 MB | 150-250 s | **0** |
| agent-24 | — | 136 MB | — | 12.0 s | 1 |
| agent-8 | 7.0 GB | 504 MB | 493 MB | **6.5 s** (fastest) | 46 |
| agent-7 | 4.3 GB | 3133 MB | 2660 MB | 6.8 s | 14 |

These hosts have 7.9 GB and **no swap**. A `llama-server` left running for days grows to ~7.4 GB
— the model itself is only 2.2 GB — leaving under 100 MB for everything else. Start a Python
agent next to that and the two thrash: page cache collapses to ~190 MB (versus 2.7 GB on a
healthy host) and a bid takes minutes.

Rank correlations across the fleet, and what restarting Ollama recovers:

| relationship | spearman | reading |
|---|---|---|
| available MB → bid latency | **−0.60** | memory pressure predicts slowness |
| bid latency → jobs won | −0.49 | slow bidders lose work |
| available MB → jobs won | **+0.05** | **a cliff, not a slope** |

| agent-14 | before restart | after `systemctl restart ollama` |
|---|---|---|
| available RAM | 79 MB | **7360 MB** |
| `llama-server` RSS | 7.4 GB | released in full |

Memory does not predict *jobs won* because the effect is a cliff: agent-8 sits at 504 MB and is
the fastest bidder in the fleet, while below ~150 MB an agent stops winning work entirely.

Three consequences:

1. **The health gate now checks memory** (`helpers.MIN_AVAILABLE_MB = 300`). Proving a host can
   infer proves nothing here — a starved host answers an idle probe in 0.4 s and still fails to
   place a single job for an entire run.
2. **Restart Ollama across the fleet before a measurement campaign**, not just between runs. The
   growth is cumulative over days.
3. **Ollama is not managed the same way on every host.** `systemctl restart ollama` is a silent
   no-op on agent-4 and agent-8, where `ollama serve` runs outside systemd (PPID 1, started by
   hand). Those two did not recover from the fleet restart. They are above the cliff, so this
   did not affect the results — but a restart script that assumes systemd will quietly skip them.

*Still to run in this tier:* `rag_poison` (injects a false-context line mid-payload — it also
splits the JOB JSON), `inject_distractor` (contradictory instruction appended to the system
prompt), `context_truncate` (the agent bids on a job it can only half see, and never sees PEERS
at all). All three are wired and their fault semantics verified; only `entity_swap` has been run.


### S09b — fixing the tie-break, and what it proved (2026-08-19)

S09's first explanation was the tie-break, so we fixed it and re-measured. The change
(`swarm/utils/tiebreak.py`) replaces "lowest agent id wins a tie" with a per-object
pseudorandom rank, in all four places that ordered agents by id — the selection engine, the
PBFT engine, the Snow engine's dominance rule, and `ProposalContainer` — and removes the
`+ self.agent_id` term from the advertised proposal cost.

Deployment and liveness were verified, not assumed:

| check | result |
|---|---|
| module imports on every host | 30/30 |
| identical rank for the same key across hosts | 30/30 (`14912286594027844952`) |
| advertised proposal cost, agent 7 | `Cost=25.00 FinalCost=32.00` → **`25.00`** |
| advertised proposal cost, agent 25 | `Cost=25.00 FinalCost=50.00` → **`25.00`** |
| unit tests | 163 pass (6 new) |

| run | code | agents 1-10 | 11-20 | 21-30 | idle agents |
|---|---|---|---|---|---|
| `cj-baseline-ref` | id tie-break | 153 | 82 | 65 | none |
| `cj-baseline-fixedtb` | fixed | 146 | 70 | 84 | 14, 22 |
| `cj-baseline-fixedtb2` | fixed | 158 | 76 | 66 | 14, 22 |

**The distribution did not move.** Two runs under the fix bracket the pre-fix run. The
hypothesis that placement was decided by agent id is therefore rejected by its own experiment,
and §S09's mechanism was rewritten around what the data does support: bid latency.

The fix is kept regardless: a ±30 id term on a 0-100 cost scale is not defensible whatever the
measured effect, it silently confounds every per-group analysis split by id, and 59% of bids
really do tie. `tests/test_tiebreak.py` pins both properties the tie-break must have at once —
which is how the first hash choice was caught:

| hash | deterministic across agents | win spread over 30 agents (3000 ties) | verdict |
|---|---|---|---|
| `hash()` | **no** — salted per process | — | unusable |
| `crc32` | yes | 166 max / 42 min (**4x**) | biased |
| **`blake2b`** | yes | **125 max / 68 min** (fair = 100) | adopted |

*Cost of the experiment:* three 30-agent runs, ~15 min each, all 300/300 complete, 0 stuck.
*Value:* a wrong explanation removed from the paper before it was published in it.


### Cloud arm — S01, S05 and S09 once each (2026-08-19)

`gpt-oss:120b` over Ollama Cloud, local Ollama stopped fleet-wide, same frozen fleet and trace.
Every row compares against `cj-baseline-cloud`, never the local reference.

| | baseline | S01 (+3 s, 100%) | S05 (503, 100%) | S09 (entity_swap, 100%) |
|---|---|---|---|---|
| jobs completed / stuck | 300 / 0 | 300 / 0 | 300 / 0 | 300 / 0 |
| fallback rate | 0.2% | 0.1% | **100%** | 16.8% |
| bid latency mean | 7.48 s | 7.34 s | — | 7.60 s |
| LLM score mean | 89.5 | 89.1 | — | **11.6** |
| sched latency mean | 235.8 s | 223.8 s | **66.7 s** | 202.2 s |
| load fairness | 0.809 | 0.823 | **0.878** | 0.764 |

**Placement by agent-id decile** — the headline is what did not happen:

| run | arm | 1-10 | 11-20 | 21-30 | idle agents |
|---|---|---|---|---|---|
| `cj-baseline-ref` | local 3B | 153 | 82 | 65 | none |
| `cj-baseline-fixedtb` | local, tie-break fixed | 146 | 70 | 84 | 14, 22 |
| `cj-baseline-fixedtb2` | local, tie-break fixed | 158 | 76 | 66 | 14, 22 |
| **`cj-baseline-cloud`** | **cloud 120B** | **112** | **100** | **88** | **none** |
| **`cj-s09-…-cloud`** | **cloud 120B** | **119** | **89** | **92** | **none** |

The skew disappears with no scheduler change — the tie-break fix (§S09b) moved none of it. Bid
latency became uniform once inference left the hosts, which confirms the S09b mechanism from the
opposite direction: **placement follows bid speed, not agent id.**

**S01 — which statistic to read.** Per-call `ReasoningTime`, cloud arm:

| statistic | baseline (n=1074) | S01 +3 s (n=1138) | delta |
|---|---|---|---|
| **min** | **1.03 s** | **3.94 s** | **+2.91** |
| p10 | 3.74 s | 4.26 s | +0.52 |
| p50 | 6.45 s | 7.12 s | +0.67 |
| mean | 7.48 s | 7.34 s | **−0.14** |
| p90 | 9.32 s | 11.42 s | +2.10 |

The mean *falls*, which reads as "the fault never applied". The minimum shows it applied to every
call. The endpoint's queueing tail (baseline p95 20.75 s) swamps a 3 s shift, whereas the same
injection moved the local arm's mean by +2.75 s. **On a shared endpoint, verify a latency
injection with order statistics, not the mean.**

**Does each finding replicate across arms?**

| scenario | metric | local 3B | cloud 120B | replicates? |
|---|---|---|---|---|
| S01 +3 s | completion | 300/300 | 300/300 | yes |
| | fallback rate | 0.0% | 0.1% | yes |
| | latency shift | mean +2.75 s | min +2.91 s | yes (different statistic) |
| | fairness | 0.681 → 0.738 | 0.809 → 0.823 | yes |
| S05 503 | fallback rate | 100% | 100% | yes |
| | completion | 300/300 | 300/300 | yes |
| | fairness | 0.681 → **0.843** | 0.809 → **0.878** | yes |
| | sched latency | 368.9 → 61.3 s | 235.8 → 66.7 s | yes |
| S09 swap | score signal | modal 75 → **25** | mean 89.5 → **11.6** | yes, sharper |
| | completion | 300/300 | 300/300 | yes |
| | fairness | −0.041 | −0.045 | yes |

Every finding survives the arm switch. S09 is *sharper* on the capable model: `gpt-oss:120b`
follows the flipped instruction precisely rather than approximately, landing on very nearly the
exact complement of its baseline score — and the schedule absorbs it either way. **A model that
reasons better does not make the swarm more fragile to that reasoning being corrupted.**

> **Caveat — S09's 16.8% fallback rate is the endpoint, not the fault.**
>
> | | value |
> |---|---|
> | fallbacks | 180 of 1072 calls (16.8%) |
> | cause | `429: too many concurrent requests` — **all 180** |
> | distribution | all 30 agents, 1-13 each (not a subset) |
> | range across the 4 cloud runs | 0.2% (baseline) … 16.8% (S09) |
>
> Semantic corruption produces no exception by construction, so none of these come from the
> fault. The rate limit is an **uncontrolled background fault that varies run to run** — the
> confound §2.1b predicted, absent from three of four runs and material in the fourth. It is
> spread evenly rather than concentrated, so it should not have triggered the S05 capture
> pathology, and completion and score are unaffected; S09-cloud's *fairness* figure carries the
> asterisk. **Check the fallback reason on a cloud campaign, not just the rate.**


### FABRIC gateway arm — S01, S05 and S09 once each (2026-08-20)

Run to answer one question: does the FABRIC LiteLLM gateway avoid the rate limiting that
contaminated the Ollama Cloud arm? It does — **0 fallbacks in all three fault-free-path runs**,
where the cloud arm saw 0.2% to 16.8%.

Driven through `provider: ollama` pointing at the gateway, not `provider: openai`: `LlmBidder`
only honours `llm.base_url` on the ollama path, while the openai path reads `OPENAI_BASE_URL` —
the same env channel CJ injects through, so the two would collide and faults would silently
no-op. Model `gpt-oss-20b`, local Ollama stopped fleet-wide, same frozen fleet and trace.

**Concurrency — the reason to prefer this endpoint:**

| simultaneous requests | FABRIC gateway (`gpt-oss-20b`) | Ollama Cloud (`gpt-oss:120b`) |
|---|---|---|
| 1 (sequential) | 2.02 s | 1.63 s |
| 8 | 4.47 s · all 200 | 4.03 s · all 200 |
| 16 | 4.62 s · all 200 | 6.24 s · all 200 |
| **30** (fleet size) | **6.11 s · all 200** | 4.10 s · **13x 429** |

> *Measurement trap:* the first sweep reported 0.05 s sequential and 0.27 s at 30-way, which is
> impossible for a 20B model. The probe sent an identical payload each time and the gateway
> **caches responses**. Vary the payload per request *and* per run — repeating ids across runs
> hits the previous run's cache, which is what produced a 0.06 s "sequential" baseline mid-sweep.

**Results** — every column against `cj-baseline-gw2`:

| | baseline | S01 (+3 s, 100%) | S05 (503, 100%) | S09 (entity_swap, 100%) |
|---|---|---|---|---|
| jobs completed / stuck | 300 / 0 | 300 / 0 | 300 / 0 | 300 / 0 |
| **fallback rate** | **0.0%** | **0.0%** | 100% | **0.0%** |
| bid latency mean / p95 | 4.95 s / 7.47 s | 7.09 s / 8.78 s | — | 5.22 s / 7.81 s |
| LLM score mean / sd | 91.0 / 10.7 | 91.5 / 10.4 | — | **13.6** / 17.9 |
| sched latency mean | 191.1 s | 261.6 s | **63.1 s** | 190.8 s |
| load fairness | 0.849 | 0.842 | **0.795** | 0.809 |
| SWIM false-fails | 7 | 0 | 2 | 1 |
| placement deciles | 125/88/87 | 133/81/86 | 123/98/79 | 121/78/101 |
| idle agents | none | none | none | none |

**Three-arm comparison, fault-free:**

| metric | local 3B | Ollama Cloud 120B | **FABRIC gw 20B** |
|---|---|---|---|
| fallback rate | 0.0% | 0.2% | **0.0%** |
| bid latency mean / **p95** | 9.90 / 12.52 s | 7.48 / **20.75 s** | **4.95 / 7.47 s** |
| sched latency mean | 368.9 s | 235.8 s | **191.1 s** |
| load fairness | 0.681 | 0.809 | **0.849** |
| placement deciles | 153/82/65 | 112/100/88 | 125/88/87 |

**S01 — the mean moves here.** +2.14 s against an injected +3.0 s, visible without order
statistics, because the gateway's tail is tight (p95 7.47 s versus the cloud arm's 20.75 s). That
confirms the cloud-arm reading was a measurement artefact and not a difference in the system:
same fault, same absorption, and the statistic that failed there works here.

**S09 — the cleanest silent-wrong measurement of the three arms.** No 429s means no confound:
the score mean inverts **91.0 → 13.6** while the queue drain is unchanged to within 0.3 s
(191.1 → 190.8 s) and fairness moves 0.04. Corrupted bids, unmoved schedule, nothing else
touched.

> **S05's fairness improvement does *not* replicate — and that is a correction to the S05 story.**
>
> | arm | fault-free fairness | under 100% outage | change |
> |---|---|---|---|
> | local 3B | 0.681 | **0.843** | **+0.162** |
> | Ollama Cloud 120B | 0.809 | **0.878** | +0.069 |
> | **FABRIC gw 20B** | **0.849** | **0.795** | **−0.054** |
>
> Losing the LLM entirely looked like it *improved* load balance. It does not: the improvement
> tracks how bad the baseline was. The local arm's 0.681 was depressed by heterogeneous host
> inference (§S09 root cause), so replacing every bid with an instant analytic one evened things
> out. Start from a baseline that is already fair — the gateway's 0.849 — and the same total
> outage makes fairness slightly *worse*. **The correct statement is that a total outage costs
> little, not that it helps.** Completion, stuck jobs and agent losses are untouched on all
> three arms, which is the claim that does replicate.

**Gateway model screening** (40 identical pairs, production prompt, json_schema):

| model | latency mean | distinct scores /40 | top-2 share | usable |
|---|---|---|---|---|
| **`gpt-oss-20b`** | **1.77 s** | 14 | 48% | **yes — used here** |
| `nemotron-nano-30b` | 15.70 s | **18** | **25%** | best signal, 9x too slow |
| `minimax-m2.7` | >22 s/call | — | — | no (900 s screen timeout) |
| `qwen3.5-122b` | >180 s | — | — | no (request times out) |

`nemotron-nano-30b` has the best cost signal measured on any endpoint — better than
`gpt-oss:120b` — but 15.7 s per bid puts it in the same range as the starved hosts that lost
their entire share of the workload, and bid latency decides placement. Speed wins.


### S01 complete — the L1 dose-response curve (FABRIC gateway, 2026-08-20)

The matrix's L1 row, swept on the arm that does not rate-limit, 100% of hosts faulted at every
point, each against `cj-baseline-gw2`.

| injected delay | bid latency mean | measured Δ | Δ / injected | sched latency mean | load fairness | SWIM false-fails | failed agents | **jobs completed** | **fallbacks** |
|---|---|---|---|---|---|---|---|---|---|
| 0 (baseline) | 4.95 s | — | — | 191.1 s | 0.849 | 7 | 0 | **300** | **0** |
| **+1 s** | 5.73 s | +0.78 | 78% | 219.7 s | 0.804 | 0 | 0 | **300** | **0** |
| **+3 s** | 7.09 s | +2.14 | 71% | 261.6 s | 0.842 | 0 | 0 | **300** | **0** |
| **+6 s** | 9.50 s | +4.55 | 76% | 364.1 s | 0.813 | 7 | 0 | **300** | **0** |
| **+10 s** | 13.05 s | +8.10 | 81% | 497.0 s | 0.859 | 2 | 0 | **300** | **0** |
| **+30 s** | 32.52 s | +27.57 | 92% | 1157.6 s | 0.827 | 6 | 0 | **300** | **0** |
| **+60 s** | 62.32 s | +57.37 | 96% | 2237.3 s | 0.808 | 5 | 0 | **300** | **0** |

Extended past the matrix's 1/3/6/10 s to 30 s and 60 s to find where scheduling — and then
membership — gives way. **Neither does.**

**Throughput degrades linearly; correctness does not degrade at all.** Across a 60x sweep:
300/300 jobs at every point, zero fallbacks, zero stuck jobs, zero agents lost. Fairness has no
trend (0.804-0.859, straddling the baseline's 0.849). The only thing that moves is speed.

**Each second of LLM delay costs ~34 s of scheduling latency.** Mean scheduling latency runs
191 → 2237 s across the sweep, a slope of ~34 s per injected second — a **~34x amplification**,
because a job's placement waits on several sequential bid rounds rather than one. That is the
number to quote for "what does a slow LLM cost the scheduler": not the per-call delay, but ~34x
it in queue time.

**The injected delay partially pays for itself, and the shortfall decomposes the baseline.** The
measured rise is 71-81% of the injected delay at small doses but 92% and 96% at 30 s and 60 s.
The *absolute* shortfall saturates rather than the ratio:

| injected | 1 s | 3 s | 6 s | 10 s | 30 s | 60 s |
|---|---|---|---|---|---|---|
| shortfall | 0.22 s | 0.86 s | 1.45 s | 1.90 s | 2.43 s | 2.63 s |

Slowing every agent removes concurrent pressure on the shared endpoint, and the relief cannot
exceed the contention that was there to begin with. It converges on **~2.6 s**, which splits the
4.95 s baseline bid into **~2.3 s of service and ~2.6 s of contention** — confirmed directly by
the +60 s point, where an uncontended call costs 62.32 − 60 = **2.32 s**. Read a sub-unit delta as
this effect, not as the fault under-applying.

> **The S01 membership hypothesis is refuted, not merely unconfirmed.** SWIM false-fails are
> 0, 0, 7, 2, 6, 5 against a baseline of 7 — no trend — and **zero agents were declared failed at
> any delay, including +60 s**, a 12x slowdown on a 5 s bid. LLM-plane latency does not leak into
> membership.
>
> That forces a correction to the §S09 root-cause section, which read the local arm's collapse
> (agents bidding at 77-250 s, most-suspected by SWIM, declared failed) as "an agent blocked
> inside a bid answers its SWIM probes late". This sweep is the control for exactly that claim,
> and it fails: a bid blocked for 60 s on a *healthy* host costs no membership at all. The
> collapse was caused by the **memory starvation** that also produced the slow bids — a thrashing
> host starves the SWIM responder thread itself — not by the bid duration. Same symptom, and the
> two mechanisms are separable only because this arm could hold one constant while varying the
> other.
>
> Practical consequence: **a slow LLM is not a membership risk; a starved host is.** The health
> gate's memory check (`MIN_AVAILABLE_MB`) is the mitigation that matters, and `suspect_timeout_s`
> tuning is not.

---

## 5. Experiment matrix

Each row is one CJ `Scenario`, run baseline-vs-fault with n≥5 repeats on a fixed job trace, across
**two topology arms**: flat mesh (30 agents) and hierarchical (LLM agents as Level-1 coordinators,
enabling the *targeted-fault-on-coordinators* story).

### Tier 1 — LLM API faults (CJ Layer 1) — core of the paper
| # | Fault | Sweep | Hypothesis |
|---|-------|-------|------------|
| L1 | `LLMLatency` | 1/3/6/10/30/60 s | **DONE** (gateway arm) — throughput degrades linearly at ~34x the injected delay; completion, fallbacks and membership untouched to +60 s |
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

> **Check memory, not just inference.** A host whose `llama-server` has grown to ~7.4 GB of its
> 7.9 GB answers a single probe in 0.4 s and passes any "can it infer" test, then places zero
> jobs for a whole run once an agent is competing with it for RAM (§S09). `helpers.health_gate()`
> now fails the run below 300 MB available. Release it with `systemctl restart ollama` — and note
> that on some hosts `ollama serve` runs outside systemd, where that command silently does
> nothing.
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

**Verifying a `semantic` fault takes a different probe.** Every Tier 1 fault announces itself in
the HTTP response (503, +3 s, malformed body), so one curl proves it landed. A semantic fault does
not: HTTP and JSON stay valid and the reply still comes from the real model. Two checks replace
the curl, and `helpers.start_fault()` runs both:

1. **Which mutation** — read the listener's own command line and require
   `--fault semantic_corrupt --semantic-mode <mode>`. A leftover proxy from an earlier mode
   answers on the same port and is otherwise indistinguishable.
2. **That it reaches the model** — `cj_probe.py` sends one payload straight to Ollama and the same
   payload through the proxy, and compares `usage.prompt_tokens`. That number is what the model
   actually received, so a delta proves the rewrite happened, with no dependence on a 3B model
   choosing to obey a probe instruction. Observed: `entity_swap` **+17**, `rag_poison` **+22**,
   `inject_distractor` **+10**, `context_truncate` **−11**.

The expansion has to be engineered into the probe payload: a swap can be token-neutral — on the
real scheduling prompt `entity_swap` changes exactly one word, `higher` → `lower`, for a delta of
**0**. To see what a mode does to *our* prompt, apply CJ's own mutation functions offline rather
than inferring it from token counts.

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
   `entity_swap` is done at 25/50/100% (§S09) and found **no tolerance threshold to locate** —
   placement never moved, because it is decided by *when* an agent bids, not by *what* it bids
   (§S09b). The remaining three modes are wired and verified but unrun, and none of them
   changes timing either, so expect the same answer from all three.
   S01 has since been swept to 30 s and 60 s (**done** — see its section): there is no threshold
   where scheduling or membership gives way, only linear slowdown at ~34x. The membership
   collapse was memory starvation, not latency.
3. **Phase 3 — ablations**, especially fallback-disabled.
4. **Phase 4 — composite X1** and hierarchical/targeted-coordinator scenarios.
5. **Report the CJ issues in §6 upstream.**

**Open question for the team:** which figures are must-haves for the paper?

- **A** — Degradation curves: completion rate & P95 latency vs fault severity
- **B** — Fallback rate by fault type (the graceful-degradation headline)
- **C** — Poisoned-agent tolerance threshold (fairness & conflicts)
- **D** — Default vs fallback-disabled (value of the analytic safety net)
- **E** — Composite "bad day", annotated with the zero-double-assignment invariant
