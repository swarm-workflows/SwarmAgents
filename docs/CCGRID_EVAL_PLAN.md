# CCGrid Submission — Evaluation Plan (LLM Agents + Snow/Gossip + Contextual Bandit)

**Status:** draft plan, 2026-08-17. Successor to `SNOW_GOSSIP_PAPER_PLAN.md` (which covered Snow only).
**Predecessor paper:** SWARM+ (eScience'26, accepted) — PBFT hierarchical consensus, data-aware
placement, Pegasus workloads, 3–4 FABRIC sites. That paper is the baseline; nothing in it can be a
contribution here.
**Testbed:** 2-slice FABRIC deployment, ~90 VMs across 17 sites (16c/16G/500G), one `database` node
(Redis) and one Prometheus/Grafana monitor VM. Driven from `notebooks/SWARM-2slice.ipynb` +
`db_node_setup/`.
**Deadline (CCGrid'27, Dallas-Fort Worth):** abstract **24 Nov 2026 AoE** (hard registration gate),
full paper **1 Dec 2026 AoE**. **10 pages including references, figures and tables**, IEEE conference
template. From 2026-08-17 that is ~15 weeks; the schedule in §9 is dated.

---

## 1. The thesis (this determines every experiment)

Three features do not make a paper. The unifying claim:

> Decentralized workload management is moving from *hand-tuned cost functions* to a **learned and
> semantic decision plane** — contextual bandits that learn where to delegate, and LLM coordinators
> that reason over job/site semantics. Both make decisions **slower and more expensive per decision**.
> Classical BFT consensus cannot absorb that cost: its coordination overhead compounds with decision
> latency and collapses. We show that **gossip-based (Snow) consensus is what makes a learned/LLM
> decision plane viable at WAN scale**, and we quantify the resulting cost/quality/scale envelope
> across 17 geographically distributed sites.

Three claims, each falsifiable, each needing its own figure:

| # | Claim | Headline evidence |
|---|---|---|
| **C1** | A learned delegation policy (LinUCB) beats static/heuristic and context-blind delegation on schedule quality, and *keeps* beating it under non-stationarity and churn. | Success rate / routing accuracy / makespan vs. epsilon-greedy, UCB1, static-greedy, and a per-type oracle. |
| **C2** | LLM coordinators improve decision quality on semantically rich placement (data locality, DTN, heterogeneity) but their inference latency is what breaks BFT coordination — **not** their reasoning. | The Hier-250 collapse (3.5–4.2% completion) is reproduced, then *removed* by swapping the coordinator tier to Snow. Decision-quality delta measured against the analytic cost model. |
| **C3** | Snow/hybrid consensus decouples coordination cost from decision cost: it keeps completion at scales and decision latencies where PBFT livelocks, at a bounded latency price. | Mesh/hierarchical ladder to 270 agents × {PBFT, Snow, Hybrid} × {analytic, bandit, LLM} decision planes, plus the real-WAN 17-site sweep. |

**The composite result is the paper.** C1, C2, C3 each alone is incremental (and C3 partly overlaps
the eScience future-work section). The novel, non-obvious result is the **interaction**: expensive
decision-making changes which consensus protocol you should run. No prior figure in your corpus shows
that axis.

### Delta vs. eScience'26 (defend this explicitly in §I)

| eScience'26 has | This paper adds |
|---|---|
| PBFT flat + hierarchical, analytic cost model | Snow + hybrid engines; learned and LLM decision planes |
| 3–4 sites, netem-emulated WAN | **17 real sites**, natural WAN heterogeneity; netem only as a controlled overlay |
| Fixed heuristic delegation | Contextual bandit delegation with non-stationarity + churn |
| LLM table **cut** (`\begin{comment}` in `expandeval.tex`) | LLM coordinators as a first-class subject, with cost/quality/latency accounting and a fix for the 250-agent collapse |

---

## 2. Prior data: reference only — **every number in the paper is re-measured**

**Decision (2026-08-17): no existing run data goes into this paper.** All results are produced
fresh on the 17-site slice, at one code revision, with one metric-extraction pipeline
(`evaluation/collect.py`). Rationale:

- The banked results span 3–4 sites, several code revisions, and pre-date the Snow driver/Snowball
  fixes. Mixing substrates and revisions inside one results section is precisely what a reviewer
  attacks, and the eScience review already probed the geo-distribution claim.
- **Concrete evidence this matters:** validating the extractor against the archived LLM runs showed
  the published "Sel. (s)" column is the `scheduling_latency` field, whereas the Snow write-ups
  report `assigned_at - selection_started_at` as "selection time." Two different quantities, one
  name, across documents that would have been combined into one table. The new campaign fixes a
  single definition (both are emitted, distinctly named, by `collect.py`).

Prior data still has three legitimate uses:
1. **Expectation-setting** — the numbers below say what each cell should roughly produce, so an
   anomalous fresh run is recognizable as a bug rather than a finding.
2. **Extractor test fixtures** — archived trees exercise `collect.py` without burning testbed time.
   (Validated 2026-08-17: job counts and reasoning times reproduce the old table exactly; GLM
   Hier-250 → 3.4–4.5% completion vs 4.1% published; reselection multiplier ~2.3 vs 2.0–2.3× published.)
3. **Sizing the campaign** — known runtimes drive the §5 budget.

| Prior result | Location | Referenced for |
|---|---|---|
| LLM coordinators: Qwen3 / GPT-OSS / GLM-4.7 at Hier-30/110/250, 5 runs each | `swarmplus-evaluation-data/runs/pegasus-llm/`; table text in `SWARM_Escience26_Consensus/expandeval.tex` (commented) | Expect the PBFT×LLM Hier-270 cell of E1 to collapse to single-digit completion. If it doesn't, suspect the harness before believing it. |
| Snow vs PBFT flat Mesh-120, 4 sites (PBFT 0% vs Snow 100%) | `.../snow-gossip/demo-mesh120-{snow,pbft}` | Expect flat PBFT to livelock at Mesh-180 on 17 sites (WAN is worse, so the wall should arrive no later). |
| Hybrid engine validated (level-0 PBFT / level-1 Snow) + driver & Snowball fixes (Mesh-120 86.8s → 22.1s) | `CONSENSUS_SCALING_PLAN.md` §post-fix; `.../snow-gossip/demo-*-fixed` | Mechanism is in place and testbed-validated — no re-implementation needed, only re-measurement. |
| LinUCB Scenario A/B/C (context-dependent failures, mid-run flip, outage+rejoin) | `docs/CONTEXTUAL_BANDIT_DESIGN.md` §8.1–8.3; `evaluation/scenario_{a,b,c}/` | Scenario **configs and drivers are reusable code**; their results are not. E2 re-runs all three at 90 agents with repeats + oracle. |
| Pegasus 547-job base set + duplication tooling | `.../pegasus-workloads/jobs/`, `duplicate_jobs.py` | **Workload inputs are reused** (re-using stimulus is not re-using results, and it keeps continuity with the eScience workload). |
| Centralized baselines (greedy / round-robin / random) + remote workers | `baselines/`, `docs/DISTRIBUTED_BASELINE_DESIGN.md` | Code reused, re-run fresh. External-baseline defense (§6). |
| Prior CCGrid'26 reviews of SWARM+ | `SWARM_Escience26_Consensus/review.txt`; `../ccgrid-paper-discussion-points.py` | **Reviewer pre-mortem — same venue, likely overlapping PC.** See §6. |

**Consequence for the budget:** E1's factorial already re-runs every consensus × decision-plane cell,
so the incremental cost of the no-reuse decision is small — mainly re-running the bandit scenarios
that previously existed only at 30 agents, plus a SWARM+ (PBFT + analytic) re-baseline so the
"improves on our own prior system" claim is measured on this substrate. Budgeted as **E0** in §5.

---

## 3. Gaps that block the thesis

**Measurement gaps**
1. No LLM × Snow/hybrid cell anywhere — the C2 fix is unmeasured.
2. No analytic-cost control row for the LLM runs → cannot claim LLM *quality*, only LLM *cost*.
3. Bandit evaluated only at 30 agents, ≤2 runs per config in places, no oracle upper bound, no
   static-heuristic baseline, and never combined with LLM coordinators.
4. Snow has no message-count / bandwidth instrumentation (E5 of the old plan) → the O(n²)→O(fanout)
   claim is asserted, not measured. The monitor VM makes this cheap now.
5. No exactly-once safety stress at scale; reviewers already flagged the safety argument as informal.
6. Decision-quality metrics are thin everywhere: completion + selection time only. Need makespan,
   Jain's fairness, data-locality hit rate, and delegation regret.

**Mechanism gaps (code work, §4)**
7. There is **no LLM-based delegation** today — `LlmAgent` only overrides *its own* bid cost
   (`_llm_or_analytic_cost`). "LLM coordinator" currently means "coordinator with an LLM-scored bid."
   To claim a semantic decision plane, the LLM must choose *among child groups*.
8. No coupling between the bandit and the LLM. The obvious and defensible mechanism —
   **bandit proposes, LLM disposes** (or the reverse) — is the paper's systems contribution.
9. No decision caching / inference-aware delegation (named as future work in the cut eScience text).

---

## 4. Code work (P0 = paper-blocking)

| ID | Work | Where | Est. |
|---|---|---|---|
| **P0-1** | **LLM group delegation.** At a coordinator, prompt over child-group summaries (capacity, inflight, DTN/site, recent failure rates — the LinUCB context features are already computed in `swarm/rl/context.py`) and return a ranked group choice + rationale. Persist to `llm_score:*` for audit. | new `swarm/agents/llm/llm_delegator.py`; hook at `resource_agent.py:2378` alongside `mab_manager.select_groups` | 3–4 d |
| **P0-2** | **Bandit×LLM composition.** Three selectable modes: `llm_only`, `bandit_only`, `bandit_gated_llm` (bandit narrows `capable_groups` → top-m, LLM ranks those m; LLM's pick is the arm the bandit is rewarded on). Config `delegation.policy`. | `mab_manager.py`, `resource_agent.scheduling_main` | 3 d |
| **P0-3** | **Decision cache + inference budget.** Cache LLM verdicts keyed by (job-type signature, coarse group-state bucket) with TTL; hard cap on in-flight inference per coordinator, fall back to bandit/analytic when exceeded. Emit hit rate + fallback counts. | `llm_delegator.py`, `llm_bidder.py` | 2–3 d |
| **P0-4** | **Instrumentation:** per-agent consensus message counts & bytes, rounds-to-finalize, LLM calls/tokens/latency/fallbacks, delegation decisions with chosen-vs-oracle labels. Export via `metrics.json` **and** node_exporter textfile collector so Grafana captures it. | `swarm/utils/metrics.py`, `gossip_engine.py`, `engine.py` | 2 d |
| **P1-1** | **Oracle / offline-optimal delegator** for regret: replays each job against known per-group failure profiles and capacity. Analysis-time only. | `evaluation/oracle.py` | 2 d |
| **P1-2** | Analysis harness: one script that walks a run tree → tidy CSV (config × run × metric), so every figure is regenerable. | `evaluation/collect.py` | 2 d |
| **P1-3** | Site-aware config generation for 17 sites (`--agent-sites-file`) so Snow's `local_sample_frac` is meaningful and figures can be grouped by site. | `generate_configs.py` (§Part C of `CONSENSUS_SCALING_PLAN.md` — partially done) | 1 d |

**LLM serving decision — GPU slice available (confirmed 2026-08-17).** Coordinators are only ~10% of
agents (Hier-270 → ~27 coordinators), so inference load is bounded and does **not** scale with agent
count. Plan:
- **On-slice GPU (primary):** one GPU VM running **vLLM** (continuous batching) at a well-connected
  site, serving all coordinators over FABNetv4 with an OpenAI-compatible endpoint — so
  `llm.provider: openai` + `LLM_BASE_URL` works with zero agent-side code change. A 7–8B model on an
  RTX6000/A30-class card gives ~50–150 ms/decision batched, which is *below* the consensus round time
  and removes inference as the throughput ceiling.
- **Add a GPU node to the inventory:** request it in the slice-creation cell of `SWARM-2slice.ipynb`
  (`add_component(model="GPU_RTX6000")` on a site that has one free — check availability early, GPU
  components are the scarcest resource on FABRIC and can block the whole campaign).
- **What the GPU actually buys the paper** — it turns inference cost from a confound into a
  *controlled variable*. E4 can now sweep model capability at roughly constant latency
  (e.g. 3B / 8B / 32B-quantized) and answer "how much model do you need for good placement?", which is
  a far more interesting question than "is the LLM slow?". Keep one deliberately slow configuration
  (CPU-served or artificially delayed) as the high-inference-cost point in the E1 factorial — that is
  the cell that breaks PBFT and it must stay in the design.
- **Hosted validation subset (RENCI gateway):** one configuration (Hier-90) against a hosted GPT-class
  model, showing findings hold off-slice and giving a realistic cloud-inference latency point.
- Record inference latency distribution and vLLM batch occupancy per run; both are independent
  variables in E4, not noise.

---

## 5. Experiment matrix

Common workload: Pegasus-derived jobs, ~20 jobs/agent, generated by duplicating the 547-job base
(`duplicate_jobs.py`). Agent counts must divide evenly by agents-per-host (`run_test.py` floors
`hosts = agents // per_host`).

**Scale ladder on ~90 VMs:**

| Agents | Agents/VM | Jobs | Notes |
|---|---|---|---|
| 30 | 1 (30 VMs) | 600 | Matches eScience Hier-30; cheap, use for sweeps |
| 90 | 1 | 1800 | Primary operating point — 1 agent/VM, 17 sites, cleanest |
| 180 | 2 | 3600 | Flat-PBFT livelock regime |
| 270 | 3 | 5400 | Reproduces the LLM Hier-250 collapse point |

### E0 — Substrate re-baseline (runs first; everything else is compared against it)
SWARM+ as published (PBFT + analytic cost model) at Hier-30 / Hier-90 / Hier-270 and Mesh-180 on the
17-site slice, current code revision. Establishes the "same system, new substrate" reference point so
every later claim is a within-campaign comparison, and quantifies the substrate delta against the
eScience numbers in the text (not in a results table).
- **Gate:** if E0 at Hier-30/90 does not land near the eScience envelope (~1 s selection, ~100%
  completion), stop and debug the deployment before spending the campaign. This is the harness's
  smoke test at full scale.
- Repeats: 5. Note these cells double as E1's `PBFT × analytic` column — do not run them twice.

### E1 — Consensus × decision-plane factorial (**the paper's core figure**)
`{PBFT, Snow, Hybrid} × {analytic, bandit(LinUCB), LLM, bandit-gated-LLM}` at Hier-90 and Hier-270,
plus flat Mesh-180 for the livelock contrast. Full crossing is 12 cells/scale — trim to the 8
informative cells (drop PBFT×LLM at 270 to a single confirming run, since the collapse is the point).
- **Metrics:** completion %, per-job selection time (p50/p95), makespan, throughput, Jain's fairness,
  consensus msgs/job, decision latency broken into (consensus | inference | queueing).
- **Payoff:** shows the interaction — PBFT is fine with a cheap decision plane and collapses with an
  expensive one; Snow/hybrid is insensitive to decision cost.
- Repeats: 5 (10 for the two headline cells).

### E2 — Learned delegation quality (C1)
Hier-90, `{static-greedy, epsilon-greedy, UCB1, LinUCB, LinTS, oracle}` under the context-dependent
failure profile (Scenario A generalized to 9 groups). Then the two stressors, now at scale:
- **E2a non-stationarity:** mid-run failure-parity flip, `discount ∈ {1.0, 0.98}` (scale up Scenario B).
- **E2b churn:** group outage + rejoin, with the liveness-gating and decayed-timeout fixes on/off
  (validates the fixes from `CONTEXTUAL_BANDIT_DESIGN.md` §8.3 as an ablation).
- **Metrics:** cumulative reward, **regret vs. oracle**, routing accuracy over time, success rate,
  time-to-re-adoption after rejoin, per-type delegation heatmap, learned-θ inspection.
- Repeats: 5 per policy (this is where reviewers will demand error bars — Scenario B ran 1 run/arm).

### E3 — Real-WAN sensitivity (**strongest differentiator; replaces the netem story**)
17 real sites means the WAN sweep is measured, not emulated. Two parts:
- **E3a observed:** bin sites by measured RTT to the coordinator tier (LAN <5 ms / regional 10–40 ms /
  continental 40–80 ms / transatlantic AMST >90 ms; HAWI as the long-tail case) and report selection
  latency and completion **per RTT bin** for PBFT vs Snow vs Hybrid. This is a figure no prior SWARM
  paper could produce.
- **E3b controlled:** netem overlay on top (+25/+50/+100 ms, 1%/2% loss) at Hier-90 to reproduce the
  eScience impairment table with the new engines. Directly rebuts the eScience reviewer who observed
  that completion collapsed at 25–50 ms delay under PBFT — show Snow/hybrid holds.
- Repeats: 5 per impairment level; 3 for the observed binning (it's per-run analysis, so E1 runs
  can be re-analyzed for free — do E3a as analysis over E1 data first).

### E4 — Cost of reasoning (C2, the honest-accounting section)
Hier-90, Hybrid engine. Two orthogonal sweeps, now separable because the GPU decouples them:
- **Model capability at ~constant latency (GPU/vLLM):** `{analytic (no LLM), 3B, 8B, 32B-quantized,
  hosted GPT-class}` → *how much model does good placement need?*
- **Inference cost at constant capability:** same 8B model served fast (GPU) vs slow (CPU or injected
  delay) × `{cache on/off, bandit-gated on/off}` → *what does decision latency cost the system?*
- **Metrics:** LLM calls per job, cache hit rate, tokens & $ (or GPU-seconds), inference p50/p95,
  fallback rate (`[LLM_COST_FALLBACK]`), and the **quality delta** vs analytic: makespan, DTN-locality
  hit rate, fairness, completion.
- **Payoff:** the "is the LLM worth it?" table. A negative or mixed result here is publishable and
  makes the paper credible — do not bury it. Expected shape: LLM wins on data-locality-sensitive
  placement, loses on throughput, and bandit-gating + caching recovers most of the loss.

### E5 — Coordination overhead (substantiates the mechanism)
Instrumented message counts/bytes and Redis op rate across the ladder, `gossip.enabled` on/off,
PBFT vs Snow. Expect O(n²) vs O(k·fanout) curves. Grafana/node_exporter gives CPU and network per node
for free — include a per-node network-bytes panel as a figure.
- Repeats: 3 (low variance).

### E6 — Safety and correctness (cheap, defuses a known reviewer attack)
- Zero double-assignment audit via the Redis `SET NX` claim keys (`try_claim_assignment`) across
  **every** run in the campaign — report as an aggregate ("0 double-assignments in N jobs across M runs").
- A dedicated partition test: split the 17 sites into two groups with iptables/netem blackhole, verify
  no two sub-groups both finalize the same job, and report behavior on heal. The eScience reviewer
  explicitly questioned quorum under partition with Redis-inferred `n_live` — answer it with data plus
  a short argument (safety comes from the Redis CAS claim, not from quorum inference; state that).
- Repeats: 3.

### E7 — External baselines (see §6)
Centralized greedy / round-robin / random with remote execution workers, plus a **sampling-based
(Sparrow-style) decentralized baseline** at Hier-90 and 270. Metrics: makespan, selection latency,
fairness, and behavior under the same failure injection.
- Repeats: 5.

**Approximate run budget** (assume ~20 min per run incl. setup/teardown, `batch_tests_v2.py --runs N`):

| Exp | Cells | Repeats | Runs | Testbed hours |
|---|---|---|---|---|
| E0 | 4 (2 shared with E1) | 5 | ~10 net | ~4 |
| E1 | 8 × 2 scales + 3 mesh | 5 (10 headline) | ~105 | ~35 |
| E2 | 6 policies + 2 stressors × 3 | 5 | ~60 | ~20 |
| E3b | 5 impairments × 3 engines | 5 | 75 | ~25 |
| E4 | 8 | 5 | 40 | ~14 |
| E5 | 8 | 3 | 24 | ~8 |
| E6 | 4 | 3 | 12 | ~4 |
| E7 | 4 × 2 scales | 5 | 40 | ~14 |
| | | | **~366** | **~124 h** |

~120 testbed-hours ≈ 3 weeks of mostly-unattended running with a driver script, assuming the slice
stays healthy. Budget 2× for reruns. **This is the reason to freeze the code by week 6.**

---

## 6. Reviewer pre-mortem (same venue, likely overlapping PC)

From `review.txt` and the CCGrid'26 revision notes, four objections will recur. Design them out now:

| Prior objection | This paper's answer |
|---|---|
| "No comparison with state of the art" (R2/R3/R4, and repeated by the eScience reviewer) | E7 with a real distributed baseline, not just your own priors. Implement one sampling-based scheduler (Sparrow-style late binding, ~200 LOC on top of `baselines/`) and discuss Firmament / Omega / Sparrow / Ray-style placement in related work with a positioning table. **Non-negotiable.** |
| "Only high-performance network / geo-distribution not really tested" | E3a — 17 sites, per-RTT-bin results, including transatlantic (AMST) and Hawaii. |
| "Synthetic workloads too easy; failure paths never exercised" | Pegasus-derived workloads throughout; E2b and E6 drive real churn and partition; report reselection counts explicitly. |
| "Safety argument is informal; Redis undermines the decentralization claim" | E6 partition test + explicit safety statement (exactly-once rests on a CAS claim, not on quorum inference). Also state the Redis role honestly and cite `DECENTRALIZED_POOL_DESIGN.md` as the direction of travel. |
| "Three features stapled together" (the *new* risk) | §1 thesis and E1's factorial. Every section must point back to the interaction claim. |

---

## 7. Metrics and statistics protocol

**Primary:** completion %, per-job selection time (p50/p95/p99), makespan, throughput (j/s).
**Decision quality:** Jain's fairness over per-agent load, DTN/data-locality hit rate, delegation
routing accuracy, regret vs. oracle.
**Cost:** consensus messages & bytes per job, LLM calls/tokens/$ per job, cache hit rate, CPU-seconds.
**Robustness:** reselection multiplier, detection latency, time-to-re-adoption, double-assignment count.

Match the eScience protocol so comparisons are legitimate: **≥5 repeats per configuration** (10 for
headline cells), two-tailed t-test at α=0.05 with Cohen's d, and report mean ± std everywhere.
Every figure regenerated from `evaluation/collect.py` output — no hand-copied numbers.

**Metric-definition discipline (learned the hard way, 2026-08-17).** Past write-ups used the name
"selection time" for two different quantities:
- `selection_*` = `assigned_at - selection_started_at` — consensus + selection only. This is what the
  Snow/hybrid results report.
- `sched_latency_*` = the agent-reported `scheduling_latency` column — includes queueing ahead of
  selection. This is what the LLM coordinator table reported.

`collect.py` emits both under distinct names. **Pick `selection_*` as the paper's "selection time"**
(it isolates the coordination cost the paper is about) and report `sched_latency_*` separately as
end-to-end scheduling latency. State both definitions explicitly in the evaluation setup — a reviewer
comparing against the eScience paper will otherwise see an unexplained discrepancy. Similarly,
completion% is always against **jobs submitted**, never jobs observed; `collect.py` refuses to guess.

---

## 8. Figure plan

**10 pages *including references*** is tight — references alone will eat ~1 page, so budget ~8.5 pages
of content and no more than **5 figures + 3 tables**. Rank ruthlessly; anything below the line goes to
a companion tech report / arXiv version and gets cited.

1. **F1** Architecture: three decision planes over pluggable consensus (single column).
2. **F2** *The interaction figure* — completion & selection latency vs. scale, grouped bars for
   {PBFT, Snow, Hybrid} × {cheap, expensive} decision plane. Shows PBFT collapsing only under
   expensive decisions. **This is the paper; give it the space.**
3. **F3** Learning curves: routing accuracy / regret over time for LinUCB vs baselines, mid-run flip
   marked (E2/E2a). Two-panel, includes the churn/re-adoption inset.
4. **F4** Real-WAN: selection-latency CDF per RTT bin, per engine (E3a).
5. **F5** Cost/quality of reasoning: placement quality vs. model capability and vs. inference latency,
   with cache/gating ablations (E4).
6. **T1** Full factorial (E1). **T2** External baselines (E7). **T3** Safety + churn summary (E6, E2b).

Below the line (tech report): F6 coordination overhead (E5) — fold its headline number into the text
as a sentence ("Snow reduces per-job consensus messages from X to Y"); E3b netem table; per-model
LLM breakdown.

---

## 9. Schedule — dated to abstract 24 Nov / paper 1 Dec 2026

| Wk | Dates | Milestone | Gate |
|---|---|---|---|
| 1 | Aug 17–23 | Freeze thesis §1. **Request the GPU node now** (scarcest resource — a denial here reshapes E4). Bring both slice parts up, verify 17-site reachability, deploy branch, Hier-30 smoke. ~~Write `evaluation/collect.py`~~ **done 2026-08-17** and validated against archived runs as fixtures. | Smoke green; GPU requested |
| 2–3 | Aug 24–Sep 6 | P0-1 LLM group delegation + P0-4 instrumentation. vLLM up on the GPU node, latency characterized. Local + Hier-30 validation. **Start E5 and the E3a analysis now** — neither needs new mechanism code. | LLM delegation works at Hier-30 |
| 4–5 | Sep 7–20 | P0-2 bandit×LLM composition, P0-3 cache + inference budget, P1-1 oracle, P1-3 site-aware configs. | All P0 merged |
| 6 | Sep 21–27 | Pilot one cell of each of E1/E2/E4 end-to-end; verify every §7 metric lands in the tidy CSV. Fix gaps. | **Code freeze Sep 27 — no mechanism changes after this** |
| 7–10 | Sep 28–Oct 25 | Main campaign, unattended, interleaved config order: **E0 (gate) →** E1 → E2 → E4 → E7. Nightly result pull + incremental figures. **Start writing architecture + related work from Oct 5** (do not wait for data). | E1 + E2 complete by Oct 18 |
| 11 | Oct 26–Nov 1 | E3b, E5, E6, hosted-LLM validation subset. | Campaign complete |
| 12 | Nov 2–8 | Rerun any high-variance cell; fill gaps. **Data freeze Nov 8.** Figures final. | All data in |
| 13 | Nov 9–15 | Full draft: evaluation written against real numbers, intro/abstract, baseline positioning. | **Complete draft Nov 15** |
| 14 | Nov 16–22 | Internal review (Hamza/Anirban), §6 reviewer pre-mortem pass, page-count surgery to 10pp incl. refs. | Reviewed draft Nov 22 |
| 15 | Nov 23–29 | **Register title+abstract Mon Nov 23** (one day before the AoE gate). Polish, artifact/repro appendix. **Target submit Wed Nov 25.** | Submitted |

**Two hard calendar facts:**
- **Abstract 24 Nov is a separate, hard gate** — miss it and the paper cannot be submitted at all.
  Register on **Nov 23**; a title and 200-word abstract are all it needs and both are known by week 13.
- **US Thanksgiving is Nov 26**, squarely inside the final week. Co-authors will be unavailable
  Nov 25–29, which is why internal review ends Nov 22 and submission targets Nov 25. The Nov 26–Dec 1
  stretch is buffer, not working time.

**Parallelization:** E5, E6 and the E3a analysis need no mechanism code — run them during weeks 2–6
while P0 lands. Writing starts Oct 5, four weeks before the data freeze; the evaluation section is the
only part that must wait.

---

## 10. Risks and fallbacks

| Risk | Mitigation / fallback |
|---|---|
| LLM delegation (P0-1) doesn't beat analytic on quality | This is a *result*, not a failure — E4 is framed as honest accounting. Fallback thesis shifts weight to "expensive decision planes break BFT; here's the envelope where reasoning pays." |
| **GPU node not granted / preempted** | Highest-probability schedule risk — request in week 1 and confirm before the campaign. Fallback: CPU-served 7–8B on a dedicated VM, which caps the model-capability sweep in E4 but leaves E1/E2/E3 fully intact (a slow LLM is still a valid expensive-decision plane — arguably a more dramatic one). |
| Inference throughput bottlenecks the 270-agent runs | With vLLM on GPU this should not bind: coordinators only (~27 LLM agents), batched. Plus cache + inference budget (P0-3). If it still binds, cap LLM runs at Hier-90 and report 270 as PBFT/Snow-only, with any projection clearly labeled as such. |
| **Abstract deadline missed (24 Nov)** | Calendar it now, independent of paper state. Register Nov 23 with the week-13 title/abstract. |
| Paper exceeds 10 pages incl. references | Enforce the 5-figure/3-table cap from §8 at first draft, not at the end. Companion tech report absorbs the overflow and gets cited. |
| Slice instability / site outages mid-campaign (17 sites is a lot of failure surface) | Run configurations in interleaved order (not blocked by config) so partial data is still balanced; keep a 30-agent single-site fallback config; log per-run site health from the monitor VM. |
| ~120 h of testbed time doesn't fit | Cut order: E7 second scale point → E3b impairment levels (keep +50 ms and 1% loss) → E5 repeats → E1 non-headline cells. Never cut E7 entirely (§6). |
| Paper reads as three stapled features | Enforce §1: every experiment section opens by naming which claim (C1/C2/C3) it tests. If reviewers still split it, the natural fallback split is [Snow + bandit systems paper] and [LLM decision plane paper] — but do not plan for the split up front. |
| CCGrid deadline earlier than assumed | Drop to the "core three" — E1 (trimmed to Hier-90 only), E2, E7 — plus the banked LLM data as motivation. That is still a coherent paper. |

---

## 11. Immediate next actions (week of Aug 17)

1. **Request the GPU node** and add it to `SWARM-2slice.ipynb` — scarcest resource, longest lead time,
   and it gates the shape of E4.
2. Put **Nov 23 (abstract registration)** and **Sep 27 (code freeze)** on the calendar as hard dates.
3. Bring up both slice parts; run the Hier-30 smoke from `SWARM-2slice.ipynb` cell 21, then pipe it
   through `evaluation/collect.py` — that is the first end-to-end test of the real pipeline.
4. ~~Write `evaluation/collect.py`~~ **done 2026-08-17**, validated against archived runs (job counts
   and reasoning times reproduce the published table; Hier-250 completion 3.4–4.5% vs 4.1%).
5. Start P0-1 (LLM group delegation) — critical path for E1, E4, and the thesis.
