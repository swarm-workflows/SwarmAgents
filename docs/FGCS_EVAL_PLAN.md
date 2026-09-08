# FGCS Submission — Evaluation Plan (LLM Agents + Snow/Gossip + Contextual Bandit)

**Status:** draft plan, 2026-08-17; **retargeted to a journal and reviewed 2026-09-08 (see §0)**.
Successor to `SNOW_GOSSIP_PAPER_PLAN.md` (which covered Snow only).
**Predecessor paper:** SWARM+ (eScience'26, accepted) — PBFT hierarchical consensus, data-aware
placement, Pegasus workloads, 3–4 FABRIC sites. That paper is the baseline; nothing in it can be a
contribution here.
**Testbed:** 2-slice FABRIC deployment, ~90 VMs across 17 sites (16c/16G/500G), one `database` node
(Redis) and one Prometheus/Grafana monitor VM. Driven from `notebooks/SWARM-2slice.ipynb` +
`db_node_setup/`.
**Venue (decided 2026-09-08):** *Future Generation Computer Systems* (Elsevier), regular article.
Rolling submission, no abstract gate; single-anonymized review, so citing SWARM+ and naming the
FABRIC slice is not an anonymity problem (the reason CCGrid'27, double-blind, was dropped — together
with its 10-page cap). Length: verify against the current Guide for Authors before drafting; a
secondary source quotes ~18 pages, and journal reviewers expect a fuller related-work and threats
section than a conference allows. Self-imposed dates in §9 replace the conference deadline.
**Fallback venue:** a conference *after* FGCS returns reviews. IPDPS'27 is **not** a fallback —
its abstract/paper deadlines are 2 / 9 Oct 2026, before the P0 code exists (§0.1).

---

## 0. Review of 2026-09-08 — what changed and what must change before testing

### 0.1 Where the plan actually stands (week 4 of the original schedule)

The original schedule had P0-1 (LLM group delegation), P0-4 (instrumentation) and vLLM done by
Sep 6. At the time of this review **none of the P0 code existed in the tree** — no
`llm_delegator.py`, no `delegation.policy`, no consensus message/byte counters, no
`evaluation/oracle.py`. P0-0 and P0-5..P0-7 landed on 2026-09-08, and **P0-1 landed the same day**
(`swarm/agents/llm/llm_delegator.py`, `delegation.policy`); P0-4, P0-8, P0-2/P0-3 and P1-1 are
still open. P1-2 (`collect.py`) and P1-3 (site-aware configs) are done. The critical path is ~3 weeks behind, which is the practical reason
the journal target is right: a Nov 24 abstract was no longer reachable with an honest campaign.
IPDPS'27 (abstract Oct 2, paper Oct 9, 2026) is out for the same reason and should not be named
as a fallback.

### 0.2 Pre-campaign fixes — silent bugs that would corrupt paper numbers

Found by the Chaos Jungle campaign (`origin/chaos:SWARMAGENTS_FINDINGS.md`); five of its fixes were
cherry-picked on 2026-09-07 (Ollama provider, structured output, DTN pool alignment, `--seed`,
one-DTN-per-job), and the DTN pool is now derived from the generated fleet.

**All eight below are FIXED as of 2026-09-08** (P0-0 in §4), with regression tests in
`tests/test_precampaign_fixes.py`. The table is kept because each row explains a number the
campaign would otherwise have reported wrongly, and because the "Fix" column is what a reader of
the paper's threats-to-validity section will want.

| # | Bug | Why it matters to *this* plan | Fix as landed |
|---|---|---|---|
| F-13 | `config_swarm_multi.yml` sets `runtime.peer_expiry_seconds` twice (300, then 45); YAML keeps **45 s** | A documented knob has never had its documented value; E2b/E6 churn timing depends on it | ✅ Duplicate removed, **300 s** kept (it is a staleness filter, not the failure detector). `swarm/utils/yaml_strict.py` now raises on any duplicate key, naming both lines; used by the agent and by `generate_configs`. The property's code default also said 20 in one place and 300 in another — unified on 300 |
| F-9 | `Job.execute()` sleeps a flat 1 s regardless of `wall_time` | **Makespan, throughput and utilisation are primary metrics in E1/E4/E7 and were meaningless** — every job took one second | ✅ `runtime.wall_time_{scale,min_s,max_s}`; simulated sleep is `clamp(wall_time * scale, min, max)`, applied to the quantum path too. Shipped **scale 1.0, cap 120 s**: the 25k-job Pegasus base set is p50 2.0 s / p90 10.7 s / p99 40.4 s / max 1992.7 s, so real durations replay faithfully and the cap bounds the ~1% tail that would otherwise own the makespan. **State the cap wherever makespan is reported.** `scale: 0` restores the flat 1 s and logs a warning that makespan is not meaningful. The split-hybrid producer/consumer paths draw from the same budget, so a workload mixing split and whole jobs has one execution model |
| F-14 | `run_test.py --runtime` is parsed and never read; a run that cannot place jobs polls forever | Unattended campaign: one stalled cell blocks the queue for the night | ✅ `wait_runtime()` now honours it as a hard cap and logs that the run ended on the clock, not on drain. **Default changed 90 → 0 (no cap)**: enforcing the old default would have truncated every run that omits the flag. With no cap and no `--shutdown-after-seconds`, the runner warns at start. **The campaign driver must pass one** |
| F-6 | `cleanup_between_runs` deletes `agent_hosts.txt`, then `generate_configs.py` reads `--agent-hosts-file agent_hosts.txt` | This is the documented remote invocation for the slice; it crashes | ✅ The delete is skipped when the resolved `--agent-hosts-file` is that path; a generated hosts file is still cleaned |
| F-7 | `llm.timeout_seconds` is parsed and never enforced (bids of 15–19 s observed under a 6 s setting) | E4 sweeps inference latency and E8 injects it; a timeout that never fires silently changes what "fallback" means | ✅ Passed as `ModelSettings(timeout=…)`; on breach the call raises and the existing except-path falls back (or abstains under `disable_fallback`). 0 disables. **Note for E4/E8: the shipped 6 s is below the measured 4–7 s bid latency, so it will fire — set it per arm deliberately** |
| — | `generate_configs` allocates capacity flavours as *percentages of fleet size*, so agent *i* differs between Hier-30 and Hier-270 even with the same seed | The scale ladder compares different fleets, not the same fleet at different sizes | ✅ `--master-fleet-size N` (forwarded by `run_test.py`) draws flavours and quantum backends for the master fleet and writes this fleet's prefix. Verified end to end: at seed 42 a 10-agent and a 30-agent fleet share byte-identical capacities and DTNs for agents 1–10 with the flag, and **7 of 10 differ without it**. **Every ladder run must pass `--master-fleet-size 270`** |
| — | `generate_configs` silently reuses an existing `agent_dtns.json`, consuming randomness differently | A "reproducibility check" that does not start clean disagrees with itself | ✅ Reuse now prints a warning naming the file and saying `--seed` will not reproduce a clean run. `cleanup_between_runs` deletes `agent_profiles.json` and `agent_dtns.json` unconditionally (commented as load-bearing). `--seed` is still the campaign driver's job to pass |
| F-12 | Hierarchical per-agent summaries are all labelled `[no_restarts]` | Analysis-only, but hierarchical logs cannot be read unambiguously | ✅ Labelled from meaning (`[level0]`/`[level1]`/`[level2]`, and `[level1,no_restarts]` when both apply) rather than from the truthiness of a filename suffix |

### 0.3 The chaos results change C2 and E4 — and add an experiment

The chaos campaign (30 agents, local Ollama and gateway arms, frozen Pegasus trace) reached one
conclusion that the plan as written does not survive unchanged: **placement is decided by bid
arrival time, and the LLM's output is close to inert.** Evidence, all measured:

- Eight LLM-blind agents (503 from the LLM, instant analytic fallback) captured **280 of 300 jobs**;
  slowing 15 agents by +3 s moved nothing. The fallback out-races reasoning.
- Inverting the LLM's reasoning (semantic corruption, four modes, 100% radius) changed the bids
  and **not the schedule**.
- **59% of qwen2.5:3b bids were the identical score 75.00**; gpt-oss-20b put **92% on two values**.
  A 0–100 rating is a degenerate cost signal at any model size. (The id-based tie-break that this
  exposed is fixed — `swarm/utils/tiebreak.py` — but the ties remain.)
- ~~**Finding 11, first half**~~ **FIXED 2026-09-08 (P0-5).** Under `consensus.protocol: snow`
  (the shipped default) peers answered queries with `_cost_job_on_agent`, the *analytic* cost, so
  the LLM's verdict entered only through the initiator. Every LLM × Snow cell in E1 would have
  measured the analytic model with LLM latency attached. Peers now answer from a cached LLM
  verdict, or abstain.
- **Finding 11's second half is WRONG, and the correction matters for the paper.** It states the
  two costs are on different scales — "analytic roughly 0–1 ... LLM 25–75". `compute_job_cost`
  ends in `* 100`, so both planes emit 0–100. Measured over 400 real Pegasus jobs × the five
  shipped flavours: analytic p10 2.59, p25 5.41, **p50 11.85**, p75 28.40, p90 71.85, 1.6% above
  100 — squarely overlapping the LLM's 25 (score 75) and 5 (score 95). A canonical rescaling
  built on the 0–1 claim was written and then removed the same day: on the real distribution it
  sent a median analytic cost to 92.2 and made every analytic agent look worthless, an inversion
  far worse than the problem. Costs are compared raw; `tests/test_cost_scale.py` pins the 0–100
  assumption so a change to either model fails a test.
- **What survives of it is smaller and is an E4 measurement, not a bug.** The two models
  *calibrate* differently: a fallback analytic bid has median 11.85 against a typical LLM bid of
  25, so an agent whose LLM is down bids ~2× lower on average. That is a difference between two
  models' opinions, not a units error, and correcting it would be putting a thumb on the scale.
  E4 should report the bid distribution per plane; E8's capture ratio has this mixed into it
  alongside the fallback's speed advantage.
- Throughput is flat in fleet size on the LLM arm (14→60 agents: 0.96→0.82 jobs/s) because every
  agent bids on every job (3.7 bidders/job). Designated bidding halves the LLM calls but its first
  run was 97.8% deadline-forced (deadline mis-tuned; counters per-iteration, not per-job).

**Consequences for the plan:**

1. **Add P0-5 — LLM cost reaches the Snow query path** (cache the last LLM verdict per job for
   `my_cost_for_job`; never call the model on the inbound thread). Blocking for E1's core cells.
   Do this before anything else in §4. *(The "normalise the two cost scales" clause originally
   here was dropped: the scales were already the same. See the correction above.)*
2. ~~**Add P0-6 — bid elicitation.**~~ **DONE 2026-09-08.** `llm.score_scale` and
   `llm.tie_break_with_analytic`, both off by default. E4 crosses them as a 2×2 and reports the
   `[STATS]` distinct-value count and modal share alongside placement quality (T5). If a finer
   scale does not move modal share, the degeneracy is the model's and not the question's — which
   is itself the answer to C2(a).
3. ~~**Add P0-7 — fallback parity.**~~ **DONE 2026-09-08.** `llm.bid_pacing: {none,
   fallback_parity, uniform}`, off by default. E8 should run S05 across all three: `none`
   reproduces the 38.5× capture, `fallback_parity` should remove it, and `uniform` says whether
   anything of the race survives once every bid arrives at the same time. Pair it with
   `disable_fallback` to separate "the fallback is fast" from "the fallback exists".
4. **Fix `designate_bidder`** before E1's 270-agent LLM cells (per-job counters; deadline sized to
   the designee's window, not to one bid) — or expect flat throughput and explain it.
5. **E4 needs a no-fault control in every fallback-rate cell** (chaos §7.5: a capture ratio is
   meaningless without it) and should add **bid latency per agent** and **per-agent capture ratio**
   to its metrics — race-to-propose is the mechanism, so measure it directly.
6. **Add E8 — decision-plane resilience under fault injection** (new, journal length permits it):
   `{LLMLatency, LLMUnavailable, SemanticCorrupt}` × blast radius `{25%, 50%, 100%}` × fallback
   `{on, off}` at Hier-90 on the 17-site slice, plus the composite scenario that also removes
   nodes (the only one that stresses the exactly-once invariant). Scenario drivers exist on
   `origin/chaos` (`scenarios/`, deliberately not cherry-picked yet); results are re-measured here
   per §2. Budget 24 cells × 3 repeats = 72 runs ≈ 24 h (in the §5 table). F7 in §8.
7. **Reframe C2** from "LLM coordinators improve decision quality" to a falsifiable pair: (a) with
   P0-5/6/7 in place, does the LLM's *output* change placement at all (S09-style inversion as the
   test), and (b) what does its *latency* cost each consensus engine. If (a) is still null after the
   fixes, that is the finding, and E8 plus T5 are its evidence.
8. **Operational:** the chaos runbook's pre-run health gate (memory ≥300 MB available per host;
   restart local inference before a campaign; verify the proxy actually forwards) goes into the
   campaign driver. Memory-starved hosts produced 139–250 s bids and zero placements while looking
   healthy.

### 0.4 What the journal format changes in the plan

- No abstract gate, no 10-page cap: §8 restores E5 and E3b as in-paper figures/tables and adds F7/T5.
- Single-anonymized review: the eScience'26 delta table in §1 can be stated openly, and the
  FABRIC 17-site deployment can be described in full (it identifies the group either way).
- Reviewers will expect a **threats-to-validity** section: single testbed, one workload family
  (Pegasus-derived), Redis in the control plane, flat-sleep execution if F-9 is not fixed, LLM
  nondeterminism (temperature 0.1) and model version pinning. Write it from the §0.2 list.
- Reproducibility appendix: seeds, frozen fleet, `collect.py` outputs, tagged revision.

### 0.5 What §0 changed in the body, and what it did not

Folded into the body on 2026-09-08: C2 reframed (§1); P0-0 and P0-5..P0-8 added to the code-work
table (§4, ~8 d); E4 gained an elicitation arm, no-fault controls and race metrics; E8 added with
its own budget row (§5); §8–§11 rewritten for the journal. Unchanged: the thesis and C1/C3 (§1), the
no-reuse rule (§2), E0–E3 and E5–E7 as designed, the metrics protocol (§7) and the pre-mortem (§6).
Budget: ~366 → ~458 runs, ~124 → ~153 testbed-hours before reruns.

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
| **C2** | *(reframed 2026-09-08, §0.3)* Two separable questions about LLM coordinators: **(a)** once the LLM's verdict actually reaches consensus (P0-5), is elicited as a continuous signal (P0-6), and cannot be out-raced by its own fallback (P0-7), does its *output* change placement on semantically rich jobs at all; **(b)** independent of (a), its inference *latency* is what breaks BFT coordination. A null on (a) is a finding, not a failure. | (a) Semantic-inversion test (E8, S09-style) plus the E4 quality delta vs analytic, with tie rate / distinct-score count per model (T5). (b) The Hier-250 collapse (3.5–4.2% completion) reproduced, then *removed* by swapping the coordinator tier to Snow. |
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
| **P0-0** ✅ | **Pre-campaign fixes (done 2026-09-08)** — the §0.2 list: duplicate `peer_expiry_seconds`, flat 1 s job sleep (restore `wall_time * scale`), `--runtime` never read, `agent_hosts.txt` delete-then-read, `llm.timeout_seconds` unenforced, prefix-stable fleets across the scale ladder, clean-state generation in the driver. | `config_swarm_multi.yml`, `swarm/models/job.py`, `run_test.py`, `llm_bidder.py`, `generate_configs.py` | 2 d |
| **P0-5** ✅ | **LLM cost reaches the Snow query path (done 2026-09-08).** Chaos finding 11. `_HostAdapter.my_cost_for_job` returns the analytic cost, so peers out-vote the LLM and compare 0–1 against 25–75. Cache the last LLM verdict per (job, agent) for the inbound path — never call the model on the consumer thread — and normalise both costs onto one scale before any comparison. **Blocking for every LLM × Snow/Hybrid cell in E1.** **As landed:** `_HostAdapter.my_cost_for_job` delegates to an overridable `wire_cost_for_job`; `LlmAgent` answers from a bounded, TTL'd verdict cache written on every bid (never calling the model on the inbound thread), and abstains on a miss rather than answering with a different plane (`llm.snow_cost_fallback: analytic` is the ablation arm). **No rescaling**: measurement showed both planes already emit 0-100, so the "normalise the two scales" half of this item was not needed and the attempt at it was harmful — see §0.3. `swarm/agents/cost_scale.py` now only tags which plane produced a cost, for instrumentation, and records the measurement. Tests in `tests/test_cost_scale.py`. | `resource_agent.py` `_HostAdapter`, `llm_agent.py`, `cost_scale.py` | 2 d |
| **P0-6** ✅ | **Bid elicitation (done 2026-09-08).** The 0–100 absolute rating ties 59–92% of the time. **As landed:** two orthogonal arms, both defaulting to the measured baseline so the control is what ships. `llm.score_scale` sets the range the model is asked for (the bound goes in the JSON schema, not just the prompt — Ollama's `NativeOutput` drives from the schema and would silently clamp a 0–1000 answer); the cost is normalised back to 0–100 either way. `llm.tie_break_with_analytic` orders surviving ties by the analytic cost: ratings are quantised to the grid the model was asked for and the term is capped at **half of one grid step**, so it separates agents that gave the same rating and provably cannot move one past a neighbouring rating. Both halves are needed — `Bid.score` is a float, so the cap alone leaves two scores 0.1 apart reversible. Quantisation discards sub-grid precision on this branch only; ask for a finer grid with `score_scale` instead. `[STATS]` now reports bid count, distinct values and modal share (T5). The rank/pairwise variant was not built: it needs a prompt over the whole candidate window and a different call shape, and the cheap arms test the same hypothesis first. | `llm_bidder.py`, `llm_agent.py`, `config_swarm_multi.yml` | 2 d |
| **P0-7** ✅ | **Fallback parity (done 2026-09-08).** **As landed:** `llm.bid_pacing` with three modes, defaulting to `none` (the measured baseline). `fallback_parity` holds a fallback bid until it has taken as long as a real one; `uniform` holds *every* bid to the same target, so arrival time carries no information about the agent — the direct test of race-to-propose. The wait **tops a bid up to** the target rather than adding to it, so a timeout failure (which already burned `timeout_seconds`) and an instant 503 are treated alike, and a slow bid waits not at all. The target self-calibrates from this agent's own observed latencies at a configurable quantile (use ~0.9 for `uniform`), but **not from those alone**: an agent whose LLM never succeeds — the entire S05 faulted population — would never learn one and would never pace, leaving the 38.5× capture untouched. `bid_pacing_bootstrap_s` covers that and the cold-start window, defaulting to `llm.timeout_seconds` **resolved through `LlmConfig`**, which (with the timeout now enforced) is an upper bound on a successful bid and so a provable parity bound. Resolving it there rather than from the raw config dict matters: `LlmConfig` defaults the key to 6 s, so a config that omits it still bounds the bidder — reading the dict with a default of 0 left pacing inert on exactly that config. Same failure shape as the duplicated `runtime.peer_expiry_seconds` in §0.2: one key, two defaults. Capped by `bid_pacing_max_s` and sliced so shutdown is not delayed. `[STATS]` reports mode, target, waits and seconds held. `llm.disable_fallback` remains the separate ablation arm and is deliberately not paced — an agent that is not a candidate gains nothing by being quick to say so. | `llm_agent.py` `_llm_or_analytic_cost` | 1 d |
| **P0-8** | **Fix `designate_bidder`** before any 270-agent LLM cell: per-job (not per-iteration) counters; deadline sized to the designee's window, not to one bid. Otherwise LLM-plane throughput is flat in fleet size (3.7 bidders/job). | `llm_agent.py` `_designate_bidders` | 1 d |
| **P0-1** ✅ | **LLM group delegation (done 2026-09-08).** At a coordinator, prompt over child-group summaries and return a ranked group choice + rationale. **As landed:** `delegation.policy` (`bandit` default = the pre-existing behaviour, `llm` = the model decides), with the decision point extracted into one overridable `ResourceAgent._select_child_groups` so both policies see the identical candidate list — the arms of E4 differ in the decision *rule*, not in what they were told. The summary the model gets is the **same `GroupSnapshot`** the contextual bandit gets, via a new public `MABManager.snapshots_for`, so headroom, inflight and the failure/timeout history are common to both. Candidate ids go in the **JSON schema** as a `Literal`, not just the prompt — the `score_scale` lesson again, since Ollama's `NativeOutput` generates from the schema and would happily answer `[0,1,2]` for groups 3, 7, 11 — and the answer is sanitised anyway (unknown ids dropped, de-duplicated, short rankings completed from the candidate order). Every failure path falls back to `bandit` **for that job**: provider error, `llm.timeout_seconds` breach, a ranking naming no group we offered, or a delegator that could not even be built. A decision that cannot change the outcome (one candidate, or `top_k ≥ len(candidates)`) spends no inference and is counted as `trivial`. Under `llm` the bandit still receives outcomes from the delegation monitor, so `MABManager.record_external_selection` hands it the **decision-time** context (not a rebuild after a multi-second call) — otherwise its arm counters would climb while the contextual model silently skipped every update, describing a run it did not steer. Delegation is **not** paced (P0-7): one coordinator owns its job, so a fast fallback out-races nobody. Audited to `llm_score:delegate:*`; `metrics.json` gets `llm_delegations` + `llm_delegation_stats`, kept apart from `mab_selections`; `[STATS]` reports calls/fallbacks/empty/trivial/mean latency. Two review findings worth recording because both are the campaign's recurring failure shape — *the fallback path is cheaper than the path it stands in for*: a failed decision originally widened the fan-out to every capable group (failing rewarded with the whole subtree, the fan-out analogue of §12.2's race-to-propose), and a timed-out call was charged nothing, which would have made `mean_s` cheapest in precisely the fallback-heavy runs E4 exists to price. Both fixed; the fallback now keeps the configured fan-out and picks at random when nothing ranks. **Two open limits, both P0-3's:** the call is synchronous on the coordinator's scheduling thread, so its delegation rate is capped at 1/latency; and `llm.timeout_seconds` is per-request, so provider and output-validation retries can stack several into one decision — there is no wall-clock deadline. Tests in `tests/test_delegation.py`. | `swarm/agents/llm/llm_delegator.py`, `resource_agent.py` `_select_child_groups`, `mab_manager.py`, `config_swarm_multi.yml` | 3–4 d |
| **P0-2** | **Bandit×LLM composition.** Config `delegation.policy` already exists (P0-1) with `bandit` and `llm`; this adds the third mode `bandit_gated_llm` — bandit narrows `capable_groups` → top-m, LLM ranks those m, and the LLM's pick is the arm the bandit is rewarded on. The reward path is in place: `record_external_selection` already lands an LLM-routed outcome on the right arm with its selection-time context. One key, one place — do **not** add a second switch. | `mab_manager.py`, `resource_agent.scheduling_main` | 3 d |
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
- **Elicitation (added 2026-09-08):** 8B model, 2×2 over `score_scale {100, 1000}` ×
  `tie_break_with_analytic {off, on}` → does a bid signal that can actually order agents let the
  LLM's output reach placement at all? Report distinct-score count and modal share (from `[STATS]`)
  alongside the quality delta. The `{100, off}` cell is the campaign baseline.
- **Controls:** every cell that reports a fallback or capture ratio has a **no-fault, same-fleet
  control** in the same table (chaos §7.5 — a ratio without its control is meaningless).
- **Metrics:** LLM calls per job, cache hit rate, tokens & $ (or GPU-seconds), inference p50/p95,
  **bid latency per agent** and **per-agent capture ratio** (race-to-propose is the mechanism —
  measure it directly), tie rate / distinct-score count, fallback rate (`[LLM_COST_FALLBACK]` and
  `[LLM_COST_NO_BID]`), and the **quality delta** vs analytic: makespan (only once P0-0 restores real
  wall times), DTN-locality hit rate, fairness, completion.
- **Payoff:** the "is the LLM worth it?" table. A negative or mixed result here is publishable and
  makes the paper credible — do not bury it. Expected shape given the chaos data: the LLM's output
  moves placement little unless elicitation changes; its latency costs throughput; bandit-gating +
  caching recover most of the loss. If the rank/pairwise arm changes placement where the rating arm
  did not, that is C2(a)'s positive result.

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

### E8 — Decision-plane resilience under fault injection (C2(a), added 2026-09-08)
Hier-90, Hybrid engine, LLM plane, frozen fleet and trace. Faults injected at the LLM endpoint via
the Chaos Jungle proxy (drivers in `origin/chaos:scenarios/`, to be merged; results re-measured here
per §2):
- **Faults × radius × fallback:** `{LLMLatency +3 s, LLMUnavailable 503, SemanticCorrupt}` ×
  blast radius `{25%, 50%, 100%}` of coordinators × `llm.disable_fallback {false, true}` = 18 cells.
- **Composite:** LLM outage + node loss at 25% radius, fallback on/off (2 cells) — the only scenario
  that stresses the exactly-once invariant while the decision plane is degraded; feeds E6's audit.
- **Controls:** no-fault, same fleet, fallback on/off (2 cells) — shared with E1's Hybrid×LLM Hier-90
  cell where the configuration is identical. Plus 2 radius points for the SemanticCorrupt partial-radius
  case (poisoned agents become the *slow* ones; capture should invert relative to the outage case).
- **Metrics:** completion, per-agent capture ratio (faulted vs healthy), Jain's fairness, fallback /
  no-bid rate, bid latency per agent, double-assignment count.
- **Payoff:** the resilience story the chaos campaign found at 30 agents — a partial outage is worse
  than a total one because the fallback routes work to agents that cannot reason — measured on the
  paper's substrate, and the direct test of whether LLM output reaches placement after P0-5/6/7.
- Repeats: 3 (24 cells). F7 in §8.

**Approximate run budget** (assume ~20 min per run incl. setup/teardown, `batch_tests_v2.py --runs N`):

| Exp | Cells | Repeats | Runs | Testbed hours |
|---|---|---|---|---|
| E0 | 4 (2 shared with E1) | 5 | ~10 net | ~4 |
| E1 | 8 × 2 scales + 3 mesh | 5 (10 headline) | ~105 | ~35 |
| E2 | 6 policies + 2 stressors × 3 | 5 | ~60 | ~20 |
| E3b | 5 impairments × 3 engines | 5 | 75 | ~25 |
| E4 | 8 + 4 (elicitation arm + no-fault controls) | 5 | 60 | ~20 |
| E5 | 8 | 3 | 24 | ~8 |
| E6 | 4 | 3 | 12 | ~4 |
| E7 | 4 × 2 scales | 5 | 40 | ~14 |
| E8 | 24 | 3 | 72 | ~24 |
| | | | **~458** | **~153 h** |

~150 testbed-hours ≈ 4 weeks of mostly-unattended running with a driver script, assuming the slice
stays healthy. Budget 2× for reruns (~300 h), which is what the 8-week campaign window in §9
(Oct 13 – Dec 7) is sized for. **This is the reason to freeze the code on Oct 12.**

---

## 6. Reviewer pre-mortem

From `review.txt` and the CCGrid'26 revision notes. The PC no longer overlaps by construction (journal
reviewers), but these are the objections any distributed-scheduling reviewer raises, and the eScience
reviewer repeated them independently. Design them out now:

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

Journal length removes the 5-figure cap, but not the discipline: every figure still has to name the
claim (C1/C2/C3) it supports. Ranked core set first; the former "below the line" items are now
in-paper (§0.5) rather than tech-report material.

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

7. **F6** Coordination overhead (E5): per-job consensus messages and bytes vs. scale, PBFT vs Snow,
   gossip on/off — the O(n²) vs O(k·fanout) curve, previously text-only.
8. **T4** Controlled-impairment table (E3b) — the direct rebuttal to the eScience delay/loss objection.
9. **F7** Decision-plane resilience under fault injection (E8, §0.3): per-agent capture ratio and
   fairness vs. blast radius, fallback on/off — the "partial outage is worse than total" result.
10. **T5** Per-model LLM breakdown (E4): distinct-score count, tie rate, tokens, latency per model.

---

## 9. Schedule — journal target, self-imposed gates (revised 2026-09-08)

The conference deadline is gone; the binding clocks are now the **slice lease**, the **code freeze**,
and reviewer turnaround (FGCS typically returns first reviews in months, so a major revision that
needs new runs must be answerable from a slice that still exists — keep the deployment reproducible
from `SWARM-2slice.ipynb` + `db_node_setup/` and tag the frozen revision).

| Wk | Dates | Milestone | Gate |
|---|---|---|---|
| 1 | Sep 8–14 | ~~**Pre-campaign fixes (§0.2)**~~ and ~~**P0-5**~~ **both DONE 2026-09-08.** Now: Hier-30 smoke on the slice through `collect.py`, with `--master-fleet-size 270` and a `--runtime` cap, checking `wire_cost_hit_rate` on an LLM cell. Confirm GPU node status and the slice lease horizon. | Smoke green |
| 2–4 | Sep 15–Oct 5 | ~~P0-1 LLM group delegation~~ (done Sep 8), P0-4 instrumentation, ~~P0-6 bid elicitation~~, ~~P0-7 fallback parity~~ (both done Sep 8), P0-8 designated bidder. vLLM up on the GPU node, latency characterized. **Start E5 and the E3a analysis now** (no mechanism code needed). | LLM delegation works at Hier-30 |
| 5 | Oct 6–12 | P0-2 bandit×LLM composition, P0-3 cache + inference budget, P1-1 oracle. Pilot one cell each of E1/E2/E4/E8 end-to-end; verify every §7 metric lands in the tidy CSV. | **Code freeze Oct 12** — tag it |
| 6–11 | Oct 13–Nov 23 | Main campaign, unattended, interleaved config order: **E0 (gate) →** E1 → E2 → E4 → E7 → E8. Nightly result pull + incremental figures. **Start writing architecture + related work from Oct 20.** | E1 + E2 complete by Nov 9 |
| 12–13 | Nov 24–Dec 7 | E3b, E5, E6, hosted-LLM validation subset. Rerun high-variance cells. | **Data freeze Dec 7.** Figures final |
| 14–18 | Dec 8–Jan 11 | Full draft (holidays inside this window — plan for it). Evaluation written against real numbers; related work with positioning table; threats-to-validity section (journal reviewers expect one). | **Complete draft Jan 11, 2027** |
| 19–20 | Jan 12–25 | Internal review (Hamza/Anirban), §6 pre-mortem pass, reproducibility appendix (configs, seeds, `collect.py` outputs). | Reviewed draft Jan 25 |
| 21 | Jan 26–31 | Polish, cover letter, submit. | **Submit to FGCS by Jan 31, 2027** |

**Parallelization:** E5, E6 and the E3a analysis need no mechanism code — run them during weeks 2–5
while P0 lands. Writing starts Oct 20, seven weeks before the data freeze.

**If a conference fallback is wanted at all,** it has to be one whose deadline falls *after* FGCS
reviews return (mid-2027 at the earliest). Do not plan the campaign around it.

---

## 10. Risks and fallbacks

| Risk | Mitigation / fallback |
|---|---|
| LLM delegation (P0-1) doesn't beat analytic on quality | This is a *result*, not a failure — E4 is framed as honest accounting. Fallback thesis shifts weight to "expensive decision planes break BFT; here's the envelope where reasoning pays." |
| **GPU node not granted / preempted** | Highest-probability schedule risk — request in week 1 and confirm before the campaign. Fallback: CPU-served 7–8B on a dedicated VM, which caps the model-capability sweep in E4 but leaves E1/E2/E3 fully intact (a slow LLM is still a valid expensive-decision plane — arguably a more dramatic one). |
| Inference throughput bottlenecks the 270-agent runs | With vLLM on GPU this should not bind: coordinators only (~27 LLM agents), batched. Plus cache + inference budget (P0-3). If it still binds, cap LLM runs at Hier-90 and report 270 as PBFT/Snow-only, with any projection clearly labeled as such. |
| Journal review cycle demands new runs months later | Tag the frozen code revision; keep the slice reproducible from the notebook; archive every run tree plus `agent_profiles.json`/`agent_dtns.json`/seeds so any cell can be re-run identically. |
| Slice lease expires mid-campaign or before revisions | Check the lease horizon now and renew ahead of the campaign; it is the only hard external clock left. |
| Slice instability / site outages mid-campaign (17 sites is a lot of failure surface) | Run configurations in interleaved order (not blocked by config) so partial data is still balanced; keep a 30-agent single-site fallback config; log per-run site health from the monitor VM. |
| ~120 h of testbed time doesn't fit | Cut order: E7 second scale point → E3b impairment levels (keep +50 ms and 1% loss) → E5 repeats → E1 non-headline cells. Never cut E7 entirely (§6). |
| Paper reads as three stapled features | Enforce §1: every experiment section opens by naming which claim (C1/C2/C3) it tests. If reviewers still split it, the natural fallback split is [Snow + bandit systems paper] and [LLM decision plane paper] — but do not plan for the split up front. |
| LLM plane's output turns out inert for placement even after P0-5/6/7 (§0.3) | Then C2 is reframed, not abandoned: the paper reports *why* (race-to-propose, degenerate scores) with E8 as the evidence, and the cost/latency envelope stands on its own. This is a publishable negative result in a journal. |

---

## 11. Immediate next actions (week of Sep 8)

1. ~~**Close the §0.2 pre-campaign fix list**~~ **DONE 2026-09-08** — all eight fixed, with
   regression tests in `tests/test_precampaign_fixes.py`. Two carry a standing obligation on every
   campaign run: pass `--master-fleet-size 270` (else the ladder compares different fleets) and a
   `--runtime` cap or `--shutdown-after-seconds` (else a stalled cell polls all night).
   `batch_tests_v2.py` forwards both flags and its own `--runtime` default moved 30 → 0, since
   enforcing the cap would otherwise have stopped every batch run after 30 s.
   **A third obligation, found reviewing P0-1: every delegation cell must pass `--co-parents 2`
   (or more).** The shipped hierarchical topology gives each Level-1 coordinator exactly one
   child group — verified on a generated Hier-30 fleet: 5 LLM coordinators, one child group
   each, none with more — and a Level-2 super-coordinator's `children` is the single Level-1
   group it manages. With one candidate there is no routing decision, so **the bandit and the
   LLM delegator are both inert**: E2's learned-delegation figures and every LLM-delegation
   cell in E1/E4 would come back empty from a run that otherwise looks healthy. Coordinators
   now log a one-time `[DELEGATION] ... can never choose` warning in that configuration; the
   topology fact is pinned by an end-to-end test in `tests/test_delegation.py`. This predates
   P0-1 and applies to the bandit arms just as much.
2. ~~**P0-5**~~ **DONE 2026-09-08.** One obligation follows: E4/E8 must report the new
   `wire_cost_hit_rate` from the `[STATS]` line. It is the fraction of peer votes that used a real
   LLM verdict rather than abstaining, so it bounds how much of any LLM × Snow result is actually
   attributable to the model. A low rate is itself a finding.
3. Confirm the **GPU node** status and the **slice lease horizon**.
4. Hier-30 smoke from `SWARM-2slice.ipynb`, piped through `evaluation/collect.py`.
5. Decide on E8 (fault-injection section) and, if yes, merge `scenarios/` from `origin/chaos`.
6. ~~Start P0-1 (LLM group delegation)~~ — **done 2026-09-08.** Next on the critical path: P0-4
   (instrumentation) and P0-8, then P0-2/P0-3 on top of `delegation.policy`. **Before any LLM
   delegation cell, measure the scheduling-thread cost**: at Hier-30 a coordinator delegating
   serially at ~1 LLM call/job caps its subtree's delegation rate at 1/latency, so if E1 shows
   LLM-plane throughput flat in fleet size, this — not the consensus protocol — may be why.
