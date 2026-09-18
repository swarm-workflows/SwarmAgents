# FGCS Submission — Evaluation Plan (LLM Agents + Snow/Gossip + Contextual Bandit)

**Status:** draft plan, 2026-08-17; **retargeted to a journal and reviewed 2026-09-08 (see §0)**.
Successor to `SNOW_GOSSIP_PAPER_PLAN.md` (which covered Snow only).
**Predecessor paper:** SWARM+ (eScience'26, accepted) — PBFT hierarchical consensus, data-aware
placement, Pegasus workloads, 3–4 FABRIC sites. That paper is the baseline; nothing in it can be a
contribution here.
**Testbed:** 2-slice FABRIC deployment, ~90 VMs across 17 sites (16c/16G/500G), one `database` node
(Redis) and one Prometheus/Grafana monitor VM. Driven from `notebooks/SWARM-2slice.ipynb` +
`db_node_setup/`.
**Two papers, one campaign (decided 2026-09-10, §0.9).** This document is the **journal**
plan — *Future Generation Computer Systems* (Elsevier), regular article, submit by **31 Jan
2027**; rolling submission, no abstract gate, single-anonymized review, so citing SWARM+ and
naming the FABRIC slice is not an anonymity problem. Length: verify against the current Guide
for Authors before drafting (a secondary source quotes ~18 pages); journal reviewers expect a
fuller related-work and threats section than a conference allows. It is **also the master for
what both papers share**: substrate rules (§0.2, §5 placement rules), the P0 code list (§4),
metric definitions (§7) and the experiment matrix and budget (§5).

**What is NOT here:** everything specific to the conference paper — its claims, figure budget,
fleet, owed code and **calendar** — lives in `docs/CCGRID27_PAPER_PLAN.md` (C1 bandit + C3
Snow; abstract 24 Nov 2026, **paper 1 Dec 2026**, verified against the CFP 2026-09-14 — both
files said 8 Dec for four days). §9 below carries only the shared campaign clock and the
journal-only window, and points at that file for the rest. The 2026-09-08 review that dropped
CCGrid (double-blind, 10 pages) was reversed on 2026-09-10 for the *two-claim* paper; the
objections stand for the *full* story, which is why the composite stays here. IPDPS'27 is not a
fallback for either (deadlines 2 / 9 Oct 2026).

---

## 0. Review of 2026-09-08 — what changed and what must change before testing

### 0.1 Where the plan actually stands (week 4 of the original schedule)

The original schedule had P0-1 (LLM group delegation), P0-4 (instrumentation) and vLLM done by
Sep 6. At the time of this review **none of the P0 code existed in the tree** — no
`llm_delegator.py`, no `delegation.policy`, no consensus message/byte counters, no
`evaluation/oracle.py`. P0-0 and P0-5..P0-7 landed on 2026-09-08, and **P0-1 landed the same day**
(`swarm/agents/llm/llm_delegator.py`, `delegation.policy`). **P0-4 landed 2026-09-14**
(`swarm/utils/instrumentation.py`), which closes the one item neither paper's figure list could
route around, and **P1-1 landed the same day** (`evaluation/oracle.py`), which was the last
one. **P0-8 landed 2026-09-14** too. P0-2/P0-3 remain open and are journal-only (E4). P1-2
(`collect.py`) and P1-3 (site-aware configs) are done. *(Historical: as of 2026-09-08 the
critical path was ~3 weeks behind, which is why this review retargeted to a journal and judged a
24 Nov abstract unreachable. §0.9 reversed that two days later for the two-claim conference
paper once P0-1 was in; with P0-4/P1-1/P0-8 landed on 09-14, every code item a conference
figure needs is done — see `CCGRID27_PAPER_PLAN.md` §4 for the four tooling items that were
never on this list.)* IPDPS'27 (abstract Oct 2, paper Oct 9, 2026) is out and should not be
named as a fallback.

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
  (Pegasus-derived), Redis in the control plane, simulated execution (F-9 is fixed, so wall times
  replay faithfully, but jobs still sleep rather than compute — state the 120 s cap), LLM
  nondeterminism (temperature 0.1) and model version pinning. Write it from the §0.2 list.
- Reproducibility appendix: seeds, frozen fleet, `collect.py` outputs, tagged revision.

### 0.5 What §0 changed in the body, and what it did not

Folded into the body on 2026-09-08: C2 reframed (§1); P0-0 and P0-5..P0-8 added to the code-work
table (§4, ~8 d); E4 gained an elicitation arm, no-fault controls and race metrics; E8 added with
its own budget row (§5); §8–§11 rewritten for the journal. Unchanged: the thesis and C1/C3 (§1), the
no-reuse rule (§2), E0–E3 and E5–E7 as designed, the metrics protocol (§7) and the pre-mortem (§6).
Budget: ~366 → ~458 runs, ~124 → ~153 testbed-hours before reruns.


### 0.6 No coordinator had a delegation decision to make (found and FIXED 2026-09-08)

`scheduling_main` filters delegation candidates through `_get_active_child_groups()` — the
groups a coordinator **actively leads**, not the ones assigned to it. A group is led by its
lowest-ID *live* co-parent (`_is_leader_for_group`), so with a healthy fleet leadership
concentrates instead of spreading. Measured on a generated Hier-30 fleet (5 Level-1
coordinators, all alive), groups actively led per coordinator:

| `--co-parents` | assigned each | **actively led** | coordinators with a choice |
|---|---|---|---|
| 1 (default) | 1 | `[1, 1, 1, 1, 1]` | **0 of 5** |
| 2 | 2 | `[0, 1, 1, 1, 2]` | 1 of 5 |
| 3 | 3 | `[0, 0, 1, 1, 3]` | 1 of 5 |
| 5 | 5 | `[0, 0, 0, 0, 5]` | 1 of 5, other four idle |

A Level-2 super-coordinator does not help: its `children` is the single Level-1 group it
manages. So **co-parenting is a failover mechanism, not a fan-out one** — raising K does not
give more coordinators a routing decision, it gives one coordinator all of them and turns the
rest into standbys, which would also skew the load and fairness figures.

With one candidate there is no decision: the bandit returns its only arm and the LLM delegator
short-circuits without calling the model. **This predates P0-1 and applies to the bandit arms
identically** — it means E2 (learned delegation quality) as specified has never been runnable
on this topology, and every LLM-delegation cell in E1/E4 would come back empty from a run that
otherwise looks healthy. It survived the contextual-bandit deployment validation because
nothing in the agent code makes the topology visible.

A second, independent way to reach the same inertness: a **fan-out that covers every
candidate**. With `delegation.top_k` (or `mab.top_k`) at or above the number of groups a
coordinator leads, every candidate is delegated to no matter how the policy ranks them —
`select_groups` returns them all and the LLM path short-circuits. Config rather than topology,
but it looks identical in the results, so any delegation cell must check both.

Landed now: coordinators log `[DELEGATION] ... can never choose` for either cause while it holds
(re-checked every heartbeat, since at startup an empty `neighbor_map` makes every co-parent believe
it leads everything), and `tests/test_delegation.py` pins the leadership distribution
end-to-end against generated configs so this cannot silently drift back.

**Resolved 2026-09-08 by `--groups-per-coordinator G`** (option 1 of the three considered;
the alternatives were making Level-2 super-coordinators the delegating agents, which needs a
3-level hierarchy in every delegation cell, and dropping E2 outright). The generator tied
Level-1 coordinators 1:1 to child groups (`parent_id = level_1_base + group`); it now builds
`ceil(num_groups / G)` coordinators, each **exclusively** parenting G groups, so leadership is
spread rather than concentrated and no coordinator is idled. Measured on Hier-30 (groups
actively led per coordinator, whole fleet alive):

| `--groups-per-coordinator` | coordinators | actively led | with a choice |
|---|---|---|---|
| 1 (default) | 5 | `[1, 1, 1, 1, 1]` | 0 of 5 |
| 2 | 3 | `[1, 2, 2]` | 2 of 3 |
| 3 | 2 | `[2, 3]` | 2 of 2 |
| 5 | 1 | `[5]` | 1 of 1 |

Compare the `--co-parents` table above: that one concentrates, this one spreads. The two
compose (G=2 with K=2 gives `[3, 2, 0]` — a genuine choice for two coordinators plus one warm
standby), so failover can still be exercised inside a delegation cell.

Design points worth keeping. Coordinator slots freed by the larger fan-out become **Level-0
agents**, so the fleet is still exactly `--agents` in size and `run_test.py`'s 1..N id range
stays valid; group sizes then differ by at most one and each agent reports its own. G=1 takes
the original code path untouched — verified byte-identical against pre-change output for
Hier-30 — so the presets and everything measured on them are unaffected. Three-level presets
(100, 990, 1000) refuse the flag: their super-groups are sized in Level-1 agents and would need
restructuring too. Forwarded by `run_test.py` and `batch_tests_v2.py`; the topology facts are
pinned by `tests/test_delegation.py`.

**G is now an E2 axis, not merely a fix.** The delegation branching factor is the natural
x-axis for learned-delegation quality: at G=1 there is nothing to learn, and the question worth
answering is how bandit and LLM delegation compare as the number of candidate groups grows.
Suggested sweep G ∈ {2, 3, 5} at Hier-30 — which also moves the coordinator count 3 → 2 → 1, so
it trades delegation breadth against coordinator parallelism and both must be reported.

**Standing obligation:** every delegation cell (all of E2, the LLM-delegation cells of E1/E4)
must pass `--groups-per-coordinator ≥ 2`. Without it the cell measures nothing, and the
coordinators now say so in their logs.


### 0.7 The plan's fleet sizes did not exist in the generator (found and FIXED 2026-09-08)

Hier-90 and Hier-270 are named throughout this plan — E2, E3b, E4, E7, E8 and the E1 factorial —
and **neither was a supported preset**. `--agents 270` errored out. `--agents 90` was worse: it
fell into the generator's `<= 110` branch, which builds a *110-agent* hierarchy with coordinators
at ids 101–110, and only ids 1..N ever get config files written. A Hier-90 run therefore produced
**90 leaf agents, zero coordinators**, every `parent` pointing at an agent that does not exist,
and no delegation of any kind — with nothing in the output saying so.

Fixed by adding the two presets the plan always assumed, chosen so the ladder scales the group
*count* and not the group *shape*:

| fleet | groups | group size | coordinators | ids |
|---|---|---|---|---|
| Hier-30 | 5 | 5 | 5 | 26–30 |
| **Hier-90** | **9** | **9** | **9** | **82–90** |
| **Hier-270** | **27** | **9** | **27** | **244–270** |

Hier-90's 9 groups are exactly what E2's "Scenario A generalized to 9 groups" always meant. The
generator also now refuses *any* hierarchical fleet size whose topology does not total the
requested agent count, rather than truncating it — that is the general form of the bug, and the
`<= 110` range branch is not the only way to hit it.

With `--groups-per-coordinator 3`: Hier-90 becomes 3 coordinators × 3 groups, Hier-270 becomes 9
× 3 — every coordinator with a genuine choice, verified end to end.

### 0.8 `metrics.json` did not describe the run that produced it (found and FIXED 2026-09-09)

The first two runs on the slice with a coordinator that actually has a choice — `smoke-g2-bandit`
and `smoke-g2-llm`, Hier-30 with `--groups-per-coordinator 2`, ~400 jobs each — **validated P0-1
and invalidated their own metrics.** From the coordinator logs, the LLM delegation plane works on
real hardware: agent-28 `calls=57/57 fallbacks=0 trivial=0 mean=2.626s`, agent-29 `calls=60/61
fallbacks=0 trivial=0 mean=2.689s`, one provider timeout in ~118 calls at `timeout_seconds: 6`.
Agent-30 leads 1 of its 1 group (30 agents = 5 groups, split 2/2/1) and emitted the §0.6
`can never choose` warning as designed, delegating 40 jobs as `trivial`. The LLM arm also *shows*
P0-3's synchronous-call limit as throughput: **315 jobs completed vs the bandit arm's 398**, with
43 still pending, at ~2.6 s of blocking inference per delegation.

Both runs' `metrics.json` were unusable, for two independent reasons:

- **`smoke-g2-bandit`: 1 agent of 30.** `on_shutdown` called `executor.shutdown(wait=True)`
  *before* `save_results`, and since P0-0 made a job sleep its real wall time (capped at 120 s),
  a SIGTERM'd agent sat in the drain while `run_test.py` — which returns from `stop_agents` as
  soon as `pkill` is sent — collected logs and ran the plotting step that reads metrics out of
  Redis. Only the one idle agent had flushed. This was latent before P0-0: flat 1 s sleeps
  usually won the race.
- **`smoke-g2-llm`: 15 of its 16 payloads belonged to the previous run.** Those bandit-run agents
  were never stopped at all (`stop_agents_v2.sh` reports an unreachable host or a missing hosts
  file on stderr, and `run_blocking(check=False)` swallows it), ran on for 19 minutes, and were
  killed by the *next* run's startup — writing their metrics **after** that run had flushed
  Redis. Payloads are keyed by agent id, so run N's numbers silently became run N+1's.

Everything downstream of per-agent metrics was affected: load traces, utilisation, Jain fairness,
`mab_selections`/`mab_rewards`, and the `llm_delegations` counters E4 needs. Job-level metrics
(`all_jobs.csv`, so makespan/throughput/latency in `evaluation/collect.py`) were not.

Fixed on five fronts, all defaults:

1. **Save before draining.** `on_shutdown` saves metrics, drains for at most
   `runtime.shutdown_drain_timeout_s` (20 s, cancelling queued jobs), then saves again. The
   payload no longer depends on the drain finishing.
2. **Run identity.** `run_test.py` mints a `run_id`, exports it as `SWARM_RUN_ID` (inherited by
   local children, re-exported over ssh for remote agents) and records it in
   `<run-dir>/run_meta.json`; agents stamp it on every metrics payload; the plotting step is
   passed `--metrics-run-id` and drops foreign payloads with a warning naming each one.
3. **A stop is not done until the processes are gone.** `stop_agents_v2.sh` waits for each agent
   to exit (`--drain-timeout`, 45 s) before SIGKILL, fans out across hosts in parallel rather
   than serially (45 s × 92 hosts of teardown otherwise), and exits non-zero if any host cannot
   confirm. `run_test.py` reports that, and derives the hosts file from its in-memory host list
   rather than the `agent_hosts.txt` that `cleanup_between_runs` may have deleted.
4. **Reap before flushing.** Leftover agents are stopped at startup *before* Redis is cleared,
   so a straggler's late write cannot land in the new keyspace.
5. **A run that cannot be measured fails.** After stopping, the runner waits up to
   `--metrics-wait-seconds` (120) for every launched agent id to report this `run_id`, then
   writes `<run-dir>/metrics_shortfall.json` and **exits 3**; `collect.py` carries
   `metrics_complete` / `agents_missing_metrics` so a partial cell cannot average in unnoticed.
   A SIGKILL-based failure test declares its silent agents with `--allow-missing-metrics N`.

Regression tests: `tests/test_teardown_metrics.py` (bounded drain, cancelled queue, save-order,
run-id filter incl. the unfiltered default, the completeness gate and its allowance, hosts-file
fallback) and one case in `tests/test_collect.py`. **The smoke pair must be re-run**; no number
from either is citable.
---

### 0.10 The bandit learns from a success signal written before the job runs (found 2026-09-14)

A code review pass (Codex, verified by hand and then on hardware) found that the reward the
delegation bandit receives is decided at *scheduling* time, not at *completion* time.
`schedule_job` persists the job as `COMPLETE` with `exit_status` at its default of 0 and then
submits it to the executor; the leaf writes the real exit status only after the simulated
wall time, up to 120 s later. The coordinator's delegation monitor polls every tick and takes
the first `COMPLETE` as terminal. Failures are therefore visible to the bandit only for jobs
that finish *before* the coordinator's next observation.

Measured on `runs/p11-oracle2` (Hier-30, ε-greedy, the P1-1 validation run):

| | count |
|---|---|
| failures injected at leaves (`[FAILURE_SIM]`) | 83 |
| leaf `[COMPLETE] … FAILED` lines | 83 |
| failures the bandit recorded (`mab_stats` over both coordinators) | **19** |
| successes the bandit recorded | 421 |

Event order for one failed job: scheduled 18:02:46.28 → coordinator logged it complete
18:02:46.54 → leaf actually finished, FAILED, 18:03:06.93. Four of six sampled failed jobs
show the same inversion; the two that were credited correctly ran under a second.

Consequences: (a) every bandit-vs-bandit comparison to date measured how *fast* a group's
jobs finish, not how often they fail — a long-running failure is a success to the learner;
(b) the `CONTEXTUAL_BANDIT_DESIGN.md` §8 deployment validation and the smoke pair inherit
this; (c) the P1-1 oracle was **not** fooled, because it labels decisions from the injected
profile rather than from rewards — which is also why its validation could pass over a blind
learner.

**Fixed the same day (P0-9 in §4).** Three consequences that change how runs behave and how
their numbers compare, all corrections rather than regressions:

1. **A run no longer ends when every job is merely *scheduled*.** `run_test.py`'s early-exit
   poll treats states 8/9/10 as terminal and `RUNNING` is 6, so a cell now waits for execution
   to finish and is longer by up to `wall_time_max_s` (120 s). Same for the `jobs-completed`
   dynamic-agent trigger, which counts state-8 members. Size `--runtime` /
   `--shutdown-after-seconds` accordingly.
2. **Completion % falls in any run that kills agents.** A job whose executor died is now
   persisted `RUNNING` with no `completed_at` and correctly counted as incomplete; before, it
   had been written `COMPLETE` at scheduling time and counted as done. E2b, E6 and every
   failure-injection cell therefore cannot be compared against a pre-fix number.
3. **Latency and makespan were *not* affected.** `Job.execute()` calls `mark_completed()`
   explicitly after the sleep, so `completed_at` always held the real finish time even while
   the state had been flipped early. The damage was confined to what the monitor read.

**Re-run on the fixed revision, 2026-09-15 — the fix is confirmed on hardware.** Same fleet,
same 400-job workload, interleaved placement, clocks checked; the failure profile was restored
into `config_swarm_multi.yml` from `runs/p11-oracle2/run_meta.json` so the arms are comparable
(the base config had been edited for the P0-8 runs in between, which is exactly the hazard
P1-1's `ground_truth` archival exists for):

| run | injected | leaf FAILED | bandit failures | bandit successes | leaf SUCCESS |
|---|---|---|---|---|---|
| `p11-oracle2` (before) | 83 | 83 | **19** | 421 | 358 |
| `p11-oracle4` (after) | 76 | 76 | **122** | 304 | 325 |
| `smoke-g4-bandit` (after) | 56 | 56 | **55** | 312 | 314 |
| `smoke-g4-llm` (after) | 108 | 108 | **108** | 230 | 230 |

`p11-oracle4`'s 122 is exactly 76 exit failures **plus 46 delegation timeouts**, which the
bandit is supposed to count as failures; `smoke-g4-llm` agrees to the job (338 outcomes for
338 leaf completions). All three re-runs reported 30/30 agents with no metrics shortfall, and
the oracle scored `p11-oracle4` end to end: **488 decisions, 0 unscored, routing accuracy
0.822** (was 0.794 on the blind run), regret 117.6, and `--validate` agreeing to **0.041**
job-weighted error over 436 jobs with no type beyond sampling noise.

The LLM arm also confirms P0-1 still works on the fixed code: coordinators 28 and 29 made
106/107 and 116/116 real ranking calls, zero fallbacks, mean 2.6 s, while coordinator 30 leads
one group and logged the §0.6 `can never choose` warning for all 121 of its trivial
delegations — the expected `[2,2,1]` shape for five groups at `G=2`.

**A further defect the verification exposed, now FIXED (2026-09-15):** a job in state
`FAILED` yielded no bandit outcome at all. `_restore_infeasible_jobs` marks a job `FAILED`
once it exhausts `max_infeasible_retries`; the delegation monitor's terminal test was
`COMPLETE` only, so such a job fell into the in-progress branch, waited out
`delegation_timeout_s + delegation_exec_grace_s` and was dropped uncounted — 2 of 400 in
`smoke-g4-bandit`, which is the whole of that arm's gap between 56 injected failures and 55
recorded. A group that *cannot run* a job is precisely the routing signal C1 is about, so
`FAILED` is now terminal and reported as a failure immediately.

**Observed and deliberately NOT changed:** with the bandit disabled, delegation fans out to
every capable group and each group runs the job. Seen in `p11-oracle3` (run before the profile was restored): 400 distinct jobs,
   **952 leaf completions, up to 6 executions of one job** across different groups. Exactly-once
   holds *within* a group — the Redis CAS is scoped per level and group — so this is redundant
execution by design, not a safety violation. But it means completion counts, throughput and
makespan from any fan-out-to-all cell are inflated, which matters if `all` is ever used as
F4's stand-in baseline (`CCGRID27_PAPER_PLAN.md` §4 already says not to call it `static`).
Whether redundant delegation *should* duplicate execution is a design question for the
hierarchy, not a defect to patch mid-campaign, so it is recorded rather than changed.

**Update 2026-09-18 — the calculus above no longer holds.** It rested on G=1 being the default,
so that a no-bandit coordinator had one group and the fan-out was inert. `--groups-per-coordinator`
now **defaults to 2**, so every analytic hierarchical run — E0, the analytic half of E1′, and
every journal cell with `mab.enabled: false` — duplicates every job across two groups unless it
passes `--groups-per-coordinator 1` explicitly. That is now a paper-blocking decision, not a
recorded curiosity: `CCGRID27_PAPER_PLAN.md` §4 (2026-09-18 row) gives the two options — pass
G=1 on every analytic cell, or make the no-bandit path pick one group at random (the LLM path's
own fallback already does exactly this) — and argues for the second. It has to land before E0
either way, because the reference cell is what every other number is compared against.
`run_meta.json` records the fan-out and coordinator type *observed* in the generated configs
(since 2026-09-18), so a cell that got the wrong one is detectable; check both at E0.

### 0.12 Code review of 2026-09-18 — see `docs/CODE_REVIEW_2026-09-18.md`

A read of the code base end to end. Three findings change campaign numbers and are owed before
E0, all recorded in `CCGRID27_PAPER_PLAN.md` §4: PBFT stragglers after quorum re-finalize and
re-broadcast COMMIT (the 2026-09-15 fix undone by its own `_forget_object`; demonstrated in
`tests/test_pbft_stragglers.py`); hierarchical latency columns exclude the coordinator tier by
construction; the selection startup barrier has no timeout. Journal-side: `LlmAgent.selection_main`
has no DAG gating and no infeasible handling; SWIM false-fails silence consensus traffic to live
peers under `broadcast()` although the docs call it advisory; `selection_threshold_pct` is inert.

### 0.11 Five more defects from the same review pass, all FIXED 2026-09-15

None was found by a failing run; all five bias a number one of the papers reports. Pinned by
`tests/test_review_fixes.py` (18 cases, 9 of which fail against the pre-fix tree, and 2 more
against the first, flawed version of fixes 3 and 5 — both caught by the stop-time review gate,
and both the same mistake in different clothes: a decision taken from state read outside the
step that changes it).

**Status of the three slice runs against these fixes.** `p11-oracle4` and the `smoke-g4` pair
were run *before* this group landed, so they remain valid as the P0-9 verification (none of
these five touches how an outcome is reported) but are **not** campaign baselines. E5 in
particular must be measured on the fixed PBFT engine.

| # | Defect | Which number it moved |
|---|---|---|
| 1 | **PBFT proposers re-broadcast COMMIT on every PREPARE past quorum.** The "already committed" guard asked `incoming.contains(...)`, which is false for the proposer's *own* proposal because that lives in `outgoing`. Now keyed on a `_commits_sent` set of `(object_id, p_id)`, released with the object — so adopting a better proposal and re-proposing after a reselection timeout both still commit | Up to `n - quorum` redundant COMMITs per job, inflating **E5/F2 messages-per-job against PBFT** — the direction that flatters this work's own argument. E5 must be measured on the fixed engine |
| 2 | **Snow's `finalized_count` / `abandoned_count` / `sends_dropped` were plain `+=` across the finalize pool and the driver thread.** Now under a dedicated `_stats_lock`, separate from `_lock` so an increment is never taken across a host callback | Undercounts finalizations and shed sends in exactly the saturated runs F1/F2 are about. The race is probabilistic under the GIL, so the tests assert the lock is *taken*, not that a count was lost |
| 3 | **A Snow finalize whose CAS or callback raised was counted nowhere.** `_finalize` has already marked the state finalized and dropped the object, so the decision was neither `finalized` nor `abandoned`. New `finalize_errors` counter, reported in `consensus_stats()`. The outcome is now counted **once, at the end** of `_finalize_work_inner`: the first version of this fix incremented `finalized_count` before the host callbacks, so a decision whose callback raised reported `finalized=1, finalize_errors=1` and broke the very invariant the counter exists for. The round/query/latency summaries moved with the count, so they describe the population they are counted with | `finalized + abandoned + finalize_errors` is now the whole population. A non-zero third term means assignments were lost — a correctness signal that used to be a warning line |
| 4 | **An unknown consensus engine name silently became PBFT.** `consensus.protocol` and both `hybrid.{level0,coordinator}` names are now validated, and an unknown one raises | A typo made a cell labelled Snow or Hybrid run PBFT throughout, with nothing in the run saying so — it would have invalidated protocol attribution in E1 |
| 5 | **SWIM's `_indirect_acked` never shrank** — `on_ack` pops the probe, so the timeout sweep never reached the flag, and the relay-forwarding loop walked every entry ever created on every tick. The cleanup takes the **whole decision under the lock on live flags** and claims the entries it will forward there: the first version decided from a snapshot, and since `on_ack` pops the probe and sets the flag in one step, a snapshot taken a moment earlier reads "not acked, probe gone" — the orphan case — so it deleted a *fresh* ack and its relay destination and the initiator suspected a live peer | Failure-detection cost grew with uptime on exactly the long unattended runs the campaign is made of; the snapshot version would have dropped acks and caused false suspicions |

Plus two harness fixes: `batch_tests_v2.py` **floored** the auto-generated host count where
`run_test.py` and `generate_configs.py` **ceil** it (270 agents at 4/VM asked for 67 hosts and
needed 68, so the run died *after* Redis had been flushed), and it could not forward
`--expect-silent-agents`, so a batched failure-injection campaign could only check *how many*
agents were silent, never *which*.

**Both findings from that pass are now closed**, the second as a defect and the first as what
it always was — a design decision, which is now named and measured rather than hidden:

- ~~**`scheduling_main`'s `# TEMP HACK`**~~ **NAMED AND MEASURED 2026-09-16.** The behaviour
  is unchanged by default, because changing it would move every hierarchical number; what
  changed is that a run now says which regime it was in. It is the config key
  `job_selection.coordinator_cost_matrix`: `self` (shipped, and the behaviour behind every
  hierarchical number measured so far) scores the coordinator against itself alone, so every
  coordinator holding a job proposes itself and the tier runs **one consensus decision per
  coordinator per job**; `peers` scores the coordinator peers exactly as level 0 does, for one
  proposal per job at the tier. An unknown value **raises** rather than defaulting, for the
  same reason `consensus.protocol` does — the two regimes differ by a factor of the
  coordinator count in proposals per job, so a typo would produce a cell nothing in the run
  could label. `peers` is **an E5 arm, not a fix**: the plumbing behind it already exists and
  is dead code under `self` (a parent publishes its subtree's aggregate capacities,
  allocations and load in its own `AgentInfo`, and `is_job_feasible` prices a peer parent off
  `max_child_capacity`), so switching it on makes that optimistic estimate load-bearing, with
  the delegation timeout as its failure mode. **The separability E5 needed is the measurement,
  not the key:** agents count proposals per tier and `collect.py` reports
  `proposers_per_job_l1` — 1.0 means the tier ran one decision per job, the coordinator count
  means every coordinator proposed itself. Counted as **distinct jobs**, never per loop pass
  (`gets()` peeks, so an unplaced job returns ~2x/s), with re-proposals in their own column so
  a reselection timeout cannot move the ratio for a reason unrelated to tier width. §III must
  still describe the behaviour, but it can now cite a number for it.
  Tests: `tests/test_coordinator_matrix.py`.
- ~~**Failed-agent reassignment only searches the local pending queue**~~ **FIXED
  2026-09-15.** A job already READY or RUNNING on a failed agent was logged as "likely
  completed" and dropped, and since its persisted state is not PENDING nothing re-added it —
  the work was stranded for the rest of the run. P0-9 widened that window from milliseconds to
  the job's whole duration. Recovery needed three things at once: candidates sourced from
  Redis (the job record's `leader_id`, not the local map); **release of the exactly-once
  claim**, which `try_claim_assignment` sets and nothing ever deleted, so a re-finalization
  returned the dead agent forever and reassignment was impossible under Snow regardless of the
  rest; and peers discarding the job from their consensus dedupe set when it returns to
  PENDING, or every commit for it is skipped. One reassigner per (job, failure) via a
  TTL'd `SET NX`. **E2b and E6 measure exactly this path, so no pre-fix failure-injection
  number describes the system.** Tests: `tests/test_failed_agent_reassignment.py`.

Two smaller findings from the same pass that touch conference figures, recorded here and in
`CCGRID27_PAPER_PLAN.md` §4:

- **PBFT proposers re-broadcast COMMIT on every PREPARE past quorum.** The "already
  committed" guard in `ConsensusEngine.on_prepare` checks `incoming.contains(...)`, which is
  false for the proposer's own `outgoing` proposal, so each extra PREPARE re-appends the
  proposal and sends another COMMIT. Inflates PBFT `sent_commit` by up to (n − quorum) per job
  — the E5/F2 messages-per-job curve is biased *against* PBFT. Fix the guard, re-measure E5.
- ~~**Coordinator-tier selection is self-only.**~~ **NAMED AND MEASURED 2026-09-16**, see the
  first bullet above. The default is unchanged (`job_selection.coordinator_cost_matrix: self`),
  `peers` is the arm that changes it, and `proposers_per_job_l1` from `collect.py` is what
  separates "n proposals per job" from PBFT's own cost in the Hier-250 collapse. Both numbers
  come out of the same E5 runs; no extra cell is needed to tell them apart.

### 0.9 The work splits into two papers (decided 2026-09-10)

C1 (bandit delegation) and C3 (Snow consensus) go to **CCGrid 2027** — abstract 24 Nov 2026,
paper **1 Dec 2026** (corrected 2026-09-14 from 8 Dec), 10 pages including references, double-blind. C2 (LLM coordinators) and the
quantum-hybrid strand stay here for **FGCS, 31 Jan 2027**. The conference plan is
`docs/CCGRID27_PAPER_PLAN.md`; **this document stays the master** for substrate rules, P0 code
work, metric definitions and the experiment matrix, and the conference paper draws cells from
it rather than redefining them.

Why the split falls here, and why Snow goes with the bandit rather than staying:

- Both are **code-complete and deployment-validated**; the LLM plane still has P0-3 (no
  wall-clock deadline on an inference call) and P0-8 open. The split is along what can be
  measured at the 12 Oct freeze.
- The strongest single result either half has is **joint**: the Hier-250 PBFT collapse
  (3.5-4.2% completion) removed by swapping the coordinator tier to Snow. A bandit-only paper
  is a component study; a Snow-only paper is an engineering swap with a message-count table.
- CCGrid's 10 pages, which §0.4 rejected as too tight for the full story, is right-sized for
  two claims. Its deadline also **precedes** the journal's, so the two do not collide.

**What the split costs, stated plainly: the interaction result stays with the journal.** §1's
novel claim is that *expensive* decision-making changes which consensus protocol you should
run, and the LLM is what makes a decision plane expensive — a LinUCB update is microseconds.
The conference paper therefore cannot carry that claim. It gets a smaller, still-real one: Snow
finalizes faster, so a coordinator's `GroupSnapshot` context is younger when it decides, so
bandit regret should fall under Snow at equal scale. **That effect may be null**, and the
campaign has a gate on 10 Nov to find out (`CCGRID27_PAPER_PLAN.md` §6).

**Caveat found 2026-09-14, and it applies to the journal's composite claim too:** the context
*age* P0-4 records cannot vary with the consensus protocol. `received_at` is stamped only when
`_refresh_neighbors` reads a fresher child `AgentInfo` from Redis, on the full-refresh cadence
(5 s with gossip on, the default); the gossip overlay updates `load` without touching the
stamp; no consensus code path is involved. The slice numbers already look like a refresh
period (p50 0.25–0.44 s, p95 7–8.7 s). Age is a validity column; the quantity that *can* move
with finalization speed is context **error** — `GroupSnapshot.inflight` versus the true
in-flight count reconstructed offline from `all_jobs.csv` at the decision timestamp. That needs
the snapshot's per-candidate `inflight` on the `DecisionRecord` (it carries candidates, selection
and ages, not the values the policy saw) plus the offline join in `collect.py`, and is owed to
the conference paper before the freeze
(`CCGRID27_PAPER_PLAN.md` §1, §4). The journal's F2 inherits the same caveat when it argues
mechanism rather than outcome.

Two consequences for this plan:

1. **P0-4 must instrument context age at decision time.** Without it the interaction cannot be
   measured for either paper, and it is the one metric neither paper's figure list can source
   from existing instrumentation.
2. **The journal's §1 thesis is unchanged and still needs C1 and C3 as supporting material.**
   FGCS gets the full three-claim composite; it cites the conference paper for the C1/C3
   mechanisms and re-uses the same campaign runs rather than re-measuring
   (`swarm-no-cross-substrate-result-reuse`). Under both venues' overlap policies the journal
   must *extend*, so its contribution is C2 plus the composite interaction — not a longer
   retelling of C1 and C3.

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
3. Bandit evaluated only at 30 agents, ≤2 runs per config in places, ~~no oracle upper bound~~
   (P1-1, done), **no static-heuristic baseline (still true — no static/greedy delegation policy
   exists in code; `mab.enabled: false` is fan-out-to-all, recorded as `all`)**, and never
   combined with LLM coordinators. LinTS exists (`mab.algorithm: lin_ts`).
4. ~~Snow has no message-count / bandwidth instrumentation~~ **Done (P0-4, 2026-09-14)** — counted
   in the transport under a lock, by direction and type.
5. No exactly-once safety stress at scale; reviewers already flagged the safety argument as informal.
6. Decision-quality metrics are thin everywhere: completion + selection time only. Need makespan,
   Jain's fairness, data-locality hit rate, and delegation regret.

**Mechanism gaps (code work, §4)**
7. ~~There is **no LLM-based delegation** today~~ **Done (P0-1, 2026-09-08)** — `delegation.policy:
   llm` puts the model in the child-group routing seat; before that "LLM coordinator" meant
   "coordinator with an LLM-scored bid".
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
| **P0-8** ✅ | **Designated bidding is measurable (done 2026-09-14).** The two items this row asked for — per-job counters and a deadline sized to the designee's window, not to one bid — were **already implemented** by the chaos-branch port: `_designate_bidders` uses a per-job deadline (`designation_deferred_at`, 30 s) and its docstring argues why a retry counter cannot work. Three other candidate causes were checked and ruled out rather than assumed: the designation arithmetic is linear and negligible (0.57 ms at 30 agents → 4.18 ms at 270, against a 500 ms loop), the deadline state survives because `gets()` returns live `Job` references and nothing replaces them, and an agent *is* in its own `neighbor_map` (it would otherwise never designate to itself and every job would wait out the deadline). **What was actually missing is the measurement**: nothing counted how many agents paid for a bid on the same job, so "3.7 bidders/job → 1" was unfalsifiable, and the mode's failure — the liveness deadline firing on everything, so every agent bids anyway — produced artefacts identical to success. **As landed:** each agent records the distinct jobs it paid an LLM bid for and its designation outcomes; `collect.py` reports `bidders_per_job` (fleet sum over the jobs the run *saw* — dividing by the declared count would make a stalled run look well partitioned) with `designate_forced_share` beside it. Three counting traps, each found by review and each biasing the flattering way: **(a)** `gets()` is a non-destructive peek, so `+= len(pending_jobs)` counts job-*passes* — `deferred` inflated ~60× and a fleet that is really 50% fallback reported 67%; counters are distinct-job sets now. **(b)** `mine` and `forced` overlap across reselection rounds (a job that loses consensus returns to PENDING and can take both routes), so the share denominator is their **union**, not their sum — summing reports 0.50 where every job hit the fallback. **(c)** `_note_bid` sat inside the bid path's `try`, whose `except` falls back to the analytic cost: an error there would have silently turned working LLM bids into analytic ones and moved placement, so it is non-raising. Retry cost stays visible as `bid_calls` − `bid_jobs`. Tests: 10 added to `test_designate_bidder.py`, 6 to `test_collect.py`. | `llm_agent.py` `_designate_bidders`, `collect.py` | 1 d |
| **P0-1** ✅ | **LLM group delegation (done 2026-09-08).** At a coordinator, prompt over child-group summaries and return a ranked group choice + rationale. **As landed:** `delegation.policy` (`bandit` default = the pre-existing behaviour, `llm` = the model decides), with the decision point extracted into one overridable `ResourceAgent._select_child_groups` so both policies see the identical candidate list — the arms of E4 differ in the decision *rule*, not in what they were told. The summary the model gets is the **same `GroupSnapshot`** the contextual bandit gets, via a new public `MABManager.snapshots_for`, so headroom, inflight and the failure/timeout history are common to both. Candidate ids go in the **JSON schema** as a `Literal`, not just the prompt — the `score_scale` lesson again, since Ollama's `NativeOutput` generates from the schema and would happily answer `[0,1,2]` for groups 3, 7, 11 — and the answer is sanitised anyway (unknown ids dropped, de-duplicated, short rankings completed from the candidate order). Every failure path falls back to `bandit` **for that job**: provider error, `llm.timeout_seconds` breach, a ranking naming no group we offered, or a delegator that could not even be built. A decision that cannot change the outcome (one candidate, or `top_k ≥ len(candidates)`) spends no inference and is counted as `trivial`. Under `llm` the bandit still receives outcomes from the delegation monitor, so `MABManager.record_external_selection` hands it the **decision-time** context (not a rebuild after a multi-second call) — otherwise its arm counters would climb while the contextual model silently skipped every update, describing a run it did not steer. Delegation is **not** paced (P0-7): one coordinator owns its job, so a fast fallback out-races nobody. Audited to `llm_score:delegate:*`; `metrics.json` gets `llm_delegations` + `llm_delegation_stats`, kept apart from `mab_selections`; `[STATS]` reports calls/fallbacks/empty/trivial/mean latency. Two review findings worth recording because both are the campaign's recurring failure shape — *the fallback path is cheaper than the path it stands in for*: a failed decision originally widened the fan-out to every capable group (failing rewarded with the whole subtree, the fan-out analogue of §12.2's race-to-propose), and a timed-out call was charged nothing, which would have made `mean_s` cheapest in precisely the fallback-heavy runs E4 exists to price. Both fixed; the fallback now keeps the configured fan-out and picks at random when nothing ranks. **Two open limits, both P0-3's:** the call is synchronous on the coordinator's scheduling thread, so its delegation rate is capped at 1/latency; and `llm.timeout_seconds` is per-request, so provider and output-validation retries can stack several into one decision — there is no wall-clock deadline. Tests in `tests/test_delegation.py`. | `swarm/agents/llm/llm_delegator.py`, `resource_agent.py` `_select_child_groups`, `mab_manager.py`, `config_swarm_multi.yml` | 3–4 d |
| **P0-9** ✅ | **The delegation bandit was rewarded before the job ran (found and FIXED 2026-09-14, §0.10).** `ResourceAgent.schedule_job` sets `job.state = COMPLETE` and persists it *before* `executor.submit(execute_job)`; `execute_job` persists again with the real `exit_status` after the (up to 120 s) simulated sleep. The coordinator's `_monitor_delegated_jobs` runs every tick and, with MAB on, treats the first `COMPLETE` it sees as terminal: `exit_status` is still its default 0, so it reports success to the bandit, removes the job from `delegated_jobs`, and never reads the real outcome. **As landed:** `schedule_job` persists **`RUNNING`** and only `execute_job` may write `COMPLETE`. The local `completed_jobs_set` is still updated at scheduling, because it is the *consensus* dedupe set and the job is genuinely out of election — the two meanings of "completed" were the trap, and they are now separated by which one is persisted. The periodic Redis state scan takes `RUNNING` alongside `COMPLETE` for the same reason, so a peer's scheduled job still leaves everyone's pending queue. The monitor no longer drops an in-progress job when `delegation_timeout_s` expires: that key bounds *selection*, and a scheduled job legitimately runs past it for its whole simulated wall time, so it waits for the `COMPLETE` that carries the real `exit_status` and gives up only after a new `delegation_exec_grace_s` — which **defaults to `Job._WALL_TIME_MAX_S`**, read from the class rather than re-read from the config, so the execution budget and the wait for it cannot drift apart ([[swarm-one-key-one-default]]). A job dropped on that grace reports **no** outcome rather than a fabricated failure: the child died mid-run, which says nothing about the group's failure rate. **No bandit success rate measured before this fix is citable** (Scenario A/B/C in `CONTEXTUAL_BANDIT_DESIGN.md` §8 included). Verified three ways: 10 tests in `tests/test_reward_timing.py`, 5 of which fail against the unfixed agent; the full suite at 567; and the `RUNNING`→`COMPLETE` secondary-index transition exercised against a real Redis. | `resource_agent.py` `schedule_job`, `_monitor_delegated_jobs`, state scan, `delegation_exec_grace_s` | 1 d |
| **P0-2** | **Bandit×LLM composition.** Config `delegation.policy` already exists (P0-1) with `bandit` and `llm`; this adds the third mode `bandit_gated_llm` — bandit narrows `capable_groups` → top-m, LLM ranks those m, and the LLM's pick is the arm the bandit is rewarded on. The reward path is in place: `record_external_selection` already lands an LLM-routed outcome on the right arm with its selection-time context. One key, one place — do **not** add a second switch. | `mab_manager.py`, `resource_agent.scheduling_main` | 3 d |
| **P0-3** | **Decision cache + inference budget.** Cache LLM verdicts keyed by (job-type signature, coarse group-state bucket) with TTL; hard cap on in-flight inference per coordinator, fall back to bandit/analytic when exceeded. Emit hit rate + fallback counts. | `llm_delegator.py`, `llm_bidder.py` | 2–3 d |
| **P0-4** ✅ | **Instrumentation (done 2026-09-14).** Per-agent consensus message counts & bytes, rounds-to-finalize, LLM calls/tokens/latency/fallbacks, delegation decisions with the fields the oracle labels offline, **plus `GroupSnapshot` age at decision time** — the conference paper's only interaction claim, which no existing counter could source. **As landed:** one module, `swarm/utils/instrumentation.py`, holding every container, so `metrics.json` and the Prometheus export render from the same dicts and cannot drift. Config block `instrumentation:` (`decision_log_max`, `textfile_dir`, `textfile_period_s`); `--textfile-dir` on `generate_configs.py`/`run_test.py` so a Grafana-instrumented campaign is not selected by a hand edit. Details worth recording, each of them a way the numbers could have been quietly wrong: **(a)** age is taken at the **decision**, not at the snapshot build — under `policy: llm` those are seconds apart and freezing it at build would define the effect away; `decide_s` is stored alongside so the age at build is recoverable. **(b)** `GroupSnapshot` carries the newest *and* oldest child stamp: a group of nine where one child went quiet is genuinely fresh on one and stale on the other. **(c)** An unknown age (no live child, or `last_updated` still 0.0) is counted as unknown and never substituted with a large one — `AgentInfo.last_updated` defaults to 0.0, so the naive reading is a 56-year age landing in the p99, exactly where the dead groups are. **(d)** A negative age is clock skew, not a measurement: clamped to 0 and counted in `ctx_skewed_ages`, which is a *validity* column — non-zero means the run's whole age distribution is suspect. **(e)** The decision record is written in a wrapper around `_select_child_groups`, not inside it, because `LlmAgent` re-enters the bandit on five fallback routes; a fallback is recorded as `bandit`, and a fan-out covering every candidate as `bandit_all`/`all`, so inert delegations never appear as decisions. **(f)** The two new `GroupSnapshot` fields are provably invisible to `ContextExtractor` — a changed `schema_version` discards every persisted LinUCB model. **(g)** Messages are counted in the transport, the one funnel every message passes through exactly once, under a lock (`d[k] = d.get(k,0)+1` loses increments across the 16 broadcast workers, i.e. undercounts in precisely the saturated runs the figure is about); a shed fan-out send counts as `dropped`, never as `sent`. **(h)** PBFT's finalize time is measured at the *proposer* only, the same vantage point Snow measures from, and a re-proposal restarts the clock (otherwise a 60 s reselection timeout lands in a sub-second distribution); PBFT deliberately emits no `rounds_*`, since a column of 3s would invite a meaningless cross-protocol comparison. **(i)** Quantiles come from a uniform reservoir sample, not a head or tail. **(j)** Failed and timed-out LLM calls are counted *and timed* — the same trap `delegation_stats.mean_s` already avoids. Collector: `evaluation/collect.py` gains the summary columns (fleet **sums**, not per-agent means — the denominator would vary with metrics completeness) and writes `decisions.csv`, one row per delegation, which is the tidy CSV F6 plots from and P1-1 joins against. Tests: `tests/test_instrumentation.py` (27), plus 10 in `test_delegation.py`, 5 in `test_collect.py`, 1 in `test_broadcast.py`. | `swarm/utils/instrumentation.py`, `grpc_transport.py`, `grpc_server.py`, `gossip_engine.py`, `engine.py`, `resource_agent.py`, `llm_agent.py`, `llm_bidder.py`, `llm_delegator.py`, `rl/context.py`, `rl/mab_manager.py`, `evaluation/collect.py` | 2 d |
| **P1-1** ✅ | **Oracle / offline-optimal delegator (done 2026-09-14).** Replays each delegation against the injected per-group failure profile and charges the gap as regret, scoring the rows P0-4's `decisions.csv` already carries; `collect.py` adds `regret_total`, `routing_accuracy` and `regret_ctx_age_corr` to the wide row, so **F6 is one collector run** rather than a manual join. The ground truth is resolved with the agents' own `_select_failure_phase`/`_resolve_failure_rate` — a second implementation would drift and make every regret number quietly wrong. **Everything it cannot know it refuses rather than approximates**, which is not a style choice: four separate times a missing input got a plausible substitute, and every substitute biased regret *downward*, the direction that flatters the policy and so never shows up in a figure. **(a)** The profile is archived in `run_meta.json` (`ground_truth`) because the base config is edited between arms; a run without it is refused rather than scored against today's config. **(b)** Phases resolve on each **member agent's own** `failure_sim_start` (now in the metrics payload), never the coordinator's or the run's, and there is **no fallback clock**: `after_s` counts from agent construction, a 30-host launch spreads that over a minute, and a member read as post-flip when it is pre-flip prices a bad group as good — exactly around the mid-run flip E2a is built on. **(c)** A decision is scored only if **every** candidate can be priced; pricing a subset and taking the best of it records a bad choice as `regret 0, optimal` when the genuinely best group is the one that dropped out. **(d)** A decision whose candidates are all equally good is not graded. **The model is checked, not trusted**: `--validate` joins predicted failure against real exit statuses on `job_id` (**not** `job_type`, which `all_jobs.csv` does not carry — a type-keyed join matches nothing and reports a clean bill of health for a check that never ran), gated on the job-weighted error and on per-type deviations beyond 2 SE. That SE is the **Poisson-binomial** one over the individual predicted rates, not the binomial at their mean: the trials are not identically distributed, and the mean-p form overstates the variance and accepts outcomes the profile rules out — at the extreme, a profile under which half the jobs cannot fail and half cannot succeed pins the observed rate exactly, yet the mean-p band would accept ±0.10. **Validated on hardware** (Hier-30, ε-greedy, contrast *within* each coordinator's candidate set — a profile varying *between* coordinators gives every candidate set one rate and reports a meaningless `routing_accuracy` of 1.0): 480 decisions scored, 0 unscored, 457 with a material choice, regret 131.6 (mean 0.274), **routing accuracy 0.794**, validation agreeing to **0.018** job-weighted error over 465 jobs with no type beyond sampling noise. Tests: `tests/test_oracle.py` (33) + 3 in `test_collect.py`. | `evaluation/oracle.py`, `collect.py`, `run_test.py`, `resource_agent.py` | 2 d |
| **P1-2** ✅ | Analysis harness (done; extended by P0-4/P1-1/P0-8 columns): one script that walks a run tree → tidy CSV (config × run × metric), so every figure is regenerable. | `evaluation/collect.py` | 2 d |
| **P1-3** ✅ | Site-aware config generation (done — `make_agent_hosts.py --sites-out`; **but nothing joins `agent_sites.txt` to `collect.py` or measures RTT, see E3a**) for 17 sites (`--agent-sites-file`) so Snow's `local_sample_frac` is meaningful and figures can be grouped by site. | `generate_configs.py` (§Part C of `CONSENSUS_SCALING_PLAN.md` — partially done) | 1 d |

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

**Paper ownership (added 2026-09-10, §0.9).** One campaign, two papers, and every cell belongs
to at least one of them:

| Experiment | CCGrid (C1/C3) | FGCS (C2 + composite) |
|---|---|---|
| E0 substrate re-baseline | shared | shared |
| E1 consensus x decision plane | `{analytic, bandit}` columns | `{LLM, bandit-gated-LLM}` columns + the full crossing |
| E2 delegation quality | **owns it** | cites it |
| E3a per-RTT-bin (analysis over E1) | shared | shared |
| E3b netem overlay | — | owns it |
| E4 cost of reasoning | — | **owns it** |
| E5 coordination overhead | shared | shared |
| E6 safety and correctness | shared | shared |
| E7 external baselines | **required** (see §6) | shared |
| E8 decision-plane resilience | — | **owns it** |

Cells marked shared are run **once**, on the frozen revision, and appear in both papers with
the conference paper cited. Nothing is re-measured between submissions.

Common workload: Pegasus-derived jobs, ~20 jobs/agent, generated by duplicating the 547-job base
(`duplicate_jobs.py`). `run_test.py` sizes hosts as `math.ceil(agents / per_host)`, so a
non-multiple is legal and the last host carries the remainder (270 at 4/VM → 68 hosts, two
agents on the last). Prefer exact multiples anyway: an uneven last host is one more thing to
disclose.

**Scale ladder** (re-revised 2026-09-14: **PSC is back, 92 of 92 VMs up**, so the original
~90-host sizing is reachable again; the 2026-09-10 83-host sizing is kept as the fallback):

| Agents | Agents/VM | Hosts | Jobs | Notes |
|---|---|---|---|---|
| 30 | 1 | 30 | 600 | Matches eScience Hier-30; cheap, use for sweeps |
| **90** | 1 | 90 | 1800 | **Primary operating point.** 9 groups of 9 + 9 coordinators; E2 uses `--groups-per-coordinator 3`. Fallback if the fleet drops below 90 before E0: **Hier-80** (8 groups of 9 + 8 coordinators, same group shape) at 1/VM |
| 180 | 2 | 90 | 3600 | Flat-PBFT livelock regime. Fallback: 3/VM on 60 |
| 270 | 3 | 90 | 5400 | The collapse point. Fallback: 4/VM on 68 |

**Pick the rung set at E0 from a fresh `make_agent_hosts.py` sweep and do not change it
mid-campaign.** The fleet lost two sites in one week in September; a ladder that mixes the
primary and fallback sizings is not a scaling curve.

Two placement rules, both load-bearing and both newly enforced:

- **Report agents-per-VM per rung in every results table.** Co-locating agents turns
  inter-agent messages into loopback, which flatters exactly the coordination cost C3 measures.
  A ladder that mixes 1/VM and 4/VM without saying so is not a scaling curve.
- **Generate the hosts file with `make_agent_hosts.py` (site-interleaved, the default).**
  Placement follows the order of `agent_hosts.txt` and agent ids map to sites in contiguous
  blocks, so the numeric ordering used by every run before 2026-09-10 left **65 of 79 adjacent
  agents at the same site** — a hierarchical group's consensus traffic never crossed the WAN.
  Interleaved is 0 of 79. Every WAN claim in either paper must come from interleaved runs;
  `--order sequential` is for a deliberate site-outage scenario only.

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
**Hier-90 with `--groups-per-coordinator 3`** — 9 groups of 9, three coordinators leading three
groups each, so every coordinator has a real routing decision (§0.6). Hier-90 only became a
buildable fleet on 2026-09-08: see §0.7. At G=1 there is one candidate group and nothing to
learn, so every arm below would score identically; **G is the second axis of this experiment**,
sweep G ∈ {3, 9} (3 coordinators, then 1) and report the coordinator count alongside it, since
raising G trades delegation breadth against coordinator parallelism.

Arms: `{epsilon-greedy, UCB1, LinUCB, LinTS}` under the context-dependent failure profile
(Scenario A generalized to 9 groups), scored offline against the **oracle** (P1-1 — it is a
label, not a runnable arm). **`static-greedy` is named here and in the conference F4 but does
not exist in code**: there is no static or greedy delegation policy; `mab.enabled: false`
delegates to *every* capable group and is recorded as `all`. Either implement a static
least-loaded/round-robin policy before the freeze or drop the arm — do not relabel `all` as
static (`CCGRID27_PAPER_PLAN.md` §4). Then the two stressors, now at scale:
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
  can be re-analyzed for free — do E3a as analysis over E1 data before spending any run on E3b).
  "First" here means *before E3b*, not early in the calendar: E3a cannot start until the E1 cells
  exist (weeks 6–11), and it needs AMST up to have a transatlantic bin at all (§9, §10).
- **Tooling gap (2026-09-14):** nothing in the tree measures RTT. There is no per-site RTT
  matrix captured into the run dir, `collect.py` has no `site`/`rtt_bin` column, and
  `agent_sites.txt` is joined to nothing. "Analysis only" describes the runs, not the code —
  E3a needs (a) a ping or gRPC-echo matrix from each coordinator host to every agent host,
  captured per run, and (b) the join in `collect.py`. Owed to the conference paper (its F3) before
  the 12 Oct freeze.

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
- **Tooling gap (2026-09-14):** no partition driver exists. The only iptables/netem in the tree is
  a netplan firewall setup script; blackholing one site group from another and healing it on a
  timer is a small script, but it has to be written and rehearsed at Hier-30 before the freeze.
- Repeats: 3.

### E7 — External baselines (see §6)
Centralized greedy / round-robin / random with remote execution workers, plus a **sampling-based
(Sparrow-style) decentralized baseline** at Hier-90 and 270. Metrics: makespan, selection latency,
fairness, and behavior under the same failure injection.
- **Not implemented (checked 2026-09-14):** `baselines/scheduler.py` has `GreedyScheduler`,
  `RoundRobinScheduler`, `RandomScheduler` only; no file mentions Sparrow or late binding. §6
  calls this non-negotiable and the conference paper needs it in T2 — it is a pre-freeze code
  item (`CCGRID27_PAPER_PLAN.md` §4), not an experiment-week task.
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
(Oct 13 – Nov 30) is sized for. **This is the reason to freeze the code on Oct 12.**

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

## 9. Schedule — one campaign clock, two submission windows (revised 2026-09-14)

**The conference calendar is in `CCGRID27_PAPER_PLAN.md` §6 and is not repeated here.** What
this section keeps is the shared campaign clock both papers run on, and the journal-only window.
Dates that appear in both files have been wrong in both before (8 Dec for a 1 Dec deadline), so
each date now has one home.

**Conference cells** (`{analytic, bandit}` only): E0, E1′, E2, E3a, E5, E6, E7 — owned and
scheduled by the conference plan. **Journal-only cells:** E1's LLM columns, E3b, E4, E8. Shared
cells are run once and nothing is re-measured between the two submissions.

| Wk | Dates | Milestone | Gate |
|---|---|---|---|
| 1 | Sep 8–14 | ~~Pre-campaign fixes~~, ~~P0-5~~, ~~P0-1~~ (Sep 8); ~~metrics attribution (§0.8)~~, ~~Hier-30 smoke~~ (Sep 9–10, `runs/smoke-g3-*`, 30/30 reporting); ~~P0-4~~, ~~P1-1~~, ~~P0-8~~ (Sep 14). Fleet clocks repaired (`fix_slice_clocks.sh`); PSC back, 92/92. | Smoke green ✅ |
| 2–5 | Sep 15–Oct 12 | **Shared:** the four tooling items the conference paper is owed (Sparrow baseline, RTT matrix + site join, partition driver, `static` arm or its removal) plus the F6 context-error column — all in `CCGRID27_PAPER_PLAN.md` §4. Pilot one E1′ and one E2 cell at Hier-30 (`--groups-per-coordinator 2`, interleaved, clocks checked). Confirm slice lease horizon. **Journal-only, only if slack:** P0-2/P0-3 design; GPU node request (do not wait on it). | **Code freeze Oct 12 — tag it** |
| 6–11 | Oct 13–Nov 23 | **Conference campaign and drafting** — see the conference plan. E6 accumulates for free. Journal work in this window: none that touches the slice. Start the journal's §II related work and threats section (§0.4) in writing time the conference does not need. | Conference gates (10 Nov framing, 24 Nov abstract) |
| 12 | Nov 24–Dec 1 | Conference submission week. | **CCGrid paper due 1 Dec (AoE)** |
| 13–17 | Dec 2–Jan 4 | **Journal campaign:** E1's LLM columns, E4, E8, E3b — same frozen revision, same slice. vLLM/GPU bring-up here. P0-2, P0-3 land before the E4 cells that need them (they are the only post-freeze code, and they must not touch anything a shared cell measured — or the shared cells are re-measured). Holidays are inside this window. | Journal data freeze Jan 4 |
| 18–20 | Jan 5–25 | Journal draft. **C1 and C3 sections are the conference paper's material, cited** — the incremental writing is C2, the composite interaction (with the §0.9 caveat stated), the threats section and the reproducibility appendix. | Complete draft Jan 25 |
| 21 | Jan 26–31 | Polish, cover letter (state the CCGrid relationship explicitly), submit. | **Submit to FGCS by Jan 31, 2027** |

**What the split costs in schedule terms.** The journal's measurement window is Dec 2 – Jan 4
with the holidays inside, and its draft window three weeks. That is only survivable because the
conference paper writes C1 and C3 first; a conference slip costs the same days twice. **The 1 Dec
date is the one to defend.** If the conference draft is not review-ready by 27 Nov, the conference
plan says to cut a figure, not to slip.

**Post-freeze code rule.** P0-2 and P0-3 are the only code allowed after 12 Oct. Both live in
`llm_delegator.py` / `mab_manager.py` behind `delegation.policy` and `llm.*` keys that no
conference cell sets, so a shared cell's measurement is unaffected — verify that with the test
suite and a diff review before the first journal cell, because "same frozen revision" is the
premise of citing the conference numbers.

**Parallelization.** What parallelizes in weeks 2–5 is code, not experiments. E5 is a conference
figure and runs in the conference window; E3a re-analyzes E1′ data and cannot start before it
exists; E6 is free (aggregate over every run); vLLM, GPU, P0-2/P0-3 are off the critical path
because everything they serve is journal-only.

## 10. Risks and fallbacks

| Risk | Mitigation / fallback |
|---|---|
| LLM delegation (P0-1) doesn't beat analytic on quality | This is a *result*, not a failure — E4 is framed as honest accounting. Fallback thesis shifts weight to "expensive decision planes break BFT; here's the envelope where reasoning pays." |
| **Site outages shrink the VM pool below the ladder** | **Updated 2026-09-14. PSC returned; 92 of 92 up, so the ~90-host ladder is primary again (§5).** The 83-host sizing from 2026-09-10 stays as the fallback: **Hier-80** (8 groups of 9 + 8 coordinators, same group shape) at 1/VM, Mesh-180 at 3/VM on 60, Hier-270 at 4/VM on 68. Choose at E0 from a fresh sweep, then hold it — agents-per-VM is reported per rung, because a substituted density is a different experiment: co-location turns inter-agent messages into loopback, flattering exactly the coordination cost C3 measures. Sweep before sizing any cell with `make_agent_hosts.py`, **not** a bare `ssh` loop: a node returning from a rebuild has a new host key, and plain ssh under `BatchMode` fails it indistinguishably from a timeout — that is how AMST was recorded as down for a day while all 7 nodes were up and answering, which would have written a whole site out of the fleet. **E3a's risk is closed**: its transatlantic bin is AMST, which is back. PSC is not a bin the plan depends on. |
| **GPU node not granted / preempted** | Highest-probability schedule risk — request in week 1 and confirm before the campaign. Fallback: CPU-served 7–8B on a dedicated VM, which caps the model-capability sweep in E4 but leaves E1/E2/E3 fully intact (a slow LLM is still a valid expensive-decision plane — arguably a more dramatic one). |
| Inference throughput bottlenecks the 270-agent runs | With vLLM on GPU this should not bind: coordinators only (~27 LLM agents), batched. Plus cache + inference budget (P0-3). If it still binds, cap LLM runs at Hier-90 and report 270 as PBFT/Snow-only, with any projection clearly labeled as such. |
| Journal review cycle demands new runs months later | Tag the frozen code revision; keep the slice reproducible from the notebook; archive every run tree plus `agent_profiles.json`/`agent_dtns.json`/seeds so any cell can be re-run identically. |
| **Two deadlines 8.5 weeks apart** | The journal's measurement window is Dec 2 – Jan 4 (holidays inside) and its draft window three weeks. Survivable only because the conference paper writes C1 and C3 first, so a conference slip costs the same days twice — **defend 1 Dec** (not 8 Dec: the CFP was checked 2026-09-14). If the conference draft is not review-ready by 27 Nov, the correct move is to cut a conference figure (F6 first, F5 second), not to slip. If CCGrid is missed outright, do **not** chase HPDC on 5 Feb: it is ~15% acceptance, its abstract lands 5 days after the FGCS deadline, and the simulated-execution framing needs work that window does not contain. Fold C1/C3 back into the journal and submit one paper. |
| Slice lease expires mid-campaign or before revisions | Check the lease horizon now and renew ahead of the campaign; it is the only hard external clock left. |
| Slice instability / site outages mid-campaign (17 sites is a lot of failure surface) | Run configurations in interleaved order (not blocked by config) so partial data is still balanced; keep a 30-agent single-site fallback config; log per-run site health from the monitor VM. |
| ~120 h of testbed time doesn't fit | Cut order: E7 second scale point → E3b impairment levels (keep +50 ms and 1% loss) → E5 repeats → E1 non-headline cells. Never cut E7 entirely (§6). |
| Paper reads as three stapled features | **This happened, and the split is now the plan, not the fallback (§0.9, decided 2026-09-10).** C1+C3 go to CCGrid (1 Dec), C2 and the composite interaction stay here. The row below it was right: the natural seam is [Snow + bandit systems paper] and [LLM decision plane paper]. What the old advice — "do not plan for the split up front" — got wrong is that planning for it *earlier* would have removed vLLM, P0-2, P0-3 and P0-8 from the critical path months ago. Each remaining experiment section still opens by naming the claim it tests. |
| LLM plane's output turns out inert for placement even after P0-5/6/7 (§0.3) | Then C2 is reframed, not abandoned: the paper reports *why* (race-to-propose, degenerate scores) with E8 as the evidence, and the cost/latency envelope stands on its own. This is a publishable negative result in a journal. |

---

## 11. Immediate next actions (week of Sep 10, reordered for the CCGrid clock)

The split (§0.9) changes what is on the critical path. In order:

1. ~~**P0-4 instrumentation, including `GroupSnapshot` age at decision time.**~~ **Done
   2026-09-14** (see the P0-4 row above). Context age reaches `metrics.json`, the `[STATS]`
   line, the Prometheus export and `decisions.csv`. **Exercised on hardware the same day, and
   the validity column fired**: 678 skewed readings, one coordinator's entire age column
   pinned at zero. The cause was not drift but 66 of 92 hosts unable to reach any NTP server
   for 30 days. Fixed (`fix_slice_clocks.sh`; `07_ntp.sh` in the slice-build pipeline), re-run
   clean — `ctx_skewed_ages` 0, 436-545 decisions, ages p50 0.25-0.44 s / p95 7.0-8.7 s — and
   the headline series moved to a monotonic clock so it no longer depends on the fleet staying
   fixed. Every campaign cell should run `./fix_slice_clocks.sh --check` first.
2. ~~**P1-1 oracle**~~ **Done 2026-09-14** (see the P1-1 row). Regret and routing accuracy
   land on the collected row beside the context age, so **F6 is one collector run**. With P0-4
   this clears every *P0* item that was blocking a conference figure. ~~P0-8~~ landed the same
   day. **What remains before the freeze is tooling the plan described as experiments:** the
   Sparrow baseline (E7), the RTT matrix and site join (E3a), the partition driver (E6), the
   `static` delegation arm or its removal (E2/F4), and the F6 context-error column — all listed
   in `CCGRID27_PAPER_PLAN.md` §4.
2b. **Two pre-run checks, both learned the hard way, both cheap.** `./fix_slice_clocks.sh
   --check` — the fleet's clocks were wrong for a month with nothing reporting it, and
   `ctx_skewed_ages`/`ctx_skew_max_s` on the collected row are the post-hoc evidence. And for
   any cell whose figure is regret, `oracle.py --validate` on the cell's first run: it is the
   only thing that catches a failure profile that does not describe the run, and a profile
   whose contrast runs *between* coordinators rather than within each one yields a meaningless
   `routing_accuracy` of 1.0.
3. **Confirm the slice lease horizon.** Still the only hard external clock. The **GPU node
   dropped off the critical path** — everything it serves (E4, the model-capability sweep) is
   journal-only — so confirm it, but do not wait on it.
4. **Decide E8** (fault-injection) and, if yes, merge `scenarios/` from `origin/chaos`. Journal
   scope now, so this can wait until December — but the decision should not.
5. **Generate every campaign hosts file with `make_agent_hosts.py`** (interleaved, the default)
   and keep `agent_sites.txt` beside it. Every WAN claim in either paper depends on it, and
   every run before 2026-09-10 was the clustered case.
6. **Before any LLM delegation cell** (journal window), measure the scheduling-thread cost: a
   coordinator delegating serially at ~1 LLM call/job caps its subtree's delegation rate at
   1/latency. Measured on the slice 2026-09-09 at Hier-30: 2.6-2.9 s mean per decision, and the
   LLM arm completed 349 of 400 jobs against the bandit arm's 367. If E1 shows LLM-plane
   throughput flat in fleet size, this — not the consensus protocol — may be why.

**Standing obligations on every campaign run** (revised 2026-09-18 for the two default
changes of that day): `--master-fleet-size 270` (else the ladder compares different fleets); a
`--runtime` cap or `--shutdown-after-seconds` (else a stalled cell polls all night);
**`--delegation-policy` on every E2 cell**, so the arm is recorded in `run_meta.json` rather
than depending on an unrecorded edit to the controller's config; and now **explicit
`--groups-per-coordinator` and `--hierarchical-level1-agent-type` on every hierarchical
cell**, both ways. The old obligation was "2 or more on any delegation cell"; 2 is the default
now, so delegation cells get it for free — but E0 and every analytic cell must pass **1**, or
they duplicate every job (§0.10, 2026-09-18 update) and E0 is not the eScience topology. The
coordinator type defaults to `resource` and does not follow `--agent-type`: a journal cell
that wants LLM coordinators passes `--hierarchical-level1-agent-type llm`, and every host that
can receive a coordinator must carry `OPENAI_API_KEY` — today only ~30 of 92 do, so **push the
key to all 92 hosts before the first journal cell**, or coordinator placement is site-clustered
and every WAN number from the LLM columns is invalid. Both values are recorded in
`run_meta.json` as *observed* from the generated configs, not as requested. `collect.py` reads
the coordinator type (it decides the LLM coverage denominator) but **not yet the fan-out** — add
`groups_per_coordinator` to the wide row before E0, or a cell that silently ran at the wrong
fan-out is visible only to someone who opens its `run_meta.json`.

Two flags that must **never** appear on a campaign cell: `--size-to-jobs` and the converter's
`--generate-agent-configs` (both 2026-09-17/18). They raise every agent to the largest job and
give every agent every DTN — right for running a workflow (conference §8), wrong for a fleet
whose heterogeneity is the thing being measured.

Done and no longer listed: the §0.2 pre-campaign fixes, P0-5, P0-1, the metrics-attribution
fix (§0.8), and the Hier-30 smoke — `runs/smoke-g3-bandit` and `runs/smoke-g3-llm`, both arms
30/30 agents reporting, on the frozen-candidate revision.
