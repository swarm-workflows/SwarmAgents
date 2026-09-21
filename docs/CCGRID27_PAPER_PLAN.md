# CCGrid 2027 — conference paper plan (bandit + Snow)

**Decided 2026-09-10; reviewed and corrected 2026-09-14; reviewed again 2026-09-18 (§4 defaults row, §8 added).** The work splits into two papers.
This one is the conference paper: contextual-bandit delegation (C1) and gossip consensus (C3) on
the 17-site FABRIC slice. The LLM decision plane (C2), the composite interaction claim and the
quantum-hybrid strand are the journal paper's (`FGCS_EVAL_PLAN.md`).

**Division of labour between the two documents.** `FGCS_EVAL_PLAN.md` is the master for what
the two papers *share*: substrate rules, the pre-campaign fixes, metric definitions, the
experiment matrix and the run budget. This file owns everything the conference submission needs
that the journal does not: its claims, its page and figure budget, its fleet, the code it is
still owed, and its calendar. **The conference calendar lives here and only here** (`FGCS_EVAL_PLAN.md`
§9 points at it rather than repeating it), so a date can no longer be right in one file and
wrong in the other — which is how both carried a submission deadline one week late for four days.

| | |
|---|---|
| Venue | IEEE/ACM CCGrid 2027, hosted by UNT (Dallas–Fort Worth), 24–27 May 2027 |
| Abstract | **24 Nov 2026 (AoE)** |
| Full paper | **1 Dec 2026 (AoE)** — *verified 2026-09-14 against the CFP; both plans previously said 8 Dec* |
| Notification | 1 Feb 2027 |
| Camera-ready | 16 Mar 2027 (up to 2 extra pages purchasable, references excluded) |
| Format | 10 pages, IEEE conference template, **including references, figures and tables** |
| Review | **Double-blind** |
| Journal paper | FGCS, submit by 31 Jan 2027 — 8.5 weeks later, same frozen revision |

Source: https://hpcclab.org/ccgrid27-call-for-papers/ — re-check it once more the week of the
abstract; CFP pages get edited.

---

## 1. What this paper claims

Two of the three claims from the journal thesis, plus the link between them:

- **C1** — a learned delegation policy (LinUCB) beats context-blind delegation on schedule
  quality, and keeps beating it under non-stationarity and churn.
- **C3** — gossip (Snow) consensus keeps a hierarchical scheduler completing at scales and RTTs
  where PBFT livelocks, at a bounded latency price.
- **The link** — Snow finalizes assignments faster, so the group state a coordinator learns
  from is *younger*; the bandit's regret should therefore fall under Snow at equal scale, for a
  reason that has nothing to do with either mechanism alone.

### The interaction result does NOT come with them

The journal thesis says the novel result is that *expensive decision-making changes which
consensus protocol you should run*. **That claim needs the LLM, and the LLM is not in this
paper.** A LinUCB decision costs microseconds — a matrix update, not a network round trip — so
the bandit cannot play the role of the expensive plane. Whatever this paper says about the
interaction has to rest on the staleness mechanism above, which is a different and smaller
claim.

That is a deliberate trade, and it has a falsifiable consequence: **if the staleness effect is
null, this paper is two mechanisms sharing a scale story.** That is still a CCGrid paper — a
real 17-site evaluation of gossip consensus for hierarchical scheduling, with an adaptive
delegation policy on top — but it is a weaker one, and we should know which paper we are
writing by the end of the campaign, not in December. See §6 for the gate.

### The link, as instrumented, cannot vary with the consensus protocol (found 2026-09-14)

Read the mechanism against the code before betting the framing on it. The context age that
P0-4 records is `decision − received_at`, and `received_at` is stamped in
`_note_agent_seen`, which fires in exactly one place: `_refresh_neighbors`, when a **fresher
`AgentInfo` record is read from Redis**. That record is written by each child on its own
periodic tick and re-read by the coordinator on the full neighbor refresh — **every 5 s when
gossip is enabled**, which is the shipped default (`_should_full_neighbor_refresh`). The gossip
overlay (`_apply_gossip_overlay`) refreshes `load` on the same entries between full refreshes
**without touching the stamp**. Nothing on that path knows which consensus engine is running.
The 2026-09-14 slice run already shows the shape: ages p50 0.25–0.44 s, p95 7.0–8.7 s — a
refresh cadence, not a finalization latency.

So under PBFT and Snow the age distributions should come out the same to within noise, and a
null on the gate as currently written would say nothing about staleness — it would say the
x-axis does not measure it. What Snow can plausibly change is not how *old* the coordinator's
view is but how *wrong* it is: `GroupSnapshot.inflight` counts delegations the coordinator has
not yet seen resolve, and a protocol that finalizes faster resolves them sooner.

Two consequences, both cheap:

1. **Add a second x-axis for F6: context error.** For each decision row in `decisions.csv`,
   reconstruct the group's true in-flight count at the decision timestamp from `all_jobs.csv`
   (`assigned_at`/`completed_at` per job, grouped by assignee's group) and record
   `|snapshot.inflight − true_inflight|` per candidate. **One small agent-side change is
   needed before the freeze:** `DecisionRecord` (`swarm/utils/instrumentation.py`) carries
   `candidates`, `selected`, `decide_s` and the ages but *not* the snapshot values the policy
   saw — add per-candidate `inflight` (and headroom) to the row; the join itself is then
   `collect.py` work. Keep `ctx_age_*` as the validity column it is (it shows the refresh path is
   healthy) and plot regret against *error*, split by protocol.
2. **Rewrite the 10 Nov gate** to ask two questions in order: does context error differ by
   protocol at equal scale, and if so does regret follow it. Age alone answering "no" is not a
   null result and must not be reported as one.

## 2. What is explicitly out of scope

Naming these keeps the 10 pages honest and keeps the journal paper's contribution intact
(CCGrid and FGCS both prohibit substantial overlap; the journal must extend, not repeat):

- Everything LLM: bidding, LLM delegation, bid pacing, elicitation, the cost of reasoning
  (E4), the semantic-inversion test (E8). C2(a) and C2(b) are the journal's spine.
- The quantum/hybrid split-scheduling strand.
- The three-level hierarchy and the 990/1000-agent presets.
- Anything requiring the GPU node or on-slice vLLM.

**In** scope, as of 2026-09-18, but as a *case study* and not a headline cell: executing one
real Pegasus workflow — its own executables, in its own container — on the slice (§8). It is
the one place in the paper where jobs compute rather than sleep, and it is bounded to what that
can honestly show.

## 3. Evaluation — cells drawn from the master matrix

All from `FGCS_EVAL_PLAN.md` §5, with the decision-plane axis restricted to `{analytic, bandit}`:

| Cell | From | Purpose here |
|---|---|---|
| E0 | Substrate re-baseline | The within-campaign reference every number is compared against |
| E1′ | `{PBFT, Snow, Hybrid} × {analytic, bandit}` | C3's core figure. Half of E1 — the LLM columns are the journal's |
| E2 | Delegation quality + E2a non-stationarity + E2b churn | C1 in full. This is the paper's deepest experiment |
| E3a | Per-RTT-bin analysis over E1′ runs | C3 on the real WAN. Analysis only — **but see §4: nothing measures RTT yet** |
| E5 | Coordination overhead | The mechanism behind C3: messages/job, rounds to finalize |
| E6 | Safety and correctness | Defuses "did your gossip protocol double-assign anything" |
| E7 | External baselines | See §7 — not optional at this venue. **Sparrow-style baseline is not written** |

**Fleet (re-revised 2026-09-14: PSC is back, 92 of 92 VMs up).** The 2026-09-10 revision
re-sized every rung for 83 hosts because PSC (`agent-10`–`18`) was out; it returned by
2026-09-14 (all 92 answering, uptime ~4 d). The original ladder is reachable again:

| Rung | Agents/VM | Hosts | Role |
|---|---|---|---|
| Hier-30 | 1 | 30 | Cheap sweeps; matches the eScience'26 operating point |
| **Hier-90** | 1 | 90 | **Primary.** 9 groups of 9 + 9 coordinators; E2 runs it with `--groups-per-coordinator 3` (3 coordinators × 3 groups) |
| Mesh-180 | 2 | 90 | Flat-PBFT livelock contrast |
| Hier-270 | 3 | 90 | The collapse point |

**Hier-80 stays as the fallback preset**, not as a second primary. The fleet is not static —
two sites dropped in one week in September — so the rule is: sweep with `make_agent_hosts.py`
at E0, pick the rung set the live fleet supports, **and do not change it mid-campaign**. A
ladder that mixes Hier-80 and Hier-90 runs, or 3/VM and 4/VM densities, is not a scaling
curve; co-locating agents turns inter-agent messages into loopback, which flatters exactly the
quantity C3 measures. If the fleet falls below 90 before E0, the 2026-09-10 sizing (Hier-80 at
1/VM, Mesh-180 at 3/VM on 60, Hier-270 at 4/VM on 68) is the fallback, and `run_test.py` sizes
hosts with a ceiling (`math.ceil(agents / per_host)`), so 270 at 4/VM is 68 hosts with two
agents on the last one. **Report agents-per-VM per rung in the results table either way.**

Hier-30 and Hier-90 run at one agent per VM with **site-interleaved placement**
(`make_agent_hosts.py`, default). Placement follows the order of `agent_hosts.txt`, and agent
ids map to sites in contiguous blocks, so the numeric ordering used by every run before
2026-09-10 put 65 of 79 adjacent agents at the *same site* — a hierarchical group's consensus
traffic never left the building. Interleaved is 0 of 79. **Every WAN number in this paper must
come from interleaved runs.** Two more pre-run checks on every cell: `./fix_slice_clocks.sh
--check` (66 of 92 hosts were free-running 0.4–1.1 s apart for a month with nothing
reporting it) and `--hierarchical-level1-agent-type resource` (the default since 2026-09-18; before that `llm`, which
needs an API key only some hosts carry, so a coordinator dies at startup depending on
placement).

### Framing that decides the reviews

**Jobs simulate their wall time; they do not compute — in every cell except the §8 case
study.** `Job.execute()` sleeps `clamp(wall_time × scale, min, max)` from the Pegasus trace,
capped at 120 s. The consensus
messages, delegation decisions, failure detection and WAN RTTs are all real; the application
work is not. So:

- **Headline metrics are control-plane:** completion %, selection/decision latency
  (p50/p95, split into consensus | queueing), consensus messages per job, rounds to finalize,
  delegation regret vs oracle, time-to-re-adoption after churn.
- **Makespan, throughput and utilisation are secondary**, reported with the cap and the
  scaling factor stated in the caption every time they appear.
- Say this in §IV before the first results table, not in a limitations paragraph at the end.
  A reviewer who discovers it late reads it as concealment.

## 4. Code this paper is still owed before the 12 Oct freeze

`FGCS_EVAL_PLAN.md` §4 tracks the P0 list, and every P0 item that blocks a conference figure
is done (P0-4 instrumentation, P1-1 oracle, both 2026-09-14). But four things this paper's
figure list depends on were never on that list, because the master plan describes them as
experiments rather than as code. Checked against the tree 2026-09-14:

| Owed to | What is missing | Why it blocks a figure |
|---|---|---|
| **E7 / T2** | A Sparrow-style sampling baseline. `baselines/scheduler.py` has greedy, round-robin and random only; no file in the tree mentions Sparrow or late binding. §7 calls E7 "non-negotiable" and the master plan estimates ~200 LOC | Without it the external-baseline table is three centralized strawmen, which is the objection R2/R3/R4 raised last time |
| **E3a / F3** | RTT measurement. No script measures per-site RTT to the coordinator tier, `collect.py` has no site column, and `agent_sites.txt` (from `make_agent_hosts.py`) is not joined to anything. "Analysis only, no extra runs" is true of the runs, not of the tooling | F3 is "the figure no prior SWARM paper could produce"; today it cannot be produced from this tree either. Needs: a ping/gRPC-echo matrix captured per run into the run dir, and a `site`/`rtt_bin` join in `collect.py` |
| **E6** | Partition tooling. The only iptables/netem in the tree is a netplan firewall setup script; there is no driver that blackholes one site group from another and heals it | The partition test is the answer to the eScience reviewer's quorum-under-partition objection; the aggregate double-assignment audit alone does not answer it |
| **F4** | The `static` arm. No static or greedy delegation policy exists; `mab.enabled: false` gives "every capable group" (fan-out to all), which is a different baseline and is recorded as `all`. `LinTS` *does* exist (`mab.algorithm: lin_ts`) and is not in F4's arm list | Either implement a static least-loaded (or round-robin) delegation policy, or drop `static` from F4 and say the baselines are ε-greedy and UCB1. Do not label `all` as `static` |
| **F6** | Context-error column (§1): per-candidate snapshot `inflight`/headroom on `DecisionRecord`, then the offline join in `collect.py` | Without it the gate cannot distinguish "no staleness effect" from "the x-axis does not vary" |
| ~~**F4, F5, F6 (P0-9)**~~ **FIXED 2026-09-14, verified on the slice 2026-09-15** | The bandit was rewarded at scheduling time, not completion time: `schedule_job` persisted `COMPLETE`/`exit_status 0` before executing and the monitor credited success on first sight. On `p11-oracle2`: 83 injected failures, **19** seen. Now `schedule_job` persists `RUNNING` and only execution writes `COMPLETE`. Re-runs: `p11-oracle4` 76 injected → 122 recorded (76 exit + 46 timeouts), `smoke-g4-bandit` 56 → 55, `smoke-g4-llm` 108 → 108. Oracle on `p11-oracle4`: 488 decisions, 0 unscored, routing accuracy 0.822, validation error 0.041. `FGCS_EVAL_PLAN.md` §0.10 / P0-9 | **No C1 number measured before 2026-09-14 is citable.** Runs are now longer (they wait for execution) and failure-injection completion % is lower and honest. One gap left: a job in state `FAILED` still yields no bandit outcome (§0.10 of the master plan) |
| ~~**F2 (E5)**~~ **FIXED 2026-09-15** | PBFT proposer re-broadcast COMMIT on every PREPARE past quorum (the guard asked `incoming`, false for the proposer's own proposal). Now keyed on a per-proposal `_commits_sent` set. Four more Snow/SWIM/config defects fixed with it — `FGCS_EVAL_PLAN.md` §0.11 | Messages-per-job was biased against PBFT by up to (n − quorum) COMMITs per job, the wrong direction for a paper arguing PBFT is expensive. **E5 must be measured on the fixed engine** |
| ~~**F1/F2 framing**~~ **NAMED AND MEASURED 2026-09-16** | The `# TEMP HACK` in `scheduling_main` is now `job_selection.coordinator_cost_matrix`. Default `self` is the old behaviour bit for bit (every coordinator proposes itself for every job); `peers` scores the coordinator tier as level 0 does and is an E5 arm, not a fix — it moves every hierarchical number, so no figure may mix the two. Unknown values raise. `FGCS_EVAL_PLAN.md` §0.11 | Both halves of the ask are delivered from the same E5 runs: **proposals/job by tier** is `proposers_per_job_l1` in `collect.py` (1.0 = one decision per job at the tier; the coordinator count = every coordinator proposed itself), which is what separates the Hier-250 collapse into "n proposals per job" versus PBFT's own O(n²); and §III can now name the behaviour and cite the number for it |

| ~~**E0, E1′ analytic columns (found 2026-09-18)**~~ **FIXED 2026-09-18 (option b)** | **Two defaults changed on 2026-09-18 and one of them switched on a known behaviour.** `--hierarchical-level1-agent-type` now defaults to `resource` (was `llm`) and `--groups-per-coordinator` to **2** (was 1). The second one matters here: with `mab.enabled: false` — the shipped default, and what every *analytic* hierarchical cell runs — a coordinator delegates to **every** capable group and each group executes the job (`FGCS_EVAL_PLAN.md` §0.10: 400 jobs → 952 leaf completions, up to 6 executions of one job, on `p11-oracle3`). At G=1 that was latent, since every coordinator had one group. **At the new default every analytic hierarchical run duplicates every job across two groups.** The job key is `job:<level>:<group>:<id>`, so exactly-once holds per group and the CAS does not prevent it. Completion %, throughput, makespan and first-finisher latency are all biased the flattering way — in E0 (the "same system as published" reference) and in half of E1′. | Decide before the freeze, one of: **(a)** E0 and every analytic hierarchical cell pass `--groups-per-coordinator 1` explicitly (E0 must anyway — Hier-30 at G=2 is 3 coordinators × 2 groups, 27 leaves in groups of 5–6, which is not the eScience topology of 5 × 5 that "as published" claims); or **(b)** make the no-bandit path choose one group per job (random, recorded as `random`; `delegation.top_k` already exists for the LLM path and the random pick already exists as its fallback) so a default hierarchical run is a scheduler and not a broadcast. (b) landed: the no-bandit path now delegates to `mab.top_k` groups at random (`tests/test_fanout_default.py`), recorded as `random` — which the oracle scores, so every analytic cell also yields the context-blind baseline F4 was missing. E0 still needs `--groups-per-coordinator 1` to be the eScience topology. Either way, `run_meta.json` now records the *observed* fan-out and coordinator type from the generated configs, so a cell that got the wrong one is detectable after the fact — check both columns at E0 |
| **Every cell (2026-09-18)** | `--size-to-jobs` (new) and the converter's `--generate-agent-configs` size the fleet to the jobs and give every agent every DTN. **Neither may be used on a campaign cell**: the ladder is measured on the standard flavour pool with the drawn DTN assignment, and a sized fleet is a different fleet. They exist for §8 | Grep the campaign driver for both flags before E0; `run_meta.json` `argv` shows it after the fact |

| ~~**F1, F2, E5 (found 2026-09-18, code review)**~~ **FIXED 2026-09-18** | **PBFT stragglers after quorum re-entered the engine.** `_forget_object` erases the COMMIT-dedupe entry at finalize and `select_job` never marks the job decided locally (at a coordinator, not until the delegation monitor marks it COMPLETE), so the `n − q` straggler PREPAREs and COMMITs every tier produces re-adopt the proposal with the sender's wire vote lists: a second COMMIT broadcast per straggler PREPARE, and a **second finalization** per straggler COMMIT — demonstrated in `tests/test_pbft_stragglers.py` (xfail until fixed). The 2026-09-15 COMMIT fix, undone by the forget() it added | Messages-per-job, `finalized` and `votes_` biased *against* PBFT, worst at the coordinator tier under `coordinator_cost_matrix: self` — the collapse cell. Fixed: the engine remembers finalized `(object, p_id)` pairs and skips their stragglers, the agent marks a job decided at finalize, and participants now record the proposer (not the last COMMIT's sender) as leader. `tests/test_pbft_stragglers.py`. **E5 must be measured on this revision.** `docs/CODE_REVIEW_2026-09-18.md` §1 |
| ~~**E1′, E3a, T1 (2026-09-18)**~~ **FIXED 2026-09-20** | Hierarchical `all_jobs.csv` used the **delegation** time as `submitted_at` and the leaf tier's selection stamps, so `job_latency`, `makespan`, `throughput`, `sched_latency` and the headline `selection_*` all exclude the coordinator tier (`plotting/data.py:267-268`). The tier where PBFT collapses shows only in completion % and `l1_selection_*` | Done: `submitted_at` is the top-tier arrival, `selection_total` sums every tier, `collect.py` reports `success_pct` beside `completion_pct`. **Per figure, say which:** `selection_*` = leaf tier, `l1_selection_*` = coordinator tier, `selection_total_*` = end to end; `completion_pct` = finished, `success_pct` = exit 0. Older hierarchical exports carry the delegation-time `submitted_at` and must not be mixed in. Review §3, §9 |
| ~~**Every hierarchical cell (2026-09-18)**~~ **BOTH FIXED 2026-09-20** | `selection_main`'s startup barrier had no timeout (`selection_barrier_s`, default `failure_threshold_seconds`; `barrier_short_agents` on the collected row is a validity column), and coordinators never detected a dead child group by staleness — only by the Redis key's 600 s TTL, so F5's re-adoption time was delegation-timeout learning rather than liveness. Existing map entries are now evicted at the staleness threshold, floored on the agent's own tier so eviction cannot outrun the failure detector that reassigns a dead peer's jobs | Review §4, §5. `tests/test_selection_barrier.py`, `tests/test_child_staleness.py`. **No F5 re-adoption number measured before 2026-09-20 is citable** |

Optional: a Redis op-rate counter for E5. The plan lists it as an E5 metric and nothing counts
it; Grafana on the `database` node's `redis_exporter` covers it without agent code.

## 5. Ten pages, and the figures that fit

Ten pages *including references* is roughly 8.5 of text. Budget:

| § | Pages | Content |
|---|---|---|
| I Intro + contributions | 1.25 | The collapse-and-repair result up front |
| II Background / related | 1.0 | BFT vs gossip; bandits for scheduling; cite eScience'26 in third person |
| III Design | 2.0 | Coordinator tier, Snow engine, LinUCB delegation with the GroupSnapshot context |
| IV Setup | 0.75 | Slice, fleet table with agents/VM, workload, the simulated-execution statement |
| V Results | 3.5 | Figures below |
| VI Threats / limitations | 0.5 | Density, simulated execution, single-implementation baselines |
| VII Conclusion + refs | 1.0 | |

**Six figures, no more.** Each has to earn a third of a page:

1. **F1 — collapse and repair.** Completion % vs scale for PBFT / Snow / Hybrid. The paper's
   reason to exist; nothing else goes on page 1.
2. **F2 — mechanism.** Consensus messages per job and rounds-to-finalize vs scale, showing
   *why* F1 happens.
3. **F3 — per-RTT-bin latency** (E3a). Selection latency by measured RTT bin per engine. No
   prior SWARM paper could produce this — and this tree cannot yet either (§4).
4. **F4 — delegation quality** (C1). Regret vs oracle over time for
   `{ε-greedy, UCB1, LinUCB, LinTS}` (plus `static` only if §4 lands it), with the
   non-stationarity flip marked.
5. **F5 — churn.** Time-to-re-adoption after a group outage and rejoin, fixes on/off.
6. **F6 — the link, or its absence.** Bandit regret under PBFT vs Snow at equal scale against
   context *error* (§1), with context age reported as the validity column. If the effect is
   null, this becomes a table in §VI and F1–F5 carry the paper. Both axes come from one
   collector run: the decision rows from `decisions.csv` (P0-4), regret from the oracle
   labelling those same rows (P1-1). Report `ctx_skewed_ages` in the caption — a non-zero
   fleet total means the remote age series is clock skew rather than staleness.

Two tables at most in the results: the fleet/workload table and the E7 baseline comparison.
The §8 case study is a third, quarter-page table (**T3**) and takes its space from §VI — or,
if the 10 Nov gate is null, from F6's slot.

### Double-blind

The slice is a shared facility, so describing it is not identifying. What is: citing
eScience'26 as "our prior work". Cite it in the third person throughout, and check the
acknowledgements and the artifact URL before submitting.

## 6. Dates, working backwards from 1 December

| By | Gate |
|---|---|
| **12 Oct** | **Code freeze — tag it.** Every §4 item is in or is cut from the figure list; P0-4 and P1-1 are in. Pilot one E1′ and one E2 cell end-to-end at Hier-30 with `--groups-per-coordinator 2`, interleaved placement, clocks checked; verify every §7 metric of the master plan lands in `collect.py`'s row |
| 13 Oct | Campaign starts. E0 first (gate: near the eScience envelope or stop and debug), then E1′, then E2, on the frozen revision, interleaved placement |
| by 20 Oct | **§8 pilot**: the candidate workflow converted on the Pegasus host, its bundle on the export, its image staged to the case-study hosts, one run at Mesh-30 under Snow that completes with byte-identical outputs. No agent code is involved, so this is not on the freeze — but a runner defect found here is the one post-freeze change allowed besides P0-2/P0-3 (§8) |
| 3–9 Nov | §8 runs: 3 repeats × {PBFT, Snow} on the same 30 hosts, alongside E5/E7 |
| 3–9 Nov | E5, E7, E3a re-analysis of the E1′ pull by RTT bin |
| **10 Nov** | All E1′/E2 cells collected. **Gate, two questions in order (§1):** does context error differ by protocol at equal scale, and does regret follow it? Yes → "two mechanisms + an interaction". No → "two mechanisms + a scale story", F6 becomes a §VI table. The intro is written differently in each case. Record the answer that day |
| 17 Nov | E3a/E5/E6 analysis complete; all six figures drafted from real data |
| **24 Nov** | **Abstract due (AoE).** Title and abstract must match the gate outcome |
| 27 Nov | Full draft to internal review (Hamza/Anirban). Three days, not a week: this is the cost of the corrected deadline |
| 30 Nov | §7 pre-mortem pass, double-blind check (eScience'26 third-person, artifact URL anonymized) |
| **1 Dec** | **Submit (AoE)** |
| 2 Dec | Journal campaign starts: C2 cells (E1 LLM columns, E4, E8, E3b) on the same frozen revision |

**What the corrected deadline changes.** Draft and review compress from 13 days to 7. If the
draft is not review-ready by 27 Nov, cut a figure rather than slip: F6 first (it has a
fallback as a table), F5 second. The journal's window *grows* by a week (2 Dec → 31 Jan, 8.5
weeks), which is where the holidays inside it get absorbed. If CCGrid is missed outright, do
not chase HPDC (abstract 5 Feb, ~15% acceptance, and the simulated-execution framing needs work
that window does not contain): fold C1/C3 back into the journal and submit one paper.

The journal reuses this campaign's substrate — **no re-measurement between the two papers**
(`swarm-no-cross-substrate-result-reuse`: one substrate, one revision, both papers).

## 7. Reviewer pre-mortem specific to this venue

| Attack | Answer |
|---|---|
| "Jobs don't run, you sleep." | §IV states it before any result; headline claims are control-plane. Real wall times come from a 25k-job Pegasus trace, so the *distribution* is real even though the work is not. **And §8 runs one real workflow for real** — same scheduler, same slice, DAG order respected, outputs byte-identical to Pegasus — with the control-plane metrics measured on jobs that compute. It does not claim a makespan against Pegasus (§8 says why) |
| "Your real-workflow result is five jobs on a shared NFS mount." | Correct on both counts, and §8 says so in its first paragraph: the case study is a correctness and control-plane demonstration, not a data-movement or makespan result. The scale and the WAN claims rest on the trace-driven cells |
| "You compare your Snow to your PBFT." | E7 external baselines are mandatory, not optional — and the Sparrow-style one has to be written first (§4). The PBFT arm must be tuned and the tuning reported, or the comparison is worthless |
| "270 agents is not scale." | Lead with 17 sites and real RTT, not the agent count. Density is disclosed per rung |
| "Avalanche and LinUCB are both off the shelf." | True. The contribution is the composition and the WAN-scale empirical result — say so in §I rather than implying novelty in the mechanisms |
| "Where is the LLM work this group published on?" | Under double-blind, cite it in third person as related work. It is the journal paper's subject and is not needed here |
| "Your staleness axis is a heartbeat period." | It is (§1). Report context *error*, not age, as the F6 x-axis, and say why age cannot move with the protocol |

## 8. Use case: executing a real Pegasus workflow on the slice (added 2026-09-18)

**Why it is in the paper.** Every headline cell replays a trace: the consensus, the delegation
and the WAN are real, the jobs sleep. The strongest objection to that framing (§7) is not
answered by disclosure alone. Since 2026-09-16 the tree can run a Pegasus workflow's *own*
executables in its *own* container, in dependency order, across the slice — and did, for
`soilmoisture` (`docs/WORKFLOW_EXECUTION.md` §3: outputs byte-identical to the Pegasus run,
per-job times within ~15%). Showing that once, on the same scheduler the rest of the paper
measures, turns "the jobs are simulated" from a limitation into a stated scope choice with a
demonstrated alternative.

**What it can claim, and what it cannot.** Be exact here — this is the section a reviewer
will test for over-reach.

| Claims | Does not claim |
|---|---|
| The scheduler places and executes a real DAG-structured scientific workflow, in dependency order, on WAN-distributed agents, with the same consensus and delegation machinery as every other cell | A makespan or throughput comparison against Pegasus. The substrates differ (a condorpool vs FABRIC VMs), Pegasus's 351 s makespan for soilmoisture was ~21 s of compute plus Condor queueing and staging, and SWARM has **no data transfer** yet — inputs sit on one shared export mounted at the same path on every agent |
| Outputs are byte-identical to the Pegasus run (computational outputs; a matplotlib PNG differs by font bytes) | Anything about data locality: the shared mount makes every agent equidistant from every file, which is exactly the variable the DTN penalties price. `--dtn-names local` is used, so no DTN term is active |
| Control-plane metrics — selection latency, consensus messages per job, DAG wave timing — measured on jobs that compute rather than sleep, under PBFT and under Snow on the same hosts | A scale result. It runs on 30 hosts, not the ladder; the jobs number in the tens, not thousands |
| Per-job wall time is real and uncapped, unlike the 120 s cap every other cell applies | Comparability with the trace cells' completion % or latency figures — different fleet, different job durations, reported in its own table only |

**Workflow.** The candidates are the workflows complete on the Pegasus host (code, catalogs,
container image, inputs — not just profile rows): `soilmoisture` (5 jobs, validated),
`nextgen` (19), `fedcast` (20), `tnseq` (35), `meerkat-2mass-mosaic` (59). `drought` and the
`s2_segmentation` runs that dominate the 25k-job trace exist only as profiles and cannot be
executed. **Primary: `nextgen`** — it is the one workflow present both in the replay trace
(two runs, 10 jobs each) and as a complete executable tree, so the paper can show the same
workflow name replayed at scale and executed for real. **Fallback: `soilmoisture`**, already
validated, if `nextgen` hits the clustered-job refusal (`WORKFLOW_EXECUTION.md` §1.3), an
external service the slice cannot reach (soilmoisture's own `fetch_soil_data` calls
open-meteo, which is blackholed from the slice), or an image that does not run under Apptainer.
`meerkat` is the more impressive DAG but has never been converted; do not discover its problems
in November.

**Fleet and configuration.** Mesh-30 at one agent per VM, site-interleaved, the *same 30 hosts*
for both engines. Real execution needs three things the ladder cells do not: Apptainer on every
host it can land on (installed on all 92, `setup_apptainer.sh --check` before the run), the
shared export mounted (`setup_nfs_workflow.sh --check`), and the container image on **local
disk** of every one of the 30 hosts (`--stage-image`; a 3 GB `.sif` read over the WAN at every
job start would dominate every timing taken). The fleet is generated once with
`generate_configs.py … --size-to-jobs <bundle>` and reused with `--use-config-dir` — sized, so
every job is feasible on every agent and a killed agent's job can go anywhere, which is also
what makes the churn variant below meaningful. **This fleet is not the campaign fleet and is
reported separately**; `--size-to-jobs` is forbidden on every ladder cell (§4).
`runtime.execution.mode: real` goes into the base config *before* generation; jobs then execute
and a refusal fails the job loudly rather than falling back to the sleep, so a misconfigured
cell cannot pass as a simulated one.

**Cells.** 3 repeats × {PBFT, Snow}, plus one churn run per engine (kill two agents mid-DAG;
the job recovers on another agent because the fleet is sized) — 8 runs, well under two hours
of slice time including image staging. **Metrics:** per-job exit status and output checksum
against the Pegasus run (the correctness column); DAG wave start times (the ordering column);
`selection_*` latency and consensus messages per job by engine (the control-plane columns,
same definitions as §7 of the master plan); real per-job wall time beside Pegasus's
`remote_duration`, labelled as descriptive, not comparative. **T3** is one table: jobs as rows,
those columns, both engines.

**Where it goes in the ten pages.** A quarter page in §V after F5, as T3 plus one paragraph;
the framing sentence in §IV ("every cell except §V.x simulates execution") costs one line.
Space comes from §VI, or from F6's slot if the 10 Nov gate is null. If the draft is not
review-ready on 27 Nov, this is cut **before** F5 — it is corroboration, not a claim.

**What it depends on, and what it must not do.**

- **No agent code.** Conversion runs on the Pegasus host, the bundle is data, and the runner
  (`swarm/execution/runner.py`) is already in the tree to be frozen. So the pilot is not on the
  freeze path. But a runner defect found in the pilot is a post-freeze code change, and the
  rule is the same as for P0-2/P0-3: the runner is exercised by no cell that runs
  `simulate`, verify that with the suite and a diff review, and then it may land.
- **It does not feed E2.** The regret oracle's ground truth is the *injected* failure profile;
  a job that really runs has a real exit status the profile did not predict. Never put a
  real-execution run through `oracle.py`, and never enable `mab.failure_simulation` on one.
- **The 10 Nov gate does not depend on it**, and neither does any figure. It is the paper's
  answer to one objection, sized accordingly.

**What would make it a result rather than a demonstration** — in the journal, not here:
transfer and stage-out between agents (the last thing between the current setup and a
defensible makespan), and Pegasus and SWARM on the same hardware. Both are listed in
`WORKFLOW_EXECUTION.md` §6. The conference version says exactly that in one sentence and moves
on.
