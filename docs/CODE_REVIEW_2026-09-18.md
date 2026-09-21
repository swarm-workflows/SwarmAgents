# Code review — 2026-09-18

Critical read of the code base against one question: **where can a number either paper reports
be wrong without anything in the run saying so?** Ranked by that. Every finding names the file
and line, the failing scenario, the metric it moves and the direction. Two are demonstrated by
tests in the tree (`tests/test_pbft_stragglers.py`, marked `xfail(strict=True)` so they flip
when fixed); the rest are verified by reading, with the mechanism traced end to end.

**Read directly** (~19k lines): `swarm/consensus/{engine,gossip_engine}.py`,
`swarm/agents/{resource_agent,agent_grpc}.py`, `swarm/agents/llm/*`, `swarm/database/repository.py`,
`swarm/comm/{grpc_transport,grpc_server}.py`, `swarm/rl/*`, `swarm/utils/{instrumentation,fleet_sizing}.py`,
`swarm/models/{job,object,agent_info}.py`, `swarm/selection/*`, `swarm/membership/swim.py`,
`swarm/gossip/disseminator.py`, `swarm/execution/runner.py`, `swarm/queue/*`, `evaluation/{collect,oracle}.py`,
`plotting/data.py` (the `all_jobs.csv` writer), `job_distributor.py`, `main.py`, `cleanup.py`,
`swarm-multi-start.sh`, `stop_agents_v2.sh`, and the parts of `run_test.py`, `generate_configs.py`
and `pegasus_to_swarm_converter.py` changed this week. **By a Codex pass, verified by reading and
fixed the same day** (commit `a02c0f84` and the defaults commit): six collector/launcher defects.
**Not reviewed in depth**: `plotting/{single_run,multi_run,comparison,mab}.py` (figure code, not
paper-figure code), `baselines/` beyond the scheduler core, `colmena_agent.py`, `swarm/quantum/`,
`pegasus_profile_extractor.py` internals, the topology builder in `generate_configs.py`.

---

## 1. PBFT stragglers re-enter the engine after quorum — **HIGH, demonstrated — FIXED the same day**

*Status:* fixed on both sides. `ConsensusEngine` remembers finalized `(object, p_id)` pairs
after the containers are cleared and skips their stragglers (`_finalized`, bounded; a new election
carries a new p_id, and the host calls `forget_decision` when a job returns to PENDING);
`ResourceAgent` marks a job decided at finalize (`_decided_jobs`, read by `is_job_completed`,
discarded wherever the completed set is discarded, assembled in `_init_decision_state` so test
doubles share it). One more defect surfaced by the new tests: participants recorded the sender of
the *last COMMIT* as the job's leader (`on_participant_commit(object, msg.agents[0]...)`), not the
proposer — a different wrong assignee per participant in `job_assignments`. Now `proposal.agent_id`.
Two rounds of the review gate sharpened it: `forget_decision` drops only `_finalized`, never the live `_commits_sent` (the host calls it from the periodic pending scan, which also sees in-flight elections), and that scan forgets a decision only on **evidence of a reset** — the fetched PENDING record's `last_transition_at` later than the decision (every return-to-pool path writes the record through the `Object.state` setter). A timer was tried first and rejected by the gate: a leader delayed before persisting READY leaves the same old record however long it takes, under exactly the load that also delays the stragglers. `tests/test_pbft_stragglers.py` (11 tests) is the regression suite; the two demonstration tests
that failed against the old engine now pass. **E5 still has to be measured on this revision.**

`swarm/consensus/engine.py:358,411` (`_forget_object` at finalize), `:389` (adopt via COMMIT),
`swarm/agents/resource_agent.py:3140` (`select_job`), `:1013` (`_update_ready_jobs`), `:161`
(`on_participant_commit`).

**Mechanism.** Finalization erases the object's `_commits_sent` entry and clears both proposal
containers, but nothing marks the job decided *locally*: `select_job` persists READY and enqueues
it, and `is_agreement_achieved` (= `completed_jobs_set`) is only fed by `schedule_job` ~0.5 s later
at a leaf — and at a **coordinator never**, until the delegation monitor marks the job COMPLETE,
because the delegation path in `scheduling_main` does not touch the set. Any tier wider than its
quorum produces `n − q` straggler PREPAREs and COMMITs per job. Each one finds the object (READY
in Redis), not "achieved", with empty containers, and **re-adopts the proposal with the sender's
wire `prepares`/`commits` lists**:

* a straggler PREPARE → wire prepares ≥ quorum, `_commits_sent` forgotten → **a second COMMIT
  broadcast** (`tests/test_pbft_stragglers.py`, scenario A: 2 COMMITs for one decision). This is
  the 2026-09-15 COMMIT-inflation fix undone by the `_forget_object` that fix introduced.
* a straggler COMMIT arriving with no PREPARE ahead of it → wire commits ≥ quorum →
  **`_record_finalize` again**: `finalized_count` and `votes_to_finalize` double, and the proposer
  takes the participant branch with the straggler as "leader" (scenario B: finalized 2, one
  election, `on_participant_commit(leader=5)` on the proposer). At a coordinator a re-elected job
  is **re-delegated**: `mark_submitted`/`mark_assigned` re-stamp the child-pool record and
  `delegated_jobs[job].delegated_at` resets, so `all_jobs.csv` timestamps move later.

**Paper impact.** F2 messages-per-job and `finalize`/`votes_` columns biased **against PBFT**;
hierarchical PBFT latency stamps shortened (flattering); worst in exactly the E1′/E5 collapse
cells, where `coordinator_cost_matrix: self` makes every coordinator a proposer. Frequency is
ordering-dependent (needs the straggler's wire list to reach quorum), so it will not show as a
constant factor — it will show as unexplained variance between repeats.

**Fix shape.** Make `is_agreement_achieved` true at finalize: a local `decided` set written in
`_HostAdapter.on_leader_elected`/`on_participant_commit`, consulted alongside
`completed_jobs_set`, and discarded where `_update_pending_jobs`/`_reassign_jobs_from_failed_agent`
already discard from the completed set. Keep `_commits_sent` and finalized ids for a grace window
rather than forgetting at finalize. **Must land before E0**; E5 must be re-measured on it. The
two xfail tests become the regression tests.

## 2. `--groups-per-coordinator 2` switches on fan-out duplication for every analytic cell — **HIGH — FIXED (option b)**

*Status:* the no-bandit path in `_select_child_groups` now delegates to `mab.top_k` groups chosen at random, recorded as `random` (a fan-out covering every candidate is still `all`). One fan-out key for both paths. `tests/test_fanout_default.py` (7 tests).

Recorded today in `CCGRID27_PAPER_PLAN.md` §4 and `FGCS_EVAL_PLAN.md` Appendix A.10: with `mab.enabled:
false` a coordinator delegates to every capable group and each group executes the job (job key is
`job:<level>:<group>:<id>`, `repository.py:340`; the CAS is per group). At G=1 that was latent. E0
and the analytic half of E1′ now duplicate every job unless they pass `--groups-per-coordinator 1`.
Recommended: the no-bandit path picks one group at random (`_select_child_groups` already has the
random pick as the LLM fallback). Decide before E0.

## 3. Hierarchical latency columns exclude the coordinator tier by construction — **HIGH, measurement — FIXED 2026-09-20**

*Status:* `plotting/data.py save_jobs(level=None)` now takes `submitted_at` as the earliest stamp (arrival at the top tier), so `scheduling_latency`, `job_latency_*`, `makespan_s` and `throughput_jobs_per_s` include the coordinator tier; a new `selection_total` column sums consensus time over every tier, summarised by `collect.py` as `selection_total_*`. `selection_*` stays the leaf tier's, as documented. Flat runs are unchanged. `tests/test_all_jobs_export.py`. The gate then caught that `collect.py`'s dedup — sorting on `completed_at` alone — kept the coordinator's copy of a job still RUNNING at teardown (both records carry `completed_at = 0`, and the coordinator's is listed last), whose `selection_total` covers one tier; dedup now sorts on the progress tuple (`completed_at`, `started_at`, `assigned_at`, `selection_total`, `selection_started_at`) so the most advanced record wins. Every hierarchical `all_jobs.csv` written before 2026-09-20 carries the delegation-time `submitted_at`; re-export from Redis dumps where they exist, or do not compare its latency columns with new runs.

`plotting/data.py:267-268`, `swarm/models/job.py` (`mark_submitted` stamps `self.level`),
`swarm/agents/resource_agent.py` `scheduling_main` (sets `job.level = level−1` then
`mark_submitted()` at delegation), `evaluation/collect.py:792` onward.

`save_jobs(level=None)` takes `submitted_at = submitted_at_dict.get(0)` — for a hierarchical run
the level-0 stamp is the **delegation** time, while the original submission sits under the top
level's key — and `selection_started_at`/`assigned_at = max()` over levels, i.e. the leaf tier's.
So `all_jobs.csv`'s `scheduling_latency`, and `collect.py`'s `selection_*`, `job_latency_*`,
`makespan_s`, `throughput_jobs_per_s` **all exclude the coordinator tier**, which is where PBFT
collapses. The collapse appears only in completion % and `l1_selection_*`. For selection this is
a documented choice ("level-0 is the headline"); for job latency, makespan and throughput it is
silent, and E1′'s "selection/decision latency" will read as leaf-tier only.

**Fix shape.** `submitted_at = min(submitted_at_dict.values())` for the `level=None` export (or a
`submitted_at_top` column), a `selection_total` = sum over levels, and a sentence per figure
naming which level's selection it plots.

## 4. The selection barrier has no timeout — **HIGH, robustness — FIXED 2026-09-20**

*Status:* `_await_peers()` (one implementation, both agents) releases at `live >= configured` or after `runtime.selection_barrier_s` (default `failure_threshold_seconds`), logs `[SEL_BARRIER]` with who is missing, records `startup_barrier` in the metrics payload, and `collect.py` surfaces `barrier_short_agents`/`barrier_short_max`. `tests/test_selection_barrier.py` (6 tests).

`swarm/agents/resource_agent.py:2384`, `swarm/agents/llm/llm_agent.py:1243`:
`while live_agent_count != configured_agent_count: sleep(0.5)`. One agent in a group that never
registers — a crash at startup, the old LLM-coordinator/API-key case — leaves **every other agent
in that group waiting forever**: no selection ever starts and the run looks busy until the cap. It
is also an equality test, so a group that ends up one agent *over* (a dynamic agent registering
early) never releases either. This is the fleet-wide form of the "two log lines then silence"
symptom in CLAUDE.md. Fix: a bounded wait (`peer_expiry_seconds` is the natural bound) that then
proceeds with whoever is live, logging who is missing; and `>=` rather than `!=`.

## 5. Coordinators never detect child failures by staleness — **MEDIUM-HIGH, E2b/F5 interpretation — FIXED 2026-09-20**

`resource_agent.py:1565` (`_refresh_agent_map`: an existing entry is only *updated* when Redis
has a fresher record; it is removed only when the key is **gone**), `:756`
(`_get_live_child_groups`, whose docstring says stale entries are pruned by peer expiry — they are
not, for existing entries). `_detect_failed_agents` scans `neighbor_map` (same tier) only. Agent
keys have TTL `max(60, 2 × peer_expiry_seconds)` = **600 s**. So a dead child group stays in
`children` for up to ten minutes, its headroom computed from a frozen record reads idle, and the
liveness gate does not gate. What actually steers the bandit away is its own timeout signal
(`delegation_timeout_s` per job) — so **F5 "time-to-re-adoption" measures delegation-timeout
learning plus Redis TTL, not liveness detection**, and Scenario C's dog-piling fix is weaker than
the design doc says. Fix: apply the `peer_expiry_seconds` staleness test to existing entries in
`_refresh_agent_map` (children and neighbors alike).

**Fixed 2026-09-20** (`tests/test_child_staleness.py`, 11 tests; 7 fail against the pre-fix
tree). An existing entry whose Redis record has stopped moving is now evicted at the staleness
threshold rather than at the key's TTL, and its `_agent_seen_at` stamp goes with it; the
insertion path already refuses a record that old, so a group returns only when it writes a
fresh one, which is exactly a rejoin. Two things the straightforward version got wrong and
that the tests pin:

* **The same tier has an owner already.** `_detect_failed_agents` scans `neighbor_map` at
  `failure_threshold_seconds` and is the only thing that reassigns a dead peer's jobs.
  Evicting an entry before the detector has judged it removes the peer with no failure ever
  recorded and strands every job it held — the 2026-09-15 defect reopened from the other end.
  The shipped config orders the two safely (60 s vs 300 s) but nothing enforced it, and
  `peer_expiry_seconds` was silently 45 s under the duplicate-key bug, which is the inverted
  order. `_staleness_eviction_threshold` floors the own-tier threshold at the detector's,
  jitter headroom included; child tiers use `peer_expiry_seconds` as written.
* **A re-read of an unchanged record must not re-stamp `_agent_seen_at`**, or the context-age
  column reads zero for a peer that has gone silent. That invariant predates this change and
  is now asserted here too.

Consequence for the plans: F5 measures liveness detection from this revision on. Any earlier
time-to-re-adoption number is delegation-timeout learning plus a 600 s Redis TTL and is not
comparable.

## 6. SWIM is not advisory for consensus traffic — **MEDIUM — FIXED 2026-09-20**

`swarm/agents/agent_grpc.py:410` (`broadcast` skips `swim.failed_agents()`),
`swarm/membership/swim.py:524` (`_merge_one` accepts FAILED over ALIVE by rank regardless of
incarnation). SWIM false-fails under bursts (documented in the adapter). A false FAILED silences
consensus traffic to a live peer while quorum is still computed from `neighbor_map` (Redis), which
includes it: under PBFT the peer cannot vote and the job waits for reselection; under Snow it merely
abstains. The docs say heartbeat is authoritative; for `broadcast` it is not. Either skip only
peers in `failed_agents` (heartbeat), or make `calculate_quorum` use the same live set.

**Fixed 2026-09-20** (`tests/test_swim_advisory.py`, 16 tests; 10 fail against the pre-fix
behaviour). Took the first option, and fixed the merge as well — the two are one defect seen
from either end.

* **`broadcast` no longer consults SWIM.** `Agent.consensus_skip_set` is heartbeat's failed
  set alone, which is a subset of what `neighbor_map` already excludes, so the peers an agent
  refuses to talk to and the peers it counts towards quorum are one set by construction.
  Lowering the quorum instead was the wrong half of the choice: SWIM's view differs per agent
  and false-fails in bursts, so subtracting it from the denominator would let two disjoint
  quorums form under exactly the conditions §10 already says PBFT has no exactly-once for.
  The efficiency given up is small — the skip and the fire-and-forget broadcast pool landed in
  the same commit (`85f26208`), and it is the pool that removed the ~8.7 s serial block per
  dead peer per phase; what is left is pool slots held until heartbeat evicts the peer, which
  the bounded semaphore sheds and counts.
* **`_merge_one` compares incarnation first, severity second** (`_supersedes`). Rank dominance
  alone rejected the ALIVE-at-incarnation+1 that `_maybe_refute` emits, so a FAILED verdict was
  permanent — and since `_pick_probe_target` never probes a FAILED peer, there was no second
  route back. A false positive removed a live peer from Snow's sample and the gossip fan-out
  for the rest of the run. A stale rumour below the subject's current incarnation is now
  dropped however severe, which is the same rule the widely deployed implementation uses.

Consequence for the plans: SWIM was capable of silently shrinking the effective voter set of a
PBFT tier under load, and the regime it fired in most is the collapse cell. **No E5/F2 message
or finalization number measured before this is citable** — which is already true of that cell
for §1.

## 7. `job_selection.selection_threshold_pct` is inert — **MEDIUM, documented knob does nothing — FIXED (deleted) 2026-09-20**

`swarm/selection/engine.py:292`: the threshold is tested against the argmin's own cost, so
`sel_cost > best × (1 + pct)` is never true (the docstring admits it). CLAUDE.md and the README
describe it as the candidate-pool tuning knob. Either implement "within pct of best" as a pool or
delete the key; today it selects nothing.

**Deleted 2026-09-20** (`tests/test_selection_threshold.py`, 10 tests; 5 fail against the
pre-fix tree, and the call-site check names all four). Not implemented, for two reasons. A
tolerance around the best only means anything for a function that returns a *pool*, and
`pick_agent_per_candidate` returns one winner per column — the parameter had no correct form
there. And 57 generated config files carry `selection_threshold_pct: 10.0`; giving that value a
real effect would silently change the bidding regime of every one of them, which is the drift
"one key, one default" exists to prevent.

* The parameter is **gone from the signature**, so a surviving call site is a `TypeError` at
  the first selection pass rather than a stale no-op. All four call sites are updated —
  resource, LLM ×2 (including the P0-8 designation path), and colmena, which hardcoded `10.0`
  next to a TODO to read it from config. `accept_if`, the *absolute* gate, was never broken
  and stays.
* An agent whose config still sets the key logs `[CONFIG] ... is IGNORED and always was` once
  at startup, via `_warn_removed_job_selection_keys`. The message says there is **no**
  replacement for a rule-based fleet, and names `designate_bidder` only with its restriction:
  `_designate_bidders` runs in `LlmAgent.selection_main` alone, so recommending it flatly
  would have answered one silently-inert key with a second one.
  Presence is tested with `is not None`, because `0.0` is falsy and that is exactly the config
  that believed it had disabled the pool.
* It was **worse than inert at the edge**: `sel_cost` and `best` are the same number, so the
  comparison reduces to `best > best × (1 + pct/100)` — false for a non-negative best, *true*
  for a negative one, where it would have discarded every assignment. Costs are 0-100 today,
  so this was latent.

Two documents were making load-bearing claims on it: `docs/COMPLEXITY.md` cited it as the
second of two mechanisms bounding proposals per job, and `docs/GOSSIP_CONSENSUS_DESIGN.md` said
it "ensures that typically only 1-3 agents propose". Both now say what actually bounds it — an
agent proposes only when it is its own argmin — which is *tighter* than they claimed, so
neither analysis weakens. The measured ~3.7 bidders/job comes from agents' views of their peers
differing, not from any tolerance.

## 8. The connectivity term prices `local` at zero — **MEDIUM, workflow cells — FIXED 2026-09-20**

`resource_agent.py:2296`: `compute_job_cost` does not exclude `local` from required DTNs (feasibility
and `_job_sig` do), so every `--dtn-names local` job scores `local` at 0.0 on every agent →
`avg_conn = 0` → `connectivity_penalty = 1 + factor` → **every cost doubled**, uniformly. Ranking
is unaffected; absolute costs, the LLM-vs-analytic 0–100 comparison and the tie-break reference
(`tie_break_ref_cost: 11.85`) are shifted for exactly the workflow replay/real cells. One-line fix.

**Fixed 2026-09-20** (`tests/test_local_dtn_cost.py`, 13 tests; 9 fail against the pre-fix
tree). Not a one-line fix in the end, because routing the set through one definition turned up
a **second site with the same bug and a worse outcome**.

* `Job.required_dtns()` is now the only definition, and nothing outside it reads the
  `_required_dtns_cache` behind it — the cache is populated lazily, so a direct reader that ran
  first would raise. Feasibility, `_job_sig`, `compute_job_cost` and the child-group filter all
  call it. The set had **six** derivations (those four plus `fleet_sizing` and the runner's
  shape summary) and two of them disagreed.
* `_get_child_groups_for_job` built the same inline set without the exclusion. No child holds a
  DTN named `local`, so an all-local converted-workflow job matched **no** group, fell through
  the "delegate to all active groups" fallback, and logged two warnings per job — one of them
  saying "feasibility check may have passed incorrectly", blaming the component that had
  correctly ignored `local` all along. So on a hierarchical workflow cell the DTN capability
  filter was inert and the bandit's candidate set was every active group rather than the
  capable subset. That is a C1 measurement, not just a cost shift.
* **The mixed case was never a uniform shift.** The finding says ranking is unaffected, which
  holds for an all-local job. A job naming `local` *and* a real DTN averaged the real score
  with 0.0, halving it — the penalty still varied per agent, so two agents differing in both
  base cost and connectivity could reorder. Pinned by a test.

Consequence for the plans: every converted-workflow cell, replay and real. Absolute costs were
doubled at the shipped `connectivity_penalty_factor: 1.0`, which moves the LLM-vs-analytic
0–100 comparison and the `tie_break_ref_cost: 11.85` reference; hierarchical workflow cells
additionally delegated without the DTN filter.

## 9. Completion % counts failed jobs — **MEDIUM, definition — FIXED 2026-09-20**

*Status:* `collect.py` now reports `jobs_succeeded`, `success_pct` and `success_pct_of_seen` beside the completion columns; `completion_pct` keeps meaning *finished* (the right denominator for latency and makespan). A caption says which.

`evaluation/collect.py:792`: `is_complete = completed_at > 0`, exit status ignored. Injected exit
failures count as completed; so does a job `_reassign_delegated_job` retires after
`max_delegation_attempts` (`resource_agent.py:1368` writes COMPLETE with `exit_status=1`). CLAUDE.md's
"failure-injection completion % is lower and honest" holds only for jobs that never finish. Define
it in the plans (finished vs succeeded) or add `jobs_succeeded`.

## 10. Exactly-once is a Snow property; PBFT has none under partition — **E6 scoping — RESOLVED (scoped) 2026-09-20**

`calculate_quorum` (`resource_agent.py:4073`) is `live//2 + 1` with a floor of 1, from each agent's
own `neighbor_map`. Under a partition both sides reach quorum; PBFT has no CAS. Expect > 0 double
assignments in PBFT partition cells and say so; the claim "safety comes from the CAS" is true of
Snow only.

**Scoped 2026-09-20.** Re-checked in the tree first, because the claim is about what the paper
may assert: `try_claim_assignment` has exactly one caller, `gossip_engine.py:577`. PBFT's
`select_job` persists the job READY and claims nothing. `calculate_quorum` is
`live_agent_count // 2 + 1` with `max(1, …)`, over each agent's *own* `neighbor_map` — so a
two-way split of 20 agents leaves each side with 10 live, each side's quorum at 6, and each side
able to reach it alone. The floor means a fully isolated agent has quorum 1.

So the statement E6 was going to make is a **Snow** statement, and making it protocol-neutral
would be a safety claim the system does not have. Written into `FGCS_EVAL_PLAN.md` §E6 and the
conference plan: the exactly-once argument is scoped to Snow, the PBFT partition cell is
reported as a measurement rather than a guarantee, and a non-zero count there is the expected
result rather than a failed run.

**The audit has a vacuity trap and it is now recorded as an obligation.** E6 audits
double-assignment "via the Redis `SET NX` claim keys". PBFT writes no claim keys, so that audit
returns 0 for every PBFT cell — *absent* read as *zero*, which is the same mistake as the
validity columns in §9 and §4. No double-assignment number may be reported for a PBFT cell from
a claim-key audit; detecting it there needs evidence that two agents executed the same job,
which no per-agent metric carries today (checked: `Metrics` records no per-job id list). Either
that evidence gets added before E6, or the PBFT column says "not measured", never "0".

Related: §5 shortened partition *detection* (a peer now goes stale at `peer_expiry_seconds`
rather than at the Redis key TTL), so E6's heal-time numbers are on the new behaviour.

## 11. Smaller — **ALL ADDRESSED 2026-09-20**

* `gossip_engine.py:580` — a Snow CAS claim followed by `get_object() is None` fires no callback and
  is counted nowhere (only exceptions reach `finalize_errors`). Recoverable only if the winner
  re-proposes. Count it.
* `LlmAgent.selection_main` has no `_data_predicate_ready` gating and no BLOCKED/infeasible handling
  — an LLM-agent run of a DAG-gated workflow ignores the DAG (journal-side; the §8 case study is
  resource agents).
* `baselines/scheduler.py:117-119` — `is_feasible` does not exclude `local`, so E7 baselines cannot
  run a `--dtn-names local` job at all (campaign jobs carry real names; matters only if E7 ever
  takes a workflow bundle).
* `engine.py:120` — PBFT `abandoned: 0` is hard-coded (PBFT leaves stuck objects to reselection);
  never compare `abandoned` across protocols.
* `_restart_selection` mutates `completed_jobs_set` without `completed_lock`; Snow's `conflicts`
  dict is unbounded where PBFT caps it; `TopologyType` members are 1-tuples (trailing commas) —
  harmless, compared by name.

**All five addressed 2026-09-20** (`tests/test_review_smaller_items.py`, 13 tests; 9 fail
against the pre-fix tree, plus updates to three tests in `test_review_fixes.py`).

1. **The lost Snow finalization is now its own outcome.** It was worse than uncounted: it was
   counted as a *finalize*. `finalize_lost` counts a decision whose CAS won but whose object
   could not be read, it stays out of `finalized` and out of the rounds/queries/time
   distributions (those describe decisions that placed a job), and the warning says whether the
   idle claim is held by this agent. `finalized + abandoned + errors + lost` is the population.
   `collect.py` carries `consensus_finalize_lost`. An existing test asserted the old behaviour
   on the grounds that a vanished object "is not an error" — still true, and why it is a third
   counter rather than folded into `finalize_errors`; the test is rewritten to say so.
2. **PBFT no longer reports `abandoned` at all.** A hard-coded 0 stood next to Snow's measured
   count, so the column compared a measurement against a placeholder. It is absent for the same
   reason `rounds_*` already was, and `collect.py` reports the column only when some agent
   reported it — a measured 0 still prints as 0.
3. **`baselines/scheduler.py` had both halves of §8**, not just feasibility. `is_feasible`
   rejected every `--dtn-names local` job, so E7 could not take a workflow bundle at all; and
   `compute_cost` doubled it, while its docstring promises a formula identical to the agent's.
   Both now call `Job.required_dtns()`, so the promise holds.
4. **Snow's `conflicts` is bounded** at the same 4096 as PBFT's, through a `_bump_conflict`
   mirroring it. It was unbounded on the engine that runs by default, and the two protocols'
   conflict columns were not counted the same way.
5. **`_restart_selection` already takes `completed_lock`** — it came with the §1 work, verified
   rather than assumed. **`TopologyType`** no longer has trailing commas; `Ring`/`Star`/`Mesh`
   were `(1,)`/`(2,)`/`(3,)` while `Hierarchical` was `4`. Nothing reads `.value` (checked), so
   this changed no behaviour.

**The LLM agent now honours the DAG.** `LlmAgent.selection_main` overrides the whole selection
loop and had no `_data_predicate_ready` gate, so an LLM-agent run of a DAG-gated workflow could
schedule a child before its parent wrote the files it reads. The gate is extracted as
`_gate_on_data_predicates` and called from both loops — two copies is how one of them came to be
missing — and in the LLM loop it runs *before* designation, so a job whose parents have not
finished spends no inference. It filters by identity, not `in`: two jobs comparing equal would
otherwise both vanish when one was gated.

**The BLOCKED/infeasible half is deliberately left alone.** The LLM loop keeps such a job
PENDING rather than BLOCKED, and says why in place: `_restore_infeasible_jobs` is called only by
`ResourceAgent.selection_main`, so a BLOCKED job set here would never come back. Changing that
would strand jobs, not fix them.

## 12. Found by the Codex pass and fixed today (commit `a02c0f84`, earlier commits)

Converter `--generate-agent-configs --base-config` `NameError` (this session's regression); collector
read absent context-validity columns and a missing `pending_jobs.csv` as zero; LLM coverage
denominator was the reporting agents, not the launched fleet, and a wholly silent LLM tier or a
coordinator dead before registering was certified; `_effective_delegation_policy`/`_effective_config`
read `<config_dir>/*.yml` first-file, wrong directory for local runs.

---

## Checked and found correct

Transport counting (sent before the call, retries excluded, drops separate, inbound before parse,
under a lock); `RunningStats` reservoir; `DecisionLog` ring and aggregates; `SelectionCounters`
distinct-job rule; PBFT one-COMMIT-per-proposal *while the object is live*; Snow counters under
`_stats_lock`, `finalize_errors`, dispatched-only `queried`, batch send path; `repository.save`
WATCH/MULTI with `produced_data` in the same MULTI; `release_assignment` + `try_claim_reassignment`;
`_reassign_jobs_from_failed_agent` from Redis `leader_id`; P0-9 reward timing (`schedule_job`
RUNNING, only `execute_job` COMPLETE, FAILED terminal); `execute_job` separation of execution from
persistence, produced names only on exit 0; runner refusals never fall back to the sleep, atomic
`os.link` staging, process-group kill and reap, environment denylist; oracle refusals (no fallback
clock, every candidate priced, equal-reward decisions excluded), Poisson-binomial validation band;
LinUCB Sherman–Morrison update, schema-version discard, epsilon clamps; `stop()` re-entry/RLock
ordering; `run_test.py` guards changed this week (launched-config scoping, three states).

## Recommended order before the 12 Oct freeze

1. §1 (straggler re-finalization) — the two xfail tests are the acceptance criterion.
2. §2 (fan-out default) — one decision, ~20 lines.
3. §3 (top-level `submitted_at`, per-level selection naming) and §9 (completion definition) —
   collector/plotting only; no agent code, but E0's numbers depend on them.
4. ~~§4 (barrier timeout) and §5 (child staleness)~~ — both done 2026-09-20; both changed
   failure behaviour, so they had to land before E0/E2b.
5. ~~§6 (SWIM advisory)~~ — done 2026-09-20.
6. ~~§7 (inert threshold)~~ — done 2026-09-20.
7. ~~§8 (local priced at zero)~~ — done 2026-09-20. It was two sites, not one.
8. ~~§10 (E6 scoping)~~ and ~~§11 (smaller)~~ — done 2026-09-20.

**All twelve findings are closed as of 2026-09-20.** §10 was resolved by scoping the claim
rather than by code: the exactly-once argument is Snow's and the paper now says so. Everything
else landed as a fix with tests that fail against the pre-fix tree.

Three of them were larger than the finding described, which is worth remembering the next time
one of these reads as "one-line":

* §7's replacement advice was itself a no-op — `designate_bidder` is read only by `LlmAgent`,
  so a resource-agent operator told to use it would have been handed a second dead key.
* §8 was two sites, and the second one disabled the DTN capability filter for hierarchical
  workflow delegation rather than merely shifting a cost.
* §11's first bullet said the lost Snow finalization was "counted nowhere". It was counted as a
  *success*, which is the opposite of nowhere and worse.

**Numbers invalidated by this pass**, collected in one place: E5/F2 message and finalization
counts (§1, §6), every hierarchical latency and completion column (§3, §9), F5 re-adoption
(§5), and every converted-workflow cost, LLM-vs-analytic comparison and hierarchical workflow
delegation (§8). `docs/FGCS_EVAL_PLAN.md` rows F-19 through F-23 carry the detail.
