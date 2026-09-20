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

Recorded today in `CCGRID27_PAPER_PLAN.md` §4 and `FGCS_EVAL_PLAN.md` §0.10: with `mab.enabled:
false` a coordinator delegates to every capable group and each group executes the job (job key is
`job:<level>:<group>:<id>`, `repository.py:340`; the CAS is per group). At G=1 that was latent. E0
and the analytic half of E1′ now duplicate every job unless they pass `--groups-per-coordinator 1`.
Recommended: the no-bandit path picks one group at random (`_select_child_groups` already has the
random pick as the LLM fallback). Decide before E0.

## 3. Hierarchical latency columns exclude the coordinator tier by construction — **HIGH, measurement — FIXED 2026-09-20**

*Status:* `plotting/data.py save_jobs(level=None)` now takes `submitted_at` as the earliest stamp (arrival at the top tier), so `scheduling_latency`, `job_latency_*`, `makespan_s` and `throughput_jobs_per_s` include the coordinator tier; a new `selection_total` column sums consensus time over every tier, summarised by `collect.py` as `selection_total_*`. `selection_*` stays the leaf tier's, as documented. Flat runs are unchanged. `tests/test_all_jobs_export.py`. Every hierarchical `all_jobs.csv` written before 2026-09-20 carries the delegation-time `submitted_at`; re-export from Redis dumps where they exist, or do not compare its latency columns with new runs.

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

## 4. The selection barrier has no timeout — **HIGH, robustness**

`swarm/agents/resource_agent.py:2384`, `swarm/agents/llm/llm_agent.py:1243`:
`while live_agent_count != configured_agent_count: sleep(0.5)`. One agent in a group that never
registers — a crash at startup, the old LLM-coordinator/API-key case — leaves **every other agent
in that group waiting forever**: no selection ever starts and the run looks busy until the cap. It
is also an equality test, so a group that ends up one agent *over* (a dynamic agent registering
early) never releases either. This is the fleet-wide form of the "two log lines then silence"
symptom in CLAUDE.md. Fix: a bounded wait (`peer_expiry_seconds` is the natural bound) that then
proceeds with whoever is live, logging who is missing; and `>=` rather than `!=`.

## 5. Coordinators never detect child failures by staleness — **MEDIUM-HIGH, E2b/F5 interpretation**

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

## 6. SWIM is not advisory for consensus traffic — **MEDIUM**

`swarm/agents/agent_grpc.py:410` (`broadcast` skips `swim.failed_agents()`),
`swarm/membership/swim.py:524` (`_merge_one` accepts FAILED over ALIVE by rank regardless of
incarnation). SWIM false-fails under bursts (documented in the adapter). A false FAILED silences
consensus traffic to a live peer while quorum is still computed from `neighbor_map` (Redis), which
includes it: under PBFT the peer cannot vote and the job waits for reselection; under Snow it merely
abstains. The docs say heartbeat is authoritative; for `broadcast` it is not. Either skip only
peers in `failed_agents` (heartbeat), or make `calculate_quorum` use the same live set.

## 7. `job_selection.selection_threshold_pct` is inert — **MEDIUM, documented knob does nothing**

`swarm/selection/engine.py:292`: the threshold is tested against the argmin's own cost, so
`sel_cost > best × (1 + pct)` is never true (the docstring admits it). CLAUDE.md and the README
describe it as the candidate-pool tuning knob. Either implement "within pct of best" as a pool or
delete the key; today it selects nothing.

## 8. The connectivity term prices `local` at zero — **MEDIUM, workflow cells**

`resource_agent.py:2296`: `compute_job_cost` does not exclude `local` from required DTNs (feasibility
and `_job_sig` do), so every `--dtn-names local` job scores `local` at 0.0 on every agent →
`avg_conn = 0` → `connectivity_penalty = 1 + factor` → **every cost doubled**, uniformly. Ranking
is unaffected; absolute costs, the LLM-vs-analytic 0–100 comparison and the tie-break reference
(`tie_break_ref_cost: 11.85`) are shifted for exactly the workflow replay/real cells. One-line fix.

## 9. Completion % counts failed jobs — **MEDIUM, definition — FIXED 2026-09-20**

*Status:* `collect.py` now reports `jobs_succeeded`, `success_pct` and `success_pct_of_seen` beside the completion columns; `completion_pct` keeps meaning *finished* (the right denominator for latency and makespan). A caption says which.

`evaluation/collect.py:792`: `is_complete = completed_at > 0`, exit status ignored. Injected exit
failures count as completed; so does a job `_reassign_delegated_job` retires after
`max_delegation_attempts` (`resource_agent.py:1368` writes COMPLETE with `exit_status=1`). CLAUDE.md's
"failure-injection completion % is lower and honest" holds only for jobs that never finish. Define
it in the plans (finished vs succeeded) or add `jobs_succeeded`.

## 10. Exactly-once is a Snow property; PBFT has none under partition — **E6 scoping**

`calculate_quorum` (`resource_agent.py:4073`) is `live//2 + 1` with a floor of 1, from each agent's
own `neighbor_map`. Under a partition both sides reach quorum; PBFT has no CAS. Expect > 0 double
assignments in PBFT partition cells and say so; the claim "safety comes from the CAS" is true of
Snow only.

## 11. Smaller

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
4. §4 (barrier timeout) and §5 (child staleness) — both change failure behaviour, so they belong
   before E0/E2b, not after.
5. §7, §8 — small, and §8 touches every workflow cell.
