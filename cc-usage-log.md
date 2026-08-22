# Claude Code — session usage log

Per-session records for `SwarmAgents-chaos` (branch `chaos`). Append-only; newest at the bottom.

## Session: 2026-08-21 18:14

> Section labels in this entry (§4c, §4d, §4f.2, …) are the test plan's **pre-2026-08-22**
> numbering. The entry is left as written because it is a dated record; the old→new table at
> the end of the plan's Contents resolves them.

- **Project**: SwarmAgents-chaos (`/Users/kthare10/swarm/agents/SwarmAgents-chaos`, branch `chaos`)
- **Task summary**: Closed S05's blast radius on the FABRIC gateway arm (25% and 50%), which
  confirmed the campaign's headline partial-outage finding on a clean baseline rather than a
  contaminated one. Restructured the 1,300-line chaos test plan from arm-and-date ordering to
  one section per CJ scenario. Found and fixed six silent metric defects — four in the chaos
  harness, two in production RL.
- **Workflow stage**: experiment execution → analysis → documentation → bug fixing → testing → shipped
- **Prompts**: 9 user prompts (excludes 5 stop-hook review messages, which are system-generated)
- **Tool calls**: ~135 (counted from the transcript, not instrumented — treat as ±5)
- **Agent tasks**: 0 sub-agents spawned deliberately. 1 skill invoked (`gpg-commit`). The Codex
  stop-review gate spawned its own reviews automatically; one (`task-mt3b0o8e-99cih7`) wedged for
  3h 2m and was cancelled.
- **Models used**: Opus 5 (`claude-opus-5[1m]`) for the whole session. Codex/GPT models were used
  by the automatic stop-time review gate (OpenAI credits, not counted here).
- **Estimated cost (USD)**: not instrumented — no per-session token accounting was available in
  this environment, so any figure would be a guess. Recording as unavailable rather than inventing
  one. Rough order of magnitude from context growth: single-digit dollars on Opus 5.
- **Input tokens**: unavailable (not instrumented)
- **Output tokens**: unavailable (not instrumented)
- **Files created**: 1 tracked — `tests/test_scenario_placement.py` (238 lines, 16 tests). Plus
  this log, and 5 throwaway analysis scripts in the session scratchpad (not in the repo).
- **Files modified**: 9 tracked — `CHAOS_JUNGLE_LLM_TEST_PLAN.md`, `SWARMAGENTS_FINDINGS.md`,
  `scenarios/helpers.py`, `scenarios/api/s05_unavailable.py`, `scenarios/api/s01_latency.py`,
  `swarm/rl/bandit.py`, `tests/test_bandit.py`, `docs/MAB_README.md`, `config_swarm_multi.yml`
  (+1,370 / −512 across 10 files)
- **Experiment runs**: 2 completed on the 30-agent FABRIC slice (`cj-s05-25pct-gw2`,
  `cj-s05-50pct-gw2`), each 300 jobs. A third batch (S09 `rag_poison`, `inject_distractor`,
  `context_truncate`) was launched at the end of the session and was still running.
- **Commits**: 6, all GPG-signed, pushed to `origin/chaos` (`802e7d34`, `432263ff`, `271424cc`,
  `b4893d4e`, `d41051b0`, `da6d2949`). The push also carried 9 older commits from previous
  sessions that had never left the machine.
- **Tests**: 152 → 171 passing (+19). `test_repository.py` still cannot collect — `fakeredis` is
  not installed, pre-existing and unrelated.

### Key decisions / milestones

1. **S05 gateway sweep — the headline survived its own control.** The partial-outage finding
   ("8 of 30 LLM-blind agents capture 86–93% of the workload") had only ever been measured on the
   local arm, whose fairness baseline the doc itself said was contaminated by heterogeneous host
   inference. Re-measured on the gateway arm it is *deeper*, not weaker: fairness 0.849 → 0.253 at
   25% (−0.596, vs −0.472 locally). L1 and L3 are now both complete on the arm the campaign runs on.
2. **Six silent defects, all biased toward the wrong answer.** Every one produced a plausible
   number rather than an error:
   - capture ratio divided by agents *named in the log*, so agents winning zero jobs left their own
     group's denominator (local 25%: 19.25× → 38.5×)
   - Jain's fairness had the same bug via `n = len(placed)`, inflating fairness by `n_logged/fleet`
     (gateway 25%: 0.304 → 0.253)
   - the capture-ratio control was pinned to the local baseline while `CJ_REFERENCE` was
     env-overridable, so a gateway run quoted the wrong arm's 1.86× control instead of its own 1.27×
   - `collect()` and `load_split()` parsed one log two ways; a restart run emits *two* blocks
     (`[all]` and `[no_restarts]`), so one double-counted and the other silently swapped populations
   - a lone non-`[all]` block was accepted as the whole fleet
   - `epsilon_min` overrode a requested `epsilon=0.0`, then persisted Redis state overrode the fix
3. **The recurring shape, worth remembering:** every defect was a *denominator or population*
   question answered by a convenient default — divide by whoever showed up in the log, assume a
   repeated block is a revision, assume a lone block is the whole thing, assume a config value
   sticks. The harness now refuses rather than guesses in all of those cases, because a wrong
   choice here never fails, it just moves the number.
4. **Jain's fairness qualified, not trusted.** Under a partial outage it is largely a restatement
   of the blast radius: if the k faulted agents took every job evenly J would be exactly k/n, and
   the gateway 25% run sits at 95% of that ceiling. Also capacity-blind on a deliberately
   heterogeneous fleet, and computed over jobs placed rather than work done. Reframed as
   *effective active agents* (`J × n` — 7.6 of 30 at 25% outage, against exactly 8 faulted) and
   demoted to corroboration of the capture ratio. A feasibility-normalised share is the metric that
   would actually answer the question; not yet implemented.
5. **Doc restructured by scenario.** S01 → §4c, S05 → §4d, S09 → §4e, each holding every arm and
   blast radius with its own cross-arm replication table, plus a scenario index and cross-scenario
   findings. Renamed the matrix's Tier-2 rows `T2-1…T2-4` so they stop colliding with CJ's
   `S01/S05/S09` ids, and moved §2.3 ahead of §3.
6. **Two production RL bugs filed and fixed** in `swarm/rl/bandit.py`: a pure-greedy policy was
   unconstructable (`epsilon_min` floor silently reinstating 1% exploration), and persisted state
   could restore an epsilon outside what the current config allows. The first was already
   half-known — one test carried a workaround and a comment describing the bug exactly, while
   another test lacked it and failed ~1 run in 11 under `pytest-randomly`. Shipped config path
   verified bit-for-bit unchanged.
7. **New finding 12** (hierarchical per-agent summaries are all labelled `[no_restarts]`, making a
   run's own blocks indistinguishable), and the test plan's findings table synced — it had drifted
   three entries behind `SWARMAGENTS_FINDINGS.md`.
8. **Recommended next**: the fallback-disabled ablation (paper figure D). §4f.2 established that
   the LLM's *output* barely reaches the scheduler while its *timing* dominates, so removing the
   cheap path that wins every race is the direct test of the analytic safety net's value.

## Session: 2026-08-22 18:50

- **Project**: SwarmAgents-chaos (`/Users/kthare10/swarm/agents/SwarmAgents-chaos`, branch `chaos`)
- **Task summary**: Retried the S05 100% no-fallback case (paper figure D's missing half) and got
  data this time: a total LLM outage with the analytic fallback removed is a **complete
  scheduling stall** — 0 of 300 jobs placed, 300 stuck, 2180 of 2181 bid attempts refused, and a
  fleet that stayed entirely healthy while never scheduling anything. Diagnosed why the first
  attempt produced nothing (`--runtime` is dead code, so the wait was unbounded; a teardown then
  erased the agent logs) and fixed both the harness and the evidence-collection gap.
- **Workflow stage**: forensics → harness fixes → testing → experiment execution → analysis → documentation
- **Prompts**: 1 user prompt (excludes 8 stop-hook review messages and 3 background-task
  notifications, all system-generated). The single prompt is the whole ask; everything after it was
  the review gate iterating on my own changes.
- **Tool calls**: ~120 (counted from the transcript, not instrumented — treat as ±10)
- **Agent tasks**: 0 sub-agents spawned. The Codex stop-review gate ran its own reviews
  automatically and caught two real defects in my own changes (see below).
- **Models used**: Opus 5 (`claude-opus-5[1m]`) throughout. Codex/GPT models via the automatic
  stop-time review gate (OpenAI credits, not counted here).
- **Estimated cost (USD)**: not instrumented — no per-session token accounting available.
- **Input tokens**: not instrumented. **Output tokens**: not instrumented.
- **Files created**: 1 — `tests/test_scenario_ablation.py` (29 tests).
- **Files modified**: 5 — `scenarios/helpers.py`, `scenarios/api/s05_unavailable.py`,
  `scenarios/clear_faults.py`, `CHAOS_JUNGLE_LLM_TEST_PLAN.md`, `SWARMAGENTS_FINDINGS.md`.
- **Experiment runs**: 1 completed on the 30-agent FABRIC slice (`cj-s05-100pct-nofb`, 300 jobs,
  gateway arm, 503 on all 30 hosts, ablation armed). The first attempt is retained as
  `cj-s05-100pct-nofb-void`.
- **Tests**: 204 → 233 passing (+29). `test_repository.py` still cannot collect (`fakeredis` not
  installed) — pre-existing and unrelated.
- **Commits**: 0 — all changes left uncommitted for review.
- **Key decisions / milestones**:
  1. **Figure D is complete, and its two halves disagree.** At 25% radius the analytic fallback is
     the pathology (86% of work to LLM-blind agents, fairness 0.253 vs 0.599 without it); at 100%
     it is the only thing that schedules at all (300 done vs 0 done / 300 stuck). Same per-call
     exception handler, no view of how many peers are also failing, so it cannot tell the two
     situations apart. Corrected the previous section's overreach ("the fallback is not what lets
     the system survive"), which is true only at partial radius.
  2. **New finding 14**: `run_test.py --runtime` is parsed and never read. Without
     `--shutdown-after-seconds` the run takes an unbounded poll loop that exits only when the job
     pool drains — so a run that cannot place a single job never ends. Every campaign run passed
     `--runtime 3000` and none was ever bounded by it; healthy runs exit on the drain condition,
     which looks exactly like a working timeout.
  3. **Corrected the record on the first attempt.** It did not hang until killed — it exited
     cleanly after a teardown flushed Redis underneath it at ~16:48, which the poll loop read as a
     drained pool. Same minute: agent logs deleted fleet-wide and the per-agent configs rewritten.
     The old write-up's account of the ending was wrong; the timestamps are in the retained
     `runs_cj-s05-100pct-nofb-void.log`.
  4. **Harness: three gaps closed.** `CJ_SHUTDOWN_AFTER` bounds runs that are expected not to
     drain; `snapshot_agent_logs()` pulls per-agent logs in a `finally` so a killed run still
     leaves evidence; `collect()` now counts `LLM_COST_NO_BID` and reports `no_bid_rate`, because
     under the ablation `fallback_rate` is 0 by construction and an inert ablation is otherwise
     indistinguishable from a healthy fleet.
  5. **Four defects in my own changes, all caught by the Codex review gate** — every one of them a
     guard that keyed off the wrong thing, which is worth noting given the whole campaign is about
     silent metric corruption. (c) The log snapshot skipped a host whose log was already in the run
     dir — presence, not freshness — so re-running a scenario into an existing run dir would have
     republished the *previous* run's logs as this run's evidence, mixing two runs into every
     per-agent metric silently. (d) The first fix still had two fallbacks to stale data: without a
     timestamp it left an unverifiable log in place, and the `mv` out of `.incoming/` was
     unconditional, so a half-finished transfer published a truncated log as complete. Final rule:
     `since` is required, stale files are displaced to `*.log.stale`, the move is gated on scp
     succeeding, a failed refresh reports a **missing** host rather than restoring what it
     displaced, and the reported count and the metric population are decided by the same freshness
     test. (e) Downstream of all that, `collect()` still summed whatever logs were present and
     `report()` printed the result as a fleet total — 22 of 30 logs would understate every LLM
     count by a quarter and manufacture a one-sided "regression" against a complete stored
     baseline. The log population is now a reported metric (`agent logs read` / `logs missing`) and
     `report()` qualifies the rows it affects. (f) That warning's first draft then over-claimed in
     the opposite direction — "treat them as lower bounds" is true of sums but false of rates and
     means, which over fewer logs are just another population's statistics (a missing slow bidder
     pulls the latency mean *down*, so quoting it as a floor is backwards). Now split three ways:
     lower bounds (counts), biased-unknown-direction (rates/means), and independent of log
     collection (orchestrator log / all_jobs.csv / metrics.json, the last verified as written from
     Redis rather than from the agent logs). Plus a line when the stored baseline records no log
     population, so the delta is not silently compared against an assumed-complete 30. (g) That
     rewrite still carried two false claims of its own: it advertised the restart/conflict counts
     as independent of log collection, when `_restarts_and_conflicts()` *falls back to counting log
     lines* whenever metrics.json is absent — so `restarts_source` is now recorded per run and the
     row is placed in whichever clause is true for that run; and `max(0, fleet - logs)` reported
     "0 missing" for a run holding MORE agent logs than configured agents, so every sum silently
     included an agent outside the fleet. Excess is now its own metric and its own warning. (h) And
     the counters themselves were still count-based: 30 files for a 30 agent fleet is also what one
     duplicate plus one hole looks like, so both read zero while every sum double-counted one agent
     and omitted another (and `load_split()` silently kept one of the duplicates). Now checked by
     ID — `agent_ids_missing` / `_duplicated` / `_unexpected`, named in the warning — and the sums
     verdict has a third case for short-and-over-counted-at-once, where they bound the truth from
     neither side. All three relevant runs (100%, 25%, gateway baseline) re-read as exactly agents
     1-30 with one log each and `restarts_source: metrics.json`, so the published figures were
     never affected. (i) The ID check itself then had two holes: it read identity from the
     *filename*, which is a label applied by whoever copied the file rather than what the agent
     wrote into every line, so a log fetched into the wrong dir certified a population that was not
     the one being summed; and with no orchestrator log `fleet_size` fell back to the module's
     AGENTS=30, which would validate a 10- or 60-agent run (this repo has both) against 30. Now
     identity comes from the log body, a name/body disagreement is reported and credited to
     neither id, and an unknown fleet withholds the verdict (blank rows, explicit notice) instead
     of rendering as clean. Also fixed three unguarded `open()` calls on globbed logs — a file
     removed between listing and reading crashed a report that had just cost a 20-minute run;
     all log reads now go through one guarded helper and count the loss. (j) Two attribution holes
     survived that: identity took the *first* `- agent-N -` line in the body, so a log holding two
     agents (two agents sharing a log path, or a stale log appended across runs — a hazard the
     runbook already documents) was attributed entirely to whoever wrote first; and the "known"
     fleet was still `fleet_size` = max(AGENTS, highest placing id), which certifies a 14-agent run
     against 30. Now every id in a body is read and a multi-agent log is credited to nobody, and the
     check compares against `fleet_configured`, parsed from the run's own "Launched N … agents"
     line, withholding the verdict when that line is absent. Verified on real data: `fleet14-gw`
     reads `fleet_configured: 14` against `fleet_size: 30` and now validates cleanly where the old
     check would have invented 16 missing agents. (a) The ablation
     mutates the *frozen fleet's* configs and only S05 reset it, so a leak would have silently
     changed what the next baseline or S01 run measured — fixed by restoring in a `finally`, and
     by making `assert_clean()` (which every scenario calls) refuse a leaked flag. (b) The
     teardown still missed failure paths: arming happened before the `try`, leaving `cleanup()` as
     an exit path with the flag set, and the two teardowns were flat so a raising config-restore
     could suppress `stop_fault()` and leak a live proxy. Now armed inside the try, teardowns
     nested so neither can suppress the other.
  6. **Honest caveats recorded** rather than smoothed over: "no-bid rate 100%" means every call
     that happened was refused, not that all 30 agents refused (14 did; the other 16 never got a
     candidate to score, because with nothing placed the job window never rotates); the refusal
     *count* is paced by the selection cache's 60 s TTL and is not a comparable rate; and "never
     terminates" is measured as "no progress for 20 minutes", not proven for all time.
  7. **Slice left idle and the frozen fleet restored** — configs verified byte-identical to
     `/root/frozen30_before_nofb.tgz`, 0 Redis keys, 0 stray agents, 0 proxies on 30 hosts.
  8. **Recommended next**: a radius-aware fallback (fall back only when enough peers are also
     failing) is the design this pair of runs argues for, and the direct test is S05 at 50% with
     the ablation on — the radius where the two behaviours should cross over.
  9. **Test plan restructured and renumbered.** Six parts with a table of contents, and sections numbered sequentially in reading order — the old labels (4.0b, 4b.1, 4d.2, 4g) were an artefact of the document growing by insertion, with two schemes colliding inside section 4. Scripted from the heading tree and verified three ways: every non-heading line survives, no label containing a letter remains, every internal link resolves. References were repointed in `SWARMAGENTS_FINDINGS.md`, `llm_agent.py` and two test docstrings; the 2026-08-21 entry above is left as written, since it is a dated record, and the mapping table at the end of the plan's Contents resolves its labels.
