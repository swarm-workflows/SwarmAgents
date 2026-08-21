# Claude Code — session usage log

Per-session records for `SwarmAgents-chaos` (branch `chaos`). Append-only; newest at the bottom.

## Session: 2026-08-21 18:14

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
