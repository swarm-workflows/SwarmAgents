# E6 at 58f3ab22: safety under a site partition, Hier-30, PBFT and Snow

Run 2026-10-30 13:16–14:38 UTC from `campaigns/e6-hier30-58f3ab22.yml` (campaign commit ad84a776, agent
code 58f3ab22), out `runs/campaign-e6-58f3ab22`. Conference plan §4.2 E6. 2 engines × 3 repeats, all `ok` on
the first attempt.

## 1. Design

| Item | Value |
|---|---|
| Fleet | Hier-30, G=1: 5 leaf groups of 5 + 5 coordinators, one agent per VM |
| Partition | `partition.py`: agent↔agent traffic cut both ways between two site groups, **Redis on `database` reachable from both sides throughout** |
| Sides | 15 hosts / 15 hosts (5 sites / 4 sites), the same in every run |
| Timing | cut 60 s after distribution starts, held **186.6 s** in every run, healed; cut and heal verified by probes on every host, 0 apply or heal failures |
| Workload | simulated 600-job replay set, Poisson 2 jobs/s (~300 s of arrivals, so arrivals span the cut) |

**Deviations from the plan:** the plan's E6 uses the mixed real-workflow workload, which needs T-6 (not built).
Arrivals are Poisson rather than E0's burst, so decisions keep being made during the cut. Say both beside any
E6 number. A Redis-side partition is not tested and nothing here claims it.

**Every group is split across the cut**, none evenly: leaf groups 3–2, 1–4, 3–2, 2–3, 2–3; coordinators 4–1.

## 2. Safety: every job ran exactly once, on both engines

| | PBFT (3 runs) | Snow (3 runs) |
|---|---|---|
| Jobs executed | 600 / 600 each | 600 / 600 each |
| **Jobs executed twice** | **0** | **0** |
| `lost_leader_races` (a second finalization that stepped aside) | 0 | 0 |
| Failure verdicts | 0 | 0 |
| Drained, 0 in flight at stop | yes | yes |

**PBFT did not finalize on both sides, contrary to the plan's expectation**, and the reason is structural
rather than lucky. PBFT's quorum is `len(neighbor_map) // 2 + 1` over the agent's own group
(`resource_agent.calculate_quorum`), and `neighbor_map` is refreshed from Redis, which both sides still
reach. No agent was declared failed (heartbeats travel through Redis), so every agent kept its whole group
in the denominator: a group of 5 needs 3 votes, and only the side holding the majority of that group can
supply them. The minority side cannot finalize anything. The plan's "each side reaches its own quorum"
assumes the cut peers leave `neighbor_map`; with the agent↔agent cut this design tests, they never do.

So, under an agent↔agent partition with Redis reachable:
* **Snow** is safe by its claim CAS (`try_claim_assignment`), as designed.
* **PBFT** is safe by majority: the minority side stalls instead of deciding. With an even group size a
  50/50 split would stall both sides (a liveness loss, not a safety one); every group here is odd.
* Neither result covers a partition that also cuts Redis, or one long enough for the far side to be
  declared failed.

The plan's Snow claim-key audit cannot be run after the fact (the claim keys are run-scoped and flushed by
the next run's cleanup); `jobs_executed_twice` covers both engines, which is what the plan's figure uses.

## 3. Cost: PBFT pays in stalled jobs, mostly after the heal

Wait = leaf start − submission, by when the job was submitted relative to the cut (`e6phase.py`):

| | PBFT wait p50 / p95 | Snow wait p50 / p95 |
|---|---|---|
| Submitted before the cut | 0.7–1.2 / 1.6–1.9 s | 1.2–1.5 / 3.6–4.2 s |
| During the cut | 0.6–1.2 / 4.5–5.5 s | 1.2 / 3.5–3.7 s |
| After the heal | 1.2–1.5 / **61–126 s** | 2.1–2.3 / 3.9–5.6 s |

| Stalled jobs (wait > 30 s), per run | PBFT | Snow |
|---|---|---|
| total | 18, 15, 14 | 1, 3, 3 |
| submitted during the cut | 7, 4, 4 | 0, 2, 2 |
| submitted after the heal | **10, 11, 9** | 1, 1, 1 |
| wait > 250 s | 4, 8, 4 | 0 |

PBFT also records 28–45 reselection restarts, 140–164 commit-lock holds and 5–10 commit splits resolved per
run; Snow records 0 restarts and 128–135 abandoned instances (peers decided those jobs: 0 unrun). PBFT's
stalls clear at its fallback timers: ~60 s (`decided_unpersisted_grace_s`) and ~300 s
(`reselection_timeout_s`). **Most PBFT stalls are jobs submitted after the heal**, so re-convergence,
not the cut, is where PBFT's cost lands.

**Traced (run02, job 524; §4).** Coordinator tier: delegated to group 1 within 0.2 s of submission. Leaf
tier: agent 7, alone on the minority side of group 1 during the cut, came back idle and therefore cheapest,
and proposed itself (correctly). Only agents 7 and 9 voted; agents 8 and 10 held that proposal with no rival
and no vote of their own, agent 6 never joined, and the job waited for the 300 s reselection (restart at
13:43:41, ran on agent 9). The engine explains the silence: an agent that first learns a proposal through
another agent's PREPARE (`ConsensusEngine.on_prepare`, the "new proposal arriving via PREPARE" branch)
adopts it and records the sender's vote but **never casts its own vote or sends a PREPARE**; only the
PROPOSAL path (`on_proposal`) does. Normally the PROPOSAL arrives later and the vote follows. PBFT never
retransmits, so when the PROPOSAL is lost every agent that heard of it second-hand stays silent: 2 of 5
votes against a quorum of 3. After a heal the formerly isolated agent is the likeliest proposer and its
channels to the far side are the likeliest to drop a send (SWIM flapped between agents 7 and 9 for ~20 s
after the heal). The lost PROPOSAL is inferred, not observed (no per-peer send log); no other path leaves
an agent holding a proposal it did not vote for with nothing better present.

Median selection time hardly moves on either engine (PBFT leaf 0.05–0.11 s, Snow leaf 0.7–1.2 s, coordinator
tier 0.06–0.13 s / 1.2 s). The cost is in the tail, not the median.

## 4. Open

* **Fixed in b9e51bbf and E6 re-run: `../e6-b9e51bbf/README.md`** (post-heal 300 s stalls gone; a 60 s tail of
  lost COMMITs remains, recovered by the certificate resend).
* **PBFT liveness defect (§3):** a proposal adopted via PREPARE is never voted for. Candidate fix: vote and
  send PREPARE on adoption, as `on_proposal` does. It is a consensus change: test it against the
  straggler and COMMIT-inflation pins (`tests/test_pbft_stragglers.py`) and re-measure every PBFT cell.
* A partition long enough for the detector to declare the far side failed would shrink `neighbor_map` and
  could let both PBFT sides reach quorum; that case is not tested.

## 5. Files

`run_summary.txt` (all script output), `e6chk.py` (partition sides and `lost_leader_races`), `e6phase.py`
(group split; selection and wait by phase), `e6tail.py` (stalled-job counts). Per-run health uses
`/root/runstats.py` on the database node; the runs and `run0N.partition.json` are in
`runs/campaign-e6-58f3ab22/` there. Snow leaf rows exclude 4–8 jobs per run that carry no selection-start
stamp (counted in `run_summary.txt`).
