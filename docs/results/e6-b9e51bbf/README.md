# E6 at b9e51bbf: the PBFT vote-on-adoption fix under the same partition

Run 2026-10-30 15:50–17:08 UTC from `campaigns/e6-hier30-b9e51bbf.yml` (code b9e51bbf, campaign fe90476a),
out `runs/campaign-e6-b9e51bbf`. **Identical to `../e6-58f3ab22/` except the code revision**; that README holds
the design, the deviations from the plan and the safety argument, and they all carry over. Only E6 was
re-run at this revision (decided 2026-10-30); every other PBFT number predates the fix.

## 1. What changed in the code

At 58f3ab22 a PBFT agent that first learned of a proposal through another agent's PREPARE adopted it without
voting, so a lost PROPOSAL left a leaf group 2 votes short of quorum until the 300 s reselection
(`../e6-58f3ab22/README.md` §3, job 524). b9e51bbf votes and sends a PREPARE on that adoption wherever the
agent never counter-proposes (every leaf), and merges a late PROPOSAL without a second PREPARE
(`ConsensusEngine.on_prepare` / `on_proposal`, `_counters_active`; `tests/test_pbft_adopt_via_prepare.py`).
The coordinator tier, where counter-proposals are live, is unchanged by design.

## 2. Safety: unchanged, every job ran exactly once

All 6 runs `ok`; same split (15/15 hosts, every group split unevenly), cut held 186.6 s, cut and heal
verified on every host. PBFT and Snow: 600/600 executed, **0 executed twice, 0 `lost_leader_races`**,
0 failure verdicts, drained with 0 in flight.

## 3. Effect on PBFT's tail: the 300 s stalls are gone, a 60 s one remains

| PBFT, per run | 58f3ab22 | b9e51bbf |
|---|---|---|
| Jobs waiting > 30 s | 18, 15, 14 | 17, 18, 15 |
| Jobs waiting > 250 s | 4, 8, 4 | 3, 2, 3 |
| Longest wait, jobs submitted after the heal | 304, 306, 302 s | **79, 133, 136 s** |
| Wait p95, jobs submitted after the heal | 61–126 s | 61–62 s |
| Reselection restarts | 45, 28, 45 | **2, 14, 23** |

The number of stalled jobs did not fall; their length did. What is left is a cluster at **60 s**: post-heal
jobs whose leaf election was assigned exactly 60.1–60.4 s after it started (jobs 526, 531, 534, 536, 539 in
run01). **Traced (run01, job 536, group 3):** agents 16, 17 and 19 finalized the decision at 15:56:15 with
agent 18 as leader; agent 18, on the minority side of group 3 during the cut, never received those COMMITs;
at 15:57:16 agent 16 resent the commit certificate (`[CON_CERT_RESEND]`, the 2026-10-14 mechanism) and agent
18 started the job at once. So the residual tail is the same root cause, messages between the former sides
lost just after the heal with no retransmit, now landing on a COMMIT instead of a PROPOSAL, and recovered by
the existing 60 s certificate resend rather than the 300 s reselection.

The 2–3 jobs per run that still wait > 250 s were all submitted before or during the cut (run01: jobs 122,
140 and 155, waits 261–277 s, all assigned at 15:57:17–18, within two seconds of the certificate resends above).
They are elections caught by the cut itself; what released them has not been traced.

Snow is unaffected (it does not use this path): 0–1 stalled jobs per run, 0 restarts, 124–133 abandons
(peers decided those jobs).

## 4. What it says

* The fix does what it was for: no post-heal leaf election waits out the 300 s reselection any more, and PBFT
  restarts fell from 28–45 to 2–23 per run.
* The remaining post-heal tail (~60 s, 10–12 jobs per run) is lost COMMITs to the new leader, recovered by the
  certificate resend. Shortening it means retransmitting or resending sooner; the root cause in both cases is
  that messages across a just-healed cut are lost for a short window and PBFT does not resend them.
* Neither change touches safety: 0 jobs twice on both engines, both revisions.

## 5. Files

`run_summary.txt` (campaign log, partition checks, stalled-job counts, per-run health, phase table),
`e6chk.py`, `e6tail.py`, `e6phase.py`, `e6stall.py` (the 58f3ab22 scripts pointed at this campaign).
