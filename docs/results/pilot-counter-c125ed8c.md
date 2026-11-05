# Pilot: PBFT coordinator counter-proposals and the Snow tie tolerance (c125ed8c)

Run 2026-10-19 16:35–17:20 UTC on the 17-site slice. All 92 agents were deployed and verified with
`/root/push_c125.sh`. Campaign file `campaigns/pilot-counter.yml` reruns four f45d9ff1 cells, one repeat
each, with identical defaults, gate and arguments. Output: `runs/pilot-counter-c125ed8c`. All four runs
were `ok` on the first attempt. Each executed 1,800 jobs with 0 run twice, 0 restarts, 0 dropped sends
and 0 Snow abandons.

Each run is compared with **run01 of the same f45d9ff1 cell** (counter-proposals off, strict Snow
vote). That is one run against one run: read it as a direction, not a result.

| Cell | Jobs won per coordinator | Leaves that ran nothing | Job p50 / p95 | Wait p95 | Coordinator decision p50 | Consensus msgs / KB per job |
|---|---|---|---|---|---|---|
| PBFT · analytic · burst, before | 82:179 83:267 84:165 85:120 86:396 87:221 88:354 89:98 **90:0** | 9 / 81 | 19.6 / 93.7 s | 70.2 s | 0.64 s | 258 / 62 |
| after | 82:188 83:158 84:247 85:127 86:206 87:253 88:264 89:162 90:195 | **0 / 81** | 15.7 / **49.4 s** | **14.6 s** | 0.65 s | **351 / 82** |
| PBFT · LinUCB shared · burst, before | 88:459 89:176 **90:1165** | 10 / 87 | 20.4 / 121.7 s | 67.8 s | 0.13 s | 149 / 35 |
| after | 88:836 89:156 90:808 | 16 / 87 | 19.0 / **75.5 s** | **27.9 s** | 0.15 s | 157 / 37 |
| PBFT · LinUCB shared · 3 jobs/s, before | 88:612 89:147 90:1041 | 31 / 87 | 11.9 / 36.9 s | 0.7 s | 0.08 s | 161 / 37 |
| after | 88:902 89:155 90:743 | 27 / 87 | 11.9 / 37.3 s | 0.7 s | 0.07 s | 165 / 38 |
| Snow · LinUCB shared · burst, before | 88:763 89:422 90:615 | 7 / 87 | 50.8 / 120.1 s | 1.5 s | **25.2 s** | 180 / 52 |
| after | 88:722 89:412 90:666 | 4 / 87 | 35.6 / 112.5 s | 10.5 s | **18.0 s** | 165 / 47 |

**PBFT counter-proposals.**

* **The scan race is broken.** In the analytic burst run every coordinator now wins 127–264 jobs (an even
  share is 200), including 90, which won none before. All 81 leaves run work, and job p95 falls
  from 94 to 49 s, close to Snow's 56 s in the f45d9ff1 campaign. In the LinUCB burst run coordinator
  90's share falls from 65 % to 45 %, and 88, cheaper than 90, now takes as much. Job p95 falls from
  122 to 76 s.
* **Nearness still counts.** Coordinator 89, the cheapest LinUCB coordinator but 74 ms from its
  peers, still wins ~9 %. Its counter-proposal arrives after a nearer coordinator has a quorum, as the
  delay-bus tests predicted.
* **Decision time is unchanged** (0.65 s analytic, 0.15 s LinUCB). The cost is messages. With nine
  coordinators there are 3.9 counter-proposals per job (6,976 in the run), commit-lock holds rise
  from 739 to 7,351, and consensus traffic rises by a third (258 → 351 messages, 62 → 82 KB per job).
  With three coordinators the rise is about 5 %. Nothing was stranded or run twice.
* **At 3 jobs/s the split moves but latency does not**, because nothing queues.

**Snow tie tolerance (5 %).**

* **It helps, but does not undo the slowdown.** The three-coordinator decision p50 falls from 25.2 to
  18.0 s, and rounds per decision from 9.8 to 7.9. Job p50 falls from 50.8 to 35.6 s and p95 from 120
  to 113 s. That is still above the 15–17 s of the old cost model. 8,748 coordinator votes and 4,764
  leaf votes were tolerance yields.
* **Wait p95 rose from 1.5 to 10.5 s**, as the price of placing within 5 % of the best. Placement stayed
  balanced: 722 / 412 / 666 jobs.
* **Options if the rest matters:** a wider tolerance (10 %), or a larger coordinator tier. A pilot at
  10 % costs one run.

**Reading.** Both changes do what they were built for and keep every run clean. Counter-proposals
remove PBFT's placement handicap at the coordinator tier, which changes the C1 comparison: under the
burst PBFT's analytic job p95 (49 s) is now level with Snow's (56 s). The f45d9ff1 claim that "Snow
places better" was largely this race. The tie tolerance recovers about 30 % of the three-coordinator
Snow slowdown. Every PBFT coordinator-tier number and every Snow number moves, so the E0/E1′ campaign
needs one more full run at this revision before any of it is cited, and before E2.
