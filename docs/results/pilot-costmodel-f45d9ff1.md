# Pilot: utilization cost model on the slice (f45d9ff1)

Run 2026-10-16 18:17–19:00 UTC on the 17-site slice, deployed to all 92 agents (`/root/push_costmodel.sh`,
every host verified). Campaign file `campaigns/pilot-costmodel.yml`: three fc9eb220 cells, one repeat each,
identical defaults, gate and arguments, output `runs/pilot-costmodel-f45d9ff1`. All three runs `ok` on
the first attempt; every agent reported `cost_model: utilization`.

Each pilot run is compared with **run01 of the same fc9eb220 cell** (`cost_model: capacity`), one
run against one run. Read it as a direction, not a result; the 5-run cell means are in
`e0-e1-fc9eb220/README.md`.

| Cell | Model | Leaves that ran nothing | Coordinators that won nothing | Job p50 | Job p95 | Wait p95 | Coordinator selection p50 |
|---|---|---|---|---|---|---|---|
| Snow · analytic · 3 jobs/s | capacity | 68 / 81 | 4 (82, 83, 85, 90) | 17.7 s | 101.9 s | 74.5 s | 0.91 s |
| | **utilization** | **30 / 81** (all 2-core) | **0** (151–261 jobs each) | 14.0 s | **38.3 s** | **0.7 s** | 0.91 s |
| PBFT · analytic · 3 jobs/s | capacity | 63 / 81 | 0 | 11.9 s | 95.2 s | 48.1 s | 0.12 s |
| | **utilization** | **20 / 81** | 0 | 11.7 s | **35.9 s** | **0.7 s** | 0.12 s |
| Snow · LinUCB shared · burst | capacity | 59 / 87 | 0 (90: 174 jobs) | 77.5 s | 239.0 s | 155.1 s | 15.4 s |
| | **utilization** | **7 / 87** | 0 (90: 683 jobs) | 55.8 s | **113.1 s** | **2.2 s** | **29.0 s** |

Validity in all three runs: 1,800 jobs executed, 0 twice, 0 reselection restarts, 0 dropped sends,
0 Snow abandons, drained with nothing pending or in flight.

**Reading.**

* **Queueing is gone.** Wait p95 falls from 48–155 s to 0.7–2.2 s, and job p95 falls by 60–62 % at 3 jobs/s and
  53 % at the burst. The latency tails of the fc9eb220 campaign were the cost model, as §9 of that
  write-up concluded.
* **Snow's coordinator tier now uses all nine groups.** 82, 83, 85 and 90 won 151–212 jobs each.
* **The 2-core leaves that stay idle at 3 jobs/s are idle by design.** A 1-core, 4 GB job fills half of a 2-core,
  8 GB leaf. That is the price a large leaf reaches only at 50 % utilization, and at this load no
  job waited (wait p95 0.7 s). Under the burst all but 7 leaves ran work.
* **New regression to track: a 3-member Snow tier decides more slowly when the costs are close.**
  Coordinator selection p50 rose from 15.4 s to 29.0 s in the burst LinUCB cell. Rounds per decision rose
  from 7.7–8.5 to 10.1–11.7 and failed rounds rose by up to 46 %, because the coordinators' costs are now
  balanced and a round there needs both peers to agree. It does not reach job latency, which still halved.
  It is the same structure §4.2 of the fc9eb220 write-up flagged (α = 2 of 2), now exposed by
  balanced costs. Options for later: a tie tolerance in the peer's dominance check, or more
  coordinators.

Next: the full E0/E1′ campaign at this revision, which replaces every fc9eb220 latency figure.
