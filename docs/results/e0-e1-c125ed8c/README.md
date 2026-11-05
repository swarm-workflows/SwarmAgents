# E0 / E1′ campaign at c125ed8c: cost model + PBFT counter-proposals + Snow tie tolerance

Collected 2026-10-20 from `runs/campaign-e0-e1-c125ed8c` on the database node. This is the f45d9ff1
campaign (`../e0-e1-f45d9ff1/README.md`) re-run unchanged except for two changes, piloted in
`../pilot-counter-c125ed8c.md`:

* **`consensus.pbft.counter_propose: true`.** A PBFT coordinator under `coordinator_cost_matrix: self`
  that hears a proposal for a job it has not proposed compares that proposal's cost with its own. If its
  own is cheaper it proposes itself, instead of preparing whatever it heard first. This removes the
  Redis-scan race of `../e0-e1-f45d9ff1/README.md` §7.
* **`consensus.snow.tie_tolerance: 0.05`.** A Snow peer backs itself only when it is cheaper than the
  initiator by more than 5 % of the initiator's cost.

**It supersedes f45d9ff1 for every PBFT coordinator-tier number and every Snow number.** The cost model
(utilization) is unchanged since f45d9ff1.

## 1. What was run

| Item | Value |
|---|---|
| Code | c125ed8c on all 92 agents (`/root/push_c125.sh`, verified per host); campaign file 529e9e9a |
| Window | 19 Oct 17:34 UTC to 20 Oct 13:44 UTC, about 20 h |
| Runs | 19 cells × 5 = 95, all `ok` on the first attempt, no `.attempt` directories |
| Everything else | identical to f45d9ff1 and fc9eb220: cells, arguments, fleet, workload, placement, gate |

## 2. Validity: every run is clean

All 95 runs: every job completed (600 / 1,800), drained with 0 pending and 0 in flight, 0 executed
twice, 0 unrun, 0 reselection restarts, 0 decided-unpersisted re-elections, 0 false failure verdicts,
0 dropped sends, 0 Snow abandons. Every agent reported `cost_model: utilization`.

## 3. Results

Mean ± 95 % t-interval over 5 runs; notation as in the earlier write-ups.

**Latency, Hier-90, burst (20 jobs/s for 90 s).** Seconds, mean ± 95% CI over 5 runs.

| Cell | Coordinator sel. p50 | Coordinator sel. p95 | Leaf sel. p50 | Job latency p50 | Job latency p95 | Wait p95 | Makespan |
|---|---|---|---|---|---|---|---|
| PBFT · analytic | 0.64 ± 0.03 | 2.41 ± 0.29 | 0.09 | 16.2 ± 0.5 | 54.0 ± 4.1 | 14.0 ± 1.2 | 232 ± 38 |
| Snow · analytic | 1.23 ± 0.11 | 2.01 ± 0.25 | 1.20 | 17.7 ± 0.4 | 54.4 ± 3.7 | 10.5 ± 2.2 | 224 ± 23 |
| Hybrid · analytic | 1.35 ± 0.18 | 2.28 ± 0.58 | 0.09 | 16.2 ± 0.3 | 51.5 ± 5.3 | 11.2 ± 1.2 | 209 ± 17 |
| PBFT · LinUCB shared | 0.15 ± 0.01 | 1.74 ± 1.02 | 0.09 | 18.2 ± 0.6 | 62.4 ± 3.0 | 27.0 ± 3.4 | 251 ± 36 |
| PBFT · LinUCB per-group | 0.14 | 1.83 ± 0.81 | 0.09 | 18.5 ± 0.3 | 62.9 ± 8.4 | 24.9 ± 4.0 | 255 ± 45 |
| Snow · LinUCB shared | 17.90 ± 2.97 | 22.55 ± 3.70 | 1.19 | 36.9 ± 3.2 | 118.5 ± 4.5 | 8.9 ± 2.6 | 293 ± 12 |
| Snow · LinUCB per-group | 19.99 ± 2.92 | 24.31 ± 3.10 | 1.19 | 38.5 ± 3.0 | 117.1 ± 12.6 | 8.6 ± 2.2 | 273 ± 21 |
| Hybrid · LinUCB shared | 20.46 ± 3.30 | 25.93 ± 3.18 | 0.09 | 38.0 ± 2.9 | 102.2 ± 12.5 | 9.9 ± 1.9 | 278 ± 24 |
| Hybrid · LinUCB per-group | 17.79 ± 3.92 | 22.16 ± 4.55 | 0.09 | 34.9 ± 4.0 | 114.0 ± 8.2 | 8.8 ± 4.0 | 278 ± 11 |

**Cost and view quality, Hier-90, burst (20 jobs/s for 90 s).**

| Cell | Consensus msgs/job | Consensus KB/job | Overlay msgs/job | Proposers/job (coord.) | Busiest coordinator share | Least-used coordinator share | Busiest group share (bandit) | Context error (mean) | Jain fairness |
|---|---|---|---|---|---|---|---|---|---|
| PBFT · analytic | 348 ± 6 | 81 ± 1 | 70 ± 9 | 5.6 ± 0.3 | 0.15 | 0.076 | – | 1.24 ± 0.10 | 0.52 |
| Snow · analytic | 262 ± 4 | 142 ± 1 | 68 ± 6 | 8.8 | 0.14 | 0.085 | – | 1.12 ± 0.07 | 0.54 |
| Hybrid · analytic | 255 ± 3 | 140 ± 2 | 64 ± 4 | 8.8 | 0.14 | 0.085 | – | 1.17 ± 0.13 | 0.52 |
| PBFT · LinUCB shared | 157 ± 2 | 37 | 74 ± 8 | 2.0 | 0.47 | 0.080 | 0.378 | 1.12 ± 0.16 | 0.43 |
| PBFT · LinUCB per-group | 158 | 37 | 75 ± 11 | 2.0 ± 0.1 | 0.48 | 0.081 | 0.351 | 1.22 ± 0.16 | 0.44 |
| Snow · LinUCB shared | 165 ± 2 | 47 | 85 ± 3 | 3.0 | 0.39 | 0.239 | 0.368 | 1.39 ± 0.04 | 0.50 |
| Snow · LinUCB per-group | 167 ± 2 | 47 | 84 ± 3 | 3.0 | 0.39 | 0.236 | 0.362 | 1.37 ± 0.09 | 0.51 |
| Hybrid · LinUCB shared | 167 | 48 | 82 ± 6 | 3.0 | 0.40 | 0.217 | 0.370 | 1.34 ± 0.07 | 0.49 |
| Hybrid · LinUCB per-group | 168 ± 1 | 48 | 81 ± 3 | 3.0 | 0.39 | 0.235 | 0.354 | 1.46 ± 0.20 | 0.50 |

**Consensus internals, Hier-90, burst (20 jobs/s for 90 s).** Per run, mean.

| Cell | Coord. finalize p50 (agent median) | Coord. rounds/decision | Leaf rounds/decision | Coord. certificate finalizes | Coord. commit-lock holds | Leaf certificate finalizes | Snow abandons (all tiers) |
|---|---|---|---|---|---|---|---|
| PBFT · analytic | 0.83 | – | – | 688 | 6367 | 25 | 0.0 |
| Snow · analytic | 1.24 | 6.2 | 6.5 | 0 | 0 | 0 | 0.0 |
| Hybrid · analytic | 1.38 | 6.3 | – | 0 | 0 | 21 | 0.0 |
| PBFT · LinUCB shared | 0.40 | – | – | 752 | 601 | 16 | 0.0 |
| PBFT · LinUCB per-group | 0.41 | – | – | 810 | 637 | 19 | 0.0 |
| Snow · LinUCB shared | 18.39 | 8.1 | 6.3 | 0 | 0 | 0 | 0.0 |
| Snow · LinUCB per-group | 20.81 | 8.0 | 6.3 | 0 | 0 | 0 | 0.0 |
| Hybrid · LinUCB shared | 20.95 | 8.2 | – | 0 | 0 | 17 | 0.0 |
| Hybrid · LinUCB per-group | 18.15 | 8.0 | – | 0 | 0 | 21 | 0.0 |

**Selection p50 by measured RTT bin, Hier-90, burst (20 jobs/s for 90 s).** Coordinator tier binned by leaf–coordinator RTT; leaf tier by the executor's median RTT to its peers. Seconds.

| Cell | Coord. LAN | Coord. regional | Coord. continental | Coord. transatlantic | Leaf regional | Leaf transatlantic |
|---|---|---|---|---|---|---|
| PBFT · analytic | 0.37 | 0.59 | 0.95 | 0.43 | 0.07 | 0.14 |
| Snow · analytic | 1.30 | 1.24 | 1.15 | 1.30 | 1.19 | 1.59 |
| Hybrid · analytic | 1.44 | 1.37 | 1.21 | 1.48 | 0.07 | 0.13 |
| PBFT · LinUCB shared | 0.24 | 0.13 | 0.63 | 0.22 | 0.07 | 0.13 |
| PBFT · LinUCB per-group | 0.27 | 0.13 | 0.66 | 0.25 | 0.07 | 0.13 |
| Snow · LinUCB shared | 18.94 | 18.32 | 16.36 | 18.74 | 1.18 | 1.58 |
| Snow · LinUCB per-group | 21.29 | 20.75 | 18.82 | 20.78 | 1.18 | 1.58 |
| Hybrid · LinUCB shared | 22.01 | 20.95 | 18.31 | 21.01 | 0.07 | 0.13 |
| Hybrid · LinUCB per-group | 18.90 | 18.10 | 16.58 | 18.08 | 0.07 | 0.13 |

**Latency, Hier-90, steady Poisson 3 jobs/s for ~610 s.** Seconds, mean ± 95% CI over 5 runs.

| Cell | Coordinator sel. p50 | Coordinator sel. p95 | Leaf sel. p50 | Job latency p50 | Job latency p95 | Wait p95 | Makespan |
|---|---|---|---|---|---|---|---|
| PBFT · analytic | 0.07 | 0.17 | 0.10 | 11.9 | 36.5 ± 0.2 | 0.7 | 621 |
| Snow · analytic | 0.91 | 1.08 | 1.20 | 13.9 | 38.8 ± 0.2 | 0.7 | 623 ± 1 |
| Hybrid · analytic | 0.91 | 1.09 ± 0.02 | 0.09 | 12.9 | 37.7 ± 0.2 | 0.7 | 621 |
| PBFT · LinUCB shared | 0.07 | 0.14 | 0.09 | 11.9 | 36.8 ± 0.4 | 0.7 | 621 ± 1 |
| PBFT · LinUCB per-group | 0.07 | 0.13 | 0.09 | 11.9 | 36.7 ± 0.3 | 0.7 | 621 ± 1 |
| Snow · LinUCB shared | 0.96 | 1.44 ± 0.04 | 1.19 | 14.5 ± 0.1 | 39.6 ± 0.8 | 0.7 | 624 ± 2 |
| Snow · LinUCB per-group | 0.96 | 1.47 ± 0.04 | 1.19 | 14.6 ± 0.1 | 39.8 ± 0.5 | 0.7 | 624 ± 3 |
| Hybrid · LinUCB shared | 0.96 | 1.46 ± 0.02 | 0.09 | 13.5 | 38.7 ± 0.5 | 0.7 | 622 ± 1 |
| Hybrid · LinUCB per-group | 0.96 | 1.48 ± 0.06 | 0.09 | 13.6 ± 0.1 | 38.8 ± 0.4 | 0.7 | 623 ± 2 |

**Cost and view quality, Hier-90, steady Poisson 3 jobs/s for ~610 s.**

| Cell | Consensus msgs/job | Consensus KB/job | Overlay msgs/job | Proposers/job (coord.) | Busiest coordinator share | Least-used coordinator share | Busiest group share (bandit) | Context error (mean) | Jain fairness |
|---|---|---|---|---|---|---|---|---|---|
| PBFT · analytic | 354 ± 4 | 79 | 188 | 5.7 ± 0.1 | 0.17 | 0.054 | – | 0.17 | 0.29 |
| Snow · analytic | 507 ± 3 | 168 | 188 | 8.9 | 0.14 | 0.077 | – | 0.13 | 0.32 |
| Hybrid · analytic | 519 ± 2 | 173 ± 1 | 188 | 8.9 | 0.14 | 0.076 | – | 0.16 | 0.31 |
| PBFT · LinUCB shared | 165 | 38 | 187 | 1.8 | 0.48 | 0.097 | 0.416 | 0.15 ± 0.01 | 0.25 |
| PBFT · LinUCB per-group | 165 | 38 | 186 | 1.8 | 0.48 | 0.092 | 0.364 | 0.14 ± 0.02 | 0.27 |
| Snow · LinUCB shared | 167 ± 1 | 42 | 187 | 2.7 | 0.36 | 0.304 | 0.376 | 0.21 ± 0.02 | 0.33 |
| Snow · LinUCB per-group | 167 ± 2 | 42 | 187 | 2.7 | 0.37 | 0.302 | 0.366 | 0.22 ± 0.02 | 0.34 |
| Hybrid · LinUCB shared | 193 | 49 | 187 | 2.7 | 0.35 | 0.308 | 0.376 | 0.24 ± 0.02 | 0.30 |
| Hybrid · LinUCB per-group | 193 ± 1 | 49 | 187 | 2.7 | 0.37 | 0.303 | 0.368 | 0.23 | 0.32 |

**Consensus internals, Hier-90, steady Poisson 3 jobs/s for ~610 s.** Per run, mean.

| Cell | Coord. finalize p50 (agent median) | Coord. rounds/decision | Leaf rounds/decision | Coord. certificate finalizes | Coord. commit-lock holds | Leaf certificate finalizes | Snow abandons (all tiers) |
|---|---|---|---|---|---|---|---|
| PBFT · analytic | 0.06 | – | – | 2428 | 6839 | 12 | 0.0 |
| Snow · analytic | 0.91 | 6.3 | 6.1 | 0 | 0 | 0 | 0.0 |
| Hybrid · analytic | 0.91 | 6.3 | – | 0 | 0 | 10 | 0.0 |
| PBFT · LinUCB shared | 0.08 | – | – | 438 | 139 | 11 | 0.0 |
| PBFT · LinUCB per-group | 0.08 | – | – | 417 | 124 | 11 | 0.0 |
| Snow · LinUCB shared | 0.96 | 6.9 | 6.1 | 0 | 0 | 0 | 0.0 |
| Snow · LinUCB per-group | 0.96 | 6.9 | 6.1 | 0 | 0 | 0 | 0.0 |
| Hybrid · LinUCB shared | 0.96 | 6.9 | – | 0 | 0 | 10 | 0.0 |
| Hybrid · LinUCB per-group | 0.96 | 7.0 | – | 0 | 0 | 7 | 0.0 |

**Selection p50 by measured RTT bin, Hier-90, steady Poisson 3 jobs/s for ~610 s.** Coordinator tier binned by leaf–coordinator RTT; leaf tier by the executor's median RTT to its peers. Seconds.

| Cell | Coord. LAN | Coord. regional | Coord. continental | Coord. transatlantic | Leaf regional | Leaf transatlantic |
|---|---|---|---|---|---|---|
| PBFT · analytic | 0.06 | 0.07 | 0.09 | 0.06 | 0.08 | 0.13 |
| Snow · analytic | 0.90 | 0.91 | 0.93 | 0.90 | 1.19 | 1.58 |
| Hybrid · analytic | 0.90 | 0.91 | 0.93 | 0.90 | 0.07 | 0.13 |
| PBFT · LinUCB shared | 0.06 | 0.06 | 0.08 | 0.05 | 0.07 | 0.13 |
| PBFT · LinUCB per-group | 0.06 | 0.06 | 0.08 | 0.06 | 0.07 | 0.13 |
| Snow · LinUCB shared | 0.94 | 0.96 | 0.96 | 0.94 | 1.17 | 1.58 |
| Snow · LinUCB per-group | 0.93 | 0.96 | 0.96 | 0.93 | 1.17 | 1.58 |
| Hybrid · LinUCB shared | 0.92 | 0.97 | 0.96 | 0.94 | 0.07 | 0.13 |
| Hybrid · LinUCB per-group | 0.92 | 0.98 | 0.96 | 0.95 | 0.07 | 0.13 |

**E0, PBFT · analytic.** Seconds, mean ± 95% CI over 5 runs.

| Metric | Hier-30 burst | Hier-90 burst | Hier-90 at 3 jobs/s |
|---|---|---|---|
| Leaf selection p50 | 0.07 ± 0.01 | 0.09 | 0.10 |
| Leaf selection p95 | 0.54 ± 0.29 | 0.40 ± 0.04 | 0.16 |
| Coordinator selection p50 | 0.32 ± 0.06 | 0.64 ± 0.03 | 0.07 |
| Coordinator selection p95 | 3.12 ± 3.49 | 2.41 ± 0.29 | 0.17 |
| Job latency p50 | 3.9 ± 0.4 | 16.2 ± 0.5 | 11.9 |
| Job latency p95 | 28.8 ± 6.1 | 54.0 ± 4.1 | 36.5 ± 0.2 |
| Wait p95 | 2.0 ± 0.6 | 14.0 ± 1.2 | 0.7 |
| Execution p95 | 16.8 | 34.6 | 34.6 |
| Makespan | 159 ± 11 | 232 ± 38 | 621 |
| Throughput (jobs/s) | 3.79 ± 0.26 | 7.87 ± 1.25 | 2.90 |
| Consensus msgs/job | 75 | 348 ± 6 | 354 ± 4 |
| Busiest coordinator share | 0.38 ± 0.03 | 0.15 ± 0.01 | 0.17 ± 0.01 |
| Least-used coordinator share | 0.098 ± 0.046 | 0.076 ± 0.010 | 0.054 ± 0.006 |

### 3.1 Against f45d9ff1 (counter-proposals off, strict Snow vote), cell means

| Hier-90 cell | Coordinator decision p50 | Job p95 | Wait p95 | Throughput | Consensus KB/job | Busiest / least coordinator |
|---|---|---|---|---|---|---|
| PBFT · analytic, burst | 0.61 → 0.64 s | 91 → **54** s | 56 → 14 s | 6.4 → 7.9 | 63 → 81 | 20 / 2 % → 15 / 8 % |
| Snow · analytic, burst | 2.22 → **1.23** s | 56 → 54 s | 11 → 11 s | 7.6 → 8.1 | 156 → 142 | 15 / 7 % → 14 / 9 % |
| Hybrid · analytic, burst | 2.87 → **1.35** s | 53 → 52 s | 11 → 11 s | 7.6 → 8.7 | 150 → 140 | 14 / 7 % → 14 / 9 % |
| PBFT · LinUCB shared, burst | 0.13 → 0.15 s | 119 → **62** s | 74 → 27 s | 6.2 → 7.3 | 36 → 37 | 62 / 7 % → 47 / 8 % |
| Snow · LinUCB shared, burst | 26.3 → **17.9** s | 113 → 118 s | 1.6 → 8.9 s | 6.8 → 6.1 | 52 → 47 | 42 / 22 % → 39 / 24 % |
| Hybrid · LinUCB shared, burst | 29.8 → **20.5** s | 108 → 102 s | 1.4 → 9.9 s | 6.8 → 6.5 | 51 → 48 | 41 / 21 % → 40 / 22 % |
| PBFT · analytic, 3 jobs/s | 0.12 → 0.07 s | 36.5 → 36.5 s | 0.7 s | 2.9 | 64 → 79 | 20 / 3 % → 17 / 5 % |
| Snow · analytic, 3 jobs/s | 0.92 → 0.91 s | 38.8 → 38.8 s | 0.7 s | 2.9 | 175 → 168 | 15 / 9 % → 14 / 8 % |
| Snow · LinUCB shared, 3 jobs/s | 0.97 → 0.96 s | 40.3 → 39.6 s | 0.7 s | 2.9 | 43 → 42 | 36 / 31 % → 36 / 30 % |

Hier-30 PBFT under the burst: job p95 35 → 29 s, wait p95 9.1 → 2.0 s, leaves that ran work 20 → 23 of 27.

## 4. What the numbers say

### 4.1 C1 at Hier-90: no engine wins outright; it depends on the coordinator tier

| Block, load | Job p95: PBFT · Snow · Hybrid | Who is ahead |
|---|---|---|
| Analytic (9 coordinators), burst | 54 ± 4 · 54 ± 4 · 52 ± 5 s | **tie** (Hybrid highest throughput, 8.7 jobs/s against 7.9 and 8.1) |
| LinUCB (3 coordinators), burst | 62–63 · 117–118 · 102–114 s | **PBFT** |
| Either block, 3 jobs/s | 36.5–36.8 · 38.8–39.8 · 37.7–38.8 s | tie (PBFT within 2–3 s) |

The f45d9ff1 reading, that Snow places better under load, was largely PBFT's scan race. With
counter-proposals PBFT spreads its coordinator tier almost as evenly as Snow: under the analytic burst
each of the nine coordinators wins 8–14 % (even is 11 %). The two engines then finish level, PBFT
deciding faster (0.64 s against 1.23 s) and Snow placing slightly more evenly. In the three-coordinator
LinUCB block Snow's vote is still slow (18–20 s per coordinator decision), and PBFT finishes in about
half the time.

**For the paper:** at Hier-90, with the coordinator setup used here, Snow is not shown to beat PBFT.
It ties with nine coordinators and loses with three. C1 still rests on scale (Hier-270, Mesh-180) and
on the E5 `peers` arm.

### 4.2 PBFT counter-proposals: the race is gone, at a message cost

| PBFT cell | Coordinator shares, nearest to the database first: f45d9ff1 → c125ed8c (run01) |
|---|---|
| Hier-90 analytic, burst | 90 @ 0.1 ms 0 % → 8 %; all nine now 8–14 % |
| Hier-90 LinUCB shared, burst | 65 / 26 / 10 % → 43 / 46 / 11 % |
| Hier-30 analytic, burst | 72 / 28 / 0 % → 56 / 33 / 11 % |

Counter-proposals per job: 3.8 at the burst and 4.4 at 3 jobs/s with nine coordinators, 0.85 with three.
Consensus bytes per job rise by about a quarter with nine coordinators (63 → 81 KB at the burst, 64 → 79 KB
at 3 jobs/s) and stay flat with three (36 → 37 KB). Coordinator decision time does not change. Nearness still
counts a little: the far-host coordinator (89, 74 ms) still wins ~10 % in the three-coordinator block,
because its counter-proposal lands after a nearer one has a quorum.

### 4.3 Snow tie tolerance: decisions faster, a little more queueing

Coordinator decision p50 falls from 2.2–2.9 s to 1.2–1.4 s with nine coordinators, and from 26–30 s to
18–20 s with three. Rounds per decision fall from 9.8–10.1 to 8.0–8.2. Votes inside the tolerance are
frequent: about 37,600 coordinator yields per analytic run, about 7,000–7,800 per LinUCB run, and about
5,000 at Snow leaves. The price shows in the LinUCB block: wait p95 rises from 1.4–3.3 s to 8.6–9.9 s, and
job p95 does not improve (113 → 118 s Snow, 108 → 102 s Hybrid). With three coordinators the remaining
gap to PBFT is structural: a round still needs both peers to agree. A 10 % tolerance or a larger tier
are the next things to try.

### 4.4 C2: shared and per-group still tie

Job latency for shared against per-group is inside the interval in every engine and load, and the
busiest group's share per coordinator stays at 36–40 %.

### 4.5 F6 input: the context-error spread is small

| Mean in-flight miscount | Analytic burst | Analytic 3 jobs/s | LinUCB burst | LinUCB 3 jobs/s |
|---|---|---|---|---|
| PBFT | 1.24 | 0.17 | 1.12–1.22 | 0.14–0.15 |
| Snow | 1.12 | 0.13 | 1.37–1.39 | 0.21–0.22 |
| Hybrid | 1.17 | 0.16 | 1.34–1.46 | 0.23–0.24 |

Only the LinUCB block at 3 jobs/s keeps a clear protocol spread (Snow about 1.5× PBFT). F6 now rests
on E2's LinUCB cells, and its x-axis is narrow. Plan for the "scale story" version of the paper (F6 as
a table) unless E2 shows otherwise.

## 5. Caveats

* One scale, one workload, `coordinator_cost_matrix: self`, no failures. Analytic and LinUCB blocks
  differ in topology.
* PBFT's counter-proposals are a protocol change made for this coordinator setup. Describe them in the
  paper as part of the system, with the race they remove.
* The Snow tolerance trades queueing for decision speed. Report both.

## 6. Files

`runs_wide.csv` (collector), `supp.csv` (`../e0-e1-fc9eb220/supp.py`), `runs_merged.csv`, `cells_agg.csv`
and `tables.md` (generated by the scripts in `../e0-e1-fc9eb220/`). `decisions.csv` and `job_rtt.csv` stay
in `/root/collect-c125ed8c/` on the database node. The coordinator-share and counter figures come from
`../e0-e1-f45d9ff1/racecorr.py` pointed at this campaign.
