# E0 / E1′ campaign at f45d9ff1: the utilization cost model

Collected 2026-10-19 from `runs/campaign-e0-e1-f45d9ff1` on the database node. This campaign is the
fc9eb220 campaign (`../e0-e1-fc9eb220/README.md`) re-run unchanged except for the cost model. It
replaces every latency, wait, throughput and placement figure from fc9eb220, which were measured
under the old `capacity` model that left most of the fleet idle (§9 of that write-up).

## 1. What was run

| Item | Value |
|---|---|
| Code | f45d9ff1 (`job_selection.cost_model: utilization`, the default), deployed to all 92 agents with `/root/push_costmodel.sh`; campaign file at fa6e11c4 |
| Window | 16 Oct 19:45 UTC to 17 Oct 16:05 UTC, about 20 h |
| Runs | 19 cells × 5 = 95, all `ok` on the first attempt, no `.attempt` directories |
| Everything else | identical to fc9eb220: same cells, arguments, base configs (each now with `cost_model: utilization`), fleet seed, workload, placement and gate |

Every agent's metrics payload reports `cost_model: utilization`. The comparability rule is unchanged:
the analytic block (9 coordinators × 1 group) and the LinUCB block (3 × 3) differ in topology, so
compare engines within a block.

## 2. Validity: every run is clean

| Check | All 95 runs |
|---|---|
| Jobs completed / succeeded | 600/600 at Hier-30, 1,800/1,800 at Hier-90 |
| Drain | drained, 0 pending, 0 in flight |
| Executed twice / unrun | 0 / 0 |
| Reselection restarts / decided-unpersisted re-elections | 0 / 0 |
| Dropped sends | 0 |
| Snow abandoned votes | 0 |

## 3. Results

Mean ± 95 % t-interval over 5 runs. Notation as in the fc9eb220 write-up: *coordinator selection* is
the level-1 decision time, *leaf selection* the level-0 decision, *wait* is assignment to start.

**Latency, Hier-90, burst (20 jobs/s for 90 s).** Seconds, mean ± 95% CI over 5 runs.

| Cell | Coordinator sel. p50 | Coordinator sel. p95 | Leaf sel. p50 | Job latency p50 | Job latency p95 | Wait p95 | Makespan |
|---|---|---|---|---|---|---|---|
| PBFT · analytic | 0.61 ± 0.17 | 2.17 ± 0.40 | 0.08 | 19.4 ± 0.7 | 91.1 ± 14.1 | 55.6 ± 17.2 | 284 ± 29 |
| Snow · analytic | 2.22 ± 0.61 | 3.40 ± 0.85 | 1.20 | 19.5 ± 0.7 | 55.8 ± 3.8 | 10.8 ± 1.8 | 237 ± 28 |
| Hybrid · analytic | 2.87 ± 0.68 | 3.86 ± 1.02 | 0.09 | 18.0 ± 1.2 | 53.5 ± 8.4 | 10.6 ± 1.2 | 243 ± 56 |
| PBFT · LinUCB shared | 0.13 | 1.88 ± 1.40 | 0.08 | 20.0 ± 0.9 | 118.7 ± 16.1 | 74.0 ± 26.2 | 291 ± 31 |
| PBFT · LinUCB per-group | 0.13 | 2.25 ± 1.16 | 0.08 | 20.4 ± 1.8 | 116.6 ± 15.9 | 71.6 ± 34.1 | 287 ± 44 |
| Snow · LinUCB shared | 26.25 ± 2.08 | 51.46 ± 3.12 | 1.19 | 52.5 ± 2.0 | 112.7 ± 6.5 | 1.6 ± 0.6 | 265 ± 24 |
| Snow · LinUCB per-group | 25.53 ± 3.36 | 48.30 ± 6.68 | 1.19 | 51.0 ± 4.1 | 109.4 ± 12.4 | 3.3 ± 2.3 | 279 ± 15 |
| Hybrid · LinUCB shared | 29.77 ± 4.57 | 54.98 ± 2.05 | 0.09 | 54.2 ± 4.7 | 107.6 ± 7.9 | 1.4 ± 0.2 | 264 ± 14 |
| Hybrid · LinUCB per-group | 27.28 ± 6.59 | 50.40 ± 8.00 | 0.09 | 50.5 ± 6.2 | 108.9 ± 5.4 | 1.4 ± 0.4 | 267 ± 28 |

**Cost and view quality, Hier-90, burst (20 jobs/s for 90 s).**

| Cell | Consensus msgs/job | Consensus KB/job | Overlay msgs/job | Proposers/job (coord.) | Busiest coordinator share | Least-used coordinator share | Busiest group share (bandit) | Context error (mean) | Jain fairness |
|---|---|---|---|---|---|---|---|---|---|
| PBFT · analytic | 260 ± 2 | 63 | 84 ± 8 | 2.0 ± 0.1 | 0.20 | 0.023 | – | 1.43 ± 0.25 | 0.46 |
| Snow · analytic | 296 ± 6 | 156 ± 3 | 71 ± 7 | 8.9 | 0.15 | 0.070 | – | 1.30 ± 0.18 | 0.54 |
| Hybrid · analytic | 271 ± 4 | 150 ± 2 | 73 ± 14 | 8.9 | 0.14 | 0.075 | – | 1.35 ± 0.31 | 0.52 |
| PBFT · LinUCB shared | 150 ± 2 | 36 | 84 ± 8 | 1.1 | 0.62 | 0.074 | 0.376 | 0.78 ± 0.18 | 0.35 |
| PBFT · LinUCB per-group | 148 ± 3 | 35 | 83 ± 11 | 1.2 | 0.63 | 0.086 | 0.360 | 0.73 ± 0.14 | 0.35 |
| Snow · LinUCB shared | 179 ± 2 | 52 | 80 ± 3 | 3.0 | 0.42 | 0.221 | 0.365 | 1.23 ± 0.08 | 0.51 |
| Snow · LinUCB per-group | 177 ± 4 | 52 | 81 ± 4 | 3.0 | 0.41 | 0.218 | 0.355 | 1.20 ± 0.12 | 0.50 |
| Hybrid · LinUCB shared | 172 | 51 | 78 ± 4 | 3.0 | 0.41 | 0.212 | 0.371 | 1.11 ± 0.11 | 0.48 |
| Hybrid · LinUCB per-group | 170 ± 2 | 50 | 78 ± 7 | 3.0 | 0.42 | 0.208 | 0.358 | 1.21 ± 0.20 | 0.48 |

**Consensus internals, Hier-90, burst (20 jobs/s for 90 s).** Per run, mean.

| Cell | Coord. finalize p50 (agent median) | Coord. rounds/decision | Leaf rounds/decision | Coord. certificate finalizes | Coord. commit-lock holds | Leaf certificate finalizes | Snow abandons (all tiers) |
|---|---|---|---|---|---|---|---|
| PBFT · analytic | 0.93 | – | – | 236 | 641 | 15 | 0.0 |
| Snow · analytic | 2.26 | 6.6 | 6.8 | 0 | 0 | 0 | 0.0 |
| Hybrid · analytic | 2.92 | 6.6 | – | 0 | 0 | 25 | 0.0 |
| PBFT · LinUCB shared | 0.45 | – | – | 27 | 182 | 18 | 0.0 |
| PBFT · LinUCB per-group | 0.46 | – | – | 32 | 247 | 17 | 0.0 |
| Snow · LinUCB shared | 27.81 | 10.0 | 6.4 | 0 | 0 | 0 | 0.0 |
| Snow · LinUCB per-group | 26.09 | 9.8 | 6.3 | 0 | 0 | 0 | 0.0 |
| Hybrid · LinUCB shared | 30.79 | 10.1 | – | 0 | 0 | 20 | 0.0 |
| Hybrid · LinUCB per-group | 28.14 | 9.8 | – | 0 | 0 | 17 | 0.0 |

**Selection p50 by measured RTT bin, Hier-90, burst (20 jobs/s for 90 s).** Coordinator tier binned by leaf–coordinator RTT; leaf tier by the executor's median RTT to its peers. Seconds.

| Cell | Coord. LAN | Coord. regional | Coord. continental | Coord. transatlantic | Leaf regional | Leaf transatlantic |
|---|---|---|---|---|---|---|
| PBFT · analytic | 0.31 | 0.58 | 0.92 | 0.46 | 0.07 | 0.14 |
| Snow · analytic | 2.39 | 2.25 | 2.08 | 2.35 | 1.19 | 1.59 |
| Hybrid · analytic | 2.98 | 2.94 | 2.67 | 2.97 | 0.07 | 0.13 |
| PBFT · LinUCB shared | 0.17 | 0.13 | 0.48 | 0.17 | 0.07 | 0.13 |
| PBFT · LinUCB per-group | 0.15 | 0.13 | 0.64 | 0.15 | 0.07 | 0.13 |
| Snow · LinUCB shared | 28.61 | 28.38 | 21.36 | 28.55 | 1.18 | 1.58 |
| Snow · LinUCB per-group | 29.84 | 26.23 | 20.97 | 29.67 | 1.18 | 1.58 |
| Hybrid · LinUCB shared | 34.72 | 30.53 | 26.75 | 32.00 | 0.08 | 0.13 |
| Hybrid · LinUCB per-group | 31.82 | 28.80 | 24.26 | 29.23 | 0.08 | 0.13 |

**Latency, Hier-90, steady Poisson 3 jobs/s for ~610 s.** Seconds, mean ± 95% CI over 5 runs.

| Cell | Coordinator sel. p50 | Coordinator sel. p95 | Leaf sel. p50 | Job latency p50 | Job latency p95 | Wait p95 | Makespan |
|---|---|---|---|---|---|---|---|
| PBFT · analytic | 0.12 | 0.18 | 0.10 | 11.7 | 36.5 ± 0.4 | 0.7 | 621 ± 2 |
| Snow · analytic | 0.92 | 1.13 ± 0.02 | 1.19 | 14.0 | 38.8 ± 0.1 | 0.7 | 624 ± 2 |
| Hybrid · analytic | 0.92 | 1.12 ± 0.01 | 0.09 | 13.0 | 37.8 ± 0.5 | 0.7 | 622 |
| PBFT · LinUCB shared | 0.08 | 0.14 ± 0.02 | 0.09 | 11.9 | 37.0 ± 0.4 | 0.7 | 620 |
| PBFT · LinUCB per-group | 0.08 | 0.15 | 0.08 | 11.9 | 37.2 ± 0.3 | 0.7 | 620 |
| Snow · LinUCB shared | 0.97 | 1.67 ± 0.13 | 1.19 | 14.6 ± 0.1 | 40.3 ± 0.5 | 0.7 | 624 |
| Snow · LinUCB per-group | 0.97 | 1.65 ± 0.16 | 1.19 | 14.6 | 40.2 ± 0.6 | 0.7 | 625 ± 2 |
| Hybrid · LinUCB shared | 0.97 | 1.65 ± 0.08 | 0.09 | 13.7 | 38.7 ± 0.5 | 0.7 | 625 ± 3 |
| Hybrid · LinUCB per-group | 0.97 | 1.65 ± 0.18 | 0.09 | 13.6 | 38.8 ± 0.4 | 0.7 | 622 ± 2 |

**Cost and view quality, Hier-90, steady Poisson 3 jobs/s for ~610 s.**

| Cell | Consensus msgs/job | Consensus KB/job | Overlay msgs/job | Proposers/job (coord.) | Busiest coordinator share | Least-used coordinator share | Busiest group share (bandit) | Context error (mean) | Jain fairness |
|---|---|---|---|---|---|---|---|---|---|
| PBFT · analytic | 283 | 64 | 188 | 1.5 | 0.20 | 0.030 | – | 0.14 | 0.29 |
| Snow · analytic | 524 ± 4 | 175 ± 1 | 188 | 8.9 | 0.15 | 0.088 | – | 0.14 ± 0.02 | 0.33 |
| Hybrid · analytic | 532 ± 2 | 178 ± 1 | 188 | 8.9 | 0.15 | 0.082 | – | 0.18 ± 0.02 | 0.31 |
| PBFT · LinUCB shared | 161 | 37 | 186 | 1.1 | 0.58 | 0.088 | 0.399 | 0.11 | 0.23 |
| PBFT · LinUCB per-group | 160 | 37 | 187 | 1.1 | 0.58 | 0.086 | 0.355 | 0.10 | 0.24 |
| Snow · LinUCB shared | 172 ± 2 | 43 | 187 | 2.7 | 0.36 | 0.309 | 0.392 | 0.21 ± 0.02 | 0.33 |
| Snow · LinUCB per-group | 172 ± 2 | 43 | 187 | 2.7 | 0.37 | 0.305 | 0.365 | 0.22 ± 0.02 | 0.34 |
| Hybrid · LinUCB shared | 195 ± 1 | 50 | 187 | 2.7 | 0.37 | 0.301 | 0.397 | 0.25 ± 0.03 | 0.30 |
| Hybrid · LinUCB per-group | 193 | 50 | 187 | 2.7 | 0.37 | 0.299 | 0.371 | 0.22 | 0.32 |

**Consensus internals, Hier-90, steady Poisson 3 jobs/s for ~610 s.** Per run, mean.

| Cell | Coord. finalize p50 (agent median) | Coord. rounds/decision | Leaf rounds/decision | Coord. certificate finalizes | Coord. commit-lock holds | Leaf certificate finalizes | Snow abandons (all tiers) |
|---|---|---|---|---|---|---|---|
| PBFT · analytic | 0.12 | – | – | 141 | 323 | 15 | 0.0 |
| Snow · analytic | 0.91 | 6.4 | 6.1 | 0 | 0 | 0 | 0.0 |
| Hybrid · analytic | 0.91 | 6.3 | – | 0 | 0 | 9 | 0.0 |
| PBFT · LinUCB shared | 0.08 | – | – | 9 | 117 | 9 | 0.0 |
| PBFT · LinUCB per-group | 0.08 | – | – | 12 | 130 | 10 | 0.0 |
| Snow · LinUCB shared | 0.97 | 7.4 | 6.1 | 0 | 0 | 0 | 0.0 |
| Snow · LinUCB per-group | 0.96 | 7.3 | 6.1 | 0 | 0 | 0 | 0.0 |
| Hybrid · LinUCB shared | 0.96 | 7.3 | – | 0 | 0 | 10 | 0.0 |
| Hybrid · LinUCB per-group | 0.96 | 7.3 | – | 0 | 0 | 7 | 0.0 |

**Selection p50 by measured RTT bin, Hier-90, steady Poisson 3 jobs/s for ~610 s.** Coordinator tier binned by leaf–coordinator RTT; leaf tier by the executor's median RTT to its peers. Seconds.

| Cell | Coord. LAN | Coord. regional | Coord. continental | Coord. transatlantic | Leaf regional | Leaf transatlantic |
|---|---|---|---|---|---|---|
| PBFT · analytic | 0.11 | 0.12 | 0.12 | 0.11 | 0.07 | 0.13 |
| Snow · analytic | 0.91 | 0.91 | 0.93 | 0.91 | 1.19 | 1.58 |
| Hybrid · analytic | 0.90 | 0.91 | 0.93 | 0.91 | 0.07 | 0.13 |
| PBFT · LinUCB shared | 0.08 | 0.08 | 0.09 | 0.08 | 0.07 | 0.13 |
| PBFT · LinUCB per-group | 0.08 | 0.08 | 0.09 | 0.08 | 0.07 | 0.13 |
| Snow · LinUCB shared | 0.96 | 1.01 | 0.97 | 0.97 | 1.17 | 1.58 |
| Snow · LinUCB per-group | 0.95 | 1.01 | 0.96 | 0.97 | 1.17 | 1.58 |
| Hybrid · LinUCB shared | 0.98 | 1.01 | 0.96 | 0.97 | 0.07 | 0.13 |
| Hybrid · LinUCB per-group | 0.97 | 1.01 | 0.97 | 0.97 | 0.07 | 0.13 |

**E0, PBFT · analytic.** Seconds, mean ± 95% CI over 5 runs.

| Metric | Hier-30 burst | Hier-90 burst | Hier-90 at 3 jobs/s |
|---|---|---|---|
| Leaf selection p50 | 0.05 | 0.08 | 0.10 |
| Leaf selection p95 | 0.90 ± 0.18 | 0.37 ± 0.09 | 0.16 |
| Coordinator selection p50 | 0.25 ± 0.04 | 0.61 ± 0.17 | 0.12 |
| Coordinator selection p95 | 1.62 ± 0.16 | 2.17 ± 0.40 | 0.18 |
| Job latency p50 | 4.2 ± 0.6 | 19.4 ± 0.7 | 11.7 |
| Job latency p95 | 35.5 ± 10.4 | 91.1 ± 14.1 | 36.5 ± 0.4 |
| Wait p95 | 9.1 ± 10.2 | 55.6 ± 17.2 | 0.7 |
| Execution p95 | 16.8 | 34.6 | 34.6 |
| Makespan | 164 ± 10 | 284 ± 29 | 621 ± 2 |
| Throughput (jobs/s) | 3.66 ± 0.22 | 6.37 ± 0.70 | 2.90 |
| Consensus msgs/job | 66 ± 1 | 260 ± 2 | 283 |
| Busiest coordinator share | 0.50 ± 0.06 | 0.20 ± 0.02 | 0.20 |
| Least-used coordinator share | 0.007 ± 0.018 | 0.023 ± 0.024 | 0.030 ± 0.003 |

### 3.1 Old model against new, cell means

| Hier-90 cell | Job p95 (s) | Wait p95 (s) | Throughput (jobs/s) | Makespan (s) | Leaves that ran work |
|---|---|---|---|---|---|
| PBFT · analytic, burst | 206 → 91 | 183 → 56 | 4.1 → 6.4 | 443 → 284 | 26 → 78 |
| Snow · analytic, burst | 378 → 56 | 351 → 11 | 2.7 → 7.6 | 670 → 237 | 20 → 81 |
| Hybrid · analytic, burst | 323 → 53 | 290 → 11 | 3.1 → 7.6 | 588 → 243 | 20 → 81 |
| PBFT · LinUCB shared, burst | 255 → 119 | 228 → 74 | 3.6 → 6.2 | 496 → 291 | 30 → 73 |
| Snow · LinUCB shared, burst | 242 → 113 | 149 → 1.6 | 4.0 → 6.8 | 451 → 265 | 28 → 80 |
| Hybrid · LinUCB shared, burst | 249 → 108 | 125 → 1.4 | 4.1 → 6.8 | 437 → 264 | 27 → 80 |
| PBFT · analytic, 3 jobs/s | 64 → 36 | 27 → 0.7 | 2.9 → 2.9 | 629 → 621 | 17 → 57 |
| Snow · analytic, 3 jobs/s | 119 → 39 | 92 → 0.7 | 2.3 → 2.9 | 773 → 624 | 13 → 51 |
| Snow · LinUCB shared, 3 jobs/s | 78 → 40 | 45 → 0.7 | 2.8 → 2.9 | 645 → 624 | 16 → 64 |

At 3 jobs/s the fleet keeps up, so throughput equals the arrival rate under both models. Hier-30 PBFT
under the burst: job p95 57 → 35 s, throughput 2.9 → 3.7 jobs/s, leaves that ran work 10 → 20 of 27.

## 4. What the numbers say

### 4.1 Queueing was the cost model

Wait p95 falls to 0.7 s in every 3 jobs/s cell, from 27–114 s. Under the burst it falls to 11 s for the
analytic Snow and Hybrid cells and to 1.4–3.3 s for the LinUCB Snow and Hybrid cells. The latency
tails of the fc9eb220 campaign, which its write-up attributed to "waiting for a free leaf", were
caused by the old model leaving 54–74 leaves idle. Now, under the burst, 78–81 of 81 leaves run work in
the analytic block and 73–81 of 87 in the LinUCB block (cell means). At 3 jobs/s the 19–31 that stay
idle in run01 are mostly 2-core leaves, plus up to 8 4-core ones under PBFT, and that is expected. A 1-core, 4 GB job fills
half of such a leaf, a large leaf reaches that price only at 50 % utilization, and at this load no
job waited.

### 4.2 C1: under load, Snow now places better than PBFT

Under the burst, the analytic block is now ranked the other way round from fc9eb220:

| Burst, Hier-90 analytic | Coordinator decision p50 | Job p95 | Wait p95 | Throughput | Busiest / least-used coordinator | Jain fairness |
|---|---|---|---|---|---|---|
| PBFT | 0.61 s | 91 s | 56 s | 6.4 jobs/s | 20 % / 2.3 % | 0.46 |
| Snow | 2.22 s | 56 s | 11 s | 7.6 jobs/s | 15 % / 7.0 % | 0.54 |
| Hybrid | 2.87 s | 53 s | 11 s | 7.6 jobs/s | 14 % / 7.5 % | 0.52 |

PBFT still decides 3.6–4.7× faster, yet Snow and Hybrid finish sooner. The difference is placement. A
Snow vote follows the cost: a peer backs the cheapest coordinator it knows of. With costs that now price
load, Snow's coordinator tier spreads work across all nine groups (121–268 jobs each in run01). PBFT's
coordinator tier is a proposal race. The first proposal to reach quorum wins, and a cheaper one only
displaces it if it is seen in time. So PBFT still concentrates. In run01 coordinator 90 won 0 jobs and
89 won 98, while 86 won 396. In the LinUCB block PBFT gives 90 64 % of all jobs (1,160 of 1,800), and its
wait p95 is 72–74 s against 1.4–3.3 s for Snow and Hybrid.

**This is a candidate C1 finding, and it must be stated with its conditions.** It comes from one scale
(Hier-90) and one workload, under `coordinator_cost_matrix: self`. The mechanism is placement quality,
not decision speed. At 3 jobs/s, where nothing queues, the engines tie on job latency (36.5–40.3 s) and
PBFT keeps its decision-time lead (0.08–0.12 s against 0.92–0.97 s, Snow's β = 6 floor). Why PBFT's race
favours some coordinators (90 in the LinUCB block, never 89, the far host) is not yet investigated.

### 4.3 Regression: a three-member Snow tier decides slowly under the burst

| Burst, LinUCB block | Coordinator decision p50 | Rounds per decision |
|---|---|---|
| Snow shared / per-group | 15–17 s → **26.3 / 25.5 s** | 8.2–8.3 → 10.0 / 9.8 |
| Hybrid shared / per-group | 15–16 s → **29.8 / 27.3 s** | 8.3–8.4 → 10.1 / 9.8 |

The values are fc9eb220 → f45d9ff1. With three coordinators the Snow sample is both peers and α needs both
to agree. Coordinator costs are now close together, so rounds split more often. It does not reach job
latency (107–113 s, the lowest in the LinUCB block), and it disappears at 3 jobs/s (0.97 s). It is still
a cost the paper must report. Candidate remedies: a tie tolerance in the peer's dominance check, or a
larger coordinator tier.

### 4.4 C2, bandit design: still a tie

The busiest group per coordinator under the burst, shared → per-group: PBFT 37.6 → 36.0 %, Snow 36.5 →
35.5 %, Hybrid 37.1 → 35.8 %. At 3 jobs/s it is 39.9 → 35.5, 39.2 → 36.5 and 39.7 → 37.1 %. Even is 33 %.
Job latency lies inside the interval for every pair. The per-group model stays a few points more even,
as in fc9eb220. Separating the designs needs E2's failure profile.

### 4.5 F6 input: the context-error spread narrowed

| Mean in-flight miscount | Analytic burst | Analytic 3 jobs/s | LinUCB burst | LinUCB 3 jobs/s |
|---|---|---|---|---|
| PBFT | 1.06 → 1.43 | 0.15 → 0.14 | 0.61 → 0.73–0.78 | 0.11 → 0.10–0.11 |
| Snow | 2.13 → 1.30 | 0.45 → 0.14 | 1.40 → 1.20–1.23 | 0.57–0.61 → 0.21–0.22 |
| Hybrid | 2.13 → 1.35 | 0.47 → 0.18 | 1.41 → 1.11–1.21 | 0.59–0.60 → 0.22–0.25 |

The values are fc9eb220 → f45d9ff1. In the analytic block the engines now differ little: at 3 jobs/s they are
equal, and under the burst Snow is now the lower. The LinUCB block keeps a 2× spread at both loads, so
that is where F6's x-axis survives. Under the old model much of Snow's larger error came from the idle
groups. A coordinator that never received work also never refreshed its view of it.

### 4.6 F3: decision time does not depend on WAN distance

At 3 jobs/s, every engine's coordinator selection is flat across the LAN, regional, continental and
transatlantic RTT bins: 0.11–0.12 s for PBFT, 0.90–1.01 s for Snow. Only the leaf tier's transatlantic
bin is slower, 1.58 s against 1.19 s for Snow leaves and 0.13 s against 0.07 s for PBFT leaves. Under
the burst the coordinator bins spread more widely, from contention rather than distance.

### 4.7 Cost of consensus

Consensus bytes per job, analytic block: PBFT 63–64 KB, Snow 156–175 KB, Hybrid 150–178 KB, about 2.5×
because every Snow coordinator runs its own instance per job (proposers/job 8.9 against 1.5–2.0).
LinUCB block: 35–37, 43–52 and 50–51 KB. Message counts rose against fc9eb220 (PBFT analytic burst 227 →
260, Snow 188 → 296). More groups take part in each decision, and Snow batches less when the vote is
spread. Report bytes beside counts, as before. PBFT's commit-certificate path finalized 236 of 1,800
coordinator decisions per run under the burst (141 at 3 jobs/s), and 641 COMMITs per run were held by
the binding-commit lock. Nothing was stranded.

## 5. Caveats to state

* One scale (Hier-90, plus Hier-30 for E0), one workload (replay-golden, simulated execution clamped at
  120 s), no injected failures. No routing-quality claim comes from E1′.
* Analytic and LinUCB blocks differ in topology. Never compare across them.
* `coordinator_cost_matrix: self` throughout. The E5 `peers` arm is still unrun.
* The Snow-over-PBFT result under the burst is about placement and holds only while there is queueing.
  At 3 jobs/s the engines tie.
* PBFT's uneven coordinator spread is a scan race decided by distance to the database (§7). It is not
  cost-driven.

## 6. Open questions

1. ~~Why PBFT's proposal race favours or starves particular coordinators.~~ Answered in §7: the first
   coordinator to scan Redis after a batch proposes it, and its peers adopt that proposal whatever it costs.
2. Whether a tie tolerance in Snow's dominance check removes the three-member slowdown without
   harming placement. **Built 2026-10-19** (`consensus.snow.tie_tolerance: 0.05`), together with
   PBFT counter-proposals for §7 (`consensus.pbft.counter_propose: true`). Neither is piloted yet.
3. Whether Snow's placement advantage survives at Hier-270 and Mesh-180, and under the E5 `peers` arm.

## 7. Why PBFT's coordinator tier favours some coordinators (investigated 2026-10-19)

**Short answer.** Under `coordinator_cost_matrix: self`, PBFT's coordinator tier gives each job to whichever
coordinator proposes it first. Cost plays almost no part. The first proposer is the coordinator whose periodic
Redis scan finds the new jobs first, which favours coordinators close to the database. The job distributor
writes a batch of 20 jobs a second, and one coordinator then wins 95–99 % of each batch. This is a property of
how the tier is set up, not a defect in the engine. Snow does not show it, because a Snow peer compares its own
price with the initiator's on every query.

**Mechanism, from the code (`engine.py` `on_proposal`).** A coordinator that has not yet proposed a job
accepts the first proposal it receives for it. `has_better_proposal` finds nothing of its own to compare
against, so it prepares the incoming proposal and marks the job in-vote. Its selection loop takes only jobs
still pending (`pending_queue.gets(states=[PENDING])`), so it never proposes that job itself. Cost decides
only between proposals a coordinator has already seen. A coordinator learns of a new job only in its periodic
tick, after the delegation monitor, the child refresh, failure detection and its own record save, all Redis
round trips (`_update_pending_jobs`). So whoever scans first after a batch lands proposes the whole batch
(`jobs_per_proposal: 20`). Its proposal reaches the others in one RTT, before their own scan does.

**Evidence, from every PBFT run of the campaign (`racecorr.py`, `batchwin.py`).**

| Cell | Coordinators, nearest to the database first: share of jobs won |
|---|---|
| Hier-30 analytic burst | 29 @ 0.1 ms 60–73 % · 28 @ 13.5 ms 26–40 % · 30 @ 84.7 ms 0–4 % (5 of 5 runs, same order) |
| Hier-90 LinUCB shared burst | 90 @ 0.1 ms 55–66 % · 88 @ 13.7 ms 26–39 % · 89 @ 73.9 ms 6–10 % (5 of 5) |
| Hier-90 LinUCB shared 3 jobs/s | 90 @ 0.1 ms 57–60 % · 88 @ 14 ms 31–34 % · 89 @ 73.8 ms 8–10 % (5 of 5) |
| Hier-90 analytic 3 jobs/s | 90 @ 0.1 ms ~20 % · 86 @ 2.4 ms ~19 % · 88 @ 13.7 ms 14 % … 82 @ 34 ms 7 % · 89 @ 74 ms 3 % (5 of 5) |
| Hier-90 analytic burst | 86 @ 2.4 ms 15–22 % … 89 @ 74 ms 3–5 %; but 90 @ 0.1 ms only 0–14 % |

* **Cost does not order the winners.** In the LinUCB block the coordinator that wins most, 90, leads the
  smallest subtree (27 leaves, 180 cores against 210 and 236), so it is the most expensive when idle. The
  cheapest, 89, wins least.
* **Batches are won whole.** In each one-second distributor batch, one coordinator wins 95–99 % of its jobs.
  79–86 of ~90 batches per run go to a single coordinator.
* **The exception confirms the mechanism.** In the analytic burst runs coordinator 90 is nearest the database,
  yet in run01 it proposed nothing at all (its selection counters are empty and its cost cache never ran).
  It was a participant for all 1,800 jobs: every job reached it already in a peer's vote. Batch winners there
  rotate among 82–89. With nine coordinators all scanning on ~0.5 s ticks against a 1 s batch, which one
  scans first after a batch depends on tick phase, and 90 lost that phase for the whole run. In run02
  and run05 it won the opening batches (14 %). Not explained: why its phase lost so consistently. Its tick is
  not logged, and the next step would be per-tick timing.
* **At steady arrivals the order is smooth.** Poisson arrivals at 3 jobs/s remove the batch alignment, and the
  shares then fall steadily with distance to the database (20 % down to 3 %).

**What it means.**
* **PBFT's coordinator placement is a scan race, not a cost decision.** It is part of why PBFT places worse than
  Snow under load (§4.2): at the burst its busiest LinUCB coordinator's groups queue (wait p95 72–74 s) while
  Snow's do not. It is also the setting behind the Hier-30 E0 quirk (coordinator 30, 85 ms from the database,
  wins 0–4 %).
* **For E2 (Hier-90, G=3, 3 coordinators) it skews learning.** Under PBFT coordinator 90 makes ~60 % of the
  delegation decisions and 89 ~8 %, so per-coordinator learning curves and E2a/E2b re-learning times would mix
  the engine's placement with the bandit's quality. Aggregate regret per decision stays valid. Report it per
  coordinator, or weight by decisions.
* **Options, none applied.** (a) Report it as measured: it is real behaviour of PBFT under this coordinator
  setup. (b) The E5 `peers` arm, where coordinators score each other and only the cheapest proposes, should
  remove the race. That is now a specific prediction for E5. (c) Change PBFT so that a coordinator receiving a
  proposal it has not matched compares the proposal's cost with its own, and proposes itself if cheaper, as a
  Snow peer does. That changes PBFT semantics and every PBFT number, so it would be a decision, not a fix.
  (d) Shrink `jobs_per_proposal` at the coordinator tier, so a batch can split across coordinators.
  That reduces the effect without changing the protocol.

## 8. Files

| File | Contents |
|---|---|
| `runs_wide.csv` | `evaluation/collect.py` output, 95 rows, including the new `cost_model` column |
| `supp.csv` | `../e0-e1-fc9eb220/supp.py` output |
| `runs_merged.csv`, `cells_agg.csv` | joined per run, and aggregated per cell by `../e0-e1-fc9eb220/agg.py` |
| `tables.md` | §3, generated by `../e0-e1-fc9eb220/tables.py` |

`decisions.csv` and `job_rtt.csv` (165,000 rows each) stay on the database node in
`/root/collect-f45d9ff1/`. The scripts are the fc9eb220 ones, run unchanged.
