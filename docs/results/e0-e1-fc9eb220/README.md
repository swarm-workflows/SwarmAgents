# E0 / E1′ campaign at fc9eb220 — results and analysis

Collected 2026-10-15 from `runs/campaign-e0-e1-fc9eb220` on the database node. This is the first
E0/E1′ campaign in which every run is citable. It supersedes the 9177551a campaign of 13–14 Oct,
whose Snow and Hybrid numbers carried the far-host send-pool defect (see §7).

## 1. What was run

| Item | Value |
|---|---|
| Code revision | fc9eb220 on every agent and the driver; campaign file `campaigns/e0-e1-hier30-hier90.yml` at b20820da |
| Window | 14 Oct 20:13 UTC to 15 Oct 19:47 UTC, about 23.5 h |
| Runs | 19 cells × 5 repeats = 95, all `ok` on the first attempt, no `.attempt` directories |
| Substrate | 17-site FABRIC slice, one agent per VM, `--agents-per-host 1`, interleaved placement, clocks checked by the campaign gate |
| Fleet | `--seed 42 --master-fleet-size 270 --all-dtns`, level-1 agents `resource` |
| Workload | replay-golden Pegasus profiles (`/root/workloads/jobs_1800`), simulated execution clamped at 120 s; 600 jobs at Hier-30, 1,800 at Hier-90; no DAG predicates, no injected failures |
| Arrival | **burst**: 20 jobs/s, all jobs in ~90 s (≈ 5× the Hier-90 drain rate of ~4 jobs/s); **poisson3**: Poisson 3 jobs/s, seeded, ~610 s of arrivals (realised 2.95 jobs/s, planned-vs-actual lag ≤ 10 ms) |
| Analytic cells | Hier-90 with `--groups-per-coordinator 1`: 9 coordinators, one group of 9 leaves each, so delegation is `trivial` |
| LinUCB cells | Hier-90 with `--groups-per-coordinator 3`: 3 coordinators, 3 groups each; `mab.load_slack 9`, `reward.shaped: true`; shared vs per-group (`mab.linucb.model: disjoint`) is the only difference within a pair |
| Engines | PBFT; Snow (shipped config: `k 10, alpha 0.7, beta 6, round_timeout_ms 300, send_workers 64, max_inflight 16`); Hybrid = PBFT leaves + Snow coordinators |
| Coordinator cost | `coordinator_cost_matrix: self` (shipped): every coordinator scores only itself |

**Comparability rule.** The analytic block (9 coordinators × 1 group) and the LinUCB block
(3 coordinators × 3 groups) differ in topology as well as decision plane. Compare engines
*within* a block. E0 Hier-90 burst is also the E1′ PBFT · analytic cell.

## 2. Validity: every run is clean

Measured from each run's `metrics.json` and `drain.json`, never from logs (run directories hold
stale logs from hosts outside the cell).

| Check | Result in all 95 runs |
|---|---|
| Jobs completed / succeeded | 600/600 at Hier-30, 1,800/1,800 at Hier-90 (completion 100 %, success 100 %) |
| Drain | `drained`, 0 pending and 0 in flight at stop |
| Jobs executed twice | 0 |
| Jobs unrun | 0 |
| Reselection restarts | 0 |
| Decided-unpersisted re-elections | 0 |
| False failure verdicts | 0 |
| Sends dropped (any type) | 0 |
| Snow abandoned votes | 0, except one coordinator vote in one run (snow-linucb-pergroup run05) |
| Metrics complete / barrier short | all agents reported; 0 agents short at the startup barrier |
| Failure detector judged from a stale view | refreshed 0–4 times per run, skipped 0; no verdicts followed |

**Reproducibility check against the previous campaign.** PBFT · analytic did not change between
9177551a and fc9eb220 in any way that should move it, and it did not move: coordinator selection
p50 0.58 → 0.56 s, job latency p95 214 → 206 s, consensus messages per job 230 → 227.

## 3. Headline results

Notation: *coordinator selection* is the level-1 decision time (job offered to the coordinator
tier → assigned to a coordinator); *leaf selection* is the level-0 decision inside a group; *wait*
is assignment → start on a leaf. All values are means over 5 runs ± a 95 % t-interval (4 degrees of
freedom); a value with no interval had a half-width below the last digit shown.

**Latency, Hier-90, burst (20 jobs/s for 90 s).** Seconds, mean ± 95% CI over 5 runs.

| Cell | Coordinator sel. p50 | Coordinator sel. p95 | Leaf sel. p50 | Job latency p50 | Job latency p95 | Wait p95 | Makespan |
|---|---|---|---|---|---|---|---|
| PBFT · analytic | 0.56 ± 0.05 | 1.98 ± 0.27 | 0.15 | 32.1 ± 2.6 | 205.9 ± 37.4 | 183.3 ± 38.8 | 443 ± 45 |
| Snow · analytic | 1.04 ± 0.05 | 1.82 ± 0.26 | 1.21 | 47.5 ± 3.8 | 378.3 ± 36.7 | 351.0 ± 33.6 | 670 ± 40 |
| Hybrid · analytic | 1.04 ± 0.09 | 1.80 ± 0.35 | 0.14 | 43.5 ± 4.1 | 322.8 ± 31.8 | 290.5 ± 27.1 | 588 ± 42 |
| PBFT · LinUCB shared | 0.13 | 2.01 ± 1.07 | 0.18 ± 0.01 | 33.2 ± 4.7 | 254.7 ± 20.2 | 228.1 ± 8.8 | 496 ± 42 |
| PBFT · LinUCB per-group | 0.13 | 2.52 ± 2.10 | 0.18 ± 0.01 | 33.1 ± 3.2 | 263.2 ± 31.6 | 228.3 ± 33.6 | 539 ± 73 |
| Snow · LinUCB shared | 14.73 ± 0.62 | 34.91 ± 2.11 | 1.21 | 73.6 ± 4.4 | 242.0 ± 12.8 | 148.6 ± 12.6 | 451 ± 51 |
| Snow · LinUCB per-group | 16.53 ± 2.33 | 39.18 ± 4.98 | 1.21 | 71.4 ± 2.9 | 253.0 ± 13.4 | 120.3 ± 19.3 | 459 ± 22 |
| Hybrid · LinUCB shared | 15.82 ± 1.01 | 39.68 ± 4.64 | 0.12 | 71.0 ± 5.8 | 249.0 ± 19.9 | 125.3 ± 33.4 | 437 ± 31 |
| Hybrid · LinUCB per-group | 15.44 ± 0.74 | 39.07 ± 3.31 | 0.12 | 70.1 ± 4.0 | 252.8 ± 8.4 | 127.6 ± 22.0 | 464 ± 67 |

**Cost and view quality, Hier-90, burst (20 jobs/s for 90 s).**

| Cell | Consensus msgs/job | Consensus KB/job | Overlay msgs/job | Proposers/job (coord.) | Busiest coordinator share | Least-used coordinator share | Busiest group share (bandit) | Context error (mean) | Jain fairness |
|---|---|---|---|---|---|---|---|---|---|
| PBFT · analytic | 227 ± 4 | 58 | 124 ± 11 | 1.9 | 0.25 | 0.007 | – | 1.06 ± 0.14 | 0.17 |
| Snow · analytic | 188 ± 6 | 145 ± 4 | 179 ± 10 | 8.8 | 0.36 | 0.000 | – | 2.13 ± 0.21 | 0.10 |
| Hybrid · analytic | 235 ± 4 | 138 ± 2 | 159 ± 10 | 8.8 | 0.34 | 0.000 | – | 2.13 ± 0.19 | 0.11 |
| PBFT · LinUCB shared | 113 ± 3 | 31 | 135 ± 11 | 1.1 | 0.62 | 0.080 | 0.384 | 0.62 ± 0.09 | 0.14 |
| PBFT · LinUCB per-group | 114 ± 2 | 31 | 146 ± 18 | 1.1 | 0.62 | 0.085 | 0.358 | 0.61 ± 0.03 | 0.14 |
| Snow · LinUCB shared | 109 ± 5 | 49 ± 5 | 124 ± 13 | 3.0 | 0.51 | 0.110 | 0.386 | 1.40 ± 0.13 | 0.19 |
| Snow · LinUCB per-group | 107 ± 3 | 47 ± 2 | 126 ± 6 | 3.0 | 0.50 | 0.120 | 0.363 | 1.43 ± 0.14 | 0.19 |
| Hybrid · LinUCB shared | 160 ± 1 | 49 | 121 ± 7 | 3.0 | 0.50 | 0.113 | 0.380 | 1.44 ± 0.12 | 0.19 |
| Hybrid · LinUCB per-group | 161 ± 2 | 49 | 127 ± 17 | 3.0 | 0.50 | 0.117 | 0.359 | 1.41 ± 0.12 | 0.19 |

**Consensus internals, Hier-90, burst (20 jobs/s for 90 s).** Per run, mean.

| Cell | Coord. finalize p50 (agent median) | Coord. rounds/decision | Leaf rounds/decision | Coord. certificate finalizes | Coord. commit-lock holds | Leaf certificate finalizes | Snow abandons (all tiers) |
|---|---|---|---|---|---|---|---|
| PBFT · analytic | 0.86 | – | – | 275 | 541 | 9 | 0.0 |
| Snow · analytic | 1.06 | 6.1 | 7.7 | 0 | 0 | 0 | 0.0 |
| Hybrid · analytic | 1.05 | 6.1 | – | 0 | 0 | 9 | 0.0 |
| PBFT · LinUCB shared | 0.46 | – | – | 29 | 191 | 10 | 0.0 |
| PBFT · LinUCB per-group | 0.46 | – | – | 29 | 183 | 8 | 0.0 |
| Snow · LinUCB shared | 17.86 | 8.2 | 7.4 | 0 | 0 | 0 | 0.0 |
| Snow · LinUCB per-group | 21.17 | 8.3 | 7.1 | 0 | 0 | 0 | 0.2 |
| Hybrid · LinUCB shared | 20.63 | 8.3 | – | 0 | 0 | 10 | 0.0 |
| Hybrid · LinUCB per-group | 19.69 | 8.4 | – | 0 | 0 | 10 | 0.0 |

**Selection p50 by measured RTT bin, Hier-90, burst (20 jobs/s for 90 s).** Coordinator tier binned by leaf–coordinator RTT; leaf tier by the executor's median RTT to its peers. Seconds.

| Cell | Coord. LAN | Coord. regional | Coord. continental | Coord. transatlantic | Leaf regional | Leaf transatlantic |
|---|---|---|---|---|---|---|
| PBFT · analytic | 0.29 | 0.56 | 0.98 | 0.34 | 0.13 | 2.27 |
| Snow · analytic | 1.15 | 1.04 | 1.01 | 1.13 | 1.19 | 2.07 |
| Hybrid · analytic | 1.22 | 1.06 | 1.00 | 1.15 | 0.13 | 1.11 |
| PBFT · LinUCB shared | 0.14 | 0.12 | 0.83 | 0.14 | 0.20 | 0.77 |
| PBFT · LinUCB per-group | 0.17 | 0.12 | 1.12 | 0.16 | 0.20 | 0.66 |
| Snow · LinUCB shared | 21.81 | 21.61 | 7.59 | 16.70 | 1.20 | 1.59 |
| Snow · LinUCB per-group | 27.30 | 25.33 | 8.61 | 18.92 | 1.20 | 1.58 |
| Hybrid · LinUCB shared | 27.42 | 25.61 | 7.59 | 19.97 | 0.09 | 0.41 |
| Hybrid · LinUCB per-group | 27.41 | 25.64 | 7.62 | 18.66 | 0.09 | 0.38 |

**Latency, Hier-90, steady Poisson 3 jobs/s for ~610 s.** Seconds, mean ± 95% CI over 5 runs.

| Cell | Coordinator sel. p50 | Coordinator sel. p95 | Leaf sel. p50 | Job latency p50 | Job latency p95 | Wait p95 | Makespan |
|---|---|---|---|---|---|---|---|
| PBFT · analytic | 0.12 | 0.18 | 0.11 | 11.9 | 63.5 ± 23.2 | 26.9 ± 17.7 | 629 ± 24 |
| Snow · analytic | 0.91 | 1.07 | 1.19 | 17.8 ± 0.2 | 119.2 ± 16.1 | 92.3 ± 19.7 | 773 ± 15 |
| Hybrid · analytic | 0.91 | 1.07 | 0.10 | 16.8 ± 0.9 | 140.3 ± 13.7 | 113.7 ± 12.4 | 772 ± 4 |
| PBFT · LinUCB shared | 0.08 | 0.14 ± 0.01 | 0.11 | 12.3 ± 0.1 | 59.4 ± 6.3 | 30.2 ± 6.7 | 621 ± 3 |
| PBFT · LinUCB per-group | 0.08 | 0.15 ± 0.01 | 0.11 | 12.3 ± 0.2 | 60.3 ± 3.4 | 28.1 ± 2.8 | 621 ± 2 |
| Snow · LinUCB shared | 0.95 | 1.20 | 1.21 | 17.9 ± 0.6 | 77.9 ± 14.5 | 45.5 ± 12.8 | 645 ± 23 |
| Snow · LinUCB per-group | 0.95 | 1.21 | 1.21 | 18.2 ± 1.2 | 84.1 ± 14.2 | 51.1 ± 12.1 | 644 ± 49 |
| Hybrid · LinUCB shared | 0.95 | 1.20 | 0.12 | 16.1 ± 0.4 | 77.9 ± 4.0 | 45.9 ± 8.1 | 626 ± 4 |
| Hybrid · LinUCB per-group | 0.95 | 1.21 | 0.12 | 16.3 ± 0.6 | 73.6 ± 4.7 | 40.8 ± 5.2 | 640 ± 24 |

**Cost and view quality, Hier-90, steady Poisson 3 jobs/s for ~610 s.**

| Cell | Consensus msgs/job | Consensus KB/job | Overlay msgs/job | Proposers/job (coord.) | Busiest coordinator share | Least-used coordinator share | Busiest group share (bandit) | Context error (mean) | Jain fairness |
|---|---|---|---|---|---|---|---|---|---|
| PBFT · analytic | 279 ± 1 | 64 | 187 | 1.5 | 0.21 | 0.023 | – | 0.15 | 0.14 |
| Snow · analytic | 479 ± 3 | 164 | 206 ± 4 | 8.9 | 0.38 | 0.000 | – | 0.45 ± 0.03 | 0.08 |
| Hybrid · analytic | 518 ± 2 | 172 | 205 ± 1 | 8.9 | 0.38 | 0.000 | – | 0.47 ± 0.02 | 0.08 |
| PBFT · LinUCB shared | 153 ± 1 | 36 | 187 | 1.1 | 0.59 | 0.088 | 0.415 | 0.11 | 0.10 |
| PBFT · LinUCB per-group | 152 ± 2 | 36 | 186 | 1.1 | 0.59 | 0.087 | 0.375 | 0.11 ± 0.01 | 0.10 |
| Snow · LinUCB shared | 129 ± 3 | 35 | 187 | 2.5 | 0.53 | 0.000 | 0.369 | 0.61 ± 0.06 | 0.09 |
| Snow · LinUCB per-group | 127 ± 1 | 35 | 187 ± 2 | 2.5 | 0.54 | 0.000 | 0.363 | 0.57 ± 0.03 | 0.09 |
| Hybrid · LinUCB shared | 194 ± 1 | 50 | 187 | 2.6 | 0.52 | 0.000 | 0.384 | 0.60 ± 0.02 | 0.09 |
| Hybrid · LinUCB per-group | 192 | 50 | 187 | 2.5 | 0.53 | 0.000 | 0.370 | 0.59 ± 0.03 | 0.09 |

**Consensus internals, Hier-90, steady Poisson 3 jobs/s for ~610 s.** Per run, mean.

| Cell | Coord. finalize p50 (agent median) | Coord. rounds/decision | Leaf rounds/decision | Coord. certificate finalizes | Coord. commit-lock holds | Leaf certificate finalizes | Snow abandons (all tiers) |
|---|---|---|---|---|---|---|---|
| PBFT · analytic | 0.12 | – | – | 89 | 296 | 3 | 0.0 |
| Snow · analytic | 0.91 | 6.2 | 6.2 | 0 | 0 | 0 | 0.0 |
| Hybrid · analytic | 0.91 | 6.1 | – | 0 | 0 | 5 | 0.0 |
| PBFT · LinUCB shared | 0.08 | – | – | 12 | 117 | 4 | 0.0 |
| PBFT · LinUCB per-group | 0.08 | – | – | 13 | 129 | 2 | 0.0 |
| Snow · LinUCB shared | 0.94 | 6.3 | 6.1 | 0 | 0 | 0 | 0.0 |
| Snow · LinUCB per-group | 0.94 | 6.3 | 6.1 | 0 | 0 | 0 | 0.0 |
| Hybrid · LinUCB shared | 0.93 | 6.3 | – | 0 | 0 | 2 | 0.0 |
| Hybrid · LinUCB per-group | 0.94 | 6.3 | – | 0 | 0 | 3 | 0.0 |

**Selection p50 by measured RTT bin, Hier-90, steady Poisson 3 jobs/s for ~610 s.** Coordinator tier binned by leaf–coordinator RTT; leaf tier by the executor's median RTT to its peers. Seconds.

| Cell | Coord. LAN | Coord. regional | Coord. continental | Coord. transatlantic | Leaf regional | Leaf transatlantic |
|---|---|---|---|---|---|---|
| PBFT · analytic | 0.11 | 0.12 | 0.11 | 0.12 | 0.09 | 0.13 |
| Snow · analytic | 0.89 | 0.91 | 0.92 | 0.91 | 1.17 | 1.57 |
| Hybrid · analytic | 0.91 | 0.91 | 0.92 | 0.91 | 0.11 | 0.12 |
| PBFT · LinUCB shared | 0.07 | 0.08 | 0.09 | 0.08 | 0.08 | 0.13 |
| PBFT · LinUCB per-group | 0.08 | 0.08 | 0.09 | 0.08 | 0.09 | 0.13 |
| Snow · LinUCB shared | 0.92 | 0.91 | 0.96 | 0.91 | 1.19 | 1.58 |
| Snow · LinUCB per-group | 0.90 | 0.91 | 0.96 | 0.91 | 1.19 | 1.69 |
| Hybrid · LinUCB shared | 0.93 | 0.91 | 0.96 | 0.91 | 0.08 | 0.12 |
| Hybrid · LinUCB per-group | 0.94 | 0.91 | 0.96 | 0.91 | 0.08 | 0.13 |

**E0, PBFT · analytic.** Seconds, mean ± 95% CI over 5 runs.

| Metric | Hier-30 burst | Hier-90 burst | Hier-90 at 3 jobs/s |
|---|---|---|---|
| Leaf selection p50 | 0.34 ± 0.10 | 0.15 | 0.11 |
| Leaf selection p95 | 11.09 ± 6.42 | 5.14 ± 2.99 | 0.17 |
| Coordinator selection p50 | 0.23 ± 0.02 | 0.56 ± 0.05 | 0.12 |
| Coordinator selection p95 | 1.35 ± 0.26 | 1.98 ± 0.27 | 0.18 |
| Job latency p50 | 6.1 ± 1.8 | 32.1 ± 2.6 | 11.9 |
| Job latency p95 | 57.4 ± 14.0 | 205.9 ± 37.4 | 63.5 ± 23.2 |
| Wait p95 | 22.5 ± 8.7 | 183.3 ± 38.8 | 26.9 ± 17.7 |
| Execution p95 | 16.8 | 34.6 | 34.6 |
| Makespan | 220 ± 73 | 443 ± 45 | 629 ± 24 |
| Throughput (jobs/s) | 2.85 ± 0.75 | 4.09 ± 0.43 | 2.87 ± 0.10 |
| Consensus msgs/job | 56 ± 2 | 227 ± 4 | 279 ± 1 |
| Busiest coordinator share | 0.56 ± 0.09 | 0.25 ± 0.04 | 0.21 |
| Least-used coordinator share | 0.001 ± 0.003 | 0.007 ± 0.011 | 0.023 ± 0.003 |

## 4. What the numbers say

### 4.1 E0: the re-baseline holds, and the latency tail is queueing

PBFT keeps decision time well under a second at both scales (leaf p50 0.34 s at Hier-30, 0.15 s at
Hier-90; coordinator p50 0.23 s and 0.56 s). The burst job-latency tail is almost all waiting for a
free leaf: at Hier-90, wait p95 is 183 of the 206 s. At 3 jobs/s the same fleet has job p95 63.5 s,
wait p95 27 s and decision p50 0.11–0.12 s at both tiers. So the burst tail is a property of the
arrival design, not of consensus. **Quote E0 at 3 jobs/s for latency and at the burst for capacity.**
The queue forms where it does because only 17–27 of the 81 leaves ever run a job (§9.3). Every 2-
and 4-core leaf sits idle, so the ~4 jobs/s drain rate is a property of the cost model, not of the
fleet's size.

Hier-30 has a coordinator-balance quirk: coordinator 30 wins at most 3 of 600 jobs in every run (none in four of five), so one of the three
groups is nearly idle (busiest coordinator 56 %). The same pattern at
Hier-90 is analysed in §4.3.

### 4.2 C1, decision time: PBFT is faster at Hier-90, and the Snow gap is now honest

With the far-host fix in (33b2bdb4), Snow's coordinator tier decides in **~0.9–1.0 s**, against
45.5 s in the 9177551a campaign. Almost every vote finalizes in the minimum β = 6 rounds (6.1–6.3
mean) when the tier is not saturated, so ~0.9 s is Snow's floor at these settings: 6 rounds of
roughly 150 ms. Snow leaves sit at ~1.2 s for the same reason.

PBFT still decides faster at every tier and load measured here: 0.08–0.12 s at 3 jobs/s, against
0.91–0.95 s for Snow. **C1, the claim that Snow beats PBFT at scale, is not shown at Hier-90.** That
is now a clean statement rather than an artefact. It rests on Hier-270 / Mesh-180 and the E5
`peers` arm, as before.

Two Snow results are specific to the burst:

* **A 3-member Snow tier saturates under the burst.** In the LinUCB cells the Snow coordinator tier
  has only 2 peers, so the sample is the whole tier and α = 0.7 rounds up to both peers (α never
  falls below 2). Every coordinator proposes every job (proposers/job 3.0). At 20 jobs/s, per-coordinator
  finalize p50 was 6–31 s (coordinator 89: 5.5–6.5 s, 88: 17–19 s, 90: 29–32 s in runs 1–2), with
  6,000–11,000 failed rounds per coordinator per run, so coordinator selection p50 is 15–17 s. At 3 jobs/s
  the same tier decides in 0.95 s. No sends were dropped and inbound handling stayed in the
  milliseconds, so this is vote contention, not transport. It does not show in job latency
  (§4.3). **Open:** whether a larger tier or `max_inflight` changes it.
* **Message counts depend on batching.** Snow's consensus messages per job rise from 188 at the
  burst to 479 at 3 jobs/s, because a `SnowQueryBatch` carries many jobs when they arrive together and
  one when they trickle in. Bytes per job move less (145 → 164 KB). **Report bytes, or both, never
  message counts alone** when comparing engines across loads.

Cost per decision, Hier-90 analytic: PBFT 58 KB of consensus traffic per job, Snow 145 KB,
Hybrid 138 KB. With 9 coordinators each running its own Snow instance per job (proposers/job 8.8
against PBFT's 1.9), Snow pays ~2.5× PBFT's bytes. With 3 coordinators the gap narrows to 49 KB
against 31 KB.

### 4.3 New finding: under `self`, the coordinator tier concentrates load, and that drives latency

Who wins at the coordinator tier decides which group runs the job. Under the shipped
`coordinator_cost_matrix: self`, each coordinator advertises only itself:

* **Snow and Hybrid analytic: coordinators 82, 83, 85 and 90 never win a job, in all 20 runs, at
  both arrival rates.** Four of nine groups (36 of 81 leaves) never run anything. The busiest
  coordinator takes 34–38 % of all jobs. PBFT · analytic spreads over all nine: busiest 21–25 %,
  and coordinator 90 is the only one that ever gets zero (2 of 5 burst runs).
* **This, not decision time, explains most of Snow's latency penalty in the analytic block.** At 3
  jobs/s Snow decides ~0.8 s slower than PBFT, yet its job p95 is 56 s higher (119 against 63.5 s)
  and wait p95 92 against 27 s. Jain fairness drops to 0.08 from 0.14. Snow's makespan is ~145 s
  longer because the five used groups drain the backlog alone.
* **LinUCB block: every engine concentrates, differently.** PBFT sends 59–62 % of jobs to
  coordinator 90 (and only 7–9 % to 89, the far host on agent-92). Snow and Hybrid split ~50/40 between
  88 and 89 and give 90 10–13 % at the burst and **nothing at 3 jobs/s** (all 20 Poisson Snow and
  Hybrid LinUCB runs).
* **That is why the LinUCB burst rows converge.** Snow's slow coordinator tier (15 s) meters jobs into
  the leaves, and PBFT's fast tier piles 62 % onto one coordinator's 29 leaves. Both end near 250 s
  p95: Snow at 242–253 s with wait p95 120–149 s, PBFT at 255–263 s with wait p95 228 s.

The never-winning set is deterministic, and its cause is now established (§9). The cost a
coordinator advertises is a job's demand divided by its group's **total** capacity. The four losers
lead the four smallest groups and cost 1.8–2.1× the cheapest coordinator on every one of the
1,800 jobs. Snow's vote only lets a coordinator win when it is among the three cheapest. The load
term cannot close the gap. The same bias idles 54–68 of 81 leaves in every engine (§9.3).

### 4.4 C2, bandit design: shared and per-group tie on latency; per-group spreads a little more evenly

| Engine | Busiest group share, shared → per-group (burst) | Worst coordinator (burst) | Busiest group share (3 jobs/s) |
|---|---|---|---|
| PBFT | 38.4 % → 35.8 % | 44.5 % → 36.6 % | 41.5 % → 37.5 % |
| Snow | 38.6 % → 36.3 % | 42.7 % → 37.8 % | 36.9 % → 36.3 % |
| Hybrid | 38.0 % → 35.9 % | 41.0 % → 37.0 % | 38.4 % → 37.0 % |

Even is 33.3 %. With the load guard on (`load_slack 9`), neither design herds. The per-group model is
consistently a few points more even and has a tighter worst case, but job latency is inside the
interval in every engine at both loads. The guard narrowed the candidate set in 51–61 % of decisions at the burst and
28–36 % at 3 jobs/s. Inside one group's slack, the policy's choice barely matters
under this workload. **C2 needs E2's failure profile to separate the designs.** E1′ only shows that
neither one herds once guarded.

### 4.5 F6 input, context error: Snow's views are 3–5× more wrong

Mean in-flight jobs miscounted per candidate group at decision time (positive = over-counted). Every
decision was scored and none refused.

| | Analytic burst | Analytic 3 jobs/s | LinUCB burst | LinUCB 3 jobs/s |
|---|---|---|---|---|
| PBFT | 1.06 | 0.15 | 0.61–0.62 | 0.11 |
| Snow | 2.13 | 0.45 | 1.40–1.43 | 0.57–0.61 |
| Hybrid | 2.13 | 0.47 | 1.41–1.44 | 0.59–0.60 |

The error is entirely over-counting (signed mean equals mean): a coordinator still counts jobs that
have finished. Snow and Hybrid are 2× PBFT at the burst and 3–5× at steady load, a clean,
protocol-dependent spread on the F6 x-axis. Context *age* (p50 0.09–0.6 s) varies much less, as
expected. E2 provides the regret side.

### 4.6 F3, latency by measured RTT: distance matters only under the burst

At 3 jobs/s, selection time is flat across RTT bins for every engine. PBFT's coordinator tier reads
0.11–0.12 s in the LAN, regional, continental and transatlantic bins alike, and Snow's 0.89–0.96 s.
Only Snow's transatlantic leaves are slower, 1.57 s against 1.2 s. Under the burst, PBFT's
transatlantic leaves rise to 2.3 s p50 against 0.13 s regional. **An F3 figure from the 3 jobs/s
cells would say that WAN distance does not drive decision time at this load.** Bin populations
differ by engine because placement differs (§4.3), so compare within an engine.

### 4.7 PBFT internals: the certificate path carries real load

At the Hier-90 coordinator tier under the burst, 275 of 1,800 decisions per run (15 %) were finalized
through the commit-certificate path (ee9ed88e), and 541 COMMITs per run were held by the binding-commit
lock. At 3 jobs/s it was 89 and 296. In the LinUCB block it was 29 and ~190. Split resolution fired 0
to 8 times per run. These are the paths that used to strand jobs. They are exercised in every run, and
nothing was stranded.

## 5. What is citable, and the caveats to state

* All 95 runs at fc9eb220. Aggregate **by cell (run-directory prefix)**: `collect.py` groups by
  agents/engine/policy and would merge shared with per-group, and burst with Poisson.
* Simulated execution with the 120 s clamp; no failures injected; one agent per VM on the 17-site
  slice. No routing-quality (regret) claim comes from E1′.
* Analytic and LinUCB blocks differ in topology; never put them on one axis.
* Burst results measure a 5× overload; quote latency from the 3 jobs/s cells.
* Under `coordinator_cost_matrix: self`, Snow and Hybrid leave 4 of 9 groups idle; say so beside
  any analytic Snow latency.
* Message counts depend on batching; pair them with bytes.

## 6. Open questions this campaign raises

1. ~~Why the same four coordinators never win under Snow.~~ Answered in §9: capacity-normalised cost,
   a load term too weak to overturn it, and Snow's α rule. The same cause idles most small leaves in
   every engine.
2. Whether the E5 `peers` arm removes the concentration, and what it does to Snow's coordinator time.
3. The 3-member Snow tier under burst: is it α = 2-of-2, `max_inflight` 16, or both? Try one LinUCB
   Snow burst run with `max_inflight` 32, or with a fourth coordinator.
4. PBFT LinUCB's 60 % share for coordinator 90 and 8 % for 89 (far host): is that the proposal race
   favouring the fastest-to-quorum coordinator?

## 7. Change from the 9177551a campaign (not citable)

| Cell (burst) | Coordinator p50 | Job p95 | Consensus msgs/job |
|---|---|---|---|
| PBFT · analytic | 0.58 → 0.56 s | 214 → 206 s | 230 → 227 |
| Snow · analytic | 45.5 → 1.04 s | 637 → 378 s | 478 → 188 |
| Hybrid · analytic | 45.9 → 1.04 s | 511 → 323 s | 375 → 235 |
| Snow · LinUCB (shared) | 18.2 → 14.7 s | 252 → 242 s | 111 → 109 |

The Snow and Hybrid analytic rows moved because of 33b2bdb4: late decided-hints are honoured, and
Snow `send_workers` went from 16 to 64. Before that, coordinator 89 on agent-92 abandoned 80 votes per
run and the Snow vote waited on it.

## 8. Files and how to reproduce

In this folder:

| File | Contents |
|---|---|
| `runs_wide.csv` | `evaluation/collect.py` output, one row per run (95) |
| `supp.csv` | `supp.py` output: per-tier consensus counters, certificate path, coordinator and group shares, message mix by type, realised arrival rate |
| `runs_merged.csv` | the two joined, with `cell` and `run` |
| `cells_agg.csv` | per cell: mean, 95 % CI half-width, min, max, n for ~90 metrics |
| `tables.md` | the tables in §3, generated from `cells_agg.csv` by `tables.py` |
| `agg.py`, `supp.py`, `tables.py` | the scripts |

On the database node, `/root/collect-fc9eb220/` also holds `decisions.csv` (165,000 delegation
decisions with `ctx_view`) and `job_rtt.csv` (per-job RTT attribution). Both are too large to keep here.

```bash
# on the database node, as root
cd /root/SwarmAgents
python3.11 evaluation/collect.py --root runs/campaign-e0-e1-fc9eb220 --out /root/collect-fc9eb220 --label revision=fc9eb220
python3.11 /root/collect-fc9eb220/supp.py runs/campaign-e0-e1-fc9eb220 > /root/collect-fc9eb220/supp.csv
# locally, with the CSVs copied here
.venv/bin/python docs/results/e0-e1-fc9eb220/agg.py docs/results/e0-e1-fc9eb220
.venv/bin/python docs/results/e0-e1-fc9eb220/tables.py docs/results/e0-e1-fc9eb220 > docs/results/e0-e1-fc9eb220/tables.md
```

## 9. Why Snow's coordinators 82, 83, 85 and 90 never win (investigated 2026-10-16)

**Short answer.** It is not a Snow defect. The analytic cost model prices a job by the fraction of
the candidate's **total** capacity it would use, with a squared bottleneck term. Those four
coordinators lead the four smallest groups, so they are 1.8–2.1× more expensive than the cheapest
coordinator for every job. The only term that could reorder candidates, the projected-load factor,
cannot grow large enough. Snow's vote then turns that fixed ordering into a fixed winner set. PBFT's
coordinator tier escapes only because its winner is whoever reaches quorum first. At the leaf tier,
where both engines follow the cost, the same bias leaves two thirds of the fleet idle under PBFT too.

### 9.1 The chain, step by step (code at fc9eb220)

1. **What a coordinator advertises.** Under `coordinator_cost_matrix: self` a coordinator scores only
   itself, using the `AgentInfo` built in `_generate_agent_info`: its children's capacities and
   allocations summed, and load = `resource_usage_score(total_allocations, total_capacities)`.
2. **What a cost is.** `compute_job_cost` = (weighted demand/capacity ratios + max ratio²) × time,
   connectivity and other penalties. Bigger total capacity means a strictly lower cost. A Snow
   peer's answer, `wire_cost_for_job`, multiplies that by `_projected_load_factor` =
   1 + ((load + proposed_load)/100)^1.5.
3. **How Snow votes.** In `_answer_query` a peer votes for itself if its own cost is lower than the
   initiator's, and otherwise for the initiator. Every coordinator initiates for every job
   (proposers/job 8.8). With 8 peers sampled and α = ⌈0.7 × 8⌉ = 6, an initiator's round passes only
   if at most 2 peers are cheaper than it. So **only the three cheapest coordinators for a job can
   ever finalize it.** The cheapest one's instance always passes, because every peer defers to it.
4. **The load term is too weak.** Feasibility (`is_job_feasible`) checks only total capacity. There
   is no free-capacity or load threshold, despite its docstring. So a saturated large group stays
   both eligible and cheapest. The load factor is the only counterweight, and parity needs
   implausible loads (§9.2).

### 9.2 Measured with the code's own cost function

`coordcost.py` prices all 1,800 jobs of `/root/workloads/jobs_1800` on each coordinator, using
`ResourceAgent.compute_job_cost` itself on the aggregate a coordinator advertises. It uses the run's
`agent_profiles.json`. Group membership comes from regenerating the configs with the run's own
flags, which gives groups of 9 contiguous ids. Costs are at zero load.

| Coordinator | Group leaves | Cores | RAM GB | GPUs | Median cost / cheapest | Cheapest for | In cheapest 3 for | Jobs won (Snow, 3 jobs/s) |
|---|---|---|---|---|---|---|---|---|
| 82 | 1–9 | 44 | 176 | 8 | 2.13 | 0 | 0 | 0 |
| 83 | 10–18 | 52 | 208 | 16 | 1.80 | 0 | 0 | 0 |
| 84 | 19–27 | 92 | 368 | 20 | 1.00 | 1,800 | 1,800 | 686 |
| 85 | 28–36 | 52 | 208 | 8 | 1.80 | 0 | 0 | 0 |
| 86 | 37–45 | 66 | 264 | 12 | 1.41 | 0 | 0 | 40 |
| 87 | 46–54 | 78 | 312 | 16 | 1.18 | 0 | 1,800 | 536 |
| 88 | 55–63 | 72 | 288 | 12 | 1.28 | 0 | 1,795 | 448 |
| 89 | 64–72 | 68 | 272 | 20 | 1.37 | 0 | 5 | 90 |
| 90 | 73–81 | 52 | 208 | 8 | 1.79 | 0 | 0 | 0 |

The four never-winners are exactly the four coordinators that are never in the cheapest three, and
the order of wins among the rest follows cost. 86 and 89 win only when load lifts the cheaper groups
above them.

**The load term never gets close.** Take coordinator 90, the cheapest never-winner at 1.79×. It enters
the cheapest three only when at least three of the five cheaper groups are priced above it while it
sits idle. That needs load factors of 1.27 for 86, 1.31 for 89, 1.40 for 88, 1.52 for 87 and 1.79
for 84, which is a projected load of 42–85 %. `groupload.py` rebuilds each group's load over time from
its leaves' load traces, the way a coordinator aggregates it:

| Group load (run01) | 84 | 86 | 87 | 88 | 89 | 82, 83, 85, 90 |
|---|---|---|---|---|---|---|
| Max, 3 jobs/s | 34 % (×1.20) | 7 % (×1.02) | 36 % (×1.22) | 24 % (×1.12) | 13 % (×1.05) | 0 |
| Max, burst | 45 % (×1.31) | 30 % (×1.16) | 47 % (×1.32) | 36 % (×1.22) | 35 % (×1.21) | 0 |

The highest factor any group reached is 1.32, so the ordering never inverts. Load stays low for two
reasons. First, a coordinator aggregates only its children's `capacity_allocations`, which are the
jobs each leaf has *scheduled*. Jobs a leaf has won but not yet scheduled sit in its selected queue,
which only that leaf's own `proposed_load` counts, and the coordinator never reads it. So a group whose
three busy leaves hold deep backlogs still advertises 15–45 %. Second, inside each group the same bias
puts all the work on its two or three largest leaves (§9.3), so most of the group's capacity, and
therefore most of the load denominator, is idle.

**LinUCB block, same rule with a smaller gap.** Coordinators 88/89/90 lead groups 0–2, 3–5 and 6–8:
leaves 1–30, 31–60 and 61–87 (`generate_configs.py` sizes the last groups at 9, not 10). Their costs
relative to the cheapest are 1.13, 1.00 and **1.32**. Coordinator 90 has 3 fewer leaves, 180 cores
against 210 and 236. With only two peers, α needs both. So 90 wins only when 88's load factor exceeds
1.17 and 89's exceeds 1.32, which takes projected loads of about 30 % and 46 % at once. That happens
under the burst, where 90 wins 10–13 %, and never at 3 jobs/s, where it wins 0 %.

### 9.3 The same bias at the leaf tier, in every engine

`leafuse.py` counts leaves that executed no job, from each leaf's own `executed_jobs`. It agrees with
the collector's `active_leaders`; `executing_agents` is the fleet size despite its name.

| Cell | Leaves that ran nothing (of 81 / 87) | Idle by size (run01) |
|---|---|---|
| PBFT · analytic, burst | 54–56 | all 30 two-core, 16 of 21 four-core, 6 of 10 eight-core, 2 of 18 sixteen-core |
| PBFT · analytic, 3 jobs/s | 63–64 | all 2- and 4-core, all 8-core, 2 sixteen-core |
| Snow · analytic, 3 jobs/s | 68 in every run | all 2-, 4- and 8-core, 7 sixteen-core |
| Hybrid · analytic, 3 jobs/s | 68 in every run | same as Snow |
| PBFT / Snow LinUCB, 3 jobs/s | 69–74 | all 2- and 4-core, most 8-core, 6–10 sixteen-core |

`leafcost.py` shows why. At zero load a 2-core leaf prices the median job at **29×** a 32-core leaf,
a 4-core leaf at 11×, an 8-core at 4.7× and a 16-core at 2.1×. For the small leaf to win, the large
one's projected load must reach about 920 %, 460 %, 240 % and 110 % respectively. Inside group 84 at
3 jobs/s, only the 16-, 16- and 32-core leaves ran anything (110, 139 and 460 jobs). The other six,
with 2–8 cores, ran none, and their load trace is 0 for the whole run.

Leaf feasibility is also checked against total, not free, capacity. So the 32-core leaf keeps
winning while it queues work, which explains the wait-dominated latency tails throughout this
campaign. The leaf tier is PBFT in the PBFT and Hybrid cells, and they idle the same small leaves.
So **this is a property of the cost model shared by both engines**. Snow just follows it at the
coordinator tier too.

### 9.4 Why PBFT's coordinator tier spreads anyway

A PBFT coordinator drops its own proposal only when it has *seen* a cheaper one
(`has_better_proposal`), and the first proposal to gather a quorum of prepares and commits wins.
Placement there is a race, not a cost ranking. The evidence is that in the LinUCB block PBFT gives
59–62 % of jobs to coordinator 90, **the most expensive of the three**, and 7–9 % to 89, the cheapest
but on the far host agent-92. So PBFT's better coordinator spread at Hier-90 is a side effect of
timing, not of better pricing.

### 9.5 What this means for the results

* The Snow-versus-PBFT latency gap in the analytic block is mostly placement. Snow faithfully executes
  a cost model that concentrates load, and PBFT's race partly undoes it at the coordinator tier.
  Report the gap with this attribution, not as a protocol property.
* Every cell's absolute latency, wait and throughput is measured with two thirds of the leaves idle.
  Comparisons between cells share the cost model and stay fair. Absolute capacity claims, such as
  "Hier-90 drains ~4 jobs/s", describe this cost model, not the fleet.
* The E5 `peers` arm does **not** fix this: coordinators would score each other with the same
  capacity-normalised cost. A fix belongs in the cost model, or in feasibility against free capacity.
  Any such change moves every number, so it must be a new campaign and a decision, not a patch.
* **Fixed 2026-10-16** (decided by the user): `job_selection.cost_model: utilization`, now the default, prices
  committed work into the cost. It combines (b) and (d) below, plus job-by-job batch assignment. Feasibility is unchanged.
  Replaying this coordinator tier, each group's share of jobs matches its share of cores (84: 16 % / 16 %,
  82: 7.7 % / 7.6 %), where `capacity` sends all 300 jobs to 84 (`tests/test_cost_model.py`). Every
  number in this document was measured under `capacity`.
* Candidate fixes considered: (a) normalise demand by a per-slot or per-core unit rather than total
  capacity, so a free small agent is not 29× dearer; (b) count queued work, including backlog beyond
  capacity, in load, and weight the binding dimension rather than averaging in disk; (c) make
  feasibility require free capacity, or a backlog bound, so a saturated agent drops out; (d) have a
  coordinator aggregate its children's backlog (their `proposed_load`), not only their scheduled jobs.

Scripts: `coordcost.py` (coordinator costs; add `g3` for the LinUCB block), `leafcost.py` (leaf costs
by size), `leafuse.py` (idle leaves per cell), `groupload.py` (group load over time from leaf traces). Run them on the database node with `python3.11` from
`/root/SwarmAgents`, passing a run directory.
