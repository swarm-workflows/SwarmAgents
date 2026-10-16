
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
