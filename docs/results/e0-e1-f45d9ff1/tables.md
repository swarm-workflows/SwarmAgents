
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
