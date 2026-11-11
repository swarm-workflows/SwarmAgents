## E1′ flat mesh (burst, 20 jobs per agent)

### Mesh-90

| metric | PBFT | Snow |
|---|---|---|
| runs | 5 | 5 |
| completed % | 100.0 ± 0.0 | 100.0 ± 0.0 |
| run twice | 0 ± 0 | 0 ± 0 |
| arrival→assigned p50 (s) | 233.9 ± 0.8 | 96.4 ± 0.8 |
| arrival→assigned p95 (s) | 500.8 ± 3.8 | 224.3 ± 1.8 |
| job latency p50 (s) | 250.0 ± 2.5 | 115.5 ± 1.6 |
| job latency p95 (s) | 515.8 ± 3.6 | 241.5 ± 2.0 |
| makespan (s) | 657 ± 37 | 393 ± 10 |
| throughput (jobs/s) | 2.75 ± 0.16 | 4.59 ± 0.12 |
| Jain, agents | 0.334 ± 0.002 | 0.440 ± 0.002 |
| consensus msgs / job | 14261 ± 104 | 168 ± 4 |
| sends dropped | 1813135 ± 22882 | 0 ± 0 |
| jobs restarted | 0.2 ± 0.4 | 0.0 ± 0.0 |
| restarts | 0 ± 0 | 0 ± 0 |

### Mesh-120

| metric | PBFT | Snow |
|---|---|---|
| runs | 5 | 5 |
| completed % | 100.0 ± 0.0 | 100.0 ± 0.0 |
| run twice | 0 ± 0 | 0 ± 0 |
| arrival→assigned p50 (s) | 605.1 ± 5.6 | 137.1 ± 3.0 |
| arrival→assigned p95 (s) | 1212.3 ± 11.4 | 343.3 ± 5.1 |
| job latency p50 (s) | 621.5 ± 6.1 | 162.4 ± 7.7 |
| job latency p95 (s) | 1226.0 ± 14.5 | 361.9 ± 4.3 |
| makespan (s) | 1446 ± 23 | 544 ± 12 |
| throughput (jobs/s) | 1.66 ± 0.03 | 4.42 ± 0.10 |
| Jain, agents | 0.277 ± 0.004 | 0.341 ± 0.005 |
| consensus msgs / job | 25776 ± 134 | 178 ± 5 |
| sends dropped | 4746221 ± 85258 | 0 ± 0 |
| jobs restarted | 7.2 ± 2.8 | 0.0 ± 0.0 |
| restarts | 806 ± 318 | 0 ± 0 |

### Mesh-180

| metric | PBFT | Snow |
|---|---|---|
| runs | 5 | 5 |
| completed % | 84.2 ± 0.7 | 100.0 ± 0.0 |
| run twice | 0 ± 0 | 0 ± 0 |
| arrival→assigned p50 (s) | 1698.1 ± 14.4 | 223.0 ± 2.1 |
| arrival→assigned p95 (s) | 3366.1 ± 6.9 | 522.9 ± 8.8 |
| job latency p50 (s) | 1715.8 ± 14.0 | 238.2 ± 3.3 |
| job latency p95 (s) | 3377.3 ± 7.6 | 539.3 ± 7.2 |
| makespan (s) | 3652 ± 5 | 767 ± 11 |
| throughput (jobs/s) | 0.83 ± 0.01 | 4.70 ± 0.07 |
| Jain, agents | 0.192 ± 0.002 | 0.346 ± 0.003 |
| consensus msgs / job | 48269 ± 168 | 164 ± 3 |
| sends dropped | 18858265 ± 492647 | 0 ± 0 |
| jobs restarted | 124.4 ± 5.9 | 1.2 ± 0.8 |
| restarts | 22853 ± 1134 | 1 ± 1 |

## E5 ablations beside their E1′ references (older revisions; see README)

### Hier-90, PBFT (burst)

| metric | E1′ reference (c125ed8c) | gossip off | peers |
|---|---|---|---|
| runs | 5 | 3 | 3 |
| completed % | — | 100.0 ± 0.0 | 100.0 ± 0.0 |
| run twice | 0 ± 0 | 0 ± 0 | 0 ± 0 |
| leaf decision p50 (s) | 0.09 ± 0.00 | 0.09 ± 0.00 | 0.08 ± 0.00 |
| coordinator decision p50 (s) | 0.64 ± 0.03 | 0.65 ± 0.02 | 0.08 ± 0.00 |
| coordinator decision p95 (s) | 2.41 ± 0.23 | 2.47 ± 0.46 | 0.39 ± 0.03 |
| arrival→assigned p95 (s) | 9.28 ± 0.48 | 8.97 ± 0.80 | 49.41 ± 1.71 |
| job latency p50 (s) | 16.2 ± 0.4 | 15.5 ± 0.4 | 27.5 ± 0.8 |
| job latency p95 (s) | 54.0 ± 3.3 | 50.5 ± 4.7 | 81.4 ± 2.3 |
| makespan (s) | 232 ± 31 | 230 ± 42 | 232 ± 14 |
| throughput (jobs/s) | 7.87 ± 1.01 | 7.99 ± 1.34 | 7.77 ± 0.47 |
| Jain, leaves | 0.517 ± 0.011 | 0.532 ± 0.012 | 0.491 ± 0.004 |
| proposers / job, coordinator tier | 5.62 ± 0.26 | 5.37 ± 0.29 | 1.01 ± 0.01 |
| consensus msgs / job | 348 ± 5 | 345 ± 12 | 229 ± 2 |
| sends dropped | 0 ± 0 | 0 ± 0 | 0 ± 0 |
| jobs restarted | 0.0 ± 0.0 | 0.0 ± 0.0 | 0.0 ± 0.0 |
| restarts | 0 ± 0 | 0 ± 0 | 0 ± 0 |

### Hier-90, Snow (burst)

| metric | E1′ reference (c125ed8c) | gossip off | peers |
|---|---|---|---|
| runs | 5 | 3 | 3 |
| completed % | — | 100.0 ± 0.0 | 100.0 ± 0.0 |
| run twice | 0 ± 0 | 0 ± 0 | 0 ± 0 |
| leaf decision p50 (s) | 1.20 ± 0.00 | 1.20 ± 0.00 | 1.19 ± 0.00 |
| coordinator decision p50 (s) | 1.23 ± 0.09 | 1.22 ± 0.07 | 1.00 ± 0.02 |
| coordinator decision p95 (s) | 2.01 ± 0.20 | 1.99 ± 0.09 | 6.73 ± 1.27 |
| arrival→assigned p95 (s) | 10.55 ± 1.21 | 9.49 ± 0.77 | 191.25 ± 9.76 |
| job latency p50 (s) | 17.7 ± 0.3 | 17.3 ± 0.1 | 97.9 ± 0.9 |
| job latency p95 (s) | 54.4 ± 3.0 | 51.7 ± 0.8 | 210.4 ± 10.1 |
| makespan (s) | 224 ± 18 | 222 ± 11 | 363 ± 14 |
| throughput (jobs/s) | 8.08 ± 0.66 | 8.12 ± 0.39 | 4.96 ± 0.19 |
| Jain, leaves | 0.542 ± 0.007 | 0.545 ± 0.011 | 0.418 ± 0.008 |
| proposers / job, coordinator tier | 8.77 ± 0.03 | 8.73 ± 0.03 | 1.08 ± 0.01 |
| consensus msgs / job | 262 ± 3 | 262 ± 5 | 252 ± 2 |
| sends dropped | 0 ± 0 | 0 ± 0 | 0 ± 0 |
| jobs restarted | 0.0 ± 0.0 | 0.0 ± 0.0 | 0.0 ± 0.0 |
| restarts | 0 ± 0 | 0 ± 0 | 0 ± 0 |

### Hier-270, peers (burst)

| metric | PBFT E1′ reference (ed8c9101) | PBFT peers | Snow E1′ reference (ed8c9101) | Snow peers |
|---|---|---|---|---|
| runs | 5 | 3 | 5 | 3 |
| completed % | 100.0 ± 0.0 | 100.0 ± 0.0 | 100.0 ± 0.0 | 100.0 ± 0.0 |
| run twice | 0 ± 0 | 0 ± 0 | 0 ± 0 | 0 ± 0 |
| leaf decision p50 (s) | 0.08 ± 0.00 | 0.08 ± 0.00 | 0.92 ± 0.00 | 0.91 ± 0.00 |
| coordinator decision p50 (s) | 1.56 ± 0.03 | 0.29 ± 0.01 | 68.29 ± 5.11 | 1.08 ± 0.01 |
| coordinator decision p95 (s) | 3.51 ± 0.23 | 1.16 ± 0.16 | 135.26 ± 4.68 | 7.92 ± 0.47 |
| arrival→assigned p95 (s) | 10.61 ± 0.60 | 160.91 ± 14.03 | 137.96 ± 4.69 | 463.71 ± 7.56 |
| job latency p50 (s) | 15.9 ± 0.2 | 63.6 ± 2.5 | 89.0 ± 5.0 | 190.4 ± 3.0 |
| job latency p95 (s) | 60.1 ± 1.9 | 186.8 ± 13.8 | 158.8 ± 4.1 | 484.3 ± 5.4 |
| makespan (s) | 408 ± 20 | 498 ± 10 | 527 ± 6 | 764 ± 5 |
| throughput (jobs/s) | 13.25 ± 0.66 | 10.85 ± 0.21 | 10.25 ± 0.11 | 7.07 ± 0.04 |
| Jain, leaves | 0.377 ± 0.010 | 0.395 ± 0.014 | 0.361 ± 0.008 | 0.306 ± 0.002 |
| proposers / job, coordinator tier | 9.25 ± 0.18 | 1.04 ± 0.00 | 26.95 ± 0.02 | 1.07 ± 0.00 |
| consensus msgs / job | 1321 ± 8 | 1261 ± 4 | 1636 ± 7 | 380 ± 5 |
| sends dropped | 57391 ± 36523 | 0 ± 0 | 0 ± 0 | 0 ± 0 |
| jobs restarted | 0.0 ± 0.0 | 0.0 ± 0.0 | 0.0 ± 0.0 | 7.3 ± 1.2 |
| restarts | 0 ± 0 | 0 ± 0 | 0 ± 0 | 7 ± 1 |
