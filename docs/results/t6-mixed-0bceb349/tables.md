## Run-level metrics, Hier-90 analytic, burst

| | PBFT mixed | Snow mixed | PBFT sim-only (c125ed8c) | Snow sim-only (c125ed8c) |
|---|---|---|---|---|
| runs | 3 | 3 | 5 | 5 |
| completed % | 100.0 | 100.0 | – | – |
| run twice | 0 | 0 | 0 | 0 |
| leaf decision p50 (s) | 0.09 ± 0.00 | 1.20 ± 0.00 | 0.09 ± 0.00 | 1.20 ± 0.00 |
| coordinator decision p50 (s) | 0.60 ± 0.03 | 1.11 ± 0.01 | 0.64 ± 0.03 | 1.23 ± 0.09 |
| coordinator decision p95 (s) | 2.34 ± 0.39 | 1.97 ± 0.23 | 2.41 ± 0.23 | 2.01 ± 0.20 |
| arrival→assigned p95 (s) | 15.2 ± 6.5 | 12.8 ± 0.5 | 9.3 ± 0.5 | 10.5 ± 1.2 |
| job latency p50 (s) | 16.8 ± 0.5 | 18.0 ± 0.2 | 16.2 ± 0.4 | 17.7 ± 0.3 |
| job latency p95 (s) | 61.7 ± 9.0 | 69.9 ± 1.8 | 54.0 ± 3.3 | 54.4 ± 3.0 |
| makespan (s) | 270 ± 13 | 304 ± 24 | 232 ± 31 | 224 ± 18 |
| throughput (jobs/s) | 7.00 ± 0.34 | 6.24 ± 0.50 | 7.87 ± 1.01 | 8.08 ± 0.66 |
| Jain, agents | 0.523 ± 0.007 | 0.527 ± 0.030 | 0.517 ± 0.011 | 0.542 ± 0.007 |
| consensus msgs / job | 341 ± 5 | 274 ± 5 | 348 ± 5 | 262 ± 3 |
| sends dropped | 0 | 0 | 0 | 0 |
| jobs restarted | 0 | 0 | 0 | 0 |
| duplicate-leader refusals (guard) | 0 | 144 ± 26 | 0 | 123 ± 25 |

## Real workflows inside the mixed cells (6 runs)

Execution time excludes staging on both sides; `adj` subtracts 0.17 s apptainer startup per containerised SWARM job (Pegasus runs kickstart inside the container).

| workflow | jobs exit 0 | Pegasus exec critical path (s) | SWARM exec critical path (s) | median exec ratio raw | adj | Pegasus end to end (s) | SWARM end to end (s) |
|---|---|---|---|---|---|---|---|
| cper-soilmoisture_run0002 | 132/132 | 38.608 | 40.4 ± 3.0 | 1.00 ± 0.08 | 0.98 ± 0.08 | 262 | 111 ± 43 |
| eht-m87_run0001 | 180/180 | 170.172 | 160.7 ± 15.2 | 0.97 ± 0.02 | 0.96 ± 0.02 | 401 | 195 ± 25 |
| tnseq_workflow_run0001 | 210/210 | 7.563 | 10.5 ± 1.1 | 1.66 ± 0.13 | 1.19 ± 0.10 | 357 | 62 ± 21 |

## tnseq standalone pilot, Mesh-30 Snow (2 runs)

Execution time excludes staging on both sides; `adj` subtracts 0.17 s apptainer startup per containerised SWARM job (Pegasus runs kickstart inside the container).

| workflow | jobs exit 0 | Pegasus exec critical path (s) | SWARM exec critical path (s) | median exec ratio raw | adj | Pegasus end to end (s) | SWARM end to end (s) |
|---|---|---|---|---|---|---|---|
| tnseq_workflow_run0001 | 70/70 | 7.563 | 10.4 ± 0.1 | 1.57 ± 0.03 | 1.14 ± 0.07 | 357 | 63 ± 2 |
