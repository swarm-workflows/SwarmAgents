# Conference paper data: the frozen campaign (eval-freeze, 0bceb349) and the review ablations (d3755e2b)

Everything a number, table or figure in the conference paper (gossip consensus + contextual bandit
delegation) is derived from, at the level of one row per run. The per-job and per-decision tables and the
raw run trees are too large for this repository; see "What is not here".

## Layout

| Path | What it is | Produced by |
|---|---|---|
| `cells.csv` | One row per cell: mean, sd and n of every reported metric. **The paper's numbers.** | `evaluation/paper/build.py --manifest evaluation/paper/manifest_frozen.yml` |
| `tables.md` | The same, as the tables the paper text was written from | `build.py` |
| `validity.md` | Per-source validity: how runs ended, metrics completeness, clocks, jobs run twice | `build.py` |
| `fig/` | F1–F6 as `build.py` draws them (frozen campaign only) | `build.py` |
| `runs_wide/<source>.csv` | One row per run for each frozen source, the direct input of `build.py` | `evaluation/collect.py` on the database node (`evaluation/paper/collect_frozen.sh`) |
| `runs_wide/*.events_summary.csv`, `*.supp.csv`, `*.t6_per_workflow.csv` | Outage/partition events, Hier-270 tier detail, real-workflow comparison with Pegasus | `failure_summary.py`, `docs/results/e1-hier270-ed8c9101/supp.py`, `evaluation/t6_compare.py` |
| `e3a/*.per_run.csv` | RTT-bin decomposition per run (the WAN-distance figure) | `docs/results/e3a-c125ed8c/e3a.py` |
| `ablation/*.csv` | One row per run for the review ablations (below) | `evaluation/collect.py` |
| `fig-ablation/` | F1 and F2 with the ablation points overlaid, and the single-column F3. **These are the figures in the paper.** | `evaluation/paper/fig_ablation.py`, `fig_f3_compact.py` |

Rebuild the paper figures from this directory alone:

```bash
.venv/bin/python evaluation/paper/fig_ablation.py   docs/results/frozen-0bceb349 /tmp/out
.venv/bin/python evaluation/paper/fig_f3_compact.py docs/results/frozen-0bceb349 /tmp/out
```

Rebuilding `cells.csv` itself needs the full collection unpacked at `runs/collect-frozen` (see below).

## The frozen campaign

521 runs (441 scheduler runs, 80 baseline runs), campaign files in `campaigns/frozen-0bceb349/`, plus the
cells that count as frozen without a repeat (flat-mesh ladder and the regime/overlay ablation from
`campaigns/first-runs-21a2d9bb.yml`, the mixed real-workflow rung). The source list, with one withdrawn run
named explicitly, is `evaluation/paper/manifest_frozen.yml`.

## The review ablations (20–22 Nov 2026)

Config-only changes on tag `eval-freeze-15` (d3755e2b). No file under `swarm/`, `main.py`, `run_test.py` or
`config_swarm_multi.yml` differs from `eval-freeze`. 3 repeats per cell, 63 runs, all `ok`, every job
completed, none executed twice.

| File | Campaign | Cells |
|---|---|---|
| `ablation/capA_runs_wide.csv` | `campaigns/review-ablations-d3755e2b.yml` (revA) | `consensus.snow.max_inflight` 64 and 256 on Hier-270 analytic, Hier-90 peers, Hier-90 LinUCB G=3 (burst) |
| `ablation/control16_runs_wide.csv` | `campaigns/review-control-d3755e2b.yml` | Same-session control at the default 16, Hier-270 analytic burst |
| `ablation/poolB_runs_wide.csv` | `review-ablations` (revB) | PBFT Mesh-180 with `runtime.fanout_workers/fanout_queue` 256/8192 |
| `ablation/sweepC_runs_wide.csv` | `review-ablations` (revC) | Hier-270 analytic, PBFT and Snow (limit 16), Poisson 9/11/13/16 jobs/s |
| `ablation/sweepC_limit64_runs_wide.csv` | `campaigns/review-sweep64-d3755e2b.yml` | Snow with limit 64 at the same four rates |
| `ablation/sweepC_limit64_poisson7_runs_wide.csv` | `campaigns/review-sweep64p7-d3755e2b.yml` | Snow with limit 64 at the main campaign's 7 jobs/s |

Configs: `campaigns/config_rev_*.yml`, each derived from the frozen cell's config with one key changed
(stated in its first line).

Headline results (p95 scheduling latency, s):

| Hier-270 analytic | 7/s | 9/s | 11/s | 13/s | 16/s | burst 20/s |
|---|---|---|---|---|---|---|
| PBFT | 2.8 (frozen) | 3.4 | 4.1 | 5.2 | 8.3 ± 0.3 | 10.5 ± 0.8 (frozen) |
| Snow, limit 16 | 4.5 (frozen) | 4.9 | 5.2 | 5.8 | 58.8 ± 14.1 | 130.3 ± 3.8 (frozen), 139.6 ± 2.4 (control) |
| Snow, limit 64 | 4.5 | 4.8 | 5.1 | 5.3 | 5.6 ± 0.1 | 7.9 ± 0.1 |

- Mesh-180 PBFT with the larger pool: 100 % complete (was 84.2 %), 1.20 jobs/s (0.83), p95 2,749 s (3,366).
- The limit does not explain the peers regime (192 / 188 s against 191 s) nor the Hier-90 G=3 tail
  (93.5 / 91.6 s against 89.8 s). The G=3 tail is one coordinator, the one 74 ms from the store: it delegates
  2.2 jobs/s in every Snow and Hybrid run and wins 3.1–4.9 jobs/s in the burst (derived from
  `level1_jobs.csv` / `level0_jobs.csv` of the run trees, which are not in this directory).

## What is not here

- **Per-job and per-decision tables** (`decisions.csv`, `job_rtt.csv`, `runs_tidy.csv`, about 0.9 GB unpacked):
  `collect-frozen.tgz` in the evaluation-data repository, directory `gossip-bandit/`.
- **Raw run trees** (agent logs, per-level job CSVs, `run_meta.json`, `rtt_matrix.json` per run): on the
  database node under `/root/SwarmAgents/runs/frozen-0bceb349/` and `runs/campaign-review-*-d3755e2b/`.
