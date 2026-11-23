# T-6 mixed Hier-90 at isc27-freeze 0bceb349: three real workflows inside the simulated load

This is the conference plan's §4.2 E1′ mixed rung (T-6, plan §10). The workload is the Hier-90
analytic burst cell's 1,800 simulated jobs with three Pegasus workflows merged in and run for
real.

| workflow | jobs | ids |
|---|---|---|
| cper-soilmoisture | 22 | `job_1801`–`1822` |
| EHT M87 | 30 | `job_1823`–`1852` |
| tnseq | 35 | `job_1853`–`1887` |

The three workflows have 198 file edges between them; the set is
`/export/swarm-wf/mixed_1800_wf`.

- **Campaign:** `campaigns/t6-mixed-hier90.yml`, PBFT and Snow, 3 repeats each. Output is in
  `runs/campaign-t6-mixed-hier90` on the database node. It ran on 11 Nov 2026, 22:41–23:43 UTC.
  **All 6 runs were `ok` on the first attempt**, each taking 514–593 s.
- **Code:** the `isc27-freeze` tag, 0bceb349, on every agent.
- **Configs:** `campaigns/config_mixed_{pbft,snow}.yml`. These run in real execution mode, with
  staging on, the bundle on the NFS export and images on each host's local disk.
- **Collection:**
  - `evaluation/collect.py --expected-jobs 1887` wrote `/root/collect-t6-mixed` and
    `runs_wide.csv` here.
  - `evaluation/t6_compare.py --container-startup-s 0.17` wrote `t6_per_job.csv` and
    `t6_per_workflow.csv`, and the `tnpilot_*` files from the two standalone tnseq pilot runs.
  - `agg.py` writes `tables.md` and `cells_agg.csv`.

## 1. Validity: every run is clean

Every one of the 6 runs:
- drained, with 0 jobs pending, in flight or undecided at the stop;
- completed all 1,887 jobs, with 0 run twice and 0 restarted;
- dropped 0 sends;
- had metrics from all 90 agents and 0 barrier-short agents.

**The real jobs:** all 87 exited 0 in every run (522 of 522), and none was run twice.

**Snow's `duplicate_leader_refusals`** (119–171 per run) count the 2026-10-07 guard refusing a
second leader call. That is the same range as every earlier Snow Hier-90 cell (89–233 at
c125ed8c), and no job ran twice.

**Known log noise:** each simulated job logs a `[STAGE] … declared output … does not exist`
ERROR, about 1,900 per run. Simulated jobs declare their replayed outputs but never write them,
so these lines change no result. Real and simulated file names are disjoint. This is left as it
is under the freeze.

## 2. Results

These are from `tables.md`, mean ± std.

| Hier-90 analytic, burst | PBFT mixed | Snow mixed | PBFT sim-only | Snow sim-only |
|---|---|---|---|---|
| coordinator decision p50 (s) | 0.60 | 1.11 | 0.64 | 1.23 |
| arrival→assigned p95 (s) | 15.2 ± 6.5 | 12.8 ± 0.5 | 9.3 | 10.5 |
| job latency p95 (s) | 61.7 ± 9.0 | 69.9 ± 1.8 | 54.0 | 54.4 |
| makespan (s) | 270 ± 13 | 304 ± 24 | 232 | 224 |
| consensus msgs / job | 341 | 274 | 348 | 262 |

The sim-only columns are the 1,800-job cells at c125ed8c, an older revision. They show scale
only; the frozen re-run supplies same-revision references. **The consensus tiers behave as they
do without the real jobs:** decision times and messages per job are unchanged. The longer tail
and makespan come from the real jobs themselves. They arrive last (ids 1801–1887), their
execution is uncapped (EHT SMILI runs for about 75 s), and each one moves data. Don't read the
gap as a consensus cost.

**Real workflows against Pegasus.** Execution time leaves staging out on both sides. See
`../../WORKFLOW_EXECUTION.md` §3.1 for the method and the container-startup asymmetry.

| workflow | exec critical path, Pegasus → SWARM (s) | median per-job exec ratio, adjusted | end to end, Pegasus → SWARM (s) |
|---|---|---|---|
| cper-soilmoisture | 38.6 → 40.4 ± 3.0 | 0.98 | 262 → 111 ± 43 |
| EHT M87 | 170.2 → 160.7 ± 15.2 | 0.96 | 401 → 195 ± 25 |
| tnseq | 7.6 → 10.5 ± 1.1 | 1.19 (raw 1.66) | 357 → 62 ± 21 |
| tnseq standalone pilot | 7.6 → 10.4 | 1.14 | 357 → 63 |

- **Execution matches** within a few percent for cper and EHT. tnseq's jobs take 0.3–1 s, so
  container startup and timer granularity dominate the ratio even after the adjustment.
- **End to end is not like for like.** Pegasus's figure includes HTCondor queueing (5–11 s per
  job) and its separate transfer, create-dir and cleanup jobs. SWARM's in the mixed cells
  includes queueing behind 1,800 simulated jobs. Quote the standalone pilot for
  workflow-to-workflow end to end, beside its breakdown in `tnpilot_t6_per_workflow.csv`.
- **Output parity** (byte and pixel comparisons) was established on the pilots, not on these
  runs: see `../../WORKFLOW_EXECUTION.md` §3.1.
