# E7 at Hier-270 — external baselines beside SWARM (code ed8c9101)

Conference plan §4.2 E7, the second scale point. Four external baselines — centralized **greedy**,
**round-robin**, **random**, and the decentralized **Sparrow-style** sampler — run 5 times each at
both E1′ Hier-270 load levels, beside that campaign's three **analytic** cells (PBFT, Snow, Hybrid).
The Hier-90 point is `../e7-7e269138/`.

- Campaign: `campaigns/e7-hier270-ed8c9101.yml` (4425207f, header fix 280bcb5a), out
  `runs/campaign-e7-hier270-ed8c9101` on the database node; 6–7 Nov 2026, 21:32–09:51 UTC.
  **40/40 `ok`, every one on the first attempt, no `.attempt` dirs.**
- SWARM cells: `runs/campaign-e1-hier270-ed8c9101/{e0-hier270-pbft,e1-hier270-snow,e1-hier270-hybrid}-analytic{,-poisson7}`
  (5 runs each), from that campaign's collection (`../e1-hier270-ed8c9101/runs_wide.csv`).
- Collector: `evaluation/collect.py`, one invocation per campaign (`/root/collect-e7-hier270-ed8c9101`
  on the database node); aggregated by cell with `agg.py` → `cells_agg.csv`, `tables.md`.

## 1. What is held equal

Every baseline runs on the SWARM cell's level-0 fleet: **243 workers**. The 27 coordinators have no
counterpart. Placement is host for host as `run_test.py` places agents (`agent_hosts.txt`, 90
hosts, 17 sites, **3 agents per VM**), with the cell's archived `agent_profiles.json` (identical md5
across the campaign's cells). The jobs are the same 5,400 (`/root/workloads/jobs_5400`, simulated
execution capped at 120 s), with the same base-config keys a baseline reads (wall-time clamp,
`executor_workers`, cost weights) and the same arrivals:

- **burst**: `--arrival batch`, 20 jobs per 1 s;
- **Poisson 7 jobs/s**: seed 42, the SWARM cells' arrival times.

Execution time is **p50 10.4 s, p95 51.71 s in all 70 runs**, so every latency difference below
is scheduling and queueing.

The centralized schedulers run on `database` and dispatch to remote workers. Sparrow's 27
schedulers run on agent hosts, but **its probes and claims go through Redis on `database`**: a
star, not peer links. State that beside the table.

## 2. Validity

Checked on every run, both arms, from `drain.json` and `metrics.json`:
- completion is 100 % (5,400 of 5,400), with **0 jobs executed twice**;
- every baseline run ended `all_terminal`, and all 243 workers reported;
- the SWARM cells' validity is in `../e1-hier270-ed8c9101/README.md` §2: all clean.

## 3. Results (mean ± std over 5 runs)

`arrival→assigned` is the collector's `sched_latency`. For SWARM it is both consensus tiers plus
any queueing ahead of them; for a baseline, its decision and dispatch. `assigned→started` is
queueing on the worker. Jain is over the 243 workers, idle ones counted as zeros. Every row is in
`tables.md`.

### Burst (20 jobs/s)

| | SWARM PBFT | SWARM Snow | SWARM Hybrid | greedy | round-robin | random | Sparrow |
|---|---|---|---|---|---|---|---|
| arrival→assigned p95 (s) | 10.6 | 138.0 | 133.4 | 0.54 | 0.49 | 0.52 | 2.09 |
| assigned→started p95 (s) | 1.56 | 0.71 | 0.79 | **46.4** | 0.51 | 0.51 | 0.51 |
| job latency p50 (s) | 15.9 ± 0.2 | 89.0 ± 5.0 | 88.1 ± 3.7 | 18.2 ± 0.1 | 10.7 ± 0.0 | 10.8 ± 0.0 | 11.1 ± 0.0 |
| job latency p95 (s) | 60.1 ± 1.9 | 158.8 ± 4.1 | 154.6 ± 12.8 | 79.4 ± 1.8 | 52.0 ± 0.2 | 52.1 ± 0.1 | 53.2 ± 0.3 |
| makespan (s) | 408 ± 20 | 527 ± 6 | 521 ± 14 | 421 ± 10 | 384 ± 0 | 384 ± 0 | 386 ± 3 |
| throughput (jobs/s) | 13.25 | 10.25 | 10.37 | 12.83 | 14.07 | 14.07 | 13.98 |
| workers that ran a job | 223 | 161 | 158 | 47 | 243 | 243 | 243 |

### Poisson 7 jobs/s

| | SWARM PBFT | SWARM Snow | SWARM Hybrid | greedy | round-robin | random | Sparrow |
|---|---|---|---|---|---|---|---|
| arrival→assigned p95 (s) | 2.82 | 4.50 | 3.91 | 0.51 | 0.49 | 0.50 | 1.27 |
| assigned→started p95 (s) | 0.68 | 0.68 | 0.68 | **54.8** | 0.51 | 0.51 | 0.44 |
| job latency p50 (s) | 11.6 ± 0.0 | 13.4 ± 0.0 | 12.6 ± 0.0 | 31.6 ± 0.3 | 10.7 ± 0.0 | 10.7 ± 0.0 | 11.0 ± 0.0 |
| job latency p95 (s) | 53.4 ± 0.2 | 54.6 ± 0.2 | 53.8 ± 0.3 | 88.6 ± 1.2 | 52.1 ± 0.1 | 52.1 ± 0.1 | 52.4 ± 0.0 |
| makespan (s) | **976 ± 103** | 868 ± 1 | 867 ± 1 | 896 ± 5 | 865 ± 1 | 865 ± 0 | 866 ± 2 |
| Jain (243 workers) | 0.272 | 0.287 | 0.271 | 0.051 | 0.997 | 0.953 | 0.764 |
| workers that ran a job | 141 | 141 | 136 | 27 | 243 | 243 | 243 |

## 4. Reading

1. **The Hier-90 picture holds at 270.** Round-robin and random are the strongest baselines on
   this workload, because every job fits every worker and execution time doesn't depend on the
   worker (inferred, as at Hier-90). Spreading blindly over 243 workers is close to optimal.
2. **At steady load all three SWARM engines are within 2.5 s of the best baseline at p95.** PBFT
   53.4, Hybrid 53.8 and Snow 54.6 s, against round-robin's 52.1 s. The gap is decision time:
   arrival→assigned p95 is 2.8–4.5 s, against 0.5 s centralized and 1.3 s for Sparrow.
3. **Under a burst:**
   - **PBFT trails round-robin by 8 s at p95** (60.1 against 52.0 s), the same order as at Hier-90.
   - **Snow and Hybrid trail by about 100 s.** Their 27-coordinator tier is the bottleneck: every
     coordinator runs its own Snow instance for every job, about 145,000 finalized instances a
     run (`../e1-hier270-ed8c9101/`, and the trace of 6 Nov).
   - **Sparrow is within 1.2 s of round-robin here.** At Hier-90 it was 15 s behind: its 27 s
     claim wait at a burst falls to 2.1 s with three times the workers to claim.
4. **Greedy is the worst baseline at both loads, and SWARM beats it in every cell except Snow and
   Hybrid under the burst.** Greedy packs work onto the cheapest workers (27 of 243 at 7 jobs/s,
   Jain 0.05) and pays in worker queueing: wait p95 46–55 s, job p95 79–89 s. It shares SWARM's
   objective.
5. **SWARM leaves about 100 of 243 workers idle at steady load on all three engines** (136–141
   ran a job, Jain 0.27–0.29). That is the same pattern as Hier-90 (50 of 81 used), so it comes
   from the shared cost model, not the consensus engine. It costs no latency here (wait p95
   0.68 s), but it matters for any fairness claim.
6. **PBFT's makespan tail (976 ± 103 s)** comes from 1–7 jobs per run that wait one 300 s
   reselection. Traced 6 Nov to vote stash overflow at the far host's three coordinators; the
   stash is now sized to the tier (9260a044) and is being piloted (`campaigns/pilot-stash-cap.yml`).
   No baseline has an equivalent tail.
7. **p99 is about 120 s for every arm except under burst congestion**: that's the
   simulated-execution cap. Quote p50 and p95.

## 5. Limits — read before quoting

- **One code revision for SWARM, but not the frozen one.** The SWARM cells ran at ed8c9101, after
  the 5 Nov PBFT fixes but before the stash fix (9260a044). The paper's table comes from the
  frozen re-run of both arms.
- **Analytic only.** The LinUCB cells have 9 coordinators at G=3 and a 261-worker fleet, not the
  243 the baselines ran on, so they're left out.
- **The PBFT arm is tuned:** counter-proposals off at 27 coordinators, and a 4× transport send
  pool. Report that beside the PBFT column.
- **Not covered:** behaviour under failure injection. The baselines have no kill-and-rejoin path.
- **Makespan is under simulated execution with the 120 s cap.** State the cap and the truncated
  fraction wherever makespan is reported.

## 6. Files

- `runs_wide_e7.csv`: one row per baseline run, every collector column. The SWARM rows are in
  `../e1-hier270-ed8c9101/runs_wide.csv`.
- `agg.py`, `cells_agg.csv` and `tables.md`: per-cell mean, std and n.
