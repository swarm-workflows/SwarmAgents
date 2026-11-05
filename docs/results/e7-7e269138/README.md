# E7 at Hier-90 — external baselines beside SWARM (baselines at 7e269138, SWARM at c125ed8c)

Conference plan §4.2 E7. Four external baselines — centralized **greedy**, **round-robin**,
**random**, and the decentralized **Sparrow-style** sampler — run 5 times each at both E1′ load
levels, beside the E0/E1′ Hier-90 **analytic** cells of all three engines (PBFT, Snow, Hybrid) of the
c125ed8c campaign.

- Campaign: `campaigns/e7-hier90-7e269138.yml` (b2b5bef1), out `runs/campaign-e7-7e269138` on the
  database node; 3–4 Nov 2026, 20:17–04:07 UTC. **40/40 `ok`, all first attempt, no `.attempt` dirs.**
- SWARM cells: `runs/campaign-e0-e1-c125ed8c/e0-hier90-pbft-analytic{,-poisson3}` and
  `e1-hier90-{snow,hybrid}-analytic{,-poisson3}` (5 runs each, 30 in all), re-collected here with the
  same collector as the baselines.
- Collector: `evaluation/collect.py` at b2b5bef1, one invocation per group, on the database node in
  `/root/collect-e7-7e269138/{e7,swarm-burst,swarm-poisson3,swarm-{snow,hybrid}-{burst,poisson3}}`; aggregated by cell with `agg.py`.
- Files here: `agg.py`, `tables.md`, `cells_agg.csv` (mean/std/n per cell), `runs_wide_*.csv`
  (one row per run, every collector column).

## 1. What is held equal

Every baseline runs on the SWARM cell's level-0 fleet (81 workers; the 9 coordinators have no
counterpart), placed host-for-host from the same `agent_hosts.txt` (90 hosts, 17 sites, one agent
per VM), with the cell's archived `agent_profiles.json` (identical md5 in both cells), the same 1,800
jobs (`/root/workloads/jobs_1800`, replay-golden, simulated execution capped at 120 s), the same
base config (`campaigns/config_pbft.yml`: wall-time clamp, `executor_workers`, cost weights), and
the same arrivals. Snow and Hybrid ran on their own base configs (`config_swarm_multi.yml`,
`campaigns/config_hybrid.yml`), which differ from PBFT's only in `consensus.protocol` (Snow's also spells out two `mab` keys at their
defaults; `mab` is off in the analytic cells); the identical
execution times below confirm the execution model matched:

- **burst** — `--arrival batch`, 20 jobs per 1 s (the E0 / eScience saturation point);
- **poisson3** — Poisson 3 jobs/s, seed 42 (`arrivals.json` planned times equal the SWARM cell's;
  checked in the rehearsal).

The centralized schedulers run on `database` and dispatch to remote workers; Sparrow's K schedulers
run on agent hosts, but **its probes and claims go through Redis on `database`** — a star, not
peer links. State that beside the table.

Check that the execution model matched: execution time (`exec`, started→completed) is
**p50 10.43 s, p95 34.61 s in every one of the 70 runs**, so all latency differences below are
scheduling and queueing.

## 2. Validity

Every run, every arm: completion 100 % (1800/1800), **0 jobs executed twice** (from
`metrics.json`, all 81 workers), `drain.json` `all_terminal`, 0 in flight at stop (Sparrow's
`drain.json` has no `inflight_at_stop`; its 1800/1800 completions cover it), `metrics_complete`
true, fairness basis `level0` (81 agents).

## 3. Results (mean ± std over 5 runs)

`arrival→assigned` is the collector's `sched_latency`: from the job's arrival at the top tier to its
assignment to the agent that ran it — for SWARM, both consensus tiers plus any queueing ahead of
them; for the baselines, the scheduler's decision plus its dispatch. `assigned→started` is `wait`
(queueing on the worker). Job latency is arrival→completed. Jain is over all 81 workers, idle ones
counted as zeros.

### Burst (20 jobs/s)

| metric | SWARM PBFT | SWARM Snow | SWARM Hybrid | greedy | round-robin | random | Sparrow |
|---|---|---|---|---|---|---|---|
| arrival→assigned p50 (s) | 3.22 ± 0.07 | 5.12 ± 0.16 | 3.74 ± 0.22 | 0.26 ± 0.00 | 0.22 ± 0.02 | 0.24 ± 0.00 | 1.10 ± 0.05 |
| arrival→assigned p95 (s) | 9.28 ± 0.48 | 10.55 ± 1.21 | 8.98 ± 1.23 | 0.51 ± 0.00 | 0.49 ± 0.01 | 0.50 ± 0.00 | 27.01 ± 1.91 |
| assigned→started p95 (s) | 14.00 ± 0.97 | 10.50 ± 1.75 | 11.25 ± 0.97 | 19.31 ± 2.49 | 2.70 ± 0.78 | 6.68 ± 1.17 | 0.68 ± 0.00 |
| job latency p50 (s) | 16.2 ± 0.4 | 17.7 ± 0.3 | 16.2 ± 0.3 | 12.2 ± 0.4 | 10.8 ± 0.1 | 11.0 ± 0.1 | 15.5 ± 0.4 |
| job latency p95 (s) | 54.0 ± 3.3 | 54.4 ± 3.0 | 51.5 ± 4.3 | 60.9 ± 1.8 | 35.3 ± 0.3 | 37.3 ± 0.2 | 50.7 ± 1.1 |
| job latency p99 (s) | 125.2 ± 1.0 | 126.1 ± 0.4 | 125.0 ± 0.7 | 120.8 ± 0.1 | 120.4 ± 0.0 | 120.4 ± 0.0 | 122.0 ± 0.6 |
| makespan (s) | 232 ± 31 | 224 ± 18 | 209 ± 14 | 221 ± 22 | 191 ± 1 | 195 ± 4 | 219 ± 10 |
| throughput (jobs/s) | 7.87 ± 1.01 | 8.08 ± 0.66 | 8.65 ± 0.55 | 8.23 ± 0.84 | 9.42 ± 0.03 | 9.25 ± 0.19 | 8.24 ± 0.37 |
| Jain (fleet) | 0.517 ± 0.011 | 0.542 ± 0.007 | 0.518 ± 0.021 | 0.229 ± 0.011 | 0.848 ± 0.005 | 0.811 ± 0.013 | 0.849 ± 0.009 |
| workers that ran a job | 81.0 ± 0.0 | 81.0 ± 0.0 | 81.0 ± 0.0 | 78.0 ± 2.5 | 81.0 ± 0.0 | 81.0 ± 0.0 | 81.0 ± 0.0 |

### Poisson 3 jobs/s

| metric | SWARM PBFT | SWARM Snow | SWARM Hybrid | greedy | round-robin | random | Sparrow |
|---|---|---|---|---|---|---|---|
| arrival→assigned p50 (s) | 1.04 ± 0.01 | 3.19 ± 0.02 | 2.06 ± 0.02 | 0.26 ± 0.00 | 0.26 ± 0.00 | 0.26 ± 0.00 | 0.54 ± 0.01 |
| arrival→assigned p95 (s) | 3.12 ± 0.09 | 5.11 ± 0.04 | 4.38 ± 0.09 | 0.49 ± 0.00 | 0.48 ± 0.00 | 0.48 ± 0.00 | 1.50 ± 0.02 |
| assigned→started p95 (s) | 0.68 ± 0.00 | 0.68 ± 0.00 | 0.68 ± 0.00 | 47.95 ± 1.81 | 0.51 ± 0.00 | 0.51 ± 0.00 | 0.35 ± 0.05 |
| job latency p50 (s) | 11.9 ± 0.1 | 13.9 ± 0.0 | 12.9 ± 0.0 | 15.6 ± 0.1 | 10.7 ± 0.0 | 10.7 ± 0.0 | 11.0 ± 0.0 |
| job latency p95 (s) | 36.5 ± 0.2 | 38.8 ± 0.2 | 37.7 ± 0.2 | 77.8 ± 1.8 | 34.7 ± 0.1 | 34.8 ± 0.2 | 35.7 ± 0.2 |
| job latency p99 (s) | 121.4 ± 0.2 | 123.6 ± 0.2 | 122.4 ± 0.2 | 120.5 ± 0.0 | 120.3 ± 0.1 | 120.4 ± 0.1 | 120.8 ± 0.1 |
| makespan (s) | 621 ± 1 | 623 ± 1 | 621 ± 1 | 642 ± 1 | 619 ± 0 | 619 ± 0 | 619 ± 0 |
| throughput (jobs/s) | 2.90 ± 0.00 | 2.89 ± 0.00 | 2.90 ± 0.00 | 2.80 ± 0.01 | 2.91 ± 0.00 | 2.91 ± 0.00 | 2.91 ± 0.00 |
| Jain (fleet) | 0.286 ± 0.013 | 0.324 ± 0.004 | 0.312 ± 0.010 | 0.064 ± 0.001 | 0.996 ± 0.001 | 0.957 ± 0.001 | 0.768 ± 0.005 |
| workers that ran a job | 50.2 ± 0.8 | 51.0 ± 0.0 | 50.6 ± 0.9 | 15.2 ± 0.4 | 81.0 ± 0.0 | 81.0 ± 0.0 | 81.0 ± 0.0 |

The full rows (with `assigned→started p50` and `completed %`/`run twice`) are in `tables.md`.

## 4. Reading

1. **Round-robin and random are the strongest baselines on this workload, and that is a property
   of the workload** (inferred, not tested here). Every arm completed every job, so no worker
   was infeasible for any job it was given, and execution time does not depend on the worker
   (§1, simulated execution replays `wall_time`). Under those conditions, even spreading over 81
   workers is close to optimal. Say this beside the table: a heterogeneous or data-bound workload
   is where cost-aware placement should pay, and E7 at Hier-90 does not test it.
2. **Steady load (3 jobs/s): every SWARM engine is within 4 s of the best baseline at p95**
   (PBFT 36.5, Hybrid 37.7, Snow 38.8 vs round-robin 34.7 s), and the gap is decision time:
   arrival→assigned p95 PBFT 3.1, Hybrid 4.4, Snow 5.1 s vs 0.5 s centralized and 1.5 s Sparrow.
   The engines order by decision time and nothing else differs (worker queueing p95 0.68 s in all
   three). Makespan and throughput are arrival-bound for every arm except greedy.
3. **Burst: SWARM's p95 is 51–54 s, 16–19 s behind round-robin, level with Sparrow (51 s) and
   ahead of greedy (61 s).** Hybrid is lowest (51.5 s), PBFT and Snow tie (54.0, 54.4 s); the
   spreads (3–4 s) overlap. SWARM pays twice: decision p95 9–11 s (two consensus tiers under a
   backlog) and worker queueing p95 11–14 s. Sparrow's cost is the opposite shape — 27 s at p95
   waiting to be claimed, then almost no queueing, because a worker claims only when a slot is
   free (late binding).
4. **Greedy is the worst baseline at both loads**: it packs work onto the cheapest workers
   (15 of 81 at 3 jobs/s, Jain 0.06) and pays it in worker queueing (wait p95 48 s; job p95
   78 s, about twice any SWARM engine's). It is the baseline that shares SWARM's objective, and
   every engine beats it.
5. **Every SWARM engine leaves about 30 of 81 workers idle at 3 jobs/s** (50–51 ran a job; Jain
   0.29–0.32) yet queues little (wait p95 0.68 s). The same count under three engines places it in
   the shared cost model, not the consensus protocol. Why the same subset wins is not traced; a
   likely cause is that idle agents price alike under the utilization cost model, so ties break
   the same way each time. It costs nothing in latency here; it matters for any fairness claim, so
   report Jain beside it.
6. **p99 is about 120 s in every arm** — the simulated-execution cap (120 s, which truncates 7.5 %
   of the base set). p99 measures the cap, not scheduling; quote p50/p95.

## 5. Limits — read before quoting

- **Two code revisions.** The baselines ran at 7e269138; the SWARM cells at c125ed8c, which
  predates the PBFT vote-on-adoption fix (b9e51bbf). The c125ed8c cells had 0 restarts, so the fix
  is unlikely to move them, but the paper rule is one substrate, **one revision**: the paper table
  comes from the frozen-tag re-run of both arms.
- **Analytic only.** The LinUCB cells run at G=3, a different topology from the baselines'
  comparison point, and are left out; compare the baselines with the analytic column.
- **Not covered:** the Hier-270 scale point (needs a SWARM Hier-270 cell to pair with) and
  behavior under failure injection (the baselines have no kill/rejoin path; `group_outage.py`
  matches `main.py` agents only).
- Makespan is for simulated execution with the 120 s cap; state the cap and the truncated
  fraction wherever makespan is reported.
