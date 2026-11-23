# E1′ at Hier-270 — three engines × two decision planes × two loads (code ed8c9101)

Conference plan §4.2 E1′, the Hier-270 rung. {PBFT, Snow, Hybrid} × {analytic, LinUCB-shared}, each
at burst (20 jobs/s) and at Poisson 7 jobs/s, 5 repeats: 12 cells, 60 runs.

- Campaign: `campaigns/e1-hier270-ed8c9101.yml` (commit c396b101, code ed8c9101), out
  `runs/campaign-e1-hier270-ed8c9101` on the database node; 5–6 Nov 2026, 21:59–14:56 UTC.
  **60/60 `ok`, every one on the first attempt, no `.attempt` dirs.**
- Collected on the database node: `evaluation/collect.py` → `/root/collect-e1-hier270-ed8c9101`;
  `supp.py` (here) → `supp.csv`; `agg.py` (here) → `cells_agg.csv`, `tables.md`.
- Pilots behind the configuration: `runs/pilot-hier270-b9e51bbf`, `runs/pilot-counter-margin3`,
  `runs/pilot-hier270-linucb-ed8c9101`.

## 1. What was run

| | analytic (`--groups-per-coordinator 1`) | LinUCB-shared (`--groups-per-coordinator 3`, `--delegation-policy bandit`) |
|---|---|---|
| Tiers | 243 leaves in 27 groups of 9, **27 coordinators** | 261 leaves in 27 groups (9 of 9, 18 of 10), **9 coordinators** |
| PBFT config | `campaigns/config_pbft_hier270.yml`: counter-proposals **off**, fan-out pool 256/8192 | `campaigns/config_pbft_linucb_shared.yml`: counter-proposals **on**, pool 64/2048 |
| Snow config | `config_swarm_multi.yml` (shipped) | `campaigns/config_snow_linucb_shared.yml` |
| Hybrid config | `campaigns/config_hybrid.yml` | `campaigns/config_hybrid_linucb_shared.yml` |

- **The PBFT tuning, reported beside every PBFT number:** counter-proposals are set by the size of
  the coordinator tier (decided 2026-11-05). They're on at 3 and 9 coordinators and off at 27.
  - At 27 coordinators with them on, the pilots gave job p95 296–310 s, 2.6–5.3 k restarts and
    1.5 M dropped sends.
  - At 9 coordinators with them off (Hier-90 E0), p95 doubled and one coordinator never won a job.
  - The analytic PBFT arm also gets a 4× transport pool. Snow and Hybrid keep the shipped 64/2048:
    Snow dropped no sends at Hier-270.
- **The fleet:** 3 agents per VM on the 90 hosts of `agent_hosts.txt` (interleaved, 17 sites),
  `--master-fleet-size 270 --seed 42 --all-dtns`, workload `/root/workloads/jobs_5400` (replay-golden
  duplicated, simulated execution capped at 120 s).
- **Arrivals:** burst is `--arrival batch`, 20 per 1 s. Poisson is 7 jobs/s, seeded by `--seed 42`,
  so every engine and repeat sees identical arrival times. 7 jobs/s is about 70 % of the slower
  engine's measured Hier-270 burst drain rate (Snow 10.3 jobs/s), the rule Hier-90's 3 jobs/s
  followed. Realised: 6.998 jobs/s, worst lag behind plan ≤ 6 ms; burst 19.83 jobs/s.

## 2. Validity: every run is clean

All 60 runs, checked from `drain.json` and `metrics.json` rather than the campaign's label:
- every run ended `drained`, with 0 pending, 0 in flight and 0 undecided (PRE_PREPARE, PREPARE or
  COMMIT) at the stop;
- none needed the end-of-run wait to recover it (`stranded_recovered` absent);
- 5,400 of 5,400 jobs executed in every run, **none twice**;
- all 270 agents wrote metrics (`metrics_complete`), and `barrier_short_agents` is 0.

## 3. Results (mean ± std over 5 runs)

`tables.md` has every row; the headline rows are below. Coordinator decision is the coordinator-tier
selection time (`l1_selection_*`); arrival→assigned is `sched_latency` (top-tier arrival to
assignment on the executing leaf).

### Burst (20 jobs/s)

| | PBFT analytic | Snow analytic | Hybrid analytic | PBFT LinUCB | Snow LinUCB | Hybrid LinUCB |
|---|---|---|---|---|---|---|
| coordinator decision p50 (s) | 1.56 ± 0.03 | 68.3 ± 5.1 | 69.0 ± 3.7 | 0.44 ± 0.02 | 1.11 ± 0.02 | 1.11 ± 0.03 |
| arrival→assigned p95 (s) | 10.6 ± 0.6 | 138.0 ± 4.7 | 133.4 ± 13.5 | 11.7 ± 0.8 | 11.7 ± 0.8 | 10.6 ± 0.5 |
| job latency p95 (s) | 60.1 ± 1.9 | 158.8 ± 4.1 | 154.6 ± 12.8 | 56.2 ± 2.0 | 56.9 ± 0.5 | 55.3 ± 0.2 |
| throughput (jobs/s) | 13.25 ± 0.66 | 10.25 ± 0.11 | 10.37 ± 0.27 | 13.80 ± 0.17 | 13.68 ± 0.12 | 13.58 ± 0.58 |
| makespan (s) | 408 ± 20 | 527 ± 6 | 521 ± 14 | 391 ± 5 | 395 ± 3 | 398 ± 18 |
| proposers / job, coordinator tier | 9.25 | 26.95 | 26.96 | 5.65 | 7.70 | 7.72 |
| consensus msgs / job | 1321 | 1636 | 1630 | 364 | 246 | 262 |
| sends dropped | 57,391 ± 36,523 | 0 | 0 | 0 | 0 | 0 |
| Jain, coordinators | 0.852 | 0.940 | 0.922 | 0.935 | 0.955 | 0.959 |

### Poisson 7 jobs/s

| | PBFT analytic | Snow analytic | Hybrid analytic | PBFT LinUCB | Snow LinUCB | Hybrid LinUCB |
|---|---|---|---|---|---|---|
| coordinator decision p50 (s) | 0.16 | 0.92 | 0.91 | 0.08 | 0.91 | 0.92 |
| coordinator decision p95 (s) | 0.38 | 1.07 | 1.06 | 0.17 | 1.08 | 1.08 |
| arrival→assigned p95 (s) | 2.82 ± 0.06 | 4.50 ± 0.04 | 3.91 ± 0.07 | 3.31 ± 0.08 | 6.20 ± 0.17 | 5.25 ± 0.15 |
| job latency p95 (s) | 53.4 ± 0.2 | 54.6 ± 0.2 | 53.8 ± 0.3 | 53.1 ± 0.3 | 55.2 ± 0.3 | 54.3 ± 0.4 |
| jobs restarted | **3.8 ± 2.4** | 0 | 0 | 0 | 0 | 0 |
| makespan (s) | **976 ± 103** | 868 ± 1 | 867 ± 1 | 866 ± 1 | 870 ± 1 | 868 ± 1 |
| consensus msgs / job | 1896 | 1668 | 1679 | 314 | 313 | 345 |

### Hier-90 against Hier-270

Cell means; Hier-90 from `../e0-e1-c125ed8c/cells_agg.csv`. Read §5 before quoting: the two rungs
ran on different code revisions and different job sets.

| cell | coordinator decision p50 (s), burst | arrival→assigned p95 (s), steady | consensus msgs / job, burst | throughput, burst (jobs/s) |
|---|---|---|---|---|
| PBFT analytic | 0.64 → 1.56 | 3.1 → 2.8 | 348 → 1321 | 7.87 → 13.25 |
| Snow analytic | 1.23 → 68.3 | 5.1 → 4.5 | 262 → 1636 | 8.08 → 10.25 |
| Hybrid analytic | 1.35 → 69.0 | 4.4 → 3.9 | 255 → 1630 | 8.65 → 10.37 |
| PBFT LinUCB | 0.15 → 0.44 | 3.3 → 3.3 | 157 → 364 | 7.26 → 13.80 |
| Snow LinUCB | 17.9 → 1.11 | 7.0 → 6.2 | 165 → 246 | 6.14 → 13.68 |
| Hybrid LinUCB | 20.5 → 1.11 | 6.0 → 5.3 | 167 → 262 | 6.51 → 13.58 |

Steady load is Poisson 3 jobs/s at Hier-90 and 7 jobs/s at Hier-270, about 70 % of each rung's
drain rate. Hier-90 coordinator tiers are 9 (analytic) and 3 (LinUCB); Hier-270's are 27 and 9.

## 4. Reading

1. **No engine collapses at Hier-270.** Completion is 100 % in all 60 runs, and the §1 livelock
   rule doesn't fire anywhere: the worst arrival→assigned p50 is under 70 s, and at most 7 of
   5,400 jobs restart. This is consistent with the F1 gate recorded on 7 Oct (no hierarchical
   collapse, so F1 is the flat Mesh ladder).
2. **Under a burst, the 27-coordinator Snow and Hybrid tier is the bottleneck.**
   - Coordinator decisions wait a median 68–69 s, the tier drains at about 10.3 jobs/s against
     PBFT's 13.3, and job p95 is 155–159 s against 60 s.
   - Every Snow and Hybrid coordinator proposes every job (27 proposers per job under
     `coordinator_cost_matrix: self`), against 9.25 under PBFT without counter-proposals.
   - With 9 coordinators (the LinUCB cells) the three engines are level: 13.6–13.8 jobs/s, job p95
     55–57 s. So the gap tracks the size of the Snow coordinator tier, not Snow at the leaves (the
     leaf tier decides in 0.92 s at both loads). Why 27 proposers saturate that tier — the
     per-coordinator instance limit `max_inflight` 16, or round count — is not traced.
3. **At steady load the engines tie end to end.**
   - Job p95 is 53–55 s in every cell.
   - PBFT decides 6–11× faster at the coordinator tier (0.08–0.16 s against about 0.9 s), but that
     is under a second against about 50 s of execution, so it barely shows.
   - Arrival→assigned p95 orders the engines PBFT < Hybrid < Snow (2.8–3.3, 3.9–5.3 and 4.5–6.2 s),
     the same order as at Hier-90.
4. **PBFT analytic still has a tail at steady load.**
   - Each run had 1 to 7 jobs (3.8 on average) whose coordinator election waited exactly one 300 s
     reselection timeout (302–304 s), and one job waited two (604 s).
   - That stretches makespan to 866–1,093 s against about 867 s for every other cell. Job p95 and
     throughput hide it.
   - Not traced. Candidates: commit splits the resolver can't finish (191 resolved per run, 23 k
     commit-lock holds), or a lost message at 27 coordinators. PBFT never retransmits.
5. **Going from 90 to 270 agents:**
   - Steady-load scheduling holds: arrival→assigned p95 is unchanged or lower, and coordinator
     decisions are the same.
   - Consensus messages per job grow 3.8–6.4× where the coordinator tier triples from 9 to 27
     (analytic). They grow 1.5–2.3× where it goes from 3 to 9 (LinUCB).
   - Burst throughput rises with the fleet everywhere except the 27-coordinator Snow and Hybrid
     tier (item 2).
6. **LinUCB at 9 coordinators is better than at 3.** At Hier-90 the 3-coordinator Snow and Hybrid
   tier saturated under a burst (decision p50 18–20 s). At Hier-270, 9 coordinators decide in 1.1 s.
   The bandit routes over all 27 groups: the busiest group gets 5.6–6.6 % of delegations, and every
   decision had 3 candidates.
7. **PBFT analytic still drops sends under a burst**, 57 k ± 37 k per run even with the 4× pool, but
   no job restarts. No other cell dropped a send.

## 5. Caveats — read before quoting

- **Two code revisions across the rungs.** Hier-90 is c125ed8c. Hier-270 is ed8c9101, which adds
  three things between them:
  - the PBFT adoption vote, the counter margin and the job-state scan fix (2026-11-05);
  - the end-of-run classification fixes;
  - the per-tier `counter_propose`.

  The Hier-90 PBFT cells would behave differently today: voting on adoption alone changes the
  9-coordinator race outcome (`tests/test_pbft_counter_proposal.py`). The cross-rung table is
  indicative. The paper's numbers come from the frozen-tag re-run of both rungs.
- **Different job sets.** Execution time alone has p95 51.7 s in `jobs_5400` against 34.6 s in
  `jobs_1800` (exec p50 10.4 s in both). Compare the rungs on scheduling latency (arrival→assigned,
  coordinator decision), never on job latency or makespan.
- **The coordinator tier changes with the plane as well as the rung:** 9 / 3 coordinators at
  Hier-90 and 27 / 9 at Hier-270 (analytic / LinUCB). Compare engines within a column.
- **Density:** 3 agents per VM at Hier-270, 1 at Hier-90.
- **The PBFT tuning differs by tier size** (§1), and the analytic PBFT arm has a 4× transport pool.
  Say both beside every PBFT number.
- **p99 is about 121 s everywhere:** that's the 120 s simulated-execution cap, not scheduling.

## 6. Files

- `runs_wide.csv`: one row per run, every `evaluation/collect.py` column.
- `supp.csv` and `supp.py`: per-run quantities the collector doesn't export: coordinator win spread,
  busiest child group, per-tier PBFT counters, and the arrivals realised.
- `agg.py`, `cells_agg.csv` and `tables.md`: per-cell mean, std and n, and the Hier-90 comparison.
