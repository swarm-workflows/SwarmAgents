# E3a: selection time by measured WAN round-trip, over the E1′ runs at c125ed8c

Analysis only, no new runs (conference plan §4.2 E3a). Input: `/root/collect-c125ed8c/job_rtt.csv`, the
collector's per-job RTT attribution for the 95 E0/E1′ runs of `../e0-e1-c125ed8c/` (19 cells × 5 repeats,
165,000 jobs). Script `e3a.py`; full table `tables.md`; per (cell, run, bin) rows `per_run.csv`; cell means
`cells.csv`.

## 1. Method

* **Unit: the job.** Each job carries the RTT between the host of the leaf that executed it and the host of
  the coordinator that delegated it, from the run's `rtt_matrix.json` (ICMP, 20 samples, median per pair,
  captured before launch; `evaluation/rtt.py`). Bins: LAN < 5 ms ≤ regional < 40 ≤ continental < 90 ≤
  transatlantic.
* **Per run, then over runs.** Each quantile is taken within one (cell, run, bin) and averaged over the
  cell's 5 runs, 95 % t-interval.
* **Two tiers, never summed:** coordinator tier `l1_selection_s` (deciding which coordinator owns the job)
  and leaf tier `selection_s` (deciding which leaf in the group runs it).
* **Attribution is complete:** 0 of 165,000 jobs unattributed. Placement is identical in every run, so bin
  shares are stable: LAN 5–12 %, regional 45–62 %, continental 15–37 %, transatlantic 8–20 % of a cell's jobs.
  Transatlantic is the AMST site (~113 ms to the coordinators).

## 2. Results (Hier-90 unless named; s, mean over 5 runs)

**Leaf tier: the WAN shows up, at the transatlantic bin only.**

| Cell | LAN | regional | continental | transatlantic |
|---|---|---|---|---|
| PBFT analytic, burst: p50 / p95 | 0.084 / 0.15 | 0.070 / 0.20 | 0.068 / 0.19 | **0.136 / 1.06** |
| PBFT analytic, 3 jobs/s | 0.094 / 0.12 | 0.078 / 0.16 | 0.072 / 0.13 | **0.127 / 0.27** |
| Snow analytic, burst | 1.19 / 1.95 | 1.19 / 1.84 | 1.20 / 1.69 | **1.59 / 2.74** |
| Snow analytic, 3 jobs/s | 1.19 / 1.39 | 1.19 / 1.38 | 1.20 / 1.59 | **1.58 / 1.62** |
| E0 Hier-30 PBFT, burst | 0.044 / 0.15 | 0.047 / 0.18 | (2 jobs) | **0.187 / 1.33** |

Below 90 ms the leaf tier is flat: LAN, regional and continental agree within a few ms on both engines.
Transatlantic executors pay +0.06 s (PBFT p50, about 2×) and +0.39 s (Snow p50, +33 %), and the PBFT tail
grows 5–7× under the burst. Snow's leaf floor (~1.19 s, identical across the near bins) is consistent with its round
structure (β = 6 consecutive successful rounds), not with distance; the WAN adds to a floor PBFT does not
have. The leaf decision is
intra-group, so this bin is in effect "the job ran in the group containing the AMST leaf": the
executor's distance to its group peers, which the executor↔coordinator RTT tracks for AMST.

**Coordinator tier: no dependence on this RTT.**

| Cell (p50) | LAN | regional | continental | transatlantic |
|---|---|---|---|---|
| PBFT analytic, 3 jobs/s | 0.06 | 0.07 | 0.09 | 0.06 |
| Snow analytic, 3 jobs/s | 0.90 | 0.91 | 0.93 | 0.90 |
| Hybrid analytic, 3 jobs/s | 0.90 | 0.91 | 0.93 | 0.90 |
| Snow LinUCB shared, burst | 18.9 | 18.3 | 16.4 | 18.7 |
| PBFT analytic, burst | 0.37 | 0.59 | 0.95 | 0.43 |

Coordinator-tier time is flat across bins for every Snow and Hybrid cell and every steady-load cell. The
one non-flat row, PBFT under the burst (and PBFT LinUCB's continental p95 of 4.4–4.7 s), is not monotone in
RTT: transatlantic is faster than continental. That is composition, not distance: each bin's jobs come from
a different mix of coordinators, and under the burst PBFT coordinators differ in queueing.

**Completion: 1.000 in every bin of every cell.** Distance did not cost a single job.

## 3. What it says

1. **The real WAN costs the leaf tier only at transatlantic distance**, and only modestly at steady load
   (PBFT +0.05 s, Snow +0.4 s at p50). Up to ~90 ms the system is insensitive to where the executor sits.
2. **The coordinator tier's decision time does not depend on the executor↔coordinator RTT**, by
   construction: the coordinators decide among themselves before the job is delegated, so the RTT that can
   enter is the coordinator↔coordinator distance, which this attribution does not vary. The plan's E3a
   attribution therefore answers the leaf-tier question and is uninformative for the coordinator tier.
3. **Engine ordering is preserved in every bin.** Snow's costs are its floor (~0.9 s coordinator, ~1.2 s
   leaf) plus WAN; PBFT stays under 0.15 s at p50 in every bin at steady load.

## 4. Caveats and a suggested addition

* **Coordinator-tier attribution.** To ask whether the WAN slows the coordinator tier, attribute each job's
  `l1_selection_s` to its deciding coordinator's median RTT to the other coordinators (from the same
  `rtt_matrix.json`). With 3 or 9 coordinators at fixed placement that is 3–9 points per cell, not a bin
  sweep; it is not in the plan and is not done here.
* The transatlantic bin is one site (AMST), so it confounds distance with that site's hosts.
* Burst cells are backlog drains: their tails mix queueing with distance. Quote the 3 jobs/s cells for the
  WAN effect.
* E3a is measured on c125ed8c (E0/E1′); the plan places it there. The E2 runs (c9c980b8) carry the same
  attribution (`/root/collect-e2-c9c980b8/job_rtt.csv`) if a second campaign is wanted.
