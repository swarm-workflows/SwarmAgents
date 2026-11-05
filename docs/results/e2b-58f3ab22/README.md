# E2b at 58f3ab22: group outage with the liveness gate on the detector's window

Collected 2026-10-30 from `runs/campaign-e2b-58f3ab22` (campaign file `campaigns/e2b-hier90-58f3ab22.yml`).
**Supersedes the E2b rows of `../e2-c9c980b8/README.md` (§3.4).** Every other E2 number stays at c9c980b8:
the gate only acts when every member of a group has been silent for more than 67 s, which in E2 happens only
here (decided 2026-10-29: re-run E2b only).

## 1. What changed and what was run

At c9c980b8 a coordinator called a child group live while any member had an entry in its children map, and
entries are evicted only at `peer_expiry_seconds` (300 s). The 180 s outage never reached it, so coordinators
kept delegating to the dead group until ~170 s into the outage. 58f3ab22 gates on each member's last fresher
record within `failure_threshold_seconds × 1.1 + 1` = 67 s, on the coordinator's monotonic clock, without
evicting anything (`_get_live_child_groups`, `tests/test_child_staleness.py`).

Same six cells, arguments, configs, profile and fleet as c9c980b8: Hier-90 G=3, Poisson 3 jobs/s, 1,800 jobs,
group 3 (agents 31–40, coordinator 89's cpu-weak group) SIGKILLed 120 s after distribution starts, held 180 s,
rejoined. 6 cells × 5 = 30 runs, 29 Oct 12:46–21:24 UTC, all `ok` on the first attempt. Rehearsal:
`runs/pilot-e2b-gate-58f3ab22` (`campaigns/e2b-gate-rehearsal.yml`).

## 2. Validity

All 30: drained, 0 in flight, **0 jobs executed twice**, every outage killed and rejoined all 10 members
(held 180 s), regret scored on 100 % of decisions. Completion 99.1–99.8 %, a lower bound (a killed agent's
first-life executions die with it). Restarts: 0 except PBFT LinUCB shared (runs 01/02/04: 9, 1, 1).

## 3. Results

Mean ± 95 % t-interval over 5 runs. Coordinator 89's decisions only for the delegation counts.

| Arm | Engine | Last delegation to dead group after kill (max over runs) | To dead group while down: before → after fix | First delegation after rejoin: before → after |
|---|---|---|---|---|
| LinUCB shared | PBFT | 65.5 s (was 158.6 s) | 10.6 → **2.6** of ~55 | 38.8 → **5.7 s** |
| LinUCB shared | Snow | 67.2 s (was 170.9 s) | 20.2 → **8.2** of ~148 | 20.4 → **3.9 s** |
| LinUCB, no decay | PBFT | 68.0 s (was 159.5 s) | 9.4 → **3.4** | 11.6 → **7.8 s** (4 of 5; run01 never returned) |
| LinUCB, no decay | Snow | 68.1 s (was 173.5 s) | 20.6 → **8.0** | 19.0 → **2.9 s** |
| ε-greedy | PBFT | 64.9 s (was 72.2 s) | 0.8 → 0.8 | 4.4 → 57 s (see below) |
| ε-greedy | Snow | 67.4 s (was 172.3 s) | 11.8 → **3.8** | 20.7 → **2.8 s** |

* **The gate now closes at the detector's window in every run.** No delegation reached the dead group later
  than 68.1 s after the kill (the window is 67 s; the extra second is the refresh tick between a record and
  its stamp). Every delegation while down falls inside the detection gap.
* **Re-adoption is faster too**, 3–8 s on every LinUCB arm against 11–39 s. With the group gated out while it
  was dead, the bandit records no timeouts against it, so it has nothing to unlearn after the rejoin.
* **Routing quality is unchanged** (regret 0.22–0.24 LinUCB, 0.37–0.41 ε-greedy; all within CI of c9c980b8),
  as is success (66–79 %). Snow job latency p95 fell 46.8 → 41.5 s (LinUCB shared): fewer jobs wait out a
  delegation timeout on a dead group.
* **Decay ablation.** One PBFT no-decay run (run01) never delegated to group 3 again after the rejoin (0 of 79
  decisions), the failure mode the decayed-timeout fix exists for; the other four re-adopted in 4–12 s. One
  run in five is a lead, not a result.
* **PBFT ε-greedy** sends group 3 only 2–6 of ~30 jobs even before the kill, so its first-return time
  (3–122 s) measures how rarely it picks that group, not the gate. Do not compare it with the LinUCB rows.

## 4. Caveats

* E2b is now at a different revision from the rest of E2. The change is inert outside a whole-group silence
  over 67 s; state that beside any figure mixing the two, and re-run everything at the final frozen tag.
* PBFT's coordinator 89 makes ~55 decisions during the outage against Snow's ~148: counts compare within an
  engine, not across.

## 5. Files

`runs_wide.csv`, `events.csv`, `timecourse.csv` (collector and `../e2-c9c980b8/timecourse.py`), `compare.py`
(prints `tables.md`, old vs new per cell). `decisions.csv` / `job_rtt.csv` stay in
`/root/collect-e2b-58f3ab22/` on the database node. The last-delegation offsets come from a raw-decision scan
of `metrics.json` against `outage.json` (`killed_at`, `rejoined_at`).
