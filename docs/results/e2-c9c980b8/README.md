# E2 campaign at c9c980b8: learned delegation quality on PBFT and Snow

Collected 2026-10-28 from `runs/campaign-e2-c9c980b8` on the database node. Campaign file
`campaigns/e2-hier90.yml` (9ee110fe), configs from `campaigns/make_e2_configs.py`, failure profile and
workloads from `../e2-prep/README.md`. Code c9c980b8 on every agent.

## 1. What was run

| Item | Value |
|---|---|
| Topology | Hier-90, `--groups-per-coordinator 3`: coordinators 88, 89, 90 each lead three groups of 9 |
| Arrivals | Poisson 3 jobs/s, 1,800 jobs, seeded (every arm sees the same times); E2c work day 8,000 jobs on `e2c_workday.json` |
| Profile | per coordinator one ram-weak, one cpu-weak, one mediocre group; oracle failure rate 5 %, best context-blind 30 % |
| Load guard | `mab.load_slack: 9` on in every bandit arm; `linucb-noguard` is the ablation; `random` has none |
| Cells | 20 per engine × PBFT and Snow = 40 cells × 5 repeats = 200 runs |
| Window | 20 Oct 18:16 UTC to 23 Oct 21:20 UTC; one repeat re-run 28 Oct (§2) |

Families: **E2** (7 arms: LinUCB shared / per-group, LinTS, UCB1, ε-greedy, random, LinUCB without the
guard), **E2a** (failure profile flips ram-weak ↔ cpu-weak 420 s into each leaf's failure clock; LinUCB
discount 1.0 / 0.98, ε-greedy step none / 0.02), **E2b** (group 3, coordinator 89's cpu-weak group,
SIGKILLed 120 s after distribution starts, held 180 s, rejoined; LinUCB shared, LinUCB without timeout
decay, ε-greedy), **E2c** (stylized work day and a job-mix shift at job 901; LinUCB shared / per-group,
ε-greedy).

## 2. Validity

All 200 citable runs: drained with 0 pending and 0 in flight, **0 jobs executed twice**, every
non-E2b run executed all its jobs, and no run outside E2b recorded a failure verdict (checked by the
new classifier, 6cafd92d).
`oracle.py --validate` passed on **every** run: mean absolute error between injected and observed failure
rate per job type 0.007–0.024, no job type beyond sampling noise. Regret is scored on 100 % of decisions
except E2a (97.5–98.2 %: decisions within the flip's boundary margin are refused).

Three attempts are excluded (`.attempt1`): `e2-hier90-snow-eps/run01` (agent-29..35 site dropped
mid-run), `e2c-workday-hier90-pbft-linucb-pergroup/run01` (ssh failure at launch), and
`e2c-workday-hier90-snow-linucb-shared/run05`. The last was recorded `ok` at the time but was not: the
database's WAN path to every site except agent-1..9 failed during it, 81 of 90 agents were declared
failed through stale Redis heartbeats, 519 jobs ran twice, and it stopped with 73 jobs in flight. It was
re-run clean on 28 Oct. `campaign.py` now classifies such runs `truncated` / `duplicated` /
`false_verdicts` and retries them; re-classifying all 200 with it changes nothing else.

E2b completion is 99.1–99.8 %, a lower bound: a killed agent's first-life executions are lost with it.
Restarts: 0 everywhere except two E2b PBFT runs (linucb-shared run02: 5, linucb-nodecay run01: 2).

## 3. Results

Mean ± 95 % t-interval over 5 runs. Regret and accuracy are over decisions with a choice; regret is
in reward units (0 = the oracle's group). Full tables: `tables.md`.

### 3.1 E2: contextual arms route best, and the load guard is half their regret

| Arm | Engine | Regret | Accuracy | Guard part | Policy part | Success % |
|---|---|---|---|---|---|---|
| LinUCB shared | PBFT / Snow | 0.251 / 0.230 | 0.57 / 0.60 | 0.130 / 0.109 | 0.121 / 0.121 | 78.1 / 79.0 |
| LinUCB per-group | PBFT / Snow | 0.251 / 0.254 | 0.58 / 0.55 | 0.134 / 0.128 | 0.117 / 0.126 | 78.0 / 77.8 |
| LinTS | PBFT / Snow | 0.231 / 0.217 | 0.60 / 0.61 | 0.119 / 0.110 | 0.112 / 0.107 | 79.4 / 80.5 |
| UCB1 | PBFT / Snow | 0.339 / 0.330 | 0.47 / 0.49 | 0.114 / 0.107 | 0.225 / 0.223 | 72.2 / 72.7 |
| ε-greedy | PBFT / Snow | 0.383 / 0.409 | 0.45 / 0.41 | 0.083 / 0.044 | 0.301 / 0.364 | 69.9 / 67.8 |
| random | PBFT / Snow | 0.500 / 0.502 | 0.33 / 0.34 | – | – | 61.4 / 61.4 |
| LinUCB, no guard | PBFT / Snow | 0.196 / 0.170 | 0.67 / 0.75 | 0 | 0.196 / 0.170 | 82.6 / 83.7 |

* **Ranking holds on both engines.** Contextual (LinUCB, LinTS) < UCB1 < ε-greedy < random on regret;
  CIs are ±0.01–0.04, so every step except LinUCB vs LinTS is clear. On the policy's own part the
  contextual arms are half of UCB1 (0.11–0.13 vs 0.22) and a third of ε-greedy.
* **The load guard costs the contextual arms about half their regret.** It narrows ~550–600 of the
  1,800 decisions and in ~420–470 of them removes the oracle's group. Off, LinUCB reaches regret
  0.17–0.20 and 83–84 % success, the best of any arm. The guard was kept on to stop herding (E1′); its
  price in routing quality is now measured. Its herding benefit is not shown here (§5).
* **Shared vs per-group LinUCB tie** (within CI on both engines), as in E1′.
* **Engine does not change routing quality.** PBFT and Snow regret agree within CI in every arm.

### 3.2 Engine: context error differs, regret does not follow

| Family | Context error PBFT | Snow | Regret PBFT | Snow | Spearman(regret, ctx error) per run |
|---|---|---|---|---|---|
| E2 | 0.149 | 0.215 | 0.307 | 0.302 | −0.01 / 0.00 |
| E2c work day | 0.346 | 0.529 | 0.288 | 0.279 | 0.01 / 0.01 |

Snow's coordinator view of group load is 1.4–1.5× more wrong than PBFT's (its coordinator decision takes
0.96 s against 0.07 s), and the gap grows with load. Regret does not move with it: per-decision
correlation is ~0 everywhere. In this profile routing quality depends on job type → group failure rate,
not on load, and the load guard is the only consumer of the load view. Cost of Snow: job latency p50
14.5 vs 11.9 s, p95 39.7 vs 36.7 s at 3 jobs/s; throughput equal (2.9 jobs/s, arrival-bound).

### 3.3 E2a: discounting re-learns after the flip, ε-greedy's step size does not

Accuracy over the 120 s before and the 300 s after the flip (each leaf flips on its own clock; the event
marker is the median):

| Arm | PBFT before → after | Snow before → after | Whole-run regret PBFT / Snow |
|---|---|---|---|
| LinUCB, discount 1.0 | 0.43 → 0.28 | 0.43 → 0.23 | 0.313 / 0.325 |
| LinUCB, discount 0.98 | 0.44 → **0.46** | 0.41 → **0.45** | 0.297 / 0.303 |
| ε-greedy, step none | 0.39 → 0.29 | 0.34 → 0.26 | 0.436 / 0.420 |
| ε-greedy, step 0.02 | 0.35 → 0.20 | 0.31 → 0.17 | 0.420 / 0.443 |

With a discount of 0.98 LinUCB's accuracy is no lower after the flip than before it; without it, it
drops by a third to a half. The recency step does nothing for ε-greedy. `recover_s` (in `tables.md`) is
not a useful comparison here: it measures return to each arm's *own* pre-event accuracy, and
ε-greedy's is near random, so it "recovers" in 20–50 s by chance.

### 3.4 E2b: re-adoption is fast, but detection does not gate the dead group

**Superseded by `../e2b-58f3ab22/README.md`** (gate fixed in 58f3ab22, E2b re-run 29 Oct): the last
delegation to the dead group now comes at most 68 s after the kill and LinUCB re-adopts in 3–8 s. The
text below is kept as the record of the defect.

| Arm | Engine | Delegations to group 3 while down (of coordinator 89's) | Re-adopted | First delegation after rejoin |
|---|---|---|---|---|
| LinUCB shared | PBFT / Snow | 10.6 of 61 / 20.2 of 144 | 5/5 / 5/5 | 38.8 s (median 16) / 20.4 s |
| LinUCB, no timeout decay | PBFT / Snow | 9.4 of 48 / 20.6 of 129 | 5/5 / 5/5 | 11.6 s / 19.0 s |
| ε-greedy | PBFT / Snow | 0.8 of 53 / 11.8 of 151 | 3/5 / 5/5 | 4.4 s / 20.7 s |

**The dead group is not withdrawn from the bandit during the outage.** Under Snow every run delegated
to it 7–22 times while it was down, the last one 165–173 s after the kill (the outage lasts 180 s); four
of five PBFT LinUCB runs did so up to 159 s after it. The heartbeat threshold is 60 s. Cause, from the
code: a coordinator gates delegation on `_get_live_child_groups` (`resource_agent.py:996`), which is
the children map, and a child-tier entry is evicted only after `peer_expiry_seconds`
(`_staleness_eviction_threshold`), **300 s** in both E2 configs. A 180 s outage never reaches it. So
E2b as configured measures re-adoption after rejoin and bandit learning from timeouts, not liveness
gating, and the decay ablation shows no difference: first delegation after rejoin 11.6 s without decay
vs 38.8 s with it on PBFT, 19.0 vs 20.4 s on Snow, with overlapping CIs.
Re-adoption itself is quick on every LinUCB run (median 16–20 s).

### 3.5 E2c: job-mix shift and the work day

**Mix shift** (best group moves at job 901): LinUCB accuracy 0.55–0.60 → 0.29–0.37 in the 300 s after,
back to 90 % of its pre-shift level in 114–174 s; ε-greedy 0.57 → 0.15–0.17, 237–243 s, and one run per
engine never recovered. Whole-run regret 0.30–0.32 (LinUCB) vs 0.44–0.45 (ε-greedy).

**Work day** (rates 1.95–7.8 jobs/s): LinUCB accuracy 0.61–0.65 over the day against ε-greedy 0.29;
LinUCB dips at the first ramp (segment 2, 5.85 jobs/s: 0.47–0.53) and is highest in the evening
segment (0.75–0.82). At this load the guard is almost all of LinUCB's regret (0.16–0.17 of 0.19–0.22;
policy part 0.04–0.05). Context error is highest here (Snow 0.53, PBFT 0.35) and regret is still equal
across engines. The schedule's shape is stylized, not from a trace: say so beside any number.

## 4. What the numbers say against the plan's four questions

1. **Gap to the oracle:** contextual arms regret 0.22–0.25 with the guard, 0.17–0.20 without; random 0.50.
2. **Re-learning after E2a:** yes for LinUCB with discount 0.98 (no accuracy loss after the flip), no
   for ε-greedy at either setting.
3. **Re-adoption after E2b:** yes, within ~20 s median; but the run did not exercise liveness gating
   (§3.4).
4. **`--validate`:** passed on all 200 runs.

## 5. Caveats and open items

* **E2b here did not exercise liveness gating** (§3.4). Resolved by gating on the failure detector's
  window (58f3ab22) and re-running E2b: `../e2b-58f3ab22/README.md`. Cite E2b from there.
* The guard's benefit (spreading load) is not measured in this campaign: `group_busiest_share` is not in
  this collection, so the herding side of the trade-off still rests on E1′.
* The engine-independence of regret is a property of this profile (failures depend on job type, not load).
  A load-dependent profile could change it.
* PBFT's coordinator 89 makes ~29 decisions before the outage against Snow's ~80: the engines split jobs
  among coordinators differently, so E2b counts are per engine, not comparable across them.
* One scale (Hier-90, G=3), one profile, simulated execution.

## 6. Files

`runs_wide.csv` (collector, 202 rows incl. the two `.attempt1` runs it saw), `cells_agg.csv`,
`events.csv` / `events_agg.csv` (E2a flip, E2c shift, E2b outage per run), `timecourse.csv` (30 s bins
per run), `workday.csv` / `workday_agg.csv`, `tables.md`, generated by `agg.py` and `timecourse.py`.
`decisions.csv` (556k rows) and `job_rtt.csv` stay in `/root/collect-e2-c9c980b8/` on the database node.
