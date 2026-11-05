# External Baseline Schedulers on the Slice (E7)

E7 compares SWARM against three **centralized** schedulers (Greedy min-cost, Round-Robin,
Random) and one **decentralized** Sparrow-style scheduler. A row in that table is only about the
*scheduler* if everything that is not the scheduler is SWARM's own, so every baseline shares
one definition (`baselines/common.py`) of:

- **the fleet** — the SWARM cell's level-0 agents, ids 1..N from its `agent_profiles.json`
  (`load_level0_fleet`). Coordinator slots never execute;
- **placement** — agent i on host `(i - 1) // agents_per_host` of the cell's hosts file, as
  `run_test.py` places agents (`host_of`);
- **execution** — SWARM's `Job.execute()` with the base config's `runtime.wall_time_*` clamp
  (`configure_execution`), `runtime.executor_workers` jobs at once per agent, and the agent's
  completion write: outputs published only on success, inside the same transaction
  (`run_job`);
- **the workflow DAG gate** — SWARM's readiness registry (`data_ready`);
- **cost parameters** (Greedy) — `job_selection` from the base config (`cost_params`);
- **outputs** — `all_jobs.csv`, `metrics.json` keyed by agent id with each agent's
  `executed_jobs` (so `collect.py` reports `jobs_executed_twice`), `all_agents.csv` (the
  level-0 fleet, so fairness counts idle agents), `run_meta.json`, `collect_meta.json`
  (`arm: baseline`, `policy: greedy|round_robin|random|sparrow`) and `drain.json`; exit 0
  completed or ended on the clock, 2 refused to start, 3 a worker never reported, 4 the store
  became unreadable — the same statuses `campaign.py` already classifies.

Since 2026-10-07 `run_test.py` archives each run's `agent_profiles.json` into its run dir, so a
baseline takes a SWARM cell's exact fleet with `--use-profiles <swarm-run>/agent_profiles.json`.

## Centralized schedulers (`baselines/scheduler.py`)

```
database                                   agent hosts (placed as run_test.py places agents)
┌──────────────────────────────┐           ┌──────────────────────────────┐
│ run_baseline_remote.py       │           │ baseline_worker.py (agent i) │
│  JobDistributor → PENDING    │           │  BLPOP baseline:<run>:q:<i>  │
│  scheduler: read PENDING,    │──Redis──→ │  run executor_workers jobs   │
│   gate on DAG, assign_jobs,  │  (star)   │   at once: RUNNING → execute │
│   persist READY + leader,    │           │   → COMPLETE (+ outputs)     │
│   RPUSH id to agent's queue  │ ←──────── │  heartbeat, stats            │
│  poll COMPLETE, free capacity│           └──────────────────────────────┘
└──────────────────────────────┘
```

One process on `database` decides every placement. Each pass it reads new PENDING jobs, holds
back those whose DAG inputs do not exist yet, and lets the strategy place the rest; a strategy
**reserves capacity on the agent it picks before considering the next job of the batch**, so no
agent is over-committed within one batch. The assignment is persisted (READY, `leader_id`,
conditional on the record still being PENDING) and the job id is pushed onto that agent's
run-scoped dispatch queue. The worker runs it — up to `executor_workers` at once, the rest
waiting in its pool exactly as in an agent's executor — and writes RUNNING then COMPLETE, both
conditional on the record still naming that worker. The scheduler learns of a completion the
way it would in a deployment: through the store, on its next poll, and only then frees the
capacity.

Every key is run-scoped (`baseline:<run_id>:…`: queues, heartbeats, stats, shutdown) and
workers are stopped by a pattern carrying the run id, so a leftover worker from another run can
neither take this run's work nor be killed by it. A worker never runs a job the scheduler did
not assign to it (a dispatch whose record is not READY with its own `leader_id` is a no-op).

**Files.** `baselines/scheduler.py` (strategies and the loop), `baselines/baseline_worker.py`
(`DispatchWorker`), `baselines/run_baseline_remote.py` (orchestrator, run on `database` as
root), `baseline-worker-start.sh` (detached start over ssh, prints the pid),
`baselines/run_baseline.py` (one local process, per-agent thread pools — smoke tests only, it
pays no WAN), `run_centralized_baselines.sh` (repeats, all arms including `sparrow`).

```bash
python3.11 baselines/run_baseline_remote.py --scheduler greedy --agents 90 --jobs 1800 \
    --db-host database --agent-hosts-file agent_hosts.txt --agents-per-host 1 \
    --run-dir runs/e7/greedy/run01 --config campaigns/config_pbft.yml \
    --use-profiles <swarm-run>/agent_profiles.json --use-jobs-dir /root/workloads/jobs_1800
```

The base config must exist at the same path under `--remote-repo-dir` on the agent hosts
(`--remote-config` otherwise). In a campaign the runner is `baselines/run_baseline_remote.py`;
a killed attempt's workers are stopped by `campaign.py`'s `stop_cell_agents`.

**Fixed 2026-10-07** (plan E7, "known defects"); before then the centralized rows were weaker
than centralization alone makes them, and none of these runs is citable: the worker ran one job
at a time while the scheduler admitted several per agent, so the excess sat READY and inflated
wait and makespan; at Hier-N every profile, coordinator slots included, became an executor;
Round-Robin and Random did not reserve inside a batch; the wall-time clamp and cost parameters
were class defaults rather than the config's; the DAG was ignored; `metrics.json` was a summary
dict the collector read as a phantom agent; and `run_centralized_baselines.sh` tested `tee`'s
exit status, so every failed run was logged as completed.

**Not reproduced, by design.** No failure handling: a worker that dies holding a job leaves it
READY/RUNNING, as a centralized scheduler without a recovery layer would. Under the E7 failure
injection that is the result being measured, not a defect.

---

## Sparrow-style decentralized baseline (2026-10-07)

The three schedulers above are centralized: one process on `database` sees every job and the
whole fleet. A reviewer's first objection to comparing SWARM against them is that they are
strawmen for a *decentralized* scheduler. The Sparrow-style arm answers it with the canonical
decentralized design: batch sampling with late binding (Ousterhout et al., SOSP 2013).

**Files.** `baselines/sparrow.py` (scheduler and worker logic), `baselines/sparrow_node.py` (one
process, `worker` or `scheduler`), `baselines/run_sparrow.py` (orchestrator),
`sparrow-node-start.sh` (detached start over ssh). Tests: `tests/test_sparrow_baseline.py`.

**How it works.**

- **Several independent schedulers.** Job ownership is a stable hash of the job id
  (`owner_of`, crc32 mod K), so no two schedulers probe for one job and none shares state with
  another. They run as separate processes spread evenly over the agent hosts (agent ids
  `1 + j·N/K`), so on a site-interleaved hosts file they sit at different sites.
- **Probes, not placements.** For each of its PENDING jobs a scheduler samples
  `--probe-ratio` (d, default 2) distinct workers among those that could *ever* run the job
  (total capacity and DTNs) and are live (fresh heartbeat), and appends a reservation to each
  one's queue. It never reads load.
- **Late binding.** A worker serves its queue in order. When it has a free slot that fits the
  head job it claims the job record by compare-and-swap, PENDING to READY naming itself; the
  first claim wins and every later reservation for that job is dropped (Sparrow's no-op). A head
  job that does not fit yet keeps its place (head-of-line, as Sparrow's slots).
- **Re-probing.** A job still unclaimed after `--reprobe-s` (30 s) gets a fresh round.
- **Same jobs, same execution.** The workers run SWARM's `Job.execute()` with the
  `runtime.wall_time_*` clamp and `executor_workers` concurrency read from the same base config
  the SWARM arm used; they persist RUNNING then COMPLETE with the real exit status, honour
  `should_fail`, gate workflow DAG jobs on SWARM's readiness registry and publish outputs in the
  completion write, only on success. The fleet is the level-0 agents of the SWARM cell's
  `agent_profiles.json`, placed on the same hosts in the same order (`(id-1)//agents_per_host`).

**What differs from Sparrow, and why it is still the right comparison.** Probes and claims go
through the shared Redis on `database` instead of direct scheduler-to-worker RPC, so each costs
a round trip to `database` — the same store and the same WAN SWARM's job pool uses, but a star
rather than peer links. There is no failure handling beyond Sparrow's own (none at the
scheduler): a worker that dies holding a claimed job leaves it READY/RUNNING. Jobs are
single-task, so the probe ratio is probes per job.

**Running it.** On the `database` node, as root, with the SWARM cell's profiles and jobs:

```bash
python baselines/run_sparrow.py --mode remote --agents 90 --jobs 1800 \
    --db-host database --agent-hosts-file agent_hosts.txt --run-dir runs/sparrow/run01 \
    --use-profiles agent_profiles.json --use-jobs-dir jobs/ --probe-ratio 2 --seed 42
# or in the batch script, beside the centralized arms:
./run_centralized_baselines.sh --mode remote --schedulers greedy,round-robin,random,sparrow \
    --reuse-jobs --agents 90 --jobs 1800 --db-host database --agent-hosts-file agent_hosts.txt
```

**Validity.** It refuses to start (exit 2) when the hosts file is too short for the fleet or any
worker or scheduler fails to register within `--startup-timeout`; exits 3 when a worker never
reports its stats (`metrics_shortfall.json`); exits 4 when the store is unreadable for 60 s.
`drain.json` says how the run ended (`all_terminal`, `timer`, `redis_unreadable`). The run
directory carries `metrics.json` keyed by agent id with each worker's `executed_jobs`, so
`collect.py` reports `jobs_executed_twice`; `all_agents.csv`, so fairness counts idle workers;
and `collect_meta.json` (`policy: sparrow`, `arm: baseline`). Compare with
`plot_comparison.py --dirs Sparrow=runs/sparrow ...` or through `evaluation/collect.py`.

**Local smoke test (2026-10-07).** 6 workers, 2 schedulers, 40 generated jobs, wall time capped
at 2 s, against a fakeredis TCP server: 40/40 complete in 11.4 s, 74 probes, 28 no-ops, no job
executed twice. Local mode pays no WAN and is for smoke tests only.
