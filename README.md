# SwarmAgents

A framework for distributed, consensus-based job scheduling. Agents score jobs against their own
resources and agree on who runs what — over gRPC, with Redis for shared state — using either a
PBFT-like three-phase protocol or a Snow/Avalanche-style sampling protocol backed by SWIM
membership and gossip. It runs synthetic workloads, replays real Pegasus workflow traces, and can
**actually execute** those workflows in their own containers for comparison against Pegasus.

- **Agents**: rule-based (`ResourceAgent`), LLM-scored (`LlmAgent`), Colmena integration
- **Topologies**: ring, mesh, star, hierarchical (with bandit-based delegation)
- **Consensus**: PBFT or Snow; SWIM failure detection; epidemic state dissemination
- **Workloads**: synthetic, replayed Pegasus traces, real workflow execution, quantum/hybrid jobs

## Contents

- [Quick start](#quick-start)
- [Running experiments](#running-experiments)
- [Running a real Pegasus workflow](#running-a-real-pegasus-workflow)
- [Results and plots](#results-and-plots)
- [Configuration](#configuration)
- [How it works](#how-it-works)
- [Utilities](#utilities)
- [Documentation](#documentation)

## Quick start

```bash
pip install -r requirements.txt
docker run -d -p 6379:6379 redis          # required for every run

python -m pytest tests/                   # unit tests

# 20 agents in a mesh, 100 jobs, all on this machine
python run_test.py --mode local --agent-type resource --agents 20 --topology mesh \
    --jobs 100 --jobs-per-interval 10 --db-host localhost --run-dir runs/test-001
```

A run generates agent configs, starts the agents, feeds jobs into Redis, waits, stops everything,
and writes results to `--run-dir`: `metrics.json`, per-agent logs (`agent-<id>.log`),
`run_meta.json`, and plots. It exits non-zero if any agent failed to report metrics, so a batch
driver never averages in a broken cell.

**Start with mesh.** Every agent talks to every other, so consensus finishes in one hop and a
small run completes quickly. Ring and star forward messages through neighbours or a hub, so the
same fleet takes noticeably longer to settle — they are worth running when the topology is what
you are measuring, not for a first look. Mesh's message count grows with the square of the fleet,
so past a few tens of agents move to hierarchical.

## Running experiments

### Resource agents

```bash
# local
python run_test.py --mode local --agent-type resource --agents 30 --topology mesh \
    --jobs 500 --db-host localhost --run-dir runs/mesh-30

# hierarchical
python run_test.py --mode local --agent-type resource --agents 30 --topology hierarchical \
    --jobs 500 --db-host localhost --run-dir runs/hier-30

# remote: agents spread over hosts listed one per line, passwordless SSH required
python run_test.py --mode remote --agent-type resource --agents 30 --agents-per-host 5 \
    --topology mesh --jobs 1000 --db-host <db-host> --agent-hosts-file agent_hosts.txt \
    --run-dir runs/remote-30
```

Hierarchical fleet sizes are presets (30, 60, 80, 90, 100, 110, 120, 250, 270, 990, 1000) —
anything else is refused rather than silently dropping agents. Add
`--groups-per-coordinator G` for any delegation measurement: at the default of 1 a coordinator
has a single candidate, so neither the bandit nor an LLM policy ever chooses.

### LLM agents

```bash
export OPENAI_API_KEY=sk-...                        # or: export LLM_BASE_URL=http://localhost:11434/v1
python run_test.py --mode local --agent-type llm --agents 10 --topology mesh \
    --jobs 200 --jobs-per-interval 20 --db-host localhost --run-dir runs/llm-001
```

### Repeated runs, dynamic agents, failures

```bash
# 10 runs of the same cell, with statistics across them
python batch_tests_v2.py --runs 10 --base-out runs/batch --mode local --agent-type resource \
    --agents 20 --topology mesh --jobs 500 --db-host localhost

# add 5 agents 30s in (also --dynamic-trigger bucket | jobs-completed)
python run_test.py --mode local --agent-type resource --agents 20 --dynamic-agents 5 \
    --dynamic-trigger time --dynamic-delay 30 --topology mesh --jobs 500 \
    --db-host localhost --run-dir runs/dynamic-time

# kill agents mid-run to exercise detection and reassignment
python kill_agents.py --mode local --count 5 --random
grep -e "RESTART: Job" -e "detected as FAILED" runs/*/agent-*.log
```

A run that deliberately kills agents must declare it (`--expect-silent-agents 3,7`), or the
metrics-completeness gate fails the run.

### Quantum / hybrid jobs

```bash
python run_test.py --mode local --agent-type resource --agents 20 --topology mesh --jobs 200 \
    --db-host localhost --run-dir runs/quantum \
    --quantum-agents-pct 0.25 --quantum-fraction 0.2 --hybrid-fraction 0.1 [--split-hybrid]
```

`--split-hybrid` decomposes hybrid jobs into co-scheduled quantum-producer / classical-consumer
sub-jobs. See [QUANTUM_HYBRID_DESIGN.md](docs/QUANTUM_HYBRID_DESIGN.md).

## Running a real Pegasus workflow

Two modes:

- **Replay** — the trace's jobs are scheduled for real, each sleeping its recorded wall time.
- **Real execution** — the workflow's own executables run in its own container.

Both follow the same four steps: **convert → copy → generate the fleet → run.** Full reference,
including the validated results and the limits, in
[WORKFLOW_EXECUTION.md](docs/WORKFLOW_EXECUTION.md).

### Replay (simulated jobs)

```bash
# 1. convert, on the Pegasus submit host (copy the two scripts there; they need only Python 3)
python3 pegasus_profile_extractor.py --root /path/to/workflows --output profiles.json
python3 pegasus_to_swarm_converter.py --input profiles.json --input-type json \
    --output-dir replay_jobs/ --dag-gating --dtn-names local --dtn-scope job

# 2. copy the bundle to wherever Redis is
rsync -a --delete replay_jobs/ <redis-host>:<repo>/replay_jobs/

# 3. generate the fleet, once, from those jobs
python3 generate_configs.py 20 10 ./config_swarm_multi.yml configs mesh localhost 0 \
    --dtns --skip-jobs --seed 42 --size-to-jobs replay_jobs/

# 4. run, as often as you like — the same fleet every time
python run_test.py --mode local --agent-type resource --agents 20 --topology mesh \
    --db-host localhost --run-dir runs/replay --pegasus-jobs-dir replay_jobs/ \
    --use-config-dir --config-dir configs
```

The bundle is self-contained apart from the container image: job records, executables in
`code/`, root inputs in `inputs/`, and `manifest.json` with a sha256 per file. The image is
referenced rather than copied — it is gigabytes, and a bundle is copied around — so it is staged
to each agent's local disk separately and the manifest records its checksum. `--bundle-images`
does copy it in, for the rare case where a fully portable directory is worth the size.

### Real execution

`simulate` is the default, so nothing here changes an ordinary run. You need a **completed**
Pegasus run: the `*.stampede.db` carries the durations and exit codes, and the catalogs hold
absolute submit-host paths, so extraction and conversion happen there.

**Two ways to get a job its inputs, and the default changed on 2026-09-21.**

Both modes read the jobs' code, root inputs and image from somewhere every agent can reach.
They differ only in what happens to the files jobs **produce**.

| | **Staging (default)** | Shared mount (still supported) |
|---|---|---|
| Where a produced file lives | on the agent that produced it, plus a staging site | the NFS export, visible to all |
| How a consumer gets one | fetches it from the producer, or the site if the producer is gone | it is already there |
| Work dir | **local** to each agent | shared, same path everywhere |
| Data movement | real, measured, over the same paths consensus uses | none — every agent is equidistant from every produced file |
| Survives a dead producer | yes, from the staging site | yes, the export is elsewhere |
| Use it for | anything reporting data movement or locality | a quick functional check; it is simpler and has fewer moving parts |

Under the shared mount every agent looks equally close to every produced file, which is exactly
the variable the scheduler's DTN penalties price — so **no data-movement or locality number can
come from it.** Staging is the default for that reason. It is also the slower of the two, and
honestly so: measured on soilmoisture, the stage-out push cost about 0.7% of makespan.

**Staging does not remove the shared mount; it changes what the mount is for.** A job still
needs its *executable*, the DAG's *root inputs* and its *container image* before it can start,
and those come from somewhere every agent can read. What staging moves is the files jobs
**produce**. So the export stays, carrying read-only inputs to the workflow, and the image goes
to each agent's local disk because a multi-gigabyte `.sif` pulled over a WAN mount at every job
start would dominate every timing you take.

Pass the host list to both setup scripts if only part of the fleet will run workflow jobs —
without it they act on every host, and `--check` on either reports without changing anything.

```bash
# 1. once per slice, on the hosts that will run workflow jobs
sudo ./setup_apptainer.sh agent-1 agent-2 agent-3 agent-4 agent-5       # run the .sif natively
sudo ./setup_nfs_workflow.sh agent-1 agent-2 agent-3 agent-4 agent-5    # export for code+inputs
sudo ./setup_nfs_workflow.sh --stage-image /root/wf-images/<image>.sif \
     agent-1 agent-2 agent-3 agent-4 agent-5      # image to each agent's LOCAL disk

# 2. extract and convert, on the Pegasus submit host.
#    NOT --bundle-images: that copies a multi-GB image into the bundle, and into every copy of
#    it. Images are referenced and staged separately (step 1); the manifest records the
#    checksum either way.
python3 pegasus_profile_extractor.py --submit-dir <run dir>/     # → all_runs_jobs_profile.json
python3 pegasus_to_swarm_converter.py --input all_runs_jobs_profile.json --input-type json \
    --output-dir converted_jobs/ \
    --dag-gating --dtn-names local --dtn-scope job
    # --bundle-source-root <tree>   # only if the workflow tree has moved since the run

# 3. put the bundle on the export — the jobs' code and the DAG's ROOT inputs.
#    Produced files do NOT travel this way: that is what staging is for.
rsync -a --delete converted_jobs/ <db-host>:/export/swarm-wf/converted_jobs/
ssh <db-host> sudo chown -R nobody:nogroup /export/swarm-wf/converted_jobs

# 4. start the staging site ONCE, on a node every agent can reach (the database node).
#    Leave it running across runs: the store is keyed by (run, name), so one site backs a
#    whole campaign without runs colliding. Do NOT pass --run-id — run_test.py mints the run
#    id at launch (it ends in a uuid), so it is not knowable in advance.
#    The store keeps one directory per run and nothing prunes it, so sweep old runs between
#    campaigns if disk is tight.
python3 staging_site.py --store-dir /export/swarm-wf/store --port 21000
```

**5. Configure execution** on the database node, in `config_swarm_multi.yml`. Every per-agent
config is a copy of it, so this comes *before* step 6. Skip it and the jobs simulate — the one
failure here that looks like success:

```yaml
runtime:
  execution:
    mode: real                            # `simulate` is the default
    work_dir: /var/tmp/swarm-wf/work      # LOCAL per agent — a shared one makes every
                                          # fetch a no-op and measures nothing
    container_runtime: auto
    bundle: /export/swarm-wf/converted_jobs   # sets roots.code and roots.inputs
    roots:
      images: /export/images              # each agent's LOCAL disk, from step 1
    staging:
      enabled: true
      store_host: database                # the staging site from step 4
      store_port: 21000
```

`bundle` expands to the bundle's `code/` and `inputs/`; `roots.images` is set explicitly beside
it because the image is deliberately *not* in the bundle. An explicit `roots` entry always wins
over the bundle, so this pairing is the expected one rather than an exception.

<details>
<summary>Shared-mount alternative (no staging)</summary>

Simpler, and the path every result before 2026-09-21 was measured on. Step 1 is unchanged (the
export and the staged image are needed either way); skip step 4, and use a **shared** work dir
on the export instead of a local one:

```yaml
runtime:
  execution:
    mode: real
    work_dir: /export/swarm-wf/work       # shared, so job B finds job A's output
    container_runtime: auto
    bundle: /export/swarm-wf/converted_jobs
    staging:
      enabled: false
```

Every agent is then equidistant from every file. Do not quote a data-movement, locality or
makespan number from such a run.
</details>

```bash
# 6. generate the fleet, once
python3 generate_configs.py 5 10 ./config_swarm_multi.yml configs mesh database 0 \
    --skip-jobs --seed 42 --size-to-jobs /export/swarm-wf/converted_jobs \
    --agent-hosts-file agent_hosts.txt --agents-per-host 1

# 7. run
python3 run_test.py --mode remote --agent-type resource --agents 5 --agents-per-host 1 \
    --topology mesh --jobs-per-interval 4 --db-host database \
    --agent-hosts-file agent_hosts.txt --run-dir runs/soil-real \
    --pegasus-jobs-dir /export/swarm-wf/converted_jobs \
    --use-config-dir --config-dir configs --runtime 600
```

**`--agent-hosts-file` on step 6 is not optional.** Without it every agent advertises
`grpc.host` verbatim from the base config — `0.0.0.0` as shipped — and a peer dialling that
reaches its own localhost. Measured on the slice 2026-09-21: every consensus finalization in
such a run reads `reason=single-node`, meaning no agent ever reached another, and staging
locations are unusable for the same reason. With the flag each agent advertises `agent-N`,
which `/etc/hosts` resolves to its data-plane address.

**The run id is minted per run and every agent needs it.** `run_test.py` builds it from the
run directory, a timestamp and a uuid, exports it as `SWARM_RUN_ID`, and re-exports it over ssh
to each agent — so a normal remote run needs nothing from you. An agent started **by hand** with
staging on must export it too, and refuses at startup if it is missing: an agent that does not
know its run cannot tell a fetch for this run from one for a previous run's identically named
file, and this fleet has had agents outlive their run.

**Checking staging actually did something.** `[STAGE_OUT]` in an agent log gives bytes and
seconds per pushed file; `[STAGE_SITE]` on the site gives `stored` / `served` counts. If a
whole DAG lands on one agent, every child finds its parent's output locally and **nothing is
transferred** — that is a real outcome, not a failure, but it means the run measured no data
movement. Measured for soilmoisture on 5 agents: 5 outputs of 831 B–228 KB pushed in
0.035–0.097 s each, 0.244 s total against a 36.3 s makespan (~0.7%), and a 228 KB file served
from the site in 0.19 s with the producer unreachable.

### What the flags do

| flag | why |
|---|---|
| `--dag-gating` | jobs wait for their parents' output; without it a child fails on a file nobody has written |
| `--bundle-images` | copies the container image into the bundle. **Normally wrong**: images are gigabytes, and every copy of the bundle carries one. Stage the image to each agent's local disk instead (step 1) and let `roots.images` point at it; the manifest records its checksum either way |
| `--dtn-names local --dtn-scope job` | makes the jobs data-location-free. Right for both modes: under staging, *where* a file is comes from the location registry, not from a DTN name |
| `--bundle-source-root` | pass it (absolute, or `OLD=NEW`) only when the workflow tree has moved since the run |
| `--size-to-jobs` | every agent can run every job — so a failed agent's work can go to any other. It raises capacity to a *floor* and gives every agent the jobs' DTNs at its own connectivity score, so the fleet stays heterogeneous ([why](docs/WORKFLOW_EXECUTION.md#27-sizing-the-fleet-to-the-workflow)) |
| `--skip-jobs` | the job pool comes from the bundle; do not synthesize one |
| `--seed` | pins the fleet draw, so a regeneration reproduces it |
| `--use-config-dir` | reuse that fleet instead of redrawing it every run. Local reads `./configs` literally; remote copies `--config-dir` to each host |
| `--pegasus-jobs-dir` | publish the bundle as it is; `--jobs` comes from its record count |
| `rsync --delete` | a plain copy does not sweep, and leftovers from a bigger conversion get published too |
| `--agent-hosts-file` (step 6) | each agent advertises `agent-N` instead of the base config's `0.0.0.0`. Without it a peer dials its own localhost: consensus degrades to `single-node` and every staging location is unusable |
| `staging.enabled` | outputs stay on the producer and move on demand. **The default.** Needs a local `work_dir` |
| `staging.store_host` | the staging site an output is pushed to as it is produced, so it survives its producer. Empty means peer-only, which is **not durable** |

**Generate the fleet once.** Left to `run_test.py`, `configs/`, `agent_profiles.json` and
`agent_dtns.json` are redrawn every run, so two runs of the "same" cell are two different
fleets. Skipping `--size-to-jobs` is for when the fleet itself is the experiment — expect
`run_test.py` to refuse the run if some job then fits nowhere.

### Hierarchical fleets

Steps 1-5 are unchanged; steps 6 and 7 both differ. The agent count must match the fleet you
generated, and it has to be one of the presets. Note the hosts file is passed to **both**
commands, for different reasons: `generate_configs.py` needs it to give each agent the address
its peers dial, and `run_test.py` needs it to place them.

```bash
# 6. generate the fleet, once
python3 generate_configs.py 30 10 ./config_swarm_multi.yml configs hierarchical database 0 \
    --hierarchical-level1-agent-type resource --skip-jobs --seed 42 \
    --size-to-jobs /export/swarm-wf/converted_jobs \
    --agent-hosts-file agent_hosts.txt --agents-per-host 1

# 7. run
python3 run_test.py --mode remote --agent-type resource --agents 30 --agents-per-host 1 \
    --topology hierarchical --hierarchical-level1-agent-type resource \
    --jobs-per-interval 4 --db-host database --agent-hosts-file agent_hosts.txt \
    --run-dir runs/wf-hier --pegasus-jobs-dir /export/swarm-wf/converted_jobs \
    --use-config-dir --config-dir configs --runtime 900
```

Three things to know:

- The converter's `--generate-agent-configs` writes flat topologies only, so this is the one
  route to a hierarchical fleet.
- Coordinators are `resource` by default and do **not** follow `--agent-type`: an all-LLM
  hierarchy needs `--hierarchical-level1-agent-type llm` on both commands, and every
  coordinator host then needs `OPENAI_API_KEY`.
- No DTN pool is generated for hierarchical, so convert with `--dtn-names local` or let
  `--size-to-jobs` attach the names the jobs use.
- Every host that can receive a job needs step 1 done to it — apptainer, the export and the
  staged image. A 30-agent hierarchical fleet is 30 hosts, not the 5 the flat example uses.

Supported fleet sizes are presets — 30, 60, 80, 90, 100, 110, 120, 250, 270, 990, 1000 — and
anything else is refused. Jobs enter at the top tier and are delegated down.

### Validated

FABRIC slice, 5 agents, mesh, DAG order respected, image on local disk:

| job | SWARM | Pegasus `remote_duration` |
|---|---|---|
| analyze_moisture | 0.915s | 1.029s |
| train_model | 14.889s | 14.863s |
| predict_irrigation | 3.772s | 4.487s |
| visualize_moisture | 2.219s | 2.459s |

All exit 0, outputs byte-identical to the Pegasus run. Read
[the limits](docs/WORKFLOW_EXECUTION.md#4-known-limits) before using it as a comparison: no
stage-in, NFS flattens data locality, the substrates differ.

`run_test.py` refuses to start on leftover records from an earlier conversion, file-name
collisions between workflows, or a fleet where no agent can run some job. At run time, a job
that cannot execute fails loudly at `ERROR` in `swarm-multi/agent-<id>.log` — it never falls
back to simulating.

### Several workflows

One bundle and one run per workflow is the safe default: each run scopes its own working
directory and readiness registry. Combining them works only if their file names are disjoint —
logical names are not namespaced, and the DAG, the readiness registry and the shared working
directory all key on them, so two workflows both writing `output.csv` get an edge nobody wrote.
The converter reports it and `run_test.py` refuses such a bundle. Renaming is not a fix: the
names are in the jobs' command lines. See
[WORKFLOW_EXECUTION.md §2.8](docs/WORKFLOW_EXECUTION.md).

## Results and plots

Every run writes `metrics.json`, `run_meta.json`, per-agent logs and plots under `--run-dir`.
All plotting lives in the `plotting/` package; the top-level scripts are thin CLI wrappers
(`--help` on any of them).

```bash
python plot_latency_jobs.py --output_dir runs/test-001 --agents 30 --db_host localhost [--hierarchical]
python plot_multi_run_results.py --base-dir runs/single-site --output-dir runs/single-site/plots
python plot_comparison.py --swarm-dir runs/swarm --greedy-dir runs/greedy --output-dir runs/comparison
python plot_mab_results.py --db-host localhost --output-dir runs/mab-test
```

Published evaluation data for **SWARM (CCGrid'25)** and **SWARM+ (eScience'26)** is
[here](https://github.com/swarm-workflows/swarm-evaluation-data).

## Configuration

Runs are driven by `config_swarm_multi.yml`; `generate_configs.py` derives one config per agent
from it. The knobs you are most likely to touch:

| key | what it does |
|---|---|
| `job_selection.cost_weights` | CPU/RAM/disk/GPU weights in the cost function (sum ≈ 1.0) |
| `job_selection.designate_bidder` | whether one agent per job bids, instead of every agent that thinks it is cheapest. **LLM agents only** — resource agents do not read it |
| `consensus.protocol` | `pbft`, `snow`, or `hybrid`; Snow tuning under `consensus.snow.*` |
| `failure_detection.protocol` | `heartbeat` or `swim` (SWIM runs alongside heartbeat) |
| `gossip.enabled` | epidemic state dissemination, so peer load reaches cost estimates |
| `runtime.wall_time_scale` / `_min_s` / `_max_s` | how a simulated job's sleep maps to its recorded wall time |
| `runtime.reselection_timeout_s` | how long a job may sit in consensus before going back to PENDING |
| `runtime.execution.*` | real execution: `mode`, `work_dir`, `bundle`, `roots`, `path_rewrites` |
| `mab.algorithm` / `delegation.policy` | who picks the child group a coordinator delegates to |
| `llm.provider` / `llm.model` | LLM backing for `--agent-type llm` |

Unknown values raise rather than silently defaulting, and duplicate keys are rejected on load.

## How it works

Five layers, each usable on its own:

| layer | where | does |
|---|---|---|
| Agent | `swarm/agents/` | scores jobs, runs them, owns all side effects |
| Consensus | `swarm/consensus/` | PBFT (`engine.py`) or Snow (`gossip_engine.py`) agreement on assignments |
| Selection | `swarm/selection/` | cost matrix over candidate jobs and agents, with caching |
| Communication | `swarm/comm/` | gRPC transport (`consensus.proto`) |
| Data | `swarm/database/` | Redis persistence for jobs, agents and consensus state |

Agents talk to the consensus and selection engines through adapters, so the engines stay
framework-agnostic. Topologies (ring, mesh, star, hierarchical) live in `swarm/topology/`, SWIM in
`swarm/membership/`, gossip in `swarm/gossip/`, bandits in `swarm/rl/`.

[ARCHITECTURE.md](docs/ARCHITECTURE.md) has the job lifecycle, the cost formula, the threading
model and the invariants; [COMPLEXITY.md](docs/COMPLEXITY.md) the message complexity;
[GOSSIP_CONSENSUS_DESIGN.md](docs/GOSSIP_CONSENSUS_DESIGN.md) the Snow/SWIM/gossip stack.

Debugging a stuck run: set `log-level: DEBUG`, then inspect Redis with
`python dump_db.py --host localhost --type redis`. For PBFT, check `engine.conflicts` and the
quorum; for Snow, grep for `[snow] consensus engine started` and `[SNOW_LEADER]`, and confirm the
exactly-once claims (`repository.try_claim_assignment`).

If you change `swarm/comm/consensus.proto`:

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. swarm/comm/consensus.proto
```

## Utilities

| script | purpose |
|---|---|
| `generate_configs.py` | per-agent configs for a topology and fleet size |
| `job_generator.py` | synthetic jobs matching the agent profiles |
| `job_distributor.py` | feed jobs into Redis at a controlled rate |
| `pegasus_profile_extractor.py` | extract job profiles from a Pegasus run (submit host) |
| `pegasus_to_swarm_converter.py` | turn those profiles into a runnable job bundle (+ agent configs) |
| `make_agent_hosts.py` | build the hosts file for a remote run |
| `dump_db.py` | inspect Redis state |
| `kill_agents.py` | simulate agent failures |
| `setup_apptainer.sh`, `setup_nfs_workflow.sh` | prepare a slice for real execution |

## Documentation

**Architecture** — [ARCHITECTURE.md](docs/ARCHITECTURE.md) ·
[COMPLEXITY.md](docs/COMPLEXITY.md) ·
[GOSSIP_CONSENSUS_DESIGN.md](docs/GOSSIP_CONSENSUS_DESIGN.md) ·
[DECENTRALIZED_POOL_DESIGN.md](docs/DECENTRALIZED_POOL_DESIGN.md)

**Workflows** — [PEGASUS_TO_SWARM.md](docs/PEGASUS_TO_SWARM.md) (replay) ·
[WORKFLOW_EXECUTION.md](docs/WORKFLOW_EXECUTION.md) (real execution)

**Quantum** — [QUANTUM_HYBRID_DESIGN.md](docs/QUANTUM_HYBRID_DESIGN.md) ·
[QUANTUM_HYBRID_IMPLEMENTATION.md](docs/QUANTUM_HYBRID_IMPLEMENTATION.md)

**Hierarchy and delegation** — [HIERARCHICAL_LLM_AGENTS.md](docs/HIERARCHICAL_LLM_AGENTS.md) ·
[CO_PARENT_USAGE.md](docs/CO_PARENT_USAGE.md) · [MAB_README.md](docs/MAB_README.md) ·
[CONTEXTUAL_BANDIT_DESIGN.md](docs/CONTEXTUAL_BANDIT_DESIGN.md)

**Baselines and planning** — [DISTRIBUTED_BASELINE_DESIGN.md](docs/DISTRIBUTED_BASELINE_DESIGN.md) ·
[ROADMAP.md](docs/ROADMAP.md)
