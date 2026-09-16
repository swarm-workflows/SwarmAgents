# Running real workflows: executables, containers, and what it takes to compare against Pegasus

Status: **working end to end** on the FABRIC slice as of 2026-09-16. A real Pegasus workflow's
jobs execute in their own container, produce byte-identical outputs, and are timed against the
Pegasus run they came from.

Until now `Job.execute()` slept for the job's (scaled) wall time. That is the right default and
is where every measured number in this project comes from — it is enough to study ordering,
placement and consensus, which is what SWARM is about. It is not enough to answer the question
this document exists for: **run the same workflow on Pegasus and on SWARM, and compare.**

Simulation remains the default. Nothing here changes a run that does not ask for it.

---

## 1. The shape of the pipeline

```
Pegasus submit host                    SwarmAgents
───────────────────                    ───────────
*.stampede.db   ─┐
workflow.yml     ├─> pegasus_profile_extractor.py ──> profiles JSON
transformations.yml ┘                                      │
                                                           v
                                        pegasus_to_swarm_converter.py
                                                           │
                                                   job_N.json  (execution block)
                                                           v
                                              Job  ──>  swarm/execution/runner.py
                                                           │
                                                   apptainer exec … / docker run …
```

Four things have to travel, and **each lives in a different place**. Getting this wrong is the
main way the pipeline fails, so it is worth stating precisely.

| what | where it lives | why not the obvious place |
|---|---|---|
| in-container executable path | `invocation.executable` (stampede DB) | — |
| host path of the code (`pfn`) | transformation catalog | the DB has no idea where the code came from |
| container image | transformation catalog `containers:` | not in the stampede DB at all |
| **arguments** | **abstract `workflow.yml`, `jobs[].arguments`** | **`invocation.argv` is empty for every compute job** |

### 1.1 Two paths, not one

`invocation.executable` is `/srv/analyze_moisture` — a path **inside the container**. The
catalog's `pfn` is `/home/ubuntu/soilmoisture-workflow/bin/analyze_moisture.py`, the host path
of the code that was staged there. Neither alone can re-run the job: the pfn says what code to
ship, the in-container path says what to invoke once it is shipped.

This is not a theory about Pegasus. `/srv/` in the soilmoisture image is **empty** — checked
directly. The code is not baked in; Pegasus stages it at run time. So the runner bind-mounts
the resolved pfn onto `spec.path`, which reproduces what Pegasus did and keeps the recorded
executable path meaningful rather than decorative. An `installed` transformation is already in
the image and is *not* bound over.

### 1.2 Arguments are in the abstract workflow

`invocation.argv` looks like the obvious source and is empty for every compute job in these
workflows — measured on the soilmoisture run. The real command line is declared in
`workflow.yml` under `jobs[].arguments`, keyed by the same abstract job id the extractor
already uses to resolve input/output files.

This was found the hard way, and the failure is worth remembering because it is what a
half-correct pipeline looks like: the container started, the bind mount worked, the code ran —
and printed `error: the following arguments are required: --polygons-file`. Everything
structural was right and the job was still not the job Pegasus ran.

A Pegasus argument may also be a `File` object rather than a string; it round-trips through
YAML as a mapping, and `str()` on it would put a Python dict repr on the command line, so the
`lfn` is taken.

### 1.3 Clustered jobs are refused, not merged

A clustered Condor job bundles several tasks, and Pegasus runs them as **separate sequential
invocations**. There is no single `(executable, argv)` pair that describes such a job: the
executable is taken from the first task, so concatenating every task's arguments would run
task A's binary with A's and B's flags together, the later overriding the earlier. Two tasks
chained by a file become one invocation with two `--input` flags — which *executes*, produces
output, and is a job that never existed. That is the worst outcome available to a comparison,
so `argv` is set to `None` (the "unknown" value `runnable()` already rejects) and the job
schedules normally but declines to execute. Input and output files are still unioned, which is
correct — a cluster really does consume and produce all of them. `clustered_tasks_db` on the
profile distinguishes this from arguments that genuinely could not be parsed.

The clustering signal is taken from the **stampede DB** (`cluster_task_ids`), never from the
abstract-workflow map. Counting the ids that resolved against `workflow.yml` fails *open*:
that list is filtered by what the workflow map contains, so a run with no `workflow.yml`
(absent file, or PyYAML not installed) yields an empty list for every job, no cluster is
recognised, and the first task's recorded argv executes as though it described the whole
cluster. Missing metadata means we know *less* about a job, which can only make executing it
less safe, never more.

The task id list is also **de-duplicated**, in invocation order. One job instance can carry
several invocation rows for the same abstract task; arguments are accumulated per entry and
deliberately not de-duplicated (an argument list is ordered and a value may legitimately
repeat), so a duplicate row doubles the command line — and since it is still one *distinct*
task, the cluster guard correctly does not fire and the doubled command line runs. The two
safeguards are therefore different functions on purpose: `cluster_task_ids` (a set, for the
guard) and `ordered_task_ids` (order-preserving and unique, for accumulation).

---

## 2. Running it

### 2.0 Getting the workflow

`soilmoisture` is public: `git clone https://github.com/pegasus-isi/soilmoisture-workflow.git`.
The clone carries the five executables, `polygons.json`, and both container recipes. Three
things it does not carry, because each is generated rather than committed: the catalogs
(`workflow.yml`/`transformations.yml`/`replicas.yml`, from `workflow_generator.py`, and they
bake in **absolute** pfn paths), the built `.sif` (from
`Apptainer/SoilMoisture_Container.def`), and any `*.stampede.db`.

The stampede DB is the one that cannot be produced from the clone at all. **This pipeline's
input is a completed Pegasus run, not a workflow definition** — the DB is where per-job
durations, exit codes and the baseline makespan come from, and comparing against a run means
having that run. Cloning and planning is not a substitute.

One consequence worth knowing: because the catalogs are generated with absolute paths,
generating them *on the machine where the code will live* makes `path_rewrites` unnecessary.
Rewrites exist for the normal case, where the run happened somewhere else.

### 2.1 Extract and convert

```bash
# On the Pegasus submit host
python3 pegasus_profile_extractor.py \
    --submit-dir ~/soilmoisture-workflow/ubuntu/pegasus/soilmoisture/run0001 \
    --output soil_profiles.json

# On the SwarmAgents side
python pegasus_to_swarm_converter.py --input soil_profiles.json \
    --input-type json --output-dir converted_jobs/ --dag-gating
```

`--dag-gating` reconstructs the dependency graph as per-job `data_predicate`s; see
`CLAUDE.md` and `tests/test_workflow_dag.py`. Check `conversion_summary.json`'s `dag.edges`
and `dag.roots` — a partial DAG still runs and looks healthy.

### 2.2 Slice setup

```bash
sudo ./setup_apptainer.sh               # all 92 agents; verifies by RUNNING a container
sudo ./setup_nfs_workflow.sh            # shared work dir at an identical path everywhere
```

Both exit non-zero on a partial fleet. Both verify by doing the thing, not by asking whether a
binary or a mount exists — see §5.

### 2.3 Config

```yaml
runtime:
  execution:
    mode: real
    work_dir: /export/swarm-wf/work
    container_runtime: auto
    timeout_s: 3600.0
    path_rewrites:
      - from: /home/ubuntu/soilmoisture-workflow
        to:   /export/swarm-wf/workflows/soilmoisture-workflow
      - from: /home/ubuntu/soilmoisture-workflow/Apptainer
        to:   /root/wf-images
```

### 2.4 Roots — for jobs you author yourself

`path_rewrites` exists because a Pegasus catalog records **absolute** submit-host paths that do
not exist on this fleet. A job written by hand has no such history and should not have to
invent one. For those, say where the three kinds of thing live:

```yaml
runtime:
  execution:
    roots:
      code:   /export/swarm-wf/workflows/soilmoisture-workflow
      inputs: /export/swarm-wf/inputs
      images: /root/wf-images
```

and name them relatively:

```json
"data_in":   [{"name": "local", "file": "field1_soil_data.csv"}],
"execution": {"path": "/srv/analyze_moisture",
              "pfn":  "bin/analyze_moisture.py",
              "arguments": ["--input", "field1_soil_data.csv", "--output", "out.json"],
              "container": {"kind": "singularity", "image": "SoilMoisture_Container.sif"}}
```

**The rule is one line:** an absolute path is used as written (so every Pegasus-derived job
keeps resolving through `path_rewrites` exactly as before), a relative one resolves under its
root. A relative path with no root configured is a **refusal naming the missing key** — never
a fall back to the process's working directory, which would run whatever happened to sit
there.

One exception, and it is not arbitrary: **a bare container image reference is not a path**.
`ubuntu:22.04` and `repo/img:1` are registry references that the runtime resolves itself,
while `Soil.sif` is a file in the images root, and neither has a scheme or a leading slash to
tell them apart. The catalog's own `kind` decides — `docker` means registry, `singularity`
means file — with an image-file suffix (`.sif`, `.simg`, `.img`, `.sqsh`) as the tiebreak when
the kind is absent or wrong. Resolving every bare name as a path breaks every catalog that
names a plain docker tag; resolving every bare name as a registry reference sends a
hand-authored `.sif` to a registry.

Once classified, the **scheme is then adjusted per runtime, in both directions**. Docker
wants a bare `repo:tag` and has any `docker://` removed; apptainer *requires* the scheme and
has one added, because it reads a bare reference as a local file name. Measured on the slice:
`apptainer exec busybox:latest …` fails with
`could not open image /home/ubuntu/busybox:latest`, while `docker://busybox:latest` runs. A
resolved `.sif` is an absolute path by that point and is left alone.

Before adding the scheme the runner **checks the filesystem**, because a suffix is a hint and
not proof. An apptainer *sandbox* is a directory with no extension at all
(`apptainer build --sandbox mybox/ …`, then `apptainer exec mybox/ …`), and an image in the
images root need not be named `.sif`. Prefixing either turns a local image into a registry
pull for a repository nobody published. Existence is checkable, so only a reference that
resolves to nothing on disk is treated as a registry reference — and it is checked **only
under the configured root**, never against the agent's own working directory, or an unrelated
file that happens to share the name would be run instead of the image.

Relative paths are also **contained**: a job record is workflow-supplied data, and
`../../usr/bin/something` joined onto a root escapes it, which defeats the point of naming a
root. `resolve_under_root` is the single place every relative path passes through, so the
check lives there and covers code, inputs and images at once. It is lexical (`normpath`),
which is what defeats `..`; a symlink *inside* a root is placed by whoever administers it and
is deliberately still followed.

**There is no new field for inputs.** The files a job reads are already `Job.data_in`, which
the converter populates; a second list would be a second source of truth for the same fact and
the two would drift. The runner stages `data_in` into the working directory before the job
starts, under three rules that each prevent a quiet corruption:

* **Never overwrite.** A file already in the working directory is a parent job's output or
  another agent's copy. Replacing the first with a stale replica corrupts a DAG in the most
  confusing way available — the parent ran, the child read something else.
* **Copy atomically, and create exclusively.** The copy goes to a temporary name in the same
  directory (the working directory is shared and several agents stage concurrently; a
  half-written file is readable and looks complete), and the destination is then created with
  `os.link`, **not** `os.replace`. The existence check above is check-then-act: a parent job
  can finish *during* the copy, and a replace would overwrite that fresh result with a stale
  replica — the never-overwrite rule defeated through a window rather than directly.
  `os.link` fails when the destination exists, which makes the rule atomic rather than
  merely intended.
* **Refuse a missing input.** A job without its input usually does not fail — it writes empty
  or default output, which is indistinguishable from a real result until someone checks the
  numbers.

Only files the run does *not* produce belong in the inputs root. An intermediate is produced
by its parent into the shared working directory.

Staging reads `DataNode.file`, never `DataNode.name` — on a data node **`name` is the site**
(`local`, `dtn3`) and `file` is the logical file name. A per-site conversion carries no `file`
at all, so its nodes describe where data lives and have nothing to stage; they are skipped
rather than given an invented name. Per-file conversion, which `--dag-gating` already forces,
is what produces stageable nodes. A declared name is reduced to its basename, so a workflow
cannot name a path that escapes the working directory.

**Two rewrites, deliberately.** A catalog's paths are all under the workflow root, but the
code and the image want to live in different places: code on the shared export (small, and
every agent must see the same bytes), the multi-gigabyte image on each agent's **local disk**.
Longest prefix wins, so the image rule takes precedence regardless of the order they are
written in — which is exactly why the rule exists.

---

## 3. What has been validated

Soilmoisture, 5 compute jobs, run on the slice against the original Pegasus run:

| job | SWARM | Pegasus `remote_duration` | outputs match |
|---|---|---|---|
| analyze_moisture | 0.92 s | 1.029 s | byte-identical (2073 B) |
| train_model | 12.94 s | 14.863 s | byte-identical (222002 B) |
| predict_irrigation | 3.77 s | 4.487 s | byte-identical (1270 B) |
| visualize_moisture | 2.17 s | 2.459 s | 228450 vs 228268 B (PNG) |
| fetch_soil_data | — | 2.038 s | not run, see §4.1 |

The PNG differs by 182 bytes: matplotlib renders text with whatever fonts the host offers.
Everything computational is byte-identical, which is the strong form of "the same job ran".

Two things this does *not* yet say:

* **The substrate differs.** Pegasus ran on its own condorpool; SWARM ran on a FABRIC VM. The
  per-job times are close, but they are not a controlled comparison, and the project's own rule
  is to re-measure every published number on one substrate.
* **Makespan is not comparable yet.** Pegasus's makespan was 351 s against ~21 s of actual
  compute — it is dominated by Condor queueing (20–47 s per job) and staging (26 s in, 13 s
  out). A SWARM makespan will look dramatically better for reasons that have nothing to do
  with scheduling quality until staging exists on both sides.

---

## 4. Known limits

### 4.1 No stage-in, so a root input must be placed by hand

The runner has no staging step; `execute()`'s two staging TODOs are still TODOs. Jobs run with
the **shared** work dir as cwd, and `data_in`/`data_out` are bare logical names, so job B finds
job A's output because both ran in the same directory. What nothing provides is the workflow's
*declared replicas* — files listed in `replicas.yml` that no job produces. Copy them into the
work dir before the run.

`fetch_soil_data` additionally calls a live external API, and
`archive-api.open-meteo.com` is **unreachable from the slice** (generic HTTPS is fine —
GitHub answers in 0.2 s — that one host times out). Its real output from the original run
stands in for it. A job whose runtime is an external service's latency is a poor comparison
subject anyway.

### 4.2 NFS is scaffolding

`setup_nfs_workflow.sh` makes execution possible without building distributed staging first.
It is not a configuration any published number comes from: it removes transfer time, makes
every agent equidistant from the data (flattening exactly the locality the scheduler exists to
exploit), and is a WAN mount, so job wall times include transatlantic latency. Real staging
replaces it.

### 4.3 The image is big

3.1 GB for soilmoisture. It belongs on each agent's local disk (`--stage-image`), never on the
export — a WAN read of that per job start would dominate every measurement taken.

---

## 5. Rules this code follows, and why

**Refuse rather than approximate.** Running *something* when the declared thing is unavailable
produces a number that looks like a result and is not one. So: a missing container runtime is a
refusal, never a fallback to running bare on the host — that is the one failure mode that
produces a *passing* job while silently redefining what was measured as "whatever this host has
installed". An unresolvable executable is a refusal, never a basename search: two workflows can
both have a `process.py`, and the wrong one would run and produce plausible output.

**A refusal fails the job; it never falls back to the sleep.** A run silently mixing executed
and simulated jobs, with nothing in the results telling them apart, is worse than a run that
stops.

**The real exit status wins over `should_fail`.** That flag replays what the job did on someone
else's cluster months ago. Letting it win would turn a measurement back into a recording while
looking perfectly healthy.

**Empty is not unknown.** `arguments: []` means the command line had none; `None` means it
could not be parsed. `runnable()` refuses `None`. Collapsing the two would run a different
command than the one being compared against.

**Verify by doing the thing.** An `apptainer --version` that cannot start a container, and an
NFS mount that cannot be written to, are the same class of healthy-looking failure — this repo
has been bitten by that shape three times now. Both installers run a real container / a real
write, and both refuse to report success for a partial fleet, because a partial setup stays
invisible until a job lands on the host nobody remembers skipping.

---

## 6. What real execution still needs

1. **Stage-in / stage-out**, so declared replicas arrive and outputs leave without a shared
   filesystem. This is the last thing standing between the current setup and a defensible
   makespan comparison.
2. **A controlled substrate** — Pegasus and SWARM on the same hardware — before any published
   number.
3. **Per-job resource enforcement.** The catalog carries `memory`/`cores` requests; the runner
   does not pass them to the container runtime, so a job can exceed what it asked for.
4. **Clustered jobs**, which currently refuse (§1.3). Supporting them means representing a job
   as an ordered list of invocations rather than one command line.

Tests: `tests/test_workflow_execution.py` (27), `tests/test_real_execution.py` (38).
Related: `docs/QUANTUM_HYBRID_DESIGN.md` for the other execution path (`_execute_quantum`).
