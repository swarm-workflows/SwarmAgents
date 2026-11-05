# Running real workflows: executables, containers, and what it takes to compare against Pegasus

Status: **working end to end** on the FABRIC slice as of 2026-09-16. A real Pegasus workflow's
jobs execute in their own container, produce byte-identical outputs, and are timed against the
Pegasus run they came from.

Until now `Job.execute()` slept for the job's (scaled) wall time. That is the right default and
is where every measured number in this project comes from — it is enough to study ordering,
placement and consensus, which is what SWARM is about. It is not enough to answer the question
this document exists for: **run the same workflow on Pegasus and on SWARM, and compare.**

Simulation remains the default. Nothing here changes a run that does not ask for it.

**The step-by-step runbook is in the [README](../README.md#real-execution)** — six commands, in
order, for a workflow you just want to run. This document is the reference behind it: where each
piece of a Pegasus job lives, what the bundle is, what the runner refuses and why, what has been
validated, and what you must not quote a number from yet.

| | |
|---|---|
| [1. The shape of the pipeline](#1-the-shape-of-the-pipeline) | the four places a job's parts live, and why two paths are needed rather than one |
| [2.1 What you need, and where](#21-what-you-need-and-where) | the completed Pegasus run, and which machine does what |
| [2.2 Slice setup](#22-slice-setup-once) | apptainer and the shared work dir |
| [2.3 Extract and convert](#23-extract-and-convert-on-the-submit-host) | the two commands, flag by flag |
| [2.4 The bundle](#24-the-bundle-is-the-deliverable) | what the output directory holds, and why it is a bundle rather than paths |
| [2.5 Configuring execution](#25-configuring-execution) | `runtime.execution`, and what `path_rewrites` is still for |
| [2.6 Running it](#26-running-it) | `--pegasus-jobs-dir`, and what is refused before the run starts |
| [2.7 Sizing the fleet](#27-sizing-the-fleet-to-the-workflow) | jobs the fleet cannot run are never scheduled and never fail |
| [2.8 Several workflows](#28-several-workflows) | file names are the one thing a bundle does not namespace |
| [2.9 Roots and image resolution](#29-roots-and-image-resolution--for-jobs-you-author-yourself) | for jobs written by hand; the full image-reference matrix |
| [3. What has been validated](#3-what-has-been-validated) | the soilmoisture numbers, and what they do not yet say |
| [4. Known limits](#4-known-limits) | read before quoting anything |
| [5. Rules this code follows](#5-rules-this-code-follows-and-why) | refuse rather than approximate, verify by doing |
| [6. What still needs building](#6-what-real-execution-still-needs) | staging, a controlled substrate, per-workflow work dirs |

---

## 1. The shape of the pipeline

```
Pegasus submit host                              SwarmAgents fleet
───────────────────                              ─────────────────
*.stampede.db   ─┐
workflow.yml     ├─> pegasus_profile_extractor.py ──> profiles JSON
transformations.yml ┘                                      │
                                                           v
                                        pegasus_to_swarm_converter.py
                                                           │
                                     converted_jobs/  (job records + code/ inputs/ images/)
                                                           │
                                        rsync -a --delete  │
                                                           v
                                              run_test.py --pegasus-jobs-dir
                                                           │
                                                   Job  ──>  swarm/execution/runner.py
                                                           │
                                                   apptainer exec … / docker run …
```

Both extraction and conversion run **on the submit host** — that is where the catalogs'
absolute paths resolve, so it is the only machine that can collect the code, the inputs and the
image into one directory. What crosses to the fleet is that directory.

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

### 2.1 What you need, and where

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

One consequence shapes everything below: because the catalogs carry absolute submit-host paths,
they resolve on the submit host and nowhere else. So the conversion runs *there* and produces a
self-contained bundle (§2.4) — which is why no path on this fleet has to be mapped back to one
on that one.

That splits the work across three machines:

| machine | does | needs |
|---|---|---|
| Pegasus submit host | extract, convert, size the fleet (§2.3) | `pegasus_profile_extractor.py`, `pegasus_to_swarm_converter.py`, `config_swarm_multi.yml`, Python 3 — no Redis, no agents, no rest of the repo |
| database node | holds Redis and the shared export, launches the run (§2.6) | the repo, the bundle copied in, the execution config (§2.5) |
| agent hosts | run the jobs | apptainer and the shared mount (§2.2); the bundle is read from the export |

### 2.2 Slice setup (once)

```bash
sudo ./setup_apptainer.sh               # all 92 agents; verifies by RUNNING a container
sudo ./setup_nfs_workflow.sh            # shared work dir at an identical path everywhere
```

Both exit non-zero on a partial fleet. Both verify by doing the thing, not by asking whether a
binary or a mount exists — see §5.

### 2.3 Extract and convert, on the submit host

Two commands, and everything after them is a copy.

```bash
python3 pegasus_profile_extractor.py \
    --submit-dir soilmoisture-workflow/ubuntu/pegasus/soilmoisture/run0001/ \
    --output soil_profiles.json

python3 pegasus_to_swarm_converter.py \
    --input soil_profiles.json --input-type json \
    --output-dir converted_jobs/ \
    --dag-gating \
    --dtn-names local --dtn-scope job
```

* `--dag-gating` reconstructs the dependency graph as per-job `data_predicate`s (see
  `tests/test_workflow_dag.py`). It forces `--data-nodes per-file`, because per-site collapses
  a job's inputs and loses edges. Without it nothing waits for its parents and a child fails on
  a file that has not been written.
* `--bundle-images` is deliberately **not** used: it copies the container image into the
  bundle, and into every copy of the bundle, and images are gigabytes. Stage the image to each
  agent's local disk instead (§4.3) and point `roots.images` at it; the manifest records its
  checksum either way. Use `--bundle-images` only when a fully portable directory is worth the
  size.
* `--dtn-names local --dtn-scope job` makes the jobs data-location-free — `local` is excluded
  from a job's required DTNs. Right in both modes: under staging, *where* a produced file lives
  comes from the location registry, not from a DTN name.
* `--bundle-source-root` is only needed when converting somewhere the recorded paths do not
  resolve. It is a prefix replacement, and a relative value resolves against the current
  directory, so pass it absolute.

Then read `conversion_summary.json` → `dag.edges`, `dag.roots` and `dag.colliding_outputs`, and
`manifest.json` → `missing`. A partial DAG and an incomplete bundle both look healthy from the
outside.

### 2.4 The bundle is the deliverable

> The output directory is the unit you copy, and the reason it is a directory of files rather
> than a set of paths. Skip to §2.5 if you only need to run it.

The converter writes a **self-contained bundle**: job records plus the executables they run
plus the root inputs they read. Copy the directory anywhere, point one config key at it, and
the jobs run.

```
converted_jobs/
  job_1.json …            the job records; `pfn` is relative to code/
  code/<transformation>/  the executables, one copy per transformation
  inputs/                 declared replicas — the files no job produces
  manifest.json           what was copied, from where, and its sha256
  conversion_summary.json
  pegasus_baseline.json   the Pegasus timings to compare against
```

```yaml
runtime:
  execution:
    bundle: /export/swarm-wf/converted_jobs
    roots:
      images: /root/wf-images        # only when the image is NOT bundled, or is staged local
```

Copying it onto the shared export is the whole hand-off — there is no workflow tree to stage
and no `path_rewrites` to work out:

```bash
rsync -a --delete converted_jobs/ <swarm-db-host>:/export/swarm-wf/converted_jobs/
sudo chown -R nobody:nogroup /export/swarm-wf/converted_jobs      # on the database node
```

`--delete` is load-bearing. The converter replaces its own output directory, but a copy does
not sweep: restaging a 4-job workflow into a directory holding a previous 400-job conversion
leaves 404 records and the distributor publishes all of them. `run_test.py` compares the record
count against `conversion_summary.json` and refuses the mismatch, so this fails loudly — but
`--delete` is what avoids it. `rsync -a` (or `tar`) also matters for the **mode bits**: nothing
on the agent side chmods or checks the executables.

Why this rather than paths into a staged tree:

* **A path says where code was, not which code it was.** Edit a script after the run and a
  replayed job silently executes a different program than the one that produced the baseline.
  Every bundled file is checksummed in `manifest.json`, so what ran can be compared with what
  was measured.
* **Nothing to stage by hand.** Root inputs — the files a workflow declares but no job
  produces — come from the replica catalog, which nothing recorded before. They land in
  `inputs/` and the runner stages them into the working directory itself.
* **A partial bundle says so**, on stdout and in `manifest.json`, instead of looking whole and
  failing per job later on whichever agent drew it.

Images are **referenced, not copied** (`--bundle-images` overrides): they are gigabytes and a
bundle is meant to be copied around. Their checksums are recorded either way, so the reference
is verifiable without being carried.

`--bundle-source-root` maps the submit-host tree onto a local copy when the converter runs
somewhere the profiles' absolute paths do not resolve. It is a **prefix replacement**, not a
search: the `OLD=NEW` form states the mapping outright, and a bare root anchors on the common
parent of everything being bundled. A path that does not resolve is reported missing rather
than guessed at — an earlier version tried progressively shorter suffixes and took the first
that existed, which matches on the *basename* at its last step, so two workflows' `process.py`
both resolved to the same file and quietly undid the pfn keying that keeps them apart. With a
single source directory the bare form is genuinely ambiguous (the common parent *is* that
directory), so it resolves deterministically and the refusal names the `OLD=NEW` form.

The anchor is computed over **every** path the mapping is applied to — executables, replicas
*and* images. Leaving images out made it too deep as well as missing them: with all code in
`/wf/bin` the common parent was `/wf/bin`, so `/wf/Apptainer/x.sif` fell outside it and the
code itself resolved one directory too high.

When a mapping is configured it is tried **first**. Checking the recorded path before it let
an incidental file at the submit-host location win over the tree the caller explicitly named,
so converting on any machine that happens to have `/home/ubuntu/…` bundled that instead — and
an explicit `OLD=NEW` was ignored for every path that happened to exist locally. Without a
mapping the recorded path is used as before, which is the common case of converting on the
submit host.

Two replicas sharing a basename are **reported, not overwritten**. The working directory is
flat, so they genuinely cannot both be staged; copying the second over the first while the
manifest claims both are bundled hands a job the wrong file silently.

A conversion **replaces** the previous bundle: job records left over from a larger earlier
conversion would otherwise be published alongside the new ones (a 4-job workflow written over
a 400-job run left 406 files, all of which got scheduled). **Nothing is removed until the replacement is installed.** The whole conversion — job
records, payload, baseline, manifest, summary — is written to a staging directory; then each
entry is moved into place, displacing its predecessor by renaming it aside first, with a
rollback that restores everything if any step fails; and only then is the surplus from a
larger previous conversion swept, skipping what was just installed.

The ordering took three attempts, and the two wrong ones are worth naming because they look
right. Clearing at the *start* loses everything if the conversion then fails. Staging only
the payload and clearing just before promoting moves the hole one step along — the job files
were still written into the output directory afterwards. And clearing immediately before
promotion is still wrong, because promotion moves several entries and can fail between them.
A conversion that dies leaves its staging directory behind rather than damaging anything;
the next successful conversion sweeps it.

The rollback catches `BaseException`, not `Exception` — an interrupt is the likeliest way a
long conversion stops, and catching only `Exception` let Ctrl-C walk out with the output half
installed and the previous copies stranded. Every step of the rollback is best effort and
cannot itself raise: one that aborts on its first problem leaves exactly the half-state it
exists to prevent, and one that raises replaces the real cause with its own (the reported
error was "rollback cleanup failed" while the actual failure went unmentioned). Displaced
copies stranded by a rollback that could not finish are swept by the next conversion, since
nothing else ever removed them.

Every removal is matched against **exactly the names a conversion writes** — `job_N.json`,
the three metadata files, the three bundle directories, and those same names suffixed with
`.replacing-<pid>` or a `.convert-staging-<pid>` directory. The output directory is shared
with whatever else you keep there and this code deletes things, so a substring test is not
good enough: matching `".replacing-"` anywhere in a name removed a user's
`notes.replacing-the-old-plan.txt`, `data.replacing-v2.csv` and an
`experiments.replacing-baseline/` directory, silently. `code.replacing-notes.txt` has an
owned base name and is still the user's, because the suffix is not a pid.

`--no-bundle` restores the old behaviour, where job records only *describe* their code by
absolute submit-host paths. Simulated jobs are unaffected throughout: a job with no execution
block has nothing to bundle and converts exactly as before.

`manifest.json` is keyed by the executable's **source pfn**, not by transformation name. A
name is not unique: converting several runs at once (`--root`, and the shipped multi-workflow
profile carries five labels) can put two different workflows' `process` in one bundle, and
keying by name bundled whichever came first and gave it to both. Directories stay named after
the transformation and are disambiguated with a short hash only when one name genuinely serves
several executables, so the common case stays readable. A transformation name is also
workflow-supplied data used as a *directory name* — untrusted input on a write path — so it is
reduced to a single safe component and the destination is checked to be inside the bundle
before anything is created.

Two forms appear in the manifest for each file and they are not interchangeable: `bundled` is
relative to the bundle (for reading and auditing), `root_relative` is relative to its root and
is what goes in the job record — `roots.code` already names `code/`, so using the bundle-relative
form put `code/` in the path twice and every job refused.

### 2.5 Configuring execution

**Staging is the default since 2026-09-21** (`docs/STAGING_DESIGN.md`). A job's outputs stay on
the agent that produced them and a consumer elsewhere fetches them, with a staging site holding
a durable copy; the work directory is therefore **local** to each agent, and a shared one would
make every fetch a no-op and measure nothing.

```yaml
runtime:
  execution:
    mode: real
    work_dir: /var/tmp/swarm-wf/work       # LOCAL per agent
    container_runtime: auto
    timeout_s: 3600.0
    bundle: /export/swarm-wf/converted_jobs   # code + root inputs, read-only
    roots:
      images: /export/images               # each agent's LOCAL disk (§4.3)
    staging:
      enabled: true
      store_host: database                 # staging_site.py, started once
      store_port: 21000
```

Start the site once, on a node every agent can reach, and leave it running — the store is keyed
by `(run, name)`, so one site backs a whole campaign without runs colliding. Do **not** pin it
with `--run-id`: `run_test.py` mints the run id at launch and it ends in a uuid, so it is not
knowable in advance.

```bash
python3 staging_site.py --store-dir /export/swarm-wf/store --port 21000
```

Every agent needs `SWARM_RUN_ID`, which `run_test.py` exports and re-exports over ssh; staging
refuses to start without it, because an agent that does not know its run cannot tell a fetch for
this run from one for a previous run's identically named file.

The export still carries the bundle — the executable and the DAG's root inputs — because a job
needs those before it can start. What staging moves is the files jobs **produce**.

<details>
<summary>Shared-mount alternative (<code>staging.enabled: false</code>)</summary>

Simpler, fewer moving parts, and what every result before 2026-09-21 was measured on. The work
directory goes back on the export, shared so job B finds job A's output:

```yaml
    work_dir: /export/swarm-wf/work
    staging:
      enabled: false
```

It must then be writable by the **squashed** NFS user, or every job refuses with
`Permission denied`:

```bash
sudo chown nobody:nogroup /export/swarm-wf/work && sudo chmod 1777 /export/swarm-wf/work
```

Every agent is then equidistant from every produced file, which is exactly the variable the DTN
penalties price — so no data-movement, locality or makespan number may come from such a run.
</details>

`path_rewrites` is for records converted with `--no-bundle`, which describe their code by
absolute submit-host path and need those prefixes mapped onto wherever it was staged. A bundle
carries the code, so there is nothing to rewrite. The one pairing that remains common is
`bundle` plus `roots.images`, when the image is staged to each agent's local disk (§4.3) rather
than read off the export — explicit `roots` entries win over `bundle`, and a bundled image is
recorded by bare basename, so a local directory holding the same file name drops straight in.

### 2.6 Running it

```bash
python3 run_test.py --mode remote --agent-type resource --agents 5 --agents-per-host 1 \
    --topology mesh --jobs-per-interval 4 --db-host database \
    --agent-hosts-file agent_hosts.txt --run-dir runs/soil-real \
    --pegasus-jobs-dir /export/swarm-wf/converted_jobs \
    --runtime 420
```

`--pegasus-jobs-dir` publishes the bundle as it is: no conversion runs, no synthetic jobs are
generated, and the directory is never cleaned between runs (it is usually the copy on the
export, so `cleanup_between_runs` deleting it would destroy the workflow). The gating came from
the converter, so `--pegasus-dag-gating` — which applies only when `run_test.py` converts —
is not used on this path, and the two flags are mutually exclusive.

`--jobs` is not passed here. It sets the agents' expected job count and drives the completion
checks, and the bundle knows it exactly — `run_test.py` counts the `job_*.json` records, which is
the same set `job_distributor.py` publishes. Pass it only to override, and it warns when the two
disagree. (It stays required for every other kind of run, where nothing can count for you.)

`run_test.py` refuses outright on the things that would run the wrong work: surplus records from
an earlier conversion, the name collisions in §2.8, and a fleet that cannot run the jobs
(§2.7).

### 2.7 Sizing the fleet to the workflow

The converted jobs carry the resources they really needed on Pegasus. The fleet is sized
independently — `generate_configs.py` draws from its own flavour pool — and with
`--pegasus-jobs-dir` the conversion ran on a machine that knew nothing about this fleet. Nothing
reconciled the two: the old convert-here path aligned the *DTN names* (it converted jobs onto
the DTNs the fleet held) and never the capacities, and a copied bundle aligns neither.

The failure is silent by construction. `is_job_feasible` returns False for every agent, so the
job is never proposed, never fails, and never appears anywhere except as still-pending at the
end — which looks like a scheduling problem and sends you to the consensus logs.

`run_test.py` therefore compares the two before starting (`check_fleet_fits_jobs`) and refuses
the run, naming an example job, what it needs, and what the largest agent has. Two details that
decide whether the check is worth anything:

* It compares **whole profiles**, not per-dimension maxima. A fleet with a big-CPU agent and a
  big-RAM agent satisfies neither a job needing both — one agent has to satisfy every dimension
  at once.
* It requires **one agent holding every DTN** a job names (`local` excluded, since it means the
  local filesystem). Two agents holding one DTN each do not place a job that needs both.
* It describes the fleet from the **per-agent configs this run launches** — the files each
  agent actually loads, ids 1..agents+dynamic, in the directory that mode launches from (§2.6).
  `agent_profiles.json` is a local artefact of the last generation: under `--use-config-dir` it
  need not describe these configs at all, and either way it lists every agent ever generated, so
  a 270-agent generation left lying around satisfied the check for a 30-agent run that then
  stalled. It is consulted only when no config describes any agent, restricted to the same ids.
* **Absent and unparseable are different, and absence means different things per mode.** A
  local run launches through `swarm-multi-start.sh`, which iterates the configs that *exist* and
  skips ids outside its range: a missing config is one fewer agent, not fleet, and not a reason
  to stop describing the rest. A remote run copies each id's config to its host and
  `start_agents_remote` raises on the first one missing, so the run does not start at all and
  any verdict about job sizes would describe a fleet that never exists — there, a missing config
  stops the check instead. Falling back to
  `agent_profiles.json` on a missing config was worse than useless: that file can describe an
  older generation, so one gap swapped every agent's real capacities for a stale guess. A config
  that is present and *will not parse* skips the check with a report rather than deciding it —
  the agent that did not parse may be the only one that fits. The execution-mode lookup follows
  the same rule and had the same bug: a missing config made it answer "cannot tell", refusing
  runs whose every launched agent was perfectly readable.

**In practice a replay converted with real DTN names is refused without sizing, and the reason
is DTNs, not capacity.** Feasibility requires a single agent holding *every* DTN a job names,
while `--dtns` gives each agent 1-4 random picks from a ten-name pool. Measured on this repo's
`converted_jobs/`: 19,542 of 25,331 jobs name two DTNs and 908 name three to five, so a default
20-agent fleet places none of them — the check refuses with 12,627 unplaceable. The jobs
themselves are small (4 cores / 14 GB / 6.2 GB / 1 GPU at the top) against a 2-core / 8 GB
smallest flavour.

**Sizing makes every job runnable on every agent, and that is the point** — a failure test only
means something if a dead agent's work can go to any other, rather than being stranded because
it fitted only the agent that died. **It does not make the fleet uniform**, and the two
dimensions that carry the variability are exactly the two that do not decide feasibility:

* **Capacity is raised to a floor.** An agent already larger than the largest job keeps its
  flavour. Measured on a 20-agent mesh sized to this bundle: five distinct profiles, 4 cores /
  14 GB (8 agents) through 32 cores / 128 GB (1 agent).
* **Locality is `connectivity_score`.** `is_job_feasible` tests DTN *names*; the score feeds
  the cost model. So every agent holds every required name — no job is infeasible anywhere —
  while each is differently well connected to it. One base score per name (drawn 0.6-0.95, as
  the standard pool is), jittered per agent.

Until 2026-09-18 sized DTNs were pinned at `connectivity_score: 1.0`, which removed locality
altogether and did it in the flattering direction: an agent's organically assigned DTNs score
0.6-0.95, so every bolted-on one outscored them. `swarm/utils/fleet_sizing.py` now draws them.

**And until 2026-09-20 the `--dtn-names local` route had no locality either, in the other
direction.** `local` is a Pegasus *site*, not a data transfer node, and no agent holds one by
that name; feasibility subtracted it, but `compute_job_cost` did not, so it scored 0.0 — the
worst connectivity there is — on every agent. Every cost in such a run was multiplied by
`1 + connectivity_penalty_factor`, i.e. **doubled** at the shipped 1.0, which moves the
LLM-vs-analytic 0–100 comparison and the `tie_break_ref_cost: 11.85` calibration. A job naming
`local` beside a real DTN had the real score halved, and that part was *not* a uniform shift.
On hierarchical runs it was worse than a shift: `_get_child_groups_for_job` had the same
omission, so an all-local job matched no child group and delegation fell back to every active
group with the DTN filter off. `Job.required_dtns()` is now the single definition of the set.
Code review §8; `tests/test_local_dtn_cost.py`.

**What sizing still costs.** The floor is raised, so a sized fleet is not the standard flavour
pool and a run on it is not comparable with results measured on one. For a run whose subject
*is* the fleet, keep the standard pool and stay feasible the other way: convert with
`--dtn-scope job` onto names the fleet already holds — one DTN per job, an agent really has it,
locality untouched. `run_test.py --pegasus-profiles` does this automatically; a bundle converted
elsewhere cannot, which is why `--pegasus-jobs-dir` normally wants `--size-to-jobs`.

To make them meet rather than merely discover that they do not, generate the fleet *from* the
jobs. **Generate it once, with `generate_configs.py`, for every topology and both modes**, and
pass `--use-config-dir` to every run:

```bash
python3 generate_configs.py 5 10 ./config_swarm_multi.yml configs mesh database 0 \
    --skip-jobs --seed 42 --size-to-jobs /export/swarm-wf/converted_jobs \
    --agent-hosts-file agent_hosts.txt --agents-per-host 1
```

**`--agent-hosts-file` is not optional on a remote run.** Without it every agent advertises
`grpc.host` verbatim from the base config — `0.0.0.0` as shipped — and a peer dialling that
reaches its own localhost. Measured on the slice 2026-09-21: every consensus finalization in
such a run reads `reason=single-node`, meaning no agent ever reached another, and every staging
location is unusable for the same reason. With the flag each agent advertises `agent-N`, which
`/etc/hosts` resolves to its data-plane address.

Left to `run_test.py` the fleet is regenerated on every run, which re-draws flavours and DTNs and
makes two runs of the "same" cell incomparable. `--use-config-dir` is what stops that: it skips
generation and stops `cleanup_between_runs` from deleting `configs/`, `agent_profiles.json` and
`agent_dtns.json`; Redis is still flushed. Generate with `--skip-jobs` (no synthetic jobs), with
the execution block (§2.5) already in the base config since these are copies of it, and with
`--seed` so the fleet is reproducible. The starter globs `./configs` for a local run;
`--config-dir` is what a remote run copies across (§2.6). Keep `agent_profiles.json` in the repo
root, which is where the check reads the fleet from.

`--size-to-jobs` is what sizes the fleet from the bundle: every agent raised to the largest job
and given every DTN the jobs name. Without it the flavour pool tops out at 32 cores / 128 GB /
1 TB and the check above is the whole safety net.

**The converter can do the same thing in the conversion pass, for flat topologies only:**

```bash
python3 pegasus_to_swarm_converter.py --input soil_profiles.json --input-type json \
    --output-dir converted_jobs/ --dag-gating \
    --dtn-names local --dtn-scope job \
    --generate-agent-configs --num-agents 5 --base-config ./config_swarm_multi.yml \
    --topology mesh --db-host database
```

It writes `agent_profiles.json` and `configs/config_swarm_multi_<id>.yml` into the bundle, sized
the same way — both routes compute "can this agent host this job" from
`swarm/utils/fleet_sizing.py`, so a fleet sized either way means the same thing. It saves a step
when the bundle is being built anyway, at the cost of two: the base config it copies on the
submit host must already carry the execution block, and `configs/` and `agent_profiles.json` have
to be copied to the database node with it. `--generate-agent-configs` writes `mesh`, `ring` or
`star`; a hierarchical fleet has no route but `generate_configs.py`.

Note that a hierarchical fleet gets **no generated DTN pool** (`--dtns` is not passed for it):
agents carry only what the base config's `dtns:` lists and `agent_dtns.json` is not written, so a
job hashed onto a generated DTN name has no holder — convert with `--dtn-names local`, or let
`--size-to-jobs` attach the names in use.

`--pegasus-profiles` converts at run time instead of publishing a bundle, and it is compatible
with `--use-config-dir`: the conversion takes its DTN pool from the per-agent configs of the
agents **this run launches** — the same source and the same id range as the fleet check above,
and for the same reason. `agent_dtns.json` is not consulted (except when nothing on disk
describes a fleet at all): it is a repo-root artefact of the last generation, so under
`--use-config-dir` it can describe a fleet generated on another machine, and it lists every
agent ever generated — hashing a job onto a DTN only agent 200 holds leaves it infeasible for
every agent that starts. If the launched fleet cannot be described, the conversion drops to
`local` (no DTN requirement) and says so, rather than naming DTNs nobody may hold. Two earlier
shapes of this: the combination used to skip the conversion entirely, so the run published
whatever `jobs/` already held.

### 2.8 Several workflows

> One bundle and one run per workflow is the safe default. This section is why, and what is
> checked if you combine them anyway.

Job ids are namespaced by run (`<dax_label>_<run dir>_<job name>`) and never collide. **Logical
file names are not namespaced**, and three things key on them: the DAG producer map
(`apply_dag_gating` matches a consumer's input against *any* job's output), the run's readiness
registry (`data_ready:<run id>:0:names`, one flat set), and the shared working directory (bare
names, one directory). Two workflows that both produce `output.csv` therefore get a
cross-workflow edge nobody wrote, and then two jobs writing one file.

This is not hypothetical, and it does not need two different workflows. Converting this repo's
`pegasus_subset_profiles.json` (7 runs, 174 jobs) reports **73 colliding output names**, all of
them between two runs of the *same* workflow — which is the obvious way to scale a replay up.

So the default is **one bundle and one run per workflow**: each run mints its own
`SWARM_RUN_ID`, which scopes both the working directory and the readiness registry, so nothing
one workflow produces can release another's jobs. Combining several into one run is fine only
when their file names are disjoint, and that is checked rather than assumed:

| what collides | where it is recorded | what happens |
|---|---|---|
| a name one workflow produces and another **reads** | `conversion_summary.json` → `dag.cross_workflow_edges` | refused whenever the run acts on names (gating, or anything executing) |
| an output name produced by more than one job | `conversion_summary.json` → `dag.colliding_outputs` | refused when the bundle is DAG-gated; a warning otherwise |
| a staged input name that some job also produces | `conversion_summary.json` → `dag.replica_conflicts` | refused across workflows; a warning within one workflow |
| one logical name declared by two workflows for different files | `manifest.json` → `missing`, flagged `collision` | refused |
| a replica or container image whose basename collides | `manifest.json` → `missing`, flagged `collision` | refused |

**The first row is the general case and the one to understand**; the rest are narrower shapes of
it. Two workflows have no data relationship, so a name they share is a coincidence — and both
mechanisms that act on names act on it anyway: gating keys its producer map by name, so the
reader waits for the other workflow's job; the working directory is flat and shared, so whoever
gets there first decides what the reader reads. It is checked as `data_in` against `data_out`,
which needs nothing but the job records. An earlier version compared the *declared replicas*
instead, and most profiles carry none — the replica catalog is not in the stampede DB — so the
common case reported nothing: on this repo's own 7-run profile, that check finds **0** conflicts
where this one finds **62**.

**"Acts on names" is the condition**, not "is a workflow" — and it is decided per run, not per
bundle, because every one of these is a warning or a refusal depending on it:

* **Gating** consults a producer map keyed by name, so a name decides scheduling order.
* **Execution** reads and writes those names in one shared directory. This takes *both* the
  run's configured `runtime.execution.mode` (which a bundle cannot know, so `run_test.py` reads
  the config the agents will use) and jobs that actually carry something to execute — a
  synthetic job in a `real` run still simulates, so one half alone would refuse ordinary
  replays. The mode is resolved by `runner.resolve_mode` and nowhere else: **an absent key means
  `simulate`**, so a config with no execution block describes a run that touches nothing.
  Re-deriving that default as "absent, so assume it executes" refused ordinary replays for a
  collision they could never act on — the same one-key-one-default rule `consensus.protocol`
  follows.

  **A config that cannot be *read* is a third state, not the default.** Absent is an answer;
  unparseable is not — and under `--use-config-dir` the run starts regardless, because the
  agents read their own per-agent files and never this process's copy. So an unreadable config
  is reported and the checks are applied as if the jobs will execute. Collapsing it into
  `simulate` skipped them for a reused config that says `mode: real` and happened not to parse
  (a duplicate key, which `yaml_strict` refuses, is the likeliest way). For the same reason the
  answer under `--use-config-dir` comes from that directory and never falls back to the base
  config: those are the files the agents will read — **all** of them, since they need not agree
  and one agent configured `real` makes the run one that executes, and only
  `config_swarm_multi_*.yml`, since an unrelated file in the directory has no runtime block and
  would otherwise answer `simulate` on behalf of configs that say `real` — and only the ids this
  run launches (1..agents+dynamic), because a config directory outlives the run that generated
  it, so an agent nobody starts must not decide what this run is. Which directory that is
  differs by mode and is not always `--config-dir`: `swarm-multi-start.sh` globs `configs/`
  literally, so a local run launches from `./configs` whatever `--config-dir` says, while a
  remote run launches from each host's own `configs/`, filled by copying `--config-dir` across.

An ungated simulated replay does neither: its names are inert, and it is warned about rather
than refused. That is exactly what the shipped multi-workflow profile is — 62 cross-workflow
names and 73 colliding outputs, refused as a gated or executing run and merely reported as the
replay it has always been. The reverse also holds, and was a real gap: a `real` run *without*
`--dag-gating` has no producer map but still has one flat directory, so two jobs writing one
name are refused there too.

A bundle converted before these checks existed carries no record of them, and silence from a
check that never ran reads exactly like a clean bill of health. That case is named as such and
refused when the names are live: re-convert it.

Rows three and four read as harmless and are not. The converter carries one file and reports the
loser — but the losing **job still names that file** in its `data_in`, and `stage_inputs`
resolves a bare name under `roots.inputs`, so it is staged the *other* workflow's bytes and runs
to completion on them. An image collision is safer only by accident: the loser keeps its
original absolute path, which does not exist on the fleet, so it refuses loudly.

Every one of these is decided on the **basename**, not the recorded name, because that is the
name the file actually gets: the working directory is flat and `stage_inputs` reduces a declared
name to its basename. Comparing the recorded strings misses the case that matters most —
`runA/out.csv` and `runB/out.csv` are two names and one file.

#### Why not prefix the names instead

The obvious fix — rename `data.csv` to `wfA_data.csv` on the way into the bundle — does not
work, for a reason that is specific to running real code. **A file name is not only an
identifier here; it is part of the command line.** The arguments come from the abstract
workflow (`--input field1_soil_data.csv`, §1.2), and a workflow's code also opens files by name
internally and writes its outputs under names it chose. Rename the staged file and the job
cannot find it; rename it back afterwards and you have to know which argument strings were file
names, which is not decidable from a catalog.

So the namespace has to be something the job never sees: a **directory**. Give each workflow its
own working directory — which is what Pegasus does with its per-workflow scratch dir — and the
names inside it stay exactly as the workflow wrote them. That is genuinely the right fix, and
it is not what the code does today: `work_dir` is per *run* (`<work_dir>/<SWARM_RUN_ID>`) and
shared by every job of that run **on the same agent** — which under the shared mount is how job
B finds job A's output, and under staging is the reason a child already co-located with its
parent fetches nothing. Per workflow
is the finer scope that keeps that property and removes the collisions; it needs the runner to
key on the job's `workflow` field (now carried on every converted record), the readiness
registry to be scoped the same way, and DAG matching to stay inside a workflow.

Until then, one run per workflow **is** that namespace — `SWARM_RUN_ID` already scopes both the
working directory and the readiness registry — and a bundle that would need the finer scope is
refused rather than quietly resolved.

### 2.9 Roots and image resolution — for jobs you author yourself

> Reference. A converted bundle needs none of this: `bundle` sets the three roots and the
> records are already relative.

A converted bundle needs none of this: `bundle` sets the three roots and the records are already
relative. Roots are for the other case — a job written by hand, or a `--no-bundle` conversion
whose absolute submit-host paths `path_rewrites` maps onto wherever the code was staged. For
those, say where the three kinds of thing live:

```yaml
runtime:
  execution:
    roots:
      code:   /export/swarm-wf/code
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

**That check is apptainer-only, and the placement is the point.** Only apptainer can run an
image from a path; docker resolves references against a registry or its local image store and
never a file, so handing it one produces `invalid reference format`, which says nothing about
the cause. So the same bare reference legitimately resolves *differently per runtime*: a name
colliding with something in the images root is a local image under apptainer and a registry
reference under docker. `resolve_image` classifies (runtime-agnostic); `build_command` adapts
(runtime-specific). Putting the filesystem check in the shared half broke every docker
reference that collided with a name in the images root.

The full table — `kind` × reference form × runtime × present-or-absent in the root — is
enumerated in `tests/test_real_execution.py::TestImageResolutionMatrix`. It is a table rather
than prose because fixing this one reported case at a time kept uncovering the next empty
cell.

Relative paths are also **contained**: a job record is workflow-supplied data, and
`../../usr/bin/something` joined onto a root escapes it, which defeats the point of naming a
root. `resolve_under_root` is the single place every relative path passes through, so the
check lives there and covers code, inputs and images at once. It is lexical (`normpath`),
which is what defeats `..`; a symlink *inside* a root is placed by whoever administers it and
is deliberately still followed.

`image_overrides` is resolved the same way, **not trusted verbatim** — it was the last door
through which a relative path reached the runtime unresolved, and therefore got resolved
against the agent's own working directory. What an override does *not* inherit is the
catalog's `kind`: substituting a docker image for a singularity one is the entire purpose of
the key, so classifying the override by the kind it replaces would refuse the case it exists
for. An override is classified by its own shape — a recognisable image-file suffix, or
something that actually exists under the images root, is a file; anything else is a registry
reference.

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

**Code and image want different homes.** Code must be identical on every agent (the export, or
the bundle on it); the multi-gigabyte image belongs on each agent's **local disk** (§4.3). With
a bundle that is `bundle` plus a `roots.images` override. With `--no-bundle` records it is two
`path_rewrites` entries under one workflow root, where **longest prefix wins** — so the image
rule takes precedence regardless of the order they are written in, which is why the rule exists.

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

### 4.1 Staging is local, not distributed

Jobs run with the **shared** work dir as cwd, and `data_in`/`data_out` are bare logical names,
so job B finds job A's output because both ran in the same directory. The workflow's *declared
replicas* — the files `replicas.yml` lists and no job produces — are carried in the bundle's
`inputs/` and copied into the work dir by `stage_inputs` before a job starts (§2.9), so nothing
has to be placed by hand any more.

**Transfer between agents exists since 2026-09-21** (`docs/STAGING_DESIGN.md`): a produced file
stays on the agent that made it, the location registry says which agent that is, and a consumer
elsewhere fetches it over a second gRPC port on the same host consensus uses. The sentence that
stood here — that staging is only a copy from a root the agent can already see, so agents not
sharing a filesystem would have nothing to stage from — described the state before that, and is
still true of the shared-mount mode.

`fetch_soil_data` additionally calls a live external API, and
`archive-api.open-meteo.com` is **unreachable from the slice** (generic HTTPS is fine —
GitHub answers in 0.2 s — that one host times out). Its real output from the original run
stands in for it. A job whose runtime is an external service's latency is a poor comparison
subject anyway.

### 4.2 NFS is scaffolding — and since 2026-09-21 it is the *alternative*, not the path

**Staging is the default now** (`runtime.execution.staging.enabled: true`,
`docs/STAGING_DESIGN.md`). A job's outputs stay on the agent that produced them and a consumer
elsewhere fetches them over a second gRPC port on the same host consensus uses, with a staging
site holding a durable copy so an output survives its producer. The work dir is then **local**
to each agent; a shared one makes every fetch a no-op and measures nothing.

Everything below about NFS still applies when you deliberately choose the shared-mount mode —
it is simpler, and it is what every result before 2026-09-21 was measured on — but it is no
longer what a fresh run does. The bundle's *code* and the DAG's *root inputs* still come from a
path every agent can read; it is the *produced* files that now travel.

### 4.2.1 Why NFS was scaffolding in the first place

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

1. ~~**Transfer, and stage-out.**~~ **BUILT 2026-09-21** — see `docs/STAGING_DESIGN.md`, and
   §6 there for what is measured and what is still missing (transfer accounting per job and
   per link, which is what a data-movement *result* would need). The paragraph below describes
   the state before that.

   *What stood here before, kept because it is what the pre-2026-09-21 results were measured
   on:* declared replicas travel in the bundle and are copied into the working directory by
   `stage_inputs` (§4.1), but that is a local copy from a root the agent can already see.
   Nothing moves data between agents, and outputs never leave the working directory. That was
   the last thing standing between the setup and a defensible makespan comparison; what remains
   of it is transfer **accounting** (bytes per job and per link in `collect.py`), without which
   staging is a capability rather than a result.
2. **A controlled substrate** — Pegasus and SWARM on the same hardware — before any published
   number.
3. **Per-job resource enforcement.** The catalog carries `memory`/`cores` requests; the runner
   does not pass them to the container runtime, so a job can exceed what it asked for.
4. **A per-workflow working directory**, so several workflows can share one run without their
   file names colliding (§2.8). The records already carry `workflow`; the runner, the readiness
   registry and the DAG matcher would all key on it.
5. **Clustered jobs**, which currently refuse (§1.3). Supporting them means representing a job
   as an ordered list of invocations rather than one command line.

Tests: `tests/test_workflow_execution.py` (74), `tests/test_real_execution.py` (76),
`tests/test_workflow_dag.py` (29), `tests/test_pegasus_jobs_dir.py` (19).
Related: `docs/QUANTUM_HYBRID_DESIGN.md` for the other execution path (`_execute_quantum`).
