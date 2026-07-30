# Pegasus → SwarmAgents Job Pipeline

Convert real Pegasus workflow executions into SwarmAgents job JSON files so that
recorded production workloads (runtimes, resource requests, input/output data)
can be replayed through the swarm scheduler and compared against the original
Pegasus execution.

The pipeline has two stages:

```
 Pegasus submit host                          SwarmAgents host
┌──────────────────────────────┐             ┌──────────────────────────────────┐
│ pegasus_profile_extractor.py │  scp JSON   │ pegasus_to_swarm_converter.py    │
│  *.stampede.db               │ ──────────► │  all_runs_jobs_profile.json      │
│  workflow.yml / braindump    │             │   → jobs/job_*.json              │
│  *.cache, rc_meta, *.sub     │             │   → pegasus_baseline.json        │
└──────────────────────────────┘             │   → conversion_summary.json      │
                                             └──────────────────────────────────┘
```

## Stage 1 — Extract job profiles (runs on the Pegasus submit host)

`pegasus_profile_extractor.py` walks one or more Pegasus submit directories
(each containing a `*.stampede.db` monitord database) and writes
`all_runs_jobs_profile.json` — a JSON array with one profile dict per job.

```bash
# Scan a whole tree for every run of every workflow
python3 pegasus_profile_extractor.py --root /home/ubuntu --output all_runs_jobs_profile.json

# Or target specific runs
python3 pegasus_profile_extractor.py \
    --submit-dir /home/ubuntu/drought/ubuntu/pegasus/drought/run0005 \
    --submit-dir /home/ubuntu/nextgen-workflow/ubuntu/pegasus/nextgen/run0001 \
    --output profiles.json
```

Options:

| Flag | Default | Meaning |
|------|---------|---------|
| `--root DIR` | — | Recursively discover run dirs (dirs containing `*.stampede.db`). `scratch/` and hidden dirs are pruned; discovery stops descending once a run dir is found. |
| `--submit-dir DIR` | — | Explicit run dir; repeatable. Can be combined with `--root`. |
| `--output FILE` | `all_runs_jobs_profile.json` | Output JSON array. |
| `--job-types LIST` | `compute` | Comma-separated stampede `job.type_desc` values to extract (others: `stage-in-tx`, `stage-out-tx`, `create-dir`, `cleanup`, `registration`). |
| `--include-failed-runs` | off | Also extract runs whose workflow did not terminate successfully (failed/running runs are skipped by default). |

The script exits non-zero and writes nothing if no profiles were extracted.

### What is extracted, and from where

| Profile field(s) | Source |
|------------------|--------|
| `remote_duration_sec_db`, `remote_cpu_time_sec_db`, `maxrss_kb_db`, `exitcode_db`, `transformation_db` | `invocation` table (dagman pre/post scripts excluded), final try |
| `execution_site_db`, `kickstart_sec_stats` (local duration), retries | `job_instance` table |
| `submit_timestamp_db`, `queue_time_sec_db` (SUBMIT→EXECUTE) | `jobstate` table |
| `runtime_{min,max,mean,stddev}_sec_stats`, `runtime_{succeed,failed}_stats`, `try_number_stats` | all tries of the job |
| `wf_uuid_db`, `dax_label_db`, `wf_status`, `wf_duration_sec` | `workflow` + `workflowstate` tables |
| `request_cpus_db`, `request_memory_mb_db`, `request_gpus_db` | HTCondor `*.sub` files under the submit dir |
| `input_files_db`, `output_files_db` (`lfn`, `site`, `size_bytes`) | job→file mapping from the abstract workflow (`workflow.yml` in the run dir, else the `dax:` pointer in `braindump.yml`); sites from the `<label>-0.cache` file; sizes from `rc_meta` |
| `total_{input,output}_size_bytes_db` | sum of the above file sizes |
| `network_db.stage_{in,out}` | aggregated `stage-in-tx` / `stage-out-tx` job durations and sites (workflow-level) |

Jobs are matched to their abstract-workflow entry via `invocation.abs_task_id`
(this handles custom job ids such as `aggregate_H2` and clustered jobs with
multiple tasks — their file lists are merged), falling back to the
`_IDnnnnnnn` suffix convention in the executable job name.

### Requirements

Python 3 with PyYAML on the submit host; everything else is stdlib. The
stampede databases are opened read-only, so extraction is safe to run next to
live workflows.

## Stage 2 — Convert to SwarmAgents jobs

Copy the JSON to the SwarmAgents host and run the converter:

```bash
python pegasus_to_swarm_converter.py \
    --input all_runs_jobs_profile.json --input-type json \
    --output-dir converted_jobs/ \
    --data-nodes per-file
```

This writes:

- `converted_jobs/job_1.json … job_N.json` — SwarmAgents job files (feed to
  `job_distributor.py` or `run_test.py`)
- `converted_jobs/pegasus_baseline.json` — per-run Pegasus ground truth
  (makespans, per-job runtimes, transfer stats) for comparison plots
- `converted_jobs/conversion_summary.json` — mapping parameters and warnings

Field mapping into each swarm job: `wall_time` ← `remote_duration_sec_db`
(fallbacks: kickstart, cpu time), `capacities.core/ram/disk/gpu` ← Condor
requests (RAM falls back to maxrss; disk from total input bytes; floors set by
`--min-*` flags), `exit_status`/`should_fail` ← exit code.

### `--data-nodes per-file` vs `per-site`

- `per-site` (default, historical behavior): `data_in`/`data_out` are
  deduplicated to one DataNode per unique site — only the first file name per
  site survives.
- `per-file`: every input/output file becomes its own DataNode, preserving the
  file name and `size_bytes`:

```json
"data_in": [
  {"name": "local", "file": "observations.csv", "size_bytes": 7218941},
  {"name": "local", "file": "region_config.json", "size_bytes": 2834}
]
```

`DataNode` (`swarm/models/data_node.py`) carries the optional `size_bytes`
field, so converted jobs round-trip cleanly through `Job.from_dict()`.

### DTN naming (`--dtn-map`, `--dtn-names`)

Single-site Pegasus runs record every file at site `local`. Two options control
the DTN names in the converted jobs:

- `--dtn-map local=dtn1,condorpool=dtn2` — rename Pegasus site names to DTN
  names one-for-one. Unlisted sites pass through unchanged.
- `--dtn-names dtn1,dtn2,dtn3` — spread files across the listed DTN pool by a
  stable hash of the file name: the same file maps to the same DTN in **every**
  job, giving consistent producer/consumer data locality while exercising
  multi-DTN selection. Overrides `--dtn-map`.

```bash
python pegasus_to_swarm_converter.py --input all_runs_jobs_profile.json \
    --input-type json --output-dir converted_jobs/ \
    --data-nodes per-file --dtn-names dtn1,dtn2,dtn3
```

`--generate-agent-configs` collects DTN names from the converted jobs, so the
generated `agent_profiles.json`/YAML configs automatically list whichever DTNs
these options produce.

Use `--generate-agent-configs --num-agents N --base-config config_swarm_multi.yml`
to also emit agent profiles/configs sized to the converted workload (agents get
every DTN site referenced by the jobs).

## End-to-end example (pegasus2 deployment)

```bash
# 1. On the Pegasus submit host
ssh pegasus2
python3 pegasus_profile_extractor.py --root /home/ubuntu --output all_runs_jobs_profile.json
exit

# 2. Fetch and convert
scp pegasus2:~/all_runs_jobs_profile.json .
python pegasus_to_swarm_converter.py --input all_runs_jobs_profile.json \
    --input-type json --output-dir converted_jobs/ --data-nodes per-file

# 3. Replay through swarm
python run_test.py --mode local --agents 20 --topology mesh \
    --jobs $(ls converted_jobs/job_*.json | wc -l) --db-host localhost \
    --run-dir runs/pegasus-replay   # point the distributor at converted_jobs/
```

A July 2026 extraction of the pegasus2 host yielded **25,331 job profiles from
9 successful runs** (drought, nextgen ×2, quantumchem vqe/shadows ×4,
s2-segmentation ×2 — s2-segmentation alone contributes 25,157 jobs; filter by
`run_name` before converting if you want a balanced mix).

## Gotchas

- **Empty `workflow_files`/`rc_pfn` tables**: monitord does not always populate
  the job→file mapping in the stampede DB — hence the abstract-workflow YAML +
  cache-file approach.
- **Missing `workflow.yml` in the run dir** (e.g. quantumchem runs): the
  extractor follows `braindump.yml`'s `dax:` pointer. Best effort — if the
  abstract workflow file was edited after the run, the file lists reflect its
  current contents.
- **`request_cpus` absent** from many `.sub` files → profile records 0 and the
  converter substitutes `--default-cores` (1.0).
- **`kickstart_sec_stats`** is the DAGMan-measured local duration, which
  includes pegasus-lite overhead (container pulls, staging). It is only a
  fallback when `remote_duration_sec_db` is missing.
- **Site names**: single-site runs report everything as `local`; the converter
  maps sites to DTN names, so multi-site Pegasus runs produce meaningful DTN
  connectivity constraints.
