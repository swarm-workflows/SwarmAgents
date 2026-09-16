#!/usr/bin/env python3
"""
Pegasus Workflow Job-Profile Extractor

Runs on a Pegasus submit host. Walks one or more Pegasus submit directories
(each containing a ``*.stampede.db`` monitord database), extracts per-job
execution profiles, and writes ``all_runs_jobs_profile.json`` — a JSON array
directly consumable by ``pegasus_to_swarm_converter.py --input-type json``.

Data sources per run directory:
  - <label>-0.stampede.db  : workflow, job, job_instance, invocation,
                             jobstate, rc_meta (file sizes)
  - workflow.yml           : abstract job -> input/output LFN mapping
  - <label>-0.cache        : LFN -> site mapping (site="..." attributes)
  - 00/**/<job>.sub        : Condor request_cpus / request_memory / request_gpus

Usage:
  # Scan a tree for all runs of all workflows
  python3 pegasus_profile_extractor.py --root /home/ubuntu --output all_runs_jobs_profile.json

  # Or specific submit dirs
  python3 pegasus_profile_extractor.py \
      --submit-dir /home/ubuntu/drought/ubuntu/pegasus/drought/run0001 \
      --submit-dir /home/ubuntu/drought/ubuntu/pegasus/drought/run0002 \
      --output drought_profiles.json

  # Then convert on the SwarmAgents side:
  python pegasus_to_swarm_converter.py --input all_runs_jobs_profile.json \
      --input-type json --output-dir converted_jobs/
"""

import argparse
import glob
import json
import math
import os
import re
import shlex
import sqlite3
import sys
from typing import Dict, List, Optional, Tuple

try:
    import yaml
except ImportError:
    yaml = None

# Job types from the stampede `job` table considered "real" jobs.
DEFAULT_JOB_TYPES = ["compute"]
ABS_ID_RE = re.compile(r"_(ID\d+)$")
CACHE_SITE_RE = re.compile(r'^(\S+)\s+\S+\s+site="([^"]*)"')


# ---------------------------------------------------------------------------
# Per-run-dir loaders
# ---------------------------------------------------------------------------

def find_stampede_db(run_dir: str) -> Optional[str]:
    hits = sorted(glob.glob(os.path.join(run_dir, "*.stampede.db")))
    return hits[0] if hits else None


def _find_abstract_workflow(run_dir: str) -> Optional[str]:
    """Locate the abstract workflow YAML for a run.

    Prefer workflow.yml inside the submit dir; otherwise follow the ``dax:``
    pointer in braindump.yml (best effort — the file may have been edited
    since the run was planned).
    """
    path = os.path.join(run_dir, "workflow.yml")
    if os.path.isfile(path):
        return path
    braindump = os.path.join(run_dir, "braindump.yml")
    if os.path.isfile(braindump):
        try:
            with open(braindump) as fh:
                bd = yaml.safe_load(fh) if yaml else {}
            dax = (bd or {}).get("dax")
            if dax and os.path.isfile(dax):
                return dax
        except Exception:  # noqa: BLE001
            pass
    return None


def load_workflow_uses(run_dir: str) -> Dict[str, Dict[str, List[str]]]:
    """Parse the abstract workflow -> {abs_job_id: {"input", "output", "arguments"}}.

    `arguments` is here because it is **the** source for them, and the obvious other
    candidate is empty. `invocation.argv` in the stampede DB records the kickstart-level
    command line, which for these workflows carries nothing: measured on the soilmoisture
    run, every compute job had `argv = ''` while the workflow declares
    `['--fetch', '--polygon-id', 'field1', ...]`. A job re-run from argv alone gets no
    arguments at all and dies on its own usage message, which is exactly how this was found.

    Keyed by abstract job id, the same key `uses` is keyed by, so the existing abs_task_id
    lookup in `extract_run` resolves both at once.
    """
    if yaml is None:
        return {}
    path = _find_abstract_workflow(run_dir)
    if not path:
        return {}
    with open(path) as fh:
        wf = yaml.safe_load(fh)
    uses_map: Dict[str, Dict[str, List[str]]] = {}
    for job in wf.get("jobs", []) or []:
        abs_id = job.get("id")
        if not abs_id:
            continue
        entry = {"input": [], "output": [], "arguments": []}
        for use in job.get("uses", []) or []:
            ftype = use.get("type")
            lfn = use.get("lfn")
            if lfn and ftype in ("input", "output"):
                entry[ftype].append(lfn)
        # An argument is usually a plain string, but the Pegasus API also permits a File
        # object, which round-trips through YAML as a mapping carrying its `lfn`. Rendering
        # that with str() would put a Python dict repr on the command line.
        for arg in job.get("arguments", []) or []:
            if isinstance(arg, dict):
                lfn = arg.get("lfn")
                if lfn:
                    entry["arguments"].append(str(lfn))
            else:
                entry["arguments"].append(str(arg))
        uses_map[abs_id] = entry
    return uses_map


def cluster_task_ids(main_tasks) -> set:
    """Distinct abstract task ids in one Condor job, straight from the stampede DB.

    This is the clustering signal, and it must come from the DB rather than from the
    abstract-workflow map, because it decides whether a job is safe to execute. The obvious
    alternative -- counting the abstract ids that resolved against `workflow.yml` -- **fails
    open**: that list is filtered by what is present in the workflow map, so a run with no
    `workflow.yml` (absent file, or PyYAML not installed) yields an empty list for *every*
    job, a clustered job is not recognised as one, and it goes on to execute the first task's
    recorded argv as though it described the whole cluster.

    Failing open is the wrong direction here. Missing metadata means we know *less* about a
    job, which can only make executing it less safe, never more.
    """
    return {tid for (_transformation, tid, *_rest) in main_tasks if tid}


def ordered_task_ids(main_tasks, known=None) -> List[str]:
    """Abstract task ids in invocation order, each appearing once.

    De-duplication is the point. One job instance can carry several invocation rows for the
    same abstract task, and the caller accumulates that task's ARGUMENTS per entry — without
    de-duplicating, because an argument list is ordered and a value may legitimately repeat.
    Those two facts together turn a duplicate row into a doubled command line
    (`--input a.csv --output b.json --input a.csv --output b.json`) that is still a single
    task, so nothing flags it as a cluster and it runs.

    Order is preserved rather than sorted: for a genuine cluster the sequence is the order
    Pegasus ran the tasks in, and it is the only ordering information available.

    `known` optionally restricts to ids present in the abstract workflow. Note the cluster
    guard must NOT be derived from the filtered result — see `cluster_task_ids`.
    """
    seen = set()
    out: List[str] = []
    for (_transformation, tid, *_rest) in main_tasks:
        if not tid or tid in seen:
            continue
        if known is not None and tid not in known:
            continue
        seen.add(tid)
        out.append(tid)
    return out


def load_transformation_catalog(run_dir: str) -> Tuple[Dict[str, dict], Dict[str, dict]]:
    """Parse the planned transformation catalog -> (transformations, containers).

    This is where the *executable* of a job lives, and it is not in the stampede DB in any
    usable form. The DB's `invocation.executable` is the path the job ran under **inside its
    container** (`/srv/analyze_moisture`); the catalog holds the `pfn`, the host path of the
    script that was staged there, plus which container it ran in and that container's image
    URI. Both halves are needed to re-run the job anywhere else: the pfn says what code to
    ship, the in-container path says what to invoke once it is shipped.

    Preferred location is `<run_dir>/catalogs/transformations.yml` — the copy Pegasus *planned
    with*, so it describes the run that actually happened. The workflow-root copy is a
    fallback and can have been edited since; a run whose catalog was rewritten afterwards
    would otherwise be described by a file that never governed it. Parent directories are
    walked only after the planned copy is missing, and the walk stops at the filesystem root.

    Returns two maps keyed by name. Both are empty when PyYAML is absent or no catalog is
    found; callers must treat an unknown transformation as "not executable" rather than
    guessing a path.
    """
    if yaml is None:
        return {}, {}
    candidates = [os.path.join(run_dir, "catalogs", "transformations.yml"),
                  os.path.join(run_dir, "transformations.yml")]
    probe = os.path.abspath(run_dir)
    while True:
        candidates.append(os.path.join(probe, "transformations.yml"))
        parent = os.path.dirname(probe)
        if parent == probe:
            break
        probe = parent
    path = next((c for c in candidates if os.path.isfile(c)), None)
    if not path:
        return {}, {}
    try:
        with open(path) as fh:
            tc = yaml.safe_load(fh) or {}
    except Exception:  # noqa: BLE001 - a malformed catalog must not abort the whole run
        return {}, {}

    containers: Dict[str, dict] = {}
    for entry in tc.get("containers", []) or []:
        name = entry.get("name")
        if not name:
            continue
        containers[name] = {
            "name": name,
            "type": entry.get("type"),
            "image": entry.get("image"),
            "image_site": entry.get("image.site"),
        }

    transformations: Dict[str, dict] = {}
    for entry in tc.get("transformations", []) or []:
        name = entry.get("name")
        if not name:
            continue
        sites = entry.get("sites", []) or []
        # A transformation can be listed for several sites. Prefer a non-local execution site
        # over "local": "local" is the submit host, whose pfn is frequently a wrapper rather
        # than the science code. Order within a site list is not meaningful, so the choice has
        # to be explicit rather than "first wins".
        site = next((s for s in sites if s.get("name") not in (None, "local")), None)
        if site is None:
            site = sites[0] if sites else {}
        transformations[name] = {
            "name": name,
            "pfn": site.get("pfn"),
            "type": site.get("type"),
            "site": site.get("name"),
            "container": site.get("container"),
        }
    return transformations, containers


def load_replica_catalog(run_dir: str) -> Dict[str, str]:
    """Parse the replica catalog -> {lfn: host path}.

    These are the run's ROOT INPUTS: files a workflow consumes but no job in it produces.
    Nothing else records them. Job `data_in` lists what each job reads, but cannot say which
    of those came from outside the run — that distinction is the difference between a file
    that must be shipped with the workflow and one that appears when its parent job finishes.

    Same lookup order as the transformation catalog: the run dir's planned copy first,
    because it is the one Pegasus used, then the workflow root, then upwards.
    """
    if yaml is None:
        return {}
    candidates = [os.path.join(run_dir, "catalogs", "replicas.yml"),
                  os.path.join(run_dir, "replicas.yml")]
    probe = os.path.abspath(run_dir)
    while True:
        candidates.append(os.path.join(probe, "replicas.yml"))
        parent = os.path.dirname(probe)
        if parent == probe:
            break
        probe = parent
    path = next((c for c in candidates if os.path.isfile(c)), None)
    if not path:
        return {}
    try:
        with open(path) as fh:
            rc = yaml.safe_load(fh) or {}
    except Exception:  # noqa: BLE001 - a malformed catalog must not abort the run
        return {}
    out: Dict[str, str] = {}
    for entry in rc.get("replicas", []) or []:
        lfn = entry.get("lfn")
        if not lfn:
            continue
        pfns = entry.get("pfns", []) or []
        # Prefer a non-local site for the same reason the transformation catalog does; fall
        # back to the first listed so a single-site catalog still resolves.
        chosen = next((p for p in pfns if p.get("site") not in (None, "local")), None)
        if chosen is None:
            chosen = pfns[0] if pfns else {}
        pfn = chosen.get("pfn")
        if pfn:
            out[str(lfn)] = str(pfn)
    return out


def load_cache_sites(run_dir: str) -> Dict[str, str]:
    """Parse <label>-0.cache -> {lfn: site}. First site seen per LFN wins."""
    sites: Dict[str, str] = {}
    for path in glob.glob(os.path.join(run_dir, "*.cache")):
        try:
            with open(path) as fh:
                for line in fh:
                    m = CACHE_SITE_RE.match(line.strip())
                    if m and m.group(1) not in sites:
                        sites[m.group(1)] = m.group(2)
        except OSError:
            continue
    return sites


def load_sub_requests(run_dir: str) -> Dict[str, dict]:
    """Parse Condor submit files -> {exec_job_id: {cpus, memory_mb, gpus, disk_kb}}."""
    requests: Dict[str, dict] = {}
    pattern = re.compile(
        r"^\s*request_(cpus|memory|gpus|disk)\s*=\s*([0-9.]+)", re.IGNORECASE
    )
    for path in glob.glob(os.path.join(run_dir, "**", "*.sub"), recursive=True):
        exec_job_id = os.path.basename(path)[:-len(".sub")]
        vals = {}
        try:
            with open(path) as fh:
                for line in fh:
                    m = pattern.match(line)
                    if m:
                        vals[m.group(1).lower()] = float(m.group(2))
        except OSError:
            continue
        if vals:
            requests[exec_job_id] = vals
    return requests


def load_lfn_sizes(conn: sqlite3.Connection) -> Dict[str, int]:
    """rc_meta 'size' entries -> {lfn: size_bytes}."""
    sizes: Dict[str, int] = {}
    rows = conn.execute(
        "SELECT l.lfn, m.value FROM rc_meta m "
        "JOIN rc_lfn l ON m.lfn_id = l.lfn_id WHERE m.\"key\" = 'size'"
    )
    for lfn, value in rows:
        try:
            sizes[lfn] = int(value)
        except (TypeError, ValueError):
            continue
    return sizes


# ---------------------------------------------------------------------------
# Stat helpers
# ---------------------------------------------------------------------------

def _stats(values: List[float]) -> dict:
    if not values:
        return {"min": None, "max": None, "mean": None, "stddev": None}
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / len(values)
    return {
        "min": min(values),
        "max": max(values),
        "mean": round(mean, 4),
        "stddev": round(math.sqrt(var), 4),
    }


# ---------------------------------------------------------------------------
# Per-run extraction
# ---------------------------------------------------------------------------

def extract_run(run_dir: str, job_types: List[str],
                default_site: str = "local") -> Tuple[List[dict], dict]:
    """Extract profiles for one run dir. Returns (profiles, run_summary)."""
    db_path = find_stampede_db(run_dir)
    if not db_path:
        raise FileNotFoundError(f"no *.stampede.db in {run_dir}")

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)

    # --- workflow-level info (root workflow) ---
    wf_row = conn.execute(
        "SELECT wf_id, wf_uuid, dax_label FROM workflow "
        "ORDER BY (parent_wf_id IS NULL) DESC, wf_id LIMIT 1"
    ).fetchone()
    if not wf_row:
        conn.close()
        raise ValueError(f"empty workflow table in {db_path}")
    wf_id, wf_uuid, dax_label = wf_row

    ws = conn.execute(
        "SELECT "
        " (SELECT MIN(timestamp) FROM workflowstate WHERE wf_id=? AND state='WORKFLOW_STARTED'),"
        " (SELECT MAX(timestamp) FROM workflowstate WHERE wf_id=? AND state='WORKFLOW_TERMINATED'),"
        " (SELECT status FROM workflowstate WHERE wf_id=? AND state='WORKFLOW_TERMINATED' "
        "  ORDER BY timestamp DESC LIMIT 1)",
        (wf_id, wf_id, wf_id),
    ).fetchone()
    wf_start, wf_end, wf_exit = ws
    wf_duration = (wf_end - wf_start) if (wf_start and wf_end) else None
    wf_status = ("successful" if wf_exit == 0 else "failed") if wf_exit is not None else "running"

    run_name = f"{dax_label}_{os.path.basename(os.path.normpath(run_dir))}"

    # --- auxiliary per-run maps ---
    uses_map = load_workflow_uses(run_dir)
    transformations_tc, containers_tc = load_transformation_catalog(run_dir)
    replicas_rc = load_replica_catalog(run_dir)
    cache_sites = load_cache_sites(run_dir)
    sub_requests = load_sub_requests(run_dir)
    lfn_sizes = load_lfn_sizes(conn)

    def file_entry(lfn: str, fallback_site: str) -> dict:
        return {
            "lfn": lfn,
            "site": cache_sites.get(lfn, fallback_site),
            "size_bytes": lfn_sizes.get(lfn, 0),
        }

    # --- network aggregates from stage-in/stage-out transfer jobs ---
    def transfer_agg(type_desc: str) -> dict:
        rows = conn.execute(
            "SELECT ji.site, COALESCE(ji.local_duration, 0) "
            "FROM job j JOIN job_instance ji ON j.job_id = ji.job_id "
            "WHERE j.wf_id = ? AND j.type_desc = ?",
            (wf_id, type_desc),
        ).fetchall()
        return {
            "jobs": len(rows),
            "transfer_duration_sec": round(sum(r[1] for r in rows), 3),
            "sites": sorted({r[0] for r in rows if r[0]}),
        }

    stage_in_agg = transfer_agg("stage-in-tx")
    stage_out_agg = transfer_agg("stage-out-tx")

    # --- per-job extraction ---
    placeholders = ",".join("?" * len(job_types))
    job_rows = conn.execute(
        f"SELECT job_id, exec_job_id, type_desc FROM job "
        f"WHERE wf_id = ? AND type_desc IN ({placeholders})",
        [wf_id] + job_types,
    ).fetchall()

    profiles: List[dict] = []
    for job_id, exec_job_id, type_desc in sorted(job_rows, key=lambda r: r[1]):
        instances = conn.execute(
            "SELECT job_instance_id, job_submit_seq, site, exitcode, "
            "COALESCE(local_duration, 0) "
            "FROM job_instance WHERE job_id = ? ORDER BY job_submit_seq",
            (job_id,),
        ).fetchall()
        if not instances:
            continue
        last = instances[-1]
        ji_id, _, site, exitcode, local_duration = last
        site = site or default_site

        # Main invocations of the final try (exclude dagman pre/post scripts)
        inv = conn.execute(
            "SELECT COALESCE(SUM(remote_duration), 0), SUM(remote_cpu_time), "
            "MAX(maxrss), MAX(exitcode) "
            "FROM invocation WHERE job_instance_id = ? "
            "AND transformation NOT LIKE 'dagman::%'",
            (ji_id,),
        ).fetchone()
        remote_duration, remote_cpu_time, maxrss_kb, inv_exit = inv
        if exitcode is None:
            exitcode = inv_exit

        # Queue time: SUBMIT -> EXECUTE of the final try
        ts = conn.execute(
            "SELECT "
            " (SELECT MIN(timestamp) FROM jobstate WHERE job_instance_id=? AND state='SUBMIT'),"
            " (SELECT MIN(timestamp) FROM jobstate WHERE job_instance_id=? AND state='EXECUTE')",
            (ji_id, ji_id),
        ).fetchone()
        submit_ts, execute_ts = ts
        queue_time = (execute_ts - submit_ts) if (submit_ts and execute_ts) else None

        # Runtime stats across all tries
        try_durations = []
        succeed = failed = 0
        for inst in instances:
            d = conn.execute(
                "SELECT COALESCE(SUM(remote_duration), 0) FROM invocation "
                "WHERE job_instance_id = ? AND transformation NOT LIKE 'dagman::%'",
                (inst[0],),
            ).fetchone()[0]
            if d:
                try_durations.append(float(d))
            if inst[3] == 0:
                succeed += 1
            elif inst[3] is not None:
                failed += 1
        rt = _stats(try_durations)

        main_tasks = conn.execute(
            "SELECT transformation, abs_task_id, executable, argv FROM invocation "
            "WHERE job_instance_id = ? AND abs_task_id IS NOT NULL",
            (ji_id,),
        ).fetchall()
        transformation = main_tasks[0][0] if main_tasks else ""
        # `executable` is the path the job ran under *inside its container*; `argv` is the
        # argument string as recorded. Both come from the same row as `transformation` so a
        # clustered job cannot pair one task's name with another task's command line.
        exec_path = (main_tasks[0][2] or "") if main_tasks else ""
        argv_raw = (main_tasks[0][3] or "") if main_tasks else ""
        try:
            argv_list = shlex.split(argv_raw)
        except ValueError:
            # An unbalanced quote in a recorded command line is not worth aborting a run
            # over, but silently returning [] would fabricate an argument-free job. Keep the
            # raw string so the converter can refuse it rather than run something wrong.
            argv_list = None
        tc_entry = transformations_tc.get(transformation, {})
        container = containers_tc.get(tc_entry.get("container")) if tc_entry else None

        # Abstract job id(s) -> input/output files from workflow.yml.
        # Prefer the DB's abs_task_id (handles custom job ids and clustered
        # jobs with multiple tasks); fall back to the _IDnnnnnnn suffix
        # convention in exec_job_id.
        # De-duplicated: duplicate invocation rows for one task would otherwise repeat that
        # task's arguments and produce a doubled command line that still looks single-task.
        abs_ids = ordered_task_ids(main_tasks, known=uses_map)
        if not abs_ids:
            m = ABS_ID_RE.search(exec_job_id)
            if m and m.group(1) in uses_map:
                abs_ids = [m.group(1)]
        uses = {"input": [], "output": [], "arguments": []}
        for aid in abs_ids:
            for ftype in ("input", "output"):
                for lfn in uses_map[aid][ftype]:
                    if lfn not in uses[ftype]:
                        uses[ftype].append(lfn)
            # Not de-duplicated and not sorted: an argument list is ordered and a value may
            # legitimately repeat.
            uses["arguments"].extend(uses_map[aid].get("arguments", []))

        # A CLUSTERED job bundles several tasks into one Condor job, and Pegasus runs them as
        # separate sequential invocations -- not as one merged command line. `transformation`
        # and `executable` above are taken from the FIRST task only, so concatenating every
        # task's arguments would describe a job that never existed: task A's executable run
        # with A's and B's flags together, the later ones overriding the earlier.
        #
        # There is no single (executable, argv) pair that represents such a job, so refuse to
        # invent one. `None` is the "arguments unknown" value that `ExecutionSpec.runnable()`
        # already rejects, so the job converts and schedules as before and declines to
        # *execute* -- rather than executing something plausible and wrong. Input and output
        # files are still unioned, which is correct for a cluster: it really does consume and
        # produce all of them.
        # From the DB, not from `abs_ids`: see `cluster_task_ids`. `abs_ids` is filtered by
        # what resolved against workflow.yml, so using it here would stop recognising
        # clusters exactly when the workflow metadata is missing.
        task_ids = cluster_task_ids(main_tasks)
        clustered = len(task_ids) > 1
        if clustered:
            argv_list = None
            argv_raw = ""
        elif uses["arguments"]:
            # The abstract workflow's declaration wins over the recorded argv, which is empty
            # for every compute job in these workflows. Falling back the other way keeps a run
            # whose abstract workflow is missing from losing arguments it did record.
            argv_list = uses["arguments"]
            argv_raw = " ".join(uses["arguments"])
        input_files = [file_entry(lfn, site) for lfn in uses["input"]]
        output_files = [file_entry(lfn, site) for lfn in uses["output"]]
        total_in = sum(f["size_bytes"] for f in input_files)
        total_out = sum(f["size_bytes"] for f in output_files)

        # Condor resource requests
        req = sub_requests.get(exec_job_id, {})

        profiles.append({
            # identity
            "run_name": run_name,
            "job_name": exec_job_id,
            "job_id_db": job_id,
            "job_type": type_desc,
            "transformation_db": transformation,
            # --- execution (what it takes to actually re-run this job) ---
            # In-container path Pegasus invoked, the host path of the code that was staged
            # there, and the container it ran in. Any of these may be None for a workflow
            # with no catalog or no container; a consumer must check rather than assume.
            "executable_db": exec_path or None,
            "argv_db": argv_list,
            "argv_raw_db": argv_raw or None,
            # >1 when this Condor job bundled several tasks. Such a job is deliberately not
            # executable (see above); it is recorded so a reader can tell "clustered" from
            # "arguments genuinely could not be parsed", which are both argv_db=None.
            "clustered_tasks_db": len(task_ids),
            "pfn_db": tc_entry.get("pfn"),
            "pfn_type_db": tc_entry.get("type"),
            "container_db": container,
            # The run's root inputs: {lfn: host path}. Run-level rather than per-job, and
            # repeated on each profile so a single job record stays self-describing -- the
            # converter reads it from whichever profile it is holding. Only the entries this
            # job actually reads are its own concern; the whole map is carried because the
            # converter bundles the union for the run.
            "replicas_db": replicas_rc,
            # workflow-level
            "wf_uuid_db": wf_uuid,
            "dax_label_db": dax_label or "",
            "wf_status": wf_status,
            "wf_duration_sec": round(wf_duration, 3) if wf_duration else None,
            # timing
            "submit_timestamp_db": submit_ts,
            "queue_time_sec_db": round(queue_time, 3) if queue_time is not None else None,
            "remote_duration_sec_db": round(float(remote_duration), 3) if remote_duration else None,
            "remote_cpu_time_sec_db": (
                round(float(remote_cpu_time), 3) if remote_cpu_time else None
            ),
            "kickstart_sec_stats": round(float(local_duration), 3) if local_duration else None,
            # resources
            "request_cpus_db": req.get("cpus", 0),
            "request_memory_mb_db": req.get("memory", 0),
            "request_gpus_db": int(req.get("gpus", 0)),
            "maxrss_kb_db": int(maxrss_kb) if maxrss_kb else 0,
            # files
            "input_files_db": input_files,
            "output_files_db": output_files,
            "total_input_size_bytes_db": total_in,
            "total_output_size_bytes_db": total_out,
            # outcome
            "exitcode_db": int(exitcode) if exitcode is not None else 0,
            "execution_site_db": site,
            # retries / stats
            "try_number_stats": len(instances),
            "runtime_min_sec_stats": rt["min"],
            "runtime_max_sec_stats": rt["max"],
            "runtime_mean_sec_stats": rt["mean"],
            "runtime_stddev_sec_stats": rt["stddev"],
            "runtime_succeed_stats": succeed,
            "runtime_failed_stats": failed,
            # network (workflow-level transfer aggregates, duplicated per job)
            "network_db": {
                "stage_in": {
                    "bytes_transferred": total_in,
                    "transfer_duration_sec": stage_in_agg["transfer_duration_sec"],
                    "sites": stage_in_agg["sites"],
                },
                "stage_out": {
                    "bytes_transferred": total_out,
                    "transfer_duration_sec": stage_out_agg["transfer_duration_sec"],
                    "sites": stage_out_agg["sites"],
                },
            },
        })

    conn.close()

    summary = {
        "run_dir": run_dir,
        "run_name": run_name,
        "wf_uuid": wf_uuid,
        "wf_status": wf_status,
        "makespan_sec": round(wf_duration, 3) if wf_duration else None,
        "jobs_extracted": len(profiles),
    }
    return profiles, summary


# ---------------------------------------------------------------------------
# Run-dir discovery
# ---------------------------------------------------------------------------

def discover_run_dirs(root: str) -> List[str]:
    """Find all dirs under root that contain a *.stampede.db file."""
    run_dirs = set()
    for dirpath, dirnames, filenames in os.walk(root):
        # skip scratch/staging trees and hidden dirs
        dirnames[:] = [d for d in dirnames if not d.startswith(".") and d != "scratch"]
        if any(f.endswith(".stampede.db") and not f.startswith("._") for f in filenames):
            run_dirs.add(dirpath)
            dirnames[:] = []  # don't descend into a run dir
    return sorted(run_dirs)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract Pegasus job profiles into the JSON format consumed "
                    "by pegasus_to_swarm_converter.py (--input-type json)."
    )
    parser.add_argument("--root", help="Directory tree to scan for *.stampede.db run dirs.")
    parser.add_argument("--submit-dir", action="append", default=[],
                        help="Specific Pegasus submit (run) directory. Repeatable.")
    parser.add_argument("--output", default="all_runs_jobs_profile.json",
                        help="Output JSON file. Default: all_runs_jobs_profile.json")
    parser.add_argument("--job-types", default=",".join(DEFAULT_JOB_TYPES),
                        help="Comma-separated stampede job type_desc values to extract. "
                             "Default: compute. (Others: stage-in-tx, stage-out-tx, "
                             "create-dir, cleanup, registration)")
    parser.add_argument("--include-failed-runs", action="store_true",
                        help="Include runs whose workflow did not finish successfully.")
    args = parser.parse_args()

    run_dirs = list(args.submit_dir)
    if args.root:
        run_dirs.extend(discover_run_dirs(args.root))
    run_dirs = sorted(set(run_dirs))
    if not run_dirs:
        print("No run directories found. Use --root or --submit-dir.", file=sys.stderr)
        return 1

    job_types = [t.strip() for t in args.job_types.split(",") if t.strip()]

    all_profiles: List[dict] = []
    summaries: List[dict] = []
    skipped: List[str] = []

    for rd in run_dirs:
        try:
            profiles, summary = extract_run(rd, job_types)
        except Exception as exc:  # noqa: BLE001 — report and continue
            print(f"WARN: skipping {rd}: {exc}", file=sys.stderr)
            skipped.append(rd)
            continue
        if summary["wf_status"] != "successful" and not args.include_failed_runs:
            print(f"SKIP (wf_status={summary['wf_status']}): {rd}", file=sys.stderr)
            skipped.append(rd)
            continue
        all_profiles.extend(profiles)
        summaries.append(summary)
        print(f"  {summary['run_name']:45s} jobs={summary['jobs_extracted']:4d} "
              f"status={summary['wf_status']} makespan={summary['makespan_sec']}s")

    if not all_profiles:
        print(f"ERROR: no job profiles extracted from {len(run_dirs)} run dir(s) "
              f"({len(skipped)} skipped). Nothing written. "
              "Use --include-failed-runs to include unsuccessful runs.",
              file=sys.stderr)
        return 1

    with open(args.output, "w") as fh:
        json.dump(all_profiles, fh, indent=2)

    print(f"\nExtracted {len(all_profiles)} job profiles from "
          f"{len(summaries)} runs ({len(skipped)} skipped) -> {args.output}")
    print("Next: python pegasus_to_swarm_converter.py "
          f"--input {args.output} --input-type json --output-dir converted_jobs/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
