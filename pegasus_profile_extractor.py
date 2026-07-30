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
    """Parse the abstract workflow -> {abs_job_id: {"input": [...], "output": [...]}}."""
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
        entry = {"input": [], "output": []}
        for use in job.get("uses", []) or []:
            ftype = use.get("type")
            lfn = use.get("lfn")
            if lfn and ftype in ("input", "output"):
                entry[ftype].append(lfn)
        uses_map[abs_id] = entry
    return uses_map


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
            "SELECT transformation, abs_task_id FROM invocation "
            "WHERE job_instance_id = ? AND abs_task_id IS NOT NULL",
            (ji_id,),
        ).fetchall()
        transformation = main_tasks[0][0] if main_tasks else ""

        # Abstract job id(s) -> input/output files from workflow.yml.
        # Prefer the DB's abs_task_id (handles custom job ids and clustered
        # jobs with multiple tasks); fall back to the _IDnnnnnnn suffix
        # convention in exec_job_id.
        abs_ids = [t[1] for t in main_tasks if t[1] in uses_map]
        if not abs_ids:
            m = ABS_ID_RE.search(exec_job_id)
            if m and m.group(1) in uses_map:
                abs_ids = [m.group(1)]
        uses = {"input": [], "output": []}
        for aid in abs_ids:
            for ftype in ("input", "output"):
                for lfn in uses_map[aid][ftype]:
                    if lfn not in uses[ftype]:
                        uses[ftype].append(lfn)
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
