#!/usr/bin/env python3
"""
Pegasus-to-SwarmAgents Job Converter

Converts Pegasus workflow job profiles into SwarmAgents-compatible job JSON files,
a baseline manifest for comparison, and a conversion summary.

Optionally generates matching agent profiles (agent_profiles.json) and per-agent
YAML configs so that agents have the right DTNs and enough capacity to run the
converted jobs.

Input sources:
  - json:   all_runs_jobs_profile.json (JSON array of profile dicts)
  - text:   all_profiles.txt (alternating Key/JSON lines)
  - redis:  Redis keys matching pegasus:profile:*
  - export: pegasus_export.json (lines of SET <key> '<json>')

Usage:
  python pegasus_to_swarm_converter.py --input all_profiles.txt --input-type text --output-dir converted_jobs/
  python pegasus_to_swarm_converter.py --input all_profiles.txt --input-type text --output-dir converted_jobs/ \
      --generate-agent-configs --num-agents 20 --base-config ../SwarmAgents/config_swarm_multi.yml
"""

import argparse
import copy
import hashlib
import json
import math
import shutil
import os
import random
import re
import sys
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Parsers — yield (redis_key, profile_dict) tuples
# ---------------------------------------------------------------------------

def parse_text_file(path: str) -> Iterator[Tuple[str, dict]]:
    """Parse all_profiles.txt: alternating 'Key: <key>' and JSON lines."""
    with open(path, "r") as fh:
        while True:
            key_line = fh.readline()
            if not key_line:
                break
            key_line = key_line.strip()
            if not key_line:
                continue
            if not key_line.startswith("Key:"):
                continue
            key = key_line[len("Key:"):].strip()
            json_line = fh.readline()
            if not json_line:
                break
            profile = json.loads(json_line.strip())
            yield key, profile


def parse_redis(host: str, port: int = 6379, pattern: str = "pegasus:profile:*") -> Iterator[Tuple[str, dict]]:
    """Scan Redis for keys matching pattern and yield (key, profile) tuples."""
    import redis
    r = redis.StrictRedis(host=host, port=port, decode_responses=True)
    for key in r.scan_iter(match=pattern):
        raw = r.get(key)
        if raw:
            yield key, json.loads(raw)


def parse_export_file(path: str) -> Iterator[Tuple[str, dict]]:
    """Parse pegasus_export.json: lines of  SET <key> '<json>'."""
    with open(path, "r") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            m = re.match(r"^SET\s+(\S+)\s+'(.+)'$", line)
            if m:
                key = m.group(1)
                profile = json.loads(m.group(2))
                yield key, profile


def parse_json_file(path: str) -> Iterator[Tuple[str, dict]]:
    """Parse all_runs_jobs_profile.json: a JSON array of profile dicts."""
    with open(path, "r") as fh:
        profiles = json.load(fh)
    for profile in profiles:
        run_name = profile.get("run_name", "unknown")
        job_name = profile.get("job_name", f"job_{profile.get('job_id_db', 0)}")
        key = f"pegasus:profile:{run_name}:{job_name}"
        yield key, profile


PARSERS = {
    "text": parse_text_file,
    "redis": parse_redis,
    "export": parse_export_file,
    "json": parse_json_file,
}


# ---------------------------------------------------------------------------
# Mapper helpers
# ---------------------------------------------------------------------------

def _map_wall_time(profile: dict, min_wall_time: float) -> Tuple[float, Optional[str]]:
    """Determine wall_time from profile with fallback chain."""
    warning = None

    # Primary: remote_duration_sec_db
    val = profile.get("remote_duration_sec_db")
    if val is not None and val > 0:
        return max(float(val), min_wall_time), warning

    # Fallback 1: kickstart_sec_stats
    val = profile.get("kickstart_sec_stats")
    if val is not None and val > 0:
        warning = "wall_time fallback: kickstart_sec_stats"
        return max(float(val), min_wall_time), warning

    # Fallback 2: remote_cpu_time_sec_db
    val = profile.get("remote_cpu_time_sec_db")
    if val is not None and val > 0:
        warning = "wall_time fallback: remote_cpu_time_sec_db"
        return max(float(val), min_wall_time), warning

    # Final fallback
    warning = "wall_time fallback: using min_wall_time default"
    return min_wall_time, warning


def _map_capacities(profile: dict, default_cores: float, min_ram_gb: float,
                    min_disk_gb: float) -> dict:
    """Map Pegasus resource fields to SwarmAgents capacities dict."""
    # Core
    core = float(profile.get("request_cpus_db", 0) or 0)
    if core <= 0:
        core = default_cores

    # RAM: MB → GB, fallback to maxrss_kb_db
    ram_mb = float(profile.get("request_memory_mb_db", 0) or 0)
    if ram_mb > 0:
        ram = ram_mb / 1024.0
    else:
        maxrss_kb = float(profile.get("maxrss_kb_db", 0) or 0)
        ram = maxrss_kb / 1024.0 / 1024.0 if maxrss_kb > 0 else 0.0
    ram = max(ram, min_ram_gb)

    # Disk: bytes → GB
    total_bytes = float(profile.get("total_input_size_bytes_db", 0) or 0)
    disk = total_bytes / (1024.0 ** 3) if total_bytes > 0 else 0.0
    disk = max(disk, min_disk_gb)

    # GPU
    gpu = int(profile.get("request_gpus_db", 0) or 0)

    return {
        "core": round(core, 4),
        "ram": round(ram, 4),
        "disk": round(disk, 4),
        "gpu": gpu,
    }


def make_dtn_resolver(dtn_map: Optional[Dict[str, str]] = None,
                      dtn_names: Optional[List[str]] = None,
                      dtn_scope: str = "file"):
    """Build a (site, lfn, group_key) -> DTN-name resolver.

    dtn_map renames Pegasus site names (e.g. {"local": "dtn1"}); sites not in
    the map pass through unchanged. dtn_names, if given, instead spreads files
    across the listed DTNs by a stable hash, so the same input always maps to
    the same DTN. dtn_names wins over dtn_map when both are provided.

    dtn_scope selects what that hash is taken over:
      "file" — spread a job's files across DTNs (same lfn -> same DTN anywhere).
      "job"  — place all of a job's files on one DTN, keyed by group_key.

    Prefer "job" when the jobs will be scheduled: agent feasibility requires an
    agent to hold *every* DTN a job references, and agents are given only a
    handful, so per-file spreading makes multi-file jobs unschedulable.
    """
    import hashlib

    def resolve(site: str, lfn: str, group_key: Optional[str] = None) -> str:
        if dtn_names:
            key = group_key if (dtn_scope == "job" and group_key) else lfn
            idx = int(hashlib.md5(key.encode()).hexdigest(), 16) % len(dtn_names)
            return dtn_names[idx]
        if dtn_map:
            return dtn_map.get(site, site)
        return site

    return resolve


def _map_data_nodes(files_list: Optional[list],
                    mode: str = "per-site",
                    resolve=None,
                    group_key: Optional[str] = None) -> Optional[list]:
    """Map Pegasus input/output file lists to SwarmAgents DataNode dicts.

    mode="per-site": one DataNode per unique non-empty DTN name (first lfn
    seen wins) — the historical behavior.
    mode="per-file": one DataNode per file, preserving every lfn (and its
    size_bytes when present).
    Files with empty site are skipped in both modes. *resolve* (see
    make_dtn_resolver) turns a Pegasus site + lfn into a DTN name.
    """
    if not files_list:
        return None
    if resolve is None:
        resolve = lambda site, lfn, group_key=None: site  # noqa: E731

    if mode == "per-file":
        nodes = []
        for f in files_list:
            site = (f.get("site") or "").strip()
            if not site:
                continue
            lfn = f.get("lfn", "")
            node = {"name": resolve(site, lfn, group_key), "file": lfn}
            if f.get("size_bytes") is not None:
                node["size_bytes"] = f["size_bytes"]
            nodes.append(node)
        return nodes or None

    seen_sites: Dict[str, str] = {}  # dtn name -> first lfn
    for f in files_list:
        site = (f.get("site") or "").strip()
        if not site:
            continue
        lfn = f.get("lfn", "")
        name = resolve(site, lfn, group_key)
        if name not in seen_sites:
            seen_sites[name] = lfn

    if not seen_sites:
        return None

    # The size of the lfn this node kept, carried like the per-file branch does. Dropping it
    # was silent data loss: `size_bytes` is the only per-file volume signal a job record has,
    # and per-site is the DEFAULT mode, so a workload converted without --dag-gating had none
    # at all (measured: 1549 of 1549 trace nodes bare, against 5194 of 5194 populated in a
    # per-file conversion of the same shape). The node already names one file; withholding
    # that file's size while keeping its name was arbitrary.
    sizes = {}
    for f in files_list:
        lfn = f.get("lfn", "")
        if lfn not in sizes and f.get("size_bytes") is not None:
            sizes[lfn] = f["size_bytes"]
    nodes = []
    for name, lfn in seen_sites.items():
        node = {"name": name, "file": lfn}
        if sizes.get(lfn) is not None:
            node["size_bytes"] = sizes[lfn]
        nodes.append(node)
    return nodes


# ---------------------------------------------------------------------------
# Mapper — single profile → SwarmAgents job dict + warnings
# ---------------------------------------------------------------------------

def map_profile(profile: dict, job_number: int,
                min_wall_time: float = 0.1,
                default_cores: float = 1.0,
                min_ram_gb: float = 0.1,
                min_disk_gb: float = 1.0,
                data_nodes_mode: str = "per-site",
                dtn_resolver=None) -> Tuple[dict, List[str]]:
    """Convert a single Pegasus profile to a SwarmAgents job dict."""
    warnings: List[str] = []

    run_name = profile.get("run_name", "unknown")
    job_name = profile.get("job_name", f"job_{job_number}")
    job_id = f"{run_name}_{job_name}"

    wall_time, wt_warning = _map_wall_time(profile, min_wall_time)
    if wt_warning:
        warnings.append(wt_warning)

    capacities = _map_capacities(profile, default_cores, min_ram_gb, min_disk_gb)

    data_in = _map_data_nodes(profile.get("input_files_db"), data_nodes_mode,
                              dtn_resolver, group_key=job_id)
    data_out = _map_data_nodes(profile.get("output_files_db"), data_nodes_mode,
                               dtn_resolver, group_key=job_id)

    exitcode = int(profile.get("exitcode_db", 0) or 0)
    should_fail = exitcode != 0

    job = {
        "id": job_id,
        # Which workflow (and which run of it) this job came from. Nothing in the scheduler
        # reads it yet — Job.from_dict ignores unknown keys — but every name collision below
        # is only decidable with it, and a per-workflow working directory would key on it.
        "workflow": run_name,
        "wall_time": round(wall_time, 4),
        "capacities": capacities,
        "data_in": data_in if data_in else [],
        "data_out": data_out if data_out else [],
        "exit_status": exitcode,
        "should_fail": should_fail,
        "target_agent": None,
    }

    execution = _map_execution(profile)
    if execution:
        job["execution"] = execution
        # A conversion that yields jobs which cannot execute must say so. Until now the only
        # per-job warning was wall-time clamping, so the 809-job trace reported
        # `warnings_count: 0` while holding 132 unrunnable jobs and 26 with no data at all —
        # a partial artifact describing itself as whole, which is the failure this project
        # guards against everywhere else. The same conversion now emits 158 warnings.
        refusal = _execution_refusal(execution)
        if refusal:
            warnings.append(f"NOT EXECUTABLE: {refusal}")

    # Reported for every job, executable or not: with no data nodes a job is feasible
    # everywhere, exercises no DTN or connectivity term, and contributes no DAG edge — so a
    # workload of them measures none of the things it appears to. Silence here is how the
    # eScience 547-job set reached a paper with `sites_seen: {}`.
    if not data_in and not data_out:
        warnings.append("no data nodes: neither inputs nor outputs were recorded, so this "
                        "job expresses no locality and can carry no DAG edge")

    return job, warnings


def _execution_refusal(execution: dict) -> Optional[str]:
    """Why the runner will refuse this execution block, or None when it will run it.

    This is a **deliberate second copy** of `ExecutionSpec.refusal_reason()`, and the
    duplication is forced: this script runs standalone on the Pegasus submit host, where the
    `swarm` package does not exist (verified — `import swarm` is a ModuleNotFoundError there),
    so importing the model would break the converter exactly where it is used.

    The first version of this warning was not a copy but a SUBSET — it tested only
    `arguments is None` and said nothing about a container declared with no image, which
    `runnable()` also refuses. A job like that converted "cleanly" and then refused at run
    time. A partial copy of a rule is worse than an honest one, because it looks like the
    rule.

    `tests/test_workflow_execution.py::TestRefusalRulesAgree` runs both over a matrix and
    fails if they ever disagree, which is the only thing that keeps two copies honest.
    """
    if not execution.get("path"):
        return "no in-container path to invoke"
    if execution.get("arguments") is None:
        n = execution.get("clustered_tasks")
        if n and int(n) > 1:
            return (f"clustered job wrapping {n} tasks: Pegasus ran them as separate "
                    f"sequential invocations, so no single command line describes it "
                    f"(docs/WORKFLOW_EXECUTION.md 1.3). Not a defect")
        return ("argv could not be parsed and the job is not clustered; the extraction is "
                "incomplete for this job")
    container = execution.get("container")
    if container is not None and not container.get("image"):
        return f"container {container.get('name', '')!r} is named but has no image"
    return None


def _map_execution(profile: dict) -> Optional[dict]:
    """Carry the executable, its arguments and its container through to the job record.

    Emitted only when the extractor found an in-container path to invoke. A profile from
    before the extractor recorded these — every profile written before 2026-09-16 — has none
    of these keys, so the job converts exactly as it did and simulates, which is what keeps
    the existing converted corpora valid rather than silently half-runnable.

    `arguments` preserves the empty/unknown distinction deliberately: `argv_db` is `None`
    when the recorded command line could not be parsed, and that `None` has to survive to
    `ExecutionSpec.runnable()`, which refuses it. Substituting `[]` here would turn "we do
    not know the arguments" into "there are no arguments" and run a different command than
    the one being compared against.
    """
    path = profile.get("executable_db")
    if not path:
        return None
    execution = {
        "transformation": profile.get("transformation_db") or "",
        "path": path,
        "arguments": profile.get("argv_db"),
    }
    # Carried so the refusal that follows can explain itself. A clustered job and a job whose
    # argv could not be parsed both arrive with `arguments: None` and both are refused, but
    # only one of them is a defect. Without this the record cannot tell them apart and the
    # designed refusal is investigated as a bug — which is exactly what happened.
    clustered = profile.get("clustered_tasks_db")
    if clustered and int(clustered) > 1:
        execution["clustered_tasks"] = int(clustered)
    pfn = profile.get("pfn_db")
    if pfn:
        execution["pfn"] = pfn
    pfn_type = profile.get("pfn_type_db")
    if pfn_type:
        execution["pfn_type"] = pfn_type
    container = profile.get("container_db")
    if container:
        # The extractor's key is `type`; the model calls it `kind`, because `type` shadows the
        # builtin and reads badly at every use site. Renamed here, at the boundary, so the
        # extractor stays a faithful record of the catalog's own vocabulary.
        execution["container"] = {
            "name": container.get("name") or "",
            "kind": container.get("type") or "",
            "image": container.get("image") or "",
            "image_site": container.get("image_site") or "",
        }
    return execution


# ---------------------------------------------------------------------------
# Workflow DAG
# ---------------------------------------------------------------------------

def apply_dag_gating(jobs: List[dict]) -> Tuple[int, List[str]]:
    """Give every job a data predicate naming the inputs another job produces.

    A Pegasus DAG's edges *are* file dependencies, so the graph is recoverable from the
    profiles alone: job B depends on job A exactly when B lists an input that A lists as an
    output. Nothing has to be extracted that the profile extractor does not already carry, and
    the scheduler is never told the graph — it only ever asks whether the names a job needs
    exist yet (`Repository.data_available`).

    Only names produced *inside this set* become predicates. A root job's inputs were staged in
    from outside the workflow and nothing in the run will ever produce them, so gating on them
    would hold the whole DAG at its root forever — the failure mode that makes an unattended
    cell look like a livelock.

    Returns (edges, roots).
    """
    producer: Dict[str, str] = {}
    for job in jobs:
        for dn in (job.get("data_out") or []):
            f = dn.get("file")
            if f:
                producer[f] = job["id"]

    edges = 0
    roots: List[str] = []
    for job in jobs:
        needs = []
        for dn in (job.get("data_in") or []):
            f = dn.get("file")
            if f and producer.get(f) and producer[f] != job["id"]:
                needs.append(f)
                edges += 1
        if needs:
            job["data_predicate"] = {"kind": "files", "files": sorted(set(needs))}
        else:
            roots.append(job["id"])
    return edges, roots


def colliding_outputs(jobs: List[dict]) -> Dict[str, List[str]]:
    """Logical file names that more than one job claims to produce.

    Two things make this worth reporting rather than resolving. The jobs of a run share ONE
    working directory — `data_in`/`data_out` are bare names, which is how job B finds job A's
    output — so two jobs producing `results.csv` overwrite each other on disk. And with
    `--dag-gating` the producer map is keyed by name, so whichever job is seen last becomes
    the producer and every consumer of that name waits on it, no matter which job it actually
    reads from.

    Within one workflow this is rare. Converting SEVERAL workflows into one bundle is where it
    bites: generic names (`output.csv`, `merged.dat`) recur across workflows that have nothing
    to do with each other, and the result is a cross-workflow edge nobody wrote — a job held
    until an unrelated workflow finishes, then reading that workflow's file.

    Returns {file name: [job ids]} for the collisions only.
    """
    producers: Dict[str, List[str]] = {}
    for job in jobs:
        for dn in (job.get("data_out") or []):
            f = dn.get("file")
            if f:
                # Keyed by BASENAME, because that is the name the file actually gets: the
                # working directory is flat and `stage_inputs` reduces every declared name to
                # its basename. Keying by the recorded string missed the case that matters
                # most — `runA/out.csv` and `runB/out.csv` are two keys and one file.
                producers.setdefault(os.path.basename(str(f)), []).append(job["id"])
    return {f: sorted(set(ids)) for f, ids in producers.items() if len(set(ids)) > 1}


def cross_workflow_edges(jobs: List[dict]) -> Dict[str, dict]:
    """File names one workflow produces and another workflow reads.

    This is the general form of every name collision in a multi-workflow bundle, and the only
    one that cannot be anything but a mistake: two workflows have no data relationship, so a
    name they share is a coincidence, not a dependency. Both mechanisms that act on names act
    on it anyway —

    * `--dag-gating` keys its producer map by name, so the reader is gated on the other
      workflow's job and waits for a run it has nothing to do with;
    * the working directory is flat and shared, so whichever job gets there first decides what
      the reader reads. `stage_inputs` never overwrites, and a producer writes directly.

    Checking `data_in` against `data_out` catches this whatever the name's provenance. An
    earlier version compared only the *declared replicas*, which are absent from most profiles
    (the replica catalog is not in the stampede DB), so the common case reported nothing.

    Keyed by basename, like everything else that decides a working-directory name.
    Returns {basename: {"read_by": [...], "produced_by": [...]}}.
    """
    produced: Dict[str, List[dict]] = {}
    for job in jobs:
        for dn in (job.get("data_out") or []):
            f = dn.get("file")
            if f:
                produced.setdefault(os.path.basename(str(f)), []).append(job)

    out: Dict[str, dict] = {}
    for job in jobs:
        for dn in (job.get("data_in") or []):
            f = dn.get("file")
            if not f:
                continue
            name = os.path.basename(str(f))
            foreign = [p for p in produced.get(name, [])
                       if p.get("workflow") != job.get("workflow")]
            if not foreign:
                continue
            entry = out.setdefault(name, {"read_by": [], "produced_by": []})
            entry["read_by"].append(job["id"])
            entry["produced_by"].extend(p["id"] for p in foreign)
    for entry in out.values():
        entry["read_by"] = sorted(set(entry["read_by"]))
        entry["produced_by"] = sorted(set(entry["produced_by"]))
    return out


def conflicting_replicas(jobs_and_profiles) -> Dict[str, dict]:
    """Names a job expects to be STAGED from the bundle while another job PRODUCES them.

    A declared replica is a file the run does not make: it is carried in `inputs/` and copied
    into the working directory before the job starts. If some other job also writes that name,
    the two are indistinguishable afterwards, and which one the consumer reads depends on who
    got there first — `stage_inputs` never overwrites, and a producer writes directly.

    With `--dag-gating` it is worse than a race: the producer map is keyed by name, so the
    consumer is gated on that job and waits for a workflow it has nothing to do with.

    Returns {basename: {"expected_by": [...], "produced_by": [...], "cross_workflow": bool}}.
    """
    produced: Dict[str, List[dict]] = {}
    for job, _profile in jobs_and_profiles:
        for dn in (job.get("data_out") or []):
            f = dn.get("file")
            if f:
                produced.setdefault(os.path.basename(str(f)), []).append(job)

    out: Dict[str, dict] = {}
    for job, profile in jobs_and_profiles:
        for lfn in (profile.get("replicas_db") or {}):
            name = os.path.basename(str(lfn))
            others = [p for p in produced.get(name, []) if p["id"] != job["id"]]
            if not others:
                continue
            entry = out.setdefault(name, {"expected_by": [], "produced_by": [],
                                          "cross_workflow": False})
            entry["expected_by"].append(job["id"])
            entry["produced_by"].extend(p["id"] for p in others)
            # Within one workflow this can be deliberate (a pre-seeded intermediate, which is
            # how a job that cannot run here — an external API call — is stood in for). Across
            # workflows it is never deliberate: they simply picked the same name.
            if any(p.get("workflow") != job.get("workflow") for p in others):
                entry["cross_workflow"] = True
    for entry in out.values():
        entry["expected_by"] = sorted(set(entry["expected_by"]))
        entry["produced_by"] = sorted(set(entry["produced_by"]))
    return out


# ---------------------------------------------------------------------------
# Baseline builder
# ---------------------------------------------------------------------------

class BaselineBuilder:
    """Accumulates per-run Pegasus data for the comparison manifest."""

    def __init__(self):
        self.runs: Dict[str, dict] = {}

    def add(self, profile: dict, swarm_job_id: str):
        run_name = profile.get("run_name", "unknown")

        if run_name not in self.runs:
            self.runs[run_name] = {
                "wf_uuid": profile.get("wf_uuid_db", ""),
                "wf_name": profile.get("dax_label_db", ""),
                "wf_status": profile.get("wf_status", ""),
                "makespan_sec": profile.get("wf_duration_sec", 0.0),
                "jobs": {},
            }

        network = profile.get("network_db", {})
        stage_in = network.get("stage_in", {})
        stage_out = network.get("stage_out", {})

        self.runs[run_name]["jobs"][swarm_job_id] = {
            "pegasus_job_name": profile.get("job_name", ""),
            "transformation": profile.get("transformation_db", ""),
            "execution_site": profile.get("execution_site_db", ""),
            "submit_timestamp": profile.get("submit_timestamp_db"),
            "queue_time_sec": profile.get("queue_time_sec_db"),
            "remote_duration_sec": profile.get("remote_duration_sec_db"),
            "remote_cpu_time_sec": profile.get("remote_cpu_time_sec_db"),
            "maxrss_kb": profile.get("maxrss_kb_db"),
            "exitcode": profile.get("exitcode_db"),
            "request_cpus": profile.get("request_cpus_db"),
            "request_memory_mb": profile.get("request_memory_mb_db"),
            "request_gpus": profile.get("request_gpus_db"),
            "total_input_bytes": profile.get("total_input_size_bytes_db"),
            "total_output_bytes": profile.get("total_output_size_bytes_db"),
            "stage_in": {
                "bytes": stage_in.get("bytes_transferred", 0),
                "duration_sec": stage_in.get("transfer_duration_sec", 0.0),
                "sites": stage_in.get("sites", []),
            },
            "stage_out": {
                "bytes": stage_out.get("bytes_transferred", 0),
                "duration_sec": stage_out.get("transfer_duration_sec", 0.0),
                "sites": stage_out.get("sites", []),
            },
            "try_count": profile.get("try_number_stats"),
            "runtime_stats": {
                "min": profile.get("runtime_min_sec_stats"),
                "max": profile.get("runtime_max_sec_stats"),
                "mean": profile.get("runtime_mean_sec_stats"),
                "stddev": profile.get("runtime_stddev_sec_stats"),
                "succeed": profile.get("runtime_succeed_stats"),
                "failed": profile.get("runtime_failed_stats"),
            },
        }

    def build(self, source: str) -> dict:
        total_jobs = sum(len(r["jobs"]) for r in self.runs.values())
        return {
            "metadata": {
                "conversion_timestamp": datetime.now(timezone.utc).isoformat(),
                "source": source,
                "total_runs": len(self.runs),
                "total_jobs": total_jobs,
            },
            "runs": self.runs,
        }


# ---------------------------------------------------------------------------
# Agent config generation
# ---------------------------------------------------------------------------

INSTANCE_FLAVORS = [
    {"name": "small",      "core": 2,  "ram": 8,   "disk": 100,  "gpu": 0},
    {"name": "medium",     "core": 4,  "ram": 16,  "disk": 250,  "gpu": 0},
    {"name": "large",      "core": 8,  "ram": 32,  "disk": 500,  "gpu": 4},
    {"name": "xtralarge",  "core": 16, "ram": 64,  "disk": 1000, "gpu": 4},
    {"name": "xxtralarge", "core": 32, "ram": 128, "disk": 1000, "gpu": 4},
]

DEFAULT_FLAVOR_PCTS = [0.40, 0.25, 0.15, 0.15, 0.05]


def _pick_flavor(num_agents: int) -> List[dict]:
    """Distribute agents across instance flavors proportionally."""
    flavors = []
    remaining = num_agents
    for i, flavor in enumerate(INSTANCE_FLAVORS):
        if i == len(INSTANCE_FLAVORS) - 1:
            count = remaining
        else:
            count = max(1, round(num_agents * DEFAULT_FLAVOR_PCTS[i]))
            count = min(count, remaining)
        remaining -= count
        flavors.extend([flavor] * count)
    random.shuffle(flavors)
    return flavors[:num_agents]


def generate_agent_configs(jobs: List[dict], num_agents: int,
                           output_dir: str, base_config_path: Optional[str],
                           db_host: str, db_port: int,
                           topology: str) -> dict:
    """Generate agent_profiles.json and per-agent YAML configs.

    Agents are sized from the standard flavor pool and given every DTN site
    that appears in the converted jobs so all jobs pass feasibility checks.
    """
    # What the jobs need, and the DTNs they name. Computed in swarm/utils/fleet_sizing.py and
    # nowhere else: generate_configs.py --size-to-jobs sizes a fleet the same way, and two
    # copies of "can this agent host this job" would drift into disagreeing about it.
    from swarm.utils.fleet_sizing import (dtn_base_scores, dtn_entries, fit_flavor,
                                          job_requirements)

    req = job_requirements(jobs)
    # One base score per DTN, one set of entries per agent: every agent holds every name (no
    # job is infeasible anywhere, so a failed agent's work can go to any other) with its own
    # connectivity to it. Feasibility tests the name, the cost model tests the score.
    dtn_bases = dtn_base_scores(req.dtns)
    flavors = [fit_flavor(f, req) for f in _pick_flavor(num_agents)]

    # Build profiles dict
    profiles: Dict[str, dict] = {}
    for agent_id in range(1, num_agents + 1):
        f = flavors[agent_id - 1]
        profiles[str(agent_id)] = {
            "core": f["core"],
            "ram": f["ram"],
            "disk": f["disk"],
            "gpu": f["gpu"],
            "dtns": dtn_entries(req.dtns, base_scores=dtn_bases),
        }

    # Write agent_profiles.json
    profiles_path = os.path.join(output_dir, "agent_profiles.json")
    with open(profiles_path, "w") as fh:
        json.dump(profiles, fh, indent=2)

    # Generate per-agent YAML configs if a base config was provided
    configs_dir = os.path.join(output_dir, "configs")
    if base_config_path and os.path.isfile(base_config_path):
        import yaml
        with open(base_config_path, "r") as fh:
            base_cfg = yaml.safe_load(fh)

        os.makedirs(configs_dir, exist_ok=True)

        # Build simple peer lists (mesh: all peers; ring: neighbors)
        all_ids = list(range(1, num_agents + 1))

        for agent_id in all_ids:
            cfg = copy.deepcopy(base_cfg)

            # Redis
            cfg.setdefault("redis", {})
            cfg["redis"]["host"] = db_host
            cfg["redis"]["port"] = db_port

            # Capacities
            p = profiles[str(agent_id)]
            cfg["capacities"] = {
                "cpu": p["core"],
                "core": p["core"],
                "gpu": p["gpu"],
                "ram": p["ram"],
                "disk": p["disk"],
                "bw": 0,
                "burst_size": 0,
                "unit": 0,
                "mtu": 0,
            }

            # DTNs — the SAME entries this agent's profile carries, not a fresh draw: the
            # fleet-fit check reads the per-agent config and the DTN pool reads the profile,
            # so a config and a profile that disagreed would describe two different agents.
            cfg["dtns"] = copy.deepcopy(profiles[str(agent_id)]["dtns"])

            # Topology — simple mesh or ring
            if topology == "ring":
                left = all_ids[(agent_id - 2) % num_agents]
                right = all_ids[agent_id % num_agents]
                peers = sorted(set([left, right]) - {agent_id})
            else:
                peers = [x for x in all_ids if x != agent_id]
            cfg["topology"] = {
                "peer_agents": peers,
                "type": topology,
                "parent": None,
                "children": None,
                "level": 0,
                "group": 0,
                "group_size": num_agents,
                "group_count": 1,
                "co_parents": None,
                "co_parent_groups": None,
                "primary_group": None,
            }

            # gRPC
            cfg["grpc"] = {
                "port": 20000 + agent_id,
                "host": "localhost",
            }

            # Runtime
            cfg.setdefault("runtime", {})
            cfg["runtime"]["total_agents"] = num_agents

            config_path = os.path.join(configs_dir, f"config_swarm_multi_{agent_id}.yml")
            with open(config_path, "w") as fh:
                yaml.dump(cfg, fh, default_flow_style=False, sort_keys=False)

    return {
        "agent_profiles_path": profiles_path,
        "configs_dir": configs_dir if base_config_path else None,
        "num_agents": num_agents,
        "flavors_used": sorted({f["name"] for f in flavors}),
        "dtn_sites": sorted(req.dtns),
        "max_job_requirements": {
            "core": req.core,
            "ram": req.ram,
            "disk": req.disk,
            "gpu": req.gpu,
        },
    }


# ---------------------------------------------------------------------------
# Importable entry point (for run_test.py integration)
# ---------------------------------------------------------------------------

def convert_pegasus_profiles(
    input_path: str,
    input_type: str = "text",
    output_dir: str = "jobs",
    redis_port: int = 6379,
    min_wall_time: float = 0.1,
    default_cores: float = 1.0,
    min_ram_gb: float = 0.1,
    min_disk_gb: float = 1.0,
    data_nodes_mode: str = "per-site",
    dtn_map: Optional[Dict[str, str]] = None,
    dtn_names: Optional[List[str]] = None,
    dtn_scope: str = "file",
    dag_gating: bool = False,
    bundle: bool = True,
    bundle_source_root: Optional[str] = None,
    bundle_images: bool = False,
) -> dict:
    """Convert Pegasus profiles to SwarmAgents job JSON files.

    This is the programmatic entry point used by run_test.py.  It writes
    job_*.json, pegasus_baseline.json, and conversion_summary.json into
    *output_dir* and returns a result dict.  It does NOT generate agent
    configs — that is handled by generate_configs.py.

    Parameters
    ----------
    input_path : str
        Path to profiles file (text/export) or Redis host.
    input_type : str
        One of "text", "redis", "export".
    output_dir : str
        Directory for output files (created if missing).
    redis_port : int
        Redis port (only used when input_type=="redis").
    min_wall_time, default_cores, min_ram_gb, min_disk_gb : float
        Mapping tunables passed through to ``map_profile()``.

    Returns
    -------
    dict with keys:
        jobs_written     (int)  – number of job files created
        baseline_path    (str)  – path to pegasus_baseline.json
        summary_path     (str)  – path to conversion_summary.json
        warnings_count   (int)  – total conversion warnings
    """
    # Select parser
    if input_type == "redis":
        profiles = list(parse_redis(input_path, redis_port))
    elif input_type == "export":
        profiles = list(parse_export_file(input_path))
    elif input_type == "json":
        profiles = list(parse_json_file(input_path))
    else:
        profiles = list(parse_text_file(input_path))

    if not profiles:
        raise ValueError(
            f"No profiles found in {input_path!r} (input_type={input_type!r}). "
            "Check the path and format."
        )

    os.makedirs(output_dir, exist_ok=True)

    dag_note = None
    if dag_gating and data_nodes_mode != "per-file":
        # per-site keeps one data node per job, which silently drops every edge after the
        # first: measured on the 11-job earthquake workflow, 10 of 13 edges survived. A
        # partial DAG runs and looks fine, so this is forced rather than warned about.
        dag_note = (f"--dag-gating forced --data-nodes per-file (was {data_nodes_mode!r}); "
                    "per-site collapses a job's inputs and loses DAG edges")
        data_nodes_mode = "per-file"

    dtn_resolver = make_dtn_resolver(dtn_map, dtn_names, dtn_scope)
    baseline = BaselineBuilder()
    all_warnings: List[dict] = []
    sites_seen: Dict[str, int] = {}
    total_data_in = 0
    total_data_out = 0

    # Two passes when gating: an edge is only known once every job's outputs are known.
    mapped: List[Tuple[int, dict, dict, List[str]]] = []
    for i, (key, profile) in enumerate(profiles, 1):
        job, warnings = map_profile(
            profile, i,
            min_wall_time=min_wall_time,
            default_cores=default_cores,
            min_ram_gb=min_ram_gb,
            min_disk_gb=min_disk_gb,
            data_nodes_mode=data_nodes_mode,
            dtn_resolver=dtn_resolver,
        )
        mapped.append((i, job, profile, warnings))

    dag_edges, dag_roots = (apply_dag_gating([j for _, j, _, _ in mapped])
                            if dag_gating else (0, []))
    # Reported, never resolved: the converter cannot know which of two same-named outputs a
    # consumer meant, and picking one silently is how an unrelated workflow's file ends up
    # feeding a job. See colliding_outputs().
    dag_collisions = colliding_outputs([j for _, j, _, _ in mapped])
    if dag_collisions:
        print(f"  WARNING:     {len(dag_collisions)} output file name(s) are produced by more "
              f"than one job; they share one flat working directory and, with --dag-gating, "
              f"one producer map. Listed in conversion_summary.json -> dag.colliding_outputs.")
    replica_conflicts = conflicting_replicas([(j, p) for _, j, p, _ in mapped])
    if replica_conflicts:
        crossing = sum(1 for v in replica_conflicts.values() if v["cross_workflow"])
        print(f"  WARNING:     {len(replica_conflicts)} staged input name(s) are also produced "
              f"by a job ({crossing} of them by a different workflow). Listed in "
              f"conversion_summary.json -> dag.replica_conflicts.")
    foreign_edges = cross_workflow_edges([j for _, j, _, _ in mapped])
    if foreign_edges:
        print(f"  WARNING:     {len(foreign_edges)} file name(s) are produced by one workflow "
              f"and read by another. Listed in conversion_summary.json -> "
              f"dag.cross_workflow_edges.")
    # Whether the names are acted upon at all: a simulated replay with no gating never touches
    # a filesystem and never consults a producer map, so its names are inert.
    execution_jobs = sum(1 for _, j, _, _ in mapped if j.get("execution"))
    # A corpus that can EXECUTE but was converted per-site is the dangerous combination, and
    # until now nothing said so: --dag-gating forces per-file, but a conversion with execution
    # blocks and no gating kept the per-site default and silently collapsed each job's file
    # list to one file per site. Measured on the 809-job trace: 1549 data nodes and 112 jobs
    # whose command line named a sibling's output they did not declare, against 119,596 nodes
    # and 0 such jobs from the same profiles per-file — 3 runs with a complete DAG rather than
    # 18. The jobs run either way, which is what makes it worth shouting about.
    if execution_jobs and data_nodes_mode != "per-file":
        print(f"  WARNING:     {execution_jobs} job(s) carry an execution block but "
              f"--data-nodes is {data_nodes_mode!r}. per-site keeps ONE file per site, so "
              f"these records under-declare what their jobs read and any DAG built from them "
              f"is incomplete. Use --data-nodes per-file for a corpus meant to execute.")

    # The payload is built in a staging directory and only promoted once everything that
    # can fail has succeeded. Clearing first — which is what this did — destroyed a working
    # bundle whenever the conversion after it raised, leaving neither the new one nor the
    # old. A conversion that fails must leave what was there alone.
    # EVERYTHING this conversion produces is written to a staging directory — job records,
    # bundle payload, baseline, manifest, summary — and the previous output is replaced only
    # at the very end, once nothing is left that can fail. Staging just the payload, which is
    # what this did first, moved the hole one step along: a failure while writing the job
    # files still destroyed the previous bundle and left a partial one.
    #
    # A conversion that dies part way leaves its staging directory behind rather than
    # damaging anything; `clear_previous_output` sweeps those on the next successful run.
    manifest = None
    write_dir = os.path.join(output_dir, f".convert-staging-{os.getpid()}")
    shutil.rmtree(write_dir, ignore_errors=True)
    os.makedirs(write_dir, exist_ok=True)
    if bundle:
        manifest = bundle_payload([(j, p) for _i, j, p, _w in mapped], write_dir,
                                  source_root=bundle_source_root,
                                  include_images=bundle_images)

    for i, job, profile, warnings in mapped:
        if manifest:
            job = rewrite_job_for_bundle(job, manifest)
        # Write job file
        job_path = os.path.join(write_dir, f"job_{i}.json")
        with open(job_path, "w") as fh:
            json.dump(job, fh, indent=2)

        baseline.add(profile, job["id"])

        if job.get("data_in"):
            total_data_in += len(job["data_in"])
            for dn in job["data_in"]:
                sites_seen[dn["name"]] = sites_seen.get(dn["name"], 0) + 1
        if job.get("data_out"):
            total_data_out += len(job["data_out"])
            for dn in job["data_out"]:
                sites_seen[dn["name"]] = sites_seen.get(dn["name"], 0) + 1

        if warnings:
            all_warnings.append({"job": job["id"], "job_number": i, "warnings": warnings})

    # Write baseline
    baseline_data = baseline.build(source=input_path)
    baseline_path = os.path.join(write_dir, "pegasus_baseline.json")
    with open(baseline_path, "w") as fh:
        json.dump(baseline_data, fh, indent=2)

    # Write summary
    warnings_count = sum(len(w["warnings"]) for w in all_warnings)
    summary = {
        "conversion_timestamp": datetime.now(timezone.utc).isoformat(),
        "source": input_path,
        "input_type": input_type,
        "total_profiles": len(profiles),
        "total_jobs_written": len(profiles),
        "total_runs": len(baseline.runs),
        "parameters": {
            "min_wall_time": min_wall_time,
            "min_ram_gb": min_ram_gb,
            "min_disk_gb": min_disk_gb,
            "default_cores": default_cores,
        },
        "data_node_stats": {
            "total_data_in_nodes": total_data_in,
            "total_data_out_nodes": total_data_out,
            "sites_seen": sites_seen,
        },
        "dag": {
            "gating": bool(dag_gating),
            "edges": dag_edges,
            "roots": dag_roots,
            "note": dag_note,
            "colliding_outputs": dag_collisions,
            "replica_conflicts": replica_conflicts,
            "cross_workflow_edges": foreign_edges,
            "execution_jobs": execution_jobs,
        },
        "warnings_count": warnings_count,
        "warnings": all_warnings,
    }

    if manifest is not None:
        # The bundle's own record: what was copied, from where, and its checksum. A bundle
        # whose payload is incomplete says so here AND on stdout, rather than looking whole
        # and failing per job later, on whichever agent happens to draw one.
        with open(os.path.join(write_dir, "manifest.json"), "w") as fh:
            json.dump(manifest, fh, indent=2)
    summary_path = os.path.join(write_dir, "conversion_summary.json")
    with open(summary_path, "w") as fh:
        json.dump(summary, fh, indent=2)

    # Install first, sweep second. Nothing is destroyed until the replacement is complete,
    # and the sweep skips what was just installed.
    installed = promote_staged_output(write_dir, output_dir)
    shutil.rmtree(write_dir, ignore_errors=True)
    stale = clear_previous_output(output_dir, installed=set(installed))
    if stale:
        print(f"  Cleared:     {stale} artefact(s) from a previous conversion")
    baseline_path = os.path.join(output_dir, os.path.basename(baseline_path))
    summary_path = os.path.join(output_dir, os.path.basename(summary_path))

    return {
        "jobs_written": len(profiles),
        "baseline_path": baseline_path,
        "summary_path": summary_path,
        "warnings_count": warnings_count,
    }


# ---------------------------------------------------------------------------
# Main converter (CLI)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Self-contained bundles
# ---------------------------------------------------------------------------

#: Layout inside an output directory. These names are also what `runtime.execution.bundle`
#: expands to as the three roots, so they are a contract, not a convention.
BUNDLE_CODE = "code"
BUNDLE_INPUTS = "inputs"
BUNDLE_IMAGES = "images"


#: Exactly the names a conversion writes into its output directory. Every removal decision
#: is made against this and nothing else — the output directory is shared with whatever else
#: the user keeps there, and this function deletes things.
_OWNED_NAME = re.compile(
    r"job_\d+\.json|conversion_summary\.json|pegasus_baseline\.json|manifest\.json"
    r"|code|inputs|images")

#: A copy displaced during promotion: an owned artefact plus the promoting process's pid.
#: Anchored and pid-numeric on purpose. A substring test for ".replacing-" deleted a user's
#: `notes.replacing-the-old-plan.txt`, `data.replacing-v2.csv` and an
#: `experiments.replacing-baseline/` directory — silently, in a function that otherwise takes
#: care to leave unowned files alone.
_DISPLACED_NAME = re.compile(r"^(?:" + _OWNED_NAME.pattern + r")\.replacing-\d+$")

#: A staging directory, likewise pid-suffixed rather than merely prefixed.
_STAGING_NAME = re.compile(r"^\.convert-staging-\d+$")


def _quiet_remove(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


def clear_previous_output(output_dir: str, keep: Optional[str] = None,
                          installed: Optional[set] = None) -> int:
    """Remove a previous conversion's artefacts from *output_dir*. Returns how many.

    The converter owns `job_*.json` in its output directory, and writing a new set does NOT
    replace the old one: a conversion producing fewer jobs than the last leaves the surplus
    behind, and every consumer of that directory then reads both. Measured on the slice — a
    4-job workflow written over a 400-job synthetic run left 406 files, the distributor
    pushed all of them, and the agents spent the run scheduling jobs from a campaign that had
    finished days earlier. Nothing in the run said so; it simply looked busy.

    The bundle directories go too, for the same reason: last conversion's executables are not
    this one's, and a stale `code/<transformation>/` is exactly the kind of payload that runs
    and produces plausible output.
    """
    removed = 0
    installed = installed or set()
    for name in os.listdir(output_dir) if os.path.isdir(output_dir) else []:
        # Never remove what this conversion just installed. Running after promotion is what
        # keeps the previous output intact until the new one is complete, and the only way
        # that is safe is if the sweep can tell the two apart.
        if name in installed:
            continue
        path = os.path.join(output_dir, name)
        if re.fullmatch(r"job_\d+\.json", name) or name in (
                "conversion_summary.json", "pegasus_baseline.json", "manifest.json"):  # noqa: E501
            os.remove(path)
            removed += 1
        elif name in (BUNDLE_CODE, BUNDLE_INPUTS, BUNDLE_IMAGES) and os.path.isdir(path):
            shutil.rmtree(path)
            removed += 1
        elif _DISPLACED_NAME.fullmatch(name):
            # A previous conversion's displaced copy, stranded by a rollback that could not
            # finish. Nothing else ever removes these, so they accumulate silently.
            shutil.rmtree(path, ignore_errors=True) if os.path.isdir(path) else _quiet_remove(path)
        elif _STAGING_NAME.fullmatch(name) and os.path.isdir(path):
            # Debris from a conversion that died part way. Harmless, but it accumulates.
            # `keep` is the staging directory of the conversion calling this, which is about
            # to be promoted — sweeping it would delete the output being installed.
            if keep and os.path.abspath(path) == os.path.abspath(keep):
                continue
            shutil.rmtree(path, ignore_errors=True)
    return removed


def _safe_component(name: str, fallback: str = "unnamed") -> str:
    """Reduce a workflow-supplied name to ONE safe path component.

    A transformation name comes from the workflow and is used as a directory name, so it is
    untrusted input on a *write* path: `../../etc/cron.d/pwn` joined onto the output
    directory escapes it, and the bundler then creates directories and copies files there.
    Everything outside a conservative set is replaced, and the result can contain no
    separator and cannot be a traversal segment.
    """
    cleaned = "".join(c if (c.isalnum() or c in "-_.") else "_" for c in str(name or ""))
    cleaned = cleaned.strip("._") or fallback
    return cleaned


def _sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def bundle_payload(jobs_and_profiles, output_dir: str, source_root: Optional[str] = None,
                   include_images: bool = False) -> dict:
    """Copy the executables and root inputs the jobs need INTO the output directory.

    The point is that a converted directory is the whole deliverable: copy it anywhere,
    point `runtime.execution.bundle` at it, and the jobs run. Without this a job record only
    *describes* its code, by an absolute path on a submit host that usually does not exist
    wherever the jobs end up.

    It also fixes something addressing alone cannot. A path says where code was, not which
    code it was: edit a script after a run and a replayed job silently executes a different
    program than the one that produced the baseline. Every copied file is checksummed into
    `manifest.json`, so what ran can be compared against what was measured.

    Rules:

    * **Refuse a partial bundle.** A missing executable raises rather than writing a
      directory that looks complete and fails per job, later, on whichever agent drew it.
    * **One copy per transformation**, not per job — a workflow runs the same executable
      many times, and the DAG's shape should not decide how many copies are made.
    * **Namespaced by transformation** (`code/<transformation>/<basename>`), because two
      transformations may ship a `run.py` and a flat directory would silently keep one.
    * **Images are referenced, not copied**, unless asked: they are gigabytes, and a bundle
      is something you copy around. The manifest records the URI and, when the image is
      reachable, its checksum, so the reference is verifiable without being carried.

    `source_root` rewrites the submit-host prefix when the converter runs somewhere the
    original paths do not resolve — the same job `path_rewrites` does at run time.
    """
    # `source_root` maps the submit-host tree onto a local copy. It is a PREFIX REPLACEMENT,
    # computed once, not a search.
    #
    # The first version tried progressively shorter suffixes and took the first that existed,
    # which at its last step matches on the BASENAME alone — so `/wf-a/bin/process.py` and
    # `/wf-b/bin/process.py` both resolved to `<root>/process.py`, and two workflows got the
    # same executable. That is the basename-search hazard this file warns about elsewhere, and
    # it silently undid the pfn-keying that exists to keep those workflows apart: distinct
    # keys, identical wrong content.
    #
    # Instead: `OLD=NEW` states the mapping outright, and a bare root is anchored to the
    # common parent of everything being bundled. Either way a path that does not resolve is
    # REPORTED MISSING rather than guessed at.
    if source_root and "=" in source_root:
        _old_prefix, _new_prefix = source_root.split("=", 1)
    elif source_root:
        # Every path the mapping will be applied to, not just the executables. Leaving
        # images out made the anchor too DEEP as well as missing them: with all code in
        # `/wf/bin` the common parent was `/wf/bin`, so `/wf/Apptainer/x.sif` fell outside
        # it and the code itself resolved one directory too high. Including them gives `/wf`,
        # and both halves resolve.
        _sources = [os.path.dirname(pfn) for job, _p in jobs_and_profiles
                    for pfn in [((job.get("execution") or {}).get("pfn") or "")] if pfn]
        _sources += [os.path.dirname(v) for _j, prof in jobs_and_profiles
                     for v in (prof.get("replicas_db") or {}).values() if v]
        _sources += [os.path.dirname(img[len("file://"):])
                     for job, _p in jobs_and_profiles
                     for img in [(((job.get("execution") or {}).get("container") or {})
                                  .get("image") or "")]
                     if img.startswith("file://")]
        try:
            _old_prefix = os.path.commonpath(_sources) if _sources else ""
        except ValueError:          # mixed absolute/relative — no common anchor
            _old_prefix = ""
        _new_prefix = source_root
    else:
        _old_prefix = _new_prefix = ""

    def _map(path: str) -> str:
        """The configured prefix mapping applied to *path*, or "" if it does not apply."""
        if not _old_prefix:
            return ""
        if path == _old_prefix or path.startswith(_old_prefix.rstrip("/") + "/"):
            rel = os.path.relpath(path, _old_prefix)
            # NOT `lstrip("./")`. That strips a CHARACTER SET, not a prefix, so any
            # dot-leading component became a different path: `.venv/bin/tool` turned into
            # `venv/bin/tool`, and if something existed there the bundler copied a
            # completely unrelated file while the manifest recorded the original source.
            # Reproduced before this fix. `relpath` under a verified prefix already yields a
            # clean downward path; nothing needs stripping.
            if rel == os.curdir or rel == os.pardir or rel.startswith(os.pardir + os.sep):
                return ""                   # not genuinely under the prefix
            candidate = os.path.normpath(os.path.join(_new_prefix, rel))
            # The prefix check above makes escape impossible, but the destination is
            # confirmed to sit under the root anyway: this decides which executable runs.
            root_norm = os.path.normpath(_new_prefix)
            prefix = root_norm if root_norm.endswith(os.sep) else root_norm + os.sep
            if candidate != root_norm and not candidate.startswith(prefix):
                return ""
            return candidate
        return ""

    def _resolve(path: str) -> str:
        """Where to read *path* from on this machine.

        The mapping is tried FIRST when one is configured. Checking `os.path.exists(path)`
        before it meant an incidental file at the submit-host location won over the tree the
        caller explicitly named — so converting on any machine that happens to have
        `/home/ubuntu/...` silently bundled that instead of the requested copy, and an
        explicit `OLD=NEW` was ignored for every path that happened to exist locally.
        """
        if not path:
            return path
        mapped = _map(path)
        if mapped and os.path.exists(mapped):
            return mapped
        if os.path.exists(path):
            return path
        # Neither resolved: report against the mapped location when there was one, since
        # that is where the caller asked us to look.
        return mapped or path

    code_dir = os.path.join(output_dir, BUNDLE_CODE)
    inputs_dir = os.path.join(output_dir, BUNDLE_INPUTS)
    manifest = {"code": {}, "inputs": {}, "images": {}, "missing": []}

    # Keyed by the PFN, not by the transformation name. A name is not unique: converting
    # several runs at once (which `--root` does, and the shipped multi-workflow profile
    # carries five labels) can put two different workflows' `process` in the same bundle,
    # and keying by name silently bundled whichever came first and handed it to both. The
    # pfn is what actually identifies the executable.
    pfn_to_transformations = {}
    replicas_wanted = {}
    replicas_conflicting = {}
    _dest_owner = {}
    _image_dest_owner = {}

    for job, profile in jobs_and_profiles:
        execution = job.get("execution") or {}
        transformation = execution.get("transformation") or profile.get("transformation_db") or ""
        pfn = execution.get("pfn")
        if pfn:
            pfn_to_transformations.setdefault(pfn, set()).add(transformation)
        for lfn, host_path in (profile.get("replicas_db") or {}).items():
            # setdefault ALONE was a silent drop: two workflows declaring the same logical
            # name for different files kept the first and never mentioned the second, so the
            # collision check below (which compares destinations) never saw a second source
            # and the loser's job was staged the winner's bytes. Same name, different file, is
            # a conflict — record it as one.
            previous = replicas_wanted.setdefault(lfn, host_path)
            if previous != host_path:
                replicas_conflicting.setdefault(lfn, {previous}).add(host_path)

    # A transformation name serving several distinct pfns needs distinct directories, or one
    # copy overwrites the other and every job of that name gets the survivor.
    names_in_use = {}
    for pfn in sorted(pfn_to_transformations):
        base = _safe_component(sorted(pfn_to_transformations[pfn])[0] or "unnamed")
        names_in_use.setdefault(base, []).append(pfn)

    # --- executables -----------------------------------------------------------------
    for base, pfns in sorted(names_in_use.items()):
        for pfn in sorted(pfns):
            # Disambiguate only when it is actually needed, so the common case keeps a
            # readable directory name.
            component = base if len(pfns) == 1 else f"{base}-{hashlib.sha256(pfn.encode()).hexdigest()[:8]}"
            src = _resolve(pfn)
            if not os.path.isfile(src):
                manifest["missing"].append({
                    "kind": "code", "transformation": base, "path": pfn,
                    "reason": ((f"not found at {src}. The bare --bundle-source-root form "
                                f"anchors on the common parent of what is being bundled "
                                f"({_old_prefix or 'none'}), which is ambiguous when those "
                                f"paths share a single directory; use "
                                f"--bundle-source-root OLD=NEW to state the mapping.")
                               if source_root else f"not found at {src}")})
                continue
            dest_dir = os.path.join(code_dir, component)
            # Belt and braces: the component is already sanitised, and the result is checked
            # to be inside the bundle before anything is created.
            if not os.path.normpath(dest_dir).startswith(os.path.normpath(code_dir) + os.sep):
                manifest["missing"].append({"kind": "code", "transformation": base,
                                            "path": pfn, "reason": "unsafe destination"})
                continue
            os.makedirs(dest_dir, exist_ok=True)
            dest = os.path.join(dest_dir, os.path.basename(src) or "executable")
            shutil.copy2(src, dest)
            manifest["code"][pfn] = {
                # Two forms on purpose. `bundled` is relative to the BUNDLE and is what a
                # reader (or a checksum audit) wants. `root_relative` is relative to the CODE
                # ROOT and is what goes in the job record, because `roots.code` already points
                # at code/. Conflating them put `code/` in the path twice and every job refused.
                "bundled": os.path.relpath(dest, output_dir),
                "root_relative": os.path.relpath(dest, code_dir),
                "transformation": base,
                "source": pfn,
                "sha256": _sha256(dest),
            }

    # --- root inputs -----------------------------------------------------------------
    for lfn, host_path in sorted(replicas_wanted.items()):
        if lfn in replicas_conflicting:
            # One logical name, several files. Whichever is carried, the jobs expecting the
            # others are staged it and run to completion on the wrong data.
            manifest["missing"].append({
                "kind": "input", "lfn": lfn, "path": host_path, "collision": True,
                "reason": "declared by more than one workflow with different files "
                          f"({', '.join(sorted(replicas_conflicting[lfn]))}); one logical name "
                          "cannot carry both"})
            continue
        src = _resolve(host_path)
        if not os.path.isfile(src):
            manifest["missing"].append({"kind": "input", "lfn": lfn, "path": host_path})
            continue
        os.makedirs(inputs_dir, exist_ok=True)
        dest = os.path.join(inputs_dir, _safe_component(os.path.basename(lfn), "input"))
        # Two distinct replicas can share a basename (`a/data.csv` and `b/data.csv`). The
        # working directory is flat, so they genuinely cannot coexist — but silently copying
        # the second over the first, while the manifest claims both are bundled, hands a job
        # the wrong file and says nothing. Report it and keep the first.
        previous = _dest_owner.get(dest)
        if previous is not None and previous != src:
            # Not merely "one of them is absent". The loser's job still names this file in its
            # data_in, and the runner resolves a bare name under roots.inputs — so it would be
            # staged the WINNER's bytes and run to completion on another workflow's data.
            # Flagged rather than described, so run_test.py can refuse the bundle instead of
            # leaving it to whoever reads manifest.json.
            manifest["missing"].append({
                "kind": "input", "lfn": lfn, "path": host_path, "collision": True,
                "reason": f"basename collides with {previous!r}; the working directory is "
                          f"flat so both cannot be staged, and the job that loses would be "
                          f"staged the other file rather than none"})
            continue
        _dest_owner[dest] = src
        shutil.copy2(src, dest)
        manifest["inputs"][lfn] = {
            "bundled": os.path.relpath(dest, output_dir),
            "root_relative": os.path.relpath(dest, inputs_dir),
            "source": host_path,
            "sha256": _sha256(dest),
        }

    # --- container images ------------------------------------------------------------
    for job, profile in jobs_and_profiles:
        container = ((job.get("execution") or {}).get("container")) or {}
        image = container.get("image") or ""
        name = container.get("name") or ""
        if not image or name in manifest["images"]:
            continue
        entry = {"image": image, "bundled": None, "sha256": None}
        local = _resolve(image[len("file://"):]) if image.startswith("file://") else None
        if local and os.path.isfile(local):
            entry["sha256"] = _sha256(local)
            if include_images:
                images_dir = os.path.join(output_dir, BUNDLE_IMAGES)
                os.makedirs(images_dir, exist_ok=True)
                dest = os.path.join(images_dir, os.path.basename(local))
                # Images land under their basename, and two workflows converted together can
                # each ship a `container.sif` built from different recipes. Copying the second
                # over the first runs one workflow's jobs inside the other's image — which
                # succeeds, and produces plausible output. Same rule as the replicas above:
                # report it, keep the first, and leave the loser pointing at its own path.
                previous = _image_dest_owner.get(dest)
                if previous is not None and previous != local:
                    manifest["missing"].append({
                        "kind": "image", "name": name, "path": local, "collision": True,
                        "reason": f"basename collides with {previous!r}; bundled images are "
                                  f"flat so both cannot be carried"})
                    manifest["images"][name] = entry
                    continue
                _image_dest_owner[dest] = local
                shutil.copy2(local, dest)
                entry["bundled"] = os.path.relpath(dest, output_dir)
                entry["root_relative"] = os.path.relpath(dest, images_dir)
        manifest["images"][name] = entry

    return manifest


def promote_staged_output(staging_dir: str, output_dir: str) -> List[str]:
    """Install a completed conversion, atomically per entry and with rollback.

    Returns the names installed.

    The ordering here is the whole point, and it took three attempts to get right. Clearing
    the previous output first — at any point before the new one is fully in place — means a
    failure afterwards leaves neither: not when it cleared at the start, and not when it
    cleared just before promoting, because promotion moves several entries and can fail
    between them.

    So nothing is removed before the replacement is installed. Each entry displaces its
    predecessor by renaming it aside first (atomic, same filesystem), and if any step fails
    everything already done is undone and the originals are put back. Surplus artefacts from
    a larger previous conversion are removed by the caller *afterwards*, when the new output
    is already complete and the worst a failure can do is leave extra files behind.
    """
    aside: List[tuple] = []
    promoted: List[str] = []

    def _discard(path: str) -> None:
        """Best effort, never raising: used only on the rollback path."""
        try:
            if os.path.isdir(path) and not os.path.islink(path):
                shutil.rmtree(path, ignore_errors=True)
            elif os.path.exists(path) or os.path.islink(path):
                os.remove(path)
        except OSError:
            pass

    try:
        for name in sorted(os.listdir(staging_dir)):
            src = os.path.join(staging_dir, name)
            dest = os.path.join(output_dir, name)
            if os.path.exists(dest) or os.path.islink(dest):
                shelved = f"{dest}.replacing-{os.getpid()}"
                _discard(shelved)
                os.rename(dest, shelved)
                aside.append((shelved, dest))
            shutil.move(src, dest)
            promoted.append(name)
    except BaseException:
        # BaseException, not Exception: an interrupt is the likeliest way a long conversion
        # stops, and `except Exception` let Ctrl-C walk out of here with the output half
        # installed and the previous copies stranded as `.replacing-*`.
        #
        # Every step below is best effort and cannot raise. A rollback that aborts on its
        # first problem leaves exactly the half-state it exists to prevent, and one that
        # raises replaces the real cause with its own — the reported error was "rollback
        # cleanup failed" while the actual failure went unmentioned.
        for name in promoted:
            _discard(os.path.join(output_dir, name))
        for shelved, dest in aside:
            try:
                if os.path.exists(shelved) or os.path.islink(shelved):
                    _discard(dest)
                    os.rename(shelved, dest)
            except OSError:
                continue
        raise
    for shelved, _dest in aside:
        _discard(shelved)
    return promoted


def rewrite_job_for_bundle(job: dict, manifest: dict) -> dict:
    """Point a job's execution block at the bundle instead of the submit host.

    Paths become **bundle-relative**, which is what `runtime.execution.bundle` resolves
    them against. A job whose executable could not be bundled keeps its original absolute
    pfn: it is still a faithful description, it simply will not run from this bundle, and
    silently rewriting it to a path that does not exist would be worse.
    """
    execution = job.get("execution")
    if not execution:
        return job
    # By pfn, because that is what identifies the executable — two workflows in one bundle
    # may share a transformation name and must not share its code.
    bundled = (manifest.get("code") or {}).get(execution.get("pfn") or "")
    if bundled:
        # Relative to the CODE ROOT, which is what `roots.code` names — not to the bundle.
        execution["pfn"] = bundled["root_relative"]
    container = execution.get("container") or {}
    name = container.get("name") or ""
    image_entry = (manifest.get("images") or {}).get(name)
    if image_entry and image_entry.get("root_relative"):
        container["image"] = image_entry["root_relative"]
    return job


def convert(args: argparse.Namespace):
    """Orchestrate parse → map → write."""
    # Select parser
    if args.input_type == "redis":
        profiles = list(parse_redis(args.input, args.redis_port))
    elif args.input_type == "export":
        profiles = list(parse_export_file(args.input))
    elif args.input_type == "json":
        profiles = list(parse_json_file(args.input))
    else:
        profiles = list(parse_text_file(args.input))

    if not profiles:
        print("No profiles found. Check --input and --input-type.")
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)

    dtn_map = None
    if args.dtn_map:
        dtn_map = dict(pair.split("=", 1) for pair in args.dtn_map.split(","))
    dtn_names = None
    if args.dtn_names:
        dtn_names = [n.strip() for n in args.dtn_names.split(",") if n.strip()]
    dtn_resolver = make_dtn_resolver(dtn_map, dtn_names, args.dtn_scope)

    baseline = BaselineBuilder()
    all_warnings: List[dict] = []
    sites_seen: Dict[str, int] = {}
    total_data_in = 0
    total_data_out = 0

    data_nodes_mode = args.data_nodes
    dag_note = None
    if args.dag_gating and data_nodes_mode != "per-file":
        dag_note = (f"--dag-gating forced --data-nodes per-file (was {data_nodes_mode!r}); "
                    "per-site collapses a job's inputs and loses DAG edges")
        data_nodes_mode = "per-file"
        print(f"  [dag] {dag_note}")

    # Two passes when gating: an edge is only known once every job's outputs are known.
    mapped = []
    for i, (key, profile) in enumerate(profiles, 1):
        job, warnings = map_profile(
            profile, i,
            min_wall_time=args.min_wall_time,
            default_cores=args.default_cores,
            min_ram_gb=args.min_ram_gb,
            min_disk_gb=args.min_disk_gb,
            data_nodes_mode=data_nodes_mode,
            dtn_resolver=dtn_resolver,
        )
        mapped.append((i, job, profile, warnings))

    dag_edges, dag_roots = (apply_dag_gating([j for _, j, _, _ in mapped])
                            if args.dag_gating else (0, []))
    # Reported, never resolved: the converter cannot know which of two same-named outputs a
    # consumer meant, and picking one silently is how an unrelated workflow's file ends up
    # feeding a job. See colliding_outputs().
    dag_collisions = colliding_outputs([j for _, j, _, _ in mapped])
    if dag_collisions:
        print(f"  WARNING:     {len(dag_collisions)} output file name(s) are produced by more "
              f"than one job; they share one flat working directory and, with --dag-gating, "
              f"one producer map. Listed in conversion_summary.json -> dag.colliding_outputs.")
    replica_conflicts = conflicting_replicas([(j, p) for _, j, p, _ in mapped])
    if replica_conflicts:
        crossing = sum(1 for v in replica_conflicts.values() if v["cross_workflow"])
        print(f"  WARNING:     {len(replica_conflicts)} staged input name(s) are also produced "
              f"by a job ({crossing} of them by a different workflow). Listed in "
              f"conversion_summary.json -> dag.replica_conflicts.")
    foreign_edges = cross_workflow_edges([j for _, j, _, _ in mapped])
    if foreign_edges:
        print(f"  WARNING:     {len(foreign_edges)} file name(s) are produced by one workflow "
              f"and read by another. Listed in conversion_summary.json -> "
              f"dag.cross_workflow_edges.")
    # Whether the names are acted upon at all: a simulated replay with no gating never touches
    # a filesystem and never consults a producer map, so its names are inert.
    execution_jobs = sum(1 for _, j, _, _ in mapped if j.get("execution"))
    # A corpus that can EXECUTE but was converted per-site is the dangerous combination, and
    # until now nothing said so: --dag-gating forces per-file, but a conversion with execution
    # blocks and no gating kept the per-site default and silently collapsed each job's file
    # list to one file per site. Measured on the 809-job trace: 1549 data nodes and 112 jobs
    # whose command line named a sibling's output they did not declare, against 119,596 nodes
    # and 0 such jobs from the same profiles per-file — 3 runs with a complete DAG rather than
    # 18. The jobs run either way, which is what makes it worth shouting about.
    if execution_jobs and data_nodes_mode != "per-file":
        print(f"  WARNING:     {execution_jobs} job(s) carry an execution block but "
              f"--data-nodes is {data_nodes_mode!r}. per-site keeps ONE file per site, so "
              f"these records under-declare what their jobs read and any DAG built from them "
              f"is incomplete. Use --data-nodes per-file for a corpus meant to execute.")

    # See the note in convert_pegasus_profiles: staged, then promoted, so a conversion that
    # fails leaves the previous bundle intact.
    # Staged in full, promoted last — see the note in convert_pegasus_profiles.
    manifest = None
    write_dir = os.path.join(args.output_dir, f".convert-staging-{os.getpid()}")
    shutil.rmtree(write_dir, ignore_errors=True)
    os.makedirs(write_dir, exist_ok=True)
    if not args.no_bundle:
        manifest = bundle_payload([(j, p) for _i, j, p, _w in mapped], write_dir,
                                  source_root=args.bundle_source_root,
                                  include_images=args.bundle_images)

    for i, job, profile, warnings in mapped:
        if manifest:
            job = rewrite_job_for_bundle(job, manifest)
        # Write job file
        job_path = os.path.join(write_dir, f"job_{i}.json")
        with open(job_path, "w") as fh:
            json.dump(job, fh, indent=2)

        # Track baseline
        baseline.add(profile, job["id"])

        # Track stats
        if job.get("data_in"):
            total_data_in += len(job["data_in"])
            for dn in job["data_in"]:
                sites_seen[dn["name"]] = sites_seen.get(dn["name"], 0) + 1
        if job.get("data_out"):
            total_data_out += len(job["data_out"])
            for dn in job["data_out"]:
                sites_seen[dn["name"]] = sites_seen.get(dn["name"], 0) + 1

        if warnings:
            all_warnings.append({"job": job["id"], "job_number": i, "warnings": warnings})

    # Write baseline
    baseline_data = baseline.build(source=args.input)
    baseline_path = os.path.join(write_dir, "pegasus_baseline.json")
    with open(baseline_path, "w") as fh:
        json.dump(baseline_data, fh, indent=2)

    # Generate agent configs if requested
    agent_info = None
    if args.generate_agent_configs:
        all_jobs = []
        for i in range(1, len(profiles) + 1):
            job_path = os.path.join(write_dir, f"job_{i}.json")
            with open(job_path) as fh:
                all_jobs.append(json.load(fh))

        agent_info = generate_agent_configs(
            jobs=all_jobs,
            num_agents=args.num_agents,
            output_dir=args.output_dir,
            base_config_path=args.base_config,
            db_host=args.db_host,
            db_port=args.db_port,
            topology=args.topology,
        )

    # Write summary
    summary = {
        "conversion_timestamp": datetime.now(timezone.utc).isoformat(),
        "source": args.input,
        "input_type": args.input_type,
        "total_profiles": len(profiles),
        "total_jobs_written": len(profiles),
        "total_runs": len(baseline.runs),
        "parameters": {
            "min_wall_time": args.min_wall_time,
            "min_ram_gb": args.min_ram_gb,
            "min_disk_gb": args.min_disk_gb,
            "default_cores": args.default_cores,
        },
        "data_node_stats": {
            "total_data_in_nodes": total_data_in,
            "total_data_out_nodes": total_data_out,
            "sites_seen": sites_seen,
        },
        "dag": {
            "gating": bool(args.dag_gating),
            "edges": dag_edges,
            "roots": dag_roots,
            "note": dag_note,
            "colliding_outputs": dag_collisions,
            "replica_conflicts": replica_conflicts,
            "cross_workflow_edges": foreign_edges,
            "execution_jobs": execution_jobs,
        },
        "warnings_count": sum(len(w["warnings"]) for w in all_warnings),
        "warnings": all_warnings,
    }
    if agent_info:
        summary["agent_configs"] = agent_info

    if manifest is not None:
        # The bundle's own record: what was copied, from where, and its checksum. A bundle
        # whose payload is incomplete says so here AND on stdout, rather than looking whole
        # and failing per job later, on whichever agent happens to draw one.
        with open(os.path.join(write_dir, "manifest.json"), "w") as fh:
            json.dump(manifest, fh, indent=2)
    summary_path = os.path.join(write_dir, "conversion_summary.json")
    with open(summary_path, "w") as fh:
        json.dump(summary, fh, indent=2)

    # Install first, sweep second — see convert_pegasus_profiles.
    installed = promote_staged_output(write_dir, args.output_dir)
    shutil.rmtree(write_dir, ignore_errors=True)
    stale = clear_previous_output(args.output_dir, installed=set(installed))
    if stale:
        print(f"  Cleared:     {stale} artefact(s) from a previous conversion")

    print(f"Converted {len(profiles)} Pegasus profiles → {args.output_dir}/")
    print(f"  Job files:   job_1.json .. job_{len(profiles)}.json")
    print(f"  Baseline:    pegasus_baseline.json ({len(baseline.runs)} runs)")
    print(f"  Summary:     conversion_summary.json ({summary['warnings_count']} warnings)")
    if manifest is not None:
        print(f"  Bundle:      {len(manifest['code'])} executable(s), "
              f"{len(manifest['inputs'])} input(s), {len(manifest['images'])} image(s) referenced")
        if manifest["missing"]:
            print(f"  INCOMPLETE:  {len(manifest['missing'])} payload file(s) not found — "
                  f"see manifest.json. These jobs will not run from this bundle.")
            for entry in manifest["missing"][:5]:
                print(f"                 {entry['kind']}: {entry['path']}")
    if sites_seen:
        print(f"  DTN sites:   {sites_seen}")
    if agent_info:
        print(f"  Agents:      {agent_info['num_agents']} agent profiles → agent_profiles.json")
        if agent_info["configs_dir"]:
            print(f"  Configs:     {agent_info['configs_dir']}/config_swarm_multi_*.yml")
        print(f"  DTN sites on agents: {agent_info['dtn_sites']}")
        print(f"  Max job needs: {agent_info['max_job_requirements']}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert Pegasus job profiles to SwarmAgents job JSON files."
    )
    parser.add_argument(
        "--input", required=True,
        help="Path to all_profiles.txt / pegasus_export.json, or Redis host."
    )
    parser.add_argument(
        "--input-type", required=True, choices=["text", "redis", "export", "json"],
        help="Input format: json (all_runs_jobs_profile.json), text, redis, or export."
    )
    parser.add_argument(
        "--output-dir", required=True,
        help="Directory to write converted job files, baseline, and summary."
    )
    parser.add_argument(
        "--redis-port", type=int, default=6379,
        help="Redis port (only used with --input-type redis). Default: 6379."
    )
    parser.add_argument(
        "--min-wall-time", type=float, default=0.1,
        help="Minimum wall_time in seconds. Default: 0.1."
    )
    parser.add_argument(
        "--min-ram-gb", type=float, default=0.1,
        help="Minimum RAM in GB. Default: 0.1."
    )
    parser.add_argument(
        "--min-disk-gb", type=float, default=1.0,
        help="Minimum disk in GB. Default: 1.0."
    )
    parser.add_argument(
        "--default-cores", type=float, default=1.0,
        help="Default core count when request_cpus_db is 0. Default: 1.0."
    )
    parser.add_argument(
        "--data-nodes", choices=["per-site", "per-file"], default="per-site",
        help="data_in/data_out granularity: 'per-site' dedups to one DataNode "
             "per site (historical behavior); 'per-file' keeps every "
             "input/output file with its size. Default: per-site."
    )
    parser.add_argument(
        "--dtn-map", type=str, default=None,
        help="Rename Pegasus sites to DTN names, e.g. 'local=dtn1,condorpool=dtn2'. "
             "Unlisted sites pass through unchanged."
    )
    parser.add_argument(
        "--dtn-names", type=str, default=None,
        help="Comma-separated DTN pool, e.g. 'dtn1,dtn2,dtn3'. Files are spread "
             "across the pool by a stable hash (see --dtn-scope), so the same "
             "input always maps to the same DTN. Overrides --dtn-map."
    )
    parser.add_argument(
        "--no-bundle", action="store_true",
        help="Do not copy executables and root inputs into the output directory. The jobs "
             "then only DESCRIBE their code, by absolute submit-host paths, and will not "
             "run anywhere those paths do not exist.")
    parser.add_argument(
        "--bundle-source-root",
        help="Where to find the workflow tree, when the converter runs somewhere the "
             "profiles' absolute submit-host paths do not resolve.")
    parser.add_argument(
        "--bundle-images", action="store_true",
        help="Also copy container images into the bundle. Off by default: they are "
             "gigabytes, and a bundle is meant to be copied around. Their checksums are "
             "recorded either way.")
    parser.add_argument(
        "--dag-gating", action="store_true",
        help="Reconstruct the workflow DAG from the profiles and emit it as a per-job data "
             "predicate, so a job is not selectable until the inputs another job produces "
             "exist. Edges are recovered by matching one job's output file names against "
             "another's inputs — no extra extraction is needed. Implies --data-nodes per-file, "
             "because per-site collapses a job's inputs and silently loses edges (measured: 10 "
             "of 13 survived on an 11-job workflow). Without this flag every job in a workflow "
             "is independent and the whole DAG is proposed at once."
    )
    parser.add_argument(
        "--dtn-scope", choices=["file", "job"], default="file",
        help="With --dtn-names, hash over the file name ('file', spreads a job's files "
             "across DTNs) or over the job ('job', puts all of a job's files on one DTN). "
             "Use 'job' for jobs that will be scheduled — feasibility requires an agent to "
             "hold every DTN a job references."
    )

    # Agent config generation
    parser.add_argument(
        "--generate-agent-configs", action="store_true",
        help="Generate agent_profiles.json and per-agent YAML configs."
    )
    parser.add_argument(
        "--num-agents", type=int, default=20,
        help="Number of agents to generate configs for. Default: 20."
    )
    parser.add_argument(
        "--base-config", type=str, default=None,
        help="Path to base SwarmAgents YAML config (e.g. config_swarm_multi.yml). "
             "Per-agent configs are derived from this template."
    )
    parser.add_argument(
        "--db-host", type=str, default="localhost",
        help="Redis host for generated agent configs. Default: localhost."
    )
    parser.add_argument(
        "--db-port", type=int, default=6379,
        help="Redis port for generated agent configs. Default: 6379."
    )
    parser.add_argument(
        "--topology", type=str, default="mesh",
        choices=["mesh", "ring", "star"],
        help="Topology for generated agent configs. Default: mesh."
    )
    return parser.parse_args()


if __name__ == "__main__":
    convert(parse_args())
