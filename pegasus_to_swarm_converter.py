#!/usr/bin/env python3
"""
Pegasus-to-SwarmAgents Job Converter

Converts Pegasus workflow job profiles into SwarmAgents-compatible job JSON files,
a baseline manifest for comparison, and a conversion summary.

Optionally generates matching agent profiles (agent_profiles.json) and per-agent
YAML configs so that agents have the right DTNs and enough capacity to run the
converted jobs.

Input sources:
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
import json
import math
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


PARSERS = {
    "text": parse_text_file,
    "redis": parse_redis,
    "export": parse_export_file,
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


def _map_data_nodes(files_list: Optional[list]) -> Optional[list]:
    """Map Pegasus input/output file lists to SwarmAgents DataNode dicts.

    Per-job dedup: one DataNode per unique non-empty site name.
    Files with empty site are skipped.
    """
    if not files_list:
        return None

    seen_sites: Dict[str, str] = {}  # site -> first lfn
    for f in files_list:
        site = (f.get("site") or "").strip()
        if not site:
            continue
        if site not in seen_sites:
            seen_sites[site] = f.get("lfn", "")

    if not seen_sites:
        return None

    return [{"name": site, "file": lfn} for site, lfn in seen_sites.items()]


# ---------------------------------------------------------------------------
# Mapper — single profile → SwarmAgents job dict + warnings
# ---------------------------------------------------------------------------

def map_profile(profile: dict, job_number: int,
                min_wall_time: float = 0.1,
                default_cores: float = 1.0,
                min_ram_gb: float = 0.1,
                min_disk_gb: float = 1.0) -> Tuple[dict, List[str]]:
    """Convert a single Pegasus profile to a SwarmAgents job dict."""
    warnings: List[str] = []

    run_name = profile.get("run_name", "unknown")
    job_name = profile.get("job_name", f"job_{job_number}")
    job_id = f"{run_name}_{job_name}"

    wall_time, wt_warning = _map_wall_time(profile, min_wall_time)
    if wt_warning:
        warnings.append(wt_warning)

    capacities = _map_capacities(profile, default_cores, min_ram_gb, min_disk_gb)

    data_in = _map_data_nodes(profile.get("input_files_db"))
    data_out = _map_data_nodes(profile.get("output_files_db"))

    exitcode = int(profile.get("exitcode_db", 0) or 0)
    should_fail = exitcode != 0

    job = {
        "id": job_id,
        "wall_time": round(wall_time, 4),
        "capacities": capacities,
        "data_in": data_in if data_in else [],
        "data_out": data_out if data_out else [],
        "exit_status": exitcode,
        "should_fail": should_fail,
        "target_agent": None,
    }

    return job, warnings


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
    # Collect all DTN site names referenced by jobs
    required_sites: Dict[str, str] = {}  # name -> first file seen
    max_core = 0.0
    max_ram = 0.0
    max_disk = 0.0
    max_gpu = 0

    for job in jobs:
        c = job["capacities"]
        max_core = max(max_core, c["core"])
        max_ram = max(max_ram, c["ram"])
        max_disk = max(max_disk, c["disk"])
        max_gpu = max(max_gpu, c["gpu"])
        for dn in (job.get("data_in") or []) + (job.get("data_out") or []):
            if dn["name"] not in required_sites:
                required_sites[dn["name"]] = dn.get("file", "")

    # Build DTN list that every agent gets
    dtn_list = []
    for i, (site_name, _) in enumerate(sorted(required_sites.items()), 1):
        dtn_list.append({
            "name": site_name,
            "ip": f"192.168.200.{i}",
            "user": f"dtn_user_{site_name}",
            "connectivity_score": 1.0,
        })

    # Assign flavors
    flavors = _pick_flavor(num_agents)

    # Ensure every flavor can handle the largest job
    for flavor in flavors:
        if flavor["core"] < max_core:
            flavor["core"] = int(math.ceil(max_core))
        if flavor["ram"] < max_ram:
            flavor["ram"] = int(math.ceil(max_ram))
        if flavor["disk"] < max_disk:
            flavor["disk"] = int(math.ceil(max_disk))
        if flavor["gpu"] < max_gpu:
            flavor["gpu"] = max_gpu

    # Build profiles dict
    profiles: Dict[str, dict] = {}
    for agent_id in range(1, num_agents + 1):
        f = flavors[agent_id - 1]
        profiles[str(agent_id)] = {
            "core": f["core"],
            "ram": f["ram"],
            "disk": f["disk"],
            "gpu": f["gpu"],
            "dtns": copy.deepcopy(dtn_list),
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

            # DTNs
            cfg["dtns"] = copy.deepcopy(dtn_list)

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
        "dtn_sites": list(required_sites.keys()),
        "max_job_requirements": {
            "core": max_core,
            "ram": max_ram,
            "disk": max_disk,
            "gpu": max_gpu,
        },
    }


# ---------------------------------------------------------------------------
# Importable entry point (for run_test_v2.py integration)
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
) -> dict:
    """Convert Pegasus profiles to SwarmAgents job JSON files.

    This is the programmatic entry point used by run_test_v2.py.  It writes
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
    else:
        profiles = list(parse_text_file(input_path))

    if not profiles:
        raise ValueError(
            f"No profiles found in {input_path!r} (input_type={input_type!r}). "
            "Check the path and format."
        )

    os.makedirs(output_dir, exist_ok=True)

    baseline = BaselineBuilder()
    all_warnings: List[dict] = []
    sites_seen: Dict[str, int] = {}
    total_data_in = 0
    total_data_out = 0

    for i, (key, profile) in enumerate(profiles, 1):
        job, warnings = map_profile(
            profile, i,
            min_wall_time=min_wall_time,
            default_cores=default_cores,
            min_ram_gb=min_ram_gb,
            min_disk_gb=min_disk_gb,
        )

        # Write job file
        job_path = os.path.join(output_dir, f"job_{i}.json")
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
    baseline_path = os.path.join(output_dir, "pegasus_baseline.json")
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
        "warnings_count": warnings_count,
        "warnings": all_warnings,
    }
    summary_path = os.path.join(output_dir, "conversion_summary.json")
    with open(summary_path, "w") as fh:
        json.dump(summary, fh, indent=2)

    return {
        "jobs_written": len(profiles),
        "baseline_path": baseline_path,
        "summary_path": summary_path,
        "warnings_count": warnings_count,
    }


# ---------------------------------------------------------------------------
# Main converter (CLI)
# ---------------------------------------------------------------------------

def convert(args: argparse.Namespace):
    """Orchestrate parse → map → write."""
    # Select parser
    if args.input_type == "redis":
        profiles = list(parse_redis(args.input, args.redis_port))
    elif args.input_type == "export":
        profiles = list(parse_export_file(args.input))
    else:
        profiles = list(parse_text_file(args.input))

    if not profiles:
        print("No profiles found. Check --input and --input-type.")
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)

    baseline = BaselineBuilder()
    all_warnings: List[dict] = []
    sites_seen: Dict[str, int] = {}
    total_data_in = 0
    total_data_out = 0

    for i, (key, profile) in enumerate(profiles, 1):
        job, warnings = map_profile(
            profile, i,
            min_wall_time=args.min_wall_time,
            default_cores=args.default_cores,
            min_ram_gb=args.min_ram_gb,
            min_disk_gb=args.min_disk_gb,
        )

        # Write job file
        job_path = os.path.join(args.output_dir, f"job_{i}.json")
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
    baseline_path = os.path.join(args.output_dir, "pegasus_baseline.json")
    with open(baseline_path, "w") as fh:
        json.dump(baseline_data, fh, indent=2)

    # Generate agent configs if requested
    agent_info = None
    if args.generate_agent_configs:
        all_jobs = []
        for i in range(1, len(profiles) + 1):
            job_path = os.path.join(args.output_dir, f"job_{i}.json")
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
        "warnings_count": sum(len(w["warnings"]) for w in all_warnings),
        "warnings": all_warnings,
    }
    if agent_info:
        summary["agent_configs"] = agent_info
    summary_path = os.path.join(args.output_dir, "conversion_summary.json")
    with open(summary_path, "w") as fh:
        json.dump(summary, fh, indent=2)

    print(f"Converted {len(profiles)} Pegasus profiles → {args.output_dir}/")
    print(f"  Job files:   job_1.json .. job_{len(profiles)}.json")
    print(f"  Baseline:    pegasus_baseline.json ({len(baseline.runs)} runs)")
    print(f"  Summary:     conversion_summary.json ({summary['warnings_count']} warnings)")
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
        "--input-type", required=True, choices=["text", "redis", "export"],
        help="Input format: text (all_profiles.txt), redis, or export."
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
