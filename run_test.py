#!/usr/bin/env python3.11
"""
run_test.py — unified local/remote test runner for SwarmAgents

Usage (local):
  ./run_test.py \
    --mode local \
    --agent-type resource \
    --agents 12 \
    --topology mesh \
    --jobs 500 \
    --db-host localhost \
    --jobs-per-proposal 10 \
    --starter ./swarm-multi-start.sh \
    --debug

Usage (remote):
  ./run_test.py \
    --mode remote \
    --agent-type llm \
    --agents 20 \
    --agents-per-host 4 \
    --topology ring \
    --jobs 1000 \
    --db-host 10.0.0.5 \
    --jobs-per-proposal 10 \
    --starter ./swarm-multi-start.sh \
    --agent-hosts-file agent_hosts.txt \
    --groups 5 --group-size 4 \
    --debug

Usage (with time-based shutdown):
  ./run_test.py \
    --mode local \
    --agent-type resource \
    --agents 20 \
    --topology mesh \
    --jobs 500 \
    --db-host localhost \
    --shutdown-after-seconds 300

Notes:
- For remote mode, you can provide --agent-hosts "agent-1,agent-2,agent-3" OR --agent-hosts-file.
- This script expects the repo directory on remote hosts at /root/SwarmAgents (adjust with --remote-repo-dir).
- It assumes passwordless SSH to those hosts.
- Use --shutdown-after-seconds for time-based test termination (bypasses bucket monitoring).
"""
from __future__ import annotations
import argparse, os, re, subprocess, sys, threading, time, math, shlex, csv, json, uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List

LINE_RE = re.compile(r"^state:\d+:\d+:(\d+):\s*\{([^}]*)\}\s*$")

# Set once teardown begins, so the dynamic-agent trigger thread (a daemon that is never
# joined) cannot launch agents after the stop sweep has already run.
_TEARDOWN = threading.Event()

# The base config every per-agent file is generated from. Named once: the same path resolved
# in two places is how `runtime.peer_expiry_seconds` came to have two different defaults.
BASE_CONFIG = "./config_swarm_multi.yml"

def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)

def run_blocking(cmd: list[str] | str, log_file: str | None = None, check: bool = True) -> subprocess.CompletedProcess:
    """
    Run a command to completion. If log_file is provided, write stdout/stderr there; else inherit.
    """
    if isinstance(cmd, str):
        shell = True
        printable = cmd
    else:
        shell = False
        printable = " ".join(shlex.quote(c) for c in cmd)

    log(f"$ {printable}")
    if log_file:
        with open(log_file, "w") as f:
            return subprocess.run(cmd, shell=shell, stdout=f, stderr=subprocess.STDOUT, text=True, check=check)
    else:
        return subprocess.run(cmd, shell=shell, text=True, check=check)

def run_once(cmd: list[str] | str) -> str:
    p = subprocess.run(cmd, shell=isinstance(cmd, str), capture_output=True, text=True, check=False)
    return (p.stdout or "") + (p.stderr or "")

def parse_bucket_set_count(text: str, bucket: int) -> int:
    total_count = 0
    for raw_line in text.splitlines():
        line = raw_line.strip()
        m = LINE_RE.match(line)
        if not m:
            continue
        b = int(m.group(1))
        if b != bucket:
            continue
        inside = m.group(2).strip()
        if not inside:
            continue
        # Format: "job_ids: 1,2,3" or set notation {'job:x', 'job:y', ...}
        parts = [x.strip() for x in inside.split(",") if x.strip()]
        # Count comma-separated entries and sum across all agents
        total_count += len(parts)
    return total_count

# ------------------------------
# Remote helpers
# ------------------------------
def ssh(host: str, cmd: str) -> int:
    return subprocess.call([
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "BatchMode=yes",
        host,
        cmd
    ])

def ssh_check(host: str, cmd: str) -> None:
    run_blocking([
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "BatchMode=yes",
        host,
        cmd
    ], check=True)

def scp_to(host: str, src: str, dst: str) -> None:
    run_blocking([
        "scp",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "BatchMode=yes",
        "-q",
        src,
        f"{host}:{dst}"
    ], check=True)

# ------------------------------
# Pegasus job conversion
# ------------------------------
# Written by generate_configs.py (--dtns) in the working directory: agent_id -> [{"name": ...}].
AGENT_DTNS_FILE = "agent_dtns.json"


def agent_dtn_pool(path: str = AGENT_DTNS_FILE) -> list[str]:
    """DTN names held by at least one agent *in this file*, sorted; [] if none are assigned.

    Blind to which agents this run launches, and to whether the file describes them at all —
    it is a repo-root artefact of the last generation. Callers converting jobs want
    `launched_dtn_pool()`, which scopes to the fleet that starts; this stays the last resort
    for when nothing else describes a fleet.

    generate_configs.assign_agent_dtns gives each agent 1-4 random picks from a 10-name pool, so
    a small fleet routinely leaves some pool names with no holder. Feasibility requires an agent
    to hold every DTN a job references, so a job hashed onto an unheld name can never run.
    Derive the pool from what was actually assigned, not from the pool it was drawn from.
    """
    p = Path(path)
    if not p.exists():
        return []
    with open(p) as f:
        assigned = json.load(f)
    names = set()
    for dtns in assigned.values():
        for d in dtns or []:
            name = d.get("name") if isinstance(d, dict) else d
            if name:
                names.add(str(name))
    return sorted(names)


def launched_dtn_pool(args) -> tuple:
    """(DTN names held by an agent THIS RUN LAUNCHES, reason the fleet could not be described).

    Same rule as `check_fleet_fits_jobs`, for the same reason: read the per-agent configs the
    agents actually load, and only the ids 1..(agents + dynamic). `agent_dtns.json` answers
    neither question. When the run generates its own fleet the two agree, which is why reading
    the file was harmless until now; under `--use-config-dir` they need not — the configs can
    have been generated on another machine, and the file routinely lists agents beyond this
    run's range (a 270-agent generation left behind, a 30-agent run).

    Converting against the wrong pool is silent in the way this codebase keeps rediscovering:
    a job hashed onto a DTN only an unlaunched agent holds is infeasible for every agent that
    starts, so it is never proposed, never fails, and shows up only as still pending at the end.

    An incomplete description is returned as such rather than guessed at — the caller drops to
    `local` (no DTN requirement) and says so, which cannot strand a job.
    """
    fleet, incomplete = _launched_fleet(args, AGENT_PROFILES_FILE)
    if fleet:
        names = set()
        for entry in fleet:
            names |= entry[5]
        return sorted(names), None
    # No fleet described. Whether that is "cannot read it" or "there is nothing to read" is the
    # difference between refusing to guess and there being nothing to refuse — so ask the disk,
    # not the reason string.
    described = (any(p.exists() for p in _launched_config_paths(args))
                 or Path(AGENT_PROFILES_FILE).exists())
    if incomplete and described:
        return [], incomplete
    # Nothing on disk describes a fleet at all, so scoping is impossible either way. This is
    # the pre-existing behaviour, named as a fallback rather than silently taken.
    names = agent_dtn_pool()
    if names:
        log(f"NOTE: no per-agent configs or profiles describe this fleet; taking the DTN pool "
            f"from {AGENT_DTNS_FILE}, which may describe a different one.")
    return names, None


def convert_pegasus_jobs(args) -> dict:
    """Convert Pegasus profiles into SwarmAgents job files in jobs/.

    Must run AFTER the fleet exists — generated by this run, or already on disk under
    `--use-config-dir` — because the job DTN pool is the set the LAUNCHED agents hold
    (`launched_dtn_pool`).
    """
    from pegasus_to_swarm_converter import convert_pegasus_profiles

    held, incomplete = launched_dtn_pool(args)
    if args.pegasus_dtn_names:
        dtn_names = [n.strip() for n in args.pegasus_dtn_names.split(",") if n.strip()]
        if incomplete:
            log(f"WARNING: cannot check --pegasus-dtn-names against the fleet — {incomplete}; "
                f"jobs hashed onto a DTN no launched agent holds can never be scheduled.")
        else:
            unheld = sorted(set(dtn_names) - set(held))
            if unheld:
                log(f"WARNING: --pegasus-dtn-names includes DTNs no agent holds: {','.join(unheld)}; "
                    f"jobs hashed onto them cannot be scheduled (fleet holds: {','.join(held) or 'none'})")
    elif incomplete:
        # Naming DTNs we cannot confirm anyone holds is the one outcome that strands jobs
        # invisibly. "local" is excluded from required DTNs by ResourceAgent.is_job_feasible,
        # so the jobs stay schedulable; the run loses the locality dimension and is told so.
        log(f"WARNING: cannot tell which DTNs this fleet holds — {incomplete}; converting onto "
            f"'local' instead, so the jobs carry no DTN requirement.")
        dtn_names = ["local"]
    elif held:
        dtn_names = held
    else:
        # No agent has any DTN (e.g. hierarchical runs, which do not pass --dtns), so any DTN
        # requirement would be unsatisfiable. "local" is excluded from required DTNs by
        # ResourceAgent.is_job_feasible, which makes the jobs data-location-free.
        dtn_names = ["local"]

    log(f"Converting Pegasus profiles: {args.pegasus_profiles} ({args.pegasus_input_type}) "
        f"[data-nodes={args.pegasus_data_nodes}, dtns={','.join(dtn_names)}] …")
    result = convert_pegasus_profiles(
        input_path=args.pegasus_profiles,
        input_type=args.pegasus_input_type,
        output_dir="jobs",
        data_nodes_mode=args.pegasus_data_nodes,
        dtn_names=dtn_names,
        # One DTN per job: agents hold only a few DTNs and feasibility requires all of a
        # job's DTNs, so spreading a job's files would make multi-file jobs unschedulable.
        dtn_scope="job",
        # Where the workflow's code actually lives on this fleet. Without it the bundler
        # looks for the executables at the paths the SUBMIT HOST recorded, which do not
        # exist here, and writes a bundle that converts cleanly and cannot run.
        bundle_source_root=getattr(args, "pegasus_bundle_source_root", None),
        # Without this a converted workflow carries no `data_predicate`, so nothing holds a
        # job until its parents have produced what it reads: the DAG is published as a flat
        # set and the agents schedule it in whatever order consensus lands. It is off by
        # default in the converter, and run_test never passed it — so every workflow run
        # through this path was unordered, which only shows up as a child failing on a file
        # its parent had not written yet.
        dag_gating=getattr(args, "pegasus_dag_gating", False),
    )
    log(f"Pegasus conversion: {result['jobs_written']} jobs written, "
        f"{result['warnings_count']} warnings")
    return result

# ------------------------------
# Preflight checks (remote mode)
# ------------------------------
def preflight_check(args, host_list: list[str]) -> None:
    """Verify SSH connectivity, repo dir, and python3.11 on each remote host."""
    log("Running preflight checks on remote hosts …")
    failures = []
    for host in host_list:
        # Check SSH + repo dir + python3.11 in a single command
        check_cmd = (
            f"test -d {shlex.quote(args.remote_repo_dir)} && "
            f"which python3.11 >/dev/null 2>&1"
        )
        rc = ssh(host, check_cmd)
        if rc != 0:
            failures.append(host)
            log(f"  FAIL: {host}")
        else:
            log(f"  OK:   {host}")

    if failures:
        raise SystemExit(
            f"Preflight failed for {len(failures)} host(s): {', '.join(failures)}. "
            f"Check SSH access, that {args.remote_repo_dir} exists, and python3.11 is installed."
        )
    log("Preflight checks passed.")

# ------------------------------
# Remote log collection
# ------------------------------
def collect_remote_logs(args, host_list: list[str]) -> None:
    """SCP agent logs from remote hosts into run_dir/hostname/ (best-effort)."""
    log("Collecting logs from remote hosts …")
    for host in host_list:
        dest_dir = os.path.join(args.run_dir, host.replace("/", "_"))
        os.makedirs(dest_dir, exist_ok=True)
        # Try log directory from config (e.g. swarm-multi/), then fall back to repo root
        for log_subdir in ("swarm-multi/", ""):
            scp_cmd = (
                f"scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
                f"-o BatchMode=yes -o ConnectTimeout=10 -q "
                f"{host}:{shlex.quote(args.remote_repo_dir)}/{log_subdir}agent-*.log "
                f"{shlex.quote(dest_dir)}/ 2>/dev/null"
            )
            rc = subprocess.call(scp_cmd, shell=True)
            if rc == 0:
                log(f"  Collected logs from {host}")
                break
        else:
            log(f"  WARN: No logs from {host} (rc={rc})")

# ------------------------------
# Config generation / cleanup
# ------------------------------
def generate_configs(args, agent_hosts_list: list[str]) -> Path:
    """
    Calls generate_configs.py once, emitting configs into args.config_dir.
    Returns absolute Path to config_dir.
    """
    cfg_dir = Path(args.config_dir).absolute()
    cfg_dir.mkdir(parents=True, exist_ok=True)

    # Write agent_hosts.txt if not provided and we computed hosts
    if args.agent_hosts_file:
        hosts_file = Path(args.agent_hosts_file)
    else:
        hosts_file = Path("agent_hosts.txt")
        with open(hosts_file, "w") as f:
            for h in agent_hosts_list:
                f.write(h + "\n")

    gen_args = [
        "python3.11", "generate_configs.py",
        str(args.agents), str(args.jobs_per_proposal),
        BASE_CONFIG, str(cfg_dir),
        args.topology, args.db_host, str(args.jobs),
        "--agent-hosts-file", str(hosts_file),
        "--agents-per-host", str(args.agents_per_host) if args.mode == "remote" else str(args.agents),
        "--agent-type", args.agent_type,
    ]
    if args.topology != "hierarchical":
        gen_args.append("--dtns")
    if args.groups:
        gen_args += ["--groups", str(args.groups)]
    if args.group_size:
        gen_args += ["--group-size", str(args.group_size)]
    if args.topology == "hierarchical":
        gen_args += ["--hierarchical-level1-agent-type", args.hierarchical_level1_agent_type]
        if hasattr(args, 'co_parents') and args.co_parents > 1:
            gen_args += ["--co-parents", str(args.co_parents)]
        # Forwarded whenever it was GIVEN, not only when > 1: an explicit 1 that is not
        # forwarded lets generate_configs apply its own default (2) instead, so the
        # launcher's idea of the fan-out and the generated fleet's would differ.
        if getattr(args, 'groups_per_coordinator', None) is not None:
            gen_args += ["--groups-per-coordinator", str(args.groups_per_coordinator)]
    if getattr(args, "delegation_policy", None):
        gen_args += ["--delegation-policy", args.delegation_policy]
    if getattr(args, "textfile_dir", None):
        gen_args += ["--textfile-dir", str(args.textfile_dir)]
    # Pass initial group size for dynamic agent addition
    if hasattr(args, 'initial_group_size') and args.initial_group_size is not None:
        gen_args += ["--initial-group-size", str(args.initial_group_size)]
    if hasattr(args, 'fit_all') and args.fit_all:
        gen_args += ["--fit-all"]
    if getattr(args, "agent_sites_file", None):
        gen_args += ["--agent-sites-file", str(args.agent_sites_file)]
    if getattr(args, "seed", None) is not None:
        gen_args += ["--seed", str(args.seed)]
    if getattr(args, "master_fleet_size", None):
        gen_args += ["--master-fleet-size", str(args.master_fleet_size)]
    if getattr(args, "pegasus_profiles", None) or getattr(args, "pegasus_jobs_dir", None):
        # jobs/ comes from convert_pegasus_jobs() (run right after this) or, with
        # --pegasus-jobs-dir, from a bundle converted elsewhere. Either way a synthetic set
        # generated here would be published alongside the workflow.
        gen_args.append("--skip-jobs")
    if getattr(args, "quantum_agents_pct", 0.0) > 0:
        gen_args += ["--quantum-agents-pct", str(args.quantum_agents_pct)]
    if getattr(args, "quantum_fraction", 0.0) > 0:
        gen_args += ["--quantum-fraction", str(args.quantum_fraction)]
    if getattr(args, "hybrid_fraction", 0.0) > 0:
        gen_args += ["--hybrid-fraction", str(args.hybrid_fraction)]

    log("Generating configs …")
    run_blocking(gen_args, check=True)
    return cfg_dir

def cleanup_between_runs(args) -> None:
    cmd = ["python3.11", "cleanup.py", "--agents", str(args.agents)]
    if args.db_host:
        cmd += ["--redis-host", args.db_host, "--cleanup-redis"]
    log("Cleanup between runs …")
    run_blocking(cmd, check=False)
    if not args.use_config_dir:
        # Deleting agent_profiles.json / agent_dtns.json is deliberate and load-bearing:
        # generate_configs.py REUSES an existing agent_dtns.json through a different code path
        # that consumes randomness differently, so a --seed run that starts dirty is not
        # reproducible. Never make these conditional.
        cmds = [["rm", "-rf", "agent_profiles.json"], ["rm", "-rf", "agent_dtns.json"]]
        # ... but agent_hosts.txt is an INPUT when --agent-hosts-file names it. Deleting it here
        # and then passing the same path to generate_configs.py is a FileNotFoundError, and that
        # is the documented remote invocation for the slice.
        hosts_arg = getattr(args, "agent_hosts_file", None)
        if not hosts_arg or Path(hosts_arg).resolve() != Path("agent_hosts.txt").resolve():
            cmds.insert(0, ["rm", "-rf", "agent_hosts.txt"])
        else:
            log(f"Keeping {hosts_arg} (it is this run's --agent-hosts-file input)")
        # Only delete jobs/ when this run is the one that fills it: a Pegasus conversion
        # overwrites it, and --pegasus-jobs-dir names a directory the run only READS — which
        # is usually the bundle on the shared export, so deleting it would destroy the
        # workflow rather than a regenerable set of synthetic jobs.
        if not getattr(args, 'pegasus_profiles', None) and not getattr(args, 'pegasus_jobs_dir', None):
            cmds.append(["rm", "-rf", "jobs"])
        for cmd in cmds:
            run_blocking(cmd, check=False)

# ------------------------------
# Agent start (local or remote)
# ------------------------------
def start_agents_local(args, agent_count: int = None, start_offset: int = 0) -> None:
    """
    Delegate to swarm-multi-start.sh locally.

    Args:
        args: Command-line arguments
        agent_count: Number of agents to start (defaults to args.agents)
        start_offset: Starting agent index offset (for dynamic addition)
    """
    if agent_count is None:
        agent_count = args.agents

    starter = Path(args.starter).absolute()
    if not starter.exists():
        raise FileNotFoundError(f"Starter script not found: {starter}")

    start_cmd = [
        "bash", str(starter),
        args.agent_type, str(agent_count),
        args.topology, str(args.jobs),
        args.db_host, str(args.jobs_per_proposal),
        "--use-config-dir",  # Always use pre-generated configs
    ]
    if args.groups:
        start_cmd += ["--groups", str(args.groups)]
    if args.group_size:
        start_cmd += ["--group-size", str(args.group_size)]
    if args.debug:
        start_cmd += ["--debug"]
    if start_offset > 0:
        start_cmd += ["--start-offset", str(start_offset)]

    phase = "initial" if start_offset == 0 else "dynamic"
    log_file = f"local_agents_{phase}_start.log"
    log(f"Starting {agent_count} {phase} agents locally …")
    # Run in background via nohup so we can proceed with the rest of the test
    run_blocking(["nohup"] + start_cmd + [">", log_file, "2>&1", "&"], check=False)

def shard_ranges(total: int, per_host: int) -> list[tuple[int,int]]:
    """
    Produce 1-based closed ranges [(start,end), ...] covering total with chunks of per_host.
    """
    ranges = []
    for start in range(1, total + 1, per_host):
        end = min(start + per_host - 1, total)
        ranges.append((start, end))
    return ranges

def start_agents_remote(args, agent_hosts_list: list[str], agent_count: int = None, start_offset: int = 0) -> None:
    """
    Copy only the config shards each host needs, then start via swarm-multi-start.sh remotely.

    Args:
        args: Command-line arguments
        agent_hosts_list: List of remote hosts
        agent_count: Number of agents to start (defaults to args.agents)
        start_offset: Starting agent index offset (for dynamic addition)
    """
    if agent_count is None:
        agent_count = args.agents

    starter = args.starter  # relative on remote (we cd into repo)
    cfg_dir = Path(args.config_dir).absolute()
    cfg_prefix = "config_swarm_multi_"

    # Calculate ranges for the agents we're starting
    base_idx = start_offset + 1
    ranges = [(base_idx + i * args.agents_per_host,
               min(base_idx + (i + 1) * args.agents_per_host - 1, base_idx + agent_count - 1))
              for i in range(math.ceil(agent_count / args.agents_per_host))]

    if len(agent_hosts_list) < len(ranges):
        raise ValueError(f"Need at least {len(ranges)} hosts for {agent_count} agents with agents_per_host={args.agents_per_host}")

    for i, (start_idx, end_idx) in enumerate(ranges):
        host = agent_hosts_list[i]
        count = end_idx - start_idx + 1
        log(f"[{host}] agents {start_idx}..{end_idx} (count={count})")

        # Prepare remote configs dir (don't wipe if this is dynamic addition)
        remote_cfg_dir = f"{args.remote_repo_dir}/configs"
        if start_offset == 0:
            ssh_check(host, f"mkdir -p {shlex.quote(remote_cfg_dir)} && rm -rf {shlex.quote(remote_cfg_dir)}/*")
        else:
            ssh_check(host, f"mkdir -p {shlex.quote(remote_cfg_dir)}")

        # Copy the needed config files
        for idx in range(start_idx, end_idx + 1):
            src = cfg_dir / f"{cfg_prefix}{idx}.yml"
            if not src.exists():
                raise FileNotFoundError(f"Missing config file: {src}")
            scp_to(host, str(src), f"{remote_cfg_dir}/")

        # Start via remote starter script
        forwarded = []
        if args.groups:
            forwarded += ["--groups", str(args.groups)]
        if args.group_size:
            forwarded += ["--group-size", str(args.group_size)]
        if args.debug:
            forwarded += ["--debug"]
        # Add start-offset for this host's agent range (0-based offset from 1-based start_idx)
        forwarded += ["--start-offset", str(start_idx - 1)]

        # The run id has to cross the ssh boundary explicitly: a remote agent is not a child
        # of this process, so it inherits nothing, and an unstamped payload is indistinguishable
        # from an older run's leftover.
        run_id = os.environ.get("SWARM_RUN_ID", "")
        start_cmd = (
            #f"source ~/.bash_profile && "
            f"source ~/.profile && "
            f"export SWARM_RUN_ID={shlex.quote(run_id)} && "
            f"cd {shlex.quote(args.remote_repo_dir)} && "
            f"nohup bash {shlex.quote(starter)} "
            f"{shlex.quote(args.agent_type)} {count} {shlex.quote(args.topology)} {args.jobs} "
            f"{shlex.quote(args.db_host)} {args.jobs_per_proposal} "
            f"--use-config-dir " + " ".join(shlex.quote(x) for x in forwarded) +
            f" > agent_{start_idx}_start.log 2>&1 &"
        )
        ssh_check(host, start_cmd)

    phase = "initial" if start_offset == 0 else "dynamic"
    log(f"Launched {agent_count} {phase} '{args.agent_type}' agents across {len(ranges)} remote host(s).")

# ------------------------------
# Dynamic agent addition
# ------------------------------
def add_dynamic_agents(args, host_list: list[str]) -> None:
    """
    Add agents dynamically during the test run.
    """
    log(f"Adding {args.dynamic_agents} dynamic agents to topology …")

    if args.mode == "local":
        start_agents_local(args, agent_count=args.dynamic_agents, start_offset=args.agents)
    else:
        # For remote mode, use additional hosts or wrap around existing hosts
        start_agents_remote(args, host_list, agent_count=args.dynamic_agents, start_offset=args.agents)

    log(f"Dynamic agents added. Total agents now: {args.agents + args.dynamic_agents}")

def wait_for_dynamic_trigger(args) -> None:
    """
    Wait for the appropriate trigger before adding dynamic agents.
    """
    trigger = args.dynamic_trigger

    if trigger == "time":
        log(f"Waiting {args.dynamic_delay} seconds before adding dynamic agents …")
        time.sleep(args.dynamic_delay)

    elif trigger == "bucket":
        log(f"Waiting for bucket {args.dynamic_trigger_bucket} to reach threshold {args.dynamic_trigger_threshold} …")
        while True:
            out = run_once(["python3.11", "dump_db.py", "--host", args.db_host, "--type", "redis", "--key", "state"])
            size = parse_bucket_set_count(out, args.dynamic_trigger_bucket)
            if size is not None and size >= args.dynamic_trigger_threshold:
                log(f"Bucket threshold reached (size={size}). Adding dynamic agents …")
                break
            time.sleep(args.check_interval)

    elif trigger == "jobs-completed":
        log(f"Waiting for {args.dynamic_trigger_jobs} jobs to be completed before adding dynamic agents …")
        # Monitor completed jobs bucket (typically bucket 3 or 4)
        while True:
            out = run_once(["python3.11", "dump_db.py", "--host", args.db_host, "--type", "redis", "--key", "state"])
            completed = parse_bucket_set_count(out, 8)  # Assuming bucket 3 is completed
            if completed >= args.dynamic_trigger_jobs:
                log(f"Job completion threshold reached ({completed} jobs). Adding dynamic agents …")
                break
            time.sleep(args.check_interval)

# ------------------------------
# Test flow: jobs, monitoring, stop, parse, plot
# ------------------------------
def jobs_dir(args) -> str:
    """Where the job records this run publishes live.

    `jobs/` by default — written by generate_configs.py (synthetic) or by
    convert_pegasus_jobs() (Pegasus profiles converted here). `--pegasus-jobs-dir` names a
    bundle converted somewhere else, typically the copy on the shared export; that directory
    is only ever READ, never generated into and never cleaned between runs.
    """
    return str(getattr(args, "pegasus_jobs_dir", None) or "jobs")


CFG_PREFIX = "config_swarm_multi_"          # what generate_configs.py writes, and what
                                            # swarm-multi-start.sh globs for


AGENT_PROFILES_FILE = "agent_profiles.json"


def _launched_config_paths(args) -> list:
    """The per-agent config files the agents of THIS run will read, in id order.

    Two things this must not get wrong, both learned the hard way:

    * **Which directory.** `swarm-multi-start.sh` globs `configs/` literally, so a local run
      launches from `./configs` whatever `--config-dir` says; a remote run launches from each
      host's own `configs/`, filled by copying `--config-dir` across.
    * **Which ids.** Only 1..(agents + dynamic agents). A config directory outlives the run that
      generated it — reusing one is what `--use-config-dir` is for — so it routinely describes a
      larger fleet than this run starts, and an agent nobody launches must not answer for it.
    """
    cfg_dir = (Path("configs") if getattr(args, "mode", None) == "local"
               else Path(getattr(args, "config_dir", "configs")))
    launched = (int(getattr(args, "agents", 0) or 0)
                + int(getattr(args, "dynamic_agents", 0) or 0))
    if launched > 0:
        return [cfg_dir / f"{CFG_PREFIX}{i}.yml" for i in range(1, launched + 1)]
    # No fleet size (a caller that is not the runner): read whatever is there.
    return sorted(cfg_dir.glob(f"{CFG_PREFIX}*.yml"))


def count_job_records(path: Path) -> int:
    """How many jobs a directory would publish: `job_*.json` and nothing else, which is exactly
    what `job_distributor.py` picks up."""
    if not path.is_dir():
        return 0
    return sum(1 for p in path.iterdir()
               if p.is_file() and p.name.startswith("job_") and p.name.endswith(".json"))


def _fleet_from_profiles(args, profiles_path: str) -> list:
    """The launched agents as `agent_profiles.json` describes them, or []."""
    profiles = _read_json(Path(profiles_path))
    if not profiles:
        return []
    launched = (int(getattr(args, "agents", 0) or 0)
                + int(getattr(args, "dynamic_agents", 0) or 0))
    fleet = []
    for aid, prof in sorted(profiles.items(), key=lambda kv: str(kv[0])):
        # The file lists every agent ever generated. A 270-agent generation left behind by an
        # earlier run satisfied this check for a 30-agent run, which then stalled.
        if launched > 0 and not (str(aid).isdigit() and 1 <= int(aid) <= launched):
            continue
        fleet.append((
            str(aid),
            float(prof.get("core", 0) or 0), float(prof.get("ram", 0) or 0),
            float(prof.get("disk", 0) or 0), float(prof.get("gpu", 0) or 0),
            {str(d.get("name")) for d in (prof.get("dtns") or []) if d.get("name")},
        ))
    return fleet


def _absent_config_is_fatal(args) -> bool:
    """Whether a missing per-agent config stops this run.

    It depends on the mode, because the two start paths do different things with one. A LOCAL
    run launches through `swarm-multi-start.sh`, which iterates the configs that exist and
    skips ids outside its range: the agent simply never starts and the rest of the run is fine.
    A REMOTE run copies each id's config to its host and `start_agents_remote` raises
    `FileNotFoundError` on the first one missing, so the run does not start at all.

    So the same absence is "one fewer agent" locally and "this run is not going to happen"
    remotely, and a reader that picks either answer for both is wrong half the time.
    """
    return getattr(args, "mode", None) != "local"


def _launched_fleet(args, profiles_path: str):
    """What the agents this run STARTS will have: [(id, core, ram, disk, gpu, {dtns})].

    Read from the per-agent configs, because those are what each agent actually loads.
    `agent_profiles.json` is a local artefact of the last generation: under `--use-config-dir`
    it need not describe these configs at all, and either way it lists every agent ever
    generated rather than the ones being started.

    Returns (fleet, reason_it_is_incomplete). Absent and unparseable are different, and what
    the starter does with each decides which is which:

    * a config that is **absent** means that agent never starts — `swarm-multi-start.sh`
      iterates the configs that exist and skips ids outside its range — so it is not part of
      this fleet, and not a reason to stop describing the rest. Falling back to
      `agent_profiles.json` here was worse than useless: that file can describe an older
      generation, so one missing config swapped every agent's real capacities for a stale
      guess;
    * a config that is **present and unparseable** means the fleet cannot be described, and a
      partial fleet is not partially usable — the agent that did not parse may be the only one
      that fits, so refusing and passing would both be guesses;
    * the profiles file is consulted only when no config described any agent at all.
    """
    from swarm.utils.yaml_strict import safe_load

    fleet, unreadable, absent = [], [], []
    for path in _launched_config_paths(args):
        try:
            with open(path) as fh:
                cfg = safe_load(fh) or {}
        except FileNotFoundError:
            if _absent_config_is_fatal(args):
                # Remote: start_agents_remote raises on this, so there is no fleet to describe
                # and any verdict about job sizes would be about agents that never exist.
                unreadable.append(f"{path} (missing; a remote run cannot start without it)")
            else:
                absent.append(str(path))
            continue
        except Exception as e:
            unreadable.append(f"{path} ({e})")
            continue
        caps = cfg.get("capacities") or {}
        fleet.append((
            path.stem,
            float(caps.get("core", 0) or 0), float(caps.get("ram", 0) or 0),
            float(caps.get("disk", 0) or 0), float(caps.get("gpu", 0) or 0),
            {str(d.get("name")) for d in (cfg.get("dtns") or []) if d.get("name")},
        ))
    if unreadable:
        return [], f"{len(unreadable)} launched config(s) could not be read: {unreadable[0]}"
    if fleet:
        if absent:
            log(f"NOTE: {len(absent)} config(s) in the launched range do not exist (e.g. "
                f"{absent[0]}); those agents will not start and are not counted as fleet.")
        return fleet, None
    # No config described any agent — nothing has been generated into this directory.
    from_profiles = _fleet_from_profiles(args, profiles_path)
    if from_profiles:
        return from_profiles, None
    return [], f"no per-agent configs and no readable {profiles_path}"


def _effective_coordinator_type(args):
    """What the coordinators of THIS run actually are — the value `run_meta.json` records.

    `--hierarchical-level1-agent-type` decides the coordinator tier only when this run
    generates the fleet. Under `--use-config-dir` it is inert: the configs were written by an
    earlier generation, possibly with the other type, and the flag's value is a claim about a
    fleet this run did not build. Since 2026-09-18 that default is `resource`, so the inert
    claim is also the *quiet* one — `evaluation/collect.py` reads this field to decide which
    agents were expected to run the LLM plane, and recording `resource` over a reused fleet of
    LLM coordinators drops the whole tier from the coverage denominator. A tier that went
    entirely dark would then read as a fully measured run.

    So under `--use-config-dir` the answer comes from the launched per-agent configs, and
    `None` (unknown) when they cannot answer — which `expected_llm_agent_ids` already handles
    by refusing to attribute roles rather than guessing in either direction.
    """
    declared = getattr(args, "hierarchical_level1_agent_type", None)
    if not getattr(args, "use_config_dir", False):
        return declared
    from swarm.utils.yaml_strict import safe_load
    found = set()
    for path in _launched_config_paths(args):
        try:
            with open(path) as fh:
                cfg = safe_load(fh) or {}
        except FileNotFoundError:
            continue
        except Exception:
            return None                      # a config that will not parse: do not guess
        if (cfg.get("topology") or {}).get("level") == 1:
            # A config with no `agent_type` launches as `resource`, and that is not a guess:
            # `swarm-multi-start.sh` reads the key with `'resource'` as its fallback, and
            # `main.py` does the same (its CLI value only wins when it is `llm`). Reading the
            # absence as "unknown" instead would block a verdict in collect.py over a tier
            # whose role is perfectly well defined.
            found.add(cfg.get("agent_type") or "resource")
    if len(found) != 1:
        # None found (not hierarchical, or no level-1 config) or the tier is mixed, which no
        # single value describes.
        return None if found else declared
    observed = found.pop()
    if observed != declared:
        log(f"NOTE: --hierarchical-level1-agent-type={declared} is inert under "
            f"--use-config-dir; the launched configs have {observed} coordinators, and that "
            f"is what run_meta.json records.")
    return observed


def _observed_groups_per_coordinator(args):
    """Child groups per coordinator, read from the configs — or None if they cannot say.

    A Level-1 config's `topology.children` holds GROUP ids, so its length is that
    coordinator's fan-out. Worth reading rather than echoing the flag, because the flag is not
    the last word: the generator caps it at the group count, and on a three-level hierarchy the
    *default* steps down to 1 (an explicit request is refused instead). Recording the request
    over a fleet that was built differently is the same defect as recording an inert
    coordinator type — `mab.top_k`/`delegation.top_k` are read against this number when
    deciding whether a delegation could ever have chosen.

    The maximum across coordinators, since an indivisible group count leaves the last one
    short (5 groups at 2 gives 2, 2, 1).
    """
    from swarm.utils.yaml_strict import safe_load
    widths = []
    for path in _launched_config_paths(args):
        try:
            with open(path) as fh:
                cfg = safe_load(fh) or {}
        except FileNotFoundError:
            continue
        except Exception:
            return None
        topo = cfg.get("topology") or {}
        if topo.get("level") == 1:
            widths.append(len(topo.get("children") or []))
    return max(widths) if widths else None


def _record_observed_fleet(args) -> None:
    """Rewrite the run_meta fields that describe the fleet, now that the fleet exists.

    `run_meta.json` is written before generation (it records the run id and argv, which the
    rest of the run needs), so the coordinator type and the fan-out in it are requests. Both
    can differ from what was built — an inert flag under `--use-config-dir`, a default fan-out
    stepped down on a three-level hierarchy, a request capped at the group count — and
    `evaluation/collect.py` reads both as facts about the fleet.

    A field the configs cannot answer keeps its requested value; a file that cannot be read or
    written is reported and skipped, since a run is not worth failing over its own metadata.
    """
    meta_path = Path(args.run_dir) / "run_meta.json"
    try:
        meta = json.loads(meta_path.read_text())
    except Exception as e:
        log(f"WARNING: could not re-read {meta_path} to record the observed fleet ({e}).")
        return
    observed = {
        "hierarchical_level1_agent_type": _effective_coordinator_type(args),
        "groups_per_coordinator": _observed_groups_per_coordinator(args),
    }
    for key, value in observed.items():
        if value is not None and meta.get(key) != value:
            log(f"run_meta: {key} = {value} (requested {meta.get(key)!r})")
        if value is not None:
            meta[key] = value
    try:
        meta_path.write_text(json.dumps(meta, indent=2))
    except Exception as e:
        log(f"WARNING: could not update {meta_path} ({e}).")


def check_fleet_fits_jobs(args, profiles_path: str = AGENT_PROFILES_FILE) -> None:
    """Refuse a fleet that cannot run the jobs it is about to be given.

    A converted workflow carries the resource requirements the jobs really had on Pegasus, and
    the names of the DTNs their data sits on. The fleet is sized independently — by
    `generate_configs.py`, from its own flavour pool — and with `--pegasus-jobs-dir` the
    conversion happened on another machine entirely, which knew nothing about this fleet. When
    the two do not meet, `is_job_feasible` returns False for every agent and the job is simply
    never selected: no error, no failure, just a run that ends with jobs still pending. That is
    indistinguishable from a scheduling problem, which is the wrong thing to go looking at.

    Two ways they fail to meet, and both are checked here because both are silent:

    * **Capacity.** One agent has to satisfy every dimension at once, so this compares whole
      profiles rather than per-dimension maxima — a fleet with a big-CPU agent and a big-RAM
      agent still cannot run a job needing both.
    * **DTNs.** Feasibility requires an agent holding *every* DTN a job references (`local` is
      excluded — it means the local filesystem). A bundle converted with `--dtn-names dtn1,…`
      against a fleet holding different names can never place a single job.
    """
    agents, incomplete = _launched_fleet(args, profiles_path)
    if incomplete or not agents:
        log(f"WARNING: cannot tell whether the jobs fit the agents — "
            f"{incomplete or 'no fleet to check against'}.")
        return

    # Distinct requirement SHAPES, not jobs: a workflow has a handful of them and a run has
    # tens of thousands of jobs, so this stays cheap however big the set is.
    shapes: dict = {}
    directory = Path(jobs_dir(args))
    for job_file in sorted(directory.glob("job_*.json")):
        job = _read_json(job_file)
        if not job:
            continue
        cap = job.get("capacities") or {}
        dtns = {str(dn.get("name")) for dn in
                (job.get("data_in") or []) + (job.get("data_out") or [])
                if dn.get("name")} - {"local"}
        key = (float(cap.get("core", 0) or 0), float(cap.get("ram", 0) or 0),
               float(cap.get("disk", 0) or 0), float(cap.get("gpu", 0) or 0),
               tuple(sorted(dtns)))
        shapes.setdefault(key, [0, job.get("id")])[0] += 1
    if not shapes:
        return

    unfittable = []
    for (core, ram, disk, gpu, dtns), (count, example) in sorted(shapes.items()):
        need = set(dtns)
        if not any(a_core >= core and a_ram >= ram and a_disk >= disk and a_gpu >= gpu
                   and need <= a_dtns
                   for _aid, a_core, a_ram, a_disk, a_gpu, a_dtns in agents):
            unfittable.append((core, ram, disk, gpu, need, count, example))
    if not unfittable:
        return

    core, ram, disk, gpu, need, count, example = unfittable[0]
    held = sorted({d for agent in agents for d in agent[5]})
    biggest = max(agents, key=lambda a: (a[1], a[2], a[3], a[4]))
    missing_dtns = sorted(need - set(held))
    raise SystemExit(
        f"No agent in this fleet can run {sum(u[5] for u in unfittable)} of the jobs "
        f"({len(unfittable)} distinct requirement shape(s)). Example: job {example!r} needs "
        f"core={core} ram={ram} disk={disk} gpu={gpu}"
        + (f" and DTN(s) {', '.join(sorted(need))}" if need else "")
        + f"; the largest of the {len(agents)} agent(s) this run starts has core={biggest[1]} "
        f"ram={biggest[2]} disk={biggest[3]} gpu={biggest[4]}"
        + (f", and no agent holds {', '.join(missing_dtns)} (fleet holds: "
           f"{', '.join(held) or 'none'})" if missing_dtns else "")
        + ". Those jobs would never be selected and the run would end with them still pending. "
        "Size the fleet to the workflow — pegasus_to_swarm_converter.py "
        "--generate-agent-configs sizes every agent to the largest job and gives it every DTN "
        "the jobs name — or convert with --dtn-names matching this fleet.")


def _run_execution_mode(args) -> str:
    """"simulate" / "real" / "unknown" — what this run's agents will do with a job that carries
    an execution block.

    **Every** config the run will use is read, not the first one: `--use-config-dir` means the
    files are used as they are on disk, so they need not agree, and one agent configured `real`
    makes the run one that executes. Reading only the first answered for a fleet rather than
    about it.

    Three states, and the middle one is why this is not a one-liner:

    * an **absent** `runtime.execution.mode` is `simulate` (`runner.resolve_mode` owns that
      default) — a real answer about a run that touches nothing;
    * a config that cannot be **read** is no answer at all, and the run starts regardless under
      `--use-config-dir`, because the agents read their own per-agent files and never this
      process's copy. So any unreadable config leaves the answer `unknown`, which the caller
      treats as executing;
    * `real` anywhere wins immediately — nothing another file says can make the run not
      execute.

    Only `config_swarm_multi_*.yml` is considered, because that is what the starter launches
    agents from. Globbing `*.yml` let an unrelated file in the directory — which has no runtime
    block and so reads as `simulate` — answer for configs that say `real`.

    And only the ones this run will actually LAUNCH, which is ids 1..(agents + dynamic agents).
    A config directory outlives the run that generated it — `--use-config-dir` exists to reuse
    one — so it routinely holds configs for a larger fleet than this run starts. Reading those
    let an agent nobody launches refuse a perfectly good simulated run.

    **Which directory the agents read is not the same one in both modes**, and `--config-dir`
    does not always name it. `swarm-multi-start.sh` globs `configs/` literally, so a local run
    launches from `./configs` whatever `--config-dir` says; a remote run launches from each
    host's own `configs/`, which is filled by copying `--config-dir` across. Reading
    `--config-dir` for a local run therefore answered from a directory no agent opens.
    """
    from swarm.execution.runner import resolve_mode
    from swarm.utils.yaml_strict import safe_load

    # Under --use-config-dir the per-agent files are the truth; otherwise they do not exist
    # yet (this runs before generate_configs) and the base config they will be derived from is.
    candidates = (_launched_config_paths(args) if getattr(args, "use_config_dir", False)
                  else [Path(BASE_CONFIG)])

    modes, unreadable = set(), []
    for candidate in candidates:
        try:
            with open(candidate) as fh:
                cfg = safe_load(fh) or {}
        except FileNotFoundError:
            # Local: that agent never starts (the starter iterates the configs that EXIST), so
            # it says nothing about this run — counting it as "cannot tell" refused runs whose
            # every launched agent was perfectly readable. Remote: the run aborts on it, so
            # nothing can be concluded about a run that will not happen.
            if _absent_config_is_fatal(args):
                unreadable.append(f"{candidate} (missing; a remote run cannot start without it)")
            continue
        except Exception as e:                       # present, but unparseable
            unreadable.append(f"{candidate} ({e})")
            continue
        modes.add(resolve_mode((cfg.get("runtime") or {}).get("execution")))
    if "real" in modes:
        return "real"
    if unreadable:
        log(f"WARNING: could not read {len(unreadable)} config(s): "
            f"{'; '.join(unreadable[:3])}")
        return "unknown"
    if modes:
        return "simulate"
    log(f"WARNING: no {CFG_PREFIX}*.yml found to read runtime.execution.mode from "
        f"({'--use-config-dir ' + str(getattr(args, 'config_dir', 'configs')) if getattr(args, 'use_config_dir', False) else BASE_CONFIG}).")
    return "unknown"


def _read_json(path: Path):
    """The JSON at *path*, or None when it is absent or unreadable (which is reported by the
    caller as a check that could not run, never as a check that passed)."""
    try:
        with open(path) as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError):
        return None


def validate_pegasus_jobs_dir(args) -> None:
    """Refuse a --pegasus-jobs-dir that cannot produce this run's jobs."""
    path = Path(args.pegasus_jobs_dir)
    if not path.is_dir():
        raise SystemExit(f"--pegasus-jobs-dir {path} is not a directory")
    count = count_job_records(path)
    if count == 0:
        # job_distributor.py publishes job_*.json and nothing else, so an otherwise
        # plausible-looking directory (a bundle's code/, a run dir) would start the whole
        # fleet with nothing to schedule and look like a scheduling failure.
        raise SystemExit(
            f"--pegasus-jobs-dir {path} holds no job_*.json records; the fleet would start "
            "with nothing to schedule.")
    # A bundle is COPIED here, and a copy does not sweep. The converter replaces its own
    # output directory (a 4-job workflow written over a 400-job one leaves 4 files, not 406),
    # but `rsync -a` without --delete restages the new conversion INTO the old one, and every
    # leftover job_*.json is published alongside — an earlier workflow's jobs running in this
    # run, with nothing in the results saying so. The conversion says how many records it
    # wrote, so the two can simply be compared.
    summary = _read_json(path / "conversion_summary.json")
    if summary is None:
        log(f"WARNING: {path} has no readable conversion_summary.json — it was not written by "
            "pegasus_to_swarm_converter.py, so neither stale records nor file-name collisions "
            "can be checked.")
    declared = (summary or {}).get("total_jobs_written")
    if isinstance(declared, int) and declared != count:
        raise SystemExit(
            f"--pegasus-jobs-dir {path} holds {count} job_*.json records but its conversion "
            f"wrote {declared}. The surplus is left over from an earlier conversion and would "
            "be published as part of this run. Re-copy the bundle with `rsync -a --delete` "
            "(or delete the directory first), then re-run.")

    # Logical file names are the one thing a bundle does NOT namespace, and three things key
    # on them: the DAG producer map, the run's readiness registry, and the shared working
    # directory. Converting several workflows together (or several runs of one workflow) is
    # where they collide, and every consequence is silent — a job gated on an unrelated
    # workflow, or reading its file and succeeding. Refused here rather than left in
    # conversion_summary.json for someone to notice.
    dag = (summary or {}).get("dag") or {}
    manifest = _read_json(path / "manifest.json")
    if manifest is None:
        log(f"WARNING: {path} has no readable manifest.json — converted with --no-bundle, so "
            "its jobs only DESCRIBE their code by submit-host path. Real execution refuses "
            "them unless those exact paths exist on every agent.")

    # Whether this run acts on file names at all, which is what decides between refusing a
    # bundle and merely reporting it. Two independent ways it can:
    #
    #   * GATING consults a producer map keyed by name, so a name decides scheduling order.
    #   * EXECUTION reads and writes those names in one shared working directory.
    #
    # Neither is true of an ungated simulated replay — the shipped multi-workflow profile —
    # whose names are inert, and refusing that would break the ordinary replay path. Execution
    # takes BOTH halves: the run's configured mode (a bundle cannot know it) and whether these
    # jobs carry anything to execute (a `real` run of synthetic jobs still simulates).
    gated = bool(dag.get("gating"))
    # Three states, and the middle one is the whole point. `simulate` (the default for an
    # absent key) is a real answer: that run touches nothing, and refusing it would block
    # ordinary replays. `unknown` means the config could not be read, which is not an answer —
    # and under --use-config-dir such a run still starts — so the checks apply as if it
    # executes rather than being skipped for a run that does.
    mode = _run_execution_mode(args)
    has_executables = bool(dag.get("execution_jobs") or (manifest or {}).get("code"))
    executes = has_executables and mode != "simulate"
    if has_executables and mode == "unknown":
        log("WARNING: this run's execution mode could not be read, so the file-name checks "
            "below are applied as if the jobs will execute.")
    names_are_live = gated or executes

    def _refuse_or_warn(problem: str, detail: str, live: bool) -> None:
        if live:
            raise SystemExit(f"--pegasus-jobs-dir {path}: {problem} {detail}")
        log(f"WARNING: {problem} Inert in this run (no gating, nothing executes), but any run "
            f"that gates or executes would be wrong.")

    # A bundle converted before these checks existed cannot be validated. Saying nothing would
    # read as "checked and clean", which is the one thing it is not.
    if summary is not None and "cross_workflow_edges" not in dag:
        _refuse_or_warn(
            f"{path} was converted before the file-name checks existed, so its names have "
            "never been checked.",
            "Re-convert it with the current pegasus_to_swarm_converter.py.", names_are_live)

    # One workflow produces a name, another reads it. Two workflows have no data relationship,
    # so this is never a real dependency — but gating makes it an edge, and a shared working
    # directory makes it the reader's input. The general case; the rest are narrower shapes.
    foreign = dag.get("cross_workflow_edges") or {}
    if foreign:
        name, entry = sorted(foreign.items())[0]
        _refuse_or_warn(
            f"{len(foreign)} file name(s) are produced by one workflow and read by another.",
            f"e.g. {name!r}, produced by {', '.join(entry.get('produced_by', [])[:2])} and read "
            f"by {', '.join(entry.get('read_by', [])[:2])}. The reader would take the other "
            "workflow's file (and under gating, wait for it). Convert and run these workflows "
            "separately — one bundle and one run each.", names_are_live)

    collisions = dag.get("colliding_outputs") or {}
    if collisions:
        _refuse_or_warn(
            f"{len(collisions)} output file name(s) are produced by more than one job.",
            f"({', '.join(sorted(collisions)[:3])}…) The working directory is flat, so they "
            "are one file written twice; under gating the producer map keeps only the last of "
            "them. Convert and run these workflows separately.", names_are_live)

    # A name a job expects to be STAGED that another job also produces. Within one workflow
    # this can be deliberate (a pre-seeded intermediate standing in for a job that cannot run
    # here); across workflows it is two workflows that picked the same name.
    conflicts = dag.get("replica_conflicts") or {}
    crossing = {k: v for k, v in conflicts.items() if v.get("cross_workflow")}
    if crossing:
        name, entry = sorted(crossing.items())[0]
        _refuse_or_warn(
            f"{len(crossing)} staged input name(s) are also produced by a job in another "
            "workflow.",
            f"e.g. {name!r}, expected by {', '.join(entry.get('expected_by', [])[:2])} and "
            f"produced by {', '.join(entry.get('produced_by', [])[:2])}. Convert and run these "
            "workflows separately.", names_are_live)
    if conflicts and not crossing:
        log(f"WARNING: {len(conflicts)} staged input name(s) are also produced by a job in the "
            "same workflow. Whichever runs first wins: stage_inputs never overwrites, and a "
            "producer writes directly.")

    # A replica or image whose basename collided was NOT bundled — but the job that lost still
    # names the file, and the runner resolves a bare name under roots.inputs, so it would be
    # staged the other workflow's bytes and succeed on the wrong data. Only staging acts on
    # this, so it is executing — not gating — that makes it matter.
    clashes = [m for m in (manifest or {}).get("missing", []) if m.get("collision")]
    if clashes:
        first = clashes[0]
        _refuse_or_warn(
            f"{len(clashes)} bundled file(s) collide by basename.",
            f"e.g. {first.get('kind')} {first.get('lfn') or first.get('name')!r}: "
            f"{first.get('reason')}. The job that lost would run on the other file rather than "
            "fail. Convert and run these workflows separately.", executes)

    log(f"Publishing pre-converted jobs from {path} ({count} job records)")
    if count != args.jobs:
        log(f"WARNING: --jobs {args.jobs} but {path} holds {count} job records. The agents' "
            f"expected job count and the completion checks follow --jobs; set it to {count}.")


def produce_jobs(args) -> None:
    """
    Produce jobs according to params. You can swap this with your job generator if needed.
    """
    jobs_cmd = [
        "python3.11", "job_distributor.py",
        "--jobs-dir", jobs_dir(args),
        "--jobs-per-interval", str(args.jobs_per_interval),
        "--redis-host", args.db_host,
    ]
    if args.topology == "hierarchical":
        # For 3-tier hierarchies (100 or 1000 agents), jobs enter at Level 2
        # For 2-tier hierarchies (30 or 110 agents), jobs enter at Level 1
        if args.agents in [100, 1000, 990]:
            jobs_cmd.extend(["--level", "2"])
        else:
            jobs_cmd.extend(["--level", "1"])
    if getattr(args, "split_hybrid", False):
        jobs_cmd.append("--split-hybrid")
    if args.debug:
        jobs_cmd.append("--debug")
    log("Producing jobs …")
    run_blocking(jobs_cmd, check=False)


def check_all_jobs_infeasible(args, bucket: int) -> bool:
    """
    Check if all remaining jobs in the specified bucket are infeasible.
    Returns True if all jobs have 0 feasible agents.
    """
    if args.mode != "local":
       return False
    # Get job IDs from Redis bucket
    out = run_once(["python3.11", "dump_db.py", "--host", args.db_host, "--type", "redis", "--key", "state"])
    job_ids = set()
    for line in out.strip().split('\n'):
        m = LINE_RE.match(line.strip())
        if m and int(m.group(1)) == bucket:
            # Extract job IDs from the set content
            content = m.group(2)
            for job_id in content.split(','):
                job_id = job_id.strip().strip("'\"")
                # Extract just the job ID from 'job:0:0:XX' format
                if 'job:' in job_id:
                    job_id = job_id.split(':')[-1]
                if job_id:
                    job_ids.add(job_id)

    if not job_ids:
        return False  # No jobs in bucket

    # Load feasibility CSV
    csv_path = Path(jobs_dir(args)) / "job_feasibility_mapping.csv"
    if not csv_path.exists():
        log(f"WARN: Feasibility CSV not found at {csv_path}, skipping infeasibility check")
        return False

    agents = find_all_agents(args, "main.py")
    # Extract agent IDs from the list of dicts
    available_agents = set(a.get('agent_id') for a in agents if a.get('agent_id'))

    # Check if all remaining jobs are infeasible
    infeasible_jobs = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['job_id'] in job_ids:
                feasible_agents_str = row['feasible_agents']
                if feasible_agents_str:
                    feasible_agents = set(feasible_agents_str.split(','))
                    # Check if any of agents 9-30 are in the feasible list
                    overlap = feasible_agents & available_agents

                    if not overlap:
                        infeasible_jobs.append(row['job_id'])

    all_infeasible = len(infeasible_jobs) == len(job_ids)
    #log(f"Infeasible jobs: {infeasible_jobs}")
    #log(f"Jobs: {job_ids}")
    if all_infeasible:
        log(f"All {len(job_ids)} remaining jobs in bucket {bucket} are infeasible!")

    return all_infeasible

def wait_runtime(args) -> None:
    # `--runtime` is a HARD CAP on this loop, not a replacement for the drain condition: a
    # slow-but-progressing run still exits early when the bucket drains, and a stalled one exits
    # on the clock. It used to be parsed and never read, so the only exits were "bucket drained"
    # and "bucket key missing" — neither of which a run that cannot place any job ever reaches.
    # One campaign run polled a bucket stuck at 300 for 28 minutes and was only ended by a
    # teardown running underneath it.
    runtime_cap = int(getattr(args, "runtime", 0) or 0)
    deadline = (time.time() + runtime_cap) if runtime_cap > 0 else None
    if deadline is not None:
        log(f"wait_runtime: drain condition, or hard cap of {runtime_cap}s, whichever first")
    else:
        log("WARNING: no --runtime cap and no --shutdown-after-seconds; a run that cannot place "
            "jobs will poll forever. Pass one of them for unattended runs.")

    low_since = None
    consecutive_misses = 0
    while True:
        if deadline is not None and time.time() >= deadline:
            log(f"Runtime cap of {runtime_cap}s reached before the pool drained → stopping. "
                f"This run did NOT finish on the drain condition; treat its results as a stall.")
            break
        out = run_once(["python3.11", "dump_db.py", "--host", args.db_host, "--type", "redis", "--key", "state"])
        size = parse_bucket_set_count(out, args.watch_bucket)
        if size is None:
            consecutive_misses += 1
            log(f"WARN: state:*:*:{args.watch_bucket} not found (miss {consecutive_misses}/{args.max_misses})")
            if consecutive_misses > args.max_misses:
                log("Bucket missing too often → treating as done.")
                break
        else:
            cond = size < args.threshold
            log(f"Bucket state:*:*:{args.watch_bucket} size={size} (thr={args.threshold}) → {'LOW' if cond else 'OK'}")

            # Check if all remaining jobs are infeasible
            if size > 0 and check_all_jobs_infeasible(args, args.watch_bucket):
                log("All remaining jobs are infeasible → triggering shutdown.")
                break

            if cond:
                if low_since is None:
                    low_since = time.time()
                elif time.time() - low_since >= args.stable_seconds:
                    log("Condition stable → proceed.")
                    break
            else:
                low_since = None
        time.sleep(args.check_interval)
    log(f"Sleeping for grace_seconds: {args.grace_seconds}s …")
    time.sleep(args.grace_seconds)

def wait_with_early_exit(args) -> None:
    """
    Wait up to shutdown_after_seconds, but exit early if all jobs reach a
    terminal state (COMPLETE, FAILED, or BLOCKED).  Polls Redis every 30s
    after an initial settling period of 120s.
    """
    import redis as _redis
    deadline = time.time() + args.shutdown_after_seconds
    settle_until = time.time() + 120          # let jobs start flowing
    poll_interval = 30
    total_jobs = args.jobs
    # Terminal states: COMPLETE=8, FAILED=9, BLOCKED=10
    terminal_states = {8, 9, 10}
    stable_count = 0                          # consecutive polls at 100%

    try:
        r = _redis.StrictRedis(host=args.db_host, port=6379, decode_responses=True)
    except Exception as e:
        log(f"Cannot connect to Redis for early-exit polling: {e}")
        time.sleep(args.shutdown_after_seconds)
        log("Shutdown timer expired, stopping test")
        return

    while time.time() < deadline:
        remaining = int(deadline - time.time())
        sleep_for = min(poll_interval, remaining)
        if sleep_for <= 0:
            break
        time.sleep(sleep_for)

        if time.time() < settle_until:
            continue

        # Count jobs in terminal states across all levels
        try:
            import json as _json
            terminal = 0
            # Check L1 jobs (hierarchical) or L0 jobs (flat)
            for k in r.scan_iter(match='job:1:0:*', count=1000):
                raw = r.get(k)
                if raw:
                    d = _json.loads(raw)
                    if d.get('state') in terminal_states:
                        terminal += 1

            # If no L1 jobs found, check L0
            if terminal == 0:
                for k in r.scan_iter(match='job:0:*:*', count=1000):
                    raw = r.get(k)
                    if raw:
                        d = _json.loads(raw)
                        if d.get('state') in terminal_states:
                            terminal += 1

            pct = 100 * terminal / total_jobs if total_jobs > 0 else 0
            log(f"Early-exit check: {terminal}/{total_jobs} jobs terminal ({pct:.1f}%), "
                f"{remaining}s remaining")

            if terminal >= total_jobs:
                stable_count += 1
                if stable_count >= 2:
                    log(f"All {total_jobs} jobs reached terminal state — exiting early "
                        f"(saved {remaining}s)")
                    return
            else:
                stable_count = 0
        except Exception as e:
            log(f"Early-exit poll error: {e}")

    log("Shutdown timer expired, stopping test")


def _launched_configs(args) -> list:
    """The per-agent configs this run launches, parsed — the same files `_launched_fleet` and
    `_run_execution_mode` read, for the same reason: a LOCAL run launches from `./configs`
    whatever `--config-dir` says, a REMOTE run from `--config-dir`, and only ids
    1..(agents + dynamic). Reading `<config_dir>/*.yml` and taking the first file — what the
    two readers below did until 2026-09-18 — answered from the wrong directory locally and
    from one arbitrary agent everywhere, so `run_meta.json` could label an arm the agents
    never ran. Returns [] when nothing is launched from a directory (no --use-config-dir).
    """
    if not getattr(args, "use_config_dir", False):
        return []
    from swarm.utils.yaml_strict import safe_load
    out = []
    for path in _launched_config_paths(args):
        try:
            with open(path) as f:
                out.append(safe_load(f) or {})
        except FileNotFoundError:
            continue
        except Exception:
            out.append(None)          # present and unreadable: keep the slot, mark unknown
    return out


def _effective_delegation_policy(args) -> str | None:
    """The policy this run will actually use, for run_meta.json.

    Recording only the flag would write `null` for every run that takes the arm from the base
    config, which is how the arm was selected until --delegation-policy existed. Under
    --use-config-dir it is read from EVERY launched config, and disagreement — or a config
    that will not parse — is recorded as None rather than as whichever agent came first:
    `collect.py` labels the arm from this field, and a wrong label moves a result between
    arms.
    """
    if getattr(args, "delegation_policy", None):
        return args.delegation_policy
    cfgs = _launched_configs(args)
    if cfgs:
        if any(c is None for c in cfgs):
            return None
        values = {((c.get("delegation") or {}).get("policy")) for c in cfgs}
        return values.pop() if len(values) == 1 else None
    try:
        from swarm.utils.yaml_strict import safe_load
        with open(BASE_CONFIG) as f:
            return ((safe_load(f) or {}).get("delegation") or {}).get("policy")
    except Exception:
        return None


def _effective_config(args) -> dict:
    """The config the agents of this run will actually read.

    Same resolution as `_effective_delegation_policy`: the launched per-agent configs under
    --use-config-dir (the first readable one — callers read fleet-wide keys such as the
    failure profile, which the generator copies unchanged into every agent), else the base
    config the generator copies from. {} when nothing can be read; callers treat that as
    unknown, not as defaults.
    """
    for cfg in _launched_configs(args):
        if cfg is not None:
            return cfg
    if getattr(args, "use_config_dir", False):
        return {}
    try:
        from swarm.utils.yaml_strict import safe_load
        with open(BASE_CONFIG) as f:
            return safe_load(f) or {}
    except Exception:
        return {}


def _ground_truth(args) -> dict:
    """The injected failure profile and reward shape, archived with the run.

    Delegation regret (P1-1) is computed against these: they are what decides whether a
    delegated job fails, so they are the only definition of a "right" routing choice. They
    live in the base config, which is edited between arms — so a regret number computed a
    week later from whatever the config says *then* would silently be scored against a
    profile the run never ran under. The ground truth has to travel with the run.

    Empty when failure simulation is off, which is the shipped default; the oracle then
    reports that the run has no signal to compute regret from rather than inventing one.
    """
    mab = (_effective_config(args).get("mab") or {})
    sim = mab.get("failure_simulation") or {}
    if not sim.get("enabled"):
        return {"enabled": False}
    return {
        "enabled": True,
        "failure_probability": sim.get("failure_probability", 0.1),
        "per_agent_failure_rates": sim.get("per_agent_failure_rates", {}),
        "per_job_type_failure_rates": sim.get("per_job_type_failure_rates", {}),
        "phases": sim.get("phases", []) or [],
        # The reward the bandit was actually given, so the oracle scores on the same scale
        # the policy was optimising rather than an assumed one.
        "reward": mab.get("reward", {}),
        "algorithm": mab.get("algorithm"),
        "top_k": mab.get("top_k"),
    }


def _hosts_file_for_stop(args, host_list: list[str] | None) -> str:
    """Path to a hosts file the stop script can read, writing one if needed.

    `agent_hosts.txt` is deleted by cleanup_between_runs unless it is this run's
    --agent-hosts-file, so falling back to that name gave the stop script a missing file,
    which it reports on stderr and run_blocking(check=False) then swallows: the agents were
    never stopped and nobody said so. The in-memory host list is the reliable source.
    """
    hosts_file = getattr(args, "agent_hosts_file", None)
    if hosts_file and Path(hosts_file).exists():
        return hosts_file
    hosts = list(host_list or [])
    if not hosts and getattr(args, "agent_hosts", None):
        hosts = [h.strip() for h in args.agent_hosts.split(",") if h.strip()]
    if not hosts and Path("agent_hosts.txt").exists():
        return "agent_hosts.txt"
    if not hosts:
        raise SystemExit("Cannot stop remote agents: no host list available "
                         "(pass --agent-hosts-file or --agent-hosts)")
    hosts_file = os.path.join(args.run_dir, "_agent_hosts.txt")
    with open(hosts_file, "w") as hf:
        for h in hosts:
            hf.write(h + "\n")
    return hosts_file


def stop_agents(args, host_list: list[str] | None = None) -> bool:
    """Stop every agent and report whether the stop was confirmed on all hosts.

    The return value matters: an unconfirmed stop means processes may still be running and
    still writing to Redis, which is how one run's metrics ended up reported as the next
    run's before payloads carried a run id.
    """
    log("Stopping agents …")
    stop_cmd = ["bash", "stop_agents_v2.sh", "--mode", args.mode]
    if args.mode == "remote":
        stop_cmd += ["--agent-hosts-file", _hosts_file_for_stop(args, host_list),
                      "--remote-repo-dir", args.remote_repo_dir]
    if args.shutdown_drain_timeout > 0:
        stop_cmd += ["--drain-timeout", str(args.shutdown_drain_timeout)]
    proc = run_blocking(stop_cmd, check=False)
    if proc.returncode != 0:
        log(f"WARNING: stop_agents_v2.sh exited {proc.returncode}; at least one agent was not "
            f"confirmed stopped. Surviving agents cannot corrupt this run's metrics (they are "
            f"filtered by run id) but they do keep consuming the fleet.")
        return False
    return True


# ------------------------------
# Metrics completeness
# ------------------------------
def _metrics_in_redis(args, run_id: str) -> tuple[set[int], dict[int, str]]:
    """(agent ids with a payload for `run_id`, {agent id: other run id}) from Redis."""
    import redis as _redis
    mine: set[int] = set()
    foreign: dict[int, str] = {}
    try:
        r = _redis.StrictRedis(host=args.db_host, port=6379, decode_responses=True)
        for key in r.scan_iter("metrics:*"):
            raw = r.get(key)
            if not raw:
                continue
            try:
                entry = json.loads(raw)
            except (ValueError, TypeError):
                continue
            if not isinstance(entry, dict) or entry.get("id") is None:
                continue
            agent_id = int(entry["id"])
            if entry.get("run_id") == run_id:
                mine.add(agent_id)
            else:
                foreign[agent_id] = str(entry.get("run_id"))
    except Exception as exc:
        log(f"WARNING: could not read metrics from Redis: {exc}")
    return mine, foreign


def wait_for_metrics(args, expected_ids: set[int], run_id: str) -> tuple[set[int], dict[int, str]]:
    """Block until every expected agent has written this run's metrics, or the deadline passes.

    Plotting reads metrics out of Redis, so doing this before plotting is what makes
    metrics.json a record of THIS run. Agents save metrics at the start of teardown rather
    than after draining their job threads, so in the normal case this returns on the first
    poll; the wait covers a slow host or a straggler that had to be SIGKILLed.
    """
    deadline = time.time() + max(0, args.metrics_wait_seconds)
    last_report = 0.0
    while True:
        have, foreign = _metrics_in_redis(args, run_id)
        missing = expected_ids - have
        if not missing or time.time() >= deadline:
            return missing, foreign
        now = time.time()
        if now - last_report >= 10:
            log(f"Waiting for metrics: {len(have)}/{len(expected_ids)} agents reported, "
                f"{int(deadline - now)}s left")
            last_report = now
        time.sleep(2)


def report_metrics_completeness(args, expected_ids: set[int], run_id: str,
                                stopped_cleanly: bool) -> bool:
    """Log and record which agents reported metrics. True when the run is measurable."""
    missing, foreign = wait_for_metrics(args, expected_ids, run_id)
    allowed = max(0, args.allow_missing_metrics)
    declared = {int(x) for x in str(args.expect_silent_agents or "").replace(" ", "").split(",")
                if x}
    if foreign:
        log(f"NOTE: {len(foreign)} metrics payload(s) in Redis belong to another run and will "
            f"be ignored: " + ", ".join(f"agent {a} (run_id={r})" for a, r in sorted(foreign.items())))
    if not missing:
        if declared:
            # Their metrics being present means the kill did not take — the fault the run was
            # measuring never happened, which is a worse outcome than a missing payload.
            log(f"WARNING: --expect-silent-agents named {sorted(declared)} but every agent "
                f"reported metrics; the intended kills did not take effect")
        log(f"Metrics complete: all {len(expected_ids)} agents reported for run_id={run_id}")
        return True

    unexpected = missing - declared if declared else set()

    shortfall = {
        "run_id": run_id,
        "expected_agents": sorted(expected_ids),
        "missing_agents": sorted(missing),
        "foreign_payloads": {str(a): r for a, r in sorted(foreign.items())},
        "allow_missing_metrics": allowed,
        "expect_silent_agents": sorted(declared),
        "unexpectedly_silent": sorted(unexpected),
        "stopped_cleanly": stopped_cleanly,
        "waited_seconds": args.metrics_wait_seconds,
    }
    with open(os.path.join(args.run_dir, "metrics_shortfall.json"), "w") as f:
        json.dump(shortfall, f, indent=2)

    # With ids declared, the count is not the question: the right agents have to be the
    # silent ones. A failure test that kills 3 and 5 but finds 7 and 9 silent measured a
    # different fault than the one it reported.
    if declared:
        accounted = not unexpected
        if accounted:
            log(f"Metrics complete apart from the {len(missing)} agent(s) declared silent "
                f"({sorted(missing)}) for run_id={run_id}")
            return True
        log(f"ERROR: {sorted(unexpected)} reported no metrics but were not declared silent "
            f"(declared: {sorted(declared)}). Wrote {args.run_dir}/metrics_shortfall.json.")
        return False

    level = "WARNING" if len(missing) <= allowed else "ERROR"
    log(f"{level}: {len(missing)} of {len(expected_ids)} agents never wrote metrics for this run "
        f"(missing: {sorted(missing)}). Wrote {args.run_dir}/metrics_shortfall.json.")
    if len(missing) > allowed:
        log("ERROR: per-agent metrics (load, utilisation, fairness, MAB and delegation counts) "
            "are incomplete, so this run is not measurable. Declare the agents a failure test "
            "SIGKILLs with --expect-silent-agents 3,7 (or, less precisely, "
            "--allow-missing-metrics N).")
        return False
    return True

def collect_logs(args) -> None:
    '''
    Path(args.log_dir).mkdir(parents=True, exist_ok=True)
    # Example collector; adjust to your filenames.
    log("Collecting logs …")
    for name in ("swarm.log", "agents.log"):
        p = Path(name)
        if p.exists():
            p.rename(Path(args.log_dir) / p.name)
    '''

def parse_and_report(args, run_id: str | None = None) -> None:
    plot_cmd = [
        "python3.11", "plot_latency_jobs.py",
        "--output_dir", args.run_dir,
        "--agents", str(args.agents),
        "--db_host", args.db_host,
        #"--save-csv",
    ]
    if run_id:
        plot_cmd += ["--metrics-run-id", run_id]
    #if not getattr(args, 'generate_plots', False):
    #    plot_cmd.append("--skip-plots")
    if args.topology == "hierarchical":
        plot_cmd.extend(["--hierarchical"])
    log("Plotting …")
    run_blocking(plot_cmd, check=False)

# ------------------------------
# CLI
# ------------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Unified local/remote test runner for SwarmAgents")
    ap.add_argument("--mode", choices=["local", "remote"], required=True, help="Where to start agents")
    ap.add_argument("--agent-type", choices=["resource", "llm"], required=True)
    ap.add_argument("--agents", type=int, required=True, help="Total number of agents")
    ap.add_argument("--agents-per-host", type=int, default=1, help="Only for remote mode; shard size")
    ap.add_argument("--topology", required=True, choices=["mesh", "ring", "star", "hierarchical"])
    ap.add_argument("--hierarchical-level1-agent-type", type=str,
                    choices=["llm", "resource"], default="resource",
                    help="Agent type for level 1 (parent) agents in hierarchical topology "
                         "(default: resource). It does NOT follow --agent-type: an all-LLM "
                         "hierarchy needs this set to llm as well.")
    ap.add_argument("--jobs", type=int, default=None,
                    help="Number of jobs this run expects. Optional with --pegasus-jobs-dir, "
                         "which counts the bundle's job_*.json records instead — the bundle is "
                         "the authority on how many jobs there are.")
    ap.add_argument("--db-host", required=True)
    ap.add_argument("--jobs-per-proposal", type=int, default=10)
    ap.add_argument("--groups", type=int, default=None)
    ap.add_argument("--group-size", type=int, default=None)
    ap.add_argument("--debug", action="store_true")

    # Starter and config
    ap.add_argument("--starter", default="./swarm-multi-start.sh", help="Path to swarm-multi-start.sh")
    ap.add_argument("--config-dir", default="configs", help="Where to write generated configs")
    ap.add_argument("--use-config-dir", action="store_true", help="Tell starters to use pre-generated configs")

    # Remote host options
    ap.add_argument("--agent-hosts", default=None, help="Comma-separated hostnames")
    ap.add_argument("--agent-hosts-file", default=None, help="File with one hostname per line")
    ap.add_argument("--agent-sites-file", default=None,
                    help="File with one site label per line, parallel to the hosts file "
                         "(enables topology-aware Snow sampling)")
    ap.add_argument("--remote-repo-dir", default="/root/SwarmAgents", help="Remote repo root")

    # Test control
    ap.add_argument("--job-interval", type=float, default=0.5, help="Seconds between job bursts")
    ap.add_argument("--jobs-per-interval", type=int, default=20)
    # Default 0, not 90: this value is now ENFORCED as a hard cap on wait_runtime(), and it was
    # silently ignored before. Defaulting to the old 90 would have truncated every run that does
    # not pass the flag, so opting in is explicit.
    ap.add_argument("--runtime", type=int, default=0,
                    help="Hard cap in seconds on waiting for the pool to drain (0 = no cap). "
                         "The run still exits early on the drain condition; this only bounds a "
                         "run that cannot place jobs. Strongly recommended for unattended runs.")
    ap.add_argument("--grace-seconds", type=int, default=30)
    ap.add_argument("--watch-bucket", type=int, default=1)
    ap.add_argument("--threshold", type=int, default=5)
    ap.add_argument("--stable-seconds", type=int, default=90)
    ap.add_argument("--check-interval", type=float, default=5.0)
    ap.add_argument("--max-misses", type=int, default=10)

    # Dynamic agent addition
    ap.add_argument("--dynamic-agents", type=int, default=0,
                    help="Number of agents to add dynamically during test (0 = disabled)")
    ap.add_argument("--dynamic-trigger", choices=["time", "bucket", "jobs-completed"], default="time",
                    help="Trigger type for adding dynamic agents")
    ap.add_argument("--dynamic-delay", type=int, default=30,
                    help="Seconds to wait before adding agents (for 'time' trigger)")
    ap.add_argument("--dynamic-trigger-bucket", type=int, default=1,
                    help="Bucket to monitor (for 'bucket' trigger)")
    ap.add_argument("--dynamic-trigger-threshold", type=int, default=50,
                    help="Threshold value for bucket/jobs trigger")
    ap.add_argument("--dynamic-trigger-jobs", type=int, default=50,
                    help="Number of completed jobs to wait for (for 'jobs-completed' trigger)")

    # Co-parent support for hierarchical topology
    ap.add_argument("--co-parents", type=int, default=1,
                    help="Number of co-parents per child group in hierarchical topology (default: 1)")
    ap.add_argument("--groups-per-coordinator", type=int, default=None,
                    help="Child groups each Level-1 coordinator exclusively parents (default: 2). "
                         "At 1 a coordinator has a single candidate, so the MAB and "
                         "delegation.policy=llm are inert and every delegation records as "
                         "trivial. Freed coordinator slots become Level-0 agents, so the fleet "
                         "size is unchanged. Two-level hierarchies only: a three-level fleet "
                         "(100, 990, 1000) refuses an explicit >1 and steps the default down to 1.")
    ap.add_argument("--delegation-policy", choices=["bandit", "llm"], default=None,
                    help="Which plane picks the child group a coordinator delegates to "
                         "(E4's arms). Overrides delegation.policy in the generated configs; "
                         "default is whatever the base config says. Recorded in run_meta.json, "
                         "so an arm is no longer selected by an unrecorded hand edit.")

    ap.add_argument("--textfile-dir", default=None,
                    help="node_exporter textfile collector directory on each agent host. "
                         "Turns on the Prometheus export of the P0-4 counters (consensus "
                         "messages and bytes, rounds to finalize, delegation context age, "
                         "LLM tokens). metrics.json carries them either way, so this is for "
                         "watching a run live rather than for the figures.")

    # Job generation
    ap.add_argument("--fit-all", action="store_true",
                    help="Size every job to fit ALL agents (min capacities). "
                         "Enables any agent to take over jobs from failed agents.")
    ap.add_argument("--quantum-agents-pct", type=float, default=0.0,
                    help="Fraction (0.0-1.0) of agents that own a quantum backend (default: 0.0)")
    ap.add_argument("--quantum-fraction", type=float, default=0.0,
                    help="Fraction (0.0-1.0) of jobs with a one-shot quantum component")
    ap.add_argument("--hybrid-fraction", type=float, default=0.0,
                    help="Fraction (0.0-1.0) of jobs with a hybrid classical<->quantum loop")
    ap.add_argument("--split-hybrid", action="store_true",
                    help="Split hybrid jobs into quantum/classical sub-jobs co-scheduled on "
                         "different agents via the measurement data layer")

    # Plot generation
    ap.add_argument("--generate-plots", action="store_true",
                    help="Generate full plots after test (default: CSV-only with --skip-plots)")

    # Test shutdown control
    ap.add_argument("--shutdown-after-seconds", type=int, default=0,
                    help="Shutdown test after N seconds (0 = use default wait_runtime behavior)")

    # Teardown / measurement integrity
    ap.add_argument("--shutdown-drain-timeout", type=int, default=0,
                    help="Seconds the stop script waits for each agent to exit before SIGKILL "
                         "(0 = the stop script's own default)")
    ap.add_argument("--metrics-wait-seconds", type=int, default=120,
                    help="How long to wait after stopping the agents for all of them to write "
                         "this run's metrics to Redis before plotting (default: 120)")
    ap.add_argument("--allow-missing-metrics", type=int, default=0,
                    help="Number of agents allowed to report no metrics without failing the run. "
                         "Set it to the number of agents killed with SIGKILL in a failure "
                         "injection test; those cannot flush metrics by design. Prefer "
                         "--expect-silent-agents, which also checks WHICH agents were silent.")
    ap.add_argument("--expect-silent-agents", default="",
                    help="Comma-separated agent ids expected to report no metrics (SIGKILLed by "
                         "a failure injection test). Stricter than --allow-missing-metrics: a "
                         "run still fails if some OTHER agent was the silent one, which a bare "
                         "count cannot distinguish. SIGTERM-killed agents still flush metrics, "
                         "so they do not belong here.")

    # Pegasus job integration
    ap.add_argument(
        "--pegasus-jobs-dir", default=None,
        help="Publish a bundle converted ELSEWHERE (pegasus_to_swarm_converter.py --output-dir) "
             "instead of converting here. The directory is only read: no conversion runs, no "
             "synthetic jobs are generated, and it is never cleaned between runs. Point the "
             "agents' runtime.execution.bundle at the same path. Excludes --pegasus-profiles.")
    ap.add_argument("--pegasus-profiles", default=None,
                    help="Path to Pegasus profiles file (text/export) or Redis host. "
                         "When set, jobs are converted from Pegasus profiles instead of generated synthetically.")
    ap.add_argument(
        "--pegasus-dag-gating", action="store_true",
        help="Reconstruct the workflow's dependency graph as per-job data predicates, so a "
             "job is not selectable until its parents have produced what it reads. Required "
             "for any real workflow; forces --pegasus-data-nodes per-file.")
    ap.add_argument(
        "--pegasus-bundle-source-root",
        help="Where the workflow tree lives on this fleet, so the converter can copy the "
             "executables and root inputs into the jobs directory. Either a path (anchored "
             "on the common parent of the recorded paths) or OLD=NEW to state the mapping.")
    ap.add_argument("--pegasus-input-type", choices=["text", "redis", "export", "json"], default="text",
                    help="Format of the Pegasus profiles source (default: text)")
    ap.add_argument("--pegasus-data-nodes", choices=["per-site", "per-file"], default="per-file",
                    help="Granularity of job data_in/data_out nodes (default: per-file, keeps every "
                         "file and its size)")
    ap.add_argument("--pegasus-dtn-names", type=str, default=None,
                    help="Comma-separated DTN pool to spread job files across. Defaults to the DTNs "
                         "the generated fleet actually holds (agent_dtns.json), so every job lands "
                         "on a DTN some agent has and the connectivity cost term matches.")

    # Output
    ap.add_argument("--run-dir", default="run_out")
    ap.add_argument("--seed", type=int, default=None,
                    help="Seed agent-profile generation so fleets are reproducible across runs")
    ap.add_argument("--master-fleet-size", type=int, default=None,
                    help="Generate per-agent flavours/backends for a fleet of this size and use "
                         "the first --agents of them. Set it to the largest rung of the scale "
                         "ladder (e.g. 270) so agent i is the same machine at every size; "
                         "without it, flavours scale with fleet size and the ladder compares "
                         "different fleets.")
    ap.add_argument("--log-dir", default="logs")

    return ap.parse_args()

def read_hosts(args: argparse.Namespace) -> list[str]:
    hosts: list[str] = []
    if args.agent_hosts:
        hosts = [h.strip() for h in args.agent_hosts.split(",") if h.strip()]
    elif args.agent_hosts_file:
        with open(args.agent_hosts_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    hosts.append(line)
    return hosts

def main() -> None:
    args = parse_args()
    Path(args.run_dir).mkdir(parents=True, exist_ok=True)

    # Identity for this run. Agents stamp it on the metrics they write to Redis and the
    # plotting step only reads payloads carrying it, so a payload written by an agent that
    # outlived an earlier run can no longer be reported as this run's numbers.
    # Exported (not passed): local agents are spawned as children and inherit it, and the
    # remote starter re-exports it over ssh.
    if args.delegation_policy and args.use_config_dir:
        # --use-config-dir skips generate_configs entirely, so the override would never reach a
        # config file while run_meta.json still recorded it — a run labelled with an arm it did
        # not run is worse than one with no label.
        raise SystemExit(
            "--delegation-policy has no effect with --use-config-dir (the configs are used as "
            "they are on disk). Drop --use-config-dir to generate configs for this arm, or set "
            "delegation.policy in the config directory yourself and drop --delegation-policy.")

    if args.jobs is None:
        # The count drives the agents' expected job total and the completion checks. A bundle
        # knows it exactly, so asking for it again only creates a way to get it wrong.
        if not args.pegasus_jobs_dir:
            raise SystemExit(
                "--jobs is required, except with --pegasus-jobs-dir where it is taken from the "
                "bundle's job_*.json records.")
        args.jobs = count_job_records(Path(args.pegasus_jobs_dir))
        log(f"--jobs not given; taking {args.jobs} from {args.pegasus_jobs_dir}")

    if args.pegasus_jobs_dir and args.pegasus_profiles:
        # One names a bundle to publish as it is, the other says to convert a profile into
        # jobs/ — running both would convert over one of them and publish a set nobody asked
        # for, with nothing in the run saying which jobs it ran.
        raise SystemExit(
            "--pegasus-jobs-dir and --pegasus-profiles are alternatives: the first publishes "
            "a bundle converted elsewhere, the second converts one here into jobs/.")
    if args.pegasus_jobs_dir:
        validate_pegasus_jobs_dir(args)

    run_id = f"{Path(args.run_dir).name}-{datetime.now():%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6]}"
    os.environ["SWARM_RUN_ID"] = run_id
    log(f"run_id={run_id}")

    # Build host list up front
    host_list = read_hosts(args) if args.mode == "remote" else []

    # Preflight checks for remote mode
    if args.mode == "remote" and host_list:
        preflight_check(args, host_list)

    # Reap anything still running from a previous run BEFORE Redis is flushed. An agent that
    # is killed after the flush writes its (old) metrics into the fresh keyspace, which is
    # exactly how smoke-g2-llm's metrics.json came to hold 15 agents from smoke-g2-bandit.
    log("Reaping agents left over from any previous run …")
    if not stop_agents(args, host_list):
        raise SystemExit(
            "Refusing to start: agents from a previous run could not be confirmed stopped. "
            "They would keep writing jobs, agent records and MAB state into the keyspace this "
            "run is about to flush — unlike metrics, none of that is stamped with a run id, so "
            "the contamination would be invisible. Fix the unreachable host(s) or stop them by "
            "hand, then re-run.")

    with open(os.path.join(args.run_dir, "run_meta.json"), "w") as f:
        json.dump({
            "run_id": run_id,
            "started_at": time.time(),
            "started_at_iso": datetime.now().isoformat(),
            "mode": args.mode,
            "agent_type": args.agent_type,
            # Recorded because "this agent has no LLM block" is otherwise ambiguous after the
            # fact: in a mixed-role run the coordinators are analytic BY DESIGN, and without
            # this the collector cannot tell them from agents whose instrumentation is simply
            # missing — so it either blocks a verdict on a supported run or draws one over a
            # fleet it did not measure.
            # Observed from the launched configs under --use-config-dir, where the flag is
            # inert — see _effective_coordinator_type.
            "hierarchical_level1_agent_type": _effective_coordinator_type(args),
            "agents": args.agents,
            "dynamic_agents": args.dynamic_agents,
            "topology": args.topology,
            "jobs": args.jobs,
            "delegation_policy": _effective_delegation_policy(args),
            # The REQUEST (None = default). Both this and the coordinator type are rewritten
            # from the generated configs by _record_observed_fleet() below, because this file
            # is written before the fleet exists and neither request is the last word.
            "groups_per_coordinator": getattr(args, "groups_per_coordinator", None),
            # What made a delegated job fail, and what the bandit was rewarded with. The
            # oracle (P1-1) scores routing choices against exactly this.
            "ground_truth": _ground_truth(args),
            "argv": sys.argv,
        }, f, indent=2)

    # Calculate total agents (initial + dynamic)
    total_agents = args.agents + args.dynamic_agents
    original_agents = args.agents

    # Generate configs for ALL agents (initial + dynamic) up front
    cleanup_between_runs(args)
    if not args.use_config_dir:
        if args.mode == "remote" and not host_list:
            raise SystemExit("Remote mode requires --agent-hosts or --agent-hosts-file")
        # Compute per-host count for writing agent_hosts.txt (if needed)
        if not args.agent_hosts_file and args.mode == "remote":
            # Match number of shards we will generate (for ALL agents)
            needed = math.ceil(total_agents / args.agents_per_host)
            host_list = host_list[:needed]

        # Set initial_group_size for dynamic agent scenarios
        if args.dynamic_agents > 0:
            args.initial_group_size = original_agents
        else:
            args.initial_group_size = None

        # Temporarily set args.agents to total for config generation
        args.agents = total_agents
        generate_configs(args, host_list if args.mode == "remote" else ["localhost"])
        args.agents = original_agents  # Restore to initial agent count

    # Convert Pegasus profiles if requested (writes to jobs/). After the fleet exists — freshly
    # generated above, or already on disk under --use-config-dir — because the job DTN pool is
    # read from agent_dtns.json, which describes whichever fleet this run will launch (see
    # convert_pegasus_jobs). This is deliberately OUTSIDE the generation branch: inside it,
    # --use-config-dir skipped the conversion altogether and the run published whatever jobs/
    # already held — a stale set from an earlier run, or nothing — while reporting nothing
    # unusual, which is the one failure mode a converted workflow cannot survive.
    if args.pegasus_profiles:
        convert_pegasus_jobs(args)

    # The fleet now exists either way, so replace the two run_meta fields that are requests
    # rather than facts with what the configs say. collect.py reads both.
    _record_observed_fleet(args)

    # After the fleet is generated (or, with --use-config-dir, as it stands) and after any
    # conversion, so both sides of the comparison are the ones this run will use.
    if args.pegasus_profiles or args.pegasus_jobs_dir:
        check_fleet_fits_jobs(args)

    # Start initial agents
    if args.mode == "local":
        start_agents_local(args)
    else:
        if not host_list:
            raise SystemExit("Remote mode requires --agent-hosts or --agent-hosts-file")
        start_agents_remote(args, host_list)

    # Start job production in background thread to avoid blocking dynamic trigger detection
    job_thread = threading.Thread(target=lambda: produce_jobs(args), daemon=True, name="JobDistributor")
    job_thread.start()
    log("Job distribution started in background thread")

    # If dynamic agents are enabled, wait for trigger and add them
    if args.dynamic_agents > 0:
        # Start a thread to wait for the trigger and add agents
        def dynamic_addition():
            wait_for_dynamic_trigger(args)
            if _TEARDOWN.is_set():
                # The metrics wait keeps this process alive well past the stop sweep, so a
                # trigger that fires late would start agents nobody will ever stop.
                log("Dynamic trigger fired after teardown began; not adding agents")
                return
            add_dynamic_agents(args, host_list)

        dynamic_thread = threading.Thread(target=dynamic_addition, daemon=True, name="DynamicAgentAdder")
        dynamic_thread.start()
        log("Dynamic agent trigger monitoring started")

    # Wait for test completion
    if args.shutdown_after_seconds > 0:
        log(f"Using time-based shutdown: will stop after {args.shutdown_after_seconds} seconds")
        wait_with_early_exit(args)
    else:
        wait_runtime(args)
    _TEARDOWN.set()
    stopped_cleanly = stop_agents(args, host_list)

    # Every agent id we launched is expected to report; plotting happens after they all have.
    measurable = report_metrics_completeness(
        args, set(range(1, total_agents + 1)), run_id, stopped_cleanly)

    if args.mode == "remote" and host_list:
        collect_remote_logs(args, host_list)
    collect_logs(args)

    # Update args.agents to total for reporting
    args.agents = total_agents
    parse_and_report(args, run_id)
    if not measurable:
        log("Done, but this run's metrics are incomplete — exiting non-zero so a batch driver "
            "does not average it in as a good cell.")
        sys.exit(3)
    log("Done.")
    sys.exit(0)

def _load_hosts():
    """Load remote host list from file."""
    with open("agent_hosts.txt", 'r') as f:
        hosts = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    print(f"Loaded {len(hosts)} remote hosts")
    return hosts

def _find_local_agents(pattern: str = 'main.py') -> List[dict]:
    """Find agent processes running locally."""
    try:
        # Find processes matching the pattern
        result = subprocess.run(
            ['ps', 'aux'],
            capture_output=True,
            text=True,
            check=True
        )

        agents = []
        for line in result.stdout.split('\n'):
            if pattern in line and 'grep' not in line and 'kill_agents.py' not in line:
                parts = line.split()
                if len(parts) >= 2:
                    pid = parts[1]
                    # Try to extract agent ID from command line
                    agent_id = None

                    # Method 1: --agent-id flag
                    if '--agent-id' in line:
                        try:
                            idx = parts.index('--agent-id')
                            if idx + 1 < len(parts):
                                agent_id = parts[idx + 1]
                        except (ValueError, IndexError):
                            pass

                    # Method 2: positional arg after main.py (e.g., "python main.py 5")
                    if agent_id is None and 'main.py' in parts:
                        try:
                            main_idx = parts.index('main.py')
                            if main_idx + 1 < len(parts):
                                potential_id = parts[main_idx + 1]
                                if potential_id.isdigit():
                                    agent_id = potential_id
                        except (ValueError, IndexError):
                            pass

                    agents.append({
                        'pid': pid,
                        'agent_id': agent_id,
                        'host': 'localhost',
                        'cmdline': ' '.join(parts[10:])[:100]  # First 100 chars of command
                    })

        return agents
    except subprocess.CalledProcessError as e:
        print(f"Error finding local agents: {e}")
        return []

def _find_remote_agents(host: str, pattern: str = 'main.py') -> List[dict]:
    """Find agent processes running on a remote host."""
    try:
        ssh_cmd = ['ssh']
        target = f"root@{host}"
        ssh_cmd.extend([target, 'ps aux'])

        result = subprocess.run(
            ssh_cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=10
        )

        agents = []
        for line in result.stdout.split('\n'):
            if pattern in line and 'grep' not in line:
                parts = line.split()
                if len(parts) >= 2:
                    pid = parts[1]
                    agent_id = None
                    if '--agent-id' in line:
                        idx = line.split().index('--agent-id')
                        if idx + 1 < len(line.split()):
                            agent_id = line.split()[idx + 1]

                    agents.append({
                        'pid': pid,
                        'agent_id': agent_id,
                        'host': host,
                        'cmdline': ' '.join(parts[10:])[:100]
                    })

        return agents
    except subprocess.CalledProcessError as e:
        print(f"Error finding agents on {host}: {e}")
        return []
    except subprocess.TimeoutExpired:
        print(f"Timeout connecting to {host}")
        return []

def find_all_agents(args, pattern: str = 'main.py') -> List[dict]:
    """Find all agent processes (local or remote)."""
    all_agents = []

    if args.mode == 'local':
        all_agents = _find_local_agents(pattern)
    elif args.mode == 'remote':
        for host in _load_hosts():
            print(f"Scanning {host}...")
            agents = _find_remote_agents(host, pattern)
            all_agents.extend(agents)

    return all_agents

if __name__ == "__main__":
    main()
