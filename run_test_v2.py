#!/usr/bin/env python3.11
"""
run_test_v2.py — unified local/remote test runner for SwarmAgents

Usage (local):
  ./run_test_v2.py \
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
  ./run_test_v2.py \
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
  ./run_test_v2.py \
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
import argparse, os, re, subprocess, sys, time, math, shlex, csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List

LINE_RE = re.compile(r"^state:\d+:\d+:(\d+):\s*\{([^}]*)\}\s*$")

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
        "./config_swarm_multi.yml", str(cfg_dir),
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
    # Pass initial group size for dynamic agent addition
    if hasattr(args, 'initial_group_size') and args.initial_group_size is not None:
        gen_args += ["--initial-group-size", str(args.initial_group_size)]

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
        cmds = [["rm", "-rf", "agent_hosts.txt"], ["rm", "-rf", "agent_profiles.json"],
                ["rm", "-rf", "agent_dtns.json"], ["rm", "-rf", "jobs"]]
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

        start_cmd = (
            #f"source ~/.bash_profile && "
            f"source ~/.profile && "
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
def produce_jobs(args) -> None:
    """
    Produce jobs according to params. You can swap this with your job generator if needed.
    """
    jobs_cmd = [
        "python3.11", "job_distributor.py",
        "--jobs-dir", "jobs",
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
    csv_path = Path("jobs") / "job_feasibility_mapping.csv"
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
    low_since = None
    consecutive_misses = 0
    while True:
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

def stop_agents(args) -> None:
    log("Stopping agents …")
    # Best-effort stop; if you have a dedicated stop script, call it here.
    run_blocking(["bash", "stop_agents_v2.sh"], check=False)

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

def parse_and_report(args) -> None:
    plot_cmd = [
        "python3.11", "plot_latency_jobs.py",
        "--output_dir", args.run_dir,
        "--agents", str(args.agents),
        "--db_host", args.db_host,
        "--save-csv", "--skip-plots"
    ]
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
                    choices=["llm", "resource"], default="llm",
                    help="Agent type for level 1 (parent) agents in hierarchical topology (default: llm)")
    ap.add_argument("--jobs", type=int, required=True)
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
    ap.add_argument("--remote-repo-dir", default="/root/SwarmAgents", help="Remote repo root")

    # Test control
    ap.add_argument("--job-interval", type=float, default=0.5, help="Seconds between job bursts")
    ap.add_argument("--jobs-per-interval", type=int, default=20)
    ap.add_argument("--runtime", type=int, default=90, help="Seconds to keep the test running")
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

    # Test shutdown control
    ap.add_argument("--shutdown-after-seconds", type=int, default=0,
                    help="Shutdown test after N seconds (0 = use default wait_runtime behavior)")

    # Output
    ap.add_argument("--run-dir", default="run_out")
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

    # Build host list up front
    host_list = read_hosts(args) if args.mode == "remote" else []

    # Calculate total agents (initial + dynamic)
    total_agents = args.agents + args.dynamic_agents
    original_agents = args.agents

    # Generate configs for ALL agents (initial + dynamic) up front
    cleanup_between_runs(args)
    if not args.use_config_dir:
        if args.mode == "remote" and not host_list:
            raise SystemExit("Remote mode with --use-config-dir requires --agent-hosts or --agent-hosts-file")
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

    # Start initial agents
    if args.mode == "local":
        start_agents_local(args)
    else:
        if not host_list:
            raise SystemExit("Remote mode requires --agent-hosts or --agent-hosts-file")
        start_agents_remote(args, host_list)

    # Start job production in background thread to avoid blocking dynamic trigger detection
    import threading
    job_thread = threading.Thread(target=lambda: produce_jobs(args), daemon=True, name="JobDistributor")
    job_thread.start()
    log("Job distribution started in background thread")

    # If dynamic agents are enabled, wait for trigger and add them
    if args.dynamic_agents > 0:
        # Start a thread to wait for the trigger and add agents
        def dynamic_addition():
            wait_for_dynamic_trigger(args)
            add_dynamic_agents(args, host_list)

        dynamic_thread = threading.Thread(target=dynamic_addition, daemon=True, name="DynamicAgentAdder")
        dynamic_thread.start()
        log("Dynamic agent trigger monitoring started")

    # Wait for test completion
    if args.shutdown_after_seconds > 0:
        log(f"Using time-based shutdown: will stop after {args.shutdown_after_seconds} seconds")
        time.sleep(args.shutdown_after_seconds)
        log("Shutdown timer expired, stopping test")
    else:
        wait_runtime(args)
    stop_agents(args)
    collect_logs(args)

    # Update args.agents to total for reporting
    args.agents = total_agents
    parse_and_report(args)
    log("Done.")

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
