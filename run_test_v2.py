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

Notes:
- For remote mode, you can provide --agent-hosts "agent-1,agent-2,agent-3" OR --agent-hosts-file.
- This script expects the repo directory on remote hosts at /root/SwarmAgents (adjust with --remote-repo-dir).
- It assumes passwordless SSH to those hosts.
"""
from __future__ import annotations
import argparse, os, re, subprocess, sys, time, math, shlex
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

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
            completed = parse_bucket_set_count(out, 3)  # Assuming bucket 3 is completed
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
        jobs_cmd.extend(["--level", "1"])
    if args.debug:
        jobs_cmd.append("--debug")
    log("Producing jobs …")
    run_blocking(jobs_cmd, check=False)


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

    # Start job production
    produce_jobs(args)

    # If dynamic agents are enabled, wait for trigger and add them
    if args.dynamic_agents > 0:
        # Start a thread to wait for the trigger and add agents
        import threading
        def dynamic_addition():
            wait_for_dynamic_trigger(args)
            add_dynamic_agents(args, host_list)

        dynamic_thread = threading.Thread(target=dynamic_addition, daemon=True)
        dynamic_thread.start()

    # Wait for test completion
    wait_runtime(args)
    stop_agents(args)
    collect_logs(args)

    # Update args.agents to total for reporting
    args.agents = total_agents
    parse_and_report(args)
    log("Done.")

if __name__ == "__main__":
    main()
