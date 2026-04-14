#!/usr/bin/env python3.11
"""
run_baseline_remote.py — Orchestration for distributed baseline scheduling.

Starts lightweight baseline workers on remote VMs via SSH, runs a centralized
scheduler (Greedy/Round-Robin/Random) on the swarm host, and coordinates via
Redis.  Workers execute jobs; the scheduler only makes assignment decisions.

Usage:
    python3.11 baselines/run_baseline_remote.py \
        --scheduler greedy --agents 30 --jobs 500 \
        --db-host 10.0.0.1 --agent-hosts-file agent_hosts.txt \
        --run-dir runs/baselines/greedy/run-1 \
        --use-profiles agent_profiles.json --use-jobs-dir jobs/

    # With config/job generation:
    python3.11 baselines/run_baseline_remote.py \
        --scheduler round-robin --agents 30 --jobs 500 \
        --db-host 10.0.0.1 --agent-hosts-file agent_hosts.txt \
        --run-dir runs/baselines/rr/run-1
"""
from __future__ import annotations

import argparse
import logging
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# Ensure SwarmAgents root is on sys.path
_SCRIPT_DIR = Path(__file__).resolve().parent
_SWARM_ROOT = _SCRIPT_DIR.parent
if str(_SWARM_ROOT) not in sys.path:
    sys.path.insert(0, str(_SWARM_ROOT))

import redis

from baselines.scheduler import (
    GreedyScheduler,
    RoundRobinScheduler,
    RandomScheduler,
)

SCHEDULER_MAP = {
    "greedy": GreedyScheduler,
    "round-robin": RoundRobinScheduler,
    "random": RandomScheduler,
}


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


# ── SSH / SCP helpers (matching run_test.py conventions) ──────────────

def ssh(host: str, cmd: str) -> int:
    return subprocess.call([
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=10",
        host,
        cmd,
    ])


def ssh_check(host: str, cmd: str) -> None:
    subprocess.run([
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=10",
        host,
        cmd,
    ], text=True, check=True)


def ssh_output(host: str, cmd: str) -> str:
    result = subprocess.run([
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=10",
        host,
        cmd,
    ], text=True, capture_output=True, check=True)
    return result.stdout.strip()


# ── Redis helpers ─────────────────────────────────────────────────────

def cleanup_redis(db_host: str, db_port: int) -> None:
    from swarm.database.repository import Repository

    log("Cleaning up Redis …")
    r = redis.StrictRedis(host=db_host, port=db_port, decode_responses=True)
    repo = Repository(redis_client=r)
    repo.delete_all(key_prefix="*")


# ── Worker lifecycle ──────────────────────────────────────────────────

def load_host_list(hosts_file: str) -> list[str]:
    """Read one hostname per line from hosts file."""
    hosts = []
    with open(hosts_file) as f:
        for line in f:
            h = line.strip()
            if h and not h.startswith("#"):
                hosts.append(h)
    return hosts


def preflight_check(hosts: list[str], remote_repo_dir: str) -> None:
    """Verify SSH connectivity and python3.11 on each host."""
    log("Running preflight checks …")
    failures = []
    for host in hosts:
        check_cmd = (
            f"test -d {shlex.quote(remote_repo_dir)} && "
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
            f"Check SSH access, {remote_repo_dir} exists, and python3.11 is installed."
        )
    log("Preflight checks passed.")


def start_workers(
    agents: list,
    db_host: str,
    db_port: int,
    remote_repo_dir: str,
    level: int,
    group: int,
) -> dict[str, list[int]]:
    """SSH into each agent's host and start baseline_worker.py.

    Returns {host: [agent_id, ...]} mapping for cleanup.
    """
    host_agents: dict[str, list[int]] = {}
    for agent in agents:
        host = agent.host
        host_agents.setdefault(host, []).append(agent.agent_id)

    log(f"Starting workers on {len(host_agents)} host(s) …")
    for host, agent_ids in host_agents.items():
        for aid in agent_ids:
            cmd = (
                f"cd {shlex.quote(remote_repo_dir)} && "
                f"nohup python3.11 baselines/baseline_worker.py "
                f"--agent-id {aid} --db-host {db_host} --db-port {db_port} "
                f"--level {level} --group {group} "
                f"> baseline-worker-{aid}.log 2>&1 & echo $!"
            )
            try:
                pid = ssh_output(host, cmd)
                log(f"  Worker {aid} on {host} (PID {pid})")
            except subprocess.CalledProcessError as e:
                log(f"  FAILED to start worker {aid} on {host}: {e}")

    return host_agents


def wait_for_workers(
    redis_client: redis.StrictRedis,
    agent_ids: list[int],
    timeout: float = 30.0,
) -> None:
    """Wait until all workers have registered their heartbeat in Redis."""
    log(f"Waiting for {len(agent_ids)} workers to register …")
    deadline = time.time() + timeout
    while time.time() < deadline:
        alive = 0
        for aid in agent_ids:
            if redis_client.exists(f"baseline:worker:{aid}:heartbeat"):
                alive += 1
        if alive >= len(agent_ids):
            log(f"All {alive} workers registered.")
            return
        time.sleep(1.0)
    log(f"WARNING: Only {alive}/{len(agent_ids)} workers registered within {timeout}s. Proceeding anyway.")


def stop_workers(redis_client: redis.StrictRedis, host_agents: dict[str, list[int]], remote_repo_dir: str) -> None:
    """Signal workers to stop and kill any remaining processes."""
    log("Signaling workers to shut down …")
    redis_client.set("baseline:shutdown", "1", ex=120)
    time.sleep(3)  # Give workers time to see the signal

    for host, agent_ids in host_agents.items():
        cmd = "pkill -f 'baseline_worker.py' 2>/dev/null; true"
        ssh(host, cmd)

    # Clean up shutdown key
    redis_client.delete("baseline:shutdown")


def collect_remote_logs(host_agents: dict[str, list[int]], run_dir: str, remote_repo_dir: str) -> None:
    """SCP worker logs from remote hosts (best-effort)."""
    log("Collecting logs from remote hosts …")
    for host in host_agents:
        dest_dir = os.path.join(run_dir, host.replace("/", "_"))
        os.makedirs(dest_dir, exist_ok=True)
        scp_cmd = (
            f"scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
            f"-o BatchMode=yes -o ConnectTimeout=10 -q "
            f"{host}:{shlex.quote(remote_repo_dir)}/baseline-worker-*.log "
            f"{shlex.quote(dest_dir)}/ 2>/dev/null"
        )
        subprocess.call(scp_cmd, shell=True)


# ── Config / job generation ──────────────────────────────────────────

def generate_configs_and_jobs(
    agents: int,
    jobs: int,
    db_host: str,
    config_dir: str,
    topology: str,
    enable_dtns: bool,
    agent_hosts_file: str | None = None,
    agents_per_host: int = 1,
) -> tuple[str, str]:
    """Generate agent configs and job files. Returns (profiles_path, jobs_dir)."""
    config_dir_path = Path(config_dir)
    config_dir_path.mkdir(parents=True, exist_ok=True)

    gen_args = [
        sys.executable, "generate_configs.py",
        str(agents), "20",
        "./config_swarm_multi.yml", str(config_dir_path),
        topology, db_host, str(jobs),
        "--agent-type", "resource",
    ]

    if agent_hosts_file:
        gen_args += ["--agent-hosts-file", agent_hosts_file, "--agents-per-host", str(agents_per_host)]

    if enable_dtns:
        gen_args.append("--dtns")

    log(f"$ {' '.join(gen_args)}")
    subprocess.run(gen_args, text=True, check=True)

    profiles = Path("agent_profiles.json")
    if not profiles.exists():
        raise FileNotFoundError(f"Expected {profiles} after config generation")

    jobs_dir = Path("jobs")
    if not jobs_dir.exists():
        raise FileNotFoundError(f"Expected {jobs_dir}/ after config generation")

    return str(profiles.resolve()), str(jobs_dir.resolve())


# ── CLI ──────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run distributed baseline scheduler with remote workers.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    p.add_argument("--scheduler", required=True, choices=list(SCHEDULER_MAP.keys()))
    p.add_argument("--agents", type=int, required=True)
    p.add_argument("--jobs", type=int, required=True)
    p.add_argument("--db-host", type=str, required=True, help="Redis host (must be reachable from all VMs)")
    p.add_argument("--db-port", type=int, default=6379)
    p.add_argument("--agent-hosts-file", type=str, required=True, help="File with one remote hostname per line")
    p.add_argument("--agents-per-host", type=int, default=1, help="Number of agents per remote host (default: 1)")
    p.add_argument("--run-dir", type=str, required=True)
    p.add_argument("--remote-repo-dir", default="/root/SwarmAgents", help="Repo path on remote hosts")

    # Job submission
    p.add_argument("--jobs-per-interval", type=int, default=20)
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--timeout", type=float, default=600.0)

    # Reuse existing configs/jobs
    p.add_argument("--use-profiles", type=str, default=None)
    p.add_argument("--use-jobs-dir", type=str, default=None)
    p.add_argument("--use-config-dir", type=str, default=None)
    p.add_argument("--topology", type=str, default="mesh")
    p.add_argument("--no-dtns", action="store_true")

    # Cost function overrides
    p.add_argument("--cpu-weight", type=float, default=0.4)
    p.add_argument("--ram-weight", type=float, default=0.3)
    p.add_argument("--disk-weight", type=float, default=0.2)
    p.add_argument("--gpu-weight", type=float, default=0.1)
    p.add_argument("--long-job-threshold", type=float, default=20.0)
    p.add_argument("--connectivity-penalty-factor", type=float, default=1.0)

    p.add_argument("--skip-cleanup", action="store_true")
    p.add_argument("--skip-preflight", action="store_true")
    p.add_argument("--worker-timeout", type=float, default=30.0, help="Seconds to wait for workers to register")
    p.add_argument("--debug", action="store_true")

    return p.parse_args()


def main():
    args = parse_args()

    level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    os.chdir(_SWARM_ROOT)

    # 1. Read host list
    host_list = load_host_list(args.agent_hosts_file)
    log(f"Loaded {len(host_list)} hosts from {args.agent_hosts_file}")

    # 2. Preflight
    if not args.skip_preflight:
        preflight_check(host_list, args.remote_repo_dir)

    # 3. Cleanup Redis
    if not args.skip_cleanup:
        cleanup_redis(args.db_host, args.db_port)

    # 4. Generate or reuse configs/jobs
    if args.use_profiles and args.use_jobs_dir:
        profiles_path = str(Path(args.use_profiles).resolve())
        jobs_dir = str(Path(args.use_jobs_dir).resolve())
        log(f"Reusing profiles={profiles_path}, jobs={jobs_dir}")
    else:
        config_dir = args.use_config_dir or "configs"
        profiles_path, jobs_dir = generate_configs_and_jobs(
            agents=args.agents,
            jobs=args.jobs,
            db_host=args.db_host,
            config_dir=config_dir,
            topology=args.topology,
            enable_dtns=not args.no_dtns,
            agent_hosts_file=args.agent_hosts_file,
            agents_per_host=args.agents_per_host,
        )

    # 5. Instantiate scheduler in remote mode
    scheduler_cls = SCHEDULER_MAP[args.scheduler]
    cost_weights = {
        "cpu": args.cpu_weight,
        "ram": args.ram_weight,
        "disk": args.disk_weight,
        "gpu": args.gpu_weight,
    }

    scheduler = scheduler_cls(
        db_host=args.db_host,
        db_port=args.db_port,
        agent_profiles_path=profiles_path,
        jobs_dir=jobs_dir,
        jobs_per_interval=args.jobs_per_interval,
        run_dir=args.run_dir,
        total_jobs=args.jobs,
        interval=args.interval,
        cost_weights=cost_weights,
        long_job_threshold=args.long_job_threshold,
        connectivity_penalty_factor=args.connectivity_penalty_factor,
        timeout=args.timeout,
        remote=True,
    )
    scheduler.load_agents()

    # 6. Start workers on remote hosts
    host_agents = start_workers(
        agents=scheduler.agents,
        db_host=args.db_host,
        db_port=args.db_port,
        remote_repo_dir=args.remote_repo_dir,
        level=0,
        group=0,
    )

    # 7. Wait for workers to register
    redis_client = redis.StrictRedis(host=args.db_host, port=args.db_port, decode_responses=True)
    agent_ids = [a.agent_id for a in scheduler.agents]
    wait_for_workers(redis_client, agent_ids, timeout=args.worker_timeout)

    # 8. Run the scheduler
    log(f"Starting {args.scheduler} scheduler (remote mode) with {args.agents} agents, {args.jobs} jobs")
    t0 = time.time()
    try:
        scheduler.run()
    finally:
        # 9. Stop workers
        stop_workers(redis_client, host_agents, args.remote_repo_dir)

        # 10. Collect logs
        collect_remote_logs(host_agents, args.run_dir, args.remote_repo_dir)

    elapsed = time.time() - t0

    # 11. Save results
    scheduler.save_results()

    # 12. Summary
    print("\n" + "=" * 60)
    print(f"  Scheduler:      {args.scheduler} (REMOTE)")
    print(f"  Agents:         {args.agents}")
    print(f"  Hosts:          {len(host_list)}")
    print(f"  Total Jobs:     {args.jobs}")
    print(f"  Completed:      {scheduler.completed_count}")
    print(f"  Makespan:       {elapsed:.1f}s")
    print(f"  Results:        {args.run_dir}/all_jobs.csv")
    print("=" * 60)


if __name__ == "__main__":
    main()
