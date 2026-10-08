#!/usr/bin/env python3.11
"""
run_baseline.py — a centralized baseline (Greedy, Round-Robin, Random) in ONE local process.

Scheduling and execution share this host: each level-0 agent gets its own pool of
`runtime.executor_workers` threads. It pays no WAN and is for smoke tests only — the E7 table
comes from `baselines/run_baseline_remote.py` on the slice. Output and exit statuses are the
remote runner's (see there).

Usage:
  python baselines/run_baseline.py \
      --scheduler greedy \
      --agents 30 --jobs 500 --db-host localhost \
      --jobs-per-interval 20 --run-dir runs/baseline-greedy-30

  # Reuse existing configs/jobs instead of regenerating:
  python baselines/run_baseline.py \
      --scheduler round-robin \
      --agents 30 --jobs 500 --db-host localhost \
      --jobs-per-interval 20 --run-dir runs/baseline-rr-30 \
      --use-config-dir configs/ --use-jobs-dir jobs/
"""
from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# Ensure SwarmAgents root is on sys.path so imports resolve
_SCRIPT_DIR = Path(__file__).resolve().parent
_SWARM_ROOT = _SCRIPT_DIR.parent
if str(_SWARM_ROOT) not in sys.path:
    sys.path.insert(0, str(_SWARM_ROOT))

import uuid

from baselines.common import configure_execution, executor_workers, load_config
from baselines.scheduler import SCHEDULER_MAP
from baselines.run_baseline_remote import count_job_files, resolve_cost


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def run_cmd(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a subprocess, logging the command."""
    log(f"$ {' '.join(cmd)}")
    return subprocess.run(cmd, text=True, check=check)


def cleanup_redis(db_host: str, db_port: int) -> None:
    """Flush all keys from Redis."""
    import redis
    from swarm.database.repository import Repository

    log("Cleaning up Redis …")
    r = redis.StrictRedis(host=db_host, port=db_port, decode_responses=True)
    repo = Repository(redis_client=r)
    repo.delete_all(key_prefix="*")


def generate_configs_and_jobs(
    agents: int,
    jobs: int,
    db_host: str,
    config_dir: str,
    topology: str = "mesh",
    enable_dtns: bool = True,
) -> tuple[str, str]:
    """Generate agent configs and job files using existing generate_configs.py.

    Returns (agent_profiles_path, jobs_dir).
    """
    config_dir_path = Path(config_dir)
    config_dir_path.mkdir(parents=True, exist_ok=True)

    # Write a minimal agent_hosts.txt
    hosts_file = Path("agent_hosts.txt")
    hosts_file.write_text("localhost\n")

    gen_args = [
        sys.executable, "generate_configs.py",
        str(agents), "20",  # jobs_per_proposal (unused by baselines, but required arg)
        "./config_swarm_multi.yml", str(config_dir_path),
        topology, db_host, str(jobs),
        "--agent-hosts-file", str(hosts_file),
        "--agents-per-host", str(agents),
        "--agent-type", "resource",
    ]
    if enable_dtns:
        gen_args.append("--dtns")

    run_cmd(gen_args)

    agent_profiles = Path("agent_profiles.json")
    if not agent_profiles.exists():
        raise FileNotFoundError(f"Expected {agent_profiles} after config generation")

    jobs_dir = Path("jobs")
    if not jobs_dir.exists():
        raise FileNotFoundError(f"Expected {jobs_dir}/ after config generation")

    return str(agent_profiles.resolve()), str(jobs_dir.resolve())


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a baseline scheduler (Greedy / Round-Robin / Random).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--scheduler", required=True, choices=list(SCHEDULER_MAP.keys()),
        help="Baseline scheduling strategy",
    )
    parser.add_argument("--agents", type=int, required=True, help="Number of agents")
    parser.add_argument("--jobs", type=int, required=True, help="Total number of jobs")
    parser.add_argument("--db-host", type=str, required=True, help="Redis host")
    parser.add_argument("--db-port", type=int, default=6379, help="Redis port")
    parser.add_argument(
        "--jobs-per-interval", type=int, default=20,
        help="Jobs submitted to Redis per interval (default: 20)",
    )
    parser.add_argument(
        "--interval", type=float, default=1.0,
        help="Seconds between job submission batches (default: 1.0)",
    )
    parser.add_argument("--run-dir", type=str, required=True, help="Output directory for results")
    parser.add_argument("--topology", type=str, default="mesh", help="Topology for config gen (default: mesh)")
    parser.add_argument("--executor-workers", type=int, default=None,
                        help="Concurrent jobs per agent (default: runtime.executor_workers)")
    parser.add_argument("--config", default=str(_SWARM_ROOT / "config_swarm_multi.yml"),
                        help="Base config: cost parameters, runtime.wall_time_*, executor_workers")
    parser.add_argument("--timeout", type=float, default=600.0, help="Max run time in seconds (default: 600)")
    parser.add_argument("--no-dtns", action="store_true", help="Disable DTN generation")

    # Reuse existing configs/jobs
    parser.add_argument(
        "--use-config-dir", type=str, default=None,
        help="Reuse existing config dir (skip generation)",
    )
    parser.add_argument(
        "--use-jobs-dir", type=str, default=None,
        help="Reuse existing jobs dir (skip generation)",
    )
    parser.add_argument(
        "--use-profiles", type=str, default=None,
        help="Path to existing agent_profiles.json (skip generation)",
    )

    # Cost function overrides (default: the base config's job_selection block)
    parser.add_argument("--cpu-weight", type=float, default=None)
    parser.add_argument("--ram-weight", type=float, default=None)
    parser.add_argument("--disk-weight", type=float, default=None)
    parser.add_argument("--gpu-weight", type=float, default=None)
    parser.add_argument("--long-job-threshold", type=float, default=None)
    parser.add_argument("--connectivity-penalty-factor", type=float, default=None)

    parser.add_argument("--skip-cleanup", action="store_true", help="Skip Redis cleanup before run")
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG logging")

    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    # Logging
    level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # Change to SwarmAgents root so relative imports and file refs work
    os.chdir(_SWARM_ROOT)

    # 1. Cleanup Redis
    if not args.skip_cleanup:
        cleanup_redis(args.db_host, args.db_port)

    # 2. Generate or reuse configs/jobs
    if args.use_profiles and args.use_jobs_dir:
        profiles_path = str(Path(args.use_profiles).resolve())
        jobs_dir = str(Path(args.use_jobs_dir).resolve())
        log(f"Reusing profiles={profiles_path}, jobs={jobs_dir}")
    else:
        config_dir = args.use_config_dir or "configs"
        enable_dtns = not args.no_dtns
        profiles_path, jobs_dir = generate_configs_and_jobs(
            agents=args.agents,
            jobs=args.jobs,
            db_host=args.db_host,
            config_dir=config_dir,
            topology=args.topology,
            enable_dtns=enable_dtns,
        )

    # 3. The base config: cost parameters, the wall-time clamp, per-agent concurrency.
    config = load_config(args.config)
    runtime = config.get("runtime") or {}
    configure_execution(runtime)
    run_id = f"baseline-{args.scheduler}-{uuid.uuid4().hex[:12]}"
    os.environ["SWARM_RUN_ID"] = run_id
    total = min(args.jobs, count_job_files(jobs_dir))

    scheduler = SCHEDULER_MAP[args.scheduler](
        db_host=args.db_host, db_port=args.db_port, agent_profiles_path=profiles_path,
        jobs_dir=jobs_dir, jobs_per_interval=args.jobs_per_interval, run_dir=args.run_dir,
        total_jobs=total, interval=args.interval,
        executor_workers=args.executor_workers or executor_workers(runtime),
        timeout=args.timeout, run_id=run_id, agents=args.agents,
        **resolve_cost(args, config))

    # 4. Load the level-0 fleet and run
    try:
        scheduler.load_agents()
    except SystemExit as exc:
        log(f"REFUSED: {exc}")
        return 2

    log(f"Starting {args.scheduler} scheduler with {len(scheduler.agents)} level-0 agents, "
        f"{total} jobs")
    t0 = time.time()
    scheduler.run()
    elapsed = time.time() - t0

    # 5. Save results
    exit_code = scheduler.save_results(extra_meta={"config": args.config, "argv": sys.argv})

    # 6. Summary
    print("\n" + "=" * 60)
    print(f"  Scheduler:      {args.scheduler}")
    print(f"  Agents:         {len(scheduler.agents)} level-0 of {args.agents}")
    print(f"  Total Jobs:     {total}")
    print(f"  Completed:      {scheduler.completed_count}")
    print(f"  Ended:          {scheduler.drain.get('status')} after {elapsed:.1f}s")
    print(f"  Results:        {args.run_dir}/all_jobs.csv (exit {exit_code})")
    print("=" * 60)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
