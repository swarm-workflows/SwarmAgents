#!/usr/bin/env python3.11
"""
run_baseline_remote.py — a centralized baseline (Greedy / Round-Robin / Random) on the slice.

The scheduler runs here, on `database`; one execution worker per **level-0** agent of the SWARM
cell runs on the host `run_test.py` would place that agent on — agent i on host
``(i - 1) // agents_per_host`` of the same hosts file — so every dispatch, every completion and
every DAG readiness check crosses the same WAN to the same store that SWARM's does. Workers run
`runtime.executor_workers` jobs at once with the base config's wall-time clamp, exactly as an
agent does (`baselines/common.py`). Run it as root on the database node:

    python3.11 baselines/run_baseline_remote.py --scheduler greedy --agents 90 --jobs 1800 \
        --db-host database --agent-hosts-file agent_hosts.txt --run-dir runs/e7/greedy/run01 \
        --use-profiles agent_profiles.json --use-jobs-dir jobs/ --config config_swarm_multi.yml

Pass the SWARM cell's own `--use-profiles`/`--use-jobs-dir` and base config. The run directory
gets `all_jobs.csv`, `metrics.json` keyed by agent with `executed_jobs`, `all_agents.csv`,
`run_meta.json`, `collect_meta.json` (`arm: baseline`, `policy: greedy|round_robin|random`) and
`drain.json`. Exit status: 0 completed or ended on the clock (see `drain.json`), 2 refused to
start, 3 some worker never reported its stats, 4 the store became unreadable.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import shlex
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

# Ensure SwarmAgents root is on sys.path
_SCRIPT_DIR = Path(__file__).resolve().parent
_SWARM_ROOT = _SCRIPT_DIR.parent
if str(_SWARM_ROOT) not in sys.path:
    sys.path.insert(0, str(_SWARM_ROOT))

import redis

from baselines.common import (configure_execution, cost_params, executor_workers, host_of,
                              load_config, wait_for_final_reports)
from baselines.scheduler import SCHEDULER_MAP


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


def start_workers(agents: list, args, run_id: str) -> tuple[dict[int, str], list[int]]:
    """Start one worker per agent on its host. Returns ({agent_id: host}, [failed ids])."""
    placement, failed = {}, []
    log(f"Starting {len(agents)} workers …")
    for agent in agents:
        cmd = (f"cd {shlex.quote(args.remote_repo_dir)} && bash baseline-worker-start.sh "
               f"{shlex.quote(args.remote_python)} {agent.agent_id} "
               f"--db-host {shlex.quote(args.db_host)} --db-port {args.db_port} "
               f"--run-id {shlex.quote(run_id)} --config {shlex.quote(args.remote_config)}")
        try:
            pid = ssh_output(agent.host, cmd)
            placement[agent.agent_id] = agent.host
            log(f"  worker {agent.agent_id} on {agent.host} (pid {pid})")
        except subprocess.CalledProcessError as e:
            log(f"  FAILED to start worker {agent.agent_id} on {agent.host}: {e}")
            failed.append(agent.agent_id)
    return placement, failed


def wait_for_workers(redis_client, keys, agent_ids: list[int], timeout: float) -> list[int]:
    """Wait until every worker's heartbeat is in the store; returns the ones still missing."""
    log(f"Waiting for {len(agent_ids)} workers to register …")
    deadline = time.time() + timeout
    missing = list(agent_ids)
    while time.time() < deadline:
        alive = redis_client.mget([keys.heartbeat(a) for a in agent_ids]) if agent_ids else []
        missing = [a for a, v in zip(agent_ids, alive) if not v]
        if not missing:
            log(f"All {len(agent_ids)} workers registered.")
            break
        time.sleep(1.0)
    return missing


def stop_workers(hosts: list[str], run_id: str) -> None:
    """Stop this run's workers only: the pattern carries the run id."""
    pattern = shlex.quote(f"baseline_worker.py.*--run-id {run_id}")
    for host in sorted(set(hosts)):
        ssh(host, f"pkill -TERM -f {pattern}; sleep 5; pkill -KILL -f {pattern}; true")


def collect_remote_logs(placement: dict[int, str], run_dir: str, remote_repo_dir: str) -> None:
    """Copy each worker's log back (best-effort)."""
    log("Collecting logs from remote hosts …")
    for aid, host in sorted(placement.items()):
        dest_dir = Path(run_dir) / "node-logs" / host
        dest_dir.mkdir(parents=True, exist_ok=True)
        subprocess.call(["scp", "-o", "StrictHostKeyChecking=no", "-o",
                         "UserKnownHostsFile=/dev/null", "-o", "BatchMode=yes", "-o",
                         "ConnectTimeout=10", "-q",
                         f"{host}:{remote_repo_dir}/baseline-worker-{aid}.log", str(dest_dir)],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


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

def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    p.add_argument("--scheduler", required=True, choices=list(SCHEDULER_MAP.keys()))
    p.add_argument("--mode", choices=["remote"], default="remote",
                   help="Accepted so a campaign's `mode: remote` default applies unchanged; "
                        "a local smoke run is baselines/run_baseline.py")
    p.add_argument("--agents", type=int, required=True,
                   help="The SWARM cell's fleet size; its level-0 agents become workers")
    p.add_argument("--jobs", type=int, required=True)
    p.add_argument("--db-host", type=str, required=True, help="Redis host (must be reachable from all VMs)")
    p.add_argument("--db-port", type=int, default=6379)
    p.add_argument("--agent-hosts-file", type=str, required=True,
                   help="The SWARM cell's hosts file; placement is its order, as in run_test.py")
    p.add_argument("--agents-per-host", type=int, default=1, help="Number of agents per remote host (default: 1)")
    p.add_argument("--run-dir", type=str, required=True)
    p.add_argument("--remote-repo-dir", default="/root/SwarmAgents", help="Repo path on remote hosts")
    p.add_argument("--remote-python", default="python3.11")
    p.add_argument("--config", default=str(_SWARM_ROOT / "config_swarm_multi.yml"),
                   help="Base config the SWARM arm ran with: cost parameters, "
                        "runtime.wall_time_* and runtime.executor_workers come from it")
    p.add_argument("--remote-config", default=None,
                   help="Path of that config on the agent hosts (default: the same file name "
                        "under --remote-repo-dir)")

    # Job submission
    p.add_argument("--jobs-per-interval", type=int, default=20)
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--timeout", type=float, default=3600.0,
                   help="Hard cap on the run; drain.json says when it ended on it")

    # Reuse existing configs/jobs
    p.add_argument("--use-profiles", type=str, default=None)
    p.add_argument("--use-jobs-dir", type=str, default=None)
    p.add_argument("--use-config-dir", type=str, default=None)
    p.add_argument("--topology", type=str, default="mesh")
    p.add_argument("--no-dtns", action="store_true")

    # Cost function overrides (default: the base config's job_selection block)
    p.add_argument("--cpu-weight", type=float, default=None)
    p.add_argument("--ram-weight", type=float, default=None)
    p.add_argument("--disk-weight", type=float, default=None)
    p.add_argument("--gpu-weight", type=float, default=None)
    p.add_argument("--long-job-threshold", type=float, default=None)
    p.add_argument("--connectivity-penalty-factor", type=float, default=None)

    p.add_argument("--skip-cleanup", action="store_true")
    p.add_argument("--skip-preflight", action="store_true")
    p.add_argument("--worker-timeout", "--startup-timeout", dest="worker_timeout", type=float,
                   default=60.0, help="Seconds to wait for workers to register")
    p.add_argument("--stats-wait-s", type=float, default=None,
                   help="Drain deadline for workers' final reports after the run ends "
                        "(default: the config's wall_time_max_s + 60, so a capped job finishes)")
    p.add_argument("--debug", action="store_true")

    args = p.parse_args(argv)
    if args.remote_config is None:
        args.remote_config = str(Path(args.remote_repo_dir) / Path(args.config).name)
    if args.stats_wait_s is None:
        from baselines.run_sparrow import drain_deadline
        args.stats_wait_s = drain_deadline(args.config)
    return args


def resolve_cost(args, config: dict) -> dict:
    """The base config's cost parameters, with any CLI override applied on top."""
    params = cost_params(config)
    for dim in ("cpu", "ram", "disk", "gpu"):
        v = getattr(args, f"{dim}_weight")
        if v is not None:
            params["cost_weights"][dim] = v
    if args.long_job_threshold is not None:
        params["long_job_threshold"] = args.long_job_threshold
    if args.connectivity_penalty_factor is not None:
        params["connectivity_penalty_factor"] = args.connectivity_penalty_factor
    return params


def count_job_files(jobs_dir: str) -> int:
    return sum(1 for _ in Path(jobs_dir).glob("job_*.json"))


def _exit_on_signal(signum, _frame):
    # SIGTERM/SIGHUP end the process WITHOUT running `finally` by default, and `finally` is
    # what stops the workers on the agent hosts. As SystemExit, teardown runs.
    raise SystemExit(128 + signum)


def main(argv=None) -> int:
    for sig in (signal.SIGTERM, signal.SIGHUP):
        try:
            signal.signal(sig, _exit_on_signal)
        except ValueError:      # not the main thread (tests)
            pass
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    os.chdir(_SWARM_ROOT)
    run_dir = Path(args.run_dir).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)

    # 1. Hosts, placed exactly as run_test.py places agents.
    host_list = load_host_list(args.agent_hosts_file)
    need = -(-args.agents // args.agents_per_host)
    if len(host_list) < need:
        log(f"REFUSED: {args.agents} agents at {args.agents_per_host}/host need {need} hosts; "
            f"{args.agent_hosts_file} lists {len(host_list)}")
        return 2
    log(f"Loaded {len(host_list)} hosts from {args.agent_hosts_file}")

    if not args.skip_preflight:
        preflight_check(host_list[:need], args.remote_repo_dir)

    run_id = f"baseline-{args.scheduler}-{uuid.uuid4().hex[:12]}"
    os.environ["SWARM_RUN_ID"] = run_id          # the distributor's readiness keys match ours

    if not args.skip_cleanup:
        cleanup_redis(args.db_host, args.db_port)

    # 2. Profiles and jobs (the SWARM cell's, for any number that goes beside SWARM's).
    if args.use_profiles and args.use_jobs_dir:
        profiles_path = str(Path(args.use_profiles).resolve())
        jobs_dir = str(Path(args.use_jobs_dir).resolve())
        log(f"Reusing profiles={profiles_path}, jobs={jobs_dir}")
    else:
        profiles_path, jobs_dir = generate_configs_and_jobs(
            agents=args.agents, jobs=args.jobs, db_host=args.db_host,
            config_dir=args.use_config_dir or "configs", topology=args.topology,
            enable_dtns=not args.no_dtns, agent_hosts_file=args.agent_hosts_file,
            agents_per_host=args.agents_per_host)

    # 3. The base config: cost parameters, the wall-time clamp, per-agent concurrency.
    config = load_config(args.config)
    runtime = config.get("runtime") or {}
    configure_execution(runtime)          # recorded in run_meta; the workers apply it too
    cost = resolve_cost(args, config)
    total = min(args.jobs, count_job_files(jobs_dir))
    if total < args.jobs:
        log(f"WARNING: --jobs {args.jobs} but {jobs_dir} holds {total} job files")

    scheduler = SCHEDULER_MAP[args.scheduler](
        db_host=args.db_host, db_port=args.db_port, agent_profiles_path=profiles_path,
        jobs_dir=jobs_dir, jobs_per_interval=args.jobs_per_interval, run_dir=str(run_dir),
        total_jobs=total, interval=args.interval,
        executor_workers=executor_workers(runtime), timeout=args.timeout, remote=True,
        run_id=run_id, agents=args.agents, **cost)
    try:
        scheduler.load_agents()
    except SystemExit as exc:          # a profile set for a smaller fleet: a refusal, not a crash
        log(f"REFUSED: {exc}")
        return 2
    for agent in scheduler.agents:
        agent.host = host_of(host_list, agent.agent_id, args.agents_per_host)
    agent_ids = [a.agent_id for a in scheduler.agents]
    worker_hosts = sorted({a.host for a in scheduler.agents})

    redis_client = redis.StrictRedis(host=args.db_host, port=args.db_port, decode_responses=True)
    keys = scheduler.keys
    placement: dict[int, str] = {}
    exit_code = 0
    t0 = time.time()
    try:
        # 4. Workers. A fleet with silent workers is a smaller fleet than the cell claims, and
        # its numbers would sit beside SWARM's as if it were not, so it is refused.
        placement, failed = start_workers(scheduler.agents, args, run_id)
        missing = wait_for_workers(redis_client, keys, agent_ids, args.worker_timeout)
        if failed or missing:
            log(f"REFUSED: workers not running — failed to start {failed[:10]}, "
                f"not registered {missing[:10]}{'…' if len(missing) > 10 else ''}")
            (run_dir / "drain.json").write_text(json.dumps(
                {"status": "startup_refused", "failed_workers": failed,
                 "missing_workers": missing}, indent=2))
            return 2

        # 5. Schedule.
        log(f"Starting {args.scheduler} (remote) over {len(agent_ids)} level-0 workers of "
            f"{args.agents} agents, {total} jobs, {scheduler.executor_workers} concurrent each")
        scheduler.run()
    finally:
        try:
            redis_client.set(keys.shutdown, "1", ex=3600)
        except redis.RedisError:
            pass
        # Every worker's FINAL report — written once its pool has drained — not any stats key:
        # periodic snapshots exist from the first 10 s, and stopping on those killed workers
        # mid-job. Past the deadline the stop below kills what is left, and save_results
        # records those workers as a shortfall (exit 3).
        wait_for_final_reports(redis_client, {a: keys.stats("worker", a) for a in agent_ids},
                               args.stats_wait_s)
        stop_workers(worker_hosts, run_id)
        collect_remote_logs(placement, str(run_dir), args.remote_repo_dir)

    elapsed = time.time() - t0
    exit_code = scheduler.save_results(extra_meta={
        "agents_per_host": args.agents_per_host, "config": args.config,
        "cost": cost, "argv": sys.argv,
        "placement": {str(a): h for a, h in sorted(placement.items())}})

    print("\n" + "=" * 60)
    print(f"  Scheduler:      {args.scheduler} (REMOTE)")
    print(f"  Workers:        {len(agent_ids)} level-0 of {args.agents} agents")
    print(f"  Hosts:          {len(worker_hosts)}")
    print(f"  Total Jobs:     {total}")
    print(f"  Completed:      {scheduler.completed_count}")
    print(f"  Ended:          {scheduler.drain.get('status')} after {elapsed:.1f}s")
    print(f"  Results:        {run_dir}/all_jobs.csv (exit {exit_code})")
    print("=" * 60)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
