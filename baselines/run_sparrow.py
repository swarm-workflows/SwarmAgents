#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""Run the Sparrow-style decentralized baseline (baselines/sparrow.py) end to end.

Remote mode (the only one admissible beside SWARM's numbers): one worker per agent id on the
same hosts, in the same order, as `run_test.py` places SWARM agents — agent i on host
``(i - 1) // agents_per_host`` of the hosts file — and ``--schedulers`` scheduler processes
spread evenly over those hosts, so every probe and every claim crosses the same WAN to the
`database` store that SWARM's job pool does. Run it on the `database` node as root.

    python baselines/run_sparrow.py --mode remote --agents 90 --jobs 1800 \\
        --db-host database --agent-hosts-file agent_hosts.txt --run-dir runs/sparrow/run01 \\
        --use-profiles agent_profiles.json --use-jobs-dir jobs/

Local mode runs every node as a subprocess on this host against a local Redis; it pays no WAN
and exists for smoke tests only.

The run directory gets what the collector and the comparison plot read: `all_jobs.csv` /
`pending_jobs.csv` (SWARM's own exporter), `metrics.json` keyed by agent id with each worker's
`executed_jobs` (so `jobs_executed_twice` is measured), `all_agents.csv` (the level-0 fleet, so
fairness counts idle workers), `run_meta.json`, `collect_meta.json` (`policy: sparrow`,
`arm: baseline`) and `drain.json`. Exit status: 0 completed or ended on the clock (see
`drain.json`), 2 refused to start, 3 some worker never reported its stats, 4 the store became
unreadable.
"""
from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import redis  # noqa: E402

from baselines.agent_sim import SimulatedAgent  # noqa: E402
from baselines.sparrow import Keys, publish_fleet  # noqa: E402
from swarm.database.repository import Repository  # noqa: E402
from swarm.models.job import ObjectState  # noqa: E402


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


SSH_OPTS = ["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
            "-o", "BatchMode=yes", "-o", "ConnectTimeout=10"]


def ssh_output(host: str, cmd: str) -> str:
    return subprocess.run(["ssh", *SSH_OPTS, host, cmd], text=True, capture_output=True,
                          check=True).stdout.strip()


def ssh_call(host: str, cmd: str) -> int:
    return subprocess.call(["ssh", *SSH_OPTS, host, cmd],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def load_hosts(path: str) -> list[str]:
    with open(path) as fh:
        return [ln.strip() for ln in fh if ln.strip() and not ln.startswith("#")]


def load_fleet_profiles(path: str, agents: int) -> list[SimulatedAgent]:
    """Agents 1..N from agent_profiles.json — level-0 executors only, as SWARM's fleet."""
    with open(path) as fh:
        profiles = json.load(fh)
    fleet = []
    for i in range(1, agents + 1):
        prof = profiles.get(str(i)) if isinstance(profiles, dict) else None
        if prof is None:
            raise SystemExit(f"agent {i} is not in {path}; the profile set is for a smaller fleet")
        if int(prof.get("level") or 0) != 0:
            continue
        fleet.append(SimulatedAgent.from_profile(i, prof))
    return fleet


def scheduler_agents(n_agents: int, n_schedulers: int) -> list[int]:
    """Agent ids whose hosts carry a scheduler: evenly spaced, so on a site-interleaved hosts
    file the schedulers land on different sites."""
    return [1 + (j * n_agents) // n_schedulers for j in range(n_schedulers)]


def count_job_files(jobs_dir: str) -> int:
    return sum(1 for p in Path(jobs_dir).glob("job_*.json"))


def completed_count(repo: Repository) -> int:
    ids = repo.get_all_ids_multi(key_prefix=Repository.KEY_JOB, level=0, group=0,
                                 states=[ObjectState.COMPLETE.value])
    return len(ids.get(ObjectState.COMPLETE.value, []))


class Nodes:
    """Launch and stop worker/scheduler processes, locally or over ssh."""

    def __init__(self, args, run_id: str, run_dir: Path):
        self.args, self.run_id, self.run_dir = args, run_id, run_dir
        self.local: list[subprocess.Popen] = []
        self.placement: dict[str, str] = {}

    def _node_args(self, role: str, extra: list[str]) -> list[str]:
        a = self.args
        common = ["--db-host", a.db_host, "--db-port", str(a.db_port), "--run-id", self.run_id]
        if a.config:
            cfg = a.config if a.mode == "local" else a.remote_config
            common += ["--config", cfg]
        return [role, *common, *extra]

    def start(self, role: str, node_id, host: str | None, extra: list[str]) -> None:
        argv = self._node_args(role, extra)
        if self.args.mode == "local":
            logf = open(self.run_dir / f"sparrow-{role}-{node_id}.log", "w")
            self.local.append(subprocess.Popen(
                [sys.executable, str(_ROOT / "baselines" / "sparrow_node.py"), *argv],
                stdout=logf, stderr=subprocess.STDOUT, cwd=str(_ROOT)))
            self.placement[f"{role}-{node_id}"] = "localhost"
            return
        cmd = (f"cd {shlex.quote(self.args.remote_repo_dir)} && bash sparrow-node-start.sh "
               f"{shlex.quote(self.args.remote_python)} {role} {node_id} "
               + " ".join(shlex.quote(x) for x in argv[1:]))
        pid = ssh_output(host, cmd)
        self.placement[f"{role}-{node_id}"] = host
        log(f"  {role} {node_id} on {host} (pid {pid})")

    def stop(self, hosts: list[str]) -> None:
        if self.args.mode == "local":
            for p in self.local:
                if p.poll() is None:
                    p.terminate()
            for p in self.local:
                try:
                    p.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    p.kill()
            return
        # Only this run's processes: the pattern carries the run id.
        pattern = shlex.quote(f"sparrow_node.py.*--run-id {self.run_id}")
        for host in sorted(set(hosts)):
            ssh_call(host, f"pkill -TERM -f {pattern}; sleep 5; pkill -KILL -f {pattern}; true")

    def collect_logs(self) -> None:
        if self.args.mode == "local":
            return
        for host in sorted(set(self.placement.values())):
            dest = self.run_dir / "node-logs" / host
            dest.mkdir(parents=True, exist_ok=True)
            subprocess.call(["scp", *SSH_OPTS, "-q",
                             f"{host}:{self.args.remote_repo_dir}/sparrow-*.log", str(dest)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def wait_registered(client, keys: Keys, workers: list[int], n_sched: int,
                    timeout: float) -> tuple[list[int], list[int]]:
    deadline = time.time() + timeout
    missing_w, missing_s = workers, list(range(n_sched))
    while time.time() < deadline:
        w_alive = client.mget([keys.heartbeat(w) for w in workers]) if workers else []
        s_alive = client.mget([keys.scheduler_heartbeat(i) for i in range(n_sched)])
        missing_w = [w for w, v in zip(workers, w_alive) if not v]
        missing_s = [i for i, v in zip(range(n_sched), s_alive) if not v]
        if not missing_w and not missing_s:
            break
        time.sleep(1.0)
    return missing_w, missing_s


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", choices=["remote", "local"], default="remote")
    p.add_argument("--agents", type=int, required=True)
    p.add_argument("--jobs", type=int, required=True)
    p.add_argument("--db-host", required=True)
    p.add_argument("--db-port", type=int, default=6379)
    p.add_argument("--run-dir", required=True)
    p.add_argument("--agent-hosts-file", help="remote: one host per line, same file as SWARM's")
    p.add_argument("--agents-per-host", type=int, default=1)
    p.add_argument("--remote-repo-dir", default="/root/SwarmAgents")
    p.add_argument("--remote-python", default="python3.11")
    p.add_argument("--config", default=str(_ROOT / "config_swarm_multi.yml"),
                   help="Base config for runtime.wall_time_* and executor_workers — the one the "
                        "SWARM arm ran with, so jobs sleep the same and workers run as many.")
    p.add_argument("--remote-config", default=None,
                   help="Path of that config on the agent hosts (default: the same file name "
                        "under --remote-repo-dir).")
    p.add_argument("--schedulers", type=int, default=0,
                   help="Independent schedulers (default: one per 10 agents, at least 1).")
    p.add_argument("--probe-ratio", type=int, default=2, help="Probes per job (Sparrow's d).")
    p.add_argument("--reprobe-s", type=float, default=30.0)
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--use-profiles", required=True, help="agent_profiles.json of the SWARM arm")
    p.add_argument("--use-jobs-dir", required=True, help="jobs directory of the SWARM arm")
    p.add_argument("--jobs-per-interval", type=int, default=20)
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--timeout", type=float, default=3600.0,
                   help="Hard cap on the run; drain.json says when it ended on it.")
    p.add_argument("--startup-timeout", type=float, default=60.0)
    p.add_argument("--stats-wait-s", type=float, default=60.0)
    p.add_argument("--skip-cleanup", action="store_true")
    args = p.parse_args(argv)
    if args.mode == "remote" and not args.agent_hosts_file:
        p.error("--mode remote needs --agent-hosts-file")
    if args.remote_config is None:
        args.remote_config = str(Path(args.remote_repo_dir) / Path(args.config).name)
    if args.schedulers <= 0:
        args.schedulers = max(1, args.agents // 10)
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    run_dir = Path(args.run_dir).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    run_id = f"sparrow-{uuid.uuid4().hex[:12]}"
    os.environ["SWARM_RUN_ID"] = run_id          # the distributor's readiness keys match ours
    started = time.time()

    fleet = load_fleet_profiles(args.use_profiles, args.agents)
    worker_ids = [a.agent_id for a in fleet]
    hosts: list[str] = []
    if args.mode == "remote":
        hosts = load_hosts(args.agent_hosts_file)
        need = -(-args.agents // args.agents_per_host)
        if len(hosts) < need:
            log(f"REFUSED: {args.agents} agents at {args.agents_per_host}/host need {need} "
                f"hosts; {args.agent_hosts_file} lists {len(hosts)}")
            return 2

    def host_of(agent_id: int) -> str | None:
        return hosts[(agent_id - 1) // args.agents_per_host] if hosts else None

    client = redis.StrictRedis(host=args.db_host, port=args.db_port, decode_responses=True)
    repo = Repository(client, run_id=run_id)
    keys = Keys(run_id)
    if not args.skip_cleanup:
        log("Flushing the store (job and sparrow keys) …")
        repo.delete_all(key_prefix="*")
    publish_fleet(client, keys, fleet)

    total = min(args.jobs, count_job_files(args.use_jobs_dir))
    if total < args.jobs:
        log(f"WARNING: --jobs {args.jobs} but {args.use_jobs_dir} holds {total} job files")

    nodes = Nodes(args, run_id, run_dir)
    sched_hosts = scheduler_agents(args.agents, args.schedulers)
    exit_code = 0
    drain = {"status": None}
    try:
        log(f"Starting {len(worker_ids)} workers and {args.schedulers} schedulers ({args.mode})")
        for w in worker_ids:
            nodes.start("worker", w, host_of(w), ["--agent-id", str(w)])
        for i in range(args.schedulers):
            extra = ["--index", str(i), "--of", str(args.schedulers),
                     "--probe-ratio", str(args.probe_ratio), "--reprobe-s", str(args.reprobe_s)]
            if args.seed is not None:
                extra += ["--seed", str(args.seed)]
            nodes.start("scheduler", i, host_of(sched_hosts[i]), extra)

        missing_w, missing_s = wait_registered(client, keys, worker_ids, args.schedulers,
                                               args.startup_timeout)
        if missing_w or missing_s:
            # Refused rather than run short: a fleet with silent workers is a smaller fleet
            # than the cell claims, and its numbers would sit beside SWARM's as if it were not.
            log(f"REFUSED: not registered within {args.startup_timeout:.0f}s — workers "
                f"{missing_w[:10]}{'…' if len(missing_w) > 10 else ''}, schedulers {missing_s}")
            drain = {"status": "startup_refused", "missing_workers": missing_w,
                     "missing_schedulers": missing_s}
            return 2

        from job_distributor import JobDistributor
        distributor = JobDistributor(redis_host=args.db_host, redis_port=args.db_port,
                                     jobs_dir=args.use_jobs_dir,
                                     jobs_per_interval=args.jobs_per_interval,
                                     interval=args.interval, level=0, group=0)
        distributor.daemon = True
        distributor.start()
        log(f"Distributing {total} jobs; probe ratio {args.probe_ratio}")

        deadline = started + args.timeout
        unreadable_since = None
        done = 0
        while True:
            try:
                done = completed_count(repo)
                unreadable_since = None
            except redis.RedisError as exc:
                unreadable_since = unreadable_since or time.time()
                if time.time() - unreadable_since > 60:
                    log(f"Store unreadable for 60 s: {exc}")
                    drain = {"status": "redis_unreadable"}
                    exit_code = 4
                    break
            if done >= total:
                drain = {"status": "all_terminal"}
                break
            if time.time() >= deadline:
                drain = {"status": "timer"}
                break
            time.sleep(2.0)
        drain["completed_at_stop"] = done
        drain["jobs"] = total
        log(f"Run ended: {drain['status']} ({done}/{total} complete)")
    finally:
        try:
            client.set(keys.shutdown, "1", ex=3600)
        except redis.RedisError:
            pass
        # Give nodes time to finish running jobs and write their final stats, then make sure.
        wait_until = time.time() + args.stats_wait_s
        while time.time() < wait_until:
            try:
                have = sum(1 for v in client.mget([keys.stats("worker", w) for w in worker_ids])
                           if v)
            except redis.RedisError:
                break
            if have >= len(worker_ids):
                break
            time.sleep(1.0)
        nodes.stop(hosts)
        nodes.collect_logs()
        drain["ended_at"] = time.time()
        (run_dir / "drain.json").write_text(json.dumps(drain, indent=2))

    return write_results(args, client, repo, keys, run_dir, run_id, fleet, nodes, total,
                         started, exit_code)


def write_results(args, client, repo, keys, run_dir: Path, run_id: str, fleet, nodes,
                  total: int, started: float, exit_code: int) -> int:
    from plotting.data import save_jobs
    save_jobs(repo.get_all_objects(key_prefix=Repository.KEY_JOB, level=0, group=0),
              str(run_dir))

    metrics, missing = {}, []
    for a in fleet:
        raw = client.get(keys.stats("worker", a.agent_id))
        if not raw:
            missing.append(a.agent_id)
            continue
        stats = json.loads(raw)
        metrics[str(a.agent_id)] = {
            "id": a.agent_id, "run_id": run_id,
            "executed_jobs": stats.pop("executed_jobs", []),
            "sparrow": stats,
        }
    (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))
    if missing:
        (run_dir / "metrics_shortfall.json").write_text(json.dumps(
            {"missing_agents": missing, "expected": len(fleet)}, indent=2))
        log(f"{len(missing)} worker(s) never reported stats: {missing[:10]}")
        exit_code = exit_code or 3

    (run_dir / "all_agents.csv").write_text(json.dumps([
        {"agent_id": a.agent_id, "level": 0, "group": 0,
         "capacities": a.capacities.to_dict() or {}, "dtns": sorted(a.dtns or {})}
        for a in fleet], indent=2))

    sched_stats = {}
    for i in range(args.schedulers):
        raw = client.get(keys.stats("scheduler", i))
        sched_stats[str(i)] = json.loads(raw) if raw else None
    (run_dir / "run_meta.json").write_text(json.dumps({
        "run_id": run_id, "scheduler": "sparrow", "mode": args.mode,
        "agents": args.agents, "agents_per_host": args.agents_per_host, "jobs": total,
        "schedulers": args.schedulers, "probe_ratio": args.probe_ratio,
        "reprobe_s": args.reprobe_s, "seed": args.seed,
        "profiles": str(Path(args.use_profiles).resolve()),
        "jobs_dir": str(Path(args.use_jobs_dir).resolve()),
        "config": args.config, "argv": sys.argv,
        "started_at": started, "ended_at": time.time(),
        "started_at_utc": datetime.fromtimestamp(started, timezone.utc).isoformat(),
        "placement": nodes.placement, "scheduler_stats": sched_stats,
    }, indent=2))
    (run_dir / "collect_meta.json").write_text(json.dumps(
        {"arm": "baseline", "policy": "sparrow"}, indent=2))

    probes = sum((s or {}).get("probes_sent", 0) for s in sched_stats.values())
    noops = sum(m["sparrow"].get("noops", 0) for m in metrics.values())
    log(f"Results in {run_dir}: probes {probes}, no-ops {noops}, exit {exit_code}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
