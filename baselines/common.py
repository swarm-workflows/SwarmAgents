# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""What every external baseline shares with SWARM, defined once.

The E7 table compares SWARM against three centralized schedulers and a Sparrow-style one. A
row there is only about the *scheduler* if everything that is not the scheduler is SWARM's own:
the fleet (level-0 agents, never coordinator slots), how long a job sleeps
(`runtime.wall_time_*`), how many jobs an agent runs at once (`runtime.executor_workers`), the
workflow DAG gate, and what a completion write publishes. Each of those is defined here once
and used by every baseline; the centralized arms used to carry their own copies, and every copy
had drifted (code review 2026-10-07, plan E7).
"""
from __future__ import annotations

import json
import logging
from typing import List, Optional

from baselines.agent_sim import SimulatedAgent
from swarm.database.repository import Repository
from swarm.models.job import Job, ObjectState

logger = logging.getLogger("baselines")

LEVEL, GROUP = 0, 0


def load_config(path: Optional[str]) -> dict:
    """The base config, read with the strict loader SWARM agents use (a duplicate key raises)."""
    if not path:
        return {}
    from swarm.utils.yaml_strict import safe_load
    with open(path) as fh:
        return safe_load(fh) or {}


def runtime_config(path: Optional[str]) -> dict:
    """`runtime:` from the base config."""
    return load_config(path).get("runtime") or {}


def configure_execution(runtime: dict) -> None:
    """The same clamp `Agent._configure_job_execution_simulation` applies, from the same keys
    and the same defaults, so a baseline job sleeps exactly as long as a SWARM job would."""
    Job.configure_execution_simulation(
        scale=float(runtime.get("wall_time_scale", 1.0)),
        min_s=float(runtime.get("wall_time_min_s", 0.0)),
        max_s=float(runtime.get("wall_time_max_s", 120.0)))


def executor_workers(runtime: dict) -> int:
    """Concurrent jobs per agent. Same key as SWARM; its fallback is the shipped value (10),
    not the agent's code fallback (3), because the shipped file is what every cell runs."""
    return max(1, int(runtime.get("executor_workers", 10)))


def cost_params(config: dict) -> dict:
    """`job_selection` cost parameters with ResourceAgent's fallbacks."""
    js = config.get("job_selection") or {}
    w = js.get("cost_weights") or {}
    return {
        "cost_weights": {"cpu": float(w.get("cpu", 0.4)), "ram": float(w.get("ram", 0.3)),
                         "disk": float(w.get("disk", 0.2)), "gpu": float(w.get("gpu", 0.1))},
        "long_job_threshold": float(js.get("long_job_threshold", 20.0)),
        "connectivity_penalty_factor": float(js.get("connectivity_penalty_factor", 1.0)),
    }


def data_ready(repo: Repository, job: Job) -> bool:
    """SWARM's workflow DAG gate, file-dependency form. A failure to check is not readiness."""
    pred = job.data_predicate
    if not pred:
        return True
    if pred.get("kind") == "files" or "files" in pred:
        try:
            return repo.data_available(list(pred.get("files") or []))
        except Exception:
            return False
    # The quantum measurement-stream predicate has no baseline analogue; never release it.
    return False


def load_level0_fleet(path: str, agents: int) -> List[SimulatedAgent]:
    """Agents 1..N from agent_profiles.json, level-0 executors only — SWARM's executing fleet.

    At Hier-N the profile set includes the coordinator slots; a baseline that took them as
    executors ran on N agents where SWARM executes on fewer, which flatters the baseline."""
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


def host_of(hosts: List[str], agent_id: int, agents_per_host: int) -> Optional[str]:
    """Where `run_test.py` places agent *agent_id*: block ``(id - 1) // agents_per_host``."""
    return hosts[(agent_id - 1) // agents_per_host] if hosts else None


def run_job(repo: Repository, job: Job, agent_id: int,
            executed: Optional[list] = None) -> Optional[bool]:
    """Execute *job* as agent *agent_id* and persist the outcome.

    Returns None when the job was NOT run: the READY -> RUNNING claim was refused because the
    record is no longer a READY job assigned to this agent (a duplicate dispatch, or one already
    taken). Otherwise True, or False when the completion write was refused.

    The claim is a compare-and-set on the record, so two deliveries of one assignment cannot
    both execute it, and a job is never run after it completed. The completion write is
    conditional on the record still being this agent's RUNNING job. Outputs are published only
    when the job succeeded, inside the completion write — ResourceAgent.execute_job's rule, so a
    DAG's children are released by the same transaction that records their parent done."""
    me = str(agent_id)

    def claimable(cur) -> bool:
        return bool(cur) and str(cur.get("leader_id")) == me \
            and cur.get("state") == ObjectState.READY.value

    def running_here(cur) -> bool:
        return bool(cur) and str(cur.get("leader_id")) == me \
            and cur.get("state") == ObjectState.RUNNING.value

    job.state = ObjectState.RUNNING
    job.mark_started()
    if not repo.save(obj=job.to_dict(), key_prefix=Repository.KEY_JOB, level=LEVEL,
                     group=GROUP, precondition=claimable):
        return None
    if executed is not None:
        executed.append(str(job.job_id))
    try:
        job.execute()
    except Exception as exc:          # an execution failure is the job's outcome
        logger.error("job %s raised: %s", job.job_id, exc)
        job.exit_status = 1
        if not job.completed_at:
            job.mark_completed()
    job.state = ObjectState.COMPLETE
    produced = ([d.file for d in (job.data_out or []) if getattr(d, "file", None)]
                if job.exit_status == 0 else None)
    return bool(repo.save(obj=job.to_dict(), key_prefix=Repository.KEY_JOB, level=LEVEL,
                          group=GROUP, produced_data=produced or None,
                          precondition=running_here))


def final_reports(redis_client, stats_keys: dict) -> dict:
    """`{id: payload}` for the nodes whose LAST stats write is in the store — `final: true`,
    written after the node stopped taking work and its running jobs finished. Periodic
    snapshots are not reports: one taken before a job started omits it from `executed_jobs`."""
    out = {}
    ids = list(stats_keys)
    for nid, raw in zip(ids, redis_client.mget([stats_keys[i] for i in ids]) if ids else []):
        if raw:
            payload = json.loads(raw)
            if payload.get("final"):
                out[nid] = payload
    return out


def wait_for_final_reports(redis_client, stats_keys: dict, timeout: float) -> dict:
    """Wait up to *timeout* for every node's final report; returns the ones that arrived. A node
    still running a job keeps the run waiting until the job ends or the deadline passes — the
    deadline should exceed `runtime.wall_time_max_s`, or a capped job is killed mid-run."""
    import time as _time
    import redis as _redis
    deadline = _time.time() + timeout
    got = {}
    while True:
        try:
            got = final_reports(redis_client, stats_keys)
        except _redis.RedisError:
            return got
        if len(got) >= len(stats_keys) or _time.time() >= deadline:
            return got
        _time.sleep(1.0)


def read_reports(redis_client, stats_keys: dict) -> dict:
    """Every node's latest stats payload, final or not: `{id: payload}`."""
    ids = list(stats_keys)
    out = {}
    for nid, raw in zip(ids, redis_client.mget([stats_keys[i] for i in ids]) if ids else []):
        if raw:
            out[nid] = json.loads(raw)
    return out


def keyed_metrics(ids, reports: dict, run_id: str, label: str):
    """`metrics.json` in SWARM's shape — keyed by agent id, each with `executed_jobs` — and the
    ids that did not deliver a FINAL report. A non-final snapshot is kept (its `executed_jobs`
    is a lower bound, which `collect.py` already reads that way when metrics are incomplete)
    but its id is still a shortfall: a worker killed mid-job may have started work its last
    snapshot does not list."""
    metrics, missing = {}, []
    for aid in ids:
        payload = reports.get(aid)
        if payload is None:
            missing.append(aid)
            continue
        payload = dict(payload)
        if not payload.get("final"):
            missing.append(aid)
        metrics[str(aid)] = {"id": aid, "run_id": run_id,
                             "executed_jobs": payload.pop("executed_jobs", []),
                             label: payload}
    return metrics, missing


def safe_write_drain(run_dir, drain: dict) -> None:
    """`drain.json` touches no store, so it is written whatever state Redis is in."""
    from pathlib import Path as _P
    _P(run_dir, "drain.json").write_text(json.dumps(drain, indent=2))
