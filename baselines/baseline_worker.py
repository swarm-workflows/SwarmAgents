#!/usr/bin/env python3.11
"""
baseline_worker.py — execution worker for the centralized baselines (E7).

One per level-0 agent, on the host `run_test.py` would place that agent. The central scheduler
(`baselines/scheduler.py`) decides placement and pushes each assigned job id onto this worker's
run-scoped queue (`baseline:<run>:q:<agent>`); the worker runs up to `runtime.executor_workers`
of them at once — what a SWARM agent's executor runs — through SWARM's own `Job.execute()`
with the base config's wall-time clamp, and writes the outcome back with the same completion
write an agent uses (outputs published only on success, in the same transaction).

Until 2026-10-07 this ran one job at a time while the scheduler admitted several per agent by
capacity, so the excess sat READY and inflated wait and makespan for a reason unrelated to
centralization; it also read a hard-coded wall-time clamp and served unscoped keys.

    python3.11 baselines/baseline_worker.py --agent-id 5 --db-host database --run-id R \\
        --config config_swarm_multi.yml

Runs until the run's shutdown key appears or SIGTERM; on the way out it lets running jobs
finish and writes its stats (`executed_jobs` included) to `baseline:<run>:stats:worker:<id>`.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_SWARM_ROOT = _SCRIPT_DIR.parent
if str(_SWARM_ROOT) not in sys.path:
    sys.path.insert(0, str(_SWARM_ROOT))

import redis  # noqa: E402

from baselines.common import (GROUP, LEVEL, configure_execution, executor_workers,  # noqa: E402
                              run_job, runtime_config)
from baselines.sparrow import Keys  # noqa: E402
from swarm.database.repository import Repository  # noqa: E402
from swarm.models.job import Job, ObjectState  # noqa: E402

logger = logging.getLogger("baseline_worker")

READY = ObjectState.READY.value


class DispatchWorker:
    """Serves one agent's dispatch queue with a pool of `max_concurrent` executors."""

    def __init__(self, agent_id: int, repo: Repository, redis_client, keys: Keys,
                 max_concurrent: int = 10, heartbeat_ttl_s: int = 30):
        self.agent_id = int(agent_id)
        self.repo, self.redis, self.keys = repo, redis_client, keys
        self.max_concurrent = max(1, int(max_concurrent))
        self.heartbeat_ttl_s = int(heartbeat_ttl_s)
        self.pool = ThreadPoolExecutor(max_workers=self.max_concurrent)
        self.executed_jobs: list[str] = []
        self._lock = threading.Lock()
        self.stats = {"dispatches": 0, "noops": 0, "completed": 0, "persist_refused": 0,
                      "claim_refused": 0, "max_concurrent": self.max_concurrent}

    def heartbeat(self) -> None:
        self.redis.set(self.keys.heartbeat(self.agent_id), "1", ex=self.heartbeat_ttl_s)

    def serve_one(self, block_s: float = 1.0) -> str:
        """Take the next dispatched job id. Returns ``empty``, ``noop`` (the record is not a
        READY job assigned here — never run something the scheduler did not give us) or
        ``submitted``. Jobs beyond `max_concurrent` wait in the pool's queue, as in an agent."""
        q = self.keys.queue(self.agent_id)
        item = self.redis.blpop([q], timeout=max(1, int(block_s))) if block_s else \
            self.redis.lpop(q)
        if not item:
            return "empty"
        job_id = item[1] if isinstance(item, (list, tuple)) else item
        self.stats["dispatches"] += 1
        rec = self.repo.get(job_id, key_prefix=Repository.KEY_JOB, level=LEVEL, group=GROUP)
        if not rec or rec.get("state") != READY or str(rec.get("leader_id")) != str(self.agent_id):
            self.stats["noops"] += 1
            return "noop"
        job = Job()
        job.from_dict(rec)
        job.level = LEVEL
        self.pool.submit(self._run, job)
        return "submitted"

    def _run(self, job: Job) -> None:
        try:
            outcome = run_job(self.repo, job, self.agent_id, self.executed_jobs)
        except Exception as exc:
            logger.error("job %s could not be persisted: %s", job.job_id, exc)
            outcome = False
        with self._lock:
            # None: the READY -> RUNNING claim was refused (a duplicate dispatch, or the job
            # already ran) and nothing executed.
            self.stats["claim_refused" if outcome is None else
                       "completed" if outcome else "persist_refused"] += 1

    def run_forever(self, stop: threading.Event, block_s: float = 1.0,
                    stats_every_s: float = 10.0) -> None:
        last_stats = time.monotonic()
        while not stop.is_set():
            self.heartbeat()
            if self.redis.get(self.keys.shutdown):
                logger.info("Shutdown key set; leaving.")
                break
            try:
                self.serve_one(block_s=block_s)
            except redis.RedisError as exc:
                logger.warning("queue read failed: %s", exc)
                stop.wait(1.0)
            if time.monotonic() - last_stats >= stats_every_s:
                self.write_stats()
                last_stats = time.monotonic()

    def shutdown(self) -> None:
        """Let running jobs finish and persist; drop dispatched jobs that never started (they
        stay READY, which is what `drain.json`'s `inflight_at_stop` counts) — the run is over,
        and running them after it would put work past the end the run reports."""
        self.pool.shutdown(wait=True, cancel_futures=True)

    def write_stats(self, final: bool = False) -> None:
        """Periodic snapshots carry ``final: false``; only the one written after the pool has
        drained carries ``final: true`` and counts as this worker's report."""
        with self._lock:
            payload = dict(self.stats)
            payload["executed_jobs"] = list(self.executed_jobs)
            payload["final"] = bool(final)
        self.redis.set(self.keys.stats("worker", self.agent_id), json.dumps(payload))


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Execution worker for the centralized baselines")
    p.add_argument("--agent-id", type=int, required=True, help="This worker's agent ID")
    p.add_argument("--db-host", type=str, required=True, help="Redis host")
    p.add_argument("--db-port", type=int, default=6379, help="Redis port")
    p.add_argument("--run-id", required=True,
                   help="The orchestrator's run id; every key this worker touches is scoped by it")
    p.add_argument("--config", default=str(_SWARM_ROOT / "config_swarm_multi.yml"),
                   help="Base config for runtime.wall_time_* and runtime.executor_workers — the "
                        "one the SWARM arm ran with")
    p.add_argument("--max-concurrent", type=int, default=None,
                   help="Concurrent jobs (default runtime.executor_workers, as SWARM)")
    p.add_argument("--debug", action="store_true", help="Enable DEBUG logging")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s [worker-%(name)s] %(levelname)s: %(message)s",
                        datefmt="%H:%M:%S")
    os.chdir(_SWARM_ROOT)
    os.environ["SWARM_RUN_ID"] = args.run_id

    runtime = runtime_config(args.config)
    configure_execution(runtime)
    client = redis.StrictRedis(host=args.db_host, port=args.db_port, decode_responses=True)
    repo = Repository(client, run_id=args.run_id)
    worker = DispatchWorker(args.agent_id, repo, client, Keys(args.run_id, prefix="baseline"),
                            max_concurrent=args.max_concurrent or executor_workers(runtime))

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    logger.info("Worker %d up: %d concurrent, run %s", args.agent_id, worker.max_concurrent,
                args.run_id)
    try:
        worker.run_forever(stop)
    finally:
        worker.shutdown()
        worker.write_stats(final=True)
    logger.info("Worker %d done: %s", args.agent_id, worker.stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
