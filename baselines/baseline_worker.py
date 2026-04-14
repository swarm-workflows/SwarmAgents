#!/usr/bin/env python3.11
"""
baseline_worker.py — Lightweight worker process for distributed baseline scheduling.

Runs on each remote VM, polls Redis for READY jobs assigned to this agent
(leader_id == agent_id), executes them, and marks them COMPLETE.

Usage:
    python3.11 baselines/baseline_worker.py \
        --agent-id 5 --db-host 10.0.0.1 \
        [--db-port 6379] [--level 0] [--group 0] \
        [--poll-interval 0.5] [--profiles agent_profiles.json]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path

# Ensure SwarmAgents root is on sys.path
_SCRIPT_DIR = Path(__file__).resolve().parent
_SWARM_ROOT = _SCRIPT_DIR.parent
if str(_SWARM_ROOT) not in sys.path:
    sys.path.insert(0, str(_SWARM_ROOT))

import redis

from swarm.database.repository import Repository
from swarm.models.job import Job, ObjectState

logger = logging.getLogger("baseline_worker")

# Graceful shutdown flag
_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    logger.info("Received signal %d, shutting down …", signum)
    _shutdown = True


def poll_and_execute(
    repo: Repository,
    agent_id: int,
    level: int,
    group: int,
    poll_interval: float,
    redis_client: redis.StrictRedis,
):
    """Main worker loop: poll for READY jobs assigned to this agent, execute them."""
    logger.info("Worker %d started, polling for READY jobs (level=%d, group=%d)", agent_id, level, group)

    # Register heartbeat so orchestrator knows we're alive
    heartbeat_key = f"baseline:worker:{agent_id}:heartbeat"
    redis_client.set(heartbeat_key, "alive", ex=300)

    while not _shutdown:
        # Check for shutdown signal from orchestrator
        if redis_client.exists("baseline:shutdown"):
            logger.info("Shutdown signal detected in Redis. Exiting.")
            break

        # Refresh heartbeat
        redis_client.set(heartbeat_key, "alive", ex=300)

        # Poll for READY jobs
        ready_dicts = repo.get_all_objects(
            key_prefix="job",
            level=level,
            group=group,
            state=ObjectState.READY.value,
        )

        my_jobs = []
        for jd in ready_dicts:
            if isinstance(jd, str):
                jd = json.loads(jd)
            # Filter: only jobs assigned to this agent
            leader = jd.get("leader_id")
            if leader is not None and int(leader) == agent_id:
                job = Job()
                job.from_dict(jd)
                job.level = level
                my_jobs.append(job)

        if not my_jobs:
            time.sleep(poll_interval)
            continue

        for job in my_jobs:
            logger.info("Executing job %s (wall_time=%.1fs)", job.job_id, job.wall_time or 0.0)
            try:
                job.execute()
            except Exception:
                job._exit_status = 1
                job.state = ObjectState.COMPLETE
                job.mark_completed()

            # Save completed job back to Redis
            repo.save(
                obj=job.to_dict(),
                key_prefix="job",
                level=level,
                group=group,
            )
            logger.info("Job %s completed (exit_status=%d)", job.job_id, job.exit_status)

    # Clean up heartbeat
    redis_client.delete(heartbeat_key)
    logger.info("Worker %d shut down.", agent_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Baseline worker for distributed scheduling")
    parser.add_argument("--agent-id", type=int, required=True, help="This worker's agent ID")
    parser.add_argument("--db-host", type=str, required=True, help="Redis host")
    parser.add_argument("--db-port", type=int, default=6379, help="Redis port")
    parser.add_argument("--level", type=int, default=0, help="Hierarchy level (default: 0)")
    parser.add_argument("--group", type=int, default=0, help="Hierarchy group (default: 0)")
    parser.add_argument("--poll-interval", type=float, default=0.5, help="Seconds between polls (default: 0.5)")
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG logging")
    return parser.parse_args()


def main():
    args = parse_args()

    # Logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [worker-%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # Signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    # Change to SwarmAgents root
    os.chdir(_SWARM_ROOT)

    # Connect to Redis
    redis_client = redis.StrictRedis(
        host=args.db_host, port=args.db_port, decode_responses=True
    )
    repo = Repository(redis_client=redis_client)

    logger.info("Baseline worker %d connecting to Redis at %s:%d", args.agent_id, args.db_host, args.db_port)

    poll_and_execute(
        repo=repo,
        agent_id=args.agent_id,
        level=args.level,
        group=args.group,
        poll_interval=args.poll_interval,
        redis_client=redis_client,
    )


if __name__ == "__main__":
    main()
