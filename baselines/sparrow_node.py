#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""One Sparrow-style baseline process: a scheduler or a worker (see baselines/sparrow.py).

Started by baselines/run_sparrow.py — over ssh in a remote run, as a subprocess locally:

    sparrow_node.py worker    --agent-id 5 --db-host database --run-id R
    sparrow_node.py scheduler --index 0 --of 9 --db-host database --run-id R --probe-ratio 2

Both read the fleet from the store (the orchestrator publishes it), apply the same
`runtime.wall_time_*` clamp SWARM agents apply from the same base config, and run until the
run's shutdown key appears or they receive SIGTERM. Stats are written to the store on a cadence
and at exit.
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import redis  # noqa: E402

from baselines.common import configure_execution, executor_workers, runtime_config  # noqa: E402
from baselines.sparrow import Keys, SparrowScheduler, SparrowWorker, load_fleet  # noqa: E402
from swarm.database.repository import Repository  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("role", choices=["worker", "scheduler"])
    ap.add_argument("--db-host", required=True)
    ap.add_argument("--db-port", type=int, default=6379)
    ap.add_argument("--run-id", required=True,
                    help="The orchestrator's run id; every Sparrow key is scoped by it.")
    ap.add_argument("--config", default=str(_ROOT / "config_swarm_multi.yml"),
                    help="Base config for runtime.wall_time_* and runtime.executor_workers.")
    ap.add_argument("--agent-id", type=int, help="worker: which fleet member this is")
    ap.add_argument("--max-concurrent", type=int, default=None,
                    help="worker: concurrent jobs (default runtime.executor_workers, as SWARM)")
    ap.add_argument("--index", type=int, help="scheduler: this scheduler's partition")
    ap.add_argument("--of", type=int, help="scheduler: how many schedulers there are")
    ap.add_argument("--probe-ratio", type=int, default=2)
    ap.add_argument("--reprobe-s", type=float, default=30.0)
    ap.add_argument("--tick-s", type=float, default=0.5)
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [sparrow-%(name)s] %(levelname)s: %(message)s")
    log = logging.getLogger(args.role)

    runtime = runtime_config(args.config)
    configure_execution(runtime)
    client = redis.StrictRedis(host=args.db_host, port=args.db_port, decode_responses=True)
    repo = Repository(client, run_id=args.run_id)
    keys = Keys(args.run_id)
    fleet = load_fleet(client, keys)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    if args.role == "worker":
        if args.agent_id is None:
            ap.error("worker needs --agent-id")
        me = next((a for a in fleet if a.agent_id == args.agent_id), None)
        if me is None:
            log.error("agent %s is not in the published fleet", args.agent_id)
            return 2
        workers = args.max_concurrent or executor_workers(runtime)
        node = SparrowWorker(me, repo, client, keys, max_concurrent=workers)
        log.info("worker %s up: %s concurrent, fleet of %d", me.agent_id, workers, len(fleet))
        try:
            node.run_forever(stop, block_s=1.0)
        finally:
            node.shutdown(wait=True)
            node.write_stats(final=True)
        log.info("worker %s done: %s", me.agent_id, node.stats)
        return 0

    if args.index is None or not args.of:
        ap.error("scheduler needs --index and --of")
    node = SparrowScheduler(args.index, args.of, repo, client, keys, fleet,
                            probe_ratio=args.probe_ratio, reprobe_s=args.reprobe_s,
                            seed=args.seed)
    log.info("scheduler %d/%d up: probe ratio %d, re-probe after %.0fs",
             args.index, args.of, args.probe_ratio, args.reprobe_s)
    last_stats = time.monotonic()
    try:
        while not stop.is_set() and not client.get(keys.shutdown):
            try:
                node.tick()
            except redis.RedisError as exc:
                log.warning("tick failed: %s", exc)
            if time.monotonic() - last_stats >= 10.0:
                node.write_stats()
                last_stats = time.monotonic()
            stop.wait(args.tick_s)
    finally:
        node.write_stats()
    log.info("scheduler %d done: %s", args.index, node.stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
