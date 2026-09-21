#!/usr/bin/env python3
"""Run a staging site: the durable home for a run's intermediate files.

An ordinary agent serves only what it produced and takes no uploads. This server takes both —
it accepts `Put` into a store directory and serves anything in it — which is what lets an
output outlive the agent that made it (`docs/STAGING_DESIGN.md` §6).

Run it on a node every agent can reach, typically the database node, then point the fleet at
it with `runtime.execution.staging.store_host`/`store_port`.

    python3 staging_site.py --store-dir /export/swarm-wf/store --port 21000

The run id must match the fleet's `SWARM_RUN_ID`, or every upload is refused: a store carrying
one run's files must not answer for another's name of the same spelling.
"""
import argparse
import logging
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from swarm.execution import staging  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--store-dir", required=True, help="where uploaded files are kept")
    ap.add_argument("--host", default="0.0.0.0", help="bind address (default: all interfaces)")
    ap.add_argument("--port", type=int, default=21000)
    ap.add_argument("--run-id", default=os.environ.get("SWARM_RUN_ID", ""),
                    help="refuse traffic for any other run (default: $SWARM_RUN_ID)")
    ap.add_argument("--chunk-bytes", type=int, default=staging.DEFAULT_CHUNK_BYTES)
    ap.add_argument("--no-verify", action="store_true",
                    help="skip the stream digest (not recommended: a truncated upload then "
                         "becomes a short file that a job reads happily)")
    ap.add_argument("--stats-every", type=float, default=30.0,
                    help="seconds between stats lines; 0 to print none")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    staging.configure(enabled=True, chunk_bytes=args.chunk_bytes, verify=not args.no_verify)

    os.makedirs(args.store_dir, exist_ok=True)
    published = staging.PublishedFiles()
    # Anything already in the store is servable at startup, so a restarted site does not lose
    # what it is holding — which would defeat the point of it being the durable copy.
    existing = {n: os.path.join(args.store_dir, n)
                for n in os.listdir(args.store_dir)
                if os.path.isfile(os.path.join(args.store_dir, n)) and not n.startswith(".")}
    published.publish_all(existing)

    server = staging.TransferServer(published, args.host, args.port, run_id=args.run_id,
                                    max_workers=16, store_dir=args.store_dir)
    server.start()
    logging.info("staging site on %s:%d, store=%s, run_id=%s, %d file(s) already held",
                 args.host, args.port, args.store_dir, args.run_id or "(any)", len(existing))

    stop = {"now": False}

    def _stop(_signum, _frame):
        stop["now"] = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    last = time.time()
    while not stop["now"]:
        time.sleep(0.5)
        if args.stats_every and time.time() - last >= args.stats_every:
            logging.info("[STAGE_SITE] %s", server.stats())
            last = time.time()
    logging.info("[STAGE_SITE] final %s", server.stats())
    server.stop(2.0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
