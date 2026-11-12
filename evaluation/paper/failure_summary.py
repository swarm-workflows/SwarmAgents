#!/usr/bin/env python3
"""One row per run of a campaign out-dir: the injected event and how the scheduler answered it.

    python3.11 evaluation/paper/failure_summary.py runs/frozen-0bceb349/<file> > events_summary.csv

Reads, per run directory `<cell>/runNN`:
- `outage.json` inside the run (group_outage.py, E2b/E7-under-failure): outcome, held_s,
  killed_at/rejoined_at relative to the run start, members;
- `runNN.partition.json` beside it (partition.py, E6): outcome, held_s, applied/healed relative;
- `run_meta.json` (baselines): `scheduler_stats` workers_failed / jobs_requeued /
  workers_readmitted and the first `failure_events` time after the kill.

Every column is blank when its source is absent, never zero: a run with no outage has no outage
outcome, and the reader must not read "0 requeued" into it. Runs in `.attempt<k>` dirs are skipped.
"""
import csv
import json
import sys
from pathlib import Path

COLS = ["cell", "run", "event", "outcome", "held_s", "event_at_s", "end_event_at_s", "members",
        "workers_failed", "jobs_requeued", "workers_readmitted", "first_requeue_after_kill_s"]


def _load(p):
    try:
        return json.loads(Path(p).read_text())
    except (OSError, ValueError):
        return None


def _start(meta):
    if not meta:
        return None
    for k in ("started_at", "start_time", "distribution_started_at"):
        if isinstance(meta.get(k), (int, float)):
            return float(meta[k])
    return None


def row_for(run):
    r = {c: "" for c in COLS}
    r["cell"], r["run"] = run.parent.name, run.name
    meta = _load(run / "run_meta.json") or {}
    t0 = _start(meta)
    rel = (lambda t: "" if t is None or t0 is None else round(float(t) - t0, 1))
    out = _load(run / "outage.json")
    part = _load(run.parent / f"{run.name}.partition.json")
    kill = None
    if out:
        kill = out.get("killed_at")
        r.update(event="outage", outcome=out.get("outcome", ""), held_s=out.get("held_s", ""),
                 event_at_s=rel(kill), end_event_at_s=rel(out.get("rejoined_at")),
                 members=" ".join(str(m) for m in out.get("members", []) or []))
    elif part:
        r.update(event="partition", outcome=part.get("outcome", ""), held_s=part.get("held_s", ""),
                 event_at_s=rel(part.get("applied_at")), end_event_at_s=rel(part.get("healed_at")))
    stats = meta.get("scheduler_stats")
    if isinstance(stats, dict) and stats and all(isinstance(v, dict) or v is None
                                                 for v in stats.values()):
        # Sparrow: one stats dict per scheduler. Each scheduler detects every dead worker itself
        # (counts are per scheduler), but requeues only its own partition's jobs (they add up).
        parts = [v for v in stats.values() if v]
        if len(parts) == len(stats):
            r["workers_failed"] = max(p.get("workers_failed", 0) for p in parts)
            r["workers_readmitted"] = max(p.get("workers_readmitted", 0) for p in parts)
            r["jobs_requeued"] = sum(p.get("jobs_requeued", 0) for p in parts)
    elif isinstance(stats, dict):
        for k in ("workers_failed", "jobs_requeued", "workers_readmitted"):
            r[k] = stats.get(k, 0)
        evs = [e for e in meta.get("failure_events") or [] if e.get("dead")]
        if evs and kill:
            r["first_requeue_after_kill_s"] = round(min(float(e["at"]) for e in evs) - float(kill), 1)
    return r


def main(root):
    w = csv.DictWriter(sys.stdout, fieldnames=COLS)
    w.writeheader()
    for run in sorted(Path(root).glob("*/run[0-9][0-9]")):
        if run.is_dir():
            w.writerow(row_for(run))


if __name__ == "__main__":
    main(sys.argv[1])
