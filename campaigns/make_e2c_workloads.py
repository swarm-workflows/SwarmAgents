#!/usr/bin/env python3
"""Build E2c's inputs: a compressed work-day arrival schedule and a job-mix-shift workload.

E2c asks how the delegation policies adapt to changing load and changing job mix (conference plan,
E2c). Two inputs, one per cell type:

1. ``schedule`` — a piecewise-constant Poisson rate for `run_test.py --arrival poisson
   --arrival-schedule <file>`, shaped as a compressed work day: a low overnight level, a morning
   ramp, a peak a little above capacity (so a backlog forms and must clear), a lunch dip, an
   afternoon peak and a decline. The shape is STYLIZED, not taken from a trace: no submission
   timestamps from Pegasus or FABRIC logs were available when this was written. Say so beside any
   E2c number, or replace `SHAPE` with a measured hourly profile. Rates are fractions of
   `--drain`, the Hier-90 G=3 fleet's measured drain rate. The plan sized this against 4.2 jobs/s,
   measured under the capacity cost model. Under the utilization cost model the E2 topology drains
   6.1–7.3 jobs/s at the burst (c125ed8c: PBFT 7.2–7.3, Snow 6.1–6.6, Hybrid 6.5), so the default is
   6.5. The script prints the number of jobs the schedule needs; build at least that many with
   duplicate_jobs.py.

2. ``mixshift`` — a job directory whose types change partway through. Jobs arrive in job-number
   order (`job_distributor.py` sorts on it), so the shift is in the ORDER of the files, not in the
   arrival schedule. The first `--shift-at` fraction of jobs is RAM-bound, the rest is every
   remaining job, which is mostly CPU-bound. With the E2 failure profile
   (`make_e2_profile.py`), that moves the best group for most jobs from each coordinator's
   cpu-weak group to its ram-weak group mid-run.

Usage:
  python campaigns/make_e2c_workloads.py schedule campaigns/e2c_workday.json [--drain 6.5]
  python campaigns/make_e2c_workloads.py mixshift /root/workloads/jobs_1800 /root/workloads/jobs_mixshift_1800
"""
import argparse
import json
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

#: (duration_s, fraction of the drain rate). About 30 minutes of arrivals in total.
SHAPE = [
    (300, 0.30),   # overnight
    (180, 0.60),   # morning ramp
    (120, 0.90),
    (240, 1.20),   # morning peak, above capacity: a backlog forms
    (180, 0.60),   # lunch dip: the backlog clears
    (240, 1.15),   # afternoon peak
    (240, 0.60),   # decline
    (300, 0.30),   # evening
]


def schedule(drain: float) -> tuple[list, int]:
    out, t, jobs = [], 0.0, 0.0
    for dur, frac in SHAPE:
        rate = round(frac * drain, 3)
        out.append([round(t, 1), rate])
        t += dur
        jobs += rate * dur
    return out, int(round(jobs))


def mixshift(src: str, dst: str, shift_at: float) -> None:
    from swarm.models.job import Job
    files = sorted((f for f in os.listdir(src) if f.startswith("job_") and f.endswith(".json")),
                   key=lambda f: int(f[4:-5]))
    jobs = []
    for f in files:
        d = json.load(open(os.path.join(src, f)))
        j = Job(); j.from_dict(dict(d))
        jobs.append((j.job_type or "", d))
    ram = [d for t, d in jobs if t.startswith("ram_bound")]
    rest = [d for t, d in jobs if not t.startswith("ram_bound")]
    n_first = int(round(shift_at * len(jobs)))
    if n_first > len(ram):
        raise SystemExit(f"only {len(ram)} ram_bound jobs, cannot fill the first {n_first}")
    ordered = ram[:n_first] + rest + ram[n_first:]
    rest_after = ordered[n_first:]
    cpu_after = sum(1 for d in rest_after if _type(d).startswith("cpu_bound"))
    if os.path.exists(dst):
        raise SystemExit(f"{dst} exists; refusing to overwrite a workload directory")
    os.makedirs(dst)
    for i, d in enumerate(ordered, start=1):
        d = dict(d); d["id"] = str(i)
        json.dump(d, open(os.path.join(dst, f"job_{i}.json"), "w"), indent=2)
    meta = {"source": os.path.abspath(src), "jobs": len(ordered), "shift_at_job": n_first + 1,
            "first_part": "ram_bound only", "second_part_cpu_share": round(cpu_after / max(1, len(rest_after)), 3)}
    json.dump(meta, open(os.path.join(dst, "mixshift_summary.json"), "w"), indent=2)
    print(json.dumps(meta))


def _type(d: dict) -> str:
    from swarm.models.job import Job
    j = Job(); j.from_dict(dict(d))
    return j.job_type or ""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("schedule"); s.add_argument("out"); s.add_argument("--drain", type=float, default=6.5)
    m = sub.add_parser("mixshift"); m.add_argument("src"); m.add_argument("dst")
    m.add_argument("--shift-at", type=float, default=0.5)
    a = ap.parse_args()
    if a.cmd == "schedule":
        sched, n = schedule(a.drain)
        json.dump(sched, open(a.out, "w"))
        print(f"wrote {a.out}: {len(sched)} segments, {sum(d for d, _ in SHAPE)} s of arrivals, "
              f"peak {max(r for _, r in sched)} jobs/s, needs about {n} jobs")
    else:
        mixshift(a.src, a.dst, a.shift_at)
    return 0


if __name__ == "__main__":
    sys.exit(main())
