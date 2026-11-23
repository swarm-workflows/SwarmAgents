"""E7 at Hier-270: the four baselines beside the SWARM analytic cells, aggregated by cell.

    python3 agg.py > tables.md        (also writes cells_agg.csv)

Inputs, one evaluation/collect.py invocation per campaign so every number comes from the same
code path:
    runs_wide_e7.csv                          runs/campaign-e7-hier270-ed8c9101 (40 runs, here)
    ../e1-hier270-ed8c9101/runs_wide.csv      the E1' Hier-270 campaign; only its six analytic
                                              cells are used (PBFT, Snow, Hybrid x two loads)
Cells are keyed by run-dir prefix; `.attempt` dirs are excluded (neither campaign has any).
Mean ± std over 5 runs; a column with fewer runs than the cell shows (n=k).
"""
import csv
import statistics as st
from pathlib import Path

HERE = Path(__file__).parent
METRICS = [
    ("completion_pct", "completed %", 1),
    ("jobs_executed_twice", "run twice", 0),
    ("exec_p95", "execution p95 (s)", 2),
    ("sched_latency_p50", "arrival→assigned p50 (s)", 2),
    ("sched_latency_p95", "arrival→assigned p95 (s)", 2),
    ("wait_p95", "assigned→started p95 (s)", 2),
    ("job_latency_p50", "job latency p50 (s)", 1),
    ("job_latency_p95", "job latency p95 (s)", 1),
    ("job_latency_p99", "job latency p99 (s)", 1),
    ("makespan_s", "makespan (s)", 0),
    ("throughput_jobs_per_s", "throughput (jobs/s)", 2),
    ("fairness_jain", "Jain (243 workers)", 3),
    ("active_leaders", "workers that ran a job", 1),
]
#: (column label, E7 cell stem or E1' cell stem)
ARMS = [("SWARM PBFT", "e0-hier270-pbft-analytic"), ("SWARM Snow", "e1-hier270-snow-analytic"),
        ("SWARM Hybrid", "e1-hier270-hybrid-analytic"), ("greedy", "e7-hier270-greedy"),
        ("round-robin", "e7-hier270-round-robin"), ("random", "e7-hier270-random"),
        ("Sparrow", "e7-hier270-sparrow")]
LOADS = (("burst", "burst (20 jobs/s)"), ("poisson7", "Poisson 7 jobs/s"))


def num(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return None if x != x else x


def cell_key(stem, load):
    if stem.startswith("e7-"):
        return f"{stem}-{load}"
    return stem if load == "burst" else f"{stem}-poisson7"


def load_cells():
    cells = {}
    for path in (HERE / "runs_wide_e7.csv", HERE.parent / "e1-hier270-ed8c9101" / "runs_wide.csv"):
        for r in csv.DictReader(open(path)):
            if ".attempt" in r["run_dir"]:
                continue
            cells.setdefault(r["run_dir"].split("/")[0], []).append(r)
    return cells


def main():
    cells = load_cells()
    out = []
    for load, _ in LOADS:
        for label, stem in ARMS:
            key = cell_key(stem, load)
            rows = cells.get(key, [])
            rec = {"cell": key, "arm": label, "load": load, "runs": len(rows)}
            for m, _, _ in METRICS:
                xs = [x for x in (num(r.get(m)) for r in rows) if x is not None]
                rec[f"{m}_mean"] = st.mean(xs) if xs else float("nan")
                rec[f"{m}_std"] = st.stdev(xs) if len(xs) > 1 else 0.0
                rec[f"{m}_n"] = len(xs)
            out.append(rec)
    with open(HERE / "cells_agg.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out[0]))
        w.writeheader()
        w.writerows(out)

    for load, title in LOADS:
        sel = [r for r in out if r["load"] == load]
        print(f"\n### Hier-270, {title}\n")
        print("| metric | " + " | ".join(r["arm"] for r in sel) + " |")
        print("|---|" + "---|" * len(sel))
        for m, label, d in METRICS:
            vals = []
            for r in sel:
                if not r[f"{m}_n"]:
                    vals.append("—")
                    continue
                s = f"{r[f'{m}_mean']:.{d}f} ± {r[f'{m}_std']:.{d}f}"
                if r[f"{m}_n"] != r["runs"]:
                    s += f" (n={r[f'{m}_n']})"
                vals.append(s)
            print(f"| {label} | " + " | ".join(vals) + " |")


if __name__ == "__main__":
    main()
