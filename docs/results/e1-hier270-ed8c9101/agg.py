"""E1' at Hier-270: aggregate by cell, and set the Hier-90 E1' cells (c125ed8c) beside them.

    python3 agg.py > tables.md        (also writes cells_agg.csv)

Inputs, all in this directory:
    runs_wide.csv   evaluation/collect.py over runs/campaign-e1-hier270-ed8c9101 (60 runs)
    supp.csv        supp.py over the same runs (coordinator spread, per-tier counters, arrivals)
and ../e0-e1-c125ed8c/cells_agg.csv for Hier-90 (cell means as published there).
Cells are keyed by run-dir prefix; `.attempt` dirs are excluded (this campaign has none).
Mean ± std over repeats; a column with fewer runs than the cell shows (n=k).
"""
import csv
import statistics as st
from pathlib import Path

HERE = Path(__file__).parent
ENGINES = ("pbft", "snow", "hybrid")
PLANES = ("analytic", "linucb-shared")
LOADS = (("", "burst (20 jobs/s)"), ("-poisson7", "Poisson 7 jobs/s"))

MAIN = [
    ("completion_pct", "completed %", 1),
    ("jobs_executed_twice", "run twice", 0),
    ("jobs_restarted", "jobs restarted", 1),
    ("msgs_dropped", "sends dropped", 0),
    ("l0_selection_p50", "leaf decision p50 (s)", 2),
    ("l1_selection_p50", "coordinator decision p50 (s)", 2),
    ("l1_selection_p95", "coordinator decision p95 (s)", 2),
    ("sched_latency_p95", "arrival→assigned p95 (s)", 2),
    ("wait_p95", "assigned→started p95 (s)", 2),
    ("job_latency_p50", "job latency p50 (s)", 1),
    ("job_latency_p95", "job latency p95 (s)", 1),
    ("makespan_s", "makespan (s)", 0),
    ("throughput_jobs_per_s", "throughput (jobs/s)", 2),
    ("fairness_jain", "Jain, leaves", 3),
    ("coord_jain", "Jain, coordinators", 3),
    ("coords_winning", "coordinators winning", 1),
    ("coord_busiest_share", "busiest coordinator share", 3),
    ("group_busiest_share", "busiest child group share", 3),
    ("consensus_msgs_per_job", "consensus msgs / job", 0),
    ("proposers_per_job_l1", "proposers / job, coordinator tier", 2),
    ("l1_counter_proposals", "counter-proposals, coordinator tier", 0),
    ("l1_counter_margin_yields", "counters held back by margin", 0),
    ("l1_commit_lock_holds", "commit-lock holds, coordinator tier", 0),
    ("l1_commit_splits_resolved", "commit splits resolved, coordinator tier", 0),
    ("arrival_rate_realised", "arrival rate realised (jobs/s)", 3),
]
#: Columns both campaigns export under the same name, for the Hier-90 / Hier-270 table.
VERSUS = [("l1_selection_p50", "coord p50 (s)", 2), ("l1_selection_p95", "coord p95 (s)", 2),
          ("job_latency_p50", "job p50 (s)", 1), ("job_latency_p95", "job p95 (s)", 1),
          ("throughput_jobs_per_s", "jobs/s", 2), ("consensus_msgs_per_job", "cons msgs/job", 0),
          ("msgs_dropped", "dropped", 0), ("restarts_total", "restarts", 0)]


def num(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return None if x != x else x


def load_runs():
    supp = {r["run_dir"]: r for r in csv.DictReader(open(HERE / "supp.csv"))}
    runs = []
    for r in csv.DictReader(open(HERE / "runs_wide.csv")):
        if ".attempt" in r["run_dir"]:
            continue
        r.update(supp.get(r["run_dir"], {}))
        r["_cell"] = r["run_dir"].split("/")[0]
        runs.append(r)
    return runs


def cell_name(engine, plane, load):
    return f"{'e0' if (engine, plane) == ('pbft', 'analytic') else 'e1'}-hier270-{engine}-{plane}{load}"


def aggregate(runs):
    cells = {}
    for r in runs:
        cells.setdefault(r["_cell"], []).append(r)
    out = []
    cols = sorted({m for m, _, _ in MAIN} | {m for m, _, _ in VERSUS})
    for c in sorted(cells):
        rec = {"cell": c, "runs": len(cells[c])}
        for m in cols:
            xs = [x for x in (num(r.get(m)) for r in cells[c]) if x is not None]
            rec[f"{m}_mean"] = st.mean(xs) if xs else float("nan")
            rec[f"{m}_std"] = st.stdev(xs) if len(xs) > 1 else 0.0
            rec[f"{m}_n"] = len(xs)
        out.append(rec)
    return {r["cell"]: r for r in out}


def fmt(rec, m, d):
    if rec is None or not rec.get(f"{m}_n"):
        return "—"
    s = f"{rec[f'{m}_mean']:.{d}f} ± {rec[f'{m}_std']:.{d}f}"
    if rec[f"{m}_n"] != rec["runs"]:
        s += f" (n={rec[f'{m}_n']})"
    return s


def main():
    agg = aggregate(load_runs())
    with open(HERE / "cells_agg.csv", "w", newline="") as f:
        rows = list(agg.values())
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    heads = [f"{e} {'LinUCB' if p != 'analytic' else 'analytic'}" for p in PLANES for e in ENGINES]
    for load, title in LOADS:
        print(f"\n### Hier-270, {title}\n")
        print("| metric | " + " | ".join(heads) + " |")
        print("|---|" + "---|" * len(heads))
        for m, label, d in MAIN:
            vals = [fmt(agg.get(cell_name(e, p, load)), m, d) for p in PLANES for e in ENGINES]
            print(f"| {label} | " + " | ".join(vals) + " |")

    h90 = {r["cell"]: r for r in csv.DictReader(open(HERE.parent / "e0-e1-c125ed8c" / "cells_agg.csv"))}
    for load, title in LOADS:
        l90 = "" if not load else "-poisson3"
        print(f"\n### Hier-90 (c125ed8c, {'burst' if not load else 'Poisson 3 jobs/s'}) "
              f"against Hier-270 ({title}), cell means\n")
        print("| cell | " + " | ".join(f"{lab} 90 → 270" for _, lab, _ in VERSUS) + " |")
        print("|---|" + "---|" * len(VERSUS))
        for p in PLANES:
            for e in ENGINES:
                pre = "e0" if (e, p) == ("pbft", "analytic") else "e1"
                r90 = h90.get(f"{pre}-hier90-{e}-{p}{l90}")
                r270 = agg.get(cell_name(e, p, load))
                vals = []
                for m, _, d in VERSUS:
                    a = num(r90.get(m)) if r90 else None
                    b = r270.get(f"{m}_mean") if r270 and r270.get(f"{m}_n") else None
                    vals.append(f"{a:.{d}f} → {b:.{d}f}" if a is not None and b is not None
                                else f"{'—' if a is None else f'{a:.{d}f}'} → "
                                     f"{'—' if b is None else f'{b:.{d}f}'}")
                print(f"| {e} {'LinUCB' if p != 'analytic' else 'analytic'} | " + " | ".join(vals) + " |")


if __name__ == "__main__":
    main()
