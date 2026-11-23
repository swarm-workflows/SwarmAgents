"""First runs at isc27-freeze: the E1' flat-mesh ladder and the E5 ablations, aggregated by cell.

    python3 agg.py > tables.md        (also writes cells_agg.csv)

Inputs:
    runs_wide.csv   evaluation/collect.py over runs/campaign-first-runs-21a2d9bb (48 runs)
    ../e0-e1-c125ed8c/runs_wide.csv       Hier-90 E1' references (code c125ed8c)
    ../e1-hier270-ed8c9101/runs_wide.csv  Hier-270 E1' references (code ed8c9101)
The references are at OLDER code revisions than the E5 arms; the frozen re-run replaces them
with same-revision cells. Cells are keyed by run-dir prefix; `.attempt` dirs are excluded.
Mean ± std over repeats; a column with fewer runs than the cell shows (n=k).
"""
import csv
import statistics as st
from pathlib import Path

HERE = Path(__file__).parent

MESH = [
    ("completion_pct", "completed %", 1),
    ("jobs_executed_twice", "run twice", 0),
    ("sched_latency_p50", "arrival→assigned p50 (s)", 1),
    ("sched_latency_p95", "arrival→assigned p95 (s)", 1),
    ("job_latency_p50", "job latency p50 (s)", 1),
    ("job_latency_p95", "job latency p95 (s)", 1),
    ("makespan_s", "makespan (s)", 0),
    ("throughput_jobs_per_s", "throughput (jobs/s)", 2),
    ("fairness_jain", "Jain, agents", 3),
    ("consensus_msgs_per_job", "consensus msgs / job", 0),
    ("msgs_dropped", "sends dropped", 0),
    ("jobs_restarted", "jobs restarted", 1),
    ("restarts_total", "restarts", 0),
]
HIER = [
    ("completion_pct", "completed %", 1),
    ("jobs_executed_twice", "run twice", 0),
    ("l0_selection_p50", "leaf decision p50 (s)", 2),
    ("l1_selection_p50", "coordinator decision p50 (s)", 2),
    ("l1_selection_p95", "coordinator decision p95 (s)", 2),
    ("sched_latency_p95", "arrival→assigned p95 (s)", 2),
    ("job_latency_p50", "job latency p50 (s)", 1),
    ("job_latency_p95", "job latency p95 (s)", 1),
    ("makespan_s", "makespan (s)", 0),
    ("throughput_jobs_per_s", "throughput (jobs/s)", 2),
    ("fairness_jain", "Jain, leaves", 3),
    ("proposers_per_job_l1", "proposers / job, coordinator tier", 2),
    ("consensus_msgs_per_job", "consensus msgs / job", 0),
    ("msgs_dropped", "sends dropped", 0),
    ("jobs_restarted", "jobs restarted", 1),
    ("restarts_total", "restarts", 0),
]
#: (title, [(column heading, source, cell)]) — source "here" or a reference directory.
E5 = [
    ("Hier-90, PBFT (burst)", [
        ("E1′ reference (c125ed8c)", "e0-e1-c125ed8c", "e0-hier90-pbft-analytic"),
        ("gossip off", "here", "e5-hier90-pbft-gossipoff"),
        ("peers", "here", "e5-hier90-pbft-peers")]),
    ("Hier-90, Snow (burst)", [
        ("E1′ reference (c125ed8c)", "e0-e1-c125ed8c", "e1-hier90-snow-analytic"),
        ("gossip off", "here", "e5-hier90-snow-gossipoff"),
        ("peers", "here", "e5-hier90-snow-peers")]),
    ("Hier-270, peers (burst)", [
        ("PBFT E1′ reference (ed8c9101)", "e1-hier270-ed8c9101", "e0-hier270-pbft-analytic"),
        ("PBFT peers", "here", "e5-hier270-pbft-peers"),
        ("Snow E1′ reference (ed8c9101)", "e1-hier270-ed8c9101", "e1-hier270-snow-analytic"),
        ("Snow peers", "here", "e5-hier270-snow-peers")]),
]


def num(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return None if x != x else x


#: Jobs each cell submits, as declared in campaigns/first-runs-21a2d9bb.yml (`jobs:`). The
#: collector computes completion only against a declared count, and one invocation over this
#: campaign cannot declare five; so completion % is jobs_completed over these, never over the
#: jobs a run happened to see.
DECLARED = {"mesh90": 1800, "mesh120": 2400, "mesh180": 3600, "hier90": 1800, "hier270": 5400}


def declared(cell):
    for key, n in DECLARED.items():
        if f"-{key}-" in cell:
            return n
    raise SystemExit(f"no declared job count for {cell}")


def cells_from(path, fill=False):
    cells = {}
    for r in csv.DictReader(open(path)):
        if ".attempt" in r["run_dir"]:
            continue
        if fill and num(r.get("completion_pct")) is None and num(r.get("jobs_completed")) is not None:
            n = declared(r["run_dir"].split("/")[0])
            r["completion_pct"] = 100.0 * float(r["jobs_completed"]) / n
        cells.setdefault(r["run_dir"].split("/")[0], []).append(r)
    return cells


def agg(runs, cols):
    rec = {"runs": len(runs)}
    for m in cols:
        xs = [x for x in (num(r.get(m)) for r in runs) if x is not None]
        rec[f"{m}_mean"] = st.mean(xs) if xs else float("nan")
        rec[f"{m}_std"] = st.stdev(xs) if len(xs) > 1 else 0.0
        rec[f"{m}_n"] = len(xs)
    return rec


def fmt(rec, m, d):
    if rec is None or not rec.get(f"{m}_n"):
        return "—"
    s = f"{rec[f'{m}_mean']:.{d}f} ± {rec[f'{m}_std']:.{d}f}"
    if rec[f"{m}_n"] != rec["runs"]:
        s += f" (n={rec[f'{m}_n']})"
    return s


def table(title, heads, recs, metrics):
    print(f"\n### {title}\n")
    print("| metric | " + " | ".join(heads) + " |")
    print("|---|" + "---|" * len(heads))
    print("| runs | " + " | ".join(str(r["runs"]) if r else "—" for r in recs) + " |")
    for m, label, d in metrics:
        print(f"| {label} | " + " | ".join(fmt(r, m, d) for r in recs) + " |")


def main():
    cols = sorted({m for m, _, _ in MESH} | {m for m, _, _ in HIER})
    here = cells_from(HERE / "runs_wide.csv", fill=True)
    agg_here = {c: agg(rs, cols) for c, rs in here.items()}
    with open(HERE / "cells_agg.csv", "w", newline="") as f:
        rows = [dict(cell=c, **r) for c, r in sorted(agg_here.items())]
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    print("## E1′ flat mesh (burst, 20 jobs per agent)")
    for size in (90, 120, 180):
        recs = [agg_here.get(f"e1-mesh{size}-{e}-analytic") for e in ("pbft", "snow")]
        table(f"Mesh-{size}", ["PBFT", "Snow"], recs, MESH)

    print("\n## E5 ablations beside their E1′ references (older revisions; see README)")
    refs = {}
    for title, spec in E5:
        recs = []
        for _, src, cell in spec:
            if src == "here":
                recs.append(agg_here.get(cell))
            else:
                refs.setdefault(src, cells_from(HERE.parent / src / "runs_wide.csv"))
                rs = refs[src].get(cell)
                recs.append(agg(rs, cols) if rs else None)
        table(title, [h for h, _, _ in spec], recs, HIER)


if __name__ == "__main__":
    main()
