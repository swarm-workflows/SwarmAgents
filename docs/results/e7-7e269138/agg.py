"""E7 at Hier-90: aggregate the four baselines and the SWARM PBFT × analytic cells by cell.

Input is evaluation/collect.py output, one invocation per group so every number comes from the
same code path:
    <collect>/e7/runs_wide.csv              runs/campaign-e7-7e269138 (40 runs)
    <collect>/swarm-burst/runs_wide.csv     c125ed8c e0-hier90-pbft-analytic (--label cell=...)
    <collect>/swarm-poisson3/runs_wide.csv  c125ed8c e0-hier90-pbft-analytic-poisson3
    <collect>/swarm-{snow,hybrid}-{burst,poisson3}/runs_wide.csv
                                            c125ed8c e1-hier90-{snow,hybrid}-analytic{,-poisson3}

    python3.11 agg.py /root/collect-e7-7e269138 > tables.md   (also writes cells_agg.csv)

Cells are keyed by run-dir prefix (E7) or the `cell` label (SWARM); `.attempt` dirs are
excluded (E7 has none). Mean ± std over repeats; `_n` is the number of runs with the metric.
"""
import csv
import statistics as st
import sys
from pathlib import Path

METRICS = [
    ("completion_pct", "completed %", 1),
    ("jobs_executed_twice", "run twice", 0),
    ("sched_latency_p50", "arrival→assigned p50 (s)", 2),
    ("sched_latency_p95", "arrival→assigned p95 (s)", 2),
    ("wait_p50", "assigned→started p50 (s)", 2),
    ("wait_p95", "assigned→started p95 (s)", 2),
    ("job_latency_p50", "job latency p50 (s)", 1),
    ("job_latency_p95", "job latency p95 (s)", 1),
    ("job_latency_p99", "job latency p99 (s)", 1),
    ("makespan_s", "makespan (s)", 0),
    ("throughput_jobs_per_s", "throughput (jobs/s)", 2),
    ("fairness_jain", "Jain (fleet)", 3),
    ("active_leaders", "agents that ran a job", 1),
]
ORDER = ["swarm-pbft", "swarm-snow", "swarm-hybrid", "greedy", "round-robin", "random", "sparrow"]


def cell_of(row):
    if row.get("cell"):
        return row["cell"]
    return row["run_dir"].split("/")[0].replace("e7-hier90-", "")


def load(root):
    rows = []
    subs = ["e7", "swarm-burst", "swarm-poisson3"] + [
        f"swarm-{e}-{l}" for e in ("snow", "hybrid") for l in ("burst", "poisson3")]
    for sub in subs:
        with open(Path(root) / sub / "runs_wide.csv") as f:
            for r in csv.DictReader(f):
                if ".attempt" in r["run_dir"]:
                    continue
                r["_cell"] = cell_of(r)
                rows.append(r)
    return rows


def num(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return None if x != x else x


def main(root):
    rows = load(root)
    cells = {}
    for r in rows:
        cells.setdefault(r["_cell"], []).append(r)

    def key(c):
        load_ = "burst" if c.endswith("burst") else "poisson3"
        arm = c.rsplit("-", 1)[0]
        return (load_ != "burst", ORDER.index(arm) if arm in ORDER else 99)

    out = []
    for c in sorted(cells, key=key):
        rec = {"cell": c, "runs": len(cells[c])}
        for m, _, _ in METRICS:
            xs = [x for x in (num(r.get(m)) for r in cells[c]) if x is not None]
            rec[f"{m}_mean"] = st.mean(xs) if xs else float("nan")
            rec[f"{m}_std"] = st.stdev(xs) if len(xs) > 1 else 0.0
            rec[f"{m}_n"] = len(xs)
        out.append(rec)

    with open(Path(__file__).with_name("cells_agg.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out[0]))
        w.writeheader()
        w.writerows(out)

    for load_ in ("burst", "poisson3"):
        sel = [r for r in out if r["cell"].endswith(load_)]
        print(f"\n### {load_}\n")
        print("| metric | " + " | ".join(r["cell"].rsplit("-", 1)[0] for r in sel) + " |")
        print("|---|" + "---|" * len(sel))
        for m, label, d in METRICS:
            cellv = []
            for r in sel:
                mu, sd, n = r[f"{m}_mean"], r[f"{m}_std"], r[f"{m}_n"]
                s = f"{mu:.{d}f} ± {sd:.{d}f}" if n else "—"
                if n != r["runs"]:
                    s += f" (n={n})"
                cellv.append(s)
            print(f"| {label} | " + " | ".join(cellv) + " |")


if __name__ == "__main__":
    main(sys.argv[1])
