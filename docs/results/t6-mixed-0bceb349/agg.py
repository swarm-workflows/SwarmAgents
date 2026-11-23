"""T-6 mixed Hier-90 at isc27-freeze 0bceb349, aggregated by cell.

    python3 agg.py > tables.md        (also writes cells_agg.csv)

Inputs:
    runs_wide.csv          evaluation/collect.py over runs/campaign-t6-mixed-hier90 (6 runs)
    t6_per_workflow.csv    evaluation/t6_compare.py over the same 6 runs (--container-startup-s 0.17)
    tnpilot_t6_per_workflow.csv   the same over the two standalone tnseq pilot runs
    ../e0-e1-c125ed8c/runs_wide.csv   the simulated-only Hier-90 analytic burst cells (OLDER code,
                                      1800 jobs): a reference for scale, not a same-revision baseline
Mean ± std over repeats.
"""
import csv
import statistics as st
from pathlib import Path

HERE = Path(__file__).parent

COLS = [
    ("completion_pct", "completed %", 1),
    ("jobs_executed_twice", "run twice", 0),
    ("l0_selection_p50", "leaf decision p50 (s)", 2),
    ("l1_selection_p50", "coordinator decision p50 (s)", 2),
    ("l1_selection_p95", "coordinator decision p95 (s)", 2),
    ("sched_latency_p95", "arrival→assigned p95 (s)", 1),
    ("job_latency_p50", "job latency p50 (s)", 1),
    ("job_latency_p95", "job latency p95 (s)", 1),
    ("makespan_s", "makespan (s)", 0),
    ("throughput_jobs_per_s", "throughput (jobs/s)", 2),
    ("fairness_jain", "Jain, agents", 3),
    ("consensus_msgs_per_job", "consensus msgs / job", 0),
    ("msgs_dropped", "sends dropped", 0),
    ("jobs_restarted", "jobs restarted", 0),
    ("duplicate_leader_refusals", "duplicate-leader refusals (guard)", 0),
]

CELLS = [
    ("PBFT mixed", HERE / "runs_wide.csv", "e1-hier90-pbft-mixed/"),
    ("Snow mixed", HERE / "runs_wide.csv", "e1-hier90-snow-mixed/"),
    ("PBFT sim-only (c125ed8c)", HERE / "../e0-e1-c125ed8c/runs_wide.csv", "e0-hier90-pbft-analytic/"),
    ("Snow sim-only (c125ed8c)", HERE / "../e0-e1-c125ed8c/runs_wide.csv", "e1-hier90-snow-analytic/"),
]


def _rows(path, prefix):
    with open(path) as fh:
        return [r for r in csv.DictReader(fh)
                if r.get("run_dir", "").startswith(prefix) and ".attempt" not in r["run_dir"]]


def _fmt(vals, nd):
    vals = [float(v) for v in vals if v not in ("", None)]
    if not vals:
        return "–"
    m = st.mean(vals)
    s = st.stdev(vals) if len(vals) > 1 else 0.0
    return f"{m:.{nd}f}" if s == 0 else f"{m:.{nd}f} ± {s:.{nd}f}"


def main():
    agg = []
    print("## Run-level metrics, Hier-90 analytic, burst\n")
    print("| | " + " | ".join(c[0] for c in CELLS) + " |")
    print("|---" * (len(CELLS) + 1) + "|")
    cell_rows = {name: _rows(path, prefix) for name, path, prefix in CELLS}
    print("| runs | " + " | ".join(str(len(cell_rows[c[0]])) for c in CELLS) + " |")
    for key, label, nd in COLS:
        cells = []
        for name, _, _ in CELLS:
            vals = [r.get(key) for r in cell_rows[name]]
            cells.append(_fmt(vals, nd))
            fv = [float(v) for v in vals if v not in ("", None)]
            agg.append({"cell": name, "metric": key, "n": len(fv),
                        "mean": st.mean(fv) if fv else "",
                        "std": st.stdev(fv) if len(fv) > 1 else ""})
        print(f"| {label} | " + " | ".join(cells) + " |")
    with open(HERE / "cells_agg.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["cell", "metric", "n", "mean", "std"])
        w.writeheader()
        w.writerows(agg)

    for title, fname in (("Real workflows inside the mixed cells (6 runs)", "t6_per_workflow.csv"),
                         ("tnseq standalone pilot, Mesh-30 Snow (2 runs)", "tnpilot_t6_per_workflow.csv")):
        with open(HERE / fname) as fh:
            rows = list(csv.DictReader(fh))
        print(f"\n## {title}\n")
        print("Execution time excludes staging on both sides; `adj` subtracts 0.17 s apptainer "
              "startup per containerised SWARM job (Pegasus runs kickstart inside the container).\n")
        print("| workflow | jobs exit 0 | Pegasus exec critical path (s) | SWARM exec critical path (s) "
              "| median exec ratio raw | adj | Pegasus end to end (s) | SWARM end to end (s) |")
        print("|---|---|---|---|---|---|---|---|")
        for wf in sorted({r["workflow"] for r in rows}):
            w = [r for r in rows if r["workflow"] == wf]
            ok = sum(int(r["swarm_exit0"]) for r in w)
            n = sum(int(r["jobs"]) for r in w)
            print(f"| {wf} | {ok}/{n} | {w[0]['peg_exec_critical_path_s']} "
                  f"| {_fmt([r['swarm_exec_critical_path_s'] for r in w], 1)} "
                  f"| {_fmt([r['exec_ratio_median'] for r in w], 2)} "
                  f"| {_fmt([r['exec_ratio_adj_median'] for r in w], 2)} "
                  f"| {w[0]['peg_end_to_end_s']} | {_fmt([r['swarm_end_to_end_s'] for r in w], 0)} |")


if __name__ == "__main__":
    main()
