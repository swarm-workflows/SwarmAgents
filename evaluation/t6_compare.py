#!/usr/bin/env python3
"""T-6: compare SWARM's real-workflow runs against the Pegasus reference, job by job.

Two comparisons, kept apart because they answer different questions:

1. **Execution time, staging excluded on both sides.** Pegasus: kickstart's `remote_duration`
   (the executable only; PegasusLite's own stage-in is outside it). SWARM: the runner's
   `duration_s`, logged as `[EXEC] Job <id> completed in <s>s` — the process only; input
   staging, the private-directory copy and the image pull are all outside it
   (`swarm/execution/runner.py`). `started_at -> completed_at` in `all_jobs.csv` is NOT this:
   it also holds the input fetch, the copy into the job directory and the output handoff, and
   is reported separately as `swarm_in_job_overhead_s`.

   One asymmetry remains: Pegasus runs kickstart INSIDE the container (PegasusLite writes a
   `<job>-cont.sh` that calls `pegasus-kickstart` and runs it with `singularity exec`), so
   `remote_duration` excludes container startup; SWARM's `duration_s` times `apptainer exec`
   and so includes it (~0.17 s per job on the slice, measured 11 Nov 2026 with
   `apptainer exec --containall <sif> true`). It dominates sub-second jobs (tnseq). Pass the
   measured startup as `--container-startup-s` to get `swarm_exec_adj_s` beside the raw value;
   it is never subtracted silently.

2. **End to end, staging included on both sides, with a breakdown.** Pegasus: the workflow's
   makespan, beside its summed HTCondor queue time and stage-in/out transfer times. SWARM: first
   arrival of the workflow's jobs to the last completion, beside the summed in-job overhead and
   eager stage-out (`[STAGE_OUT] ... in <s>s`). A critical path of execution times alone is given
   for both sides, so `makespan - critical path` is each system's non-compute time on the DAG.

In a mixed cell the real jobs queue behind the simulated ones, so SWARM's end-to-end there is
scheduling under load, not workflow performance: take workflow-to-workflow end-to-end numbers
from the standalone pilots.

Refuses rather than approximates: a job with no `[EXEC]` line has no SWARM execution time
(counted in `swarm_exec_missing`), and a critical path that would need one is left empty with the
reason, never computed over the jobs that happen to have one.

Usage (on the database node, stdlib only):
    python3.11 evaluation/t6_compare.py --jobs-dir /export/swarm-wf/mixed_1800_wf \\
        --run runs/campaign-t6-mixed-hier90/e1-hier90-pbft-mixed/run01 [--run ...] --out OUT
"""
import argparse
import csv
import glob
import json
import os
import re
import statistics
import sys
from collections import defaultdict

EXEC_RE = re.compile(
    r"^(?P<ts>\S+ \S+) - \S+ - \w+ - \[EXEC\] Job (?P<job>\S+) "
    r"(?:completed in (?P<ok>[0-9.]+)s|exited (?P<rc>-?\d+) after (?P<bad>[0-9.]+)s)")
STAGE_OUT_RE = re.compile(
    r"^(?P<ts>\S+ \S+) - \S+ - \w+ - \[STAGE_OUT\] (?P<name>\S+) \((?P<bytes>\d+) bytes\) "
    r"to \S+ in (?P<s>[0-9.]+)s")


def _load_jobs(jobs_dir):
    """Real jobs (those carrying an `execution` block) and the Pegasus baseline."""
    jobs = {}
    for path in glob.glob(os.path.join(jobs_dir, "job_*.json")):
        with open(path) as fh:
            rec = json.load(fh)
        for j in (rec if isinstance(rec, list) else [rec]):
            if j.get("execution"):
                jobs[str(j["id"])] = j
    base_path = os.path.join(jobs_dir, "pegasus_baseline.json")
    if not os.path.isfile(base_path):
        sys.exit(f"refusing: {base_path} not found; it is the Pegasus side of the comparison")
    with open(base_path) as fh:
        baseline = json.load(fh)
    return jobs, baseline.get("runs", {})


def _edges(jobs):
    """Producer -> consumer edges inside one workflow, from the gating predicate (else data_in)."""
    producer = {}
    for jid, j in jobs.items():
        for d in j.get("data_out") or []:
            producer[(j.get("workflow"), d.get("file"))] = jid
    parents = defaultdict(set)
    for jid, j in jobs.items():
        pred = j.get("data_predicate") or {}
        files = pred.get("files") or [d.get("file") for d in j.get("data_in") or []]
        for f in files:
            p = producer.get((j.get("workflow"), f))
            if p and p != jid:
                parents[jid].add(p)
    return parents


def _critical_path(ids, parents, weight):
    """Longest path by node weight over `ids`; None (with the missing ids) if any weight is."""
    missing = sorted(i for i in ids if weight.get(i) is None)
    if missing:
        return None, missing
    memo = {}

    def finish(i, stack=()):
        if i in memo:
            return memo[i]
        if i in stack:
            raise ValueError(f"cycle through {i}")
        best = max((finish(p, stack + (i,)) for p in parents.get(i, ()) if p in ids), default=0.0)
        memo[i] = best + weight[i]
        return memo[i]

    return max((finish(i) for i in ids), default=0.0), []


def _parse_logs(run_dir):
    """Last `[EXEC]` outcome per job (with attempt count) and stage-out seconds per file name."""
    exec_by_job = {}
    attempts = defaultdict(int)
    stage_out = defaultdict(float)
    for path in glob.glob(os.path.join(run_dir, "agent-*", "*.log")) + \
            glob.glob(os.path.join(run_dir, "agent-*.log")):
        with open(path, errors="replace") as fh:
            for line in fh:
                if "[EXEC] Job " in line:
                    m = EXEC_RE.match(line)
                    if not m:
                        continue
                    job = m["job"]
                    attempts[job] += 1
                    secs = float(m["ok"] if m["ok"] is not None else m["bad"])
                    rc = 0 if m["ok"] is not None else int(m["rc"])
                    prev = exec_by_job.get(job)
                    if prev is None or m["ts"] >= prev[0]:
                        exec_by_job[job] = (m["ts"], secs, rc)
                elif "[STAGE_OUT] " in line:
                    m = STAGE_OUT_RE.match(line)
                    if m:
                        stage_out[m["name"]] += float(m["s"])
    return ({j: (v[1], v[2]) for j, v in exec_by_job.items()}, attempts, stage_out)


def _parse_all_jobs(run_dir, wanted):
    """Arrival (earliest submission at any tier) and the executing row's started/completed."""
    out = {}
    with open(os.path.join(run_dir, "all_jobs.csv")) as fh:
        for r in csv.DictReader(fh):
            jid = r["job_id"]
            if jid not in wanted:
                continue
            rec = out.setdefault(jid, {"arrival": None, "started": None, "completed": None,
                                       "exit_status": None})
            sub = float(r["submitted_at"] or 0)
            if sub > 0 and (rec["arrival"] is None or sub < rec["arrival"]):
                rec["arrival"] = sub
            st, co = float(r["started_at"] or 0), float(r["completed_at"] or 0)
            if st > 0 and co > 0:
                rec["started"], rec["completed"] = st, co
                rec["exit_status"] = int(float(r["exit_status"]))
    return out


def _r(x, n=3):
    return None if x is None else round(x, n)


def compare_run(run_dir, jobs, peg_runs, parents, startup_s=None):
    exec_by_job, attempts, stage_out = _parse_logs(run_dir)
    timing = _parse_all_jobs(run_dir, set(jobs))
    # cell/run: run names repeat across a campaign's cells.
    run = "/".join(os.path.normpath(run_dir).split(os.sep)[-2:])
    rows, by_wf = [], defaultdict(list)
    for jid, j in sorted(jobs.items()):
        wf = j.get("workflow")
        peg = peg_runs.get(wf, {}).get("jobs", {}).get(jid, {})
        t = timing.get(jid, {})
        sw_exec, sw_rc = exec_by_job.get(jid, (None, None))
        span = (t["completed"] - t["started"]) if t.get("started") else None
        outs = [d.get("file") for d in j.get("data_out") or []]
        so = [stage_out[f] for f in outs if f in stage_out]
        row = {
            "run": run, "workflow": wf, "job_id": jid,
            "transformation": (j.get("execution") or {}).get("transformation"),
            "peg_exec_s": peg.get("remote_duration_sec"),
            "swarm_exec_s": _r(sw_exec),
            "exec_ratio": _r(sw_exec / peg["remote_duration_sec"], 3)
            if sw_exec is not None and peg.get("remote_duration_sec") else None,
            "swarm_exec_adj_s": _r(max(sw_exec - startup_s, 0.0))
            if sw_exec is not None and startup_s is not None
            and (j.get("execution") or {}).get("container") else
            (_r(sw_exec) if startup_s is not None else None),
            "exec_ratio_adj": None,
            "swarm_exec_attempts": attempts.get(jid, 0),
            "swarm_exit": sw_rc if sw_rc is not None else t.get("exit_status"),
            "swarm_span_s": _r(span),
            "swarm_in_job_overhead_s": _r(span - sw_exec) if span is not None and sw_exec is not None else None,
            "swarm_stage_out_s": _r(sum(so)) if so else (0.0 if not outs else None),
            "swarm_wait_s": _r(t["started"] - t["arrival"]) if t.get("started") and t.get("arrival") else None,
            "peg_queue_s": peg.get("queue_time_sec"),
            "peg_stage_in_s": (peg.get("stage_in") or {}).get("duration_sec"),
            "peg_stage_out_s": (peg.get("stage_out") or {}).get("duration_sec"),
            "_arrival": t.get("arrival"), "_completed": t.get("completed"),
        }
        if row["swarm_exec_adj_s"] is not None and row["peg_exec_s"]:
            row["exec_ratio_adj"] = _r(row["swarm_exec_adj_s"] / row["peg_exec_s"])
        rows.append(row)
        by_wf[wf].append(row)

    summaries = []
    for wf, wrows in sorted(by_wf.items()):
        ids = {r["job_id"] for r in wrows}
        peg_w = {r["job_id"]: r["peg_exec_s"] for r in wrows}
        sw_w = {r["job_id"]: r["swarm_exec_s"] for r in wrows}
        peg_cp, peg_missing = _critical_path(ids, parents, peg_w)
        sw_cp, sw_missing = _critical_path(ids, parents, sw_w)
        arrivals = [r["_arrival"] for r in wrows if r["_arrival"]]
        comps = [r["_completed"] for r in wrows if r["_completed"]]
        sw_e2e = (max(comps) - min(arrivals)) if len(comps) == len(wrows) and arrivals else None
        peg_e2e = peg_runs.get(wf, {}).get("makespan_sec")
        ratios = [r["exec_ratio"] for r in wrows if r["exec_ratio"] is not None]
        ratios_adj = [r["exec_ratio_adj"] for r in wrows if r["exec_ratio_adj"] is not None]

        def tot(key):
            vals = [r[key] for r in wrows]
            return None if any(v is None for v in vals) else _r(sum(vals))

        summaries.append({
            "run": run, "workflow": wf, "jobs": len(wrows),
            "swarm_exit0": sum(1 for r in wrows if r["swarm_exit"] == 0),
            "swarm_exec_missing": len(sw_missing),
            "swarm_rerun_jobs": sum(1 for r in wrows if r["swarm_exec_attempts"] > 1),
            "peg_exec_sum_s": tot("peg_exec_s"), "swarm_exec_sum_s": tot("swarm_exec_s"),
            "exec_ratio_median": _r(statistics.median(ratios)) if ratios else None,
            "swarm_exec_adj_sum_s": tot("swarm_exec_adj_s") if startup_s is not None else None,
            "exec_ratio_adj_median": _r(statistics.median(ratios_adj)) if ratios_adj else None,
            "peg_exec_critical_path_s": _r(peg_cp), "swarm_exec_critical_path_s": _r(sw_cp),
            "critical_path_refused": "; ".join(
                f"{side}: no exec time for {len(m)} job(s), e.g. {m[0]}"
                for side, m in (("pegasus", peg_missing), ("swarm", sw_missing)) if m) or None,
            "peg_end_to_end_s": peg_e2e, "swarm_end_to_end_s": _r(sw_e2e),
            "peg_non_compute_s": _r(peg_e2e - peg_cp) if peg_e2e is not None and peg_cp is not None else None,
            "swarm_non_compute_s": _r(sw_e2e - sw_cp) if sw_e2e is not None and sw_cp is not None else None,
            "peg_queue_sum_s": tot("peg_queue_s"), "peg_stage_in_sum_s": tot("peg_stage_in_s"),
            "peg_stage_out_sum_s": tot("peg_stage_out_s"),
            "swarm_in_job_overhead_sum_s": tot("swarm_in_job_overhead_s"),
            "swarm_stage_out_sum_s": tot("swarm_stage_out_s"),
            "swarm_wait_sum_s": tot("swarm_wait_s"),
        })
    for r in rows:
        r.pop("_arrival"), r.pop("_completed")
    return rows, summaries


def _write(path, rows):
    if not rows:
        return
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--jobs-dir", required=True,
                    help="the set the runs used (job_*.json + pegasus_baseline.json)")
    ap.add_argument("--run", action="append", required=True, help="a run directory; repeatable")
    ap.add_argument("--out", required=True, help="output directory")
    ap.add_argument("--container-startup-s", type=float, default=None,
                    help="measured `apptainer exec <sif> true` seconds; adds swarm_exec_adj_s "
                         "(raw values are always kept)")
    a = ap.parse_args(argv)
    jobs, peg_runs = _load_jobs(a.jobs_dir)
    if not jobs:
        sys.exit(f"refusing: no real jobs (with an execution block) in {a.jobs_dir}")
    parents = _edges(jobs)
    os.makedirs(a.out, exist_ok=True)
    all_rows, all_sum = [], []
    for run_dir in a.run:
        rows, sums = compare_run(run_dir, jobs, peg_runs, parents, a.container_startup_s)
        all_rows += rows
        all_sum += sums
    _write(os.path.join(a.out, "t6_per_job.csv"), all_rows)
    _write(os.path.join(a.out, "t6_per_workflow.csv"), all_sum)
    cols = ["run", "workflow", "jobs", "swarm_exit0", "swarm_exec_missing",
            "peg_exec_critical_path_s", "swarm_exec_critical_path_s", "exec_ratio_median",
            "exec_ratio_adj_median",
            "peg_end_to_end_s", "swarm_end_to_end_s"]
    print("\t".join(cols))
    for s in all_sum:
        print("\t".join("" if s[c] is None else str(s[c]) for c in cols))
    for s in all_sum:
        if s["critical_path_refused"]:
            print(f"[refused] {s['run']} {s['workflow']}: {s['critical_path_refused']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
