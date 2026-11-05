"""E2 time-resolved delegation quality, per run (E2a flip, E2b outage, E2c shift and work day).

    PYTHONPATH=/root/SwarmAgents python3.11 timecourse.py <campaign-out-dir> <out-dir>

Scores every clean run with evaluation/oracle.py (the same scoring collect.py's regret columns
use) and writes:

* timecourse.csv -- per run, 30 s bins from the run's first decision: decisions, decisions with a
  choice, routing accuracy and mean regret over the decisions with a choice.
* events.csv     -- per run, the cell's event and what happened around it:
    E2a      flip = median leaf failure-clock start + the profile's after_s (each leaf flips on
             its own clock; the median is the event marker only, scoring stays per member).
    E2c-mix  shift = first decision for a job numbered >= 901 (jobs arrive in number order).
    both:    accuracy/regret over the 120 s before the event and the 300 s after it, and
             recover_s = first time after the event at which accuracy over the last 50
             post-event decisions with a choice is back to >= 0.9 x the pre-event accuracy
             (absent if never). Its floor is the time the first 50 such decisions take.
    E2b      the outage's group and its coordinator: that coordinator's delegations to the group
             before / while down / after, first delegation to it after the rejoin, and regret
             share scored (a decision naming a killed group cannot be priced).
* workday.csv    -- E2c work-day runs: accuracy/regret per arrival segment of e2c_workday.json,
  each decision assigned to the segment its job was planned in (arrivals.json).

`.attempt<k>` directories are skipped: they are failed attempts the campaign re-ran.
"""
import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

from evaluation.oracle import OracleError, load_run, score_run

BIN_S = 30.0
PRE_S, POST_S, ROLL_N, RECOVER_FRAC = 120.0, 300.0, 50, 0.9
MIX_SHIFT_JOB = 901


def acc_regret(rows):
    rows = [r for r in rows if not r["no_choice"]]
    if not rows:
        return None, None, 0
    return (sum(r["optimal"] for r in rows) / len(rows),
            sum(r["regret"] for r in rows) / len(rows), len(rows))


def recover_s(rows, t_event, pre_acc):
    # The window holds post-event decisions only: a window straddling the event "recovers" at
    # once on the pre-event decisions still in it.
    if pre_acc is None:
        return None
    window = []
    for r in rows:
        if r["no_choice"] or float(r["ts"]) < t_event:
            continue
        window.append(r)
        window = window[-ROLL_N:]
        if len(window) == ROLL_N:
            if sum(w["optimal"] for w in window) / ROLL_N >= RECOVER_FRAC * pre_acc:
                return float(r["ts"]) - t_event
    return None


def around(rows, t_event, cell, run, kind):
    pre = [r for r in rows if t_event - PRE_S <= float(r["ts"]) < t_event]
    post = [r for r in rows if t_event <= float(r["ts"]) < t_event + POST_S]
    pa, pr, pn = acc_regret(pre)
    qa, qr, qn = acc_regret(post)
    return {"cell": cell, "run": run, "event": kind, "pre_acc": pa, "pre_regret": pr, "pre_n": pn,
            "post_acc": qa, "post_regret": qr, "post_n": qn,
            "recover_s": recover_s(rows, t_event, pa)}


def main(root: Path, out: Path):
    out.mkdir(parents=True, exist_ok=True)
    tc, ev, wd = [], [], []
    for run_dir in sorted(root.glob("*/run[0-9][0-9]")):
        cell, run = run_dir.parent.name, run_dir.name
        try:
            data = load_run(run_dir)
            rows, _ = score_run(data)
        except OracleError as e:
            print(f"skip {cell}/{run}: {e}", file=sys.stderr)
            continue
        rows.sort(key=lambda r: float(r["ts"]))
        t0 = float(rows[0]["ts"])

        bins = defaultdict(list)
        for r in rows:
            bins[int((float(r["ts"]) - t0) // BIN_S)].append(r)
        for b in sorted(bins):
            a, g, n = acc_regret(bins[b])
            tc.append({"cell": cell, "run": run, "t_s": b * BIN_S, "decisions": len(bins[b]),
                       "with_choice": n, "accuracy": a, "regret_mean": g})

        if cell.startswith("e2a-"):
            phases = data["truth"].get("phases") or []
            leaves = {a for m in data["group_members"].values() for a in m}
            starts = [float(v) for k, v in data["agent_starts"].items() if k in leaves and v]
            if phases and starts:
                t_flip = statistics.median(starts) + float(phases[0]["after_s"])
                ev.append({**around(rows, t_flip, cell, run, "flip"),
                           "event_at_s": t_flip - t0})
        elif cell.startswith("e2c-mixshift"):
            after = [float(r["ts"]) for r in rows
                     if str(r["job_id"]).isdigit() and int(r["job_id"]) >= MIX_SHIFT_JOB]
            if after:
                t_shift = min(after)
                ev.append({**around(rows, t_shift, cell, run, "mix_shift"),
                           "event_at_s": t_shift - t0})
        elif cell.startswith("e2b-"):
            o = json.loads((run_dir / "outage.json").read_text())
            group, k, j = int(o["group"]), float(o["killed_at"]), float(o.get("rejoined_at") or 0)
            # Counted from the RAW decisions: a decision naming the killed group cannot be
            # priced, so the scored rows would drop exactly the delegations made while it was down.
            grp = lambda xs: [int(x) for x in (xs or [])]
            raw = [d for d in data["decisions"] if d.get("ts")]
            coord = {d["agent_id"] for d in raw if group in grp(d.get("candidates"))}
            phase = {"before": [], "down": [], "after": []}
            for d in raw:
                if d["agent_id"] not in coord:
                    continue
                t = float(d["ts"])
                phase["before" if t < k else "down" if (not j or t < j) else "after"].append(d)
            to_g = {p: sum(group in grp(d.get("selected")) for d in v) for p, v in phase.items()}
            first = next((float(d["ts"]) - j for d in phase["after"]
                          if group in grp(d.get("selected"))), None)
            ev.append({"cell": cell, "run": run, "event": "outage", "event_at_s": k - t0,
                       "group": group, "coordinator": ",".join(sorted(coord)),
                       "held_s": (j - k) if j else None,
                       "dec_before": len(phase["before"]), "to_group_before": to_g["before"],
                       "dec_down": len(phase["down"]), "to_group_down": to_g["down"],
                       "dec_after": len(phase["after"]), "to_group_after": to_g["after"],
                       "readopt_s": first,
                       "decisions_scored": len(rows), "decisions_total": len(data["decisions"])})
        elif cell.startswith("e2c-workday"):
            arr = json.loads((run_dir / "arrivals.json").read_text())
            sched = arr["schedule"]
            planned = {str(j["job_id"]): float(j["planned_s"]) for j in arr["jobs"]}
            seg_of = lambda s: max(i for i, (start, _) in enumerate(sched) if s >= start)
            segs = defaultdict(list)
            for r in rows:
                p = planned.get(str(r["job_id"]))
                if p is not None:
                    segs[seg_of(p)].append(r)
            for i, (start, rate) in enumerate(sched):
                a, g, n = acc_regret(segs.get(i, []))
                wd.append({"cell": cell, "run": run, "segment": i, "start_s": start,
                           "rate": rate, "decisions": len(segs.get(i, [])), "with_choice": n,
                           "accuracy": a, "regret_mean": g})

    for name, data in (("timecourse.csv", tc), ("events.csv", ev), ("workday.csv", wd)):
        if data:
            keys = list(dict.fromkeys(k for d in data for k in d))
            with open(out / name, "w", newline="") as fh:
                w = csv.DictWriter(fh, fieldnames=keys)
                w.writeheader()
                w.writerows(data)
        print(f"{name}: {len(data)} rows")


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))
