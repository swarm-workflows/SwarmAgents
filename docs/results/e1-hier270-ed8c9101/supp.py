"""Per-run quantities evaluation/collect.py does not export, for E1' at Hier-270.

    python3.11 supp.py runs/campaign-e1-hier270-ed8c9101 > supp.csv

Per run (one row, keyed like runs_wide.csv's run_dir):
- coordinator tier: how many coordinators won a job, the busiest one's share, Jain over all
  coordinators (idle ones count as zeros) — from level1_jobs.csv `leader_id`;
- the busiest child group's share of delegations (LinUCB cells; analytic cells at G=1 have one
  candidate per coordinator) — from metrics.json `delegation_decisions`;
- PBFT/Snow counters summed per tier from metrics.json `instrumentation.consensus`;
- arrivals submitted, realised rate, span and worst lag behind plan, from arrivals.json `jobs`.
Refuses a run with no level1_jobs.csv rather than writing zeros.
"""
import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

TIER_COUNTERS = ("counter_proposals", "counter_margin_yields", "commit_lock_holds",
                 "commit_splits_resolved", "certificate_finalizes", "certificates_resent",
                 "abandoned", "finalized")


def jain(xs):
    s = sum(xs)
    q = sum(x * x for x in xs)
    return (s * s) / (len(xs) * q) if xs and q else float("nan")


def levels(run):
    try:
        agents = json.loads((run / "all_agents.csv").read_text())
    except (OSError, ValueError):
        return {}
    return {str(a.get("agent_id")): int(a.get("level") or 0) for a in agents}


def row(run: Path, root: Path) -> dict:
    lv = levels(run)
    coords = sorted(a for a, l in lv.items() if l == 1)
    l1 = run / "level1_jobs.csv"
    if not l1.is_file():
        raise SystemExit(f"{run}: no level1_jobs.csv — refusing to score this run")
    wins = Counter()
    with l1.open() as f:
        for r in csv.DictReader(f):
            lid = (r.get("leader_id") or "").strip()
            if lid:
                wins[str(int(float(lid)))] += 1
    counts = [wins.get(c, 0) for c in coords] or list(wins.values())
    total = sum(counts)
    out = {"run_dir": str(run.relative_to(root)),
           "coordinators": len(coords),
           "coords_winning": sum(1 for c in counts if c),
           "coord_busiest_share": round(max(counts) / total, 4) if total else float("nan"),
           "coord_jain": round(jain(counts), 4)}

    metrics = json.loads((run / "metrics.json").read_text())
    groups = Counter()
    per_tier = defaultdict(Counter)
    for aid, payload in metrics.items():
        if not isinstance(payload, dict):
            continue
        inst = payload.get("instrumentation") or {}
        # Top level of the payload, as evaluation/collect.py reads them (not under instrumentation).
        for dec in payload.get("delegation_decisions") or []:
            if dec.get("selected") is not None:
                groups[str(dec["selected"])] += 1
        cons = inst.get("consensus") or {}
        tier = lv.get(str(aid), 0)
        for k in TIER_COUNTERS:
            if cons.get(k) is not None:
                per_tier[tier][k] += int(cons.get(k) or 0)
    gt = sum(groups.values())
    out["child_groups_used"] = len(groups)
    out["group_busiest_share"] = round(max(groups.values()) / gt, 4) if gt else float("nan")
    for tier in (0, 1):
        for k in TIER_COUNTERS:
            v = per_tier[tier].get(k)
            out[f"l{tier}_{k}"] = v if v is not None else ""

    # arrivals.json `jobs`: each job's planned offset (None under batch) and actual submission
    # offset, both from the distributor's monotonic start.
    jobs = json.loads((run / "arrivals.json").read_text()).get("jobs") or []
    sub = [float(j["submitted_s"]) for j in jobs if j.get("submitted_s") is not None]
    lags = [float(j["submitted_s"]) - float(j["planned_s"]) for j in jobs
            if j.get("planned_s") is not None and j.get("submitted_s") is not None]
    span = (max(sub) - min(sub)) if len(sub) > 1 else 0.0
    out["arrivals"] = len(sub)
    out["arrival_span_s"] = round(span, 2) if span else ""
    out["arrival_rate_realised"] = round((len(sub) - 1) / span, 3) if span else ""
    out["arrival_lag_max_s"] = round(max(lags), 3) if lags else ""
    return out


def main(root_arg):
    root = Path(root_arg)
    runs = sorted(p for p in root.glob("*/run0?") if p.is_dir())
    rows = [row(r, root) for r in runs]
    w = csv.DictWriter(sys.stdout, fieldnames=list(rows[0]))
    w.writeheader()
    w.writerows(rows)


if __name__ == "__main__":
    main(sys.argv[1])
