#!/usr/bin/env python3
"""Scenario A analysis: routing accuracy over time from a full Redis dump.

A job is routed 'correctly' when its resource class avoids the group parity
that fails it: ram_bound -> odd group, cpu_bound -> even group.
Coordinator 26 leads groups {0, 4} (both even/ram-failing), so ram jobs it
delegates are counted separately — no correct arm exists for them.
"""
import json
import sys
from collections import Counter


def analyze(path, label):
    data = json.load(open(path))
    jobs = data["jobs"]

    rows = []
    for key, d in jobs.items():
        if not key.startswith("job:0:"):
            continue
        group = int(key.split(":")[2])
        jt = d.get("job_type") or "?"
        rc = jt.split("_")[0]  # ram | cpu
        if rc not in ("ram", "cpu"):
            continue
        # earliest level-0 assignment timestamp for ordering
        ts_dict = d.get("assigned_at") or d.get("submitted_at") or {}
        if isinstance(ts_dict, dict) and ts_dict:
            ts = min(float(v) for v in ts_dict.values())
        else:
            ts = float(ts_dict) if ts_dict else 0.0
        success = int(d.get("exit_status") or 0) == 0
        correct = (rc == "ram" and group % 2 == 1) or (rc == "cpu" and group % 2 == 0)
        rows.append((ts, rc, group, correct, success))

    rows.sort()
    n = len(rows)
    print(f"\n=== {label}: {n} L0 job records ===")
    succ = sum(1 for r in rows if r[4])
    corr = sum(1 for r in rows if r[3])
    print(f"overall: success {succ}/{n} ({100*succ/n:.1f}%), "
          f"routed-correct {corr}/{n} ({100*corr/n:.1f}%)")

    for name, part in (("first half", rows[:n//2]), ("second half", rows[n//2:])):
        s = sum(1 for r in part if r[4])
        c = sum(1 for r in part if r[3])
        print(f"{name:12s}: success {100*s/len(part):5.1f}%  "
              f"routed-correct {100*c/len(part):5.1f}%")

    # quarters for a finer curve
    q = n // 4
    quarters = [rows[i*q:(i+1)*q] for i in range(3)] + [rows[3*q:]]
    curve = ["%.0f%%" % (100*sum(1 for r in p if r[3])/len(p)) for p in quarters]
    scurve = ["%.0f%%" % (100*sum(1 for r in p if r[4])/len(p)) for p in quarters]
    print(f"routing by quarter: {' -> '.join(curve)}")
    print(f"success by quarter: {' -> '.join(scurve)}")

    by = Counter((r[1], "even" if r[2] % 2 == 0 else "odd", r[4]) for r in rows)
    for k in sorted(by):
        print("  ", k, by[k])
    return rows


if __name__ == "__main__":
    for path, label in zip(sys.argv[1::2], sys.argv[2::2]):
        analyze(path, label)
