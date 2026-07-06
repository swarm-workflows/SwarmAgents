#!/usr/bin/env python3
"""Scenario C analysis: dynamic agent addition / cold-start adoption.

Group 4 (agents 21-25, low failure 0.05) is killed just after startup and
restarted mid-run; groups 0-3 fail everything at 0.30. Measures how quickly
the bandit adopts the superior group once it (re)appears, using the driver's
events file ('<epoch> group4_restarted') to locate the join.

Usage: analyze_scenario_c.py <full_dump.json> <events_file> <label> [...]
"""
import json
import sys


def load_rows(path):
    data = json.load(open(path))
    rows = []
    for key, d in data["jobs"].items():
        if not key.startswith("job:0:"):
            continue
        ts_dict = d.get("assigned_at") or d.get("submitted_at") or {}
        if isinstance(ts_dict, dict) and ts_dict:
            ts = min(float(v) for v in ts_dict.values())
        else:
            ts = float(ts_dict) if ts_dict else 0.0
        rows.append({
            "ts": ts,
            "group": int(key.split(":")[2]),
            "success": int(d.get("exit_status") or 0) == 0,
            "complete": d.get("state") == 8,
        })
    rows.sort(key=lambda r: r["ts"])
    return rows


def load_join_ts(events_path):
    join = None
    with open(events_path) as fh:
        for line in fh:
            parts = line.split(None, 1)
            if len(parts) == 2 and "restarted" in parts[1]:
                join = float(parts[0])
    return join


def analyze(dump_path, events_path, label):
    rows = load_rows(dump_path)
    join_ts = load_join_ts(events_path)
    if join_ts is None:
        print(f"{label}: no restart event found in {events_path}")
        return
    pre = [r for r in rows if r["ts"] < join_ts]
    post = [r for r in rows if r["ts"] >= join_ts]

    def stats(part):
        if not part:
            return 0, 0.0, 0.0
        g4 = sum(1 for r in part if r["group"] == 4)
        succ = sum(1 for r in part if r["success"])
        return len(part), 100 * g4 / len(part), 100 * succ / len(part)

    n_pre, g4_pre, s_pre = stats(pre)
    n_post, g4_post, s_post = stats(post)
    print(f"\n=== {label} ===")
    print(f"pre-join  ({n_pre:3d} records): group-4 share {g4_pre:5.1f}%, success {s_pre:5.1f}%")
    print(f"post-join ({n_post:3d} records): group-4 share {g4_post:5.1f}%, success {s_post:5.1f}%")

    first_g4 = next((i for i, r in enumerate(post) if r["group"] == 4), None)
    print(f"records to first group-4 delegation after join: "
          f"{first_g4 if first_g4 is not None else 'never'}")

    # adoption curve: group-4 share per post-join quarter
    if n_post >= 8:
        q = n_post // 4
        parts = [post[i*q:(i+1)*q] for i in range(3)] + [post[3*q:]]
        curve = [f"{100*sum(1 for r in p if r['group']==4)/len(p):.0f}%" for p in parts]
        scurve = [f"{100*sum(1 for r in p if r['success'])/len(p):.0f}%" for p in parts]
        print(f"post-join group-4 share by quarter: {' -> '.join(curve)}")
        print(f"post-join success by quarter:       {' -> '.join(scurve)}")


if __name__ == "__main__":
    args = sys.argv[1:]
    for dump, events, label in zip(args[0::3], args[1::3], args[2::3]):
        analyze(dump, events, label)
