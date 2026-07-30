#!/usr/bin/env python3
"""Scenario B analysis: mid-run failure-profile flip recovery.

Phase 0 parity: ram_bound -> odd groups, cpu_bound -> even groups.
Phase 1 (flipped): ram_bound -> even, cpu_bound -> odd.
Reports per-decile routing accuracy under BOTH parity definitions — the
crossover locates the flip; how fast phase-1 accuracy climbs afterwards
measures adaptation (discount / failure-window recovery).

Usage: analyze_scenario_b.py <full_dump.json> <label> [...]
"""
import json
import sys


def load_rows(path):
    data = json.load(open(path))
    rows = []
    for key, d in data["jobs"].items():
        if not key.startswith("job:0:"):
            continue
        group = int(key.split(":")[2])
        rc = (d.get("job_type") or "?").split("_")[0]
        if rc not in ("ram", "cpu"):
            continue
        ts_dict = d.get("assigned_at") or d.get("submitted_at") or {}
        if isinstance(ts_dict, dict) and ts_dict:
            ts = min(float(v) for v in ts_dict.values())
        else:
            ts = float(ts_dict) if ts_dict else 0.0
        success = int(d.get("exit_status") or 0) == 0
        p0_correct = (rc == "ram" and group % 2 == 1) or (rc == "cpu" and group % 2 == 0)
        rows.append((ts, rc, group, p0_correct, success))
    rows.sort()
    return rows


def analyze(path, label):
    rows = load_rows(path)
    n = len(rows)
    succ = sum(1 for r in rows if r[4])
    print(f"\n=== {label}: {n} L0 records, success {succ}/{n} ({100*succ/n:.1f}%) ===")
    print(f"{'decile':>7} {'phase0-correct':>15} {'phase1-correct':>15} {'success':>9}")
    d = max(1, n // 10)
    for i in range(10):
        part = rows[i*d:(i+1)*d] if i < 9 else rows[9*d:]
        if not part:
            continue
        p0 = 100 * sum(1 for r in part if r[3]) / len(part)
        p1 = 100 - p0
        s = 100 * sum(1 for r in part if r[4]) / len(part)
        print(f"{i:>7} {p0:>14.0f}% {p1:>14.0f}% {s:>8.0f}%")

    # Empirical flip: first decile AFTER the phase0-correct peak where the
    # majority swings to phase1 (guards against early exploration noise).
    p0_by_dec = [sum(1 for r in rows[i*d:(i+1)*d] if r[3]) /
                 max(1, len(rows[i*d:(i+1)*d])) for i in range(10)]
    peak = max(range(10), key=lambda i: p0_by_dec[i])
    flip_i = next((i for i in range(peak + 1, 10) if p0_by_dec[i] < 0.5), None)
    if flip_i is not None:
        post = rows[flip_i*d:]
        p1_acc = 100 * sum(1 for r in post if not r[3]) / len(post)
        s_acc = 100 * sum(1 for r in post if r[4]) / len(post)
        print(f"phase0 peak at decile {peak}; majority swings to phase1 at "
              f"decile {flip_i}; from there: phase1-correct {p1_acc:.1f}%, "
              f"success {s_acc:.1f}%")
    # Recovery: records after the success trough until rolling (window 20)
    # phase1-correct >= 70%
    trough = min(range(2*d, n), key=lambda i:
                 sum(1 for r in rows[max(0, i-10):i+10] if r[4]))
    rec = None
    for i in range(trough, n - 20):
        window = rows[i:i+20]
        if sum(1 for r in window if not r[3]) >= 14:
            rec = i - trough
            break
    print(f"success trough at record {trough}/{n}; "
          f"records to 70% phase1 routing: {rec if rec is not None else 'not reached'}")
    return rows


if __name__ == "__main__":
    for path, label in zip(sys.argv[1::2], sys.argv[2::2]):
        analyze(path, label)
