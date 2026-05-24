#!/usr/bin/env python3
"""Extract and compare metrics from batch co-parent failover experiments."""
import csv
import os
import sys

import numpy as np


def extract_metrics(csv_path):
    sel_times = []
    sched_lats = []
    total_rows = 0
    pending_rows = 0
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_rows += 1
            try:
                sel_start = float(row["selection_started_at"])
                assigned = float(row["assigned_at"])
                completed = float(row.get("completed_at", 0))
                if completed == 0 or assigned == 0:
                    pending_rows += 1
                    continue
                sel = assigned - sel_start
                sched = float(row["scheduling_latency"]) if row.get("scheduling_latency") else assigned - float(row["submitted_at"])
                if sel < 0 or sched < 0:
                    continue
                sel_times.append(sel)
                sched_lats.append(sched)
            except (ValueError, KeyError):
                continue
    return {
        "total": total_rows,
        "completed": len(sel_times),
        "pending": pending_rows,
        "mean_sel": np.mean(sel_times) if sel_times else 0,
        "std_sel": np.std(sel_times) if sel_times else 0,
        "mean_sched": np.mean(sched_lats) if sched_lats else 0,
        "std_sched": np.std(sched_lats) if sched_lats else 0,
    }


def main():
    base = sys.argv[1] if len(sys.argv) > 1 else "runs/coparent-failure"
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 5

    print("=" * 95)
    hdr = "%s %s %s %s | %s %s" % ("Run".ljust(40), "Compl".rjust(6), "Total".rjust(6), "Rate".rjust(6), "Sel (s)".rjust(14), "Sched (s)".rjust(14))
    print(hdr)
    print("=" * 95)

    for phase in ["baseline", "failover"]:
        for i in range(1, runs + 1):
            path = os.path.join(base, phase, "run-%d" % i, "level0_jobs.csv")
            if not os.path.exists(path):
                print("  %s/run-%d  ** NOT FOUND **" % (phase, i))
                continue
            m = extract_metrics(path)
            rate = m["completed"] / m["total"] * 100 if m["total"] > 0 else 0
            print("  %s/run-%-34d %6d %6d %5.1f%% | %6.2f +/- %-5.2f %6.2f +/- %-5.2f" % (
                phase, i, m["completed"], m["total"], rate,
                m["mean_sel"], m["std_sel"], m["mean_sched"], m["std_sched"]))
        print()

    # Aggregate stats
    print("=" * 95)
    print("AGGREGATE STATISTICS")
    print("=" * 95)
    for phase in ["baseline", "failover"]:
        all_sel = []
        all_sched = []
        all_completed = []
        all_total = []
        for i in range(1, runs + 1):
            path = os.path.join(base, phase, "run-%d" % i, "level0_jobs.csv")
            if os.path.exists(path):
                m = extract_metrics(path)
                all_sel.append(m["mean_sel"])
                all_sched.append(m["mean_sched"])
                all_completed.append(m["completed"])
                all_total.append(m["total"])
        n = len(all_sel)
        if n == 0:
            print("  %s: NO DATA" % phase)
            continue
        mean_comp = np.mean(all_completed)
        std_comp = np.std(all_completed)
        mean_tot = np.mean(all_total)
        rate = mean_comp / mean_tot * 100 if mean_tot > 0 else 0
        print("  %s (n=%d):" % (phase, n))
        print("    Completed: %.1f +/- %.1f / %.1f  (%.1f%%)" % (mean_comp, std_comp, mean_tot, rate))
        print("    Selection time:   %.3f +/- %.3f s" % (np.mean(all_sel), np.std(all_sel)))
        print("    Scheduling latency: %.3f +/- %.3f s" % (np.mean(all_sched), np.std(all_sched)))
        print()


if __name__ == "__main__":
    main()
