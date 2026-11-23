#!/bin/bash
# Collect the frozen re-run (isc27-freeze 0bceb349) into one tree, on the database node, as root.
#
#   cd /root/SwarmAgents && bash evaluation/paper/collect_frozen.sh [runs-root] [out]
#
# Defaults: runs/frozen-0bceb349 -> /root/collect-frozen. Every campaign out-dir found under the
# runs root is collected on its own (one collect.py invocation each, so one code path per number),
# and then the per-experiment analyses that need the raw run trees run beside it:
#
#   <out>/<file>/runs_wide.csv ...   evaluation/collect.py (also job_rtt.csv, decisions.csv)
#   <out>/e2-hier90/{timecourse,events,workday}.csv          E2 time course (e2-c9c980b8/timecourse.py)
#   <out>/e3a-<file>/{per_run,cells}.csv                      E3a RTT bins (e3a-c125ed8c/e3a.py)
#   <out>/e1-hier270/supp.csv                                 Hier-270 tier detail (e1-hier270-ed8c9101/supp.py)
#   <out>/<file>/events_summary.csv                           outage / partition / requeue (failure_summary.py)
#   <out>/e6-hier30-mixed/t6/<cell>-<run>/t6_per_*.csv        real workflows against Pegasus (t6_compare.py)
#
# A missing campaign directory is skipped with a note, so this runs on a partial campaign too.
# Ends with <out>.tgz for the copy home; evaluation/paper/build.py reads the unpacked tree.
set -u
cd "$(dirname "$0")/../.."
RUNS=${1:-runs/frozen-0bceb349}
OUT=${2:-/root/collect-frozen}
PY=${PY:-python3.11}
mkdir -p "$OUT"
export PYTHONPATH=$PWD
note() { echo "[$(date -u '+%F %T')] $*" | tee -a "$OUT/collect.log"; }

for d in "$RUNS"/*/; do
    n=$(basename "$d")
    note "collect $n"
    $PY evaluation/collect.py --root "$d" --out "$OUT/$n" > "$OUT/$n.collect.log" 2>&1 \
        || note "collect $n FAILED (see $OUT/$n.collect.log)"
    $PY evaluation/paper/failure_summary.py "$d" > "$OUT/$n/events_summary.csv" 2>> "$OUT/$n.collect.log" \
        || note "failure_summary $n FAILED"
    if [ -s "$OUT/$n/job_rtt.csv" ]; then
        $PY docs/results/e3a-c125ed8c/e3a.py "$OUT/$n/job_rtt.csv" "$OUT/e3a-$n" > "$OUT/e3a-$n.md" 2>&1 \
            || note "e3a $n FAILED"
    fi
done

if [ -d "$RUNS/e2-hier90" ]; then
    note "E2 time course"
    $PY docs/results/e2-c9c980b8/timecourse.py "$RUNS/e2-hier90" "$OUT/e2-hier90" >> "$OUT/collect.log" 2>&1 \
        || note "timecourse FAILED"
fi
if [ -d "$RUNS/e1-hier270" ]; then
    note "Hier-270 supp"
    $PY docs/results/e1-hier270-ed8c9101/supp.py "$RUNS/e1-hier270" > "$OUT/e1-hier270/supp.csv" 2>> "$OUT/collect.log" \
        || note "supp FAILED"
fi
if [ -d "$RUNS/e6-hier30-mixed" ]; then
    note "T-6 compare on E6 mixed"
    for r in "$RUNS"/e6-hier30-mixed/*/run[0-9][0-9]; do
        [ -d "$r" ] || continue
        c=$(basename "$(dirname "$r")"); k=$(basename "$r")
        $PY evaluation/t6_compare.py --jobs-dir /export/swarm-wf/mixed_600_wf --run "$r" \
            --out "$OUT/e6-hier30-mixed/t6/$c-$k" --container-startup-s 0.17 >> "$OUT/collect.log" 2>&1 \
            || note "t6_compare $c/$k FAILED"
    done
    # one file for build.py: every run's per-workflow rows, labelled by cell and run
    $PY - "$OUT/e6-hier30-mixed" <<'PY' || note "t6 concat FAILED"
import csv, sys
from pathlib import Path
root = Path(sys.argv[1]); rows = []
for f in sorted(root.glob("t6/*/t6_per_workflow.csv")):
    cell, _, run = f.parent.name.rpartition("-")
    for r in csv.DictReader(open(f)):
        rows.append({"cell": cell, "run": run, **r})
if rows:
    with open(root / "t6_per_workflow.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader(); w.writerows(rows)
PY
fi

tar czf "$OUT.tgz" -C "$(dirname "$OUT")" "$(basename "$OUT")"
note "DONE -> $OUT.tgz"
