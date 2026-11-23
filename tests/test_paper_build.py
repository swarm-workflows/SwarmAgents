"""evaluation/paper: failure_summary.py rows and build.py's cell table, validity and completion basis."""
import csv
import io
import json
import os
import sys
from contextlib import redirect_stdout

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "evaluation", "paper"))

import build  # noqa: E402
import failure_summary  # noqa: E402


def _summary(root):
    buf = io.StringIO()
    with redirect_stdout(buf):
        failure_summary.main(str(root))
    return {(r["cell"], r["run"]): r for r in csv.DictReader(io.StringIO(buf.getvalue()))}


def test_baseline_requeue_is_read_from_run_meta(tmp_path):
    run = tmp_path / "e7f-hier90-greedy-poisson3" / "run01"
    run.mkdir(parents=True)
    (run / "run_meta.json").write_text(json.dumps({
        "started_at": 1000.0,
        "scheduler_stats": {"workers_failed": 9, "jobs_requeued": 14, "workers_readmitted": 9},
        "failure_events": [{"at": 1190.0, "dead": [28, 29], "readmitted": []},
                           {"at": 1400.0, "dead": [], "readmitted": [28, 29]}]}))
    (run / "outage.json").write_text(json.dumps({"outcome": "ok", "held_s": 180.0, "killed_at": 1123.0,
                                                 "rejoined_at": 1303.0, "members": [28, 29]}))
    r = _summary(tmp_path)[("e7f-hier90-greedy-poisson3", "run01")]
    assert r["event"] == "outage" and r["outcome"] == "ok"
    assert float(r["event_at_s"]) == 123.0 and float(r["end_event_at_s"]) == 303.0
    assert (r["workers_failed"], r["jobs_requeued"], r["workers_readmitted"]) == ("9", "14", "9")
    assert float(r["first_requeue_after_kill_s"]) == 67.0
    assert r["members"] == "28 29"


def test_sparrow_counts_workers_once_and_sums_requeues(tmp_path):
    run = tmp_path / "e7f-hier90-sparrow-poisson3" / "run01"
    run.mkdir(parents=True)
    (run / "run_meta.json").write_text(json.dumps({"started_at": 0, "scheduler_stats": {
        "0": {"workers_failed": 9, "jobs_requeued": 3, "workers_readmitted": 9},
        "1": {"workers_failed": 9, "jobs_requeued": 5, "workers_readmitted": 9}}}))
    r = _summary(tmp_path)[("e7f-hier90-sparrow-poisson3", "run01")]
    assert (r["workers_failed"], r["jobs_requeued"], r["workers_readmitted"]) == ("9", "8", "9")


def test_sparrow_with_a_missing_scheduler_report_is_left_blank(tmp_path):
    run = tmp_path / "e7f-hier90-sparrow-poisson3" / "run01"
    run.mkdir(parents=True)
    (run / "run_meta.json").write_text(json.dumps({"started_at": 0, "scheduler_stats": {
        "0": {"workers_failed": 9, "jobs_requeued": 3}, "1": None}}))
    r = _summary(tmp_path)[("e7f-hier90-sparrow-poisson3", "run01")]
    assert r["jobs_requeued"] == "" and r["workers_failed"] == ""   # never a partial sum


def test_no_event_means_blank_not_zero(tmp_path):
    run = tmp_path / "e1-hier90-snow-analytic" / "run01"
    run.mkdir(parents=True)
    (run / "run_meta.json").write_text(json.dumps({"started_at": 0}))
    r = _summary(tmp_path)[("e1-hier90-snow-analytic", "run01")]
    assert r["event"] == "" and r["jobs_requeued"] == ""


def test_partition_record_beside_the_run(tmp_path):
    cell = tmp_path / "e6-hier30-pbft"
    (cell / "run02").mkdir(parents=True)
    (cell / "run02" / "run_meta.json").write_text(json.dumps({"started_at": 100}))
    (cell / "run02.partition.json").write_text(json.dumps({"outcome": "ok", "held_s": 186.6,
                                                           "applied_at": 190, "healed_at": 377}))
    r = _summary(tmp_path)[("e6-hier30-pbft", "run02")]
    assert r["event"] == "partition" and float(r["event_at_s"]) == 90.0


@pytest.mark.parametrize("cell,exp", [
    ("e1-hier270-snow-linucb-shared-poisson7", dict(exp="e1", topo="hier", agents=270, engine="snow",
                                                    plane="linucb-shared", arrival="poisson7")),
    ("e7-hier90-round-robin-burst", dict(exp="e7", engine="round-robin", plane="baseline", arrival="burst")),
    ("e2c-mixshift-hier90-pbft-eps", dict(exp="e2c", variant="mixshift", engine="pbft", plane="eps")),
    ("e1-mesh180-pbft-analytic", dict(topo="mesh", agents=180, plane="analytic", arrival="burst")),
    ("e7f-hier90-sparrow-poisson3", dict(exp="e7f", engine="sparrow", plane="baseline", arrival="poisson3")),
])
def test_cell_names_parse(cell, exp):
    got = build.parse_cell(cell)
    for k, v in exp.items():
        assert got[k] == v, (k, got)


def _wide(path, rows):
    cols = ["run_dir", "jobs_completed", "jobs_executed_twice", "msgs_dropped", "agents", "engine",
            "job_latency_p95"]
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in cols})


def test_build_completion_uses_the_plan_and_flags_bad_runs(tmp_path):
    camp = tmp_path / "c.yml"
    camp.write_text("defaults:\n  args:\n    jobs: 600\ncells:\n"
                    "- name: e0-hier30-pbft-analytic\n"
                    "- name: e1-hier90-snow-analytic\n  args:\n    jobs: 1800\n")
    _wide(tmp_path / "w.csv", [
        {"run_dir": "/x/e0-hier30-pbft-analytic/run01", "jobs_completed": 600, "jobs_executed_twice": 0,
         "agents": 999, "engine": "wrong", "job_latency_p95": 10},
        {"run_dir": "/x/e0-hier30-pbft-analytic/run02", "jobs_completed": 540, "jobs_executed_twice": 1,
         "job_latency_p95": 20},
        {"run_dir": "/x/e0-hier30-pbft-analytic/run02.attempt1", "jobs_completed": 1},
        {"run_dir": "/x/e1-hier90-snow-analytic/run01", "jobs_completed": 1800, "msgs_dropped": 5},
        {"run_dir": "/x/e9-unplanned-cell/run01", "jobs_completed": 7},
    ])
    (tmp_path / "m.yml").write_text(
        "out: o\nsources:\n  - name: s\n    runs_wide: w.csv\n    campaign: c.yml\n")
    assert build.main(["--manifest", str(tmp_path / "m.yml"), "--no-figures"]) == 0
    with open(tmp_path / "o" / "cells.csv") as fh:
        cells = {r["cell"]: r for r in csv.DictReader(fh)}
    pbft = cells["e0-hier30-pbft-analytic"]
    assert pbft["n"] == "2"                                   # the .attempt run is dropped
    assert float(pbft["completion_pct"]) == pytest.approx(95.0)   # (100 + 90) / 2, against 600
    assert pbft["agents"] == "30" and pbft["engine"] == "pbft"    # the cell name wins
    assert float(cells["e1-hier90-snow-analytic"]["completion_pct"]) == 100.0
    assert cells["e9-unplanned-cell"]["completion_pct"] == ""     # no plan, no completion
    v = (tmp_path / "o" / "validity.md").read_text()
    assert "a job ran twice (1)" in v and "completed 90.0 %" in v and "sends dropped (5)" in v
    assert "run01 | a job ran twice" not in v


def test_t6_table_keeps_engines_apart(tmp_path):
    cols = ["run", "workflow", "jobs", "swarm_exit0", "peg_exec_critical_path_s",
            "swarm_exec_critical_path_s", "exec_ratio_adj_median"]
    with open(tmp_path / "t6.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        w.writerow(["e1-hier90-pbft-mixed/run01", "wf", 3, 3, 10, 11, 1.0])
        w.writerow(["e1-hier90-pbft-mixed/run02", "wf", 3, 3, 10, 13, 1.1])
        w.writerow(["e1-hier90-snow-mixed/run01", "wf", 3, 3, 10, 30, 2.0])
    md = build.tables_md(None, build.pd.DataFrame(columns=["source", "exp"]),
                         {"t6_per_workflow": "t6.csv"}, tmp_path)
    rows = [l for l in md.splitlines() if l.startswith("| ") and "| wf |" in l]
    assert len(rows) == 2
    pbft = next(l for l in rows if "pbft-mixed" in l)
    assert "| 2 | 6/6 |" in pbft and "30" not in pbft


def test_completion_note_is_run_level_and_never_rounds_into_a_claim():
    pd = build.pd
    cells = pd.DataFrame({"cell": ["a", "b"], "source": ["s", "s"]})
    def runs(rows):
        return pd.DataFrame([{"cell": c, "source": "s", "jobs_completed": d, "planned_jobs": p}
                             for c, d, p in rows])
    assert build.completion_note(runs([("a", 600, 600), ("b", 1800, 1800)]), cells) == " (all 2 runs complete)"
    # 99.6 % must not print as "all 100 %"; a mean of 101 and 99 must not read 100
    n = build.completion_note(runs([("a", 1793, 1800), ("b", 1800, 1800)]), cells)
    assert "all" not in n and "99.6" in n
    n = build.completion_note(runs([("a", 606, 600), ("a", 594, 600), ("b", 1800, 1800)]), cells)
    assert "all" not in n and "99.0" in n and "101.0" in n
    n = build.completion_note(runs([("a", 600, 600), ("b", 1800, float("nan"))]), cells)
    assert n == " (all 1 runs complete, 1 unknown)"
