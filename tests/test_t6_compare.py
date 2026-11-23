"""evaluation/t6_compare.py: per-job execution time against Pegasus, staging excluded on both sides."""
import csv
import json
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from evaluation import t6_compare  # noqa: E402

WF = "wf_run0001"


def _job(jid, outs, ins=(), container=True):
    return {"id": jid, "workflow": WF,
            "data_in": [{"name": "local", "file": f} for f in ins],
            "data_out": [{"name": "local", "file": f} for f in outs],
            "data_predicate": {"kind": "files", "files": list(ins)} if ins else None,
            "execution": {"transformation": jid.split("_")[-1], "path": "/srv/x",
                          "arguments": [],
                          "container": {"name": "c", "image": "c.sif"} if container else None}}


def _setup(tmp, exec_lines, extra_rows=()):
    jobs = tmp / "jobs"
    jobs.mkdir()
    recs = [_job(f"{WF}_a", ["a.out"]), _job(f"{WF}_b", ["b.out"], ["a.out"]),
            _job(f"{WF}_c", ["c.out"], ["a.out"]), {"id": "7", "workflow": "sim"}]
    for i, r in enumerate(recs):
        (jobs / f"job_{i}.json").write_text(json.dumps(r))
    peg = {"runs": {WF: {"makespan_sec": 100, "jobs": {
        f"{WF}_a": {"remote_duration_sec": 10.0, "queue_time_sec": 5,
                    "stage_in": {"duration_sec": 2}, "stage_out": {"duration_sec": 1}},
        f"{WF}_b": {"remote_duration_sec": 4.0, "queue_time_sec": 5,
                    "stage_in": {"duration_sec": 2}, "stage_out": {"duration_sec": 1}},
        f"{WF}_c": {"remote_duration_sec": 6.0, "queue_time_sec": 5,
                    "stage_in": {"duration_sec": 2}, "stage_out": {"duration_sec": 1}}}}}}
    (jobs / "pegasus_baseline.json").write_text(json.dumps(peg))
    run = tmp / "run01"
    (run / "agent-1").mkdir(parents=True)
    pre = "2026-11-11 22:46:46,375 - agent-3 - INFO - "
    (run / "agent-1" / "agent-3.log").write_text("".join(pre + l + "\n" for l in exec_lines))
    with open(run / "all_jobs.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["job_id", "submitted_at", "selection_started_at", "assigned_at",
                    "started_at", "completed_at", "exit_status", "leader_id", "reasoning_time",
                    "scheduling_latency", "selection_total", "group", "refused"])
        # level-1 row: earlier submission, never started
        for jid, sub, st, co in ((f"{WF}_a", 1000, 1001, 1012), (f"{WF}_b", 1000, 1013, 1018),
                                 (f"{WF}_c", 1000, 1013, 1020)):
            w.writerow([jid, sub - 5, 0, 0, 0, 0, 0, 90, 0, 0, 0, 0, 0])
            w.writerow([jid, sub, 0, 0, st, co, 0, 3, 0, 0, 0, 1, 0])
        for r in extra_rows:
            w.writerow(r)
    return jobs, run


def _run(tmp, jobs, run, *extra):
    out = tmp / "out"
    t6_compare.main(["--jobs-dir", str(jobs), "--run", str(run), "--out", str(out), *extra])
    with open(out / "t6_per_workflow.csv") as fh:
        wf = list(csv.DictReader(fh))
    with open(out / "t6_per_job.csv") as fh:
        per = {r["job_id"]: r for r in csv.DictReader(fh)}
    return wf, per


EXEC = [f"[EXEC] Job {WF}_a completed in 9.000s: apptainer exec ...",
        f"[EXEC] Job {WF}_b completed in 4.400s: apptainer exec ...",
        f"[EXEC] Job {WF}_c completed in 5.000s: apptainer exec ...",
        "[STAGE_OUT] a.out (100 bytes) to database:21000 in 0.250s"]


def test_execution_time_is_the_runner_duration_not_the_span(tmp_path):
    jobs, run = _setup(tmp_path, EXEC)
    wf, per = _run(tmp_path, jobs, run)
    a = per[f"{WF}_a"]
    assert float(a["swarm_exec_s"]) == 9.0           # the [EXEC] duration
    assert float(a["swarm_span_s"]) == 11.0          # started -> completed
    assert float(a["swarm_in_job_overhead_s"]) == 2.0
    assert float(a["swarm_stage_out_s"]) == 0.25
    assert float(a["swarm_wait_s"]) == 6.0           # from the EARLIEST submission
    assert "7" not in per                            # simulated jobs are not compared


def test_critical_path_and_end_to_end(tmp_path):
    jobs, run = _setup(tmp_path, EXEC)
    (s,), _ = _run(tmp_path, jobs, run)
    assert float(s["peg_exec_critical_path_s"]) == 16.0   # a(10) -> c(6)
    assert float(s["swarm_exec_critical_path_s"]) == 14.0  # a(9) -> c(5)
    assert float(s["swarm_end_to_end_s"]) == 25.0         # 995 -> 1020
    assert float(s["peg_end_to_end_s"]) == 100.0
    assert float(s["peg_non_compute_s"]) == 84.0
    assert int(s["swarm_exit0"]) == 3 and int(s["swarm_exec_missing"]) == 0


def test_a_missing_exec_line_refuses_the_critical_path(tmp_path):
    jobs, run = _setup(tmp_path, EXEC[:2])
    (s,), per = _run(tmp_path, jobs, run)
    assert s["swarm_exec_critical_path_s"] == ""
    assert "swarm: no exec time for 1 job" in s["critical_path_refused"]
    assert s["swarm_exec_sum_s"] == ""                    # never a sum over the jobs that have one
    assert per[f"{WF}_c"]["swarm_exec_s"] == ""


def test_the_last_attempt_wins_and_attempts_are_counted(tmp_path):
    lines = [f"[EXEC] Job {WF}_a exited 1 after 3.000s — boom (stderr: x)"] + EXEC
    jobs, run = _setup(tmp_path, lines)
    _, per = _run(tmp_path, jobs, run)
    assert per[f"{WF}_a"]["swarm_exec_attempts"] == "2"
    assert per[f"{WF}_a"]["swarm_exit"] == "0"


def test_container_startup_is_only_subtracted_when_asked(tmp_path):
    jobs, run = _setup(tmp_path, EXEC)
    _, per = _run(tmp_path, jobs, run)
    assert per[f"{WF}_a"]["swarm_exec_adj_s"] == ""
    (s,), per = _run(tmp_path, jobs, run, "--container-startup-s", "0.5")
    assert float(per[f"{WF}_a"]["swarm_exec_adj_s"]) == 8.5
    assert float(per[f"{WF}_a"]["swarm_exec_s"]) == 9.0   # the raw value is kept
    assert float(s["swarm_exec_adj_sum_s"]) == 16.9
