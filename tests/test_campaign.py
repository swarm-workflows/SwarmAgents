"""The campaign driver: cells x repeats, gated, classified, resumable.

Pins what an unattended night depends on: a refusal stops the campaign (it would repeat on
every retry); a metrics shortfall, an unmeasurable run or a timeout is retried and the failed
attempt's directory is kept; a run that ended on its cap is a finished cell, not a failure (the
collapse cell ends that way); a failing gate skips the repeat instead of launching onto a broken
fleet; finished repeats are never re-run on resume; a hung attempt is killed at its timeout.
"""
import json
import os
import sys
import time
from pathlib import Path

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

import campaign  # noqa: E402
from campaign import Campaign, Gate, cell_argv, cell_config, classify, hosts_needed  # noqa: E402


def _spec(tmp, cells=None, **defaults):
    return {"out": str(tmp / "out"),
            "defaults": {"repeats": 1, "retries": 1, "timeout_s": 60,
                         "args": {"mode": "local", "agent-type": "resource"}, **defaults},
            "gate": {"retries": 2, "wait_s": 0},
            "cells": cells or [{"name": "c1", "args": {"agents": 3, "jobs": 10}}]}


class FakeRunner:
    """Plays back (exit status, drain status) per call and writes drain.json like a run."""

    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def __call__(self, argv, run_dir, timeout_s, on_start=None):
        self.calls.append(run_dir)
        if on_start:
            on_start(999999)
        rc, drain, timed_out = self.outcomes.pop(0)
        if rc == "interrupt":
            raise KeyboardInterrupt
        run_dir.mkdir(parents=True, exist_ok=True)
        Path(f"{run_dir}.log").write_text(f"attempt {len(self.calls)}\n")
        if drain:
            (run_dir / "drain.json").write_text(json.dumps({"status": drain}))
        return rc, timed_out


def _gate(spec, ok=True, ssh=lambda h: True):
    return Gate(spec, ssh_check=ssh, redis_check=lambda: ok, clock_check=lambda: True)


def _campaign(tmp, runner, spec=None, gate_ok=True, stopped=None, **kw):
    spec = spec or _spec(tmp)
    stops = stopped if stopped is not None else []
    kw.setdefault("ours", lambda pgid, run_dir, started: True)
    return Campaign(spec, tmp / "out", _gate(spec, gate_ok), runner=runner,
                    sleep=lambda s: None, stopper=lambda cfg: stops.append(cfg["name"]), **kw)


# --------------------------------------------------------------------------- building

def test_args_become_flags_and_the_driver_owns_run_dir(tmp_path):
    spec = _spec(tmp_path, cells=[{"name": "c", "args": {"agents": 30, "debug": True,
                                                         "skip": False, "none": None},
                                   "extra": ["--x", "1"]}])
    cfg = cell_config(spec, spec["cells"][0])
    argv = cell_argv(cfg, tmp_path / "r", python="py")
    assert argv[:2] == ["py", str(Path(REPO) / "run_test.py")]
    assert argv[argv.index("--agents") + 1] == "30"
    assert "--debug" in argv and "--skip" not in argv and "--none" not in argv
    assert argv[argv.index("--run-dir") + 1] == str(tmp_path / "r")
    assert argv[-2:] == ["--x", "1"]
    assert argv[argv.index("--mode") + 1] == "local", "defaults merge under the cell"
    bad = cell_config(spec, {"name": "b", "args": {"run-dir": "x"}})
    with pytest.raises(SystemExit):
        cell_argv(bad, tmp_path / "r")


def test_hosts_needed_is_the_cells_share_of_the_hosts_file(tmp_path):
    hosts = tmp_path / "hosts"
    hosts.write_text("a\nb\nc\nd\n")
    cfg = cell_config({}, {"name": "c", "args": {"mode": "remote", "agents": 5,
                                                 "agents-per-host": 2,
                                                 "agent-hosts-file": str(hosts)}})
    assert hosts_needed(cfg) == ["a", "b", "c"]
    cfg["args"]["agents"] = 12
    assert hosts_needed(cfg)[-1].startswith("<missing-"), "a short file fails the gate"


@pytest.mark.parametrize("rc,drain,timed_out,expected", [
    (0, "drained", False, "ok"), (0, "all_terminal", False, "ok"),
    (0, "cap", False, "ok_on_cap"), (0, "timer", False, "ok_on_cap"),
    (0, "infeasible", False, "ok_infeasible"), (0, None, False, "ok_unverified"),
    (2, None, False, "refused"), (3, "drained", False, "metrics_shortfall"),
    (4, "redis_unreadable", False, "unmeasurable"), (1, None, False, "failed"),
    (None, None, True, "timeout"),
])
def test_classification(tmp_path, rc, drain, timed_out, expected):
    if drain:
        (tmp_path / "drain.json").write_text(json.dumps({"status": drain}))
    assert classify(rc, tmp_path, timed_out) == expected


# --------------------------------------------------------------------------- driving

def test_a_shortfall_is_retried_and_the_failed_attempt_is_kept(tmp_path):
    runner = FakeRunner([(3, "drained", False), (0, "drained", False)])
    c = _campaign(tmp_path, runner)
    assert c.run() == 0
    run = tmp_path / "out" / "c1" / "run01"
    assert (run.parent / "run01.attempt1").is_dir(), "the failed attempt is kept"
    assert Path(f"{run.parent / 'run01.attempt1'}.log").read_text() == "attempt 1\n", \
        "its log moves with it instead of being overwritten"
    assert Path(f"{run}.log").read_text() == "attempt 2\n"
    state = json.loads((tmp_path / "out" / "campaign_state.json").read_text())
    assert state["c1/run01"]["outcome"] == "ok"
    assert [a["outcome"] for a in state["c1/run01"]["attempts"]] == ["metrics_shortfall", "ok"]


def test_a_refusal_stops_the_campaign_without_a_retry(tmp_path):
    spec = _spec(tmp_path, cells=[{"name": "a"}, {"name": "b"}])
    runner = FakeRunner([(2, None, False)])
    c = _campaign(tmp_path, runner, spec=spec)
    assert c.run() == 2
    assert len(runner.calls) == 1, "no retry, and cell b never ran"


def test_continue_on_refusal_moves_to_the_next_cell(tmp_path):
    spec = _spec(tmp_path, cells=[{"name": "a"}, {"name": "b"}])
    runner = FakeRunner([(2, None, False), (0, "drained", False)])
    c = _campaign(tmp_path, runner, spec=spec, continue_on_refusal=True)
    assert c.run() == 1
    assert len(runner.calls) == 2


def test_a_run_that_ended_on_its_cap_is_finished_not_failed(tmp_path):
    runner = FakeRunner([(0, "cap", False)])
    assert _campaign(tmp_path, runner).run() == 0
    assert len(runner.calls) == 1


def test_retries_are_bounded(tmp_path):
    runner = FakeRunner([(1, None, False), (1, None, False)])
    assert _campaign(tmp_path, runner).run() == 1
    assert len(runner.calls) == 2, "one attempt plus one retry"


def test_a_failing_gate_skips_the_repeat_without_launching(tmp_path):
    runner = FakeRunner([])
    c = _campaign(tmp_path, runner, gate_ok=False)
    assert c.run() == 1
    assert runner.calls == []
    state = json.loads((tmp_path / "out" / "campaign_state.json").read_text())
    assert state["c1/run01"]["outcome"] == "gate_failed"
    assert "redis not answering" in state["c1/run01"]["attempts"][0]["problems"]


def test_a_down_host_fails_the_gate(tmp_path):
    hosts = tmp_path / "hosts"
    hosts.write_text("up-1\ndown-2\n")
    spec = _spec(tmp_path, cells=[{"name": "c", "args": {"mode": "remote", "agents": 2,
                                                         "agent-hosts-file": str(hosts)}}])
    gate = _gate(spec, ssh=lambda h: h.startswith("up"))
    assert gate.check(hosts_needed(cell_config(spec, spec["cells"][0]))) == [
        "1 host(s) down: down-2"]


def test_resume_never_reruns_a_finished_repeat(tmp_path):
    spec = _spec(tmp_path, repeats=2)
    first = FakeRunner([(0, "drained", False), (1, None, False), (1, None, False)])
    assert _campaign(tmp_path, first, spec=spec).run() == 1
    second = FakeRunner([(0, "drained", False)])
    assert _campaign(tmp_path, second, spec=spec).run() == 0
    assert [p.name for p in second.calls] == ["run02"]


def test_dry_run_prints_and_runs_nothing(tmp_path, capsys):
    runner = FakeRunner([])
    assert _campaign(tmp_path, runner, dry_run=True).run() == 0
    assert runner.calls == [] and "c1/run01:" in capsys.readouterr().out
    assert not (tmp_path / "out").exists()


def test_a_hung_attempt_is_killed_at_its_timeout(tmp_path):
    script = tmp_path / "hang.py"
    script.write_text("import time\ntime.sleep(60)\n")
    started = time.time()
    rc, timed_out = campaign.run_attempt([sys.executable, str(script)], tmp_path / "r", 1.0)
    assert timed_out and rc is None
    assert time.time() - started < 30


def test_the_real_attempt_path_records_exit_status_and_log(tmp_path):
    script = tmp_path / "ok.py"
    script.write_text("import json,sys,pathlib\nd=pathlib.Path(sys.argv[1]);d.mkdir()\n"
                      "(d/'drain.json').write_text(json.dumps({'status':'drained'}))\n"
                      "print('hello');sys.exit(0)\n")
    run = tmp_path / "r"
    rc, timed_out = campaign.run_attempt([sys.executable, str(script), str(run)], run, 30)
    assert (rc, timed_out) == (0, False)
    assert classify(rc, run, timed_out) == "ok"
    assert "hello" in Path(f"{run}.log").read_text()


def test_the_shipped_example_campaign_loads_and_dry_runs(tmp_path, capsys):
    spec = campaign.load_campaign(Path(REPO) / "campaigns" / "example.yml")
    c = Campaign(spec, tmp_path / "out", _gate(spec), dry_run=True, runner=FakeRunner([]))
    assert c.run() == 0
    out = capsys.readouterr().out
    assert "e0-hier30-pbft/run05:" in out and "--base-config" in out


def test_a_missing_base_config_is_a_refusal(tmp_path):
    import subprocess
    proc = subprocess.run(
        [sys.executable, "run_test.py", "--mode", "local", "--agent-type", "resource",
         "--agents", "1", "--jobs", "1", "--topology", "mesh", "--db-host", "localhost",
         "--run-dir", str(tmp_path / "r"), "--base-config", str(tmp_path / "nope.yml")],
        cwd=REPO, capture_output=True, text=True, timeout=60)
    assert proc.returncode == 2 and "no such file" in proc.stderr



# --------------------------------------------------------------------------- stop-gate fixes

def test_kept_attempts_never_collide(tmp_path):
    run = tmp_path / "run01"
    (tmp_path / "run01.attempt1").mkdir()
    run.mkdir()
    Path(f"{run}.log").write_text("x")
    kept = campaign.keep_failed_attempt(run)
    assert kept.name == "run01.attempt2" and Path(f"{kept}.log").read_text() == "x"
    assert not run.exists()


def test_an_interrupted_driver_records_the_attempt_and_leaves_no_running_marker(tmp_path):
    runner = FakeRunner([("interrupt", None, False)])
    assert _campaign(tmp_path, runner).run() == 130
    state = json.loads((tmp_path / "out" / "campaign_state.json").read_text())
    assert state["c1/run01"]["outcome"] == "interrupted"
    assert "running" not in state["c1/run01"]


def test_an_interrupted_attempt_kills_its_run(tmp_path):
    import subprocess
    script = tmp_path / "hang.py"
    script.write_text("import time\ntime.sleep(120)\n")
    seen = {}

    def on_start(pgid):
        seen["pgid"] = pgid
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        campaign.run_attempt([sys.executable, str(script)], tmp_path / "r", 60, on_start=on_start)
    assert not campaign.session_alive(seen["pgid"]), "the run outlived the driver"


def test_a_run_left_by_a_dead_driver_is_stopped_on_resume(tmp_path):
    import subprocess
    # A real orphan: started by an intermediate process that exits, so (like the run a dead
    # driver leaves) it is reparented to init and reaped there, not by this test.
    spawn = ("import subprocess,sys; p=subprocess.Popen([sys.executable,'-c',"
             "'import time; time.sleep(120)'], start_new_session=True, stdin=subprocess.DEVNULL,"
             " stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL); print(p.pid)")
    pgid = int(subprocess.run([sys.executable, "-c", spawn], capture_output=True,
                              text=True).stdout.strip())
    assert campaign.session_alive(pgid)
    out = tmp_path / "out"
    out.mkdir()
    (out / "campaign_state.json").write_text(json.dumps(
        {"c1/run01": {"attempts": [], "running": {"pgid": pgid, "at": 1.0}}}))
    runner = FakeRunner([(0, "drained", False)])
    assert _campaign(tmp_path, runner, ours=lambda g, r, t: True).run() == 0
    deadline = time.time() + 15
    while campaign.session_alive(pgid) and time.time() < deadline:
        time.sleep(0.2)
    assert not campaign.session_alive(pgid), "the dead driver's run is still going"
    state = json.loads((out / "campaign_state.json").read_text())
    assert [a["outcome"] for a in state["c1/run01"]["attempts"]] == ["interrupted", "ok"]


def test_a_second_driver_on_the_same_out_dir_is_refused(tmp_path):
    import fcntl
    out = tmp_path / "out"
    out.mkdir()
    holder = open(out / "campaign.lock", "w")
    fcntl.flock(holder, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        runner = FakeRunner([])
        assert _campaign(tmp_path, runner).run() == 2
        assert runner.calls == []
    finally:
        holder.close()


def test_the_exit_status_covers_only_the_selected_cells(tmp_path):
    spec = _spec(tmp_path, cells=[{"name": "a"}, {"name": "b"}])
    first = FakeRunner([(1, None, False), (1, None, False)])
    assert _campaign(tmp_path, first, spec=spec, only=["a"]).run() == 1
    second = FakeRunner([(0, "drained", False)])
    assert _campaign(tmp_path, second, spec=spec, only=["b"]).run() == 0, \
        "cell a's failure is not b's"



def test_a_killed_attempt_stops_its_remote_agents_and_a_finished_one_does_not(tmp_path):
    stopped = []
    runner = FakeRunner([(None, None, True), (0, "drained", False)])
    assert _campaign(tmp_path, runner, stopped=stopped).run() == 0
    assert stopped == ["c1"], "once, after the timeout; not after the clean attempt"


def test_an_interrupted_attempt_stops_its_remote_agents(tmp_path):
    stopped = []
    runner = FakeRunner([("interrupt", None, False)])
    assert _campaign(tmp_path, runner, stopped=stopped).run() == 130
    assert stopped == ["c1"]


def test_recovery_stops_the_dead_drivers_agents_and_never_kills_a_reused_id(tmp_path):
    import subprocess
    spawn = ("import subprocess,sys; p=subprocess.Popen([sys.executable,'-c',"
             "'import time; time.sleep(120)'], start_new_session=True, stdin=subprocess.DEVNULL,"
             " stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL); print(p.pid)")
    pgid = int(subprocess.run([sys.executable, "-c", spawn], capture_output=True,
                              text=True).stdout.strip())
    out = tmp_path / "out"
    out.mkdir()
    (out / "campaign_state.json").write_text(json.dumps(
        {"c1/run01": {"attempts": [], "running": {"pgid": pgid, "at": 1.0,
                                                  "run_dir": "/elsewhere"}}}))
    stopped = []
    runner = FakeRunner([(0, "drained", False)])
    c = _campaign(tmp_path, runner, stopped=stopped, ours=lambda g, r, t: False)
    try:
        assert c.run() == 0
        assert campaign.session_alive(pgid), "a session that is not ours must not be killed"
        assert stopped == ["c1"], "the dead driver's remote agents are stopped regardless"
    finally:
        campaign.kill_session(pgid, grace_s=5)


def test_session_ownership_needs_the_leader_its_start_time_and_its_run_dir(tmp_path):
    import subprocess
    marker = str(tmp_path / "marker-run-dir")
    proc = subprocess.Popen([sys.executable, "-c", "import time,sys; time.sleep(60)", marker],
                            start_new_session=True)
    try:
        time.sleep(0.3)
        started = campaign.process_start(proc.pid)
        assert started
        assert campaign.session_is_ours(proc.pid, marker, started)
        assert not campaign.session_is_ours(proc.pid, str(tmp_path / "other"), started)
        assert not campaign.session_is_ours(proc.pid, marker, "Thu Jan  1 00:00:00 1970"), \
            "a reused pid has a different start time"
        assert not campaign.session_is_ours(proc.pid, marker, None), "no record, no kill"
        # A campaign-looking command is not enough: this is a run_test.py, but not ours.
        other = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)",
                                  "run_test.py", "--run-dir", "/some/other/run"],
                                 start_new_session=True)
        try:
            time.sleep(0.3)
            assert not campaign.session_is_ours(other.pid, marker,
                                                campaign.process_start(other.pid))
        finally:
            campaign.kill_session(other.pid, grace_s=5, proc=other)
            other.wait()
    finally:
        campaign.kill_session(proc.pid, grace_s=5, proc=proc)
        proc.wait()


def test_the_stop_command_matches_the_runner(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(campaign.subprocess, "call", lambda cmd, **k: calls.append(cmd) or 0)
    cfg = cell_config({}, {"name": "c", "args": {"mode": "remote",
                                                 "agent-hosts-file": "hosts.txt"}})
    campaign.stop_cell_agents(cfg)
    assert calls[-1][:4] == ["bash", "stop_agents_v2.sh", "--mode", "remote"]
    assert "--agent-hosts-file" in calls[-1] and "hosts.txt" in calls[-1]
    local = cell_config({}, {"name": "s", "runner": "baselines/run_sparrow.py",
                             "args": {"mode": "local"}})
    campaign.stop_cell_agents(local)
    assert calls[-1][:3] == ["pkill", "-TERM", "-f"]


def test_run_sparrow_turns_sigterm_into_its_teardown():
    import signal as _signal
    from baselines.run_sparrow import _exit_on_signal
    with pytest.raises(SystemExit) as exc:
        _exit_on_signal(_signal.SIGTERM, None)
    assert exc.value.code == 128 + _signal.SIGTERM



def test_the_launch_records_the_leaders_start_time(tmp_path):
    seen = {}
    spec = _spec(tmp_path)

    def runner(argv, run_dir, timeout_s, on_start=None):
        import subprocess
        p = subprocess.Popen([sys.executable, "-c", "pass"], start_new_session=True)
        on_start(p.pid)
        seen.update(json.loads((tmp_path / "out" / "campaign_state.json").read_text()))
        p.wait()
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "drain.json").write_text(json.dumps({"status": "drained"}))
        return 0, False

    assert _campaign(tmp_path, runner, spec=spec).run() == 0
    running = seen["c1/run01"]["running"]
    assert running["leader_start"] and running["run_dir"].endswith("run01")


def test_a_differently_named_base_config_still_yields_launchable_config_names(tmp_path):
    """--base-config campaigns/config_pbft.yml produced config_pbft_<id>.yml, which no launcher
    globs; the first slice pilot was refused for it (2026-10-07)."""
    import run_test
    src = tmp_path / "config_pbft.yml"
    src.write_text((Path(REPO) / "campaigns" / "config_pbft.yml").read_text())
    staged = run_test.stage_base_config(str(src), str(tmp_path / "run"))
    assert Path(staged).name == "config_swarm_multi.yml"
    assert Path(staged).read_text() == src.read_text()
    same = tmp_path / "config_swarm_multi.yml"
    same.write_text("x: 1\n")
    assert run_test.stage_base_config(str(same), str(tmp_path / "run2")) == str(same)
    # And the generator then names agent files with the prefix the launchers expect.
    import subprocess
    out = tmp_path / "cfgs"
    proc = subprocess.run([sys.executable, "generate_configs.py", "3", "10", staged, str(out),
                           "mesh", "localhost", "6", "--skip-jobs"],
                          cwd=REPO, capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0, proc.stderr[-400:]
    assert sorted(p.name for p in out.glob("*.yml"))[:1] == ["config_swarm_multi_1.yml"]
