"""Code review 2026-10-05 §60: a failed agent start is detected at launch, not at the end.

The remote start command ended with `&`, backgrounding the whole `&&` chain, so ssh returned 0
before anything had run; the starter itself exited 0 having merely forked the agents. Both are
fixed: the starter checks its agents survive startup, and the remote command runs it in the
foreground. These run the REAL starter with a fake `python3.11` on PATH.
"""
import os
import shutil
import stat
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

FAKE = """#!/bin/bash
# `-c` is the starter reading agent_type out of a config.
if [ "$1" = "-c" ]; then echo resource; exit 0; fi
if [ "$FAKE_MODE" = crash ]; then exit 1; fi
exec sleep 3
"""


def _run_starter(tmp_path, mode, count=2):
    shutil.copy(os.path.join(REPO, "swarm-multi-start.sh"), tmp_path / "start.sh")
    (tmp_path / "configs").mkdir()
    for i in range(1, count + 1):
        (tmp_path / "configs" / f"config_swarm_multi_{i}.yml").write_text("agent_type: resource\n")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    fake = bindir / "python3.11"
    fake.write_text(FAKE)
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    env = {**os.environ, "PATH": f"{bindir}:{os.environ['PATH']}", "FAKE_MODE": mode,
           "STARTUP_CHECK_S": "1"}
    return subprocess.run(["bash", "start.sh", "resource", str(count), "mesh", "10", "localhost",
                           "10", "--use-config-dir", "--add"],
                          cwd=tmp_path, env=env, capture_output=True, text=True, timeout=30)


def test_agents_that_die_at_startup_fail_the_starter(tmp_path):
    res = _run_starter(tmp_path, "crash")
    assert res.returncode != 0
    assert "exited during startup" in res.stderr


def test_agents_that_survive_pass(tmp_path):
    res = _run_starter(tmp_path, "ok")
    assert res.returncode == 0, res.stderr
    assert "2 verified running" in res.stdout


def test_the_remote_command_runs_the_starter_in_the_foreground():
    src = open(os.path.join(REPO, "run_test.py")).read()
    assert 'f" > agent_{start_idx}_start.log 2>&1 < /dev/null"' in src
    assert 'f" > agent_{start_idx}_start.log 2>&1 &"' not in src


def test_a_failed_host_fails_the_whole_launch(tmp_path, monkeypatch):
    import argparse
    import run_test
    import yaml
    cfg = tmp_path / "cfg"
    cfg.mkdir()
    for i in (1, 2):
        (cfg / f"{run_test.CFG_PREFIX}{i}.yml").write_text(yaml.safe_dump({"grpc": {"host": f"h{i}"}}))
    args = argparse.Namespace(
        agents=2, agents_per_host=1, config_dir=str(cfg), starter="swarm-multi-start.sh",
        remote_repo_dir="/root/SwarmAgents", groups=None, group_size=None, debug=False,
        agent_type="resource", topology="mesh", jobs=1, db_host="database", jobs_per_proposal=1)

    def ssh_check(host, cmd, **k):
        if "nohup bash" in cmd and host == "h2":
            raise subprocess.CalledProcessError(1, "ssh")
    monkeypatch.setattr(run_test, "ssh_check", ssh_check)
    monkeypatch.setattr(run_test, "scp_to", lambda *a: None)
    with pytest.raises(SystemExit, match=r"failed on 1 host\(s\)"):
        run_test.start_agents_remote(args, ["h1", "h2"])


def test_cleanup_fails_loudly_when_redis_is_unreachable():
    res = subprocess.run([sys.executable, os.path.join(REPO, "cleanup.py"), "--agents", "1",
                          "--redis-host", "127.0.0.1", "--redis-port", "1", "--cleanup-redis"],
                         capture_output=True, text=True, cwd=REPO, timeout=60)
    assert res.returncode != 0


# --------------------------------------------------------------------------- stop-time review
PARTIAL = """#!/bin/bash
if [ "$1" = "-c" ]; then echo resource; exit 0; fi
# main.py <index>: agent 2 crashes; the others stay up and record their pid.
if [ "$2" = "2" ]; then exit 1; fi
echo $$ > "$PIDDIR/agent-$2.pid"
exec sleep 30          # exec: the recorded pid IS the long-lived process, as a real agent's is
"""


def test_a_failed_start_stops_the_agents_that_survived(tmp_path):
    shutil.copy(os.path.join(REPO, "swarm-multi-start.sh"), tmp_path / "start.sh")
    (tmp_path / "configs").mkdir()
    for i in (1, 2, 3):
        (tmp_path / "configs" / f"config_swarm_multi_{i}.yml").write_text("agent_type: resource\n")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    fake = bindir / "python3.11"
    fake.write_text(PARTIAL)
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    piddir = tmp_path / "pids"
    piddir.mkdir()
    env = {**os.environ, "PATH": f"{bindir}:{os.environ['PATH']}", "STARTUP_CHECK_S": "1",
           "PIDDIR": str(piddir)}
    res = subprocess.run(["bash", "start.sh", "resource", "3", "mesh", "10", "localhost", "10",
                          "--use-config-dir", "--add"],
                         cwd=tmp_path, env=env, capture_output=True, text=True, timeout=30)
    assert res.returncode != 0
    survivors = [int((piddir / f).read_text()) for f in os.listdir(piddir)]
    assert survivors, "the healthy agents should have started"
    for pid in survivors:
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)


def test_a_failed_launch_stops_every_agent_the_run_started(monkeypatch):
    import argparse
    import run_test
    stopped = []
    monkeypatch.setattr(run_test, "start_agents_remote",
                        lambda args, hosts: (_ for _ in ()).throw(SystemExit("h2 failed")))
    monkeypatch.setattr(run_test, "stop_agents", lambda args, hosts: stopped.append(hosts))
    with pytest.raises(SystemExit):
        run_test.launch_or_teardown(argparse.Namespace(mode="remote"), ["h1", "h2"])
    assert stopped == [["h1", "h2"]]


def test_a_failed_dynamic_addition_fails_the_run():
    import run_test
    run_test._DRAIN.clear()
    run_test._PRODUCER_RC.clear()
    run_test._DRAIN["dynamic_start_failed"] = "h3 failed"
    try:
        assert "dynamic agent addition failed" in run_test.run_failed_to_measure()
    finally:
        run_test._DRAIN.clear()
