"""group_outage.py: take one leaf group down and bring it back (E2b, F5)."""
import json
import os
import re
import sys

import pytest
import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

import group_outage as go  # noqa: E402


def _configs(tmp_path, groups):
    """groups: {agent_id: (level, group)}"""
    d = tmp_path / "configs"
    d.mkdir()
    for aid, (level, group) in groups.items():
        (d / f"config_swarm_multi_{aid}.yml").write_text(yaml.safe_dump(
            {"agent_type": "resource", "topology": {"level": level, "group": group}}))
    return str(d)


class FakeFleet:
    """Agent processes per host; answers the kill and restart scripts like the real hosts."""

    def __init__(self, placed, leaky=(), dead_on_restart=()):
        self.up = {aid: True for aid in placed}
        self.placed = placed
        self.leaky = set(leaky)
        self.dead_on_restart = set(dead_on_restart)
        self.restarts = []
        self.scripts = []

    def __call__(self, host, script, timeout=60.0):
        self.scripts.append((host, script))
        aid = int(re.search(r"main\\?\.py (\d+)", script).group(1))
        assert self.placed[aid] == host, "script sent to the wrong host"
        if script.startswith("pkill"):
            if aid not in self.leaky:
                self.up[aid] = False
            return 0, "ALIVE" if self.up[aid] else "GONE"
        if self.up[aid]:                       # the script's own guard: already running
            return 0, "ALREADY"
        self.restarts.append(aid)
        self.up[aid] = aid not in self.dead_on_restart
        return 0, "UP" if self.up[aid] else "DOWN"


def _args(tmp_path, cfg_dir, hosts, **kw):
    hf = tmp_path / "hosts.txt"
    hf.write_text("\n".join(hosts))
    run_dir = tmp_path / "run01"
    run_dir.mkdir(exist_ok=True)
    (run_dir / "run_meta.json").write_text(json.dumps({"run_id": "run01-abc"}))
    argv = ["run", "--hosts-file", str(hf), "--agents", "6", "--group", "1", "--config-dir", cfg_dir,
            "--duration", "60", "--run-dir", str(run_dir), "--out", str(tmp_path / "outage.json")]
    for k, v in kw.items():
        argv += [f"--{k.replace('_', '-')}"] + ([] if v is True else [str(v)])
    return argv


@pytest.fixture
def cell(tmp_path, monkeypatch):
    # agents 1-2 group 0, 3-4 group 1, 5 group 2, 6 a coordinator (level 1, group 0)
    cfg = _configs(tmp_path, {1: (0, 0), 2: (0, 0), 3: (0, 1), 4: (0, 1), 5: (0, 2), 6: (1, 0)})
    hosts = [f"agent-{i}" for i in range(1, 7)]
    clock = [1000.0]
    monkeypatch.setattr(go.time, "time", lambda: clock[0])
    monkeypatch.setattr(go.time, "sleep", lambda s: clock.__setitem__(0, clock[0] + s))
    return tmp_path, cfg, hosts, clock


def _record(tmp_path):
    return json.loads((tmp_path / "outage.json").read_text())


# --------------------------------------------------------------------------- who

def test_members_are_the_level0_agents_of_the_group(tmp_path):
    cfg = _configs(tmp_path, {1: (0, 0), 2: (0, 1), 3: (0, 1), 9: (1, 1)})
    assert go.group_members(cfg, 1) == {2: "resource", 3: "resource"}


def test_an_empty_group_is_refused(tmp_path):
    cfg = _configs(tmp_path, {1: (0, 0)})
    with pytest.raises(SystemExit):
        go.group_members(cfg, 7)


def test_placement_follows_run_test():
    assert go.place({1: "r", 2: "r", 3: "r"}, ["h1", "h2"], 2) == {1: "h1", 2: "h1", 3: "h2"}
    with pytest.raises(SystemExit):
        go.place({5: "r"}, ["h1"], 2)


def test_the_pattern_never_matches_another_agent():
    pat = re.compile(go._pattern(5))
    assert pat.search("python3.11 main.py 5 --agent-type resource")
    assert pat.search("python3.11 main.py 5")
    assert not pat.search("python3.11 main.py 50 --agent-type resource")
    assert not pat.search("python3.11 main.py 15")


# --------------------------------------------------------------------------- the run

def test_a_full_outage_and_rejoin_exits_zero(cell, monkeypatch):
    tmp_path, cfg, hosts, clock = cell
    placed = {3: "agent-3", 4: "agent-4"}
    fleet = FakeFleet(placed)
    monkeypatch.setattr(go, "ssh_run", fleet)
    assert go.main(_args(tmp_path, cfg, hosts)) == 0
    rec = _record(tmp_path)
    assert rec["members"] == [3, 4] and rec["outcome"] == "ok"
    assert rec["rejoined_at"] - rec["killed_at"] >= 60 and rec["held_s"] >= 59
    assert sorted(fleet.restarts) == [3, 4]
    restart = [s for h, s in fleet.scripts if not s.startswith("pkill")][0]
    assert "SWARM_RUN_ID=run01-abc" in restart and ("main.py 3" in restart or "main.py 4" in restart)


def test_stopped_during_the_outage_restarts_nobody(cell, monkeypatch):
    tmp_path, cfg, hosts, clock = cell
    fleet = FakeFleet({3: "agent-3", 4: "agent-4"})
    monkeypatch.setattr(go, "ssh_run", fleet)

    def stop(s):
        clock[0] += 10
        raise go._Stop()
    monkeypatch.setattr(go.time, "sleep", stop)
    assert go.main(_args(tmp_path, cfg, hosts)) == 3
    rec = _record(tmp_path)
    assert rec["stopped_early"] and rec["outcome"] == "held_short"
    assert fleet.restarts == []


def test_a_kill_that_did_not_take_fails(cell, monkeypatch):
    tmp_path, cfg, hosts, clock = cell
    monkeypatch.setattr(go, "ssh_run", FakeFleet({3: "agent-3", 4: "agent-4"}, leaky=(4,)))
    assert go.main(_args(tmp_path, cfg, hosts)) == 1
    assert _record(tmp_path)["kill"]["4"]["verdict"] == "ALIVE"


def test_a_rejoin_that_did_not_take_fails(cell, monkeypatch):
    tmp_path, cfg, hosts, clock = cell
    monkeypatch.setattr(go, "ssh_run", FakeFleet({3: "agent-3", 4: "agent-4"}, dead_on_restart=(3,)))
    assert go.main(_args(tmp_path, cfg, hosts)) == 1
    assert _record(tmp_path)["rejoin_check"]["3"]["verdict"] == "DOWN"


def test_no_rejoin_leaves_the_group_down_and_names_it(cell, monkeypatch):
    tmp_path, cfg, hosts, clock = cell
    fleet = FakeFleet({3: "agent-3", 4: "agent-4"})
    monkeypatch.setattr(go, "ssh_run", fleet)
    assert go.main(_args(tmp_path, cfg, hosts, no_rejoin=True)) == 0
    rec = _record(tmp_path)
    assert fleet.restarts == [] and rec["expect_silent_agents"] == "3,4"


def test_a_rejoin_without_a_run_id_does_not_start(cell, monkeypatch):
    tmp_path, cfg, hosts, clock = cell
    fleet = FakeFleet({3: "agent-3", 4: "agent-4"})
    monkeypatch.setattr(go, "ssh_run", fleet)
    argv = _args(tmp_path, cfg, hosts)
    (tmp_path / "run01" / "run_meta.json").unlink()
    assert go.main(argv) == 3
    assert _record(tmp_path)["outcome"] == "no_run_id"
    assert fleet.scripts == []


def test_a_run_that_never_started(cell, monkeypatch):
    tmp_path, cfg, hosts, clock = cell
    monkeypatch.setattr(go, "ssh_run", FakeFleet({3: "agent-3", 4: "agent-4"}))
    monkeypatch.setattr(go, "wait_for_log", lambda *a: False)
    assert go.main(_args(tmp_path, cfg, hosts, start_after_log=str(tmp_path / "x.log"))) == 2
    assert _record(tmp_path)["outcome"] == "run_never_started"


def test_a_member_whose_kill_failed_is_never_restarted(cell, monkeypatch):
    """Stop-time review: restarting an agent that survived its kill makes two agents with one id."""
    tmp_path, cfg, hosts, clock = cell
    fleet = FakeFleet({3: "agent-3", 4: "agent-4"}, leaky=(4,))
    monkeypatch.setattr(go, "ssh_run", fleet)
    assert go.main(_args(tmp_path, cfg, hosts)) == 1
    rec = _record(tmp_path)
    assert fleet.restarts == [3]
    assert rec["rejoin_skipped_alive"] == [4] and "4" not in rec["rejoin_check"]


def test_the_restart_script_refuses_a_running_agent():
    script = go.restart_script(5, "resource", "run-x")
    assert script.startswith("if pgrep -f") and "echo ALREADY" in script
    assert script.index("echo ALREADY") < script.index("nohup")


def test_an_already_running_agent_is_not_a_successful_rejoin(cell, monkeypatch):
    tmp_path, cfg, hosts, clock = cell
    fleet = FakeFleet({3: "agent-3", 4: "agent-4"})
    real_call = fleet.__call__

    def revive_before_restart(host, script, timeout=60.0):
        if not script.startswith("pkill"):
            fleet.up[int(re.search(r"main\\?\.py (\d+)", script).group(1))] = True
        return real_call(host, script, timeout)
    monkeypatch.setattr(go, "ssh_run", revive_before_restart)
    assert go.main(_args(tmp_path, cfg, hosts)) == 1
    assert {v["verdict"] for v in _record(tmp_path)["rejoin_check"].values()} == {"ALREADY"}
    assert fleet.restarts == []


def test_the_pattern_never_matches_the_shell_running_our_own_scripts():
    """Stop-time review: the restart script launches `python3.11 main.py <id>` literally, so the
    remote `bash -c` running it matched an unanchored pattern — the guard reported ALREADY for
    every agent and the post-launch check reported UP whether or not the agent started."""
    pat = re.compile(go._pattern(5))
    for script in (go.restart_script(5, "resource", "run-x"), go.kill_script(5)):
        for shell in ("bash -c ", "sh -c ", "/bin/bash -c "):
            assert not pat.search(shell + script), shell + script[:40]
    assert pat.search("python3.11 main.py 5 --agent-type resource")
    assert pat.search("/usr/bin/python3.11 main.py 5")
    assert not pat.search("python3.11 main.py 50 --agent-type resource")
