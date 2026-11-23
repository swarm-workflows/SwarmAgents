"""E7c: the scheduler itself goes down and comes back (2026-11-17).

Central baselines: the scheduler loop stops for `duration_s`, loses every in-memory structure and
restarts from the store, while arrivals continue and workers keep running what they hold.
Sparrow: one scheduler process is killed and restarted by the outage tool from its launch record.
SWARM: one coordinator is killed by the outage tool with explicit `--members`.

Also the race fixed in the outage tool the same day: group members are read after the run has
started, not when the companion starts (the first frozen E7-under-failure PBFT run read the
previous cell's configs and killed the wrong 15 agents).
"""
import json
import os
import re
import sys
import threading
import time

import fakeredis
import pytest
import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

import group_outage as go  # noqa: E402
from baselines.agent_sim import SimulatedAgent  # noqa: E402
from baselines.baseline_worker import DispatchWorker  # noqa: E402
from baselines.scheduler import GreedyScheduler, RoundRobinScheduler  # noqa: E402
from baselines.sparrow import Keys  # noqa: E402
from swarm.database.repository import Repository  # noqa: E402
from swarm.models.capacities import Capacities  # noqa: E402
from swarm.models.job import Job, ObjectState  # noqa: E402

RUN = "run-e7c"


@pytest.fixture(autouse=True)
def fast_jobs():
    saved = (Job._WALL_TIME_SCALE, Job._WALL_TIME_MIN_S, Job._WALL_TIME_MAX_S)
    Job.configure_execution_simulation(scale=1.0, min_s=0.0, max_s=0.3)
    yield
    Job.configure_execution_simulation(scale=saved[0], min_s=saved[1], max_s=saved[2])


@pytest.fixture
def env():
    r = fakeredis.FakeStrictRedis(decode_responses=True)
    return r, Repository(r, run_id=RUN)


def _agent(i, core=4):
    return SimulatedAgent(agent_id=i, capacities=Capacities(core=core, ram=16, disk=100), dtns={})


def _put(repo, job_id, state=ObjectState.PENDING, leader=None, wall=0.05):
    j = Job()
    j.job_id = job_id
    j.wall_time = wall
    j.capacities = Capacities(core=1, ram=1, disk=1)
    j.state = state
    j.leader_id = leader
    repo.save(obj=j.to_dict(), key_prefix=Repository.KEY_JOB, level=0, group=0)
    return j


def _rec(repo, job_id):
    return repo.get(job_id, key_prefix=Repository.KEY_JOB, level=0, group=0)


def _assigned_at(rec):
    v = rec.get("assigned_at")
    vals = list(v.values()) if isinstance(v, dict) else [v]
    return max(float(x) for x in vals if x is not None)


def _sched(cls, r, tmp_path, total, outage=None, timeout=30.0, agents=None):
    s = cls(db_host="x", db_port=0, agent_profiles_path="", jobs_dir="", jobs_per_interval=1,
            run_dir=str(tmp_path / "run"), total_jobs=total, executor_workers=4,
            timeout=timeout, remote=True, run_id=RUN, redis_client=r, outage=outage)
    s.agents = agents or [_agent(1), _agent(2)]
    return s


def _workers(r, repo, ids):
    keys = Keys(RUN, prefix="baseline")
    stop = threading.Event()
    ws = [DispatchWorker(i, repo, r, keys, max_concurrent=4) for i in ids]
    ts = [threading.Thread(target=w.run_forever, args=(stop, 0), daemon=True) for w in ws]
    for t in ts:
        t.start()

    def done():
        stop.set()
        for t in ts:
            t.join(timeout=5)
        for w in ws:
            w.shutdown()
    return ws, done


# --------------------------------------------------------------------------- central scheduler

def test_nothing_is_placed_while_the_scheduler_is_down_and_everything_runs_once(env, tmp_path):
    """Jobs arriving during the outage wait for the restart; every job completes exactly once."""
    r, repo = env
    for i in range(4):
        _put(repo, f"a{i}")
    s = _sched(GreedyScheduler, r, tmp_path, total=8, outage={"at_s": 0.4, "duration_s": 1.2})
    ws, done = _workers(r, repo, [1, 2])

    def late_arrivals():
        time.sleep(0.9)                       # inside the outage
        for i in range(4):
            _put(repo, f"b{i}")
    t = threading.Thread(target=late_arrivals, daemon=True)
    t.start()
    s.run(start_distributor=False)
    t.join()
    done()

    out = json.loads((tmp_path / "run" / "outage.json").read_text())
    assert out["target"] == "central-scheduler" and out["outcome"] == "ok"
    assert out["held_s"] >= 1.2 - 0.05
    assert s.drain["status"] == "all_terminal" and s.drain["scheduler_outage"] == "ok"
    for i in range(4):
        assert _assigned_at(_rec(repo, f"b{i}")) >= out["rejoined_at"], \
            "a job that arrived while the scheduler was down was placed before it came back"
    executed = sorted(j for w in ws for j in w.executed_jobs)
    assert executed == sorted([f"a{i}" for i in range(4)] + [f"b{i}" for i in range(4)])
    assert s.drain["inflight_at_stop"] == 0


def test_the_restart_rebuilds_reservations_from_the_store(env, tmp_path):
    r, repo = env
    _put(repo, "r1", ObjectState.READY, leader=1)
    _put(repo, "r2", ObjectState.RUNNING, leader=1)
    _put(repo, "r3", ObjectState.RUNNING, leader=2)
    _put(repo, "c1", ObjectState.COMPLETE, leader=2)
    _put(repo, "p1")
    s = _sched(GreedyScheduler, r, tmp_path, total=5)
    s.repo = repo
    s.agents[0].current_alloc = Capacities(core=3, ram=3, disk=3)   # stale first-life view
    s._undispatched["gone"] = 2
    waiting, seen = {"x": Job()}, {"y"}
    rebuilt = s._restart_from_store(waiting, seen)
    assert rebuilt == {"pending": 1, "ready": 1, "running": 2, "complete": 1, "redispatched": 1}
    assert s.agents[0].current_alloc.core == 2 and s.agents[1].current_alloc.core == 1
    assert s._undispatched == {"r1": 1}, "only READY jobs are owed a dispatch; stale ones go"
    assert s._released == {"c1"} and waiting == {} and seen == {"r1", "r2", "r3", "c1"}
    assert s.assigned_count == 4


def test_a_job_that_finished_during_the_rebuild_is_never_reserved(env, tmp_path, monkeypatch):
    """Listed RUNNING by the index read, COMPLETE by the record read: released, not reserved,
    or the next completion poll would free capacity that was never taken."""
    r, repo = env
    _put(repo, "j", ObjectState.RUNNING, leader=1)
    s = _sched(GreedyScheduler, r, tmp_path, total=1)
    s.repo = repo
    real = repo.get_many

    def finishing(ids, **kw):
        _put(repo, "j", ObjectState.COMPLETE, leader=1)
        return real(ids, **kw)
    monkeypatch.setattr(repo, "get_many", finishing)
    s._restart_from_store({}, set())
    assert s.agents[0].current_alloc.core == 0 and "j" in s._released


def test_a_redispatched_job_already_queued_runs_once(env, tmp_path):
    """The restart pushes every READY job again; a copy the worker also holds is refused by its
    READY -> RUNNING claim, never executed twice."""
    r, repo = env
    keys = Keys(RUN, prefix="baseline")
    _put(repo, "q", ObjectState.READY, leader=1)
    r.rpush(keys.queue(1), "q")                       # dispatched in the first life
    s = _sched(GreedyScheduler, r, tmp_path, total=1, agents=[_agent(1)])
    s.repo = repo
    s._restart_from_store({}, set())
    s._flush_dispatches()
    assert r.lrange(keys.queue(1), 0, -1) == ["q", "q"]
    ws, done = _workers(r, repo, [1])
    deadline = time.time() + 5
    while _rec(repo, "q")["state"] != ObjectState.COMPLETE.value and time.time() < deadline:
        time.sleep(0.05)
    time.sleep(0.3)
    done()
    assert ws[0].executed_jobs == ["q"]


def test_round_robin_restarts_its_pointer(env, tmp_path):
    r, repo = env
    s = _sched(RoundRobinScheduler, r, tmp_path, total=1)
    s.repo = repo
    s._rr_index = 7
    s._restart_from_store({}, set())
    assert s._rr_index == 0


def test_an_outage_cut_short_by_the_timeout_is_not_ok(env, tmp_path):
    r, repo = env
    _put(repo, "j")
    s = _sched(GreedyScheduler, r, tmp_path, total=2, timeout=1.0,
               outage={"at_s": 0.2, "duration_s": 5.0})
    s.run(start_distributor=False)
    out = json.loads((tmp_path / "run" / "outage.json").read_text())
    assert out["outcome"] == "held_short" and "rejoined_at" not in out
    assert s.drain["status"] == "timer" and s.drain["scheduler_outage"] == "held_short"


def test_no_outage_means_no_record_and_the_e7_path_unchanged(env, tmp_path):
    r, repo = env
    _put(repo, "j")
    s = _sched(GreedyScheduler, r, tmp_path, total=1, timeout=1.0)
    s.run(start_distributor=False)
    assert not (tmp_path / "run" / "outage.json").exists()
    assert "scheduler_outage" not in s.drain and "scheduler_outages" not in s.stats


def test_a_scheduler_outage_needs_remote_mode(tmp_path):
    with pytest.raises(ValueError, match="remote"):
        GreedyScheduler(db_host="x", db_port=0, agent_profiles_path="", jobs_dir="",
                        jobs_per_interval=1, run_dir=str(tmp_path), total_jobs=1,
                        outage={"at_s": 1, "duration_s": 1})


def test_the_runner_takes_both_outage_flags_or_neither():
    from baselines.run_baseline_remote import parse_args
    base = ["--scheduler", "greedy", "--agents", "9", "--jobs", "1", "--db-host", "d",
            "--agent-hosts-file", "h", "--run-dir", "r", "--stats-wait-s", "1"]
    a = parse_args(base + ["--scheduler-outage-at", "120", "--scheduler-outage-duration", "180"])
    assert (a.scheduler_outage_at, a.scheduler_outage_duration) == (120.0, 180.0)
    with pytest.raises(SystemExit):
        parse_args(base + ["--scheduler-outage-at", "120"])


# --------------------------------------------------------------------------- outage tool

@pytest.fixture
def clock(monkeypatch):
    c = [1000.0]
    monkeypatch.setattr(go.time, "time", lambda: c[0])
    monkeypatch.setattr(go.time, "sleep", lambda s: c.__setitem__(0, c[0] + s))
    return c


def test_the_sparrow_scheduler_pattern_matches_only_its_own_index():
    p = re.compile(go._baseline_pattern(3, "sparrow-scheduler"))
    assert p.search("python3.11 baselines/sparrow_node.py scheduler --db-host d --run-id R "
                    "--config c --index 3 --of 9 --probe-ratio 2")
    assert not p.search("python3.11 baselines/sparrow_node.py scheduler --index 30 --of 90")
    assert not p.search("python3.11 baselines/sparrow_node.py worker --agent-id 3")
    assert not p.search("bash -c pgrep -f python3.11 baselines/sparrow_node.py scheduler --index 3")


def test_a_sparrow_scheduler_outage_restarts_it_from_its_launch_record(tmp_path, clock,
                                                                       monkeypatch):
    run = tmp_path / "run01"
    run.mkdir()
    cmd = ("cd /root/SwarmAgents && bash sparrow-node-start.sh python3.11 scheduler 3 "
           "--db-host d --run-id sparrow-1 --index 3 --of 9")
    (run / "worker_launch.json").write_text(json.dumps({
        "run_id": "sparrow-1", "workers": {"3": {"host": "agent-3", "cmd": "worker"}},
        "schedulers": {"3": {"host": "agent-31", "cmd": cmd}}}))
    calls = []
    up = {"agent-31": True}

    def fake(host, script, timeout=60.0):
        calls.append((host, script))
        assert host == "agent-31", "the scheduler's own host, not worker 3's"
        if script.startswith("pkill"):
            assert "scheduler .* --index 3" in script
            up[host] = False
            return 0, "GONE"
        if up[host]:
            return 0, "ALREADY"
        assert "sparrow-node-start.sh python3.11 scheduler 3" in script
        up[host] = True
        return 0, "UP"
    monkeypatch.setattr(go, "ssh_run", fake)
    hf = tmp_path / "hosts.txt"
    hf.write_text("\n".join(f"agent-{i}" for i in range(1, 91)))
    rc = go.main(["run", "--hosts-file", str(hf), "--agents", "90", "--group", "3",
                  "--target", "sparrow-scheduler", "--members", "3", "--duration", "180",
                  "--run-dir", str(run), "--out", str(tmp_path / "outage.json")])
    rec = json.loads((tmp_path / "outage.json").read_text())
    assert rc == 0 and rec["outcome"] == "ok" and rec["hosts"] == {"3": "agent-31"}
    assert rec["held_s"] == 180.0 and len(calls) == 2


def _write_configs(d, groups):
    d.mkdir(parents=True, exist_ok=True)
    for f in d.glob("*.yml"):
        f.unlink()
    for g, ids in groups.items():
        for i in ids:
            (d / f"config_swarm_multi_{i}.yml").write_text(
                yaml.safe_dump({"topology": {"level": 0, "group": g}}))


def test_group_members_are_read_after_the_run_starts(tmp_path, clock, monkeypatch):
    """configs/ holds the PREVIOUS cell's layout when the companion starts; the run regenerates
    it before distribution. The outage must take the new layout's group."""
    cfg = tmp_path / "configs"
    _write_configs(cfg, {3: [16, 17, 18, 31, 32]})            # previous cell's group 3
    log = tmp_path / "run01.log"
    log.write_text("")

    def run_starts(path, marker, timeout_s):
        _write_configs(cfg, {3: [28, 29, 30]})                 # run_test.py regenerates
        return True
    monkeypatch.setattr(go, "wait_for_log", run_starts)
    killed = []

    def fake(host, script, timeout=60.0):
        aid = int(re.search(r"main\\.py (\d+)", script).group(1))
        if script.startswith("pkill"):
            killed.append(aid)
            return 0, "GONE"
        return 0, "UP"
    monkeypatch.setattr(go, "ssh_run", fake)
    hf = tmp_path / "hosts.txt"
    hf.write_text("\n".join(f"agent-{i}" for i in range(1, 91)))
    rc = go.main(["run", "--hosts-file", str(hf), "--agents", "90", "--group", "3",
                  "--config-dir", str(cfg), "--duration", "60", "--run-id", "R",
                  "--start-after-log", str(log), "--out", str(tmp_path / "outage.json")])
    rec = json.loads((tmp_path / "outage.json").read_text())
    assert rc == 0 and sorted(killed) == [28, 29, 30] and rec["members"] == [28, 29, 30]
    assert "members_read_at" in rec


def test_a_coordinator_can_be_named_explicitly(tmp_path, clock, monkeypatch):
    """E7c SWARM: one coordinator (Hier-90 G=1: agent 85 is group 3's parent) by --members."""
    started = []

    def fake(host, script, timeout=60.0):
        if script.startswith("pkill"):
            return 0, "GONE"
        started.append(script)
        return 0, "UP"
    monkeypatch.setattr(go, "ssh_run", fake)
    hf = tmp_path / "hosts.txt"
    hf.write_text("\n".join(f"agent-{i}" for i in range(1, 91)))
    rc = go.main(["run", "--hosts-file", str(hf), "--agents", "90", "--group", "3",
                  "--members", "85", "--duration", "180", "--run-id", "R",
                  "--out", str(tmp_path / "outage.json")])
    rec = json.loads((tmp_path / "outage.json").read_text())
    assert rc == 0 and rec["members"] == [85] and rec["hosts"] == {"85": "agent-85"}
    assert len(started) == 1 and "main.py 85 --agent-type resource" in started[0]
    assert "SWARM_RUN_ID=R" in started[0]
