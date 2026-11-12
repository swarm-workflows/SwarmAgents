"""E7 under failure: the baselines requeue a dead worker's jobs, and the outage tool can take
their workers down and bring them back (2026-11-12).

Before this, a killed baseline worker's READY/RUNNING jobs stayed assigned to it forever and the
run never drained. Recovery is opt-in (`--requeue-dead-workers`), so the no-failure E7 path is
unchanged, and it waits SWARM's own detector window before acting.
"""
import json
import os
import re
import sys
import threading
import time

import fakeredis
import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

import group_outage as go  # noqa: E402
from baselines.agent_sim import SimulatedAgent  # noqa: E402
from baselines.baseline_worker import DispatchWorker  # noqa: E402
from baselines.common import WorkerLiveness, failure_window_s, requeue_jobs_of  # noqa: E402
from baselines.scheduler import GreedyScheduler  # noqa: E402
from baselines.sparrow import Keys, SparrowScheduler, owner_of  # noqa: E402
from swarm.database.repository import Repository  # noqa: E402
from swarm.models.capacities import Capacities  # noqa: E402
from swarm.models.job import Job, ObjectState  # noqa: E402

RUN = "run-e7f"


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


def _put(repo, job_id, state=ObjectState.PENDING, leader=None, wall=0.01):
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


# --------------------------------------------------------------------------- the pieces

def test_the_window_is_swarms_detector_window():
    assert failure_window_s({}) == pytest.approx(67.0)
    assert failure_window_s({"failure_threshold_seconds": 30}) == pytest.approx(34.0)


def test_a_worker_is_dead_only_after_the_window_and_back_on_its_first_heartbeat():
    now = [0.0]
    lv = WorkerLiveness([1, 2], 67.0, clock=lambda: now[0])
    now[0] = 10.0
    assert lv.observe({1}) == ([], [])
    now[0] = 66.0
    assert lv.observe({1}) == ([], []), "2 last seen at 0: 66 s is inside the window"
    now[0] = 68.0
    assert lv.observe({1}) == ([2], [])
    assert lv.observe({1}) == ([], []), "declared once"
    now[0] = 200.0
    assert lv.observe({1, 2}) == ([], [2])
    assert lv.dead == set()


def test_requeue_returns_only_the_dead_workers_ready_and_running_jobs(env):
    r, repo = env
    _put(repo, "a", ObjectState.READY, 3)
    _put(repo, "b", ObjectState.RUNNING, 3)
    _put(repo, "c", ObjectState.RUNNING, 4)
    _put(repo, "d", ObjectState.COMPLETE, 3)
    _put(repo, "e", ObjectState.PENDING)
    assert sorted(requeue_jobs_of(repo, {3})) == ["a", "b"]
    for jid in ("a", "b"):
        rec = _rec(repo, jid)
        assert rec["state"] == ObjectState.PENDING.value and rec.get("leader_id") is None
    assert _rec(repo, "c")["state"] == ObjectState.RUNNING.value
    assert _rec(repo, "d")["state"] == ObjectState.COMPLETE.value
    pending = repo.get_all_ids_multi(key_prefix=Repository.KEY_JOB, level=0, group=0,
                                     states=[ObjectState.PENDING.value])[ObjectState.PENDING.value]
    assert sorted(pending) == ["a", "b", "e"], "the state index moved with the record"


def test_requeue_respects_a_partition(env):
    r, repo = env
    for i in range(12):
        _put(repo, f"j{i}", ObjectState.RUNNING, 3)
    mine = [f"j{i}" for i in range(12) if owner_of(f"j{i}", 2) == 0]
    got = requeue_jobs_of(repo, {3}, owns=lambda j: owner_of(j, 2) == 0)
    assert sorted(got) == sorted(mine) and 0 < len(got) < 12


# --------------------------------------------------------------------------- centralized

def test_a_dead_workers_jobs_are_requeued_and_run_elsewhere(env, tmp_path):
    """Worker 1 takes jobs and dies (never runs them, heartbeat stops); after the window the
    scheduler hands them to worker 2. Every job completes, once."""
    r, repo = env
    for i in range(6):
        _put(repo, f"j{i}", wall=0.05)
    agents = [_agent(1, core=4), _agent(2, core=2)]
    s = GreedyScheduler(db_host="x", db_port=0, agent_profiles_path="", jobs_dir="",
                        jobs_per_interval=1, run_dir=str(tmp_path / "run"), total_jobs=6,
                        executor_workers=4, timeout=30.0, remote=True, run_id=RUN,
                        redis_client=r, failure_window_s=1.0)
    s.agents = agents
    keys = Keys(RUN, prefix="baseline")
    w2 = DispatchWorker(2, repo, r, keys, max_concurrent=2)
    stop = threading.Event()
    t = threading.Thread(target=w2.run_forever, args=(stop, 0), daemon=True)
    t.start()
    r.set(keys.heartbeat(1), "1", ex=1)      # worker 1 registered, then silent: it is dead
    s.run(start_distributor=False)
    stop.set()
    t.join(timeout=5)
    w2.shutdown()
    assert s.drain["status"] == "all_terminal"
    assert s.stats.get("workers_failed") == 1 and s.stats.get("jobs_requeued", 0) >= 1
    assert all(_rec(repo, f"j{i}")["state"] == ObjectState.COMPLETE.value for i in range(6))
    assert sorted(w2.executed_jobs) == [f"j{i}" for i in range(6)]
    assert s.drain["inflight_at_stop"] == 0, "a requeued job is not counted in flight twice"
    assert s.failure_events and s.failure_events[0]["dead"] == [1]


def test_without_the_flag_nothing_is_requeued(env, tmp_path):
    """The no-failure E7 path is unchanged: no window, no liveness check, no requeue."""
    r, repo = env
    _put(repo, "j0")
    s = GreedyScheduler(db_host="x", db_port=0, agent_profiles_path="", jobs_dir="",
                        jobs_per_interval=1, run_dir=str(tmp_path / "run"), total_jobs=1,
                        executor_workers=1, timeout=1.5, remote=True, run_id=RUN,
                        redis_client=r)
    s.agents = [_agent(1)]
    s.run(start_distributor=False)
    assert "workers_failed" not in s.stats and "jobs_requeued" not in s.stats
    assert _rec(repo, "j0")["state"] == ObjectState.READY.value


def test_a_dead_agent_is_never_feasible():
    a = _agent(1)
    j = Job()
    j.capacities = Capacities(core=1, ram=1, disk=1)
    assert GreedyScheduler.is_feasible(j, a)
    a.alive = False
    assert not GreedyScheduler.is_feasible(j, a)


# --------------------------------------------------------------------------- sparrow

def test_sparrow_requeues_a_dead_workers_claims_in_its_partition(env):
    r, repo = env
    keys = Keys(RUN)
    now = [0.0]
    fleet = [_agent(1), _agent(2)]
    r.set(keys.heartbeat(2), "1")
    jobs = [f"k{i}" for i in range(8)]
    for jid in jobs:
        _put(repo, jid, ObjectState.RUNNING, 1)
    s = SparrowScheduler(0, 2, repo, r, keys, fleet, clock=lambda: now[0], failure_window_s=67)
    s.tick()
    assert "jobs_requeued" not in s.stats
    now[0] = 70.0
    s.tick()
    mine = [j for j in jobs if owner_of(j, 2) == 0]
    assert s.stats["workers_failed"] == 1 and s.stats["jobs_requeued"] == len(mine)
    for j in jobs:
        want = ObjectState.PENDING.value if j in mine else ObjectState.RUNNING.value
        assert _rec(repo, j)["state"] == want


def test_a_requeued_sparrow_job_keeps_its_selection_start(env):
    r, repo = env
    keys = Keys(RUN)
    r.set(keys.heartbeat(2), "1")
    j = _put(repo, "x")
    j.mark_selection_started(123.0)
    repo.save(obj=j.to_dict(), key_prefix=Repository.KEY_JOB, level=0, group=0)
    s = SparrowScheduler(0, 1, repo, r, keys, [_agent(2)], probe_ratio=1)
    assert s.tick() == 1
    rec = Job()
    rec.from_dict(_rec(repo, "x"))
    assert rec.selection_started_at == pytest.approx(123.0)


# --------------------------------------------------------------------------- outage tool

def test_the_baseline_patterns_match_only_their_own_worker():
    b = re.compile(go._baseline_pattern(5, "baseline"))
    assert b.search("python3.11 baselines/baseline_worker.py --agent-id 5 --db-host d --run-id R")
    assert not b.search("python3.11 baselines/baseline_worker.py --agent-id 50 --db-host d")
    assert not b.search("bash -c pgrep -f python3.11 baselines/baseline_worker.py --agent-id 5")
    sp = re.compile(go._baseline_pattern(5, "sparrow"))
    assert sp.search("python3.11 baselines/sparrow_node.py worker --db-host d --run-id R "
                     "--config c --agent-id 5")
    assert not sp.search("python3.11 baselines/sparrow_node.py scheduler --index 5 --of 9")
    assert not sp.search("python3.11 baselines/sparrow_node.py worker --agent-id 51")


class FakeWorkers:
    def __init__(self, placed):
        self.placed, self.up, self.restarts = placed, {a: True for a in placed}, []

    def __call__(self, host, script, timeout=60.0):
        aid = int(re.search(r"--agent-id (\d+)", script).group(1))
        assert self.placed[aid] == host
        if script.startswith("pkill"):
            self.up[aid] = False
            return 0, "GONE"
        if self.up[aid]:
            return 0, "ALREADY"
        assert "baseline-worker-start.sh" in script, "the worker's own launch command is replayed"
        self.restarts.append(aid)
        self.up[aid] = True
        return 0, "UP"


def _outage_args(tmp_path, **kw):
    hf = tmp_path / "hosts.txt"
    hf.write_text("\n".join(f"agent-{i}" for i in range(1, 7)))
    argv = ["run", "--hosts-file", str(hf), "--agents", "6", "--group", "1",
            "--target", "baseline", "--duration", "60", "--run-dir", str(tmp_path / "run01"),
            "--out", str(tmp_path / "outage.json")]
    for k, v in kw.items():
        argv += [f"--{k.replace('_', '-')}", str(v)]
    return argv


@pytest.fixture
def clock(monkeypatch):
    c = [1000.0]
    monkeypatch.setattr(go.time, "time", lambda: c[0])
    monkeypatch.setattr(go.time, "sleep", lambda s: c.__setitem__(0, c[0] + s))
    return c


def _launch(tmp_path, ids):
    run = tmp_path / "run01"
    run.mkdir(exist_ok=True)
    (run / "worker_launch.json").write_text(json.dumps({"run_id": "baseline-greedy-1", "workers": {
        str(a): {"host": f"agent-{a}",
                 "cmd": f"cd /root/SwarmAgents && bash baseline-worker-start.sh python3.11 {a} "
                        f"--run-id baseline-greedy-1"} for a in ids}}))


def test_a_baseline_outage_kills_and_restarts_the_named_workers(tmp_path, clock, monkeypatch):
    _launch(tmp_path, [1, 2, 3, 4])
    fleet = FakeWorkers({3: "agent-3", 4: "agent-4"})
    monkeypatch.setattr(go, "ssh_run", fleet)
    assert go.main(_outage_args(tmp_path, members="3,4")) == 0
    rec = json.loads((tmp_path / "outage.json").read_text())
    assert rec["target"] == "baseline" and rec["members"] == [3, 4] and rec["outcome"] == "ok"
    assert sorted(fleet.restarts) == [3, 4]


def test_a_baseline_outage_needs_explicit_members(tmp_path, clock):
    _launch(tmp_path, [1, 2])
    with pytest.raises(SystemExit, match="--members"):
        go.main(_outage_args(tmp_path))


def test_a_baseline_outage_without_a_launch_record_does_not_start(tmp_path, clock, monkeypatch):
    (tmp_path / "run01").mkdir()
    fleet = FakeWorkers({3: "agent-3"})
    monkeypatch.setattr(go, "ssh_run", fleet)
    assert go.main(_outage_args(tmp_path, members="3")) == 3
    assert json.loads((tmp_path / "outage.json").read_text())["outcome"] == \
        "no_worker_launch_record"
    assert all(fleet.up.values()), "nothing was killed"
