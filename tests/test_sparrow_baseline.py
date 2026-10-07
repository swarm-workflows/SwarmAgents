"""Sparrow-style baseline (batch sampling + late binding), against an in-memory Redis.

Pins the properties that make it Sparrow rather than another centralized baseline:

* each job is owned by exactly one of several independent schedulers;
* a scheduler sends `probe_ratio` reservations to distinct, live workers that could ever run
  the job (capacity and DTNs), and never looks at their load;
* a worker late-binds: the first claim wins the job, every later reservation is a no-op;
* a job still unclaimed after `reprobe_s` is probed again;
* a workflow DAG is honoured with SWARM's own readiness registry, and outputs are published
  only on success, in the completion write;
* end to end, every job runs exactly once.
"""
import json
import os
import sys
import threading
import time

import fakeredis
import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from baselines.agent_sim import SimulatedAgent  # noqa: E402
from baselines.sparrow import (Keys, SparrowScheduler, SparrowWorker, load_fleet,  # noqa: E402
                               owner_of, publish_fleet)
from swarm.database.repository import Repository  # noqa: E402
from swarm.models.capacities import Capacities  # noqa: E402
from swarm.models.data_node import DataNode  # noqa: E402
from swarm.models.job import Job, ObjectState  # noqa: E402

RUN = "sp-test"


@pytest.fixture(autouse=True)
def fast_jobs():
    saved = (Job._WALL_TIME_SCALE, Job._WALL_TIME_MIN_S, Job._WALL_TIME_MAX_S)
    Job.configure_execution_simulation(scale=1.0, min_s=0.0, max_s=0.05)
    yield
    Job.configure_execution_simulation(scale=saved[0], min_s=saved[1], max_s=saved[2])


@pytest.fixture
def env():
    r = fakeredis.FakeStrictRedis(decode_responses=True)
    return r, Repository(r, run_id=RUN), Keys(RUN)


def _agent(i, core=4, ram=8, disk=50, dtns=()):
    return SimulatedAgent(agent_id=i, capacities=Capacities(core=core, ram=ram, disk=disk),
                          dtns={d: DataNode(name=d) for d in dtns})


def _job(repo, job_id, core=1, files_in=None, files_out=None, wall=0.01):
    j = Job()
    j.job_id = job_id
    j.wall_time = wall
    j.capacities = Capacities(core=core, ram=1, disk=1)
    if files_in:
        j.data_predicate = {"kind": "files", "files": list(files_in)}
    for name in files_out or []:
        j.add_outgoing_data_dep(DataNode(name="local", file=name))
    j.state = ObjectState.PENDING
    repo.save(obj=j.to_dict(), key_prefix=Repository.KEY_JOB, level=0, group=0)
    return j


def _alive(r, keys, ids):
    for i in ids:
        r.set(keys.heartbeat(i), "1")


def _record(repo, job_id):
    return repo.get(job_id, key_prefix=Repository.KEY_JOB, level=0, group=0)


def _queue(r, keys, w):
    return [json.loads(x)["job"] for x in r.lrange(keys.queue(w), 0, -1)]


# --------------------------------------------------------------------------- scheduler

def test_every_job_has_exactly_one_owning_scheduler():
    owners = [owner_of(f"job-{i}", 3) for i in range(300)]
    assert set(owners) == {0, 1, 2}
    assert owners == [owner_of(f"job-{i}", 3) for i in range(300)], "stable across calls"


def test_probes_go_to_distinct_live_feasible_workers(env):
    r, repo, keys = env
    fleet = [_agent(1), _agent(2), _agent(3, core=0), _agent(4)]     # 3 can never run it
    _alive(r, keys, [1, 2, 3])                                       # 4 is not alive
    _job(repo, "j1")
    s = SparrowScheduler(0, 1, repo, r, keys, fleet, probe_ratio=2, seed=1)
    assert s.tick() == 2
    probed = [w for w in (1, 2, 3, 4) if _queue(r, keys, w) == ["j1"]]
    assert sorted(probed) == [1, 2]
    assert _record(repo, "j1")["state"] == ObjectState.PENDING.value
    stamped = Job(); stamped.from_dict(_record(repo, "j1"))
    assert stamped.selection_started_at, "selection is stamped once, at the first probe"


def test_a_dtn_the_worker_lacks_rules_it_out(env):
    r, repo, keys = env
    fleet = [_agent(1, dtns=("dtn1",)), _agent(2)]
    _alive(r, keys, [1, 2])
    j = Job()
    j.job_id = "j1"; j.wall_time = 0.01
    j.capacities = Capacities(core=1, ram=1, disk=1)
    j.add_incoming_data_dep(DataNode(name="dtn1", file="x"))
    j.state = ObjectState.PENDING
    repo.save(obj=j.to_dict(), key_prefix=Repository.KEY_JOB, level=0, group=0)
    SparrowScheduler(0, 1, repo, r, keys, fleet, probe_ratio=2, seed=1).tick()
    assert _queue(r, keys, 1) == ["j1"] and _queue(r, keys, 2) == []


def test_a_scheduler_probes_only_its_own_partition(env):
    r, repo, keys = env
    fleet = [_agent(1), _agent(2)]
    _alive(r, keys, [1, 2])
    ids = [f"j{i}" for i in range(20)]
    for i in ids:
        _job(repo, i)
    s0 = SparrowScheduler(0, 2, repo, r, keys, fleet, probe_ratio=1, seed=1)
    s0.tick()
    assert set(s0.probed) == {i for i in ids if owner_of(i, 2) == 0}


def test_an_unclaimed_job_is_probed_again_after_the_timeout(env):
    r, repo, keys = env
    fleet = [_agent(1), _agent(2)]
    _alive(r, keys, [1, 2])
    _job(repo, "j1")
    now = [100.0]
    s = SparrowScheduler(0, 1, repo, r, keys, fleet, probe_ratio=1, reprobe_s=30,
                         seed=1, clock=lambda: now[0])
    assert s.tick() == 1
    now[0] = 120.0
    assert s.tick() == 0, "not yet"
    now[0] = 131.0
    assert s.tick() == 1
    assert s.stats["reprobes"] == 1 and s.stats["jobs_probed"] == 1


def test_no_live_worker_sends_nothing_and_is_counted(env):
    r, repo, keys = env
    _job(repo, "j1")
    s = SparrowScheduler(0, 1, repo, r, keys, [_agent(1)], seed=1)
    assert s.tick() == 0
    assert s.stats["no_live_worker"] == 1


def test_a_gated_job_is_not_probed_until_its_inputs_exist(env):
    r, repo, keys = env
    _alive(r, keys, [1])
    _job(repo, "child", files_in=["a.csv"])
    s = SparrowScheduler(0, 1, repo, r, keys, [_agent(1)], seed=1)
    assert s.tick() == 0 and s.stats["gated"] == 1
    repo.mark_data_available(["a.csv"])
    assert s.tick() == 1


# --------------------------------------------------------------------------- worker

def test_the_first_claim_wins_and_the_other_reservation_is_a_noop(env):
    r, repo, keys = env
    fleet = [_agent(1), _agent(2)]
    _alive(r, keys, [1, 2])
    _job(repo, "j1")
    SparrowScheduler(0, 1, repo, r, keys, fleet, probe_ratio=2, seed=1).tick()
    w1 = SparrowWorker(fleet[0], repo, r, keys)
    w2 = SparrowWorker(fleet[1], repo, r, keys)
    assert w1.serve_one(block_s=0) == "claimed"
    assert w2.serve_one(block_s=0) == "noop"
    w1.shutdown()
    rec = _record(repo, "j1")
    assert rec["leader_id"] == 1
    assert rec["state"] == ObjectState.COMPLETE.value
    assert w1.executed_jobs == ["j1"] and w2.executed_jobs == []


def test_a_claim_that_loses_the_race_does_not_run_the_job(env):
    r, repo, keys = env
    a = _agent(1)
    _job(repo, "j1")
    r.rpush(keys.queue(1), json.dumps({"job": "j1", "sched": 0, "at": 0}))
    w = SparrowWorker(a, repo, r, keys)
    stale = _record(repo, "j1")
    # Another worker claims between this worker's read and its CAS.
    j = Job(); j.from_dict(stale); j.state = ObjectState.READY; j.leader_id = 2
    real_get = repo.get
    repo.get = lambda *a_, **k: (repo.save(obj=j.to_dict(), key_prefix=Repository.KEY_JOB,
                                           level=0, group=0), stale)[1]
    assert w.serve_one(block_s=0) == "lost"
    repo.get = real_get
    assert _record(repo, "j1")["leader_id"] == 2
    assert w.executed_jobs == []


def test_a_job_that_does_not_fit_now_keeps_its_place_at_the_head(env):
    r, repo, keys = env
    a = _agent(1, core=2)
    a.allocate(Capacities(core=2))            # busy
    _job(repo, "big", core=2)
    _job(repo, "small", core=1)
    for jid in ("big", "small"):
        r.rpush(keys.queue(1), json.dumps({"job": jid, "sched": 0, "at": 0}))
    w = SparrowWorker(a, repo, r, keys)
    assert w.serve_one(block_s=0) == "wait"
    assert _queue(r, keys, 1) == ["big", "small"], "order kept"


def test_outputs_are_published_only_on_success_and_in_the_completion_write(env):
    r, repo, keys = env
    a = _agent(1)
    ok = _job(repo, "ok", files_out=["ok.out"])
    bad = _job(repo, "bad", files_out=["bad.out"])
    j = Job(); j.from_dict(_record(repo, "bad")); j._should_fail = True
    repo.save(obj=j.to_dict(), key_prefix=Repository.KEY_JOB, level=0, group=0)
    for jid in ("ok", "bad"):
        r.rpush(keys.queue(1), json.dumps({"job": jid, "sched": 0, "at": 0}))
    w = SparrowWorker(a, repo, r, keys)
    w.serve_one(block_s=0); w.serve_one(block_s=0)
    w.shutdown()
    assert repo.data_available(["ok.out"])
    assert not repo.data_available(["bad.out"])
    assert _record(repo, "bad")["exit_status"] == 1


def test_the_fleet_round_trips_through_the_store(env):
    r, repo, keys = env
    publish_fleet(r, keys, [_agent(3, core=8, dtns=("dtn2",))])
    (a,) = load_fleet(r, keys)
    assert a.agent_id == 3 and a.capacities.core == 8 and set(a.dtns) == {"dtn2"}


# --------------------------------------------------------------------------- end to end

def test_every_job_runs_exactly_once_and_a_dag_runs_in_order(env):
    r, repo, keys = env
    fleet = [_agent(i, core=2) for i in range(1, 7)]
    _alive(r, keys, [a.agent_id for a in fleet])
    for i in range(30):
        _job(repo, f"j{i}")
    _job(repo, "parent", files_out=["p.out"])
    _job(repo, "child", files_in=["p.out"])

    schedulers = [SparrowScheduler(i, 2, repo, r, keys, fleet, probe_ratio=2, seed=7)
                  for i in range(2)]
    workers = [SparrowWorker(a, repo, r, keys, max_concurrent=2) for a in fleet]
    stop = threading.Event()
    threads = [threading.Thread(target=w.run_forever, args=(stop, 0), daemon=True)
               for w in workers]
    for t in threads:
        t.start()
    deadline = time.time() + 20
    done = set()
    while time.time() < deadline:
        for s in schedulers:
            s.tick()
        ids = repo.get_all_ids_multi(key_prefix=Repository.KEY_JOB, level=0, group=0,
                                     states=[ObjectState.COMPLETE.value])
        done = set(ids.get(ObjectState.COMPLETE.value, []))
        if len(done) == 32:
            break
        time.sleep(0.02)
    stop.set()
    for t in threads:
        t.join(timeout=5)
    for w in workers:
        w.shutdown()
    assert len(done) == 32
    executed = [j for w in workers for j in w.executed_jobs]
    assert len(executed) == len(set(executed)) == 32, "a job ran twice or not at all"
    parent, child = Job(), Job()
    parent.from_dict(_record(repo, "parent")); child.from_dict(_record(repo, "child"))
    assert child.started_at >= parent.completed_at, "the child ran before its parent finished"
    assert sum(w.stats["noops"] for w in workers) >= 1, "probe_ratio 2 must leave no-ops"


# --------------------------------------------------------------------------- orchestrator

def test_schedulers_are_spread_over_the_fleet():
    from baselines.run_sparrow import scheduler_agents
    assert scheduler_agents(90, 9) == [1, 11, 21, 31, 41, 51, 61, 71, 81]
    assert scheduler_agents(6, 1) == [1]


def test_the_fleet_is_level_0_agents_only(tmp_path):
    from baselines.run_sparrow import load_fleet_profiles
    prof = {str(i): {"level": 0, "core": 4, "ram": 8, "disk": 10} for i in range(1, 4)}
    prof["4"] = {"level": 1, "core": 4, "ram": 8, "disk": 10}      # a coordinator slot
    path = tmp_path / "p.json"
    path.write_text(json.dumps(prof))
    assert [a.agent_id for a in load_fleet_profiles(str(path), 4)] == [1, 2, 3]
    with pytest.raises(SystemExit):
        load_fleet_profiles(str(path), 5)


def test_a_hosts_file_too_short_for_the_fleet_is_refused(tmp_path):
    from baselines.run_sparrow import main
    prof = {str(i): {"level": 0, "core": 4, "ram": 8, "disk": 10} for i in range(1, 5)}
    (tmp_path / "p.json").write_text(json.dumps(prof))
    (tmp_path / "jobs").mkdir()
    (tmp_path / "hosts").write_text("agent-1\n")
    rc = main(["--mode", "remote", "--agents", "4", "--jobs", "1", "--db-host", "x",
               "--run-dir", str(tmp_path / "run"), "--agent-hosts-file",
               str(tmp_path / "hosts"), "--use-profiles", str(tmp_path / "p.json"),
               "--use-jobs-dir", str(tmp_path / "jobs")])
    assert rc == 2


def _free_port():
    import socket
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_a_local_run_end_to_end_writes_what_the_collector_reads(tmp_path):
    """Separate processes for every node, against a TCP Redis, then the collector."""
    from fakeredis import TcpFakeServer
    from baselines.run_sparrow import main
    from evaluation.collect import run_metrics

    port = _free_port()
    server = TcpFakeServer(("127.0.0.1", port), server_type="redis")
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        prof = {str(i): {"level": 0, "core": 4, "ram": 16, "disk": 100} for i in range(1, 4)}
        (tmp_path / "p.json").write_text(json.dumps(prof))
        jobs = tmp_path / "jobs"
        jobs.mkdir()
        for i in range(1, 13):
            j = Job()
            j.job_id = str(i)
            j.wall_time = 0.2
            j.capacities = Capacities(core=1, ram=1, disk=1)
            (jobs / f"job_{i}.json").write_text(json.dumps(j.to_dict()))
        cfg = tmp_path / "cfg.yml"
        cfg.write_text("runtime:\n  wall_time_max_s: 1\n  executor_workers: 2\n")
        run = tmp_path / "sparrow" / "run01"
        rc = main(["--mode", "local", "--agents", "3", "--jobs", "12", "--db-host", "127.0.0.1",
                   "--db-port", str(port), "--run-dir", str(run), "--schedulers", "2",
                   "--probe-ratio", "2", "--use-profiles", str(tmp_path / "p.json"),
                   "--use-jobs-dir", str(jobs), "--config", str(cfg), "--timeout", "90",
                   "--jobs-per-interval", "6", "--stats-wait-s", "20"])
        assert rc == 0
        assert json.loads((run / "drain.json").read_text())["status"] == "all_terminal"
        row = run_metrics(run, expected_jobs=12)
        assert row["completion_pct"] == 100.0
        assert row["jobs_executed"] == 12 and row["jobs_executed_twice"] == 0
        assert row["metrics_complete"] is True
    finally:
        server.shutdown()
        server.server_close()
