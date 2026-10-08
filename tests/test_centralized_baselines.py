"""Centralized E7 baselines (Greedy / Round-Robin / Random), fixed 2026-10-07.

Each test pins one defect the plan's E7 section listed, every one of which made the
centralized rows weaker than centralization alone would:

* the remote worker ran one job at a time while the scheduler admitted several per agent;
* at Hier-N, coordinator slots became executors;
* Round-Robin and Random did not reserve inside a batch, so an agent could be over-committed;
* `runtime.wall_time_*` and `executor_workers` were not read from the config;
* the workflow DAG was ignored;
* `metrics.json` was not keyed by agent (the collector read `agent_job_counts` as an agent);
* the batch script tested `tee`'s exit status.
"""
import json
import os
import subprocess
import sys
import threading
import time

import fakeredis
import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from baselines.agent_sim import SimulatedAgent  # noqa: E402
from baselines.baseline_worker import DispatchWorker  # noqa: E402
from baselines.common import cost_params, executor_workers, load_config  # noqa: E402
from baselines.scheduler import (GreedyScheduler, RandomScheduler,  # noqa: E402
                                 RoundRobinScheduler)
from baselines.sparrow import Keys  # noqa: E402
from swarm.database.repository import Repository  # noqa: E402
from swarm.models.capacities import Capacities  # noqa: E402
from swarm.models.data_node import DataNode  # noqa: E402
from swarm.models.job import Job, ObjectState  # noqa: E402

RUN = "bl-test"
COMPLETE = ObjectState.COMPLETE.value


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
    return SimulatedAgent(agent_id=i, capacities=Capacities(core=core, ram=16, disk=100),
                          dtns={})


def _job(job_id, core=1, wall=0.01, files_in=None, files_out=None):
    j = Job()
    j.job_id = job_id
    j.wall_time = wall
    j.capacities = Capacities(core=core, ram=1, disk=1)
    if files_in:
        j.data_predicate = {"kind": "files", "files": list(files_in)}
    for name in files_out or []:
        j.add_outgoing_data_dep(DataNode(name="local", file=name))
    j.state = ObjectState.PENDING
    return j


def _put(repo, *jobs):
    for j in jobs:
        repo.save(obj=j.to_dict(), key_prefix=Repository.KEY_JOB, level=0, group=0)


def _sched(cls, r, tmp_path, agents, total, remote=False, workers=10, timeout=20.0):
    s = cls(db_host="x", db_port=0, agent_profiles_path="", jobs_dir="", jobs_per_interval=1,
            run_dir=str(tmp_path / "run"), total_jobs=total, executor_workers=workers,
            timeout=timeout, remote=remote, run_id=RUN, redis_client=r)
    s.agents = list(agents)
    s._executed = {a.agent_id: [] for a in s.agents}
    return s


def _record(repo, job_id):
    j = Job()
    j.from_dict(repo.get(job_id, key_prefix=Repository.KEY_JOB, level=0, group=0))
    return j


# --------------------------------------------------------------------------- fleet

def test_coordinator_slots_never_execute(tmp_path):
    prof = {str(i): {"level": 0, "core": 4, "ram": 8, "disk": 10} for i in range(1, 4)}
    prof["4"] = {"level": 1, "core": 4, "ram": 8, "disk": 10}
    path = tmp_path / "p.json"
    path.write_text(json.dumps(prof))
    s = GreedyScheduler(db_host="x", db_port=0, agent_profiles_path=str(path), jobs_dir="",
                        jobs_per_interval=1, run_dir=str(tmp_path / "r"), total_jobs=1,
                        agents=4)
    s.load_agents()
    assert [a.agent_id for a in s.agents] == [1, 2, 3]
    s.fleet_size = None                 # without a fleet size: every level-0 profile
    s.load_agents()
    assert [a.agent_id for a in s.agents] == [1, 2, 3]


# --------------------------------------------------------------------------- batches

@pytest.mark.parametrize("cls", [GreedyScheduler, RoundRobinScheduler, RandomScheduler])
def test_a_batch_never_overcommits_an_agent(cls, tmp_path):
    """One 2-core agent, three 1-core jobs in one batch: two fit, the third waits."""
    r = fakeredis.FakeStrictRedis(decode_responses=True)
    s = _sched(cls, r, tmp_path, [_agent(1, core=2)], 3)
    picked = s.assign_jobs([_job("a"), _job("b"), _job("c")])
    assert len(picked) == 2
    assert s.agents[0].can_fit(Capacities(core=1, ram=1, disk=1)) is False


def test_a_refused_assignment_frees_its_reservation_without_counting_a_completion(env,
                                                                                    tmp_path):
    r, repo = env
    s = _sched(GreedyScheduler, r, tmp_path, [_agent(1, core=1)], 1, remote=True)
    s.repo = repo
    job = _job("x")
    _put(repo, job)
    stale = _job("x")
    stale.state = ObjectState.READY                   # someone else moved it
    _put(repo, stale)
    s._reserve(s.agents[0], job)
    assert s._commit(job, s.agents[0], {}) == "refused"
    assert s.agents[0].can_fit(job.capacities) and s.agents[0].jobs_completed == 0
    assert r.llen(s.keys.queue(1)) == 0


# --------------------------------------------------------------------------- config

def test_cost_and_execution_come_from_the_base_config(tmp_path):
    cfg = tmp_path / "c.yml"
    cfg.write_text("job_selection:\n  cost_weights: {cpu: 0.7, ram: 0.1, disk: 0.1, gpu: 0.1}\n"
                   "  long_job_threshold: 9.0\nruntime:\n  executor_workers: 4\n")
    conf = load_config(str(cfg))
    params = cost_params(conf)
    assert params["cost_weights"]["cpu"] == 0.7 and params["long_job_threshold"] == 9.0
    assert params["connectivity_penalty_factor"] == 1.0
    assert executor_workers(conf["runtime"]) == 4


def test_a_cli_override_wins_over_the_config(tmp_path):
    from baselines.run_baseline_remote import parse_args, resolve_cost
    cfg = tmp_path / "c.yml"
    cfg.write_text("job_selection:\n  cost_weights: {cpu: 0.7}\n")
    args = parse_args(["--scheduler", "greedy", "--agents", "1", "--jobs", "1", "--db-host",
                       "x", "--agent-hosts-file", "h", "--run-dir", "r", "--cpu-weight", "0.2",
                       "--config", str(cfg)])
    assert resolve_cost(args, load_config(str(cfg)))["cost_weights"]["cpu"] == 0.2
    assert args.remote_config == "/root/SwarmAgents/c.yml"


# --------------------------------------------------------------------------- local runs

def _run_local(s, timeout=30):
    t = threading.Thread(target=s.run, kwargs={"start_distributor": False}, daemon=True)
    t.start()
    t.join(timeout)
    assert not t.is_alive(), "the scheduler did not finish"


def test_an_agent_runs_executor_workers_jobs_at_once(env, tmp_path):
    r, repo = env
    _put(repo, *[_job(f"j{i}", wall=0.3) for i in range(3)])
    s = _sched(GreedyScheduler, r, tmp_path, [_agent(1, core=4)], 3, workers=3)
    _run_local(s)
    starts = sorted(_record(repo, f"j{i}").started_at for i in range(3))
    assert starts[-1] - starts[0] < 0.25, "jobs on one agent ran one after another"


def test_a_dag_runs_in_order_and_outputs_are_published(env, tmp_path):
    r, repo = env
    _put(repo, _job("child", files_in=["p.out"]), _job("parent", wall=0.2, files_out=["p.out"]))
    s = _sched(RoundRobinScheduler, r, tmp_path, [_agent(1), _agent(2)], 2)
    _run_local(s)
    parent, child = _record(repo, "parent"), _record(repo, "child")
    assert child.started_at >= parent.completed_at
    assert repo.data_available(["p.out"])
    assert s.stats["gated"] == 1


def test_results_are_keyed_by_agent_and_the_collector_sees_no_phantom(env, tmp_path):
    from evaluation.collect import run_metrics
    r, repo = env
    _put(repo, *[_job(f"j{i}") for i in range(6)])
    agents = [_agent(1), _agent(2), _agent(3)]
    s = _sched(RandomScheduler, r, tmp_path, agents, 6)
    _run_local(s)
    assert s.save_results() == 0
    run = tmp_path / "run"
    metrics = json.loads((run / "metrics.json").read_text())
    assert sorted(metrics) == ["1", "2", "3"]
    assert sum(len(m["executed_jobs"]) for m in metrics.values()) == 6
    assert json.loads((run / "collect_meta.json").read_text()) == {"arm": "baseline",
                                                                    "policy": "random"}
    assert json.loads((run / "drain.json").read_text())["status"] == "all_terminal"
    assert len(json.loads((run / "all_agents.csv").read_text())) == 3
    row = run_metrics(run, expected_jobs=6)
    assert row["completion_pct"] == 100.0
    assert row["jobs_executed"] == 6 and row["jobs_executed_twice"] == 0


def test_a_run_that_hits_its_timeout_says_so(env, tmp_path):
    r, repo = env
    _put(repo, _job("big", core=99))                  # fits nowhere
    s = _sched(GreedyScheduler, r, tmp_path, [_agent(1)], 1, timeout=1.0)
    _run_local(s)
    assert s.drain["status"] == "timer" and s.drain["completed_at_stop"] == 0


# --------------------------------------------------------------------------- remote path

def test_remote_dispatch_runs_concurrently_and_exactly_once(env, tmp_path):
    r, repo = env
    _put(repo, *[_job(f"j{i}", wall=0.3) for i in range(8)])
    agents = [_agent(1, core=4), _agent(2, core=4)]
    s = _sched(GreedyScheduler, r, tmp_path, agents, 8, remote=True)
    keys = Keys(RUN, prefix="baseline")
    workers = [DispatchWorker(a.agent_id, repo, r, keys, max_concurrent=4) for a in agents]
    stop = threading.Event()
    threads = [threading.Thread(target=w.run_forever, args=(stop, 0), daemon=True)
               for w in workers]
    for t in threads:
        t.start()
    _run_local(s)
    stop.set()
    for t in threads:
        t.join(timeout=5)
    for w in workers:
        w.shutdown()
        w.write_stats()
    assert s.drain["status"] == "all_terminal"
    executed = [j for w in workers for j in w.executed_jobs]
    assert len(executed) == len(set(executed)) == 8
    by_agent = {}
    for i in range(8):
        rec = _record(repo, f"j{i}")
        by_agent.setdefault(rec.leader_id, []).append(rec.started_at)
    widest = max(by_agent.values(), key=len)
    assert len(widest) >= 2 and max(widest) - min(widest) < 0.25, \
        "the worker ran its jobs one at a time"
    assert sorted(s.worker_stats()) == [1, 2]


def test_a_worker_never_runs_a_job_it_was_not_given(env):
    r, repo = env
    job = _job("x")
    job.state = ObjectState.READY
    job.leader_id = 2
    _put(repo, job)
    keys = Keys(RUN, prefix="baseline")
    w = DispatchWorker(1, repo, r, keys)
    r.rpush(keys.queue(1), "x")
    assert w.serve_one(block_s=0) == "noop"
    assert w.executed_jobs == []


def test_a_hosts_file_too_short_for_the_fleet_is_refused(tmp_path):
    from baselines.run_baseline_remote import main
    (tmp_path / "hosts").write_text("agent-1\n")
    rc = main(["--scheduler", "greedy", "--agents", "4", "--jobs", "1", "--db-host", "x",
               "--run-dir", str(tmp_path / "run"), "--agent-hosts-file",
               str(tmp_path / "hosts"), "--skip-preflight"])
    assert rc == 2


def test_sigterm_becomes_the_orchestrators_teardown():
    import signal as _signal
    from baselines.run_baseline_remote import _exit_on_signal
    with pytest.raises(SystemExit) as exc:
        _exit_on_signal(_signal.SIGTERM, None)
    assert exc.value.code == 128 + _signal.SIGTERM


# --------------------------------------------------------------------------- batch script

def test_the_batch_script_reports_the_runners_exit_not_tees(tmp_path):
    """A runner that exits 3 must be counted FAILED. `if runner | tee` tested tee."""
    fake_py = tmp_path / "py"
    fake_py.write_text("#!/bin/bash\n"
                       "if [[ \"$1\" == -c ]]; then exit 0; fi\n"   # the Redis probe
                       "echo running; exit 3\n")
    fake_py.chmod(0o755)
    proc = subprocess.run(
        ["bash", os.path.join(REPO, "run_centralized_baselines.sh"), "--schedulers", "greedy",
         "--runs", "1", "--python", str(fake_py), "--base-dir", str(tmp_path / "b")],
        capture_output=True, text=True, cwd=REPO, timeout=60)
    assert "FAILED (exit 3" in proc.stdout, proc.stdout + proc.stderr
    assert "Failed:       1" in proc.stdout


# --------------------------------------------------------------------------- campaign

def test_a_killed_centralized_cell_stops_its_workers(monkeypatch):
    import campaign
    from campaign import cell_config
    calls = []
    monkeypatch.setattr(campaign.subprocess, "call", lambda cmd, **k: calls.append(cmd) or 0)
    cfg = cell_config({}, {"name": "g", "runner": "baselines/run_baseline_remote.py",
                           "args": {"mode": "local"}})
    campaign.stop_cell_agents(cfg)
    assert calls[-1] == ["pkill", "-TERM", "-f", "baseline_worker.py"]


# --------------------------------------------------------------------------- run_test archive

def test_run_test_keeps_the_fleet_profiles_an_external_baseline_needs(tmp_path, monkeypatch):
    import argparse
    import run_test
    monkeypatch.chdir(tmp_path)
    (tmp_path / "agent_profiles.json").write_text('{"1": {"level": 0}}')
    run = tmp_path / "run"
    run.mkdir()
    run_test.archive_fleet_profiles(argparse.Namespace(run_dir=str(run), use_config_dir=None))
    assert json.loads((run / "agent_profiles.json").read_text()) == {"1": {"level": 0}}
    other = tmp_path / "run2"
    other.mkdir()
    run_test.archive_fleet_profiles(argparse.Namespace(run_dir=str(other),
                                                       use_config_dir="configs"))
    assert not (other / "agent_profiles.json").exists(), \
        "a reused config dir's root profiles may be stale and must not be archived"


# --------------------------------------------------------------------------- stop-gate review

def test_a_duplicate_dispatch_runs_the_job_once(env):
    """The RUNNING write is a READY -> RUNNING claim; a second delivery of the same assignment,
    or one arriving after completion, executes nothing."""
    from baselines.common import run_job
    r, repo = env
    job = _job("x", wall=0.2)
    job.state = ObjectState.READY
    job.leader_id = 1
    _put(repo, job)
    keys = Keys(RUN, prefix="baseline")
    w = DispatchWorker(1, repo, r, keys, max_concurrent=4)
    r.rpush(keys.queue(1), "x", "x")
    assert w.serve_one(block_s=0) == "submitted"
    assert w.serve_one(block_s=0) in ("submitted", "noop")
    w.shutdown()
    assert w.executed_jobs == ["x"]
    assert _record(repo, "x").state == ObjectState.COMPLETE
    late = Job()
    late.from_dict(repo.get("x", key_prefix=Repository.KEY_JOB, level=0, group=0))
    assert run_job(repo, late, 1) is None, "a completed job ran again"


def test_a_periodic_snapshot_is_not_a_final_report(env):
    from baselines.common import keyed_metrics, read_reports, wait_for_final_reports
    r, repo = env
    keys = Keys(RUN, prefix="baseline")
    w1, w2 = DispatchWorker(1, repo, r, keys), DispatchWorker(2, repo, r, keys)
    w1.executed_jobs.append("a")
    w1.write_stats()                      # periodic
    w2.write_stats(final=True)
    stats_keys = {1: keys.stats("worker", 1), 2: keys.stats("worker", 2)}
    t0 = time.time()
    got = wait_for_final_reports(r, stats_keys, timeout=1.5)
    assert sorted(got) == [2] and time.time() - t0 >= 1.0, "stopped waiting on a snapshot"
    metrics, missing = keyed_metrics([1, 2], read_reports(r, stats_keys), RUN, "baseline")
    assert missing == [1]
    assert metrics["1"]["executed_jobs"] == ["a"], "a stale snapshot is still a lower bound"


class _DeadRedis:
    def __getattr__(self, name):
        def fail(*a, **k):
            import redis as _r
            raise _r.ConnectionError("down")
        return fail


def test_results_are_written_and_exit_4_when_the_store_is_gone(tmp_path):
    s = _sched(GreedyScheduler, _DeadRedis(), tmp_path, [_agent(1)], 1, remote=True)
    s.repo = Repository(_DeadRedis(), run_id=RUN)
    s.drain = {"status": "all_terminal"}
    assert s.save_results() == 4
    run = tmp_path / "run"
    assert json.loads((run / "drain.json").read_text())["status"] == "all_terminal"
    assert (run / "metrics_shortfall.json").exists() and (run / "run_meta.json").exists()


def _raising_once(fn, write_first):
    """Wrap repo.save: on the first call, optionally perform the write, then raise as if the
    EXEC reply was lost."""
    import redis as _r
    state = {"n": 0}

    def wrapped(*a, **k):
        state["n"] += 1
        if state["n"] == 1:
            if write_first:
                fn(*a, **k)
            raise _r.ConnectionError("reply lost")
        return fn(*a, **k)
    return wrapped


def test_a_lost_commit_reply_is_reconciled_and_dispatched(env, tmp_path, monkeypatch):
    r, repo = env
    s = _sched(GreedyScheduler, r, tmp_path, [_agent(1, core=1)], 1, remote=True)
    s.repo = repo
    job = _job("x")
    _put(repo, job)
    s._reserve(s.agents[0], job)
    monkeypatch.setattr(repo, "save", _raising_once(repo.save, write_first=True))
    assert s._commit(job, s.agents[0], {}) == "uncertain"
    assert not s.agents[0].can_fit(job.capacities), "an unconfirmed write keeps its reservation"
    waiting, seen = {}, {"x"}
    s._reconcile(waiting, seen, {})
    assert s.assigned_count == 1 and not waiting
    assert r.lrange(s.keys.queue(1), 0, -1) == ["x"], "the landed assignment was never dispatched"


def test_an_assignment_that_never_landed_returns_to_waiting(env, tmp_path, monkeypatch):
    r, repo = env
    s = _sched(GreedyScheduler, r, tmp_path, [_agent(1, core=1)], 1, remote=True)
    s.repo = repo
    job = _job("x")
    _put(repo, job)
    s._reserve(s.agents[0], job)
    monkeypatch.setattr(repo, "save", _raising_once(repo.save, write_first=False))
    assert s._commit(job, s.agents[0], {}) == "uncertain"
    waiting, seen = {}, {"x"}
    s._reconcile(waiting, seen, {})
    assert "x" in waiting and "x" not in seen
    assert s.agents[0].can_fit(job.capacities) and s.assigned_count == 0


def test_an_undeliverable_dispatch_is_kept_and_fails_the_run(env, tmp_path, monkeypatch):
    import redis as _r
    r, repo = env
    _put(repo, _job("x"))
    s = _sched(GreedyScheduler, r, tmp_path, [_agent(1)], 1, remote=True, timeout=1.5)
    real_rpush = r.rpush
    monkeypatch.setattr(r, "rpush", lambda *a, **k: (_ for _ in ()).throw(
        _r.ConnectionError("down")))
    _run_local(s)
    assert s.drain["undispatched_at_stop"] == 1 and s.exit_code == 4
    # Once the store recovers, the owed dispatch is delivered rather than forgotten.
    monkeypatch.setattr(r, "rpush", real_rpush)
    s._flush_dispatches()
    assert r.lrange(s.keys.queue(1), 0, -1) == ["x"]


def test_a_lost_exec_reply_retried_as_watch_error_is_still_dispatched(env, tmp_path,
                                                                      monkeypatch):
    """Pipeline level: EXEC commits, then the client raises WatchError (what a dropped
    connection looks like while watching). Repository.save retries, finds READY and returns
    False — which must not be read as a refusal."""
    import redis as _r
    r, repo = env
    s = _sched(GreedyScheduler, r, tmp_path, [_agent(1, core=1)], 1, remote=True)
    s.repo = repo
    job = _job("x")
    _put(repo, job)
    real_pipeline = r.pipeline
    state = {"n": 0}

    def pipeline(*a, **k):
        pipe = real_pipeline(*a, **k)
        real_execute = pipe.execute

        def execute(*ea, **ek):
            out = real_execute(*ea, **ek)
            state["n"] += 1
            if state["n"] == 1:
                raise _r.WatchError("connection lost after EXEC")
            return out
        pipe.execute = execute
        return pipe

    monkeypatch.setattr(r, "pipeline", pipeline)
    s._reserve(s.agents[0], job)
    assert s._commit(job, s.agents[0], {}) == "ok"
    monkeypatch.setattr(r, "pipeline", real_pipeline)
    assert _record(repo, "x").state == ObjectState.READY
    assert r.lrange(s.keys.queue(1), 0, -1) == ["x"]
    assert s.assigned_count == 1 and s.stats["assign_reply_lost"] == 1


def test_an_older_assignment_to_the_same_agent_is_not_mistaken_for_ours(env, tmp_path):
    r, repo = env
    s = _sched(GreedyScheduler, r, tmp_path, [_agent(1, core=1)], 1, remote=True)
    s.repo = repo
    old = _job("x")
    old.state = ObjectState.READY
    old.leader_id = 1
    old.mark_assigned(ts=1000.0)
    _put(repo, old)
    job = _job("x")
    s._reserve(s.agents[0], job)
    assert s._commit(job, s.agents[0], {}) == "refused"
    assert r.llen(s.keys.queue(1)) == 0 and s.agents[0].can_fit(job.capacities)
