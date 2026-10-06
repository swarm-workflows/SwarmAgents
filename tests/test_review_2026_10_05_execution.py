"""Code review 2026-10-05 §61-§63: real execution, before any real-workflow run.

§61 A refusal (inputs not stageable, no runtime, an unrunnable spec) was persisted as exit 1
    and its reason dropped, so it read exactly like the workflow failing.
§62 A docker job's timeout killed the `docker run` client, not the container, which dockerd
    owns; and jobs run in their own sessions, so none died with the agent after its drain.
§63 "Produced by this run" was tested against the LOCATION hash: a name in the readiness set
    with no location fell through to the inputs root and read a stale same-named file.
"""
import csv
import os
import subprocess
import sys
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from swarm.execution import runner, staging  # noqa: E402
from swarm.models.job import Job  # noqa: E402


@pytest.fixture(autouse=True)
def _clean_policy():
    runner.configure()
    staging.configure()
    staging.set_context()
    yield
    runner.configure()
    staging.configure()
    staging.set_context()


class _Node:
    def __init__(self, file):
        self.file = file
        self.name = "local"


# --------------------------------------------------------------------------- §63
class TestProducedWithoutLocation:
    def _setup(self, tmp_path):
        inputs_root = str(tmp_path / "inputs")
        os.makedirs(inputs_root)
        with open(os.path.join(inputs_root, "out.csv"), "wb") as f:
            f.write(b"last week's file")
        work = str(tmp_path / "work")
        os.makedirs(work)
        staging.configure(enabled=True)
        runner.configure(mode="real", roots={"inputs": inputs_root})
        return work

    def test_a_produced_name_with_no_location_is_refused(self, tmp_path):
        work = self._setup(tmp_path)
        staging.set_context(produced=lambda names: {"out.csv"} & set(names))
        staged, refusal = runner.stage_inputs([_Node("out.csv")], work,
                                              locator=lambda names: {}, run_id="r1")
        assert staged == []
        assert "produced by this run but has no recorded location" in refusal
        assert not os.path.exists(os.path.join(work, "out.csv"))

    def test_a_root_input_nobody_produced_still_resolves(self, tmp_path):
        work = self._setup(tmp_path)
        staging.set_context(produced=lambda names: set())
        staged, refusal = runner.stage_inputs([_Node("out.csv")], work,
                                              locator=lambda names: {}, run_id="r1")
        assert refusal == "" and staged == ["out.csv"]

    def test_a_failed_produced_lookup_refuses(self, tmp_path):
        work = self._setup(tmp_path)

        def broken(names):
            raise ConnectionError("redis down")
        staging.set_context(produced=broken)
        _staged, refusal = runner.stage_inputs([_Node("out.csv")], work,
                                               locator=lambda names: {}, run_id="r1")
        assert "produced-names lookup failed" in refusal

    def test_staging_off_is_unchanged(self, tmp_path):
        work = self._setup(tmp_path)
        staging.configure(enabled=False)
        staging.set_context(produced=lambda names: {"out.csv"})
        _staged, refusal = runner.stage_inputs([_Node("out.csv")], work)
        assert refusal == ""

    def test_the_agent_installs_the_query(self):
        src = open(os.path.join(REPO, "swarm/agents/agent_grpc.py")).read()
        assert "produced=self.repository.produced_names" in src


# --------------------------------------------------------------------------- §62
class TestContainersCanBeKilled:
    def test_docker_run_gets_a_name_and_init(self):
        cmd, name = runner._name_container(
            ["docker", "run", "--rm", "-v", "/w:/w", "--entrypoint", "/x", "img"], "job/1", "run-9")
        assert cmd[:5] == ["docker", "run", "--name", name, "--init"]
        assert cmd[5:] == ["--rm", "-v", "/w:/w", "--entrypoint", "/x", "img"]
        assert name.startswith("swarm-run-9-job_1-")

    def test_other_runtimes_are_untouched(self):
        for cmd in (["apptainer", "exec", "x.sif", "/x"], ["/bin/true"]):
            assert runner._name_container(cmd, "j", "r") == (cmd, None)

    def test_a_timeout_kills_the_container_too(self, monkeypatch):
        calls = []
        monkeypatch.setattr(runner.subprocess, "run",
                            lambda cmd, **k: calls.append(cmd) or MagicMock(returncode=0))
        proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
        runner._kill_group(proc, "swarm-r-j-1")
        assert ["docker", "kill", "swarm-r-j-1"] in calls
        assert proc.poll() is not None

    def test_terminate_all_kills_what_is_still_running(self, monkeypatch):
        monkeypatch.setattr(runner.subprocess, "run", lambda cmd, **k: MagicMock(returncode=0))
        proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
        runner._register(proc, None)
        try:
            assert runner.terminate_all() >= 1
            assert proc.poll() is not None
        finally:
            runner._unregister(proc)

    def test_a_finished_job_leaves_the_registry(self, tmp_path):
        from swarm.models.execution import ExecutionSpec
        script = tmp_path / "ok.sh"
        script.write_text("#!/bin/sh\nexit 0\n")
        script.chmod(0o755)
        runner.configure(mode="real", work_dir=str(tmp_path / "w"))
        spec = ExecutionSpec(path=str(script), arguments=[], pfn=str(script))
        res = runner.run(spec, "j1")
        assert res.exit_status == 0, res.reason
        assert runner._LIVE == {}

    def test_the_agent_drain_calls_terminate_all(self):
        src = open(os.path.join(REPO, "swarm/agents/resource_agent.py")).read()
        body = src[src.index("    def _drain_executor"):src.index("    def _save_results_safely")]
        assert "runner.terminate_all()" in body


# --------------------------------------------------------------------------- §61
class TestRefusalsAreRecorded:
    def test_a_refusal_reason_round_trips(self):
        j = Job()
        j.job_id = "j1"
        j._refusal_reason = "input 'x' could not be staged"
        back = Job()
        back.from_dict(j.to_dict())
        assert back.refusal_reason == "input 'x' could not be staged"

    def test_no_refusal_is_none(self):
        back = Job()
        back.from_dict(Job().to_dict() | {"id": "j2"})
        assert back.refusal_reason is None

    def test_a_refused_execution_sets_it(self, monkeypatch):
        j = Job()
        j.job_id = "j1"
        j._execution = MagicMock()
        monkeypatch.setattr(runner, "run", lambda *a, **k: runner.ExecutionResult(
            exit_status=1, refused=True, reason="no container runtime"))
        assert j._execute_real() == 1
        assert j.refusal_reason == "no container runtime"

    def test_the_export_and_collector_carry_it(self, tmp_path):
        from plotting.data import save_jobs
        from evaluation.collect import run_metrics
        jobs = []
        for jid, reason, status in (("a", None, 0), ("b", "refused: no runtime", 1), ("c", None, 2)):
            j = Job()
            j.job_id = jid
            j.leader_id = 1
            j.state = j.state.__class__.COMPLETE
            j.mark_submitted()
            j.mark_completed()
            j.exit_status = status
            j._refusal_reason = reason
            jobs.append(j)
        run_dir = tmp_path / "mesh-3" / "run01"
        run_dir.mkdir(parents=True)
        save_jobs(jobs, str(run_dir))
        with open(run_dir / "all_jobs.csv") as f:
            rows = list(csv.reader(f))
        assert rows[0][-1] == "refused"
        assert all(len(r) == len(rows[0]) for r in rows)
        m = run_metrics(run_dir, expected_jobs=3)
        assert m["exit_failures"] == 2
        assert m["exec_refusals"] == 1


# --------------------------------------------------------------------------- §61 retry
class TestRefusalClassification:
    """Transient refusals may succeed on a retry; configuration ones would refuse anywhere."""

    def _staged(self, tmp_path, **ctx):
        work = str(tmp_path / "work")
        os.makedirs(work, exist_ok=True)
        staging.configure(enabled=True, timeout_s=0.5)
        runner.configure(mode="real", roots={"inputs": str(tmp_path / "inputs")})
        staging.set_context(**ctx)
        return work

    def test_an_unreachable_producer_is_transient(self, tmp_path):
        work = self._staged(tmp_path)
        locs = {"x.csv": [{"agent_id": 9, "host": "127.0.0.1", "port": 1}]}
        _s, refusal = runner.stage_inputs([_Node("x.csv")], work,
                                          locator=lambda n: locs, run_id="r1")
        assert isinstance(refusal, runner.TransientRefusal)

    def test_a_failed_lookup_is_transient(self, tmp_path):
        work = self._staged(tmp_path)

        def broken(names):
            raise ConnectionError("redis down")
        _s, refusal = runner.stage_inputs([_Node("x.csv")], work, locator=broken, run_id="r1")
        assert isinstance(refusal, runner.TransientRefusal)

    def test_no_lookup_configured_is_not(self, tmp_path):
        work = self._staged(tmp_path)
        _s, refusal = runner.stage_inputs([_Node("x.csv")], work, locator=None, run_id="r1")
        assert refusal and not isinstance(refusal, runner.TransientRefusal)

    def test_produced_without_location_is_not(self, tmp_path):
        work = self._staged(tmp_path, produced=lambda n: {"x.csv"})
        _s, refusal = runner.stage_inputs([_Node("x.csv")], work,
                                          locator=lambda n: {}, run_id="r1")
        assert refusal and not isinstance(refusal, runner.TransientRefusal)

    def test_run_carries_the_flag(self, tmp_path, monkeypatch):
        from swarm.models.execution import ExecutionSpec
        runner.configure(mode="real", work_dir=str(tmp_path / "w"))
        monkeypatch.setattr(runner, "stage_inputs",
                            lambda *a, **k: ([], runner.TransientRefusal("producer down")))
        res = runner.run(ExecutionSpec(path="/bin/true", arguments=[], pfn="/bin/true"), "j1")
        assert res.refused and res.transient and res.reason == "producer down"

    def test_a_config_refusal_is_not_transient(self, tmp_path):
        from swarm.models.execution import ExecutionSpec
        runner.configure(mode="real")                       # no work dir
        res = runner.run(ExecutionSpec(path="/bin/true", arguments=[], pfn="/bin/true"), "j1")
        assert res.refused and not res.transient

    def test_retries_round_trip_on_the_record(self):
        j = Job()
        j.job_id = "j1"
        j.refusal_retries = 2
        back = Job()
        back.from_dict(j.to_dict())
        assert back.refusal_retries == 2


def _retry_agent(repo, cap=3):
    from swarm.agents.resource_agent import ResourceAgent
    from swarm.queue.simple_queue import SimpleQueue
    from swarm.utils.metrics import Metrics
    from swarm.utils.thread_safe_dict import ThreadSafeDict
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = MagicMock()
    a.agent_id = 5
    a.runtime_config = {"execution": {"refusal_retries": cap}}
    a.metrics = Metrics()
    a.repository = repo
    a.topology = MagicMock(level=0, group=0)
    a.queues = MagicMock()
    a.queues.ready_queue = SimpleQueue()
    a.engine = MagicMock()
    a.job_assignments = ThreadSafeDict()
    a.start_idle = lambda: None
    a._init_decision_state()
    return a


def _refused_job(repo, retries=0):
    from swarm.database.repository import Repository
    from swarm.models.object import ObjectState
    j = Job()
    j.job_id = "j1"
    j.leader_id = 5
    j.state = ObjectState.COMPLETE
    j.exit_status = 1
    j._refusal_reason = "producer down"
    j._refusal_transient = True
    j.refusal_retries = retries
    repo.save(obj=j.to_dict(), key_prefix=Repository.KEY_JOB, level=0, group=0)
    repo.try_claim_assignment("j1", 5, level=0, group=0)
    return j


class TestAgentRetriesTransientRefusals:
    def _repo(self):
        from swarm.database.repository import Repository
        sys.path.insert(0, os.path.join(REPO, "tests"))
        from test_failed_agent_reassignment import _FakeRedis
        return Repository(_FakeRedis(), run_id="t")

    def test_under_the_cap_the_job_goes_back_to_the_pool(self):
        repo = self._repo()
        a = _retry_agent(repo)
        j = _refused_job(repo)
        a.queues.ready_queue.add(j)
        assert a._retry_refused(j) is True
        rec = repo.get("j1", level=0, group=0)
        assert rec["state"] == j.state.__class__.PENDING.value
        assert rec["leader_id"] is None and rec["refusal_retries"] == 1
        assert repo.try_claim_assignment("j1", 7, level=0, group=0) == 7   # claim released
        assert "j1" not in a.queues.ready_queue
        assert a.metrics.refusal_retries == 1

    def test_at_the_cap_the_refusal_is_recorded(self):
        repo = self._repo()
        a = _retry_agent(repo, cap=3)
        j = _refused_job(repo, retries=3)
        assert a._retry_refused(j) is False
        assert j.refusal_retries == 3

    def test_a_cap_of_zero_never_retries(self):
        repo = self._repo()
        assert _retry_agent(repo, cap=0)._retry_refused(_refused_job(repo)) is False

    def test_a_failed_write_records_the_refusal(self):
        repo = self._repo()
        a = _retry_agent(repo)
        j = _refused_job(repo)

        def boom(*a_, **k):
            raise ConnectionError("redis blip")
        repo.save = boom
        assert a._retry_refused(j) is False
        assert j.exit_status == 1 and j.refusal_retries == 0

    def test_execute_job_does_not_persist_a_retried_refusal(self, monkeypatch):
        repo = self._repo()
        a = _retry_agent(repo)
        a.failure_sim_enabled = False
        a._persist_completion = MagicMock()
        j = _refused_job(repo)
        j.sub_role = None

        def refuse():
            j._refusal_reason, j._refusal_transient = "producer down", True
            j.exit_status = 1
        j.execute = refuse
        a.execute_job(j)
        a._persist_completion.assert_not_called()
        assert repo.get("j1", level=0, group=0)["state"] == j.state.__class__.PENDING.value


def test_a_refused_start_is_not_counted_as_an_execution():
    """Otherwise every retried refusal would read as a double execution."""
    from swarm.database.repository import Repository
    sys.path.insert(0, os.path.join(REPO, "tests"))
    from test_failed_agent_reassignment import _FakeRedis
    repo = Repository(_FakeRedis(), run_id="t")
    a = _retry_agent(repo)
    a.failure_sim_enabled = False
    a._persist_completion = MagicMock()
    j = _refused_job(repo)
    j.sub_role = None

    def refuse():
        j._refusal_reason, j._refusal_transient = "producer down", True
        j.exit_status = 1
    j.execute = refuse
    a.execute_job(j)
    assert a.metrics.executed_jobs == []


def test_retry_releases_the_claim_before_the_record_reads_pending():
    """Saving PENDING first let a peer re-elect the job while the claim still named this agent;
    its CAS returned this agent, every peer took the participant path, and nobody ran it."""
    from swarm.database.repository import Repository
    sys.path.insert(0, os.path.join(REPO, "tests"))
    from test_failed_agent_reassignment import _FakeRedis
    repo = Repository(_FakeRedis(), run_id="t")
    a = _retry_agent(repo)
    j = _refused_job(repo)
    order = []
    real_save, real_release = repo.save, repo.release_assignment
    repo.save = lambda **k: (order.append(("save", k["obj"]["state"])), real_save(**k))[1]
    repo.release_assignment = lambda *a_, **k: (order.append(("release",)),
                                                real_release(*a_, **k))[1]
    assert a._retry_refused(j) is True
    pending = j.state.__class__.PENDING.value
    assert order.index(("release",)) < order.index(("save", pending))


# --------------------------------------------------------------------------- §65
class TestCompletionOnlyOverOurOwnRecord:
    def _setup(self, record_state, leader):
        from swarm.database.repository import Repository
        sys.path.insert(0, os.path.join(REPO, "tests"))
        from test_failed_agent_reassignment import _FakeRedis
        repo = Repository(_FakeRedis(), run_id="t")
        a = _retry_agent(repo)
        a._unpublished_lock = __import__("threading").Lock()
        a._unpersisted_completions = {}
        rec = _refused_job(repo)
        rec.state = record_state
        rec.leader_id = leader
        repo.save(obj=rec.to_dict(), level=0, group=0)
        done = Job()
        done.job_id = "j1"
        done.leader_id = 5
        done.state = done.state.__class__.COMPLETE
        done.exit_status = 0
        return repo, a, done

    def test_our_running_record_is_completed(self):
        from swarm.models.object import ObjectState
        repo, a, done = self._setup(ObjectState.RUNNING, 5)
        a._persist_completion(done, None)
        assert repo.get("j1", level=0, group=0)["state"] == ObjectState.COMPLETE.value

    def test_a_reassigned_record_is_not_overwritten(self):
        from swarm.models.object import ObjectState
        repo, a, done = self._setup(ObjectState.RUNNING, 7)      # now agent 7's
        a._persist_completion(done, None)
        rec = repo.get("j1", level=0, group=0)
        assert rec["state"] == ObjectState.RUNNING.value and rec["leader_id"] == 7

    def test_a_reset_record_is_not_overwritten(self):
        from swarm.models.object import ObjectState
        repo, a, done = self._setup(ObjectState.PENDING, None)
        a._persist_completion(done, None)
        assert repo.get("j1", level=0, group=0)["state"] == ObjectState.PENDING.value

    def test_a_queued_retry_is_dropped_once_the_record_moved_on(self):
        from swarm.models.object import ObjectState
        repo, a, done = self._setup(ObjectState.RUNNING, 7)
        a._unpersisted_completions["j1"] = (done.to_dict(), [], {})
        a._retry_unpersisted_completions()
        assert a._unpersisted_completions == {}
        assert repo.get("j1", level=0, group=0)["leader_id"] == 7

    def test_a_record_without_a_leader_is_still_completed(self):
        from swarm.models.object import ObjectState
        repo, a, done = self._setup(ObjectState.RUNNING, None)
        a._persist_completion(done, None)
        assert repo.get("j1", level=0, group=0)["state"] == ObjectState.COMPLETE.value
