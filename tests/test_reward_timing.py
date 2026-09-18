"""P0-9: the delegation bandit must be rewarded when a job *finishes*, not when it is scheduled.

`ResourceAgent.schedule_job` used to persist the job as COMPLETE — with `exit_status` still at
its default of 0 — and only then hand it to the executor, which re-persisted it with the real
exit status after the simulated wall time (up to 120 s). The coordinator's delegation monitor
polls every tick and takes the first COMPLETE it sees as terminal, so it credited a success the
moment the job was scheduled and never saw the outcome. Measured on `runs/p11-oracle2`: the
leaves injected 83 failures, the bandit recorded 19. Every bandit-vs-bandit comparison before
the fix compared learners that saw a quarter of the failures, weighted toward short jobs.

What these tests hold to:

* `schedule_job` persists RUNNING, never COMPLETE, and leaves `exit_status` alone;
* `execute_job` is the only writer of COMPLETE, and it carries the real exit status;
* a RUNNING job at the children earns no bandit outcome and stays tracked, even past
  `delegation_timeout_s` — which bounds selection, not execution;
* a COMPLETE job at the children earns exactly one outcome with its real exit status;
* a job stuck in progress is dropped without an outcome only after the execution grace,
  which defaults to the wall-time cap so the two budgets cannot drift apart.
"""
import os
import sys
import threading
import time
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from swarm.agents.resource_agent import ResourceAgent  # noqa: E402
from swarm.database.repository import Repository  # noqa: E402
from swarm.models.job import Job, ObjectState  # noqa: E402
from swarm.utils.metrics import Metrics  # noqa: E402
from swarm.utils.thread_safe_dict import ThreadSafeDict  # noqa: E402


class _Log:
    def __init__(self):
        self.warnings = []

    def warning(self, m, *a, **k):
        self.warnings.append(m)

    def __getattr__(self, _name):
        return lambda *a, **k: None


class _Repo:
    """Records every save in order, and serves the child-level view the monitor prefetches."""

    def __init__(self):
        self.saves = []          # (state, exit_status, key_prefix)
        self.child_view = {}     # (job_id, group) -> job dict

    def save(self, obj, key_prefix=Repository.KEY_JOB, key=None, level=0, group=0, **kw):
        self.saves.append((obj.get("state"), obj.get("exit_status"), key_prefix))

    def get_many_grouped(self, pairs, key_prefix=Repository.KEY_JOB, level=0):
        return {p: self.child_view[p] for p in pairs if p in self.child_view}

    def get(self, *a, **k):
        return None


class _Topology:
    def __init__(self, level, children):
        self.level = level
        self.group = 0
        self.children = children


class _Executor:
    """Captures the submission instead of running it, so the test controls when execution ends."""

    def __init__(self):
        self.submitted = []

    def submit(self, fn, *args):
        self.submitted.append((fn, args))


def _job(job_id="j1", wall_time=0.0):
    j = Job()
    j.job_id = job_id
    j.wall_time = wall_time
    j.state = ObjectState.READY
    return j


def make_leaf():
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = _Log()
    a.agent_id = 7
    a.config = {"runtime": {}}
    a.runtime_config = {}
    a.metrics = Metrics()
    a.repository = _Repo()
    a.topology = _Topology(level=0, children=[])
    a.executor = _Executor()
    a.queues = MagicMock()
    a.engine = MagicMock()
    a._init_decision_state()   # the shipped dedupe state, not a hand-built copy
    a.failure_sim_enabled = False
    a.measurement_layer = None
    a.consumer_timeout_s = 1.0
    a.site = None
    a.shutdown = False
    return a


def make_coordinator(runtime=None):
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = _Log()
    a.agent_id = 30
    a.config = {"runtime": runtime or {}}
    a.runtime_config = runtime or {}
    a.metrics = Metrics()
    a.repository = _Repo()
    a.topology = _Topology(level=1, children=[1, 2])
    a.delegated_jobs = ThreadSafeDict()
    a._get_active_child_groups = lambda: [1, 2]
    a.mab_enabled = True
    a.mab_manager = MagicMock()
    a.mab_manager.report_outcome.return_value = 1.0
    a._reassign_delegated_job = MagicMock()
    a.shutdown = False
    return a


def _delegate(a, job_id, group, ago_s):
    a.delegated_jobs.set(job_id, {"delegated_at": time.time() - ago_s, "groups": [group]})


# --------------------------------------------------------------------------------------------
# The leaf: what it persists, and when.
# --------------------------------------------------------------------------------------------

def test_schedule_persists_running_not_complete_and_does_not_touch_exit_status():
    a = make_leaf()
    job = _job()
    job.exit_status = 0

    a.schedule_job(job)

    assert a.repository.saves == [(ObjectState.RUNNING.value, 0, Repository.KEY_JOB)]
    assert job.state is ObjectState.RUNNING
    # Handed to the executor exactly once; execution has not happened yet.
    assert len(a.executor.submitted) == 1
    assert a.executor.submitted[0][0] == a.execute_job
    # Still the consensus dedupe set: the job is no longer up for election.
    assert a.is_job_completed("j1")


def test_execute_is_the_only_writer_of_complete_and_carries_the_real_exit_status():
    a = make_leaf()
    job = _job()
    job._should_fail = True      # the generator's pre-determined failure
    a.schedule_job(job)
    fn, args = a.executor.submitted[0]

    fn(*args)

    states = [s for s, _, _ in a.repository.saves]
    assert states == [ObjectState.RUNNING.value, ObjectState.COMPLETE.value]
    assert a.repository.saves[-1][1] == 1, "COMPLETE must carry the real exit status"
    # Nothing was ever persisted as COMPLETE with a default-zero exit status.
    assert (ObjectState.COMPLETE.value, 0, Repository.KEY_JOB) not in a.repository.saves


def test_injected_failure_reaches_the_persisted_outcome():
    a = make_leaf()
    a.failure_sim_enabled = True
    a._get_failure_rate = lambda job: 1.0
    job = _job()
    a.schedule_job(job)
    fn, args = a.executor.submitted[0]

    fn(*args)

    assert a.repository.saves[-1][:2] == (ObjectState.COMPLETE.value, 1)


# --------------------------------------------------------------------------------------------
# The coordinator: when the bandit hears, and what it hears.
# --------------------------------------------------------------------------------------------

def test_running_job_earns_no_outcome_and_stays_tracked():
    a = make_coordinator({"delegation_timeout_s": 10.0})
    _delegate(a, "j1", group=1, ago_s=1.0)
    a.repository.child_view[("j1", 1)] = {"state": ObjectState.RUNNING.value, "exit_status": 0}

    a._monitor_delegated_jobs()

    a.mab_manager.report_outcome.assert_not_called()
    assert "j1" in a.delegated_jobs
    assert a.metrics.mab_rewards == {}


def test_running_job_past_the_delegation_timeout_is_still_awaited():
    """`delegation_timeout_s` bounds selection. A job the children have scheduled can run for
    its whole simulated wall time past it; that is not a timeout and not a reassignment."""
    a = make_coordinator({"delegation_timeout_s": 10.0, "delegation_exec_grace_s": 100.0})
    _delegate(a, "j1", group=1, ago_s=50.0)
    a.repository.child_view[("j1", 1)] = {"state": ObjectState.RUNNING.value, "exit_status": 0}

    a._monitor_delegated_jobs()

    a.mab_manager.report_outcome.assert_not_called()
    a._reassign_delegated_job.assert_not_called()
    assert "j1" in a.delegated_jobs


def test_complete_job_earns_exactly_one_outcome_with_its_real_exit_status():
    a = make_coordinator({"delegation_timeout_s": 10.0})
    _delegate(a, "j1", group=1, ago_s=30.0)
    a.repository.child_view[("j1", 1)] = {"state": ObjectState.COMPLETE.value, "exit_status": 1}

    a._monitor_delegated_jobs()

    a.mab_manager.report_outcome.assert_called_once()
    kwargs = a.mab_manager.report_outcome.call_args
    assert kwargs.args[:2] == (1, "j1") or kwargs.kwargs.get("group_id") == 1
    assert kwargs.args[2] is False if len(kwargs.args) > 2 else kwargs.kwargs["success"] is False
    assert "j1" not in a.delegated_jobs
    assert len(a.metrics.mab_rewards[1]) == 1


def test_the_p09_sequence_running_then_complete_reports_the_failure_not_a_success():
    """The exact sequence the monitor used to get wrong: first observation is the scheduled
    job, second is its real outcome. The bandit must hear once, and hear the failure."""
    a = make_coordinator({"delegation_timeout_s": 10.0})
    _delegate(a, "j1", group=2, ago_s=1.0)
    a.repository.child_view[("j1", 2)] = {"state": ObjectState.RUNNING.value, "exit_status": 0}
    a._monitor_delegated_jobs()
    a.repository.child_view[("j1", 2)] = {"state": ObjectState.COMPLETE.value, "exit_status": 1}
    a._monitor_delegated_jobs()

    assert a.mab_manager.report_outcome.call_count == 1
    call = a.mab_manager.report_outcome.call_args
    success = call.args[2] if len(call.args) > 2 else call.kwargs["success"]
    assert success is False


def test_stuck_in_progress_job_is_dropped_only_after_the_grace_and_without_an_outcome():
    a = make_coordinator({"delegation_timeout_s": 10.0, "delegation_exec_grace_s": 20.0})
    _delegate(a, "j1", group=1, ago_s=31.0)          # past timeout + grace
    a.repository.child_view[("j1", 1)] = {"state": ObjectState.RUNNING.value, "exit_status": 0}

    a._monitor_delegated_jobs()

    a.mab_manager.report_outcome.assert_not_called()
    assert "j1" not in a.delegated_jobs
    assert any("dropping it from delegation tracking" in w for w in a.logger.warnings)


def test_exec_grace_defaults_to_the_wall_time_cap_and_never_to_zero():
    a = make_coordinator({})
    saved = Job._WALL_TIME_MAX_S
    try:
        Job._WALL_TIME_MAX_S = 120.0
        assert a.delegation_exec_grace_s == 120.0
        Job._WALL_TIME_MAX_S = 0.0                    # uncapped execution
        assert a.delegation_exec_grace_s == 1800.0
    finally:
        Job._WALL_TIME_MAX_S = saved
    assert make_coordinator({"delegation_exec_grace_s": 7}).delegation_exec_grace_s == 7.0


def test_pending_job_timeout_path_is_unchanged():
    """Selection timeouts still reassign and still report a timed-out failure."""
    a = make_coordinator({"delegation_timeout_s": 10.0})
    _delegate(a, "j1", group=1, ago_s=30.0)
    a.repository.child_view[("j1", 1)] = {"state": ObjectState.PENDING.value, "exit_status": 0}

    a._monitor_delegated_jobs()

    a.mab_manager.report_outcome.assert_called_once()
    call = a.mab_manager.report_outcome.call_args
    assert call.kwargs.get("timed_out") is True
    a._reassign_delegated_job.assert_called_once()
