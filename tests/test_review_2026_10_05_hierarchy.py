"""Code review 2026-10-05 §13-§17, §20 and §H: the hierarchy paths that ran a job twice.

§13 A dead coordinator's DELEGATED jobs were reset by a peer coordinator — the parent record
    sits READY under the coordinator for the job's whole delegated life — re-elected,
    re-delegated over a running child copy, and run again.
§14 Pulling a job back (`_reassign_delegated_job`) did not release the Snow claim, so every
    re-finalization returned the coordinator that had just barred itself; nobody ran it.
§15 Withdrawing a delegated job DELETES the child record; a deleted key is in no state index, so
    children never dropped their local copy and could later run it alongside the re-delegation.
    `move_to_end` also resurrected a job another thread had just removed.
§16 With fan-out > 1 the monitor broke on the first unpicked copy past the timeout even while
    another group was running the job, charged every group a timeout and pulled the job back.
§17 Completion was propagated as state only (exit 0 for a job its child failed), and the
    execution-grace drop wrote COMPLETE for a job nobody finished.
§20 `scheduling_main` set PENDING on the very object still in the coordinator's pending queue.
§H  Nothing in a run reported a double execution; `executed_jobs` → `jobs_executed_twice`.

Uses the real `Repository` over the in-memory Redis from test_failed_agent_reassignment.
"""
import os
import sys
import time
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "tests"))

from swarm.agents.resource_agent import ResourceAgent  # noqa: E402
from swarm.database.repository import Repository  # noqa: E402
from swarm.models.job import Job, ObjectState  # noqa: E402
from swarm.queue.simple_queue import SimpleQueue  # noqa: E402
from swarm.utils.metrics import Metrics  # noqa: E402
from swarm.utils.thread_safe_dict import ThreadSafeDict  # noqa: E402

from test_failed_agent_reassignment import _FakeRedis  # noqa: E402

COORD_LEVEL, COORD_GROUP = 1, 0
CHILD_LEVEL = 0


class _Topology:
    def __init__(self, level, group, children):
        self.level, self.group, self.children = level, group, children


def _queues():
    q = MagicMock()
    q.pending_queue = SimpleQueue()
    q.selected_queue = SimpleQueue()
    return q


def _coordinator(repo, agent_id=30, children=(1, 2), runtime=None):
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = MagicMock()
    a.agent_id = agent_id
    a.config = {"runtime": runtime or {}}
    a.runtime_config = runtime or {}
    a.metrics = Metrics()
    a.repository = repo
    a.topology = _Topology(COORD_LEVEL, COORD_GROUP, list(children))
    a.queues = _queues()
    a.engine = MagicMock()
    a.delegated_jobs = ThreadSafeDict()
    a.job_assignments = ThreadSafeDict()
    a._init_decision_state()
    a._get_active_child_groups = lambda: list(children)
    a.mab_enabled = True
    a.mab_manager = MagicMock()
    a.mab_manager.report_outcome.return_value = 1.0
    a.shutdown = False
    return a


def _leaf(repo, agent_id=5, group=1):
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = MagicMock()
    a.agent_id = agent_id
    a.metrics = Metrics()
    a.repository = repo
    a.topology = _Topology(CHILD_LEVEL, group, [])
    a.queues = _queues()
    a.engine = MagicMock()
    a._init_decision_state()
    return a


def _job(job_id="j1", state=ObjectState.READY, leader=None):
    j = Job()
    j.job_id = job_id
    j.wall_time = 1.0
    j.state = state
    j.leader_id = leader
    return j


def _record(repo, job_id, level, group):
    """The record, or None when there is none (`Repository.get` returns {} for a missing key)."""
    return repo.get(job_id, key_prefix=Repository.KEY_JOB, level=level, group=group) or None


def _set_child_state(repo, job_id, group, state, exit_status=None):
    rec = _record(repo, job_id, CHILD_LEVEL, group)
    rec["state"] = state.value
    if exit_status is not None:
        rec["exit_status"] = exit_status
    repo.save(obj=rec, key_prefix=Repository.KEY_JOB, level=CHILD_LEVEL, group=group)


def _delegated(repo, coord, job_id="j1", groups=(1,)):
    """A job coordinator `coord` won and delegated to `groups`, via the real write path."""
    job = _job(job_id, ObjectState.READY, leader=coord.agent_id)
    repo.save(obj=job.to_dict(), key_prefix=Repository.KEY_JOB, level=COORD_LEVEL, group=COORD_GROUP)
    repo.try_claim_assignment(job_id, coord.agent_id, level=COORD_LEVEL, group=COORD_GROUP)
    coord.queues.pending_queue.add(job)
    coord.queues.selected_queue.add(job)
    coord._delegate_child_groups = lambda j, cg: list(groups)
    coord._delegate_to_children(job, list(groups))
    return job


# --------------------------------------------------------------------------- #
# The write path (§13 record, §19 ordering, §20 separate object)
# --------------------------------------------------------------------------- #

class TestDelegationWritePath:
    def test_the_coordinator_record_names_the_groups(self):
        repo = Repository(_FakeRedis(), run_id="t")
        coord = _coordinator(repo)
        _delegated(repo, coord, groups=(1, 2))
        rec = _record(repo, "j1", COORD_LEVEL, COORD_GROUP)
        assert rec["delegated_groups"] == [1, 2]
        assert rec["state"] == ObjectState.READY.value

    def test_child_copies_are_pending_and_carry_no_delegation(self):
        repo = Repository(_FakeRedis(), run_id="t")
        coord = _coordinator(repo)
        _delegated(repo, coord, groups=(1, 2))
        for g in (1, 2):
            rec = _record(repo, "j1", CHILD_LEVEL, g)
            assert rec["state"] == ObjectState.PENDING.value
            assert rec.get("delegated_groups") == []

    def test_the_coordinators_own_object_is_not_flipped_to_pending(self):
        """§20: the object in the coordinator's pending queue stays READY, so the selection
        loop (which asks for PENDING) cannot propose it again before the next scan."""
        repo = Repository(_FakeRedis(), run_id="t")
        coord = _coordinator(repo)
        job = _delegated(repo, coord)
        assert job.state == ObjectState.READY
        assert coord.queues.pending_queue.gets(states=[ObjectState.PENDING]) == []

    def test_a_failed_child_write_leaves_the_job_selected_for_retry(self):
        """§19: local state changes only after every write. A raise leaves the job in
        selected_queue and untracked, so the next pass tries again."""
        repo = Repository(_FakeRedis(), run_id="t")
        coord = _coordinator(repo)
        job = _job("j1", ObjectState.READY, leader=coord.agent_id)
        coord.queues.selected_queue.add(job)
        coord._delegate_child_groups = lambda j, cg: [1]
        real_save = repo.save

        def flaky(obj, key_prefix=Repository.KEY_JOB, level=0, group=0, **kw):
            if level == CHILD_LEVEL:
                raise ConnectionError("redis blip")
            return real_save(obj=obj, key_prefix=key_prefix, level=level, group=group, **kw)
        repo.save = flaky
        with pytest.raises(ConnectionError):
            coord._delegate_to_children(job, [1])
        assert "j1" in coord.queues.selected_queue
        assert coord.delegated_jobs.get("j1") is None


# --------------------------------------------------------------------------- #
# §13 — a dead coordinator's delegated jobs stay with its children
# --------------------------------------------------------------------------- #

class TestDeadCoordinator:
    @pytest.mark.parametrize("child_state", [ObjectState.READY, ObjectState.RUNNING,
                                             ObjectState.COMPLETE])
    def test_a_job_its_children_hold_is_left_alone(self, child_state):
        repo = Repository(_FakeRedis(), run_id="t")
        dead = _coordinator(repo, agent_id=30)
        _delegated(repo, dead, groups=(1,))
        _set_child_state(repo, "j1", 1, child_state)

        peer = _coordinator(repo, agent_id=31, children=(3, 4))
        peer._reassign_jobs_from_failed_agent(30)

        assert _record(repo, "j1", COORD_LEVEL, COORD_GROUP)["state"] == ObjectState.READY.value
        assert _record(repo, "j1", CHILD_LEVEL, 1)["state"] == child_state.value
        assert "j1" not in peer.metrics.reassignments

    def test_a_job_no_child_picked_up_is_taken_back(self):
        """Never left the tier in practice: withdraw the unpicked copies and re-elect it."""
        repo = Repository(_FakeRedis(), run_id="t")
        dead = _coordinator(repo, agent_id=30)
        _delegated(repo, dead, groups=(1, 2))

        peer = _coordinator(repo, agent_id=31, children=(3, 4))
        peer._reassign_jobs_from_failed_agent(30)

        rec = _record(repo, "j1", COORD_LEVEL, COORD_GROUP)
        assert rec["state"] == ObjectState.PENDING.value
        assert rec["leader_id"] is None and rec["delegated_groups"] == []
        assert _record(repo, "j1", CHILD_LEVEL, 1) is None
        assert _record(repo, "j1", CHILD_LEVEL, 2) is None

    def test_one_picked_copy_among_several_keeps_it_with_the_children(self):
        repo = Repository(_FakeRedis(), run_id="t")
        dead = _coordinator(repo, agent_id=30)
        _delegated(repo, dead, groups=(1, 2))
        _set_child_state(repo, "j1", 2, ObjectState.RUNNING)
        _coordinator(repo, agent_id=31)._reassign_jobs_from_failed_agent(30)
        assert _record(repo, "j1", COORD_LEVEL, COORD_GROUP)["state"] == ObjectState.READY.value
        assert _record(repo, "j1", CHILD_LEVEL, 2)["state"] == ObjectState.RUNNING.value

    def test_an_undelegated_job_is_still_reassigned(self):
        """Won but not yet delegated when the coordinator died: unchanged behaviour."""
        repo = Repository(_FakeRedis(), run_id="t")
        job = _job("j1", ObjectState.READY, leader=30)
        repo.save(obj=job.to_dict(), key_prefix=Repository.KEY_JOB, level=COORD_LEVEL,
                  group=COORD_GROUP)
        _coordinator(repo, agent_id=31)._reassign_jobs_from_failed_agent(30)
        assert _record(repo, "j1", COORD_LEVEL, COORD_GROUP)["state"] == ObjectState.PENDING.value


# --------------------------------------------------------------------------- #
# §14 — pulling back releases the claim
# --------------------------------------------------------------------------- #

def test_pulling_back_a_delegated_job_releases_the_claim():
    repo = Repository(_FakeRedis(), run_id="t")
    coord = _coordinator(repo)
    _delegated(repo, coord, groups=(1,))
    info = coord.delegated_jobs.get("j1")
    coord._reassign_delegated_job("j1", info, 999.0)
    # A different coordinator can now win the re-election.
    assert repo.try_claim_assignment("j1", 31, level=COORD_LEVEL, group=COORD_GROUP) == 31
    rec = _record(repo, "j1", COORD_LEVEL, COORD_GROUP)
    assert rec["state"] == ObjectState.PENDING.value and rec["delegated_groups"] == []


# --------------------------------------------------------------------------- #
# §15 — children drop a withdrawn job
# --------------------------------------------------------------------------- #

class TestPurge:
    def _scan(self, leaf):
        state_map = leaf.repository.get_all_ids_multi(
            key_prefix=Repository.KEY_JOB, level=CHILD_LEVEL, group=leaf.topology.group,
            states=[ObjectState.PENDING.value, ObjectState.READY.value,
                    ObjectState.RUNNING.value, ObjectState.COMPLETE.value])
        leaf._purge_vanished_jobs(present={j for ids in state_map.values() for j in ids})

    def test_a_withdrawn_job_is_dropped_after_two_scans(self):
        repo = Repository(_FakeRedis(), run_id="t")
        coord = _coordinator(repo)
        _delegated(repo, coord, groups=(1,))
        leaf = _leaf(repo, group=1)
        leaf.queues.pending_queue.add(_job("j1", ObjectState.PENDING))
        self._scan(leaf)
        assert "j1" in leaf.queues.pending_queue        # present: kept
        coord._withdraw_child_copies("j1", [1])
        self._scan(leaf)
        assert "j1" in leaf.queues.pending_queue        # absent once: not yet
        self._scan(leaf)
        assert "j1" not in leaf.queues.pending_queue    # absent twice: dropped
        leaf.engine.incoming.remove_object.assert_called_with(object_id="j1")

    def test_a_job_seen_again_is_not_dropped(self):
        """Index reads are not transactional; one absent scan must not drop a live job."""
        repo = Repository(_FakeRedis(), run_id="t")
        leaf = _leaf(repo, group=1)
        leaf.queues.pending_queue.add(_job("j1", ObjectState.PENDING))
        leaf._purge_vanished_jobs(present=set())
        leaf._purge_vanished_jobs(present={"j1"})
        leaf._purge_vanished_jobs(present=set())
        assert "j1" in leaf.queues.pending_queue

    def test_move_to_end_does_not_resurrect(self):
        q = SimpleQueue()
        j = _job("j1", ObjectState.PENDING)
        q.add(j)
        q.remove("j1")
        q.move_to_end(j)
        assert "j1" not in q


# --------------------------------------------------------------------------- #
# §16-§17 — the monitor decides over every copy, and propagates the outcome
# --------------------------------------------------------------------------- #

class TestMonitor:
    def _age(self, coord, job_id, seconds):
        info = coord.delegated_jobs.get(job_id)
        coord.delegated_jobs.set(job_id, dict(info, delegated_at=time.time() - seconds))

    def test_a_running_copy_is_not_pulled_back_by_an_unpicked_one(self):
        repo = Repository(_FakeRedis(), run_id="t")
        coord = _coordinator(repo)
        _delegated(repo, coord, groups=(1, 2))
        _set_child_state(repo, "j1", 2, ObjectState.RUNNING)
        self._age(coord, "j1", coord.delegation_timeout_s + 5)
        coord._reassign_delegated_job = MagicMock()

        coord._monitor_delegated_jobs()

        coord._reassign_delegated_job.assert_not_called()
        for c in coord.mab_manager.report_outcome.call_args_list:
            assert not c.kwargs.get("timed_out"), "the executing group was charged a timeout"
        assert _record(repo, "j1", CHILD_LEVEL, 2)["state"] == ObjectState.RUNNING.value

    def test_once_one_group_has_it_the_others_unpicked_copies_are_withdrawn(self):
        repo = Repository(_FakeRedis(), run_id="t")
        coord = _coordinator(repo)
        _delegated(repo, coord, groups=(1, 2))
        _set_child_state(repo, "j1", 2, ObjectState.RUNNING)
        coord._monitor_delegated_jobs()
        assert _record(repo, "j1", CHILD_LEVEL, 1) is None
        assert coord.delegated_jobs.get("j1")["groups"] == [2]

    def test_all_unpicked_past_the_timeout_is_still_pulled_back(self):
        repo = Repository(_FakeRedis(), run_id="t")
        coord = _coordinator(repo)
        _delegated(repo, coord, groups=(1, 2))
        self._age(coord, "j1", coord.delegation_timeout_s + 5)
        coord._reassign_delegated_job = MagicMock()
        coord._monitor_delegated_jobs()
        coord._reassign_delegated_job.assert_called_once()

    def test_a_child_failure_reaches_the_coordinator_record(self):
        repo = Repository(_FakeRedis(), run_id="t")
        coord = _coordinator(repo)
        _delegated(repo, coord, groups=(1,))
        _set_child_state(repo, "j1", 1, ObjectState.COMPLETE, exit_status=3)
        coord._monitor_delegated_jobs()
        rec = _record(repo, "j1", COORD_LEVEL, COORD_GROUP)
        assert rec["state"] == ObjectState.COMPLETE.value
        assert rec["exit_status"] == 3

    def test_the_grace_drop_does_not_write_complete(self):
        repo = Repository(_FakeRedis(), run_id="t")
        coord = _coordinator(repo)
        _delegated(repo, coord, groups=(1,))
        _set_child_state(repo, "j1", 1, ObjectState.RUNNING)
        self._age(coord, "j1", coord.delegation_timeout_s + coord.delegation_exec_grace_s + 5)
        coord._monitor_delegated_jobs()
        assert coord.delegated_jobs.get("j1") is None
        assert _record(repo, "j1", COORD_LEVEL, COORD_GROUP)["state"] == ObjectState.READY.value


# --------------------------------------------------------------------------- #
# §H — double executions are counted
# --------------------------------------------------------------------------- #

class TestExecutionEvidence:
    def test_counts_jobs_started_more_than_once_anywhere(self):
        from evaluation.collect import execution_evidence
        out = execution_evidence({
            "1": {"executed_jobs": ["a", "b"]},
            "2": {"executed_jobs": ["b", "c"]},     # b on two agents
            "3": {"executed_jobs": ["d", "d"]},     # d twice on one agent
        })
        assert out == {"jobs_executed": 4, "jobs_executed_twice": 2, "executions_extra": 2,
                       "exec_refusal_retries": 0}

    def test_clean_is_zero_not_absent(self):
        from evaluation.collect import execution_evidence
        assert execution_evidence({"1": {"executed_jobs": ["a"]}, "2": {"executed_jobs": []}}) \
            == {"jobs_executed": 1, "jobs_executed_twice": 0, "executions_extra": 0,
                "exec_refusal_retries": 0}

    def test_unmeasured_is_absent_not_zero(self):
        from evaluation.collect import execution_evidence
        assert execution_evidence({"1": {"restarts": {}}}) == {}

    def test_the_agent_exports_what_it_executed(self):
        src = open(os.path.join(REPO, "swarm/agents/resource_agent.py")).read()
        body = src[src.index("    def execute_job"):src.index("    def ", src.index("    def execute_job") + 10)]
        assert "self.metrics.executed_jobs.append(job_id)" in body
        assert '"executed_jobs": list(' in src


# --------------------------------------------------------------------------- races (stop-time review)
class TestWithdrawalRaces:
    """Withdrawal deleted unconditionally and `select_job` saved unconditionally, so a child that
    picked a copy up between the coordinator's read and its delete kept running a copy the
    coordinator had withdrawn — or re-created it — and the job ran in two groups."""

    def test_a_copy_picked_up_after_the_read_is_not_deleted(self):
        repo = Repository(_FakeRedis(), run_id="t")
        coord = _coordinator(repo)
        _delegated(repo, coord, groups=(1, 2))
        _set_child_state(repo, "j1", 1, ObjectState.READY)      # child 1 picked it up
        kept = coord._withdraw_child_copies("j1", [1, 2])
        assert kept == [1]
        assert _record(repo, "j1", CHILD_LEVEL, 1)["state"] == ObjectState.READY.value
        assert _record(repo, "j1", CHILD_LEVEL, 2) is None

    def test_select_job_does_not_recreate_a_withdrawn_copy(self):
        repo = Repository(_FakeRedis(), run_id="t")
        coord = _coordinator(repo)
        _delegated(repo, coord, groups=(1,))
        leaf = _leaf(repo, group=1)
        leaf.job_assignments = ThreadSafeDict()
        child = Job()
        child.from_dict(_record(repo, "j1", CHILD_LEVEL, 1))
        leaf.queues.pending_queue.add(child)
        coord._withdraw_child_copies("j1", [1])                  # withdrawn before it wins
        leaf.select_job(child)                                    # ...and then it wins
        assert _record(repo, "j1", CHILD_LEVEL, 1) is None
        assert "j1" not in leaf.queues.selected_queue
        assert "j1" not in leaf.queues.pending_queue

    def test_select_job_still_selects_a_live_copy(self):
        repo = Repository(_FakeRedis(), run_id="t")
        coord = _coordinator(repo)
        _delegated(repo, coord, groups=(1,))
        leaf = _leaf(repo, group=1)
        leaf.job_assignments = ThreadSafeDict()
        child = Job()
        child.from_dict(_record(repo, "j1", CHILD_LEVEL, 1))
        leaf.select_job(child)
        assert _record(repo, "j1", CHILD_LEVEL, 1)["state"] == ObjectState.READY.value
        assert "j1" in leaf.queues.selected_queue

    def test_a_pull_back_that_loses_the_race_leaves_the_job_with_the_child(self):
        repo = Repository(_FakeRedis(), run_id="t")
        coord = _coordinator(repo)
        _delegated(repo, coord, groups=(1,))
        info = coord.delegated_jobs.get("j1")
        _set_child_state(repo, "j1", 1, ObjectState.RUNNING)    # picked after the timeout read
        coord._reassign_delegated_job("j1", info, 999.0)
        assert _record(repo, "j1", CHILD_LEVEL, 1)["state"] == ObjectState.RUNNING.value
        assert _record(repo, "j1", COORD_LEVEL, COORD_GROUP)["state"] == ObjectState.READY.value
        assert coord.delegated_jobs.get("j1") is not None

    def test_delete_if_state(self):
        repo = Repository(_FakeRedis(), run_id="t")
        repo.save(obj=_job("j9", ObjectState.PENDING).to_dict(), level=0, group=1)
        assert repo.delete_if_state("j9", ObjectState.READY, level=0, group=1) is False
        assert repo.get("j9", level=0, group=1)
        assert repo.delete_if_state("j9", ObjectState.PENDING, level=0, group=1) is True
        assert not repo.get("j9", level=0, group=1)
        assert "j9" not in repo.get_all_ids_multi(          # the state index is cleaned too
            level=0, group=1, states=[ObjectState.PENDING.value]).get(ObjectState.PENDING.value, [])
        assert repo.delete_if_state("j9", ObjectState.PENDING, level=0, group=1) is True


# --------------------------------------------------------------------------- §19
class TestScheduleWritesFirst:
    def _leaf_with_job(self, repo):
        leaf = _leaf(repo, group=1)
        leaf.queues.ready_queue = SimpleQueue()
        leaf.executor = MagicMock()
        leaf.end_idle = lambda: None
        leaf._update_completed_jobs = MagicMock()
        job = _job("j1", ObjectState.READY, leader=5)
        repo.save(obj=job.to_dict(), level=CHILD_LEVEL, group=1)
        return leaf, job

    def test_a_failed_write_changes_nothing_local_and_retries(self):
        repo = Repository(_FakeRedis(), run_id="t")
        leaf, job = self._leaf_with_job(repo)

        def boom(*a, **k):
            raise ConnectionError("redis blip")
        repo.save = boom
        leaf.schedule_job(job)
        assert "j1" not in leaf.queues.ready_queue
        assert "j1" in leaf.queues.selected_queue
        assert job.state == ObjectState.READY
        leaf.executor.submit.assert_not_called()
        leaf._update_completed_jobs.assert_not_called()

    def test_a_withdrawn_record_is_not_run(self):
        repo = Repository(_FakeRedis(), run_id="t")
        leaf, job = self._leaf_with_job(repo)
        repo.delete("j1", level=CHILD_LEVEL, group=1)
        leaf.schedule_job(job)
        leaf.executor.submit.assert_not_called()
        assert _record(repo, "j1", CHILD_LEVEL, 1) is None

    def test_the_normal_path_still_runs(self):
        repo = Repository(_FakeRedis(), run_id="t")
        leaf, job = self._leaf_with_job(repo)
        leaf.schedule_job(job)
        leaf.executor.submit.assert_called_once()
        assert _record(repo, "j1", CHILD_LEVEL, 1)["state"] == ObjectState.RUNNING.value
        assert "j1" in leaf.queues.ready_queue
