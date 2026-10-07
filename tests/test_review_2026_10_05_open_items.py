"""Code review 2026-10-05 §18, §21, §68 — the three items the header called closed and were not.

§18 Failed-agent reassignment ran once, when the detector first removed the peer. The
    per-(job, failure) claim has a 300 s TTL so a dead reassigner does not strand the job — but
    nothing came back after it expired, so the job stayed READY/RUNNING under a corpse for the
    run. The Snow variant: a leader that died between winning the CAS and persisting READY left
    a PENDING record whose claim named it; reassignment (READY/RUNNING only) never saw it.
§21 `_update_pending_jobs` took the PENDING id list as a snapshot and fetched the records later;
    a record that had moved to READY in between, with a later transition stamp, read as a reset:
    decision forgotten, READY record put back in `pending_queue`, dedupe entry discarded.
§68 `delegation_exec_grace_s` defaulted to the simulated cap (120 s) under
    `runtime.execution.mode: real` too, where the budget is the runner's `timeout_s` (3600 s),
    so a coordinator dropped every real job longer than `delegation_timeout_s + 120 s` with no
    bandit outcome.

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
from swarm.execution import runner  # noqa: E402
from swarm.models.job import Job, ObjectState  # noqa: E402
from swarm.queue.simple_queue import SimpleQueue  # noqa: E402
from swarm.utils.metrics import Metrics  # noqa: E402
from swarm.utils.thread_safe_dict import ThreadSafeDict  # noqa: E402

from test_failed_agent_reassignment import _FakeRedis, _place, _state_of  # noqa: E402


def _agent(repo, agent_id=1):
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = MagicMock()
    a.agent_id = agent_id
    a.config = {"runtime": {}}
    a.runtime_config = {}
    a.metrics = Metrics()
    a.repository = repo
    a.topology = MagicMock(level=0, group=0)
    a.queues = MagicMock()
    a.queues.pending_queue = SimpleQueue()
    a.engine = MagicMock()
    a.job_assignments = ThreadSafeDict()
    a.failed_agents = ThreadSafeDict()
    a.shutdown = False
    a._init_decision_state()
    a.pending_proposals = {}
    a.pending_prepares = {}
    a.pending_commits = {}
    return a


def _reassign_claim_key(repo, job_id, failed_agent):
    return f"{Repository.KEY_REASSIGN}:{repo.run_id}:0:0:{job_id}:{failed_agent}"


# --------------------------------------------------------------------------- #
# §18 — reassignment is retried until the dead agent holds nothing
# --------------------------------------------------------------------------- #

class TestReassignmentRetries:

    def test_a_job_stranded_by_a_dead_reassigner_is_retried_after_the_claim_expires(self):
        redis = _FakeRedis()
        repo = Repository(redis, run_id="t")
        a = _agent(repo)
        _place(repo, "j1", leader=7, state=ObjectState.RUNNING)
        a.failed_agents.set(7, 100.0)

        # Another agent won the reassignment claim and died before resetting the job.
        assert repo.try_claim_reassignment("j1", 7, level=0, group=0)
        a._resweep_failed_agent_jobs(current_time=200.0)
        assert _state_of(repo, "j1") == (ObjectState.RUNNING.value, 7), \
            "the claim is held: the sweep must not reset the job under someone else's claim"

        # The claim's TTL expires (the fake has no clock; drop the key as Redis would).
        redis.delete(_reassign_claim_key(repo, "j1", 7))
        a._resweep_failed_agent_jobs(current_time=300.0)
        assert _state_of(repo, "j1") == (ObjectState.PENDING.value, None)
        assert repo.get_assignments(["j1"]) == {}, "the exactly-once claim is released too"

    def test_the_sweep_converges_to_a_no_op_once_nothing_is_held(self):
        repo = Repository(_FakeRedis(), run_id="t")
        a = _agent(repo)
        _place(repo, "j1", leader=7, state=ObjectState.READY)
        a.failed_agents.set(7, 100.0)
        a._resweep_failed_agent_jobs(current_time=200.0)
        assert _state_of(repo, "j1") == (ObjectState.PENDING.value, None)
        a._reassign_jobs_from_failed_agent = MagicMock()
        a._resweep_failed_agent_jobs(current_time=300.0)
        a._reassign_jobs_from_failed_agent.assert_not_called()

    def test_the_sweep_is_rate_limited(self):
        repo = Repository(_FakeRedis(), run_id="t")
        a = _agent(repo)
        a.failed_agents.set(7, 100.0)
        a.repository = MagicMock(wraps=repo)
        a._resweep_failed_agent_jobs(current_time=200.0)
        a._resweep_failed_agent_jobs(current_time=201.0)
        assert a.repository.get_all_ids_multi.call_count == 1
        a._resweep_failed_agent_jobs(current_time=200.0 + a._REASSIGN_RESWEEP_S)
        assert a.repository.get_all_ids_multi.call_count == 2

    def test_nothing_is_read_while_no_peer_is_failed(self):
        repo = Repository(_FakeRedis(), run_id="t")
        a = _agent(repo)
        a.repository = MagicMock(wraps=repo)
        a._resweep_failed_agent_jobs(current_time=200.0)
        a.repository.get_all_ids_multi.assert_not_called()

    def test_a_readmitted_peer_is_not_swept(self):
        repo = Repository(_FakeRedis(), run_id="t")
        a = _agent(repo)
        _place(repo, "j1", leader=7, state=ObjectState.RUNNING)
        a.failed_agents.set(7, 100.0)
        a.failed_agents.remove(7)          # heartbeat resumed: readmitted
        a._resweep_failed_agent_jobs(current_time=200.0)
        assert _state_of(repo, "j1") == (ObjectState.RUNNING.value, 7)

    def test_jobs_of_a_live_leader_are_untouched_by_the_sweep(self):
        repo = Repository(_FakeRedis(), run_id="t")
        a = _agent(repo)
        _place(repo, "j1", leader=7, state=ObjectState.RUNNING)
        _place(repo, "j2", leader=8, state=ObjectState.RUNNING)
        a.failed_agents.set(7, 100.0)
        a._resweep_failed_agent_jobs(current_time=200.0)
        assert _state_of(repo, "j1") == (ObjectState.PENDING.value, None)
        assert _state_of(repo, "j2") == (ObjectState.RUNNING.value, 8)

    def test_the_sweep_reads_the_indexes_once_for_several_failed_peers(self):
        repo = Repository(_FakeRedis(), run_id="t")
        a = _agent(repo)
        _place(repo, "j1", leader=7, state=ObjectState.RUNNING)
        _place(repo, "j2", leader=8, state=ObjectState.READY)
        a.failed_agents.set(7, 100.0)
        a.failed_agents.set(8, 100.0)
        a.repository = MagicMock(wraps=repo)
        a._resweep_failed_agent_jobs(current_time=200.0)
        assert a.repository.get_all_ids_multi.call_count == 1
        assert _state_of(repo, "j1") == (ObjectState.PENDING.value, None)
        assert _state_of(repo, "j2") == (ObjectState.PENDING.value, None)

    def test_the_sweep_is_off_with_reassignment_disabled(self):
        repo = Repository(_FakeRedis(), run_id="t")
        a = _agent(repo)
        a.runtime_config = {"job_reassignment_enabled": False}
        _place(repo, "j1", leader=7, state=ObjectState.RUNNING)
        a.failed_agents.set(7, 100.0)
        a._resweep_failed_agent_jobs(current_time=200.0)
        assert _state_of(repo, "j1") == (ObjectState.RUNNING.value, 7)


class TestStaleSnowClaims:
    """The Snow variant: a PENDING record whose claim names a dead leader."""

    def _pending_with_claim(self, repo, job_id, claimant):
        j = Job()
        j.job_id = job_id
        j.wall_time = 1.0
        j.state = ObjectState.PENDING
        repo.save(obj=j.to_dict(), key_prefix=Repository.KEY_JOB, level=0, group=0)
        assert repo.try_claim_assignment(job_id, claimant, level=0, group=0)

    def test_a_claim_naming_a_dead_leader_on_a_pending_record_is_released(self):
        repo = Repository(_FakeRedis(), run_id="t")
        a = _agent(repo)
        self._pending_with_claim(repo, "j1", claimant=7)
        a.failed_agents.set(7, 100.0)
        a._resweep_failed_agent_jobs(current_time=200.0)
        assert repo.get_assignments(["j1"]) == {}
        assert _state_of(repo, "j1") == (ObjectState.PENDING.value, None)
        assert a.metrics.reassignments["j1"]["reason"] == "stale_claim"

    def test_a_claim_naming_a_live_leader_is_kept(self):
        repo = Repository(_FakeRedis(), run_id="t")
        a = _agent(repo)
        self._pending_with_claim(repo, "j1", claimant=8)   # 8 won the CAS, READY not yet written
        a.failed_agents.set(7, 100.0)
        a._resweep_failed_agent_jobs(current_time=200.0)
        assert repo.get_assignments(["j1"]) == {"j1": 8}

    def test_release_is_compare_and_delete(self):
        """Two sweepers read one stale claim; the second must not delete the live claim a
        peer won after the first release."""
        repo = Repository(_FakeRedis(), run_id="t")
        self._pending_with_claim(repo, "j1", claimant=7)
        assert repo.release_assignment_if_held_by("j1", 7, level=0, group=0) is True
        assert repo.try_claim_assignment("j1", 9, level=0, group=0)
        assert repo.release_assignment_if_held_by("j1", 7, level=0, group=0) is False
        assert repo.get_assignments(["j1"]) == {"j1": 9}

    def test_release_of_an_unclaimed_job_is_false_and_silent(self):
        repo = Repository(_FakeRedis(), run_id="t")
        assert repo.release_assignment_if_held_by("nope", 7, level=0, group=0) is False


# --------------------------------------------------------------------------- #
# §21 — reset evidence must come from a record that still says PENDING
# --------------------------------------------------------------------------- #

def _save_record(repo, job_id, state, transition_at, leader=None):
    j = Job()
    j.job_id = job_id
    j.wall_time = 1.0
    j.leader_id = leader
    j.state = state
    rec = j.to_dict()
    rec["last_transition_at"] = transition_at
    repo.save(obj=rec, key_prefix=Repository.KEY_JOB, level=0, group=0)
    return rec


class TestResetEvidenceChecksTheRecordState:

    def test_a_record_that_moved_to_ready_is_not_a_reset(self):
        repo = Repository(_FakeRedis(), run_id="t")
        a = _agent(repo)
        decided_at = time.time() - 30.0
        a._decided_jobs["j1"] = decided_at
        a.completed_jobs_set.add("j1")
        # Snapshot said PENDING; by the fetch the leader had persisted READY, later than
        # our decision — exactly the shape the old rule read as a reset.
        _save_record(repo, "j1", ObjectState.READY, decided_at + 10.0, leader=4)
        a._update_pending_jobs(["j1"])
        assert "j1" in a._decided_jobs, "the decision was forgotten"
        assert "j1" in a.completed_jobs_set, "the dedupe entry was discarded"
        assert "j1" not in a.queues.pending_queue, "a READY record was put up for election"

    def test_a_record_still_pending_with_a_later_stamp_is_a_reset(self):
        repo = Repository(_FakeRedis(), run_id="t")
        a = _agent(repo)
        decided_at = time.time() - 30.0
        a._decided_jobs["j1"] = decided_at
        a.completed_jobs_set.add("j1")
        _save_record(repo, "j1", ObjectState.PENDING, decided_at + 10.0)
        a._update_pending_jobs(["j1"])
        assert "j1" not in a._decided_jobs
        assert "j1" not in a.completed_jobs_set
        assert "j1" in a.queues.pending_queue

    def test_an_undecided_job_whose_record_moved_on_is_not_queued(self):
        repo = Repository(_FakeRedis(), run_id="t")
        a = _agent(repo)
        a.completed_jobs_set.add("j1")
        _save_record(repo, "j1", ObjectState.RUNNING, time.time(), leader=4)
        a._update_pending_jobs(["j1"])
        assert "j1" not in a.queues.pending_queue
        assert "j1" in a.completed_jobs_set

    def test_reset_evidence_direct(self):
        repo = Repository(_FakeRedis(), run_id="t")
        a = _agent(repo)
        a._decided_jobs["j1"] = 1000.0
        assert a._reset_evidence("j1", {"state": ObjectState.READY.value,
                                        "last_transition_at": 1100.0}) is False
        assert a._reset_evidence("j1", {"state": ObjectState.PENDING.value,
                                        "last_transition_at": 1100.0}) is True
        assert a._reset_evidence("j1", {"state": "garbage",
                                        "last_transition_at": 1100.0}) is False


# --------------------------------------------------------------------------- #
# §68 — the execution-grace budget follows the execution mode
# --------------------------------------------------------------------------- #

@pytest.fixture
def restore_policy():
    saved = runner._POLICY
    yield
    runner._POLICY = saved


class TestExecutionGraceFollowsTheMode:

    def test_simulated_runs_keep_the_wall_time_cap(self, restore_policy):
        runner.configure(mode="simulate")
        a = _agent(Repository(_FakeRedis(), run_id="t"))
        assert a.delegation_exec_grace_s == pytest.approx(Job._WALL_TIME_MAX_S)

    def test_real_runs_use_the_runner_timeout(self, restore_policy):
        runner.configure(mode="real", work_dir="/tmp/x", timeout_s=3600.0)
        a = _agent(Repository(_FakeRedis(), run_id="t"))
        assert a.delegation_exec_grace_s == pytest.approx(3600.0)

    def test_a_mixed_run_takes_the_larger_budget(self, restore_policy):
        runner.configure(mode="real", work_dir="/tmp/x", timeout_s=10.0)
        a = _agent(Repository(_FakeRedis(), run_id="t"))
        assert a.delegation_exec_grace_s == pytest.approx(
            max(10.0, Job._WALL_TIME_MAX_S))

    def test_an_explicit_value_still_wins(self, restore_policy):
        runner.configure(mode="real", work_dir="/tmp/x", timeout_s=3600.0)
        a = _agent(Repository(_FakeRedis(), run_id="t"))
        a.runtime_config = {"delegation_exec_grace_s": 42}
        assert a.delegation_exec_grace_s == 42.0


class TestTheResetIsConditional:
    """Stop-time review of the §18 fix: the sweep reads records and resets later, so a record
    that moved in between must not be overwritten with PENDING."""

    def _agent_with_snapshot(self, repo, job_id, leader, state):
        a = _agent(repo)
        _place(repo, job_id, leader=leader, state=state)
        snapshot = repo.get_many([job_id], key_prefix=Repository.KEY_JOB, level=0, group=0)
        a.failed_agents.set(leader, 100.0)
        return a, snapshot

    def test_a_job_re_won_by_a_live_agent_since_the_read_is_left_alone(self):
        repo = Repository(_FakeRedis(), run_id="t")
        a, snapshot = self._agent_with_snapshot(repo, "j1", leader=7, state=ObjectState.RUNNING)
        # Between the sweep's read and its write a peer reset the job and agent 9 re-won it.
        repo.release_assignment("j1", level=0, group=0)
        _place(repo, "j1", leader=9, state=ObjectState.READY)
        a._reassign_jobs_from_failed_agent(7, records=snapshot)
        assert _state_of(repo, "j1") == (ObjectState.READY.value, 9)
        assert repo.get_assignments(["j1"]) == {"j1": 9}, "the live claim was deleted"
        assert "j1" not in a.metrics.reassignments

    def test_a_job_completed_by_a_misjudged_peer_is_left_alone(self):
        repo = Repository(_FakeRedis(), run_id="t")
        a, snapshot = self._agent_with_snapshot(repo, "j1", leader=7, state=ObjectState.RUNNING)
        j = Job(); j.from_dict(snapshot["j1"]); j.state = ObjectState.COMPLETE
        repo.save(obj=j.to_dict(), key_prefix=Repository.KEY_JOB, level=0, group=0)
        a._reassign_jobs_from_failed_agent(7, records=snapshot)
        assert _state_of(repo, "j1") == (ObjectState.COMPLETE.value, 7)

    def test_an_unchanged_record_is_still_reset(self):
        repo = Repository(_FakeRedis(), run_id="t")
        a, snapshot = self._agent_with_snapshot(repo, "j1", leader=7, state=ObjectState.RUNNING)
        a._reassign_jobs_from_failed_agent(7, records=snapshot)
        assert _state_of(repo, "j1") == (ObjectState.PENDING.value, None)
        assert repo.get_assignments(["j1"]) == {}
