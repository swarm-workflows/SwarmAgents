"""A reselection-timeout reset is written, so peers that witnessed a decision re-vote.

Found in the first E0 cells (2026-10-09): Hier-30 coordinators 26, 27 and 30 finalized jobs 28
and 36 with 29 as leader, while 29 itself never did — it had dropped its own proposal for a
cheaper late one from 28, which then got no votes because the others had decided. At the 300 s
timeout 29 reset the jobs locally only, so the participants never saw reset evidence, skipped
every new proposal, and the jobs sat PENDING until the run retired them as infeasible.
"""
import os
import sys
import time
from unittest.mock import MagicMock

import fakeredis

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from swarm.agents.resource_agent import ResourceAgent  # noqa: E402
from swarm.database.repository import Repository  # noqa: E402
from swarm.models.job import Job, ObjectState  # noqa: E402
from swarm.queue.simple_queue import SimpleQueue  # noqa: E402


def _job(jid="28", state=ObjectState.PENDING, leader=None):
    j = Job()
    j.job_id = jid
    j.wall_time = 1.0
    j.state = state
    j.leader_id = leader
    return j


def _agent(agent_id, repo):
    a = ResourceAgent.__new__(ResourceAgent)
    a.agent_id = agent_id
    a.logger = MagicMock()
    a.repository = repo
    a.topology = MagicMock(level=1, group=0)
    a.job_assignments = MagicMock()
    a.queues = MagicMock()
    a.queues.pending_queue = SimpleQueue()
    a.metrics = MagicMock(restarts={})
    a.engine = MagicMock()
    a.__dict__["runtime_config"] = {"reselection_timeout_s": 300}
    a._init_decision_state()
    return a


def _repo_with(job):
    repo = Repository(fakeredis.FakeStrictRedis(decode_responses=True), run_id="t")
    if job is not None:
        repo.save(obj=job.to_dict(), key_prefix=Repository.KEY_JOB, level=1, group=0)
    return repo


def _record(repo, jid="28"):
    return repo.get(obj_id=jid, key_prefix=Repository.KEY_JOB, level=1, group=0)


def _stuck_in_commit(agent, jid="28", age_s=400):
    j = _job(jid, state=ObjectState.COMMIT)
    j._last_transition_at = time.time() - age_s
    agent.queues.pending_queue.add(j)
    return j


def test_the_reset_is_written_and_a_participant_reads_it_as_reset_evidence():
    original = _job()
    original._last_transition_at = time.time() - 500     # the pre-election PENDING record
    repo = _repo_with(original)
    proposer, participant = _agent(29, repo), _agent(30, repo)
    participant._note_decided("28")
    participant._decided_jobs["28"] = time.time() - 300  # it finalized 300 s ago
    assert not participant._reset_evidence("28", _record(repo))

    _stuck_in_commit(proposer)
    proposer._restart_selection()

    rec = _record(repo)
    assert rec["state"] == ObjectState.PENDING.value
    assert proposer.restarts_persisted == 1
    assert participant._reset_evidence("28", rec)


def test_a_job_another_agent_already_holds_is_not_re_elected():
    repo = _repo_with(_job(state=ObjectState.READY, leader=26))
    a = _agent(29, repo)
    _stuck_in_commit(a)
    a._restart_selection()
    rec = _record(repo)
    assert rec["state"] == ObjectState.READY.value and rec["leader_id"] == 26
    assert "28" not in [j.job_id for j in a.queues.pending_queue.gets()]
    a.job_assignments.set.assert_called_with("28", 26)
    assert a.is_job_completed("28") or "28" in a._decided_jobs
    assert getattr(a, "restarts_persisted", 0) == 0


def test_a_withdrawn_copy_is_not_recreated():
    repo = _repo_with(None)
    a = _agent(29, repo)
    _stuck_in_commit(a)
    a._restart_selection()
    assert not _record(repo)
    assert getattr(a, "restarts_persisted", 0) == 0


def test_a_job_inside_the_timeout_is_left_alone():
    repo = _repo_with(_job())
    a = _agent(29, repo)
    _stuck_in_commit(a, age_s=10)
    a._restart_selection()
    assert getattr(a, "restarts_persisted", 0) == 0


def test_collector_sums_persisted_restarts():
    from evaluation.collect import execution_evidence
    out = execution_evidence({"1": {"executed_jobs": ["a"], "restarts_persisted": 2},
                              "2": {"executed_jobs": ["b"], "restarts_persisted": 1}})
    assert out["restarts_persisted"] == 3
