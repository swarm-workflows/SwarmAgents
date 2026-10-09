"""Two winners of one PBFT election at one tier: the second to persist steps aside.

Found in the first E0 cells (2026-10-09): Hier-90 PBFT coordinators 87 and 84 both finalized
job 536 20 s apart — 84's inbox was ~2000 messages behind, and voters that had committed to its
proposal before switching to 87's cheaper one stayed counted in its quorum. Each delegated to its
own group and the job ran twice (8 of 12,000 jobs). `select_job` now persists READY only while
the record does not already name another agent as the decided leader.
"""
import os
import sys
from unittest.mock import MagicMock

import fakeredis

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from swarm.agents.resource_agent import ResourceAgent  # noqa: E402
from swarm.database.repository import Repository  # noqa: E402
from swarm.models.job import Job, ObjectState  # noqa: E402
from swarm.queue.simple_queue import SimpleQueue  # noqa: E402


def _job(jid="536", state=ObjectState.PENDING, leader=None):
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
    a.queues.selected_queue = SimpleQueue()
    a._init_decision_state()
    a.engine = MagicMock()
    return a


def _store(job):
    repo = Repository(fakeredis.FakeStrictRedis(decode_responses=True), run_id="t")
    repo.save(obj=job.to_dict(), key_prefix=Repository.KEY_JOB, level=1, group=0)
    return repo


def _record(repo, jid="536"):
    return repo.get(obj_id=jid, key_prefix=Repository.KEY_JOB, level=1, group=0)


def test_second_winner_does_not_select_a_job_another_leader_holds():
    repo = _store(_job())
    first, second = _agent(87, repo), _agent(84, repo)
    first.queues.pending_queue.add(_job())
    second.queues.pending_queue.add(_job())

    first.select_job(_job(leader=87))
    second.select_job(_job(leader=84))

    assert len(first.queues.selected_queue.gets()) == 1
    assert second.queues.selected_queue.gets() == []
    assert second.lost_leader_races == 1
    second.job_assignments.set.assert_called_with("536", 87)
    assert "536" not in [j.job_id for j in second.queues.pending_queue.gets()]
    rec = _record(repo)
    assert rec["leader_id"] == 87 and rec["state"] == ObjectState.READY.value


def test_a_pending_record_is_selected_as_before():
    repo = _store(_job(leader=12))          # a reset keeps the old leader beside PENDING
    a = _agent(84, repo)
    a.select_job(_job(leader=84))
    assert len(a.queues.selected_queue.gets()) == 1
    assert getattr(a, "lost_leader_races", 0) == 0
    assert _record(repo)["leader_id"] == 84


def test_the_same_leader_rewriting_its_own_record_is_not_a_race():
    repo = _store(_job(state=ObjectState.READY, leader=84))
    a = _agent(84, repo)
    a.select_job(_job(leader=84))
    assert len(a.queues.selected_queue.gets()) == 1


def test_a_withdrawn_record_still_takes_the_withdrawal_path():
    repo = Repository(fakeredis.FakeStrictRedis(decode_responses=True), run_id="t")
    a = _agent(84, repo)
    a.select_job(_job(leader=84))
    assert a.queues.selected_queue.gets() == []
    assert getattr(a, "lost_leader_races", 0) == 0
    a.job_assignments.remove.assert_called_with("536")


def test_decided_by_other_reads_every_terminal_state_and_refuses_to_guess():
    a = _agent(84, None)
    for st in (ObjectState.READY, ObjectState.RUNNING, ObjectState.COMPLETE, ObjectState.FAILED):
        assert a._decided_by_other({"state": st.value, "leader_id": 87})
        assert a._decided_by_other({"state": st.name, "leader_id": "87"})
    assert not a._decided_by_other({"state": ObjectState.PENDING.value, "leader_id": 87})
    assert not a._decided_by_other({"state": ObjectState.READY.value, "leader_id": None})
    assert not a._decided_by_other({"state": ObjectState.READY.value, "leader_id": 84})
    assert not a._decided_by_other({"state": "bogus", "leader_id": 87})
    assert not a._decided_by_other(None)


def test_collector_sums_lost_races():
    from evaluation.collect import execution_evidence
    out = execution_evidence({"1": {"executed_jobs": ["a"], "lost_leader_races": 2},
                              "2": {"executed_jobs": ["b"], "lost_leader_races": 1}})
    assert out["lost_leader_races"] == 3
    assert "lost_leader_races" not in execution_evidence({"1": {"executed_jobs": ["a"]}})
