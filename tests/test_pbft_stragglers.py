"""PBFT after quorum: stragglers must not re-enter the engine (2026-09-18 code review).

Found and demonstrated the same day: these two tests failed against the engine as it was, and
now pin the fix. The mechanism they guard against — `on_commit` called `_forget_object` at
finalize, erasing `_commits_sent` for the object, and `select_job` never marked the job decided
locally (the completed set is fed by `schedule_job` ~0.5 s later at a leaf, and at a coordinator
not until the delegation monitor marks the job COMPLETE). So for the stragglers every tier wider
than its quorum produces:

* a late PREPARE found the object (READY in Redis), not "achieved", with empty containers,
  re-adopted the proposal with the sender's wire `prepares` list, found no `_commits_sent` entry
  and broadcast a SECOND COMMIT (scenario A) — the 2026-09-15 COMMIT-inflation defect,
  reintroduced by the forget() that fix added;
* a late COMMIT re-adopted with the sender's wire `commits` list (>= quorum) and finalized AGAIN:
  `finalized_count` and `votes_to_finalize` doubled, and the proposer ran `on_participant_commit`
  with the straggler recorded as leader (scenario B).

Both biased messages-per-job and finalization counts AGAINST PBFT, worst at the coordinator tier
under `coordinator_cost_matrix: self`. The fix is on both sides: the engine remembers finalized
(object, p_id) pairs after the containers are cleared and skips their stragglers
(`ConsensusEngine._finalized`), and the agent marks a job decided at finalize so
`is_agreement_achieved` is true at once (`ResourceAgent._decided_jobs`). A genuinely new election
carries a new p_id, and the host calls `forget_decision` when a job returns to PENDING.
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from swarm.consensus.engine import ConsensusEngine
from swarm.consensus.messages.commit import Commit
from swarm.consensus.messages.prepare import Prepare
from swarm.consensus.messages.proposal_info import ProposalInfo
from swarm.models.agent_info import AgentInfo
from swarm.models.object import ObjectState


class _Obj:
    def __init__(self, object_id="j1"):
        self.object_id = object_id
        self.state = ObjectState.PENDING
        self.leader_id = None
    @property
    def is_commit(self):
        return self.state is ObjectState.COMMIT


class _Host:
    """Mirrors the agent between finalize and schedule_job: the object still exists (READY in
    Redis) and is_agreement_achieved is False — for a coordinator, for the whole delegation."""
    def __init__(self, obj, quorum):
        self.obj, self._quorum = obj, quorum
        self.leader_elected = 0
        self.participant_commits = []
    def get_object(self, oid): return self.obj if oid == self.obj.object_id else None
    def is_agreement_achieved(self, oid): return False
    def calculate_quorum(self): return self._quorum
    def on_leader_elected(self, obj, p_id): self.leader_elected += 1
    def on_participant_commit(self, obj, leader_id, p_id): self.participant_commits.append(leader_id)
    def now(self): return 0.0
    def log_debug(self, m): pass
    def log_info(self, m): pass
    def log_warn(self, m): pass


class _Transport:
    def __init__(self): self.broadcasts = []
    def send(self, dest, payload): pass
    def broadcast(self, payload): self.broadcasts.append(payload)


class _Router:
    def should_forward(self): return False


def _wire(p_id, oid, agent_id, prepares=(), commits=()):
    # A fresh object per message, as JSON decoding would produce, carrying the SENDER's view.
    return ProposalInfo(p_id=p_id, object_id=oid, cost=1.0, agent_id=agent_id,
                        prepares=list(prepares), commits=list(commits))


def _prepare(sender, info):
    return Prepare(source=sender, agents=[AgentInfo(agent_id=sender)], proposals=[info])


def _commit(sender, info):
    return Commit(source=sender, agents=[AgentInfo(agent_id=sender)], proposals=[info])


def _commits_sent(transport):
    return sum(1 for b in transport.broadcasts if isinstance(b, Commit))


def run_scenario():
    # Five agents, quorum 3, agent 1 proposes.
    obj = _Obj("j1")
    host, transport = _Host(obj, quorum=3), _Transport()
    eng = ConsensusEngine(1, host, transport, router=_Router())
    eng.propose([ProposalInfo(p_id="p1", object_id="j1", cost=1.0, agent_id=1)])

    eng.on_prepare(_prepare(2, _wire("p1", "j1", 1, prepares=[1, 2])))
    eng.on_prepare(_prepare(3, _wire("p1", "j1", 1, prepares=[1, 2, 3])))      # quorum -> COMMIT #1
    commits_after_quorum = _commits_sent(transport)

    eng.on_commit(_commit(2, _wire("p1", "j1", 1, commits=[2])))
    eng.on_commit(_commit(3, _wire("p1", "j1", 1, commits=[2, 3])))
    eng.on_commit(_commit(4, _wire("p1", "j1", 1, commits=[2, 3, 4])))          # quorum -> finalize #1
    finalized_after_first = eng.finalized_count
    elected_after_first = host.leader_elected

    # Stragglers, routine in any tier wider than the quorum.
    eng.on_prepare(_prepare(4, _wire("p1", "j1", 1, prepares=[1, 2, 3, 4])))
    eng.on_prepare(_prepare(5, _wire("p1", "j1", 1, prepares=[1, 2, 3, 4, 5])))
    eng.on_commit(_commit(5, _wire("p1", "j1", 1, commits=[2, 3, 4, 5])))

    return dict(
        commits_after_quorum=commits_after_quorum,
        commits_total=_commits_sent(transport),
        finalized_after_first=finalized_after_first,
        finalized_total=eng.finalized_count,
        elected_after_first=elected_after_first,
        elected_total=host.leader_elected,
        participant_commits=host.participant_commits,
    )


def test_a_straggler_prepare_does_not_broadcast_a_second_commit():
    r = run_scenario()
    assert r["commits_after_quorum"] == 1 and r["finalized_after_first"] == 1
    assert r["commits_total"] == 1, r
    assert r["finalized_total"] == 1, r
    assert r["elected_total"] == 1 and not r["participant_commits"], r



def run_scenario_b():
    """All PREPAREs arrive promptly (first phase); COMMITs trickle in. The quorum-th COMMIT
    finalizes; the remaining COMMITs are stragglers whose wire `commits` list is full."""
    obj = _Obj("j1")
    host, transport = _Host(obj, quorum=3), _Transport()
    eng = ConsensusEngine(1, host, transport, router=_Router())
    eng.propose([ProposalInfo(p_id="p1", object_id="j1", cost=1.0, agent_id=1)])
    for s in (2, 3, 4, 5):
        eng.on_prepare(_prepare(s, _wire("p1", "j1", 1, prepares=[1] + list(range(2, s + 1)))))
    eng.on_commit(_commit(2, _wire("p1", "j1", 1, commits=[2])))
    eng.on_commit(_commit(3, _wire("p1", "j1", 1, commits=[2, 3])))
    eng.on_commit(_commit(4, _wire("p1", "j1", 1, commits=[2, 3, 4])))          # finalize #1
    first = (eng.finalized_count, host.leader_elected)
    eng.on_commit(_commit(5, _wire("p1", "j1", 1, commits=[2, 3, 4, 5])))       # straggler
    return dict(first=first, finalized_total=eng.finalized_count,
                elected_total=host.leader_elected, participant_commits=host.participant_commits,
                votes_recorded=eng.votes_to_finalize.count)


def test_a_straggler_commit_does_not_refinalize():
    r = run_scenario_b()
    assert r["first"] == (1, 1)
    assert r["finalized_total"] == 1, r
    assert r["elected_total"] == 1 and not r["participant_commits"], r


def test_a_new_election_for_the_same_object_still_runs():
    """Remembering a decision by (object, p_id) must not block the object's NEXT election —
    a reassignment after the assignee died proposes the same object under a new p_id."""
    obj = _Obj("j1")
    host, transport = _Host(obj, quorum=2), _Transport()
    eng = ConsensusEngine(1, host, transport, router=_Router())
    eng.propose([ProposalInfo(p_id="p1", object_id="j1", cost=1.0, agent_id=1)])
    eng.on_prepare(_prepare(2, _wire("p1", "j1", 1, prepares=[1, 2])))
    eng.on_commit(_commit(2, _wire("p1", "j1", 1, commits=[2])))
    eng.on_commit(_commit(3, _wire("p1", "j1", 1, commits=[2, 3])))
    assert eng.finalized_count == 1

    # The host reset the job to PENDING (assignee died) and a peer proposes it afresh.
    eng.forget_decision("j1")
    eng.on_prepare(_prepare(2, _wire("p2", "j1", 2, prepares=[2])))
    eng.on_prepare(_prepare(3, _wire("p2", "j1", 2, prepares=[2, 3])))
    eng.on_commit(_commit(2, _wire("p2", "j1", 2, commits=[2])))
    eng.on_commit(_commit(3, _wire("p2", "j1", 2, commits=[2, 3])))
    assert host.participant_commits == [2], host.participant_commits
    assert _commits_sent(transport) == 2, "the new election must be allowed its own COMMIT"


def test_a_different_p_id_is_not_mistaken_for_a_straggler_even_without_forget():
    """Keying on (object, p_id) rather than object alone: a competing proposal for the same
    object that arrives after our finalization is still dominance-checked, not skipped as a
    straggler of ours. (It is then dropped by the host's achieved-check in the real agent.)"""
    obj = _Obj("j1")
    host, transport = _Host(obj, quorum=2), _Transport()
    eng = ConsensusEngine(1, host, transport, router=_Router())
    eng.propose([ProposalInfo(p_id="p1", object_id="j1", cost=1.0, agent_id=1)])
    eng.on_prepare(_prepare(2, _wire("p1", "j1", 1, prepares=[1, 2])))
    eng.on_commit(_commit(2, _wire("p1", "j1", 1, commits=[2])))
    eng.on_commit(_commit(3, _wire("p1", "j1", 1, commits=[2, 3])))
    assert eng._already_finalized("j1", "p1")
    assert not eng._already_finalized("j1", "p9")


def test_the_finalized_memory_is_bounded():
    obj = _Obj("j1")
    eng = ConsensusEngine(1, _Host(obj, quorum=1), _Transport(), router=_Router())
    eng._finalized_max = 3
    for i in range(6):
        eng._note_finalized(f"j{i}", f"p{i}", float(i))
    assert len(eng._finalized) == 3
    assert not eng._already_finalized("j0", "p0") and eng._already_finalized("j5", "p5")


# ---------------------------------------------------------------------------------------
# The host side: a job is decided the instant it is finalized, and forgotten when it is
# genuinely up for election again.
# ---------------------------------------------------------------------------------------
import threading
import time
from unittest.mock import MagicMock

from swarm.agents.resource_agent import ResourceAgent, _HostAdapter
from swarm.models.job import Job


def _agent():
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = MagicMock()
    a.agent_id = 1
    a.engine = MagicMock()
    a.queues = MagicMock()
    a.queues.pending_queue.__contains__ = lambda self, jid: False
    a.repository = MagicMock()
    a.repository.get_many.return_value = {}
    a.topology = MagicMock(level=1, group=0)
    a.job_assignments = MagicMock()
    a.pending_proposals, a.pending_prepares, a.pending_commits = {}, {}, {}
    a._init_decision_state()
    return a


def test_a_participant_commit_marks_the_job_decided_at_once():
    a = _agent()
    obj = _Obj("j1")
    assert not a.is_job_completed("j1")
    _HostAdapter(a).on_participant_commit(obj, leader_id=7, proposal_id="p1")
    assert a.is_job_completed("j1"), "achieved must be true before the periodic Redis scan"
    assert "j1" not in a.completed_jobs_set, "decided is not the same as scheduled"


def test_a_leader_election_marks_the_job_decided_before_selecting():
    a = _agent()
    a.select_job = MagicMock()
    _HostAdapter(a).on_leader_elected(_Obj("j1"), "p1")
    assert a.is_job_completed("j1")
    a.select_job.assert_called_once()


def _pending_record(job_id, transitioned_at):
    """What the pending scan fetches from Redis for *job_id*."""
    j = Job(); j.job_id = job_id; j.wall_time = 1.0
    d = j.to_dict(); d["state"] = ObjectState.PENDING.value
    d["last_transition_at"] = transitioned_at
    return d


def test_a_reset_record_is_forgotten_here_and_in_the_engine():
    """A reassignment after the assignee died writes the job back to the pool; every such path
    goes through the `Object.state` setter, so the record's `last_transition_at` is LATER than
    our decision. That is the evidence. If the local memory stayed, every message of the fresh
    election would be skipped as a straggler of the old one."""
    a = _agent()
    a._note_decided("j1")
    a.repository.get_many.return_value = {"j1": _pending_record("j1", time.time() + 60)}
    a._update_pending_jobs(["j1"])
    assert not a.is_job_completed("j1")
    a.engine.forget_decision.assert_called_with("j1")


def test_a_delayed_ready_is_not_a_reset_however_long_it_takes():
    """The record the scan sees is the pre-election one — its transition stamp predates the
    decision — and it stays that way for as long as the leader's READY write is delayed. Redis
    is slow under exactly the load that delays the stragglers, so a timer here re-opened the
    straggler window at the worst moment. Evidence, not elapsed time."""
    a = _agent()
    a._note_decided("j1")
    a._decided_jobs["j1"] -= 3600                                   # decided an hour ago
    a.repository.get_many.return_value = {"j1": _pending_record("j1", time.time() - 7200)}
    a._update_pending_jobs(["j1"])
    assert a.is_job_completed("j1"), "an old PENDING record is not evidence of a reset"
    a.engine.forget_decision.assert_not_called()
    a.queues.pending_queue.add.assert_not_called(), "the local COMMIT object must not be replaced"


def test_no_record_is_no_evidence():
    a = _agent()
    a._note_decided("j1")
    a.repository.get_many.return_value = {}
    a._update_pending_jobs(["j1"])
    assert a.is_job_completed("j1")
    a.engine.forget_decision.assert_not_called()


def test_a_decided_job_held_locally_is_still_fetched_for_its_evidence():
    """`missing` used to skip jobs already in the pending queue — which a participant's decided
    job is, sitting in COMMIT. Without the record there is no evidence to weigh."""
    a = _agent()
    a.queues.pending_queue.__contains__ = lambda self, jid: True
    a._note_decided("j1")
    a.repository.get_many.return_value = {}
    a._update_pending_jobs(["j1"])
    fetched_ids = a.repository.get_many.call_args.args[0]
    assert "j1" in fetched_ids


def test_forget_decision_leaves_the_live_commit_dedupe_alone():
    """The host may call forget_decision from its pending scan while an election is IN FLIGHT.
    It must not erase `_commits_sent`, or the next PREPARE past quorum broadcasts another
    COMMIT — the 2026-09-15 defect, re-enabled by routine housekeeping."""
    obj = _Obj("j1")
    host, transport = _Host(obj, quorum=2), _Transport()
    eng = ConsensusEngine(1, host, transport, router=_Router())
    eng.propose([ProposalInfo(p_id="p1", object_id="j1", cost=1.0, agent_id=1)])
    eng.on_prepare(_prepare(2, _wire("p1", "j1", 1, prepares=[1, 2])))      # quorum -> COMMIT #1
    assert _commits_sent(transport) == 1
    eng.forget_decision("j1")                                                # routine scan
    eng.on_prepare(_prepare(3, _wire("p1", "j1", 1, prepares=[1, 2, 3])))   # a late PREPARE
    assert _commits_sent(transport) == 1, "forget_decision must not re-enable a second COMMIT"


def test_scheduling_promotes_decided_into_the_completed_set():
    a = _agent()
    a._note_decided("j1")
    a._update_completed_jobs(["j1"])
    assert "j1" in a.completed_jobs_set and "j1" not in a._decided_jobs
    assert a.is_job_completed("j1")
