# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""A quorum of commits is a decision, and the leader is told (2026-10-14).

Steady-arrival pilot, Hier-90 PBFT coordinator tier: six coordinators finalized agent 90's
proposal for jobs 1550, 1648 and 1649 and recorded 90 as leader; 90 never finalized, so nobody
ran them. A cheaper uncommitted proposal had replaced 90's own, after which every COMMIT for its
own proposal was dropped as "worse" (`on_commit`'s dominance check), and `_resolve_split` stood
aside because a quorum existed. The 60 s re-election did not land before the run ended.

Now (1) a COMMIT whose known voters reach quorum finalizes its proposal whatever the receiver
holds, (2) the voters a COMMIT carries count, not just its sender, and (3) a participant that
sees a decision unpersisted past the grace resends the certificate to the leader before it
re-elects."""
import os
import sys
import threading
import time
from unittest.mock import MagicMock

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "tests"))

from swarm.consensus.messages.commit import Commit  # noqa: E402
from swarm.consensus.messages.proposal import Proposal  # noqa: E402
from swarm.consensus.messages.proposal_info import ProposalInfo  # noqa: E402
from swarm.models.agent_info import AgentInfo  # noqa: E402
from swarm.models.object import ObjectState  # noqa: E402

from test_review_2026_10_05_pbft import _cluster  # noqa: E402


def _run_withholding_from(bus, agent):
    """Deliver everything except messages addressed to `agent`; return what was held."""
    held = []
    while bus.queue:
        dest, wire = bus.queue.popleft()
        if dest == agent:
            held.append((dest, wire))
            continue
        bus.queue.appendleft((dest, wire))
        bus.run(limit=1)
    return held


def _decided_without_its_leader(n=5):
    """Agent 1 proposes P; every peer commits and finalizes it with 1 as leader, while
    nothing reaches agent 1."""
    bus, hosts = _cluster(n)
    bus.engines[1].propose([ProposalInfo(p_id="pP", object_id="j1", cost=5.0, agent_id=1)])
    held = _run_withholding_from(bus, 1)
    assert all(hosts[a].assignee.get("j1") == 1 for a in range(2, n + 1))
    assert hosts[1].leader_of == []
    return bus, hosts, held


def _cheaper_proposal_reaches(bus, agent, from_agent=2):
    q = ProposalInfo(p_id="pQ", object_id="j1", cost=1.0, agent_id=from_agent)
    bus.engines[agent].on_proposal(Proposal(source=from_agent,
                                            agents=[AgentInfo(agent_id=from_agent)],
                                            proposals=[q]))
    bus.queue.clear()          # agent 1's PREPARE for Q goes nowhere useful here


class TestCertificateFinalizes:
    def test_a_replaced_proposal_still_finalizes_on_its_commits(self):
        bus, hosts, held = _decided_without_its_leader()
        _cheaper_proposal_reaches(bus, 1)
        assert not bus.engines[1].outgoing.contains(object_id="j1", p_id="pP")
        bus.queue.extend(held)
        bus.run()
        assert hosts[1].leader_of == ["j1"], "the leader must act on its own decision"
        assert bus.engines[1].consensus_stats()["certificate_finalizes"] >= 1

    def test_commits_short_of_quorum_still_defer_to_a_cheaper_proposal(self):
        bus, hosts = _cluster(5)
        bus.engines[1].outgoing.add_proposal(
            ProposalInfo(p_id="pQ", object_id="j1", cost=1.0, agent_id=1))
        weak = ProposalInfo(p_id="pP", object_id="j1", cost=5.0, agent_id=2, commits=[2])
        bus.engines[1].on_commit(Commit(source=2, agents=[AgentInfo(agent_id=2)],
                                        proposals=[weak]))
        assert hosts[1].assignee == {} and hosts[1].leader_of == []
        assert not bus.engines[1].incoming.contains(object_id="j1", p_id="pP")


class TestCertificateResend:
    def test_a_participant_sends_the_certificate_to_a_leader_that_never_heard(self):
        bus, hosts, held = _decided_without_its_leader()
        _cheaper_proposal_reaches(bus, 1)         # and everything sent to 1 is lost
        assert bus.engines[2].resend_certificate("j1")
        bus.run()
        assert hosts[1].leader_of == ["j1"]
        assert bus.engines[2].consensus_stats()["certificates_resent"] == 1

    def test_nothing_to_send_after_a_reset_or_from_the_leader(self):
        bus, hosts, held = _decided_without_its_leader()
        bus.engines[2].forget_decision("j1")
        assert not bus.engines[2].resend_certificate("j1")
        assert not bus.engines[3].resend_certificate("never-seen")


def _stub_agent(decided_ago, resend_ok, grace=60.0):
    from swarm.agents.resource_agent import ResourceAgent
    a = ResourceAgent.__new__(ResourceAgent)
    a.__dict__["runtime_config"] = {"decided_unpersisted_grace_s": grace}
    a.completed_lock = threading.RLock()
    a._decided_jobs = {"j1": time.time() - decided_ago}
    a.engine = MagicMock()
    a.engine.resend_certificate.return_value = resend_ok
    a.logger = MagicMock()
    return a


PENDING = {"job_id": "j1", "state": ObjectState.PENDING.value}


class TestAgentTellsTheLeaderFirst:
    def test_past_the_grace_the_certificate_goes_first(self):
        a = _stub_agent(decided_ago=70, resend_ok=True)
        assert not a._decided_but_unpersisted("j1", PENDING)
        a.engine.resend_certificate.assert_called_once_with("j1")
        assert not a._decided_but_unpersisted("j1", PENDING)      # sent once, then wait
        assert a.engine.resend_certificate.call_count == 1

    def test_still_unpersisted_after_a_second_grace_is_reelected(self):
        a = _stub_agent(decided_ago=70, resend_ok=True)
        a._decided_but_unpersisted("j1", PENDING)
        a._decided_jobs["j1"] = time.time() - 130
        assert a._decided_but_unpersisted("j1", PENDING)
        assert a.decided_unpersisted_resets == 1

    def test_without_a_certificate_it_reelects_at_once(self):
        a = _stub_agent(decided_ago=70, resend_ok=False)
        assert a._decided_but_unpersisted("j1", PENDING)


class TestResetSupersedes:
    def test_delayed_commits_of_a_reset_election_cannot_finalize_it(self):
        bus, hosts, held = _decided_without_its_leader()
        # Agent 2 resets the election; then the old election's COMMITs reach it again.
        hosts[2].decided.discard("j1")
        hosts[2].assignee.clear()
        bus.engines[2].forget_decision("j1")
        for voter in (3, 4, 5):
            old = ProposalInfo(p_id="pP", object_id="j1", cost=5.0, agent_id=1,
                               commits=[voter])
            bus.engines[2].on_commit(Commit(source=voter, agents=[AgentInfo(agent_id=voter)],
                                            proposals=[old]))
        assert "j1" not in hosts[2].assignee
        assert bus.engines[2].consensus_stats()["superseded_skips"] == 3

    def test_a_new_election_that_arrived_first_is_not_cancelled(self):
        bus, hosts, held = _decided_without_its_leader()
        new = ProposalInfo(p_id="pR", object_id="j1", cost=2.0, agent_id=3)
        bus.engines[2].incoming.add_proposal(new)      # proposed by a peer that reset first
        bus.engines[2].forget_decision("j1")
        assert ("j1", "pR") not in bus.engines[2]._superseded
        assert ("j1", "pP") in bus.engines[2]._superseded
