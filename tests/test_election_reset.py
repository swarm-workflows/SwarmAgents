"""A reset election is fully reset: no stale Snow instance, no stale local winner.

Found in the first E1′ bandit cells (2026-10-10). A coordinator took back a job its child group
never placed: it released the claim and forgot the decision, but its `job_assignments` entry —
and its peers' — still named it, so every Snow answer was an "already decided for 88" hint,
which is not a vote; every round came back empty and the vote was abandoned every reselection
period until the 3,600 s cap (6-8 jobs per run, 4 runs capped). Separately, the old election's
Snow instance kept running after a reset, holding an in-flight slot for up to 1,008 s while
`propose` skipped the new election as "already in flight".
"""
import os
import sys
from unittest.mock import MagicMock

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "tests"))

from swarm.agents.resource_agent import ResourceAgent, _HostAdapter  # noqa: E402
from swarm.consensus.messages.proposal_info import ProposalInfo  # noqa: E402
from swarm.utils.thread_safe_dict import ThreadSafeDict  # noqa: E402
from test_snow import _make_engine  # noqa: E402


def _prop(job="job-1", pid="p-1"):
    return ProposalInfo(p_id=pid, object_id=job, cost=1.0, agent_id="1")


def test_forget_decision_cancels_the_running_instance_and_frees_its_slot():
    eng, _host, _t, _cas = _make_engine()
    eng.propose([_prop()])
    eng._tick(now=0.0)                      # round 1 in flight
    assert "job-1" in eng._states
    eng.forget_decision("job-1")
    assert "job-1" not in eng._states
    assert not eng.outgoing.contains(object_id="job-1", p_id="p-1")
    assert eng.consensus_stats()["cancelled"] == 1
    assert eng.consensus_stats()["abandoned"] == 0


def test_after_a_reset_a_fresh_proposal_starts_a_new_instance():
    eng, _host, _t, _cas = _make_engine()
    eng.propose([_prop(pid="old")])
    eng.forget_decision("job-1")
    eng.propose([_prop(pid="new")])
    assert eng._states["job-1"].proposal.p_id == "new"


def test_forgetting_an_unknown_job_is_harmless():
    eng, _host, _t, _cas = _make_engine()
    eng.forget_decision("never-seen")
    assert eng.consensus_stats()["cancelled"] == 0


def _agent():
    a = ResourceAgent.__new__(ResourceAgent)
    a.agent_id = 88
    a.logger = MagicMock()
    a.job_assignments = ThreadSafeDict()
    a._init_decision_state()
    a.engine = MagicMock()
    return a


def test_forgetting_a_decision_drops_the_local_winner_peers_are_told_about():
    a = _agent()
    host = _HostAdapter(a)
    a._note_decided("1350")
    a.job_assignments.set("1350", 88)
    assert host.get_assignment_local("1350") == 88
    a._forget_decided("1350")
    assert host.get_assignment_local("1350") is None
    a.engine.forget_decision.assert_called_with("1350")


def test_a_snow_peer_stops_answering_already_decided_once_the_decision_is_forgotten():
    a = _agent()
    host = _HostAdapter(a)
    eng, _h, _t, _c = _make_engine(agent_id=88)
    eng.host = host
    a.engine = eng
    host.my_cost_for_job = lambda _jid: 5.0
    a.job_assignments.set("1350", 88)
    assert eng._answer_query("q1", "1350", 89, 1.0)["already_decided"] is True
    a._forget_decided("1350")
    ans = eng._answer_query("q2", "1350", 89, 1.0)
    assert ans["already_decided"] is False and ans["preferred_agent"] == 89
