"""SWIM must not be able to silence a peer that quorum still counts (code review §6).

Two defects, one consequence.

* `broadcast` skipped every peer in SWIM's FAILED set, while `calculate_quorum` went on
  counting `neighbor_map`, which is Redis/heartbeat-derived and still contained that peer. A
  SWIM false positive therefore removed a live voter from every consensus phase without
  lowering the bar it had to clear. Under PBFT the job waited out a reselection timeout; under
  Snow the peer merely abstained. SWIM false-fails precisely under consensus bursts, so the
  regime where this cost the most was the regime it fired most in.
* `_merge_one` compared claims by severity alone, so the ALIVE-at-a-higher-incarnation that
  `_maybe_refute` emits was rejected by every peer and a FAILED verdict was permanent. A
  FAILED peer is never probed again either, so there was no second route back: it stayed out
  of Snow's live sample and the gossip fan-out for the rest of the run.
"""
import os
import sys
from unittest.mock import MagicMock

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from swarm.agents.resource_agent import ResourceAgent  # noqa: E402
from swarm.consensus.messages.membership_update import MembershipUpdate  # noqa: E402
from swarm.membership.swim import (ALIVE, FAILED, SUSPECT, SwimMembership,  # noqa: E402
                                   _Member, _supersedes)
from swarm.models.agent_info import AgentInfo  # noqa: E402
from swarm.utils.thread_safe_dict import ThreadSafeDict  # noqa: E402


# --------------------------------------------------------------------------- #
# broadcast / quorum
# --------------------------------------------------------------------------- #

def _agent(peers, swim_failed=(), heartbeat_failed=()):
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = MagicMock()
    a.agent_id = 1
    a.topology = MagicMock()
    a.topology.peers = list(peers)
    a.topology.group_size = len(peers) + 1
    a.neighbor_map = ThreadSafeDict()
    a.neighbor_map.set(1, AgentInfo(agent_id=1))
    for p in peers:
        if p not in heartbeat_failed:      # heartbeat detection evicts from the map
            a.neighbor_map.set(p, AgentInfo(agent_id=p))
    a.failed_agents = ThreadSafeDict()
    for p in heartbeat_failed:
        a.failed_agents.set(p, 0.0)
    a.swim = MagicMock()
    a.swim.failed_agents.return_value = list(swim_failed)
    a.transport = MagicMock()
    return a


def _broadcast_peers(agent):
    agent.broadcast(MagicMock())
    return sorted(agent.transport.broadcast.call_args.kwargs["peers"])


def test_a_swim_failed_peer_still_receives_consensus_traffic():
    a = _agent(peers=[2, 3, 4], swim_failed=[3])
    assert _broadcast_peers(a) == [2, 3, 4]


def test_a_heartbeat_failed_peer_is_skipped():
    a = _agent(peers=[2, 3, 4], heartbeat_failed=[3])
    assert _broadcast_peers(a) == [2, 4]


def test_we_talk_to_everyone_we_count():
    """The invariant. Peers broadcast to, plus self, is the population quorum is taken over."""
    a = _agent(peers=[2, 3, 4, 5], swim_failed=[3, 4])
    assert len(_broadcast_peers(a)) + 1 == a.live_agent_count


def test_the_skip_set_never_contains_a_peer_quorum_counts():
    a = _agent(peers=[2, 3, 4], swim_failed=[2, 3, 4], heartbeat_failed=[4])
    for peer in a.consensus_skip_set():
        assert peer not in a.neighbor_map


def test_a_swim_false_fail_burst_leaves_quorum_reachable():
    """Seven agents, quorum four. SWIM false-fails four of them — which is what a consensus
    burst does to it. Before the fix only two peers plus self could vote and no job in the
    tier could ever finalize."""
    a = _agent(peers=[2, 3, 4, 5, 6, 7], swim_failed=[4, 5, 6, 7])
    reachable = len(_broadcast_peers(a)) + 1
    assert a.calculate_quorum() == 4
    assert reachable >= a.calculate_quorum()


# --------------------------------------------------------------------------- #
# merge order
# --------------------------------------------------------------------------- #

class TestSupersedes:
    def test_a_refutation_outranks_a_confirmation(self):
        assert _supersedes(ALIVE, 1, FAILED, 0)

    def test_a_stale_claim_is_dropped_however_severe(self):
        assert not _supersedes(FAILED, 1, ALIVE, 3)
        assert not _supersedes(SUSPECT, 1, ALIVE, 3)

    def test_severity_decides_within_one_incarnation(self):
        assert _supersedes(SUSPECT, 0, ALIVE, 0)
        assert _supersedes(FAILED, 0, SUSPECT, 0)
        assert not _supersedes(ALIVE, 0, FAILED, 0)

    def test_a_repeat_of_what_is_held_changes_nothing(self):
        assert not _supersedes(ALIVE, 2, ALIVE, 2)
        assert not _supersedes(FAILED, 2, FAILED, 2)


def _swim(agent_id=1):
    host = MagicMock()
    host.agent_id = agent_id
    host.known_peers.return_value = []
    return SwimMembership(host=host)


def test_a_falsely_failed_peer_comes_back_when_it_refutes():
    sw = _swim()
    with sw._lock:
        sw._members[2] = _Member(agent_id=2, status=FAILED, incarnation=0)

    sw._merge_one(2, ALIVE, 1)          # the refutation `_maybe_refute` emits

    assert sw._members[2].status == ALIVE
    assert 2 in sw.live_agents() and 2 not in sw.failed_agents()
    sw.host.on_agent_alive.assert_called_with(2)


def test_a_stale_alive_does_not_undo_a_confirmation():
    sw = _swim()
    with sw._lock:
        sw._members[2] = _Member(agent_id=2, status=FAILED, incarnation=1)

    sw._merge_one(2, ALIVE, 1)          # same incarnation: word from before the verdict

    assert sw._members[2].status == FAILED


def test_a_stale_failure_rumour_does_not_undo_a_refutation():
    sw = _swim()
    with sw._lock:
        sw._members[2] = _Member(agent_id=2, status=ALIVE, incarnation=3)

    sw._merge_one(2, FAILED, 1)

    assert sw._members[2].status == ALIVE


def test_a_genuine_failure_still_propagates():
    sw = _swim()
    with sw._lock:
        sw._members[2] = _Member(agent_id=2, status=ALIVE, incarnation=0)

    sw._merge_one(2, SUSPECT, 0)
    assert sw._members[2].status == SUSPECT
    sw._merge_one(2, FAILED, 0)
    assert sw._members[2].status == FAILED
    assert sw.failed_agents() == [2]


def test_the_accuser_accepts_the_victims_own_refutation():
    """End to end across two nodes, which is the loop that was broken: the accuser's verdict
    reaches the victim on somebody else's piggyback, the victim bumps its incarnation, and the
    accuser must take that word over its own."""
    accuser = _swim(agent_id=1)
    victim = _swim(agent_id=2)
    with accuser._lock:
        accuser._members[2] = _Member(agent_id=2, status=ALIVE, incarnation=0)
    accuser._mark_failed(2, reason="probe-timeout")
    assert accuser.failed_agents() == [2]

    verdict = [u for u in accuser._fresh_piggy() if u.agent_id == 2]
    assert verdict, "the accuser must gossip its verdict"
    victim._absorb_updates(verdict)                       # victim hears it about itself

    refutation = [u for u in victim._fresh_piggy() if u.agent_id == 2]
    assert refutation and refutation[0].status == ALIVE
    assert refutation[0].incarnation > verdict[0].incarnation

    accuser._absorb_updates(refutation)

    assert accuser._members[2].status == ALIVE
    assert accuser.failed_agents() == []


def test_an_unknown_peer_merges_as_given():
    sw = _swim()
    sw._merge_one(9, FAILED, 0)
    assert sw._members[9].status == FAILED


def test_a_refutation_reaches_snows_live_sample():
    """What the stickiness actually cost: `live_peer_ids` prefers SWIM's live set, so a
    permanently-FAILED peer was dropped from every Snow round for the rest of the run."""
    sw = _swim()
    with sw._lock:
        sw._members[2] = _Member(agent_id=2, status=FAILED, incarnation=0)
        sw._members[3] = _Member(agent_id=3, status=ALIVE, incarnation=0)
    others = lambda: sorted(i for i in sw.live_agents() if i != sw.host.agent_id)
    assert others() == [3]

    sw._merge_one(2, ALIVE, 1)

    assert others() == [2, 3]
