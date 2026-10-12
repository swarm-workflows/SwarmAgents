"""One job, one decision: the PBFT sticky COMMIT, the decided-but-unpersisted recovery, and the
Snow per-round re-pricing (2026-10-12).

PBFT  A voter that had sent COMMIT for one proposal still switched to a cheaper late one and
      committed again, its first COMMIT never withdrawn, so one voter counted toward two
      quorums and two proposals for one job both finalized. In the first E0 cells that ran 8
      jobs twice; in Hier-90 PBFT run05 it finalized job 1063 for 84 at agent 83 and for 83
      everywhere else, so neither ran it. `ConsensusEngine._committed_to` makes a COMMIT
      binding until the election ends or is reset.
Snow  The advertised cost was frozen at proposal time while peers compared it with their
      CURRENT cost; once the initiator's load fell, a peer in the gap voted for itself every
      round and, with two peers, no candidate ever reached α. Every failed round of 429
      abandons in a Hier-90 Hybrid+LinUCB run was such a split.
"""
import json
import os
import sys
import time
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "tests"))

from swarm.consensus.engine import ConsensusEngine  # noqa: E402
from swarm.consensus.messages.commit import Commit  # noqa: E402
from swarm.consensus.messages.message_builder import MessageBuilder  # noqa: E402
from swarm.consensus.messages.proposal_info import ProposalInfo  # noqa: E402
from swarm.models.object import ObjectState  # noqa: E402

from test_review_2026_10_05_pbft import _cluster  # noqa: E402
from test_snow import _make_engine, _latest_query_item  # noqa: E402


def _run_holding_commits(bus, release_to=()):
    """Deliver everything except COMMITs addressed to agents outside `release_to`, which are
    held back — the inbox lag that let a late cheaper proposal overtake a decided one."""
    held = []
    while bus.queue:
        dest, wire = bus.queue.popleft()
        msg = MessageBuilder.from_dict(json.loads(wire))
        if isinstance(msg, Commit) and dest not in release_to:
            held.append((dest, wire))
            continue
        bus.queue.appendleft((dest, wire))
        bus.run(limit=1)
    return held


def _race(n=5):
    bus, hosts = _cluster(n)
    e = bus.engines
    # Agent 1 proposes A; every voter reaches prepare quorum and COMMITs it, but only agent 1
    # receives the COMMITs in time, and finalizes A as leader.
    e[1].propose([ProposalInfo(p_id="pA", object_id="j1", cost=5.0, agent_id=1)])
    held = _run_holding_commits(bus, release_to=(1,))
    assert hosts[1].leader_of == ["j1"]
    # A cheaper proposal B from agent 5 arrives everywhere before the held COMMITs.
    e[5].propose([ProposalInfo(p_id="pB", object_id="j1", cost=1.0, agent_id=5)])
    bus.run()
    bus.queue.extend(held)
    bus.run()
    return bus, hosts


class TestStickyCommit:
    def test_two_quorums_cannot_form(self):
        _bus, hosts = _race()
        leaders = [aid for aid, h in hosts.items() if "j1" in h.leader_of]
        recorded = {h.assignee.get("j1") for h in hosts.values() if "j1" in h.assignee}
        assert leaders == [1]
        assert recorded == {1}, f"participants recorded {recorded}"

    def test_held_prepare_quorum_is_counted(self):
        bus, _hosts = _race()
        assert sum(eng.consensus_stats()["commit_lock_holds"] for eng in bus.engines.values()) > 0

    def test_one_batch_cannot_commit_two_proposals(self):
        bus, _hosts = _cluster(3)
        eng = bus.engines[2]
        a = ProposalInfo(p_id="pA", object_id="j1", cost=5.0, agent_id=1, prepares=[1, 2, 3])
        b = ProposalInfo(p_id="pB", object_id="j1", cost=1.0, agent_id=3, prepares=[1, 2, 3])
        eng._commit_where_prepared([a, b])
        sent = [json.loads(w) for _d, w in bus.queue]
        committed = {p["p_id"] for m in sent for p in m["proposals"]}
        assert committed == {"pA"}
        assert eng._committed_to["j1"] == "pA"

    def test_committed_proposal_is_not_dropped_as_worse(self):
        bus, hosts = _cluster(3)
        eng = bus.engines[2]
        eng._committed_to["j1"] = "pA"
        a = ProposalInfo(p_id="pA", object_id="j1", cost=5.0, agent_id=1)
        eng.incoming.add_proposal(a)
        eng._remove_worse_proposals_from_container(
            ProposalInfo(p_id="pB", object_id="j1", cost=1.0, agent_id=3), eng.incoming)
        assert eng.incoming.contains(object_id="j1", p_id="pA")

    def test_reset_releases_the_commit(self):
        bus, _hosts = _cluster(3)
        eng = bus.engines[2]
        eng._committed_to["j1"] = "pA"
        eng.forget_decision("j1")
        assert "j1" not in eng._committed_to

    def test_finalize_releases_the_commit(self):
        bus, hosts = _cluster(3)
        bus.engines[1].propose([ProposalInfo(p_id="pA", object_id="j1", cost=1.0, agent_id=1)])
        bus.run()
        assert all("j1" in h.decided for h in hosts.values())
        assert all("j1" not in eng._committed_to for eng in bus.engines.values())


def _deadlock(n=3, silent=()):
    """Every agent commits to its OWN proposal — prepares from itself and one peer each — so
    the commits split 1/1/1 and no proposal can reach a quorum of 2."""
    bus, hosts = _cluster(n)
    for h in hosts.values():
        h.live_voter_ids = (lambda h=h: set(h.live))
    bus.silent = set(silent)
    costs = {1: 5.0, 2: 3.0, 3: 4.0}
    for aid, eng in bus.engines.items():
        nxt = aid % n + 1
        p = ProposalInfo(p_id=f"p{aid}", object_id="j1", cost=costs[aid], agent_id=aid,
                         prepares=[aid, nxt])
        eng.outgoing.add_proposal(p)
        eng._commit_where_prepared([p])
    bus.run()
    return bus, hosts


class TestCommitSplit:
    def test_a_full_split_moves_every_commit_to_the_best_proposal(self):
        bus, hosts = _deadlock()
        leaders = [aid for aid, h in hosts.items() if "j1" in h.leader_of]
        assert leaders == [2], "the cheapest committed proposal is agent 2's"
        assert all(h.assignee.get("j1", 2) == 2 for h in hosts.values())
        assert sum(e.consensus_stats()["commit_splits_resolved"] for e in bus.engines.values()) >= 2

    def test_a_missing_commit_is_not_a_split(self):
        bus, hosts = _deadlock(silent=(3,))
        assert not any("j1" in h.leader_of for h in hosts.values())
        assert all(e.commit_splits_resolved == 0 for e in bus.engines.values())

    def test_a_commit_from_outside_the_live_set_does_not_complete_it(self):
        bus, hosts = _cluster(3)
        eng = bus.engines[1]
        hosts[1].live_voter_ids = lambda: {1, 2, 3}
        for aid in (1, 2, 4):                       # 3 is live and unheard; 4 is not live
            eng._ledger_note(ProposalInfo(p_id=f"p{aid}", object_id="j1", cost=1.0,
                                          agent_id=aid), [aid])
        eng._resolve_split("j1")
        assert eng.commit_splits_resolved == 0

    def test_one_id_in_two_types_counts_once(self):
        bus, hosts = _cluster(3)
        eng = bus.engines[1]
        hosts[1].live_voter_ids = lambda: {1, 2, 3}
        eng._ledger_note(ProposalInfo(p_id="p1", object_id="j1", cost=1.0, agent_id=1), [1, "2"])
        eng._ledger_note(ProposalInfo(p_id="p2", object_id="j1", cost=2.0, agent_id=2), [2])
        eng._resolve_split("j1")
        assert eng.commit_splits_resolved == 0

    def test_no_resolution_without_a_live_count(self):
        bus, hosts = _cluster(3)
        eng = bus.engines[1]
        for aid in (1, 2, 3):
            eng._ledger_note(ProposalInfo(p_id=f"p{aid}", object_id="j1", cost=1.0,
                                          agent_id=aid), [aid])
        eng._resolve_split("j1")
        assert eng.commit_splits_resolved == 0


class TestSnowRepricing:
    def test_own_proposal_is_repriced_each_round(self):
        eng, host, transport, _ = _make_engine(agent_id=1, peers=(2, 3), my_cost=0.55, k=2)
        eng.propose([ProposalInfo(p_id="p1", object_id="job-1", cost=0.63, agent_id="1")])
        eng._tick(now=0.0)
        assert _latest_query_item(transport, "job-1")["preferred_cost"] == 0.55
        host._my_cost = 0.40
        eng._tick(now=1.0)            # round 1 times out
        eng._tick(now=1.0)            # round 2 is armed
        assert _latest_query_item(transport, "job-1")["preferred_cost"] == 0.40

    def test_a_peers_candidate_is_not_repriced(self):
        eng, host, transport, _ = _make_engine(agent_id=1, peers=(2, 3), my_cost=0.55, k=2)
        eng.propose([ProposalInfo(p_id="p1", object_id="job-1", cost=0.63, agent_id="1")])
        state = eng._states["job-1"]
        state.preferred, state.preferred_cost = 3, 0.2
        eng._tick(now=0.0)
        item = _latest_query_item(transport, "job-1")
        assert (item["preferred_agent"], item["preferred_cost"]) == (3, 0.2)

    def test_unknown_own_cost_keeps_the_advertised_one(self):
        eng, host, transport, _ = _make_engine(agent_id=1, peers=(2, 3), my_cost=None, k=2)
        eng.propose([ProposalInfo(p_id="p1", object_id="job-1", cost=0.63, agent_id="1")])
        eng._tick(now=0.0)
        assert _latest_query_item(transport, "job-1")["preferred_cost"] == 0.63


def _stub_agent(decided_ago, grace=60.0, snow=False):
    from swarm.agents.resource_agent import ResourceAgent
    from swarm.consensus.gossip_engine import GossipConsensusEngine
    a = ResourceAgent.__new__(ResourceAgent)
    a.__dict__["runtime_config"] = {"decided_unpersisted_grace_s": grace}
    a.completed_lock = threading.RLock()
    a._decided_jobs = {} if decided_ago is None else {"j1": time.time() - decided_ago}
    a.engine = MagicMock(spec=GossipConsensusEngine) if snow else MagicMock()
    a.logger = MagicMock()
    return a


PENDING = {"job_id": "j1", "state": ObjectState.PENDING.value}


class TestDecidedButUnpersisted:
    def test_stale_decision_on_a_pending_record_is_reelected(self):
        a = _stub_agent(decided_ago=90)
        assert a._decided_but_unpersisted("j1", PENDING)
        assert a.decided_unpersisted_resets == 1

    def test_within_grace_is_left_alone(self):
        assert not _stub_agent(decided_ago=10)._decided_but_unpersisted("j1", PENDING)

    def test_a_record_past_pending_is_left_alone(self):
        rec = {"job_id": "j1", "state": ObjectState.READY.value}
        assert not _stub_agent(decided_ago=90)._decided_but_unpersisted("j1", rec)

    def test_grace_zero_disables(self):
        assert not _stub_agent(decided_ago=900, grace=0)._decided_but_unpersisted("j1", PENDING)

    def test_snow_is_excluded(self):
        assert not _stub_agent(decided_ago=900, snow=True)._decided_but_unpersisted("j1", PENDING)

    def test_undecided_is_left_alone(self):
        assert not _stub_agent(decided_ago=None)._decided_but_unpersisted("j1", PENDING)
