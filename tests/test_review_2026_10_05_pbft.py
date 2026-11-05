"""Code review 2026-10-05 §5-§6: PBFT, driven as a cluster of real engines.

Every PBFT test before this file drove ONE engine with a fake host whose quorum was a constant,
and fed it hand-built messages. That is how both defects below survived: neither is visible
until several engines talk to each other.

§5  An agent's own PREPARE and COMMIT were never counted. Votes were appended only when they
    ARRIVED from a peer (`msg.agents[0]`), and `broadcast` sends to `topology.peers`, which
    excludes self — while `calculate_quorum` is a majority of `neighbor_map`, which INCLUDES self.
    So an agent needed q votes from the n-1 others instead of q-1: one fewer crash tolerated than
    a majority quorum promises. A 2-agent group never finalized; a 3-agent group with one peer
    silent (or evicted after a failure, leaving 2 live) stalled forever, every job cycling
    through the reselection timeout.
§6  The leader branch asked `outgoing.contains(...)` as well as `proposal.agent_id == self`.
    `_restart_selection` and `_clear_consensus_for_failed_agent` both remove an agent's own
    proposal from `outgoing` while its election is still live; when late votes then carried it
    to quorum it finalized on the PARTICIPANT branch with leader = self — `select_job` never ran,
    every peer recorded this agent as the assignee, and nobody ran the job.
"""
import json
import os
import sys
from collections import deque

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from swarm.consensus.engine import ConsensusEngine  # noqa: E402
from swarm.consensus.messages.commit import Commit  # noqa: E402
from swarm.consensus.messages.message_builder import MessageBuilder  # noqa: E402
from swarm.consensus.messages.prepare import Prepare  # noqa: E402
from swarm.consensus.messages.proposal import Proposal  # noqa: E402
from swarm.consensus.messages.proposal_info import ProposalInfo  # noqa: E402
from swarm.models.object import ObjectState  # noqa: E402


class _Obj:
    def __init__(self, object_id):
        self.object_id = object_id
        self.state = ObjectState.PENDING
        self.leader_id = None

    @property
    def is_commit(self):
        return self.state is ObjectState.COMMIT


class _Host:
    """One agent's view. `live` mirrors `neighbor_map`, which contains the agent itself."""

    def __init__(self, agent_id, live):
        self.agent_id = agent_id
        self.live = set(live)
        self.objects = {}
        self.decided = set()
        self.leader_of = []          # object ids this agent was elected leader for
        self.assignee = {}           # object id -> leader recorded as a participant

    def add(self, oid):
        self.objects[oid] = _Obj(oid)

    # ConsensusHost
    def get_object(self, oid): return self.objects.get(oid)
    def is_agreement_achieved(self, oid): return oid in self.decided
    def calculate_quorum(self): return len(self.live) // 2 + 1   # = Agent.calculate_quorum
    def on_leader_elected(self, obj, p_id):
        self.decided.add(obj.object_id)
        self.leader_of.append(obj.object_id)
    def on_participant_commit(self, obj, leader_id, p_id):
        self.decided.add(obj.object_id)
        self.assignee[obj.object_id] = leader_id
    def now(self): return 0.0
    def log_debug(self, m): pass
    def log_info(self, m): pass
    def log_warn(self, m): pass


class _Bus:
    """FIFO delivery to every OTHER engine, the way `Agent.broadcast` sends to topology.peers.
    Silent agents are counted by everyone (they are still in neighbor_map) but never deliver
    or receive: a crash the detector has not acted on yet."""

    def __init__(self):
        self.engines = {}
        self.silent = set()
        self.queue = deque()

    def transport_for(self, agent_id):
        bus = self

        class _T:
            # Encoded at send time, decoded per receiver — exactly GrpcTransport and the agent's
            # inbound consumer — so no two engines ever share a ProposalInfo, and a vote list is
            # the SENDER's view as of sending, as on the wire.
            def send(self, dest, payload):
                bus.queue.append((dest, json.dumps(payload.to_dict())))

            def broadcast(self, payload):
                wire = json.dumps(payload.to_dict())
                for dest in bus.engines:
                    if dest != agent_id:
                        bus.queue.append((dest, wire))
        return _T()

    def run(self, limit=10_000):
        n = 0
        while self.queue and n < limit:
            dest, wire = self.queue.popleft()
            msg = MessageBuilder.from_dict(json.loads(wire))
            n += 1
            if dest in self.silent or msg.agents[0].agent_id in self.silent:
                continue
            eng = self.engines[dest]
            # Prepare and Commit SUBCLASS Proposal, so the order matters — it is the order of
            # ResourceAgent's inbound dispatch.
            if isinstance(msg, Prepare):
                eng.on_prepare(msg)
            elif isinstance(msg, Commit):
                eng.on_commit(msg)
            elif isinstance(msg, Proposal):
                eng.on_proposal(msg)
        return n


class _Router:
    def should_forward(self): return False


def _cluster(n, live=None, silent=(), objects=("j1",)):
    bus = _Bus()
    ids = list(range(1, n + 1))
    hosts = {}
    for aid in ids:
        h = _Host(aid, live if live is not None else ids)
        for oid in objects:
            h.add(oid)
        hosts[aid] = h
        bus.engines[aid] = ConsensusEngine(aid, h, bus.transport_for(aid), router=_Router())
    bus.silent = set(silent)
    return bus, hosts


def _propose(bus, proposer, oid="j1", cost=1.0, p_id=None):
    p = ProposalInfo(p_id=p_id or f"p-{proposer}-{oid}", object_id=oid, cost=cost,
                     agent_id=proposer)
    bus.engines[proposer].propose([p])
    return p


# --------------------------------------------------------------------------- #
# §5 — own votes count
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("n,silent", [
    (2, ()),            # quorum 2: proposer + the one peer
    (3, (3,)),          # quorum 2 with one peer down: proposer + one live peer
    (4, (4,)),          # quorum 3 with one down: proposer + two live peers
    (5, (4, 5)),        # quorum 3 with two down
    (7, (5, 6, 7)),     # quorum 4 with three down
])
def test_a_majority_finalizes_with_the_minority_silent(n, silent):
    """Exactly a majority alive, the rest crashed but not yet evicted. Each of these stalled."""
    bus, hosts = _cluster(n, silent=silent)
    _propose(bus, 1)
    bus.run()
    assert hosts[1].leader_of == ["j1"], f"n={n} silent={silent}: proposer never finalized"
    for aid in hosts:
        if aid != 1 and aid not in bus.silent:
            assert hosts[aid].assignee.get("j1") == 1


def test_one_fewer_than_a_majority_does_not_finalize():
    """The other side of the boundary: quorum must still mean a majority."""
    bus, hosts = _cluster(5, silent=(3, 4, 5))       # 2 of 5 alive, quorum 3
    _propose(bus, 1)
    bus.run()
    assert hosts[1].leader_of == []
    assert hosts[2].assignee == {}


def test_a_group_shrunk_to_two_by_eviction_still_works():
    """3-agent group, one evicted by heartbeat: live set {1, 2}, quorum 2."""
    bus, hosts = _cluster(3, live=[1, 2], silent=(3,))
    _propose(bus, 1)
    bus.run()
    assert hosts[1].leader_of == ["j1"]
    assert hosts[2].assignee == {"j1": 1}


def test_an_isolated_agent_finalizes_alone():
    """`calculate_quorum` of a live set of one is 1, as CLAUDE.md documents. The agent could
    still never finalize, because the only vote that could reach 1 was its own."""
    bus, hosts = _cluster(1)
    _propose(bus, 1)
    bus.run()
    assert hosts[1].leader_of == ["j1"]


def test_full_cluster_finalizes_once_everywhere():
    bus, hosts = _cluster(5)
    _propose(bus, 3)
    bus.run()
    assert hosts[3].leader_of == ["j1"]
    assert {aid: h.assignee.get("j1") for aid, h in hosts.items() if aid != 3} == \
        {1: 3, 2: 3, 4: 3, 5: 3}
    assert sum(e.finalized_count for e in bus.engines.values()) == 1   # proposer only


def test_each_agent_sends_at_most_one_commit_per_proposal():
    """Counting own votes must not reintroduce the COMMIT inflation fixed 2026-09-15."""
    bus, hosts = _cluster(7)
    sent = []
    for aid, eng in bus.engines.items():
        orig = eng.transport.broadcast

        def wrapped(payload, _aid=aid, _orig=orig):
            if isinstance(payload, Commit):
                sent.append(_aid)
            _orig(payload)
        eng.transport.broadcast = wrapped
    _propose(bus, 1)
    bus.run()
    assert sorted(sent) == sorted(set(sent)), f"duplicate COMMITs: {sent}"


def test_competing_proposals_still_resolve_to_the_cheaper():
    bus, hosts = _cluster(5)
    _propose(bus, 1, cost=5.0)
    _propose(bus, 2, cost=1.0)
    bus.run()
    leaders = [aid for aid, h in hosts.items() if h.leader_of]
    assert leaders == [2]


def test_votes_counted_once_each():
    """Own votes are appended idempotently — an agent never counts itself twice."""
    bus, hosts = _cluster(3)
    seen = {}
    eng = bus.engines[2]
    orig = eng.host.on_participant_commit

    def spy(obj, leader, p_id):
        for c in (eng.incoming, eng.outgoing):
            pr = c.get_proposal(p_id=p_id)
            if pr is not None:
                seen["prepares"], seen["commits"] = list(pr.prepares), list(pr.commits)
        orig(obj, leader, p_id)
    eng.host.on_participant_commit = spy
    _propose(bus, 1)
    bus.run()
    assert seen and len(seen["prepares"]) == len(set(seen["prepares"]))
    assert len(seen["commits"]) == len(set(seen["commits"]))


# --------------------------------------------------------------------------- #
# §6 — the leader is whoever proposed, whatever container the proposal is in
# --------------------------------------------------------------------------- #

def test_own_proposal_dropped_from_outgoing_still_elects_self_as_leader():
    """`_restart_selection` / `_clear_consensus_for_failed_agent` remove the agent's own entry
    from `outgoing` mid-election. Late votes then carry it to quorum. Before the fix the agent
    took the participant branch, recorded ITSELF as a remote leader, and never ran the job."""
    bus, hosts = _cluster(3)
    p = _propose(bus, 1)
    # Peers get the proposal and answer; before their votes land, agent 1 drops its entry.
    for _ in range(2):                       # deliver the two PROPOSAL copies
        dest, wire = bus.queue.popleft()
        bus.engines[dest].on_proposal(MessageBuilder.from_dict(json.loads(wire)))
    bus.engines[1].outgoing.remove_object(object_id=p.object_id)
    bus.run()
    assert hosts[1].leader_of == ["j1"]
    assert "j1" not in hosts[1].assignee


# --------------------------------------------------------------------------- §9
def test_concurrent_finalizes_of_one_proposal_finalize_once():
    """The engine is driven from three threads; two that both saw quorum both finalized."""
    import threading
    bus, hosts = _cluster(3)
    eng = bus.engines[1]
    p = ProposalInfo(p_id="px", object_id="j1", cost=1.0, agent_id=1,
                     prepares=[1, 2, 3], commits=[1, 2, 3])
    eng.outgoing.add_proposal(p)
    obj = hosts[1].objects["j1"]
    barrier = threading.Barrier(8)
    # Widen the window deterministically: in the unlocked engine the quorum lookup sat between
    # the "already finalized?" check and the mark, so a host call that yields let every thread
    # through. Under the GIL the natural window is too narrow to hit reliably.
    import time as _time
    real_quorum = hosts[1].calculate_quorum
    hosts[1].calculate_quorum = lambda: (_time.sleep(0.05), real_quorum())[1]

    def go():
        barrier.wait()
        eng._finalize_if_quorum(obj, p)
    threads = [threading.Thread(target=go) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert hosts[1].leader_of == ["j1"]
    assert eng.finalized_count == 1
