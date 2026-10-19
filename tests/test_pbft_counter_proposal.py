"""PBFT counter-proposals: a coordinator answers a dearer proposal with its own (2026-10-19).

Under `coordinator_cost_matrix: self` every coordinator is a candidate for every job but proposes
only itself, and PBFT's `on_proposal` PREPAREd the first proposal an agent heard for a job it had
not proposed — whatever it cost — after which its selection loop never proposed the job. The
coordinator tier became a Redis-scan race: on the slice the coordinator nearest the database won
55-73 % of jobs and one coordinator won 95-99 % of every one-second batch, while the cheapest
coordinator (the far host) won 6-10 % (docs/results/e0-e1-f45d9ff1/README.md §7).

Now an agent with no proposal of its own asks the host for its price (`counter_proposal_cost`)
and, if it dominates the incoming proposal, proposes itself instead of preparing — on the
PROPOSAL path and on the adopt-via-PREPARE path. These tests run real `ConsensusEngine`s over a
bus that delivers by arrival time, so distance matters as it does on the slice.
"""
import heapq
import itertools
import json
import os
import random
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "tests"))

from swarm.consensus.engine import ConsensusEngine  # noqa: E402
from swarm.consensus.messages.commit import Commit  # noqa: E402
from swarm.consensus.messages.message_builder import MessageBuilder  # noqa: E402
from swarm.consensus.messages.prepare import Prepare  # noqa: E402
from swarm.consensus.messages.proposal import Proposal  # noqa: E402
from swarm.consensus.messages.proposal_info import ProposalInfo  # noqa: E402

from test_review_2026_10_05_pbft import _Host, _Router  # noqa: E402


class _CostHost(_Host):
    """A host that knows its own price for every job (None = the old behaviour)."""

    def __init__(self, agent_id, live, cost=None):
        super().__init__(agent_id, live)
        self.cost = cost
        self.countered = []

    def counter_proposal_cost(self, oid):
        return self.cost

    def on_counter_proposals(self, proposals):
        self.countered.extend(p.object_id for p in proposals)


class _OldHost(_Host):
    """No `counter_proposal_cost`: the engine must behave exactly as before."""


class _DelayBus:
    """Delivers each message at send time + the one-way delay of its link, in time order."""

    def __init__(self, delay):
        self.delay = delay            # (src, dst) -> seconds
        self.engines = {}
        self.now = 0.0
        self.heap = []
        self._seq = itertools.count()

    def transport_for(self, agent_id):
        bus = self

        class _T:
            def send(self, dest, payload):
                bus._push(agent_id, dest, payload)

            def broadcast(self, payload):
                for dest in bus.engines:
                    if dest != agent_id:
                        bus._push(agent_id, dest, payload)
        return _T()

    def _push(self, src, dst, payload):
        at = self.now + self.delay(src, dst)
        heapq.heappush(self.heap, (at, next(self._seq), dst, json.dumps(payload.to_dict())))

    def run(self, until=None, limit=100_000):
        n = 0
        while self.heap and n < limit:
            if until is not None and self.heap[0][0] > until:
                break
            at, _s, dst, wire = heapq.heappop(self.heap)
            self.now = at
            msg = MessageBuilder.from_dict(json.loads(wire))
            eng = self.engines[dst]
            if isinstance(msg, Prepare):
                eng.on_prepare(msg)
            elif isinstance(msg, Commit):
                eng.on_commit(msg)
            elif isinstance(msg, Proposal):
                eng.on_proposal(msg)
            n += 1
        return n


def _cluster(costs, delay, old=False, objects=("j1",)):
    bus = _DelayBus(delay)
    ids = sorted(costs)
    hosts = {}
    for aid in ids:
        h = _OldHost(aid, ids) if old else _CostHost(aid, ids, costs[aid])
        for oid in objects:
            h.add(oid)
        hosts[aid] = h
        bus.engines[aid] = ConsensusEngine(aid, h, bus.transport_for(aid), router=_Router())
    return bus, hosts


def _propose(bus, aid, cost, oid="j1", at=0.0):
    bus.run(until=at)
    bus.now = max(bus.now, at)
    bus.engines[aid].propose([ProposalInfo(p_id=f"p{aid}-{oid}", object_id=oid, cost=cost,
                                           agent_id=aid)])


def _outcome(hosts, oid="j1"):
    leaders = [aid for aid, h in hosts.items() if oid in h.leader_of]
    recorded = {h.assignee[oid] for h in hosts.values() if oid in h.assignee}
    return leaders, recorded


#: The slice's LinUCB coordinator tier (README §7): 90 next to the database and the dearest
#: (smallest subtree), 88 13.6 ms away, 89 the far host and the cheapest. One-way = RTT / 2.
SLICE_RTT_MS = {(90, 88): 13.6, (90, 89): 74.0, (88, 89): 79.9}
SLICE_COSTS = {90: 1.32, 88: 1.13, 89: 1.00}


def _slice_delay(a, b):
    return SLICE_RTT_MS.get((a, b), SLICE_RTT_MS.get((b, a))) / 2000.0


# --------------------------------------------------------------------------- #
# the race, before and after
# --------------------------------------------------------------------------- #

class TestTheRace:
    def test_without_the_hook_the_first_proposer_wins(self):
        """The old behaviour, pinned: the dearest coordinator wins because it scanned first."""
        bus, hosts = _cluster(SLICE_COSTS, _slice_delay, old=True)
        _propose(bus, 90, SLICE_COSTS[90])
        bus.run()
        leaders, recorded = _outcome(hosts)
        assert leaders == [90] and recorded == {90}

    def test_a_cheaper_coordinator_counters_and_wins(self):
        """With counter-proposals the near coordinator 88, cheaper than 90, takes the job. The
        far, cheapest one (89) counters too, but its proposal arrives after 88's has a quorum —
        PBFT still rewards nearness, it no longer ignores cost."""
        bus, hosts = _cluster(SLICE_COSTS, _slice_delay)
        _propose(bus, 90, SLICE_COSTS[90])
        bus.run()
        leaders, recorded = _outcome(hosts)
        assert leaders == [88]
        assert recorded == {88}
        assert "j1" in hosts[88].countered and "j1" in hosts[89].countered

    def test_the_cheapest_wins_when_it_is_not_far(self):
        bus, hosts = _cluster(SLICE_COSTS, lambda a, b: 0.007)
        _propose(bus, 90, SLICE_COSTS[90])
        bus.run()
        leaders, recorded = _outcome(hosts)
        assert leaders == [89] and recorded == {89}

    def test_a_dearer_agent_prepares_as_before(self):
        """Nobody cheaper than the proposer: no counter-proposal, the ordinary path."""
        costs = {1: 1.0, 2: 2.0, 3: 3.0}
        bus, hosts = _cluster(costs, lambda a, b: 0.005)
        _propose(bus, 1, 1.0)
        bus.run()
        assert _outcome(hosts) == ([1], {1})
        assert not hosts[2].countered and not hosts[3].countered

    def test_an_unpriced_job_is_prepared(self):
        """The host answers None (it does not hold the job, or is not a coordinator): prepare."""
        costs = {1: 5.0, 2: None, 3: None}
        bus, hosts = _cluster(costs, lambda a, b: 0.005)
        _propose(bus, 1, 5.0)
        bus.run()
        assert _outcome(hosts) == ([1], {1})


class TestNoCounterAfterCommitting:
    def test_a_committed_agent_does_not_counter(self):
        bus, hosts = _cluster({1: 5.0, 2: 1.0, 3: 4.0}, lambda a, b: 0.005)
        bus.engines[2]._committed_to["j1"] = "pX"
        inc = ProposalInfo(p_id="p1", object_id="j1", cost=5.0, agent_id=1)
        assert bus.engines[2]._counter_proposal(hosts[2].objects["j1"], inc) is None

    def test_an_agent_with_its_own_proposal_does_not_counter(self):
        bus, hosts = _cluster({1: 5.0, 2: 1.0, 3: 4.0}, lambda a, b: 0.005)
        bus.engines[2].outgoing.add_proposal(
            ProposalInfo(p_id="mine", object_id="j1", cost=1.0, agent_id=2))
        inc = ProposalInfo(p_id="p1", object_id="j1", cost=5.0, agent_id=1)
        assert bus.engines[2]._counter_proposal(hosts[2].objects["j1"], inc) is None

    def test_the_adopt_via_prepare_path_counters_too(self):
        """A coordinator that hears of a proposal first through a peer's PREPARE used to adopt
        it there; it must make the same comparison."""
        bus, hosts = _cluster({1: 5.0, 2: 1.0, 3: 4.0}, lambda a, b: 0.005)
        p = ProposalInfo(p_id="p1", object_id="j1", cost=5.0, agent_id=1, prepares=[1, 3])
        wire = json.dumps(Prepare(source=3, agents=[_agent(3)], proposals=[p]).to_dict())
        bus.engines[2].on_prepare(MessageBuilder.from_dict(json.loads(wire)))
        assert hosts[2].countered == ["j1"]
        assert bus.engines[2].counter_proposals == 1
        assert bus.engines[2].outgoing.contains(object_id="j1")


def _agent(aid):
    from swarm.models.agent_info import AgentInfo
    return AgentInfo(agent_id=aid)


# --------------------------------------------------------------------------- #
# safety: one decision per job, whatever the costs, delays and proposal times
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("n", [3, 5, 9])
def test_one_decision_per_job_under_random_races(n):
    rng = random.Random(1019 + n)
    for trial in range(60):
        ids = list(range(1, n + 1))
        costs = {aid: round(rng.uniform(0.5, 3.0), 2) for aid in ids}
        rtt = {(a, b): rng.uniform(0.2, 90.0) for a in ids for b in ids if a < b}
        delay = (lambda a, b, rtt=rtt: rtt[(min(a, b), max(a, b))] / 2000.0)
        bus, hosts = _cluster(costs, delay, objects=("j1", "j2"))
        # 1-3 coordinators "scan first" at slightly different times, for each of two jobs
        for oid in ("j1", "j2"):
            for aid in rng.sample(ids, rng.randint(1, min(3, n))):
                _propose(bus, aid, costs[aid], oid=oid, at=rng.uniform(0.0, 0.05))
        bus.run()
        for oid in ("j1", "j2"):
            leaders, recorded = _outcome(hosts, oid)
            assert len(leaders) <= 1, f"trial {trial} {oid}: two leaders {leaders}"
            assert len(recorded) <= 1, f"trial {trial} {oid}: participants disagree {recorded}"
            if leaders:
                assert recorded <= {leaders[0]}, f"trial {trial} {oid}: {leaders} vs {recorded}"


@pytest.mark.parametrize("n,cheaper_min,optimal_min", [(3, 0.70, 0.65), (9, 0.85, 0.85)])
def test_cost_now_orders_the_outcome(n, cheaper_min, optimal_min):
    """The point of the change, measured over random races (200 per size). The winner is never
    dearer than the first proposer, is strictly cheaper in most races where a cheaper
    coordinator exists, and is the cheapest coordinator in most races. Measured when written:
    3 coordinators 101/128 cheaper, 153/200 cheapest; 9 coordinators 179/185 and 184/200.
    Without counter-proposals both rates of improvement are 0 and the cheapest wins 72/200 and
    15/200, only when it happens to be the first proposer."""
    rng = random.Random(77 + n)
    better = possible = optimal = 0
    trials = 200
    for trial in range(trials):
        ids = list(range(1, n + 1))
        costs = {aid: round(rng.uniform(0.5, 3.0), 2) for aid in ids}
        rtt = {(a, b): rng.uniform(0.2, 90.0) for a in ids for b in ids if a < b}
        delay = (lambda a, b, rtt=rtt: rtt[(min(a, b), max(a, b))] / 2000.0)
        bus, hosts = _cluster(costs, delay)
        first = rng.choice(ids)
        _propose(bus, first, costs[first])
        bus.run()
        leaders, _ = _outcome(hosts)
        assert len(leaders) == 1, f"trial {trial}: no single leader {leaders}"
        assert costs[leaders[0]] <= costs[first], (trial, first, leaders, costs)
        if any(c < costs[first] for c in costs.values()):
            possible += 1
            better += costs[leaders[0]] < costs[first]
        optimal += costs[leaders[0]] == min(costs.values())
    assert better / possible >= cheaper_min, f"winner cheaper than first in {better}/{possible}"
    assert optimal / trials >= optimal_min, f"cheapest won {optimal}/{trials}"


# --------------------------------------------------------------------------- #
# the agent side: who answers
# --------------------------------------------------------------------------- #

class TestWhoCounters:
    def _agent(self, level=1, matrix="self", enabled=True, engine="pbft"):
        import types
        from unittest.mock import MagicMock
        from swarm.agents.resource_agent import ResourceAgent
        from swarm.consensus.gossip_engine import GossipConsensusEngine
        a = ResourceAgent.__new__(ResourceAgent)
        a.logger = MagicMock()
        a.topology = types.SimpleNamespace(level=level)
        a.coordinator_cost_matrix = matrix
        a.pbft_counter_propose = enabled
        a.engine = (GossipConsensusEngine.__new__(GossipConsensusEngine) if engine == "snow"
                    else object())
        a.wire_cost_for_job = lambda oid: 1.25
        return a

    def test_a_pbft_coordinator_under_self_answers_with_its_wire_cost(self):
        assert self._agent().counter_proposal_cost("j1") == 1.25

    @pytest.mark.parametrize("kw", [dict(level=0), dict(matrix="peers"), dict(enabled=False),
                                    dict(engine="snow")])
    def test_nobody_else_does(self, kw):
        assert self._agent(**kw).counter_proposal_cost("j1") is None

    def test_the_key_defaults_on_and_refuses_a_non_boolean(self):
        from swarm.agents.resource_agent import resolve_pbft_counter_propose
        assert resolve_pbft_counter_propose({}) is True
        assert resolve_pbft_counter_propose({"pbft": {"counter_propose": False}}) is False
        with pytest.raises(ValueError):
            resolve_pbft_counter_propose({"pbft": {"counter_propose": "yes"}})
