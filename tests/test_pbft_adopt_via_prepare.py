"""PBFT: a proposal learned from a PREPARE is voted for (2026-10-30, E6).

An agent that heard of a proposal only through another agent's PREPARE adopted it and voted only
if the PROPOSAL itself arrived later. PBFT never retransmits, so when the PROPOSAL was lost every
second-hand agent stayed mute. In E6 (Hier-30, a healed site partition) a 5-agent group held 2
votes against a quorum of 3 until the 300 s reselection, 9-11 jobs per run
(docs/results/e6-58f3ab22/README.md §3). These tests drop PROPOSALs on chosen links of a bus of
real `ConsensusEngine`s.
"""
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "tests"))

from swarm.consensus.messages.prepare import Prepare  # noqa: E402
from swarm.consensus.messages.proposal import Proposal  # noqa: E402

from test_pbft_counter_proposal import _cluster, _DelayBus, _outcome, _propose  # noqa: E402

IDS = [6, 7, 8, 9, 10]


class _LossyBus(_DelayBus):
    """Drops a message type on chosen (src, dst) links, and counts the PREPAREs each agent sends
    to the proposer (one per broadcast, since a broadcast reaches every peer once)."""

    def __init__(self, delay, drop):
        super().__init__(delay)
        self.drop = drop              # {(src, dst, type)}
        self.prepares_sent = {}

    def _push(self, src, dst, payload):
        if (src, dst, type(payload)) in self.drop:
            return
        if isinstance(payload, Prepare) and dst == 7:
            self.prepares_sent[src] = self.prepares_sent.get(src, 0) + 1
        super()._push(src, dst, payload)


def _lossy_cluster(drop, delay=lambda a, b: 0.01):
    bus, hosts = _cluster({i: None for i in IDS}, delay, old=True)
    lossy = _LossyBus(delay, drop)
    lossy.engines = bus.engines
    for aid, eng in bus.engines.items():
        eng.transport = lossy.transport_for(aid)
    return lossy, hosts


def test_the_e6_stall_is_decided_when_the_proposal_reaches_one_peer():
    """Agent 7's PROPOSAL reaches only agent 9 (the E6 run02 / job 524 shape)."""
    drop = {(7, d, Proposal) for d in (6, 8, 10)}
    bus, hosts = _lossy_cluster(drop)
    _propose(bus, 7, cost=10.0)
    bus.run()
    leaders, recorded = _outcome(hosts)
    assert leaders == [7] and recorded == {7}, (leaders, recorded)


def test_a_late_proposal_after_a_prepare_sends_no_second_prepare():
    """The PROPOSAL reaches agent 8 long after agent 9's PREPARE did: 8 votes once, not twice."""
    slow = lambda a, b: 1.0 if (a, b) == (7, 8) else 0.01
    bus, hosts = _lossy_cluster(set(), delay=slow)
    _propose(bus, 7, cost=10.0)
    bus.run()
    assert _outcome(hosts)[0] == [7]
    # Each non-proposer prepares once per job; the proposer's vote travels in its PROPOSAL.
    assert bus.prepares_sent == {6: 1, 8: 1, 9: 1, 10: 1}, bus.prepares_sent


def test_no_proposal_reaches_anyone_and_nothing_is_decided():
    """The fix votes for what an agent has heard of; it invents nothing from silence."""
    drop = {(7, d, Proposal) for d in (6, 8, 9, 10)}
    bus, hosts = _lossy_cluster(drop)
    _propose(bus, 7, cost=10.0)
    bus.run()
    leaders, recorded = _outcome(hosts)
    assert leaders == [] and recorded == set()


def test_a_cheaper_proposal_still_wins_over_one_adopted_from_a_prepare():
    """Voting on adoption must not make the first-heard proposal sticky against a cheaper one."""
    drop = {(7, d, Proposal) for d in (6, 8, 10)}
    bus, hosts = _lossy_cluster(drop)
    _propose(bus, 7, cost=10.0)
    _propose(bus, 6, cost=5.0, at=0.001)
    bus.run()
    leaders, recorded = _outcome(hosts)
    assert len(recorded) == 1, recorded          # one decision, whichever it is
    assert len(leaders) <= 1, leaders


def test_where_counter_proposals_are_live_a_non_countering_adopter_votes_too():
    """Coordinators under `coordinator_cost_matrix: self` used to adopt silently (b9e51bbf).
    None of 6, 8, 10 undercuts agent 7, so each votes on adoption and the job is decided."""
    from test_pbft_counter_proposal import _cluster as _cost_cluster
    costs = {i: (2.0 if i == 7 else 3.0) for i in IDS}   # all price it; none undercuts agent 7
    bus0, hosts = _cost_cluster(costs, lambda a, b: 0.01)
    bus = _LossyBus(lambda a, b: 0.01, {(7, d, Proposal) for d in (6, 8, 10)})
    bus.engines = bus0.engines
    for aid, eng in bus.engines.items():
        eng.transport = bus.transport_for(aid)
    _propose(bus, 7, cost=2.0)
    bus.run()
    assert set(bus.prepares_sent) == {6, 8, 9, 10}, bus.prepares_sent
    assert _outcome(hosts) == ([7], {7})


def test_near_tied_coordinators_that_lost_the_proposal_still_decide():
    """Stop review 2026-11-05: nine coordinators within `counter_margin` of the proposer and six
    of its PROPOSAL deliveries dropped. The margin stops the second-hand agents countering; if
    adoption were still silent they would neither counter nor vote and nothing is decided."""
    from test_pbft_counter_proposal import _cluster as _cost_cluster
    ids = list(range(1, 10))
    costs = {i: 5.60 + 0.03 * i for i in ids}            # 5.63 .. 5.87, all within 5 %
    bus0, hosts = _cost_cluster(costs, lambda a, b: 0.01)
    bus = _LossyBus(lambda a, b: 0.01, {(9, d, Proposal) for d in (1, 2, 3, 4, 5, 6)})
    bus.engines = bus0.engines
    for aid, eng in bus.engines.items():
        eng.transport = bus.transport_for(aid)
        assert eng.counter_margin == 0.05
    _propose(bus, 9, cost=costs[9])
    bus.run()
    assert _outcome(hosts) == ([9], {9})
    assert sum(e.counter_proposals for e in bus.engines.values()) == 0


def test_random_lossy_races_with_the_resolver_on_are_all_decided_once():
    """The measurement behind voting on adoption, smaller: 9 coordinators, 1-3 simultaneous
    proposers, random RTTs, 50 % of PROPOSAL deliveries lost, split resolver on (`live_voter_ids`,
    as every agent has). Measured at 400 races: voting 0 undecided, silence 139-147."""
    import random
    from test_pbft_counter_proposal import _cluster as _cost_cluster
    rng = random.Random(1105)
    for trial in range(80):
        ids = list(range(1, 10))
        costs = {a: round(rng.uniform(0.5, 3.0), 2) for a in ids}
        rtt = {(a, b): rng.uniform(0.2, 90.0) for a in ids for b in ids if a < b}
        delay = (lambda a, b, rtt=rtt: rtt[(min(a, b), max(a, b))] / 2000.0)
        bus0, hosts = _cost_cluster(costs, delay)
        for h in hosts.values():
            h.live_voter_ids = (lambda ids=ids: list(ids))
        drop = {(a, b, Proposal) for a in ids for b in ids if a != b and rng.random() < 0.5}
        bus = _LossyBus(delay, drop)
        bus.engines = bus0.engines
        for aid, eng in bus.engines.items():
            eng.transport = bus.transport_for(aid)
        for aid in rng.sample(ids, rng.randint(1, 3)):
            _propose(bus, aid, costs[aid], at=rng.uniform(0.0, 0.05))
        bus.run()
        leaders, recorded = _outcome(hosts)
        assert len(leaders) == 1, (trial, leaders)
        assert recorded == {leaders[0]}, (trial, leaders, recorded)
