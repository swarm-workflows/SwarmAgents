# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""
Unit tests for the Snow consensus engine.

We exercise the engine against an in-memory transport + Redis double; no
threads, no real Redis. Time and randomness are controlled.
"""

import unittest
from typing import Dict, List, Optional

from swarm.consensus.gossip_engine import GossipConsensusEngine
from swarm.consensus.messages.proposal_info import ProposalInfo
from swarm.consensus.messages.snow_query import SnowQuery
from swarm.consensus.messages.snow_response import SnowResponse


class _Clock:
    def __init__(self, start: float = 1_000_000.0):
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


class _FakeJob:
    def __init__(self, object_id: str):
        self.object_id = object_id
        self.leader_id = None
        self.state = None
        self.is_commit = False


class _FakeCAS:
    """In-process stand-in for the Redis SET-NX claim."""

    def __init__(self):
        self.store: Dict[str, int] = {}

    def claim(self, object_id: str, agent_id: int) -> int:
        if object_id in self.store:
            return self.store[object_id]
        self.store[object_id] = int(agent_id)
        return int(agent_id)

    def get(self, object_id: str) -> Optional[int]:
        return self.store.get(object_id)


class _Host:
    def __init__(self, agent_id: int, peers: List[int], my_cost: float, cas: _FakeCAS):
        self.agent_id = agent_id
        self._peers = peers
        self._my_cost = my_cost
        self._jobs: Dict[str, _FakeJob] = {}
        self._cas = cas
        self.leader_events: List[str] = []
        self.participant_events: List[tuple] = []

    # --- ConsensusHost surface used by Snow ---
    def get_object(self, oid):
        return self._jobs.setdefault(oid, _FakeJob(oid))

    def set_pending_proposal(self, msg, oid): pass
    def set_pending_prepare(self, msg, oid): pass
    def set_pending_commit(self, msg, oid): pass
    def is_agreement_achieved(self, oid): return self._cas.get(oid) is not None
    def calculate_quorum(self): return 1

    def on_leader_elected(self, obj, p_id):
        self.leader_events.append(obj.object_id)

    def on_participant_commit(self, obj, leader_id, p_id):
        self.participant_events.append((obj.object_id, leader_id))

    def now(self): return 0.0
    def log_debug(self, m): pass
    def log_info(self, m): pass
    def log_warn(self, m): pass

    # --- SnowHost extensions ---
    def live_peer_ids(self): return list(self._peers)
    def my_cost_for_job(self, oid): return self._my_cost

    def try_claim_assignment(self, oid, aid):
        return self._cas.claim(oid, aid)

    def get_assignment(self, oid):
        return self._cas.get(oid)


class _Transport:
    """Drops outgoing into a list; tests deliver by hand."""

    def __init__(self):
        self.outbox: List[tuple] = []  # (dest, payload)

    def send(self, dest, payload):
        self.outbox.append((dest, payload))

    def broadcast(self, payload):
        # Not used by Snow but Protocol requires it.
        self.outbox.append((None, payload))


class _Router:
    def should_forward(self): return False


def _make_engine(agent_id=1, peers=(2, 3, 4), my_cost=1.0, cas=None,
                 k=3, alpha=0.7, beta=2):
    cas = cas or _FakeCAS()
    host = _Host(agent_id, list(peers), my_cost, cas)
    transport = _Transport()
    router = _Router()
    eng = GossipConsensusEngine(
        agent_id=agent_id, host=host, transport=transport, router=router,
        k=k, alpha=alpha, beta=beta, max_rounds=20,
        round_timeout_s=0.5, tick_interval_s=0.01,
        # Tests drive _tick(now=...) with a synthetic timeline starting at 0.0; the
        # engine's own clock must match or propose() stamps wall-clock deadlines that
        # the _tick send gate compares against synthetic `now`.
        time_fn=lambda: 0.0,
    )
    return eng, host, transport, cas


class SnowProposeRoundTests(unittest.TestCase):
    """A proposal that gets unanimous peer agreement commits via CAS."""

    def test_unanimous_agreement_commits(self):
        eng, host, transport, cas = _make_engine(k=3, beta=2)
        p = ProposalInfo(p_id="p-1", object_id="job-1", cost=1.0, agent_id="1")
        eng.propose([p])

        # First round: tick sends queries to peers 2,3,4.
        eng._tick(now=0.0)
        first_round_outbox = [m for m in transport.outbox if isinstance(m[1], SnowQuery)]
        self.assertEqual(len(first_round_outbox), 3)
        query = first_round_outbox[0][1]

        # Simulate unanimous "yield to initiator (agent 1)" responses.
        for peer in (2, 3, 4):
            eng.on_snow_response(SnowResponse(
                source=peer, query_id=query.query_id, job_id="job-1",
                preferred_agent=1, cost=1.0, already_decided=False,
            ))

        # Evaluate that round.
        eng._tick(now=0.0)
        # confidence == 1 now; the next tick fires round 2 (pending_query_id is None).
        eng._tick(now=0.0)
        second_round = [m for m in transport.outbox
                        if isinstance(m[1], SnowQuery)
                        and m[1].query_id != query.query_id]
        self.assertEqual(len(second_round), 3)
        q2 = second_round[0][1]
        for peer in (2, 3, 4):
            eng.on_snow_response(SnowResponse(
                source=peer, query_id=q2.query_id, job_id="job-1",
                preferred_agent=1, cost=1.0, already_decided=False,
            ))
        eng._tick(now=0.0)  # evaluates round 2 -> confidence reaches beta -> finalize

        self.assertEqual(cas.get("job-1"), 1)
        self.assertEqual(host.leader_events, ["job-1"])


class SnowPeerResponseTests(unittest.TestCase):
    """A peer that is cheaper than the initiator votes for itself; else yields."""

    def test_dominates_initiator(self):
        eng, host, transport, _ = _make_engine(agent_id=5, peers=[1], my_cost=0.1)
        # Initiator (agent 1) claims preferred=1 at cost=1.0; we are cheaper.
        q = SnowQuery(source=1, query_id="q1", job_id="job-1",
                      preferred_agent=1, preferred_cost=1.0, round=1)
        eng.on_snow_query(q)
        self.assertEqual(len(transport.outbox), 1)
        resp = transport.outbox[0][1]
        self.assertIsInstance(resp, SnowResponse)
        self.assertEqual(resp.preferred_agent, 5)  # self-promotion
        self.assertEqual(resp.cost, 0.1)

    def test_yields_when_initiator_cheaper(self):
        eng, host, transport, _ = _make_engine(agent_id=5, peers=[1], my_cost=10.0)
        q = SnowQuery(source=1, query_id="q1", job_id="job-1",
                      preferred_agent=1, preferred_cost=1.0, round=1)
        eng.on_snow_query(q)
        resp = transport.outbox[0][1]
        self.assertEqual(resp.preferred_agent, 1)  # yielded
        self.assertEqual(resp.cost, 1.0)

    def test_short_circuits_when_decided(self):
        cas = _FakeCAS()
        cas.claim("job-1", 42)
        eng, host, transport, _ = _make_engine(agent_id=5, peers=[1], cas=cas)
        q = SnowQuery(source=1, query_id="q1", job_id="job-1",
                      preferred_agent=1, preferred_cost=1.0, round=1)
        eng.on_snow_query(q)
        resp = transport.outbox[0][1]
        self.assertTrue(resp.already_decided)
        self.assertEqual(resp.preferred_agent, 42)


class SnowCASExactlyOnceTests(unittest.TestCase):
    """Two engines committing concurrently to the same job — only one wins."""

    def test_cas_picks_one_winner(self):
        cas = _FakeCAS()
        e1, h1, t1, _ = _make_engine(agent_id=1, peers=[2], cas=cas)
        e2, h2, t2, _ = _make_engine(agent_id=2, peers=[1], cas=cas)

        # Both finalize independently with different candidates.
        p1 = ProposalInfo(p_id="p1", object_id="job-x", cost=1.0, agent_id="1")
        p2 = ProposalInfo(p_id="p2", object_id="job-x", cost=1.0, agent_id="2")
        e1.propose([p1])
        e2.propose([p2])
        # Bypass rounds: directly finalize on each engine.
        state1 = e1._states["job-x"]
        state2 = e2._states["job-x"]
        e1._finalize(state1, candidate=1, reason="test")
        e2._finalize(state2, candidate=2, reason="test")

        winner = cas.get("job-x")
        # Exactly one of (1, 2) should have won; the loser sees the winner via CAS.
        self.assertIn(winner, (1, 2))
        if winner == 1:
            self.assertEqual(h1.leader_events, ["job-x"])
            self.assertEqual(h2.participant_events, [("job-x", 1)])
        else:
            self.assertEqual(h2.leader_events, ["job-x"])
            self.assertEqual(h1.participant_events, [("job-x", 2)])


class SnowFastFinalizeTests(unittest.TestCase):
    """If a peer reports already_decided, we adopt that winner immediately."""

    def test_already_decided_fast_path(self):
        cas = _FakeCAS()
        cas.claim("job-y", 99)
        eng, host, transport, _ = _make_engine(agent_id=1, peers=[2, 3, 4], cas=cas)
        p = ProposalInfo(p_id="p", object_id="job-y", cost=1.0, agent_id="1")
        eng.propose([p])
        eng._tick(now=0.0)  # sends round 1
        q = next(m[1] for m in transport.outbox if isinstance(m[1], SnowQuery))

        # First response says already_decided -> finalize as participant.
        eng.on_snow_response(SnowResponse(
            source=2, query_id=q.query_id, job_id="job-y",
            preferred_agent=99, cost=0.0, already_decided=True,
        ))
        self.assertEqual(host.participant_events, [("job-y", 99)])


class SnowFewPeersTests(unittest.TestCase):
    """Small groups (fewer live peers than k) must commit without waiting the full
    per-round timeout, and must be able to clear the supermajority threshold.

    Regression guard for the latency fix: a round completes when all *queried* peers
    respond (min(k, queried)) rather than requiring k responses, and the alpha
    threshold is relative to the peers actually sampled — so a 2-peer group under
    k=8 still commits at t=0 instead of burning round_timeout on every round."""

    def test_commits_without_waiting_when_peers_below_k(self):
        eng, host, transport, cas = _make_engine(
            agent_id=1, peers=[2, 3], k=8, alpha=0.7, beta=2)
        p = ProposalInfo(p_id="p", object_id="job-s", cost=1.0, agent_id="1")
        eng.propose([p])

        # Two rounds, both evaluated at t=0.0 — well before the 0.5s round deadline.
        for _ in range(2):
            eng._tick(now=0.0)  # send a round
            q = max((m[1] for m in transport.outbox if isinstance(m[1], SnowQuery)),
                    key=lambda x: x.round)
            self.assertEqual(len([m for m in transport.outbox
                                  if isinstance(m[1], SnowQuery) and m[1].query_id == q.query_id]), 2)
            for peer in (2, 3):
                eng.on_snow_response(SnowResponse(
                    source=peer, query_id=q.query_id, job_id="job-s",
                    preferred_agent=1, cost=1.0, already_decided=False))
            eng._tick(now=0.0)  # evaluate the round at t=0.0 (no deadline wait)

        self.assertEqual(cas.get("job-s"), 1)
        self.assertEqual(host.leader_events, ["job-s"])


def _deliver_round(eng, transport, job_id, votes):
    """Send the next round for job_id and deliver (peer, choice) votes, then evaluate."""
    eng._tick(now=0.0)  # sends a round (pending_query_id was None)
    q = max((m[1] for m in transport.outbox
             if isinstance(m[1], SnowQuery) and m[1].job_id == job_id),
            key=lambda x: x.round)
    for peer, choice in votes:
        eng.on_snow_response(SnowResponse(
            source=peer, query_id=q.query_id, job_id=job_id,
            preferred_agent=choice, cost=1.0, already_decided=False))
    eng._tick(now=0.0)  # evaluates the round


class SnowballStickyTests(unittest.TestCase):
    """Snowball keeps `preferred` as the argmax of cumulative support, so a transient
    minority flip does not thrash the preference (Snowflake used to switch every flip,
    broadcasting unstable preferences that blocked network convergence)."""

    def test_preferred_sticky_under_minority_flip(self):
        eng, host, transport, cas = _make_engine(
            agent_id=1, peers=[2, 3, 4], k=3, alpha=0.7, beta=10)
        eng.propose([ProposalInfo(p_id="p", object_id="j", cost=1.0, agent_id="1")])
        st = eng._states["j"]
        _deliver_round(eng, transport, "j", [(2, 7), (3, 7), (4, 7)])   # supermajority for 7
        self.assertEqual(st.preferred, 7)
        self.assertEqual(st.d[7], 1)
        _deliver_round(eng, transport, "j", [(2, 7), (3, 7), (4, 9)])   # one flip to 9
        self.assertEqual(st.preferred, 7)   # sticky
        self.assertEqual(st.d[7], 2)


class SnowDroppedSendAccountingTests(unittest.TestCase):
    """state.queried must count only DISPATCHED sends. A pool-dropped query is never
    answered; counting it made rounds wait the full round_timeout (measured on the
    testbed as snow_sends_dropped=5742 with rounds resolving by deadline)."""

    class _DroppyEngine(GossipConsensusEngine):
        """Simulate pool saturation: drop sends to a configured set of peers."""
        drop_to: set = set()

        def _safe_send(self, dest, payload):
            if dest in self.drop_to:
                self.sends_dropped += 1
                return False
            return super()._safe_send(dest, payload)

    def _make(self, peers, drop_to, k=8, beta=2):
        cas = _FakeCAS()
        host = _Host(1, list(peers), 1.0, cas)
        transport = _Transport()
        eng = self._DroppyEngine(
            agent_id=1, host=host, transport=transport, router=_Router(),
            k=k, alpha=0.7, beta=beta, max_rounds=20,
            round_timeout_s=0.5, tick_interval_s=0.01,
            time_fn=lambda: 0.0)
        eng.drop_to = set(drop_to)
        return eng, host, transport, cas

    def test_round_completes_with_only_dispatched_peers(self):
        # Peers 2,3 reachable; sends to 4,5 dropped. Round must complete at t=0
        # once 2 and 3 answer — not wait the deadline for 4 and 5.
        eng, host, transport, cas = self._make(peers=[2, 3, 4, 5], drop_to={4, 5})
        eng.propose([ProposalInfo(p_id="p", object_id="j-d", cost=1.0, agent_id="1")])
        for _ in range(2):  # beta=2 rounds
            eng._tick(now=0.0)
            st = eng._states.get("j-d")
            if st is None:  # finalized during previous evaluate
                break
            self.assertEqual(st.queried, 2)  # only dispatched sends counted
            q = max((m[1] for m in transport.outbox if isinstance(m[1], SnowQuery)),
                    key=lambda x: x.round)
            for peer in (2, 3):
                eng.on_snow_response(SnowResponse(
                    source=peer, query_id=q.query_id, job_id="j-d",
                    preferred_agent=1, cost=1.0, already_decided=False))
            eng._tick(now=0.0)  # evaluates at t=0.0 — no deadline wait
        self.assertEqual(cas.get("j-d"), 1)
        self.assertEqual(eng.sends_dropped, 4)  # 2 dropped peers x 2 rounds

    def test_fully_dropped_round_backs_off_before_retry(self):
        # A saturated pool must NOT retry on the next 50ms tick — that spin multiplied
        # send demand ~20x and congestion-collapsed the coordinator tier (2M drops).
        eng, host, transport, cas = self._make(peers=[2, 3], drop_to={2, 3})
        eng.propose([ProposalInfo(p_id="p", object_id="j-z", cost=1.0, agent_id="1")])
        eng._tick(now=0.0)  # all sends dropped
        st = eng._states["j-z"]
        self.assertIsNone(st.pending_query_id)
        self.assertEqual(st.round_no, 0)          # a round that never left doesn't count
        drops_after_first = eng.sends_dropped
        eng._tick(now=0.1)                        # inside the backoff window: no resend
        self.assertEqual(eng.sends_dropped, drops_after_first)
        eng.drop_to = set()                       # pool recovers
        eng._tick(now=0.6)                        # past round_timeout: retry fires
        self.assertIsNotNone(st.pending_query_id)
        self.assertEqual(st.queried, 2)

    def test_max_inflight_caps_concurrent_rounds(self):
        eng, host, transport, cas = self._make(peers=[2, 3], drop_to=set())
        eng.max_inflight = 1
        eng.propose([ProposalInfo(p_id="p1", object_id="j-1", cost=1.0, agent_id="1"),
                     ProposalInfo(p_id="p2", object_id="j-2", cost=1.0, agent_id="1")])
        eng._tick(now=0.0)
        pending = [s for s in eng._states.values() if s.pending_query_id]
        self.assertEqual(len(pending), 1)         # only one round in flight at the cap


class SnowAsyncFinalizeTests(unittest.TestCase):
    """With the pool running, _finalize returns immediately and the CAS/callback work
    completes on a worker; double-finalize is deduped by the finalized flag."""

    def test_async_finalize_completes_and_dedupes(self):
        import time as _t
        eng, host, transport, cas = _make_engine(agent_id=1, peers=[2])
        eng.propose([ProposalInfo(p_id="p", object_id="j-a", cost=1.0, agent_id="1")])
        state = eng._states["j-a"]
        eng.start()
        try:
            eng._finalize(state, candidate=1, reason="test")
            eng._finalize(state, candidate=2, reason="dup")  # must be a no-op
            deadline = _t.time() + 2.0
            while cas.get("j-a") is None and _t.time() < deadline:
                _t.sleep(0.01)
        finally:
            eng.stop()
        self.assertEqual(cas.get("j-a"), 1)          # first finalize won
        self.assertEqual(host.leader_events, ["j-a"])  # exactly one callback


class SnowNonBlockingSendTests(unittest.TestCase):
    """With the send pool running, a slow transport must not block the caller/driver."""

    def test_slow_send_does_not_block(self):
        import time as _t

        class SlowTransport:
            def send(self, dest, payload): _t.sleep(0.5)
            def broadcast(self, payload): pass

        eng, host, transport, cas = _make_engine()
        eng.transport = SlowTransport()
        eng.start()  # spins the (idle) driver + the send pool
        try:
            q = SnowQuery(source=1, query_id="q", job_id="j",
                          preferred_agent=1, preferred_cost=1.0, round=1)
            t0 = _t.time()
            for d in (2, 3, 4, 5, 6):
                eng._safe_send(d, q)      # submitted to pool, must return immediately
            self.assertLess(_t.time() - t0, 0.2)  # did not wait on the 0.5s sends
        finally:
            eng.stop()


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
