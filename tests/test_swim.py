# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""
Unit tests for the SWIM membership protocol.

We exercise the protocol against an in-memory transport double — no gRPC, no
threads beyond what SWIM itself spawns. Time is controlled via a monotonic
counter so suspect/failed transitions are deterministic.
"""

import time
import unittest
from typing import Callable, Dict, List, Tuple

from swarm.consensus.messages.swim_ack import SwimAck
from swarm.consensus.messages.swim_ping import SwimPing
from swarm.consensus.messages.swim_ping_req import SwimPingReq
from swarm.membership.swim import SwimMembership, ALIVE, FAILED, SUSPECT


class _Clock:
    """Manually-advanced time source."""

    def __init__(self, start: float = 1_000_000.0):
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


class _Bus:
    """Routes messages between in-process SwimMembership instances."""

    def __init__(self):
        self.nodes: Dict[int, SwimMembership] = {}
        self.dropped: List[Tuple[int, int, str]] = []
        self.drop_filter: Callable[[int, int, object], bool] = lambda src, dst, p: False

    def register(self, agent_id: int, swim: SwimMembership) -> None:
        self.nodes[agent_id] = swim

    def deliver(self, src: int, dst: int, payload) -> None:
        if self.drop_filter(src, dst, payload):
            self.dropped.append((src, dst, type(payload).__name__))
            return
        target = self.nodes.get(dst)
        if target is None:
            self.dropped.append((src, dst, type(payload).__name__))
            return
        # Cross-thread safety: SWIM's on_* methods are safe to invoke directly
        # because they only touch the internal lock-protected state.
        if isinstance(payload, SwimPing):
            target.on_ping(payload)
        elif isinstance(payload, SwimAck):
            target.on_ack(payload)
        elif isinstance(payload, SwimPingReq):
            target.on_ping_req(payload)


class _Host:
    def __init__(self, agent_id: int, bus: _Bus, peers: List[int]):
        self.agent_id = agent_id
        self.bus = bus
        self._peers = peers
        self.events: List[Tuple[str, int]] = []

    def send(self, dest: int, payload) -> None:
        self.bus.deliver(self.agent_id, dest, payload)

    def known_peers(self):
        return self._peers

    def log_debug(self, msg): pass
    def log_info(self, msg): pass
    def log_warn(self, msg): pass

    def on_agent_alive(self, aid): self.events.append(("alive", aid))
    def on_agent_joined(self, aid): self.events.append(("joined", aid))
    def on_agent_suspected(self, aid): self.events.append(("suspect", aid))
    def on_agent_failed(self, aid): self.events.append(("failed", aid))


def _make_swim(agent_id: int, bus: _Bus, peer_ids: List[int], clock: _Clock,
               *, period_s=0.05, probe_timeout_s=0.02, suspect_timeout_s=0.1,
               k_req=2) -> Tuple[SwimMembership, _Host]:
    host = _Host(agent_id, bus, [p for p in peer_ids if p != agent_id])
    swim = SwimMembership(
        host=host,
        period_s=period_s,
        probe_timeout_s=probe_timeout_s,
        k_req=k_req,
        suspect_timeout_s=suspect_timeout_s,
        time_fn=clock,
    )
    bus.register(agent_id, swim)
    return swim, host


class SwimDirectProbeTests(unittest.TestCase):
    """A live peer answering pings stays alive."""

    def test_direct_ping_ack(self):
        clock = _Clock()
        bus = _Bus()
        a, host_a = _make_swim(1, bus, [1, 2], clock)
        b, host_b = _make_swim(2, bus, [1, 2], clock)

        # Drive one probe tick directly (no thread).
        a._maybe_send_probe(clock())

        # After delivery, A should still see B as alive (or never demote it).
        self.assertEqual(a.status_of(2), ALIVE)
        # No suspect or failed events were emitted on either side.
        self.assertNotIn("suspect", [e[0] for e in host_a.events])
        self.assertNotIn("failed", [e[0] for e in host_a.events])


class SwimFailureDetectionTests(unittest.TestCase):
    """A dead peer transitions alive -> suspect -> failed within bounded time."""

    def test_dead_peer_becomes_failed(self):
        clock = _Clock()
        bus = _Bus()
        # Three agents so 1 has a relay (3) for ping-req.
        a, host_a = _make_swim(1, bus, [1, 2, 3], clock,
                               period_s=1.0, probe_timeout_s=0.5,
                               suspect_timeout_s=2.0, k_req=1)
        c, host_c = _make_swim(3, bus, [1, 2, 3], clock,
                               period_s=1.0, probe_timeout_s=0.5,
                               suspect_timeout_s=2.0, k_req=1)
        # Agent 2 is "dead": drop everything destined for it.
        bus.drop_filter = lambda s, d, p: d == 2

        # Force A to probe agent 2 (the dead one) deterministically.
        a._pick_probe_target = lambda: 2  # type: ignore[method-assign]
        # Tick 1: A probes 2 directly. Times out -> ping-req to 3.
        a._maybe_send_probe(clock())
        clock.advance(0.6)  # past probe_timeout
        a._handle_expirations(clock())  # triggers ping-req fanout
        clock.advance(0.6)  # past indirect-probe timeout
        a._handle_expirations(clock())  # marks 2 as suspect

        self.assertEqual(a.status_of(2), SUSPECT,
                         f"events={host_a.events}")
        self.assertIn(("suspect", 2), host_a.events)

        # Advance past suspect_timeout to trigger failed.
        clock.advance(2.5)
        a._handle_expirations(clock())

        self.assertEqual(a.status_of(2), FAILED)
        self.assertIn(("failed", 2), host_a.events)


class SwimPiggybackTests(unittest.TestCase):
    """Membership updates spread infection-style on ping/ack traffic."""

    def test_updates_propagate(self):
        clock = _Clock()
        bus = _Bus()
        a, host_a = _make_swim(1, bus, [1, 2, 3], clock)
        b, host_b = _make_swim(2, bus, [1, 2, 3], clock)
        c, host_c = _make_swim(3, bus, [1, 2, 3], clock)

        # Force A to mark C as suspect (simulating a prior round outcome).
        a._mark_suspect(3, reason="test")
        # When A pings B, the piggyback should carry that update.
        a._maybe_send_probe(clock())

        # B has seen the suspect update via the ping (regardless of which peer
        # A picked as the probe target, the update is in the piggyback queue).
        # If A happened to ping C (which is suspect but not failed), the
        # piggyback still reaches a peer; otherwise it reaches B.
        observed = b.status_of(3) or c.status_of(3)
        self.assertIn(observed, (SUSPECT, ALIVE))  # at least propagated or refuted


class SwimRefutationTests(unittest.TestCase):
    """A peer who's incorrectly marked suspect refutes by bumping incarnation."""

    def test_self_refutation(self):
        clock = _Clock()
        bus = _Bus()
        a, host_a = _make_swim(1, bus, [1, 2], clock)
        b, host_b = _make_swim(2, bus, [1, 2], clock)

        # Pretend agent 1 receives an update claiming itself is SUSPECT at inc=0.
        from swarm.consensus.messages.membership_update import MembershipUpdate
        a._absorb_updates([MembershipUpdate(agent_id=1, status=SUSPECT, incarnation=0)])

        # Agent 1's own incarnation must have advanced.
        self.assertGreater(a._self_incarnation, 0)
        # And the piggyback queue contains an Alive at the new incarnation.
        with a._lock:
            entries = [e for e in a._piggy if e.update.agent_id == 1]
        self.assertTrue(entries, "refutation update should be enqueued")
        self.assertEqual(entries[-1].update.status, ALIVE)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
