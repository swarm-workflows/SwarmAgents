# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""Unit tests for the gossip state disseminator."""

import unittest
from typing import Dict, List

from swarm.consensus.messages.agent_state_entry import AgentStateEntry
from swarm.consensus.messages.gossip_state import GossipState
from swarm.gossip.disseminator import GossipStateDisseminator


class _Clock:
    def __init__(self, start: float = 1_000_000.0):
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


class _Bus:
    def __init__(self):
        self.nodes: Dict[int, GossipStateDisseminator] = {}

    def register(self, aid: int, g: GossipStateDisseminator) -> None:
        self.nodes[aid] = g

    def deliver(self, src: int, dst: int, payload) -> None:
        target = self.nodes.get(dst)
        if target is None or not isinstance(payload, GossipState):
            return
        target.on_gossip(payload)


class _Host:
    def __init__(self, agent_id: int, bus: _Bus, peers: List[int]):
        self.agent_id = agent_id
        self.bus = bus
        self._peers = peers

    def send(self, dest, payload):
        self.bus.deliver(self.agent_id, dest, payload)

    def live_peers(self):
        return [p for p in self._peers if p != self.agent_id]

    def log_debug(self, m): pass
    def log_warn(self, m): pass


def _make(aid: int, bus: _Bus, peers: List[int], clock: _Clock,
          period_s=0.05, ttl=1.0, fanout=2):
    host = _Host(aid, bus, peers)
    g = GossipStateDisseminator(host=host, period_s=period_s,
                                fanout=fanout, state_ttl_s=ttl,
                                time_fn=clock)
    bus.register(aid, g)
    return g


class GossipMergeTests(unittest.TestCase):
    def test_higher_version_wins(self):
        clock = _Clock()
        bus = _Bus()
        g = _make(1, bus, [1, 2], clock)
        g._merge_entry(AgentStateEntry(agent_id=2, version=1, load=10.0))
        g._merge_entry(AgentStateEntry(agent_id=2, version=5, load=99.0))
        g._merge_entry(AgentStateEntry(agent_id=2, version=3, load=50.0))  # stale, ignored
        self.assertEqual(g.get(2).version, 5)
        self.assertEqual(g.get(2).load, 99.0)

    def test_self_record_protected(self):
        clock = _Clock()
        bus = _Bus()
        g = _make(1, bus, [1, 2], clock)
        # Publish our own state at version=1.
        g.publish_local(load=10.0)
        # An incoming entry claiming to be us at version=0 must be ignored.
        g._merge_entry(AgentStateEntry(agent_id=1, version=0, load=0.0))
        self.assertEqual(g.get(1).load, 10.0)


class GossipPushTests(unittest.TestCase):
    def test_push_round_propagates(self):
        clock = _Clock()
        bus = _Bus()
        a = _make(1, bus, [1, 2, 3], clock, fanout=3)
        b = _make(2, bus, [1, 2, 3], clock, fanout=3)
        c = _make(3, bus, [1, 2, 3], clock, fanout=3)

        a.publish_local(load=42.0)
        a._push_round()

        # Both other nodes should now know about agent 1.
        self.assertIsNotNone(b.get(1))
        self.assertIsNotNone(c.get(1))
        self.assertEqual(b.get(1).load, 42.0)
        self.assertEqual(c.get(1).load, 42.0)


class GossipExpiryTests(unittest.TestCase):
    def test_entries_expire(self):
        clock = _Clock()
        bus = _Bus()
        g = _make(1, bus, [1, 2], clock, ttl=1.0)
        g._merge_entry(AgentStateEntry(agent_id=2, version=1, load=10.0))
        self.assertIsNotNone(g.get(2))

        clock.advance(2.0)
        g._evict_expired()
        self.assertNotIn(2, [e.agent_id for e in g.snapshot()])

    def test_self_entry_never_evicted(self):
        clock = _Clock()
        bus = _Bus()
        g = _make(1, bus, [1, 2], clock, ttl=1.0)
        g.publish_local(load=10.0)

        clock.advance(2.0)
        g._evict_expired()
        # Self is never evicted by the disseminator's TTL.
        self.assertIsNotNone(g.get(1))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
