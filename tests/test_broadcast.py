# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""
Tests for the parallel best-effort PBFT broadcast fan-out (scalability Phase 1).

Serially, one dead peer cost ~8.7s per consensus phase (2s timeout x 4 retries +
backoff, x3 phases per job). broadcast() must submit sends concurrently and never
block the calling (consensus/inbound) thread on a slow peer.
"""

import logging
import time
import unittest

from swarm.comm.grpc_transport import GrpcTransport
from swarm.consensus.messages.prepare import Prepare
from swarm.consensus.messages.proposal_info import ProposalInfo
from swarm.models.agent_info import AgentInfo
from swarm.utils.thread_safe_dict import ThreadSafeDict


def _bare_transport():
    """GrpcTransport without server/client construction (no port binds in tests)."""
    t = GrpcTransport.__new__(GrpcTransport)
    t.logger = logging.getLogger("test-bcast")
    t.observers = []
    t.broadcasts = 0
    t.broadcast_time_total = 0.0
    t.broadcast_time_max = 0.0
    t._bcast_pool = None
    t._bcast_sem = None
    t.bcast_workers = 4
    t.bcast_sends_dropped = 0
    return t


class _SlowSendTransport(GrpcTransport):
    """Records sends; every send simulates a dead peer (blocks 0.5s)."""

    def _send_raw(self, host, port, src, dest, payload_json, msg_type,
                  timeout=2.0, retries=4):
        self.sent.append((dest, retries))
        time.sleep(0.5)


def _neighbor_map(ids):
    m = ThreadSafeDict()
    for i in ids:
        m.set(i, AgentInfo(agent_id=i, host="h", port=1000 + i))
    return m


def _msg():
    return Prepare(source=1, agents=[AgentInfo(agent_id=1)],
                   proposals=[ProposalInfo(p_id="p", object_id="j", cost=1.0, agent_id="1")])


class ParallelBroadcastTests(unittest.TestCase):
    def test_broadcast_does_not_block_on_slow_peers(self):
        t = _SlowSendTransport.__new__(_SlowSendTransport)
        for k, v in vars(_bare_transport()).items():
            setattr(t, k, v)
        t.sent = []
        try:
            begin = time.time()
            t.broadcast(payload=_msg(), peers=[2, 3, 4, 5],
                        neighbor_map=_neighbor_map([2, 3, 4, 5]), sender=1)
            elapsed = time.time() - begin
            self.assertLess(elapsed, 0.2)  # 4 x 0.5s sends did NOT run serially inline
            deadline = time.time() + 3.0
            while len(t.sent) < 4 and time.time() < deadline:
                time.sleep(0.02)
            self.assertEqual(sorted(d for d, _ in t.sent), [2, 3, 4, 5])
            # Fan-out sends use reduced retries to bound dead-peer worker occupancy.
            self.assertTrue(all(r == 2 for _, r in t.sent))
        finally:
            if t._bcast_pool is not None:
                t._bcast_pool.shutdown(wait=True)

    def test_saturated_pool_drops_and_counts(self):
        t = _SlowSendTransport.__new__(_SlowSendTransport)
        for k, v in vars(_bare_transport()).items():
            setattr(t, k, v)
        t.sent = []
        t.bcast_workers = 1
        try:
            t._bcast_pool = None  # force re-init with 1 worker / backlog 8
            peers = list(range(2, 30))  # 28 sends >> 1 worker x 8 backlog
            t.broadcast(payload=_msg(), peers=peers,
                        neighbor_map=_neighbor_map(peers), sender=1)
            self.assertGreater(t.bcast_sends_dropped, 0)
        finally:
            if t._bcast_pool is not None:
                t._bcast_pool.shutdown(wait=False)


class SwimFailedAgentsTests(unittest.TestCase):
    def test_failed_excludes_suspects(self):
        from swarm.membership.swim import SwimMembership, _Member, ALIVE, SUSPECT, FAILED

        class _H:
            agent_id = 1
            def peer_ids(self): return [2, 3, 4]
            def send(self, dest, payload): pass
            def log_debug(self, m): pass
            def log_info(self, m): pass
            def log_warn(self, m): pass

        sw = SwimMembership(host=_H())
        with sw._lock:
            sw._members[2] = _Member(agent_id=2, status=FAILED)
            sw._members[3] = _Member(agent_id=3, status=SUSPECT)
            sw._members[4] = _Member(agent_id=4, status=ALIVE)
        self.assertEqual(sw.failed_agents(), [2])   # suspects keep receiving traffic
        self.assertIn(4, sw.live_agents())


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
