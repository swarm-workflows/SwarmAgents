"""The inbound consumer never waits on Redis or on a peer (2026-10-12).

In a Hier-90 Hybrid+LinUCB run a leaf's inbound queue reached 3,477 messages while it received
~10/s. `inbound_timing` (43596385) put the time in two places: `get_object`'s Redis fallback,
one WAN round trip per message about a job the leaf did not hold (310 s on one leaf, 7,371
calls on another), and SWIM acks sent synchronously from the ping handler (250 s).
"""
import threading
import time
from unittest.mock import MagicMock

from swarm.agents.resource_agent import ResourceAgent, _HostAdapter, _SwimAdapter
from swarm.models.job import Job
from swarm.models.object import ObjectState


def _agent():
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = MagicMock()
    a.queues = MagicMock()
    a.queues.pending_queue.get.return_value = None
    a.repository = MagicMock()
    a.topology = MagicMock(level=0, group=0)
    a._pending_lock = threading.RLock()
    a.pending_proposals, a.pending_prepares, a.pending_commits = {}, {}, {}
    a.pending_consensus_dropped = 0
    return a


def _record(job_id, state=ObjectState.PENDING):
    j = Job()
    j.job_id = job_id
    j.state = state
    return j.to_dict()


class TestGetObject:
    def test_the_inbound_thread_never_reads_redis(self):
        a = _agent()
        a._inbound_thread_ident = threading.get_ident()
        assert _HostAdapter(a).get_object("j1") is None
        a.repository.get.assert_not_called()
        assert a.inbound_fetch_deferred == 1

    def test_another_thread_still_reads_redis(self):
        a = _agent()
        a._inbound_thread_ident = -1
        a.repository.get.return_value = _record("j1")
        assert _HostAdapter(a).get_object("j1") is not None
        a.repository.get.assert_called_once()


class TestStashThenFetch:
    def test_a_stash_requests_the_job_after_storing_the_message(self):
        a = _agent()
        seen = []
        a._request_job_fetch = lambda jid: seen.append((jid, list(a.pending_prepares.get(jid, []))))
        _HostAdapter(a).set_pending_prepare("msg", "j1")
        assert seen == [("j1", ["msg"])]

    def test_a_pending_record_joins_the_queue_and_replays(self):
        a = _agent()
        a._fetch_guard, a._fetch_inflight = threading.Lock(), {"j1", "j2"}
        a.repository.get_many.return_value = {"j1": _record("j1"),
                                              "j2": _record("j2", ObjectState.READY)}
        a._replay_pending_consensus = MagicMock()
        a._job_fetch_batch(["j1", "j2"])
        a.queues.pending_queue.add.assert_called_once()
        a._replay_pending_consensus.assert_called_once_with("j1")
        assert "j2" in a._fetch_negative and "j1" not in a._fetch_negative
        assert not a._fetch_inflight

    def test_a_job_past_pending_is_not_fetched_again_soon(self):
        a = _agent()
        a._fetch_negative = {"j2": time.monotonic() + 10}
        a._fetch_queue = MagicMock()
        a._request_job_fetch("j2")
        a._fetch_queue.put.assert_not_called()

    def test_one_request_per_job_in_flight(self):
        a = _agent()
        a._fetch_queue = MagicMock()
        a._request_job_fetch("j1")
        a._request_job_fetch("j1")
        assert a._fetch_queue.put.call_count == 1


class TestAsyncSends:
    def test_swim_sends_fire_and_forget(self):
        agent = MagicMock()
        _SwimAdapter(agent).send(3, "ack")
        agent.send_async.assert_called_once_with(3, "ack")
        agent.send.assert_not_called()

    def test_a_saturated_pool_drops_and_counts(self):
        from swarm.comm.grpc_transport import GrpcTransport
        from swarm.consensus.messages.swim_ack import SwimAck
        t = GrpcTransport.__new__(GrpcTransport)
        t._init_instrumentation()
        t.bcast_workers = 1
        t._bcast_pool = MagicMock()
        t._bcast_sem = threading.BoundedSemaphore(1)
        t._bcast_sem.acquire()
        ok = t.send_async(host="h", port=1, src=1, dest=2,
                          payload=SwimAck(source=1, probe_id="p", target_agent=1, updates=[]))
        assert ok is False and t.bcast_sends_dropped == 1
        t._bcast_pool.submit.assert_not_called()
