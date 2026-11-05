"""The send pool is sized from config and holds a burst (2026-10-13).

16 workers / 128 queued sends, with a full pool DROPPING the send, made Hier-90 PBFT
coordinators drop 49,426 sends a run once the inbound consumer stopped blocking; PBFT never
retransmits, so ~200 elections per coordinator stalled until the reselection timeout.
"""
import threading
from unittest.mock import MagicMock

import pytest

from swarm.agents.resource_agent import ResourceAgent as Agent
from swarm.comm.grpc_transport import FANOUT_QUEUE_DEFAULT, FANOUT_WORKERS_DEFAULT, GrpcTransport
from swarm.consensus.messages.swim_ack import SwimAck
from swarm.utils.yaml_strict import safe_load


def _transport(workers, queue_len):
    t = GrpcTransport.__new__(GrpcTransport)
    t._init_instrumentation()
    t._bcast_pool = None
    t._bcast_sem = None
    t.bcast_workers, t.bcast_queue = workers, queue_len
    return t


def test_the_queue_admits_its_configured_depth_before_dropping():
    t = _transport(2, 5)
    t._ensure_pool()
    t._bcast_pool.shutdown(wait=False)
    t._bcast_pool = MagicMock()              # submissions never complete: the queue just fills
    ack = SwimAck(source=1, probe_id="p", target_agent=1, updates=[])
    sent = [t.send_async(host="h", port=1, src=1, dest=2, payload=ack) for _ in range(7)]
    assert sent == [True] * 5 + [False] * 2
    assert t.bcast_sends_dropped == 2


def _agent(runtime):
    a = Agent.__new__(Agent)
    a.runtime_config = runtime
    a.transport = MagicMock()
    return a


def test_the_agent_applies_the_keys():
    a = _agent({"fanout_workers": 32, "fanout_queue": 512})
    a._configure_fanout()
    assert (a.transport.bcast_workers, a.transport.bcast_queue) == (32, 512)


def test_absent_keys_take_the_one_default():
    a = _agent({})
    a._configure_fanout()
    assert (a.transport.bcast_workers, a.transport.bcast_queue) == (
        FANOUT_WORKERS_DEFAULT, FANOUT_QUEUE_DEFAULT)


@pytest.mark.parametrize("runtime", [{"fanout_workers": 0}, {"fanout_workers": 64, "fanout_queue": 8}])
def test_nonsense_raises(runtime):
    with pytest.raises(ValueError):
        _agent(runtime)._configure_fanout()


def test_the_shipped_config_equals_the_code_defaults():
    with open("config_swarm_multi.yml") as f:
        runtime = safe_load(f)["runtime"]
    assert runtime["fanout_workers"] == FANOUT_WORKERS_DEFAULT
    assert runtime["fanout_queue"] == FANOUT_QUEUE_DEFAULT
