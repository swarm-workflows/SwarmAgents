# MIT License
#
# Copyright (c) 2024 swarm-workflows
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Author: Komal Thareja(kthare10@renci.org)
# grpc_client.py
from __future__ import annotations
import logging
import threading
import time
from typing import Callable, Dict, Optional, Any, Tuple

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc
from swarm.comm import consensus_pb2_grpc
from swarm.utils.utils import normalize_host

LOG = logging.getLogger(__name__)

CONNECTIVITY_NAMES = {
    grpc.ChannelConnectivity.IDLE: "IDLE",
    grpc.ChannelConnectivity.CONNECTING: "CONNECTING",
    grpc.ChannelConnectivity.READY: "READY",
    grpc.ChannelConnectivity.TRANSIENT_FAILURE: "TRANSIENT_FAILURE",
    grpc.ChannelConnectivity.SHUTDOWN: "SHUTDOWN",
}

_DEFAULT_OPTS = [
    ("grpc.keepalive_time_ms", 30000),
    ("grpc.keepalive_timeout_ms", 10000),
    ("grpc.keepalive_permit_without_calls", 1),
    ("grpc.http2.max_pings_without_data", 0),
    ("grpc.max_send_message_length", 50 * 1024 * 1024),
    ("grpc.max_receive_message_length", 50 * 1024 * 1024),
    ("grpc.use_local_subchannel_pool", 1),
]

class ChannelEntry:
    def __init__(self, target: str, ch: grpc.Channel, app_stub: Any):
        self.target = target
        self.channel = ch
        self.app_stub = app_stub
        self.health_stub = health_pb2_grpc.HealthStub(ch)
        self.state = grpc.ChannelConnectivity.IDLE
        self.up = False
        self.last_change = time.time()

class ChannelPool:
    """
    Maintains one persistent channel per neighbor, monitors connectivity and health,
    and emits UP/DOWN events via callback.
    """
    def __init__(self,
                 stub_factory: Callable[[grpc.Channel], Any],
                 on_status: Optional[Callable[[str, bool, str], None]] = None,
                 options=None,
                 health_period: float = 2.0):
        self._lock = threading.Lock()
        self._entries: Dict[str, ChannelEntry] = {}
        self._stub_factory = stub_factory
        self._opts = options or _DEFAULT_OPTS
        self._on_status = on_status or (lambda target, up, reason: None)
        self._stop = False
        self._health_period = health_period
        self._watcher = threading.Thread(target=self._health_loop, daemon=True)
        self._watcher.start()


    def get_entry(self, host: str, port: int) -> ChannelEntry:
        target = normalize_host(host, port)
        with self._lock:
            entry = self._entries.get(target)
            if entry is None:
                ch = grpc.insecure_channel(target, options=self._opts)
                app_stub = self._stub_factory(ch)
                entry = ChannelEntry(target, ch, app_stub)
                self._entries[target] = entry

                # Subscribe to connectivity transitions; try_to_connect triggers immediate attempt
                def _cb(state):
                    entry.state = state
                    entry.last_change = time.time()
                    LOG.debug("Channel %s -> %s", target, CONNECTIVITY_NAMES[state])
                    if state == grpc.ChannelConnectivity.READY:
                        self._set_up(target, True, "connectivity=READY")
                    elif state in (grpc.ChannelConnectivity.TRANSIENT_FAILURE, grpc.ChannelConnectivity.SHUTDOWN):
                        self._set_up(target, False, f"connectivity={CONNECTIVITY_NAMES[state]}")

                ch.subscribe(_cb, try_to_connect=True)
            return entry

    def _set_up(self, target: str, up: bool, reason: str):
        with self._lock:
            entry = self._entries.get(target)
            if entry and entry.up != up:
                entry.up = up
                LOG.info("Peer %s marked %s (%s)", target, "UP" if up else "DOWN", reason)
                # Emit event outside the lock to avoid deadlocks in callbacks
        self._on_status(target, up, reason)

    def _health_loop(self):
        while not self._stop:
            time.sleep(self._health_period)
            with self._lock:
                entries = list(self._entries.values())
            for e in entries:
                # If clearly down, reflect that and skip RPC spam
                if e.state in (grpc.ChannelConnectivity.TRANSIENT_FAILURE, grpc.ChannelConnectivity.SHUTDOWN):
                    self._set_up(e.target, False, f"connectivity={CONNECTIVITY_NAMES[e.state]}")
                    continue
                try:
                    # Empty service name = overall server health
                    resp = e.health_stub.Check(
                        health_pb2.HealthCheckRequest(service=''),
                        timeout=0.7
                    )
                    self._set_up(
                        e.target,
                        resp.status == health_pb2.HealthCheckResponse.SERVING,
                        f"health={resp.status}"
                    )
                except grpc.RpcError as ex:
                    # Treat UNAVAILABLE/DEADLINE as down until reconnect
                    self._set_up(e.target, False, f"health_rpc={ex.code().name}")

    def close(self):
        self._stop = True
        self._watcher.join(timeout=1.0)
        with self._lock:
            for e in self._entries.values():
                e.channel.close()
            self._entries.clear()

class GrpcClient:
    """
    High-level client facade that:
      - Reuses one channel per neighbor
      - Provides a generic unary send helper by RPC method name
      - Surfaces liveness via on_peer_status callback
    """
    def __init__(self, on_peer_status: Optional[Callable[[str, bool, str], None]] = None):
        # Build ConsensusStub for each channel
        self.pool = ChannelPool(stub_factory=consensus_pb2_grpc.ConsensusServiceStub, on_status=on_peer_status)

    def get_stub(self, host: str, port: int):
        return self.pool.get_entry(host, port).app_stub

    def call_unary(self, host: str, port: int, rpc_name: str, request, timeout: float = 2.0, retries: int = 4):
        """
        Invoke a unary RPC by name on the ConsensusStub, with light retry on UNAVAILABLE.
        Example: client.call_unary(host, port, "SendProposal", req)
        """
        entry = self.pool.get_entry(host, port)
        rpc = getattr(entry.app_stub, rpc_name)
        delay = 0.05
        for _ in range(max(1, retries)):
            try:
                return rpc(request, timeout=timeout)
            except grpc.RpcError as ex:
                if ex.code() == grpc.StatusCode.UNAVAILABLE:
                    time.sleep(delay)
                    delay = min(delay * 2, 0.8)
                    continue
                raise

    def close(self):
        self.pool.close()
