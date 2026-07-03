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
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional
import json

from swarm.comm import consensus_pb2
from swarm.comm.consensus_pb2_grpc import add_ConsensusServiceServicer_to_server
from swarm.comm.grpc_client import GrpcClient
from swarm.comm.grpc_server import GrpcServer, ConsensusServiceServicer
from swarm.consensus.messages.message import Message
from swarm.comm.observer import Observer
from swarm.utils.thread_safe_dict import ThreadSafeDict


class GrpcTransport(Observer):
    def __init__(self, host: str, port: int, logger: logging.Logger = logging.getLogger(),
                 on_peer_status = None):
        #self.server = GrpcServer(on_message=self.on_message, bind_host=host, bind_port=port)
        self.server = GrpcServer(on_message=self.on_message, bind_host="0.0.0.0", bind_port=port) # Start in all interfaces
        self.client = GrpcClient(on_peer_status=on_peer_status)
        self.observers = []
        self.logger = logger
        # Broadcast latency instrumentation (post-parallelization this measures the
        # submit loop; [BCAST_SLOW] then indicates pool saturation, not a dead peer).
        self.broadcasts = 0
        self.broadcast_time_total = 0.0
        self.broadcast_time_max = 0.0
        # Parallel fan-out pool: broadcast() must never block a consensus phase on one
        # slow/dead peer (serially, one dead peer cost ~8.7s per phase: 2s timeout x 4
        # retries + backoff). Sends are fire-and-forget with reduced retries — PBFT
        # quorum plus the pending-message replay recover from individual losses.
        self._bcast_pool: Optional[ThreadPoolExecutor] = None
        self._bcast_sem: Optional[threading.BoundedSemaphore] = None
        self.bcast_workers = 16
        self.bcast_sends_dropped = 0

    def register_observers(self, observer: Observer):
        if observer not in self.observers:
            self.observers.append(observer)

    def notify_observers(self, msg: dict):
        for o in self.observers:
            o.on_message(msg)

    def on_message(self, message: Any):
        self.logger.debug(f"Received consensus message: {message}")
        self.notify_observers(msg=message.get("payload"))

    def start(self):
        self.server.add_service(add_ConsensusServiceServicer_to_server, ConsensusServiceServicer(self))
        self.server.start()

    def stop(self):
        self.server.stop()
        if self._bcast_pool is not None:
            self._bcast_pool.shutdown(wait=False)
            self._bcast_pool = None
            self._bcast_sem = None

    def send(self, host: str, port: int, src: int, dest: int, payload: object,
             timeout: float = 2.0, retries: int = 4) -> None:
        if not isinstance(payload, Message):
            raise TypeError("Payload must be of Message type")
        req = consensus_pb2.ConsensusMessage(
            sender_id=str(src),
            receiver_id=str(dest),
            message_type=str(payload.message_type),
            payload=json.dumps(payload.to_dict()),
            timestamp=int(time.time())
        )
        self.client.call_unary(host, port, "SendMessage", req, timeout=timeout, retries=retries)

    def _send_raw(self, host: str, port: int, src: int, dest: int,
                  payload_json: str, msg_type: str,
                  timeout: float = 2.0, retries: int = 4) -> None:
        req = consensus_pb2.ConsensusMessage(
            sender_id=str(src),
            receiver_id=str(dest),
            message_type=msg_type,
            payload=payload_json,
            timestamp=int(time.time())
        )
        self.client.call_unary(host, port, "SendMessage", req, timeout=timeout, retries=retries)

    def _bcast_send(self, host: str, port: int, src: int, dest: int,
                    payload_json: str, msg_type: str) -> None:
        """One fan-out send on a pool worker. Reduced retries (2 vs 4) bound how long a
        dead peer can occupy a worker (~4s vs ~8.7s); quorum + pending-replay recover
        individual losses."""
        try:
            self._send_raw(host=host, port=port, src=src, dest=dest,
                           payload_json=payload_json, msg_type=msg_type,
                           timeout=2.0, retries=2)
        except Exception as exc:
            self.logger.debug(f"[bcast] send -> {dest} failed: {exc}")
        finally:
            if self._bcast_sem is not None:
                self._bcast_sem.release()

    def broadcast(self, payload: object, peers: list[int], neighbor_map: object, sender: int) -> None:
        if not isinstance(payload, Message):
            raise TypeError("Payload must be of Message type")

        if not isinstance(neighbor_map, ThreadSafeDict):
            raise TypeError("Neighbor map must be ThreadSafeDict")

        # Lazy pool init (broadcast can be called before/without start()).
        if self._bcast_pool is None:
            self._bcast_pool = ThreadPoolExecutor(
                max_workers=self.bcast_workers, thread_name_prefix="bcast")
            self._bcast_sem = threading.BoundedSemaphore(self.bcast_workers * 8)

        payload.path.append(sender)
        payload_json = json.dumps(payload.to_dict())
        msg_type = str(payload.message_type)
        begin = time.time()
        for peer_id in peers:
            if peer_id in payload.path:
                continue

            peer_info = neighbor_map.get(peer_id)
            if not peer_info:
                continue

            # Concurrent fire-and-forget: a consensus phase must never serially block
            # on one slow/dead peer (previously ~8.7s per dead peer per phase).
            if not self._bcast_sem.acquire(blocking=False):
                self.bcast_sends_dropped += 1
                continue
            try:
                self._bcast_pool.submit(
                    self._bcast_send, peer_info.host, peer_info.port,
                    sender, peer_info.agent_id, payload_json, msg_type)
            except RuntimeError:  # pool shutting down
                self._bcast_sem.release()
                self.bcast_sends_dropped += 1
        elapsed = time.time() - begin
        self.broadcasts += 1
        self.broadcast_time_total += elapsed
        if elapsed > self.broadcast_time_max:
            self.broadcast_time_max = elapsed
        if elapsed > 1.0:
            self.logger.warning(
                f"[BCAST_SLOW] {msg_type} submit to {len(peers)} peers took {elapsed:.2f}s "
                f"(fan-out pool saturated; dropped so far: {self.bcast_sends_dropped})")
