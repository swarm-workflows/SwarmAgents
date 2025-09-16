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
# File: swarm/comm/grpc_consensus_server.py
# grpc_server.py
from __future__ import annotations

import json
import logging
import grpc
from concurrent import futures
from swarm.comm import consensus_pb2_grpc, consensus_pb2
# Standard gRPC health service
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

from swarm.comm.observer import Observer

LOG = logging.getLogger(__name__)

_DEFAULT_OPTS = [
    ('grpc.keepalive_time_ms', 30000),
    ('grpc.keepalive_timeout_ms', 10000),
    ('grpc.keepalive_permit_without_calls', 1),
    ('grpc.http2.max_pings_without_data', 0),
    ('grpc.max_send_message_length', 50 * 1024 * 1024),
    ('grpc.max_receive_message_length', 50 * 1024 * 1024),
]

class ConsensusServiceServicer(consensus_pb2_grpc.ConsensusServiceServicer):
    def __init__(self, observer: Observer):
        self.observer = observer

    def SendMessage(self, request, context):
        msg = {
            "sender_id": request.sender_id,
            "receiver_id": request.receiver_id,
            "message_type": request.message_type,
            "payload": json.loads(request.payload),
            "timestamp": request.timestamp
        }
        self.observer.dispatch_message(msg)
        return consensus_pb2.Ack(success=True, info="Processed")

class GrpcServerV2:
    def __init__(self, observer: Observer, bind_host: str, bind_port: int, max_workers: int = 32, options=None):
        self.observer = observer
        self.bind_host = bind_host or "127.0.0.1"     # normalize to IPv4 loopback
        self.bind_port = int(bind_port)
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers),
                                   options=options or _DEFAULT_OPTS)
        self._health = health.HealthServicer()
        health_pb2_grpc.add_HealthServicer_to_server(self._health, self._server)
        self._serving = False

    def add_service(self, add_servicer_fn, servicer_impl):
        """
        add_servicer_fn: the generated `add_<Service>Servicer_to_server`
        servicer_impl: your Servicer implementation instance
        """
        add_servicer_fn(servicer_impl, self._server)

    def start(self):
        # Mark health as SERVING only after we’ve registered all services
        self._health.set('', health_pb2.HealthCheckResponse.SERVING)
        endpoint = f"{self.bind_host}:{self.bind_port}"
        self._server.add_insecure_port(endpoint)
        self._server.start()
        self._serving = True
        LOG.info("gRPC server listening on %s", endpoint)

    def stop(self, grace: float = 1.0):
        if self._serving:
            try:
                self._health.set('', health_pb2.HealthCheckResponse.NOT_SERVING)
            except Exception:
                pass
        self._server.stop(grace)
        self._serving = False
        LOG.info("gRPC server stopped (%s)", f"{self.bind_host}:{self.bind_port}")

    @property
    def health(self) -> health.HealthServicer:
        return self._health

    @property
    def server(self):
        return self._server
