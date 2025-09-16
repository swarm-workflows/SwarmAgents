# MIT License
#
# Copyright (c) 2024 swarm-workflows
import json
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
import time
import traceback
from typing import Any

from swarm.comm import consensus_pb2
from swarm.comm.consensus_pb2_grpc import add_ConsensusServiceServicer_to_server
from swarm.comm.grpc_clientv2 import GrpcClientV2
from swarm.comm.grpc_serverv2 import GrpcServerV2, ConsensusServiceServicer
from swarm.comm.observer import Observer


class MessageServiceGrpcV2(Observer):
    def __init__(self, host: str, port: int, logger: logging.Logger = logging.getLogger(), on_peer_status = None):
        self.server = GrpcServerV2(observer=self, bind_host=host, bind_port=port)
        self.client = GrpcClientV2(on_peer_status=on_peer_status)
        self.observers = []
        self.logger = logger

    def register_observers(self, agent: Observer):
        if agent not in self.observers:
            self.observers.append(agent)

    def notify_observers(self, msg: dict):
        for o in self.observers:
            o.dispatch_message(msg)

    def dispatch_message(self, message: Any):
        self.logger.debug(f"Received consensus message: {message}")
        self.notify_observers(msg=message.get("payload"))

    def start(self):
        self.server.add_service(add_ConsensusServiceServicer_to_server, ConsensusServiceServicer(self))
        self.server.start()

    def stop(self):
        self.server.stop()

    def send(self, host: str, port: int, msg: dict, dest: int, src: int = None, fwd: int = None):
        sender = src
        if fwd:
            sender = src
        message_dict = {
            "sender_id": str(sender),
            "receiver_id": str(dest),
            "message_type": "consensus",
            "payload": msg
        }
        req = consensus_pb2.ConsensusMessage(
            sender_id=message_dict["sender_id"],
            receiver_id=message_dict["receiver_id"],
            message_type=message_dict["message_type"],
            payload=json.dumps(message_dict["payload"]),
            timestamp=message_dict.get("timestamp", int(time.time()))
        )
        return self.client.call_unary(host, port, "SendMessage", req)