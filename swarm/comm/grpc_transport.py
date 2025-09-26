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
from typing import Any

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
        self.server = GrpcServer(on_message=self.on_message, bind_host=host, bind_port=port)
        self.client = GrpcClient(on_peer_status=on_peer_status)
        self.observers = []
        self.logger = logger

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

    def send(self, host: str, port: int, src: int, dest: int, payload: object) -> None:
        if not isinstance(payload, Message):
            raise TypeError("Payload must be of Message type")
        message_dict = {
            "sender_id": str(src),
            "receiver_id": str(dest),
            "message_type": str(payload.message_type),
            "payload": payload.to_dict()
        }
        req = consensus_pb2.ConsensusMessage(
            sender_id=message_dict["sender_id"],
            receiver_id=message_dict["receiver_id"],
            message_type=message_dict["message_type"],
            payload=json.dumps(message_dict["payload"]),
            timestamp=message_dict.get("timestamp", int(time.time()))
        )
        self.client.call_unary(host, port, "SendMessage", req, timeout=2.0, retries=4)

    def broadcast(self, payload: object, peers: list[int], neighbor_map: object, sender: int) -> None:
        if not isinstance(payload, Message):
            raise TypeError("Payload must be of Message type")

        if not isinstance(neighbor_map, ThreadSafeDict):
            raise TypeError("Neighbor map must be ThreadSafeDict")

        payload.path.append(sender)
        for peer_id in peers:
            if peer_id in payload.path:
                continue

            peer_info = neighbor_map.get(peer_id)
            if not peer_info:
                continue

            self.send(host=peer_info.host, port=peer_info.port, payload=payload,
                      dest=peer_info.agent_id, src=sender)
