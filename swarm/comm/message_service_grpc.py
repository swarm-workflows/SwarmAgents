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
from typing import Any

from swarm.comm.grpc_client import GrpcClientManager
from swarm.comm.grpc_server import GrpcServer
from swarm.comm.observer import Observer


class MessageServiceGrpc(Observer):
    def __init__(self, port: int, logger: logging.Logger = logging.getLogger()):
        self.server = GrpcServer(port=port, observer=self, logger=logger)
        self.client = GrpcClientManager()
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
        self.server.start()

    def stop(self):
        self.server.stop()
        self.server.wait_for_termination()

    def produce_message(self, json_message: dict, topic: str = None, src: int = None,
                        dest: int = None, fwd: int = None):
        host, port = topic.split(":")

        sender = src
        if fwd:
            sender = src
        self.client.send_consensus_message(host=host,
                                           port=port,
                                           message_dict={
                                               "sender_id": str(sender),
                                               "receiver_id": str(dest),
                                               "message_type": "consensus",
                                               "payload": json_message
                                           })
