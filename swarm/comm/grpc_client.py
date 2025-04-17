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
import grpc
import json
import time

from swarm.comm import consensus_pb2_grpc, consensus_pb2


class GrpcClientManager:
    def __init__(self):
        self.stub_cache = {}  # (host, port) -> (stub, channel)

    def _create_stub(self, host, port):
        channel = grpc.insecure_channel(f"{host}:{port}")
        stub = consensus_pb2_grpc.ConsensusServiceStub(channel)
        self.stub_cache[(host, port)] = (stub, channel)
        return stub

    def get_stub(self, host, port):
        key = (host, port)
        if key in self.stub_cache:
            stub, channel = self.stub_cache[key]
            state = channel._channel.check_connectivity_state(True)
            if state != grpc.ChannelConnectivity.READY:
                self.stub_cache.pop(key)
                return self._create_stub(host, port)
            return stub
        else:
            return self._create_stub(host, port)

    def send_consensus_message(self, host, port, message_dict):
        stub = self.get_stub(host, port)
        request = consensus_pb2.ConsensusMessage(
            sender_id=message_dict["sender_id"],
            receiver_id=message_dict["receiver_id"],
            message_type=message_dict["message_type"],
            payload=json.dumps(message_dict["payload"]),
            timestamp=message_dict.get("timestamp", int(time.time()))
        )
        try:
            return stub.SendMessage(request, timeout=2)
        except grpc.RpcError as e:
            print(f"[ERROR] gRPC to {host}:{port} failed: {e.code()} - {e.details()}")
            self.stub_cache.pop((host, port), None)
            return None
