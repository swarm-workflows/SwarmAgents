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
from swarm.comm.messages.message import Message, MessageType
from swarm.models.agent_info import AgentInfo


class HeartBeat(Message):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.message_type = MessageType.HeartBeat


if __name__ == '__main__':
    from swarm.models.capacities import Capacities

    agent = AgentInfo(agent_id="agent-1", load=0.1, capacities=Capacities(core=1.0, disk=1.0, ram=1.0),)
    print(agent)
    print(agent.to_dict())

    heartbeat = HeartBeat(agents=[agent])
    print(heartbeat)
    print(heartbeat.to_dict())
    print(heartbeat.to_json())

    h = HeartBeat.from_dict(heartbeat.to_dict())
    print(f"Heartbeat object: {h}")
