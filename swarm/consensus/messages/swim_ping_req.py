# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Author: Komal Thareja(kthare10@renci.org)
from typing import List, Union

from swarm.consensus.messages.message import Message, MessageType
from swarm.consensus.messages.membership_update import MembershipUpdate


class SwimPingReq(Message):
    """
    Indirect-probe request: ask the recipient to ping `target_agent` on the
    initiator's behalf and relay the resulting ack back.
    """

    def __init__(self, **kwargs):
        self._probe_id = None
        self._target_agent = None
        self._updates: List[MembershipUpdate] = []
        super().__init__(**kwargs)
        self._message_type = MessageType.SwimPingReq

    @property
    def probe_id(self):
        return self._probe_id

    @probe_id.setter
    def probe_id(self, v):
        self._probe_id = v

    @property
    def target_agent(self):
        return self._target_agent

    @target_agent.setter
    def target_agent(self, v):
        self._target_agent = int(v) if v is not None else None

    @property
    def updates(self) -> List[MembershipUpdate]:
        return self._updates

    @updates.setter
    def updates(self, values: Union[List[MembershipUpdate], List[dict]]):
        if not isinstance(values, list):
            raise ValueError("updates must be a list")
        self._updates = [
            v if isinstance(v, MembershipUpdate) else MembershipUpdate.from_dict(v)
            for v in values
        ]
