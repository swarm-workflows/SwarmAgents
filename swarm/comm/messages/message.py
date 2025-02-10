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
import enum
from typing import List, Union

from swarm.models.json_field import JSONField
from swarm.models.agent_info import AgentInfo


class MessageType(enum.Enum):
    HeartBeat = enum.auto()   #1
    JobStatus = enum.auto()  #2
    Proposal = enum.auto()    #3
    Prepare = enum.auto()     #4
    Commit = enum.auto()      #5

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name


class MessageException(Exception):
    """
    Exception with a capacity
    """
    def __init__(self, msg: str):
        assert msg is not None
        super().__init__(f"Peer exception: {msg}")


class Message(JSONField):
    def __init__(self, **kwargs):
        self._forwarded_by = None
        self._message_type = None
        self._agents = []  # List of agents
        self._set_fields(**kwargs)

    @property
    def forwarded_by(self) -> str:
        return self._forwarded_by

    @forwarded_by.setter
    def forwarded_by(self, value: str):
        self._forwarded_by = value

    @property
    def message_type(self) -> MessageType:
        return self._message_type

    @message_type.setter
    def message_type(self, value: Union[MessageType, int]):
        if isinstance(value, MessageType):
            self._message_type = value
        elif isinstance(value, int):
            self._message_type = MessageType(value)
        else:
            raise ValueError("Unsupported value type for message type")

    @property
    def agents(self) -> List[AgentInfo]:
        return self._agents

    @agents.setter
    def agents(self, value: Union[List[dict], List[AgentInfo]]):
        if isinstance(value, list):
            self._agents = [AgentInfo.from_dict(v) if isinstance(v, dict) else v for v in value]
        else:
            raise ValueError("Agents must be a list of AgentInfo objects or dictionaries")

    def _set_fields(self, forgiving=False, **kwargs):
        """
        Set fields
        :param kwargs:
        :return: self to support call chaining
        """
        for k, v in kwargs.items():
            try:
                # will toss an exception if field is not defined
                self.__getattribute__(k)
                self.__setattr__(k, v)
            except AttributeError:
                report = f"Unable to set field {k} of message, no such field available "\
                       f"{[k for k in self.__dict__.keys()]}"
                if forgiving:
                    print(report)
                else:
                    raise MessageException(report)
        return self

    def __str__(self):
        return f"message_type: {self.message_type}, agents: {self.agents}, forwarded_by: {self.forwarded_by}"
