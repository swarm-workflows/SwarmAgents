import enum
from typing import Tuple

from swarm.models.json_field import JSONField
from swarm.models.agent_info import AgentInfo


class MessageType(enum.Enum):
    HeartBeat = enum.auto()   #1
    TaskStatus = enum.auto()  #2
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
        self._message_type = None
        self._agent = None
        self._set_fields(**kwargs)

    @property
    def message_type(self) -> MessageType:
        return self._message_type

    @message_type.setter
    def message_type(self, value: Tuple[MessageType, int]):
        if isinstance(value, MessageType):
            self._message_type = value
        elif isinstance(value, int):
            self._message_type = MessageType(value)
        else:
            raise ValueError("Unsupported value type for message type")

    @property
    def agent(self) -> AgentInfo:
        return self._agent

    @agent.setter
    def agent(self, value: dict):
        if isinstance(value, AgentInfo):
            self._agent = value
        elif isinstance(value, dict):
            self._agent = AgentInfo.from_dict(value)
        else:
            raise ValueError("Unsupported value type for agent")

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
        return f"message_type: {self.message_type}, agent: {self.agent}"
