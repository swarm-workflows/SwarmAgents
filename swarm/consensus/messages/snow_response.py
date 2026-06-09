# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
from swarm.consensus.messages.message import Message, MessageType


class SnowResponse(Message):
    """
    Peer's reply to a SnowQuery.

    ``preferred_agent`` is the assignee the peer thinks should win for this
    job, derived from its local cost computation and (when available) gossip
    state. When ``already_decided`` is True the peer reports that the job has
    been committed; ``preferred_agent`` then equals the committed winner.
    """

    def __init__(self, **kwargs):
        self._query_id = None
        self._job_id = None
        self._preferred_agent = None
        self._cost = None
        self._already_decided = False
        super().__init__(**kwargs)
        self._message_type = MessageType.SnowResponse

    @property
    def query_id(self): return self._query_id
    @query_id.setter
    def query_id(self, v): self._query_id = v

    @property
    def job_id(self): return self._job_id
    @job_id.setter
    def job_id(self, v): self._job_id = v

    @property
    def preferred_agent(self): return self._preferred_agent
    @preferred_agent.setter
    def preferred_agent(self, v):
        self._preferred_agent = int(v) if v is not None else None

    @property
    def cost(self): return self._cost
    @cost.setter
    def cost(self, v):
        self._cost = float(v) if v is not None else None

    @property
    def already_decided(self): return self._already_decided
    @already_decided.setter
    def already_decided(self, v): self._already_decided = bool(v)
