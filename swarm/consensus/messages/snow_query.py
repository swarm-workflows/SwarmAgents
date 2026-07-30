# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
from swarm.consensus.messages.message import Message, MessageType


class SnowQuery(Message):
    """
    Initiator's k-peer query for a job assignment decision.

    Carries the initiator's currently preferred assignee and its cost, plus a
    query_id correlating responses for this round.
    """

    def __init__(self, **kwargs):
        self._query_id = None
        self._job_id = None
        self._preferred_agent = None
        self._preferred_cost = None
        self._round = 0
        super().__init__(**kwargs)
        self._message_type = MessageType.SnowQuery

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
    def preferred_cost(self): return self._preferred_cost
    @preferred_cost.setter
    def preferred_cost(self, v):
        self._preferred_cost = float(v) if v is not None else None

    @property
    def round(self): return self._round
    @round.setter
    def round(self, v): self._round = int(v or 0)
