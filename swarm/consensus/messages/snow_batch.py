# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""
Per-peer batched Snow messages.

Sending one wire message per (job, peer, round) makes Snow's message rate
max_inflight x k per tick — measured at ~2,900 sends/s at a 10-coordinator tier,
~5x beyond the send pool's drain rate, causing sustained drops. Batching all of a
tick's queries destined for the same peer into ONE message (and the peer's answers
into one response) collapses demand to at most `peers` messages per tick per agent,
independent of how many decisions are in flight.

Items are plain dicts (query: query_id/job_id/preferred_agent/preferred_cost/round;
response: query_id/job_id/preferred_agent/cost/already_decided) so serialization
stays a single json pass.
"""
from swarm.consensus.messages.message import Message, MessageType


class SnowQueryBatch(Message):
    """All of one tick's Snow queries from `source` destined for one peer."""

    def __init__(self, **kwargs):
        self._items = []
        super().__init__(**kwargs)
        self._message_type = MessageType.SnowQueryBatch

    @property
    def items(self): return self._items

    @items.setter
    def items(self, v): self._items = list(v) if v else []


class SnowResponseBatch(Message):
    """One peer's answers to a SnowQueryBatch, in a single message."""

    def __init__(self, **kwargs):
        self._items = []
        super().__init__(**kwargs)
        self._message_type = MessageType.SnowResponseBatch

    @property
    def items(self): return self._items

    @items.setter
    def items(self, v): self._items = list(v) if v else []
