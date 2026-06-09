# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
from typing import List, Union

from swarm.consensus.messages.agent_state_entry import AgentStateEntry
from swarm.consensus.messages.message import Message, MessageType


class GossipState(Message):
    """
    Push gossip of one or more agents' versioned state to a peer.

    The disseminator drains its local state cache once per round and sends the
    full set to ``fanout`` random peers. Receivers merge by version.
    """

    def __init__(self, **kwargs):
        self._entries: List[AgentStateEntry] = []
        super().__init__(**kwargs)
        self._message_type = MessageType.GossipState

    @property
    def entries(self) -> List[AgentStateEntry]:
        return self._entries

    @entries.setter
    def entries(self, values: Union[List[AgentStateEntry], List[dict]]):
        if not isinstance(values, list):
            raise ValueError("entries must be a list")
        self._entries = [
            v if isinstance(v, AgentStateEntry) else AgentStateEntry.from_dict(v)
            for v in values
        ]
