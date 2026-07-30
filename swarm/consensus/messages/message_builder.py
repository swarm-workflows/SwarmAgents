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
from typing import Union

from swarm.consensus.messages.commit import Commit
from swarm.consensus.messages.heart_beat import HeartBeat
from swarm.consensus.messages.message import MessageType, MessageException
from swarm.consensus.messages.prepare import Prepare
from swarm.consensus.messages.proposal import Proposal
from swarm.consensus.messages.job_status import JobStatus
from swarm.consensus.messages.swim_ping import SwimPing
from swarm.consensus.messages.swim_ack import SwimAck
from swarm.consensus.messages.swim_ping_req import SwimPingReq
from swarm.consensus.messages.gossip_state import GossipState
from swarm.consensus.messages.snow_query import SnowQuery
from swarm.consensus.messages.snow_response import SnowResponse
from swarm.consensus.messages.snow_batch import SnowQueryBatch, SnowResponseBatch


class MessageBuilder:
    @staticmethod
    def from_dict(message: dict) -> Union[HeartBeat, Proposal, Prepare, Commit, JobStatus,
                                          SwimPing, SwimAck, SwimPingReq, GossipState,
                                          SnowQuery, SnowResponse,
                                          SnowQueryBatch, SnowResponseBatch]:
        message_type = MessageType(message.get('message_type'))

        if message_type == MessageType.HeartBeat:
            return HeartBeat.from_dict(message)

        elif message_type == MessageType.Proposal:
            return Proposal.from_dict(message)

        elif message_type == MessageType.Prepare:
            return Prepare.from_dict(message)

        elif message_type == MessageType.Commit:
            return Commit.from_dict(message)

        elif message_type == MessageType.JobStatus:
            return JobStatus.from_dict(message)

        elif message_type == MessageType.SwimPing:
            return SwimPing.from_dict(message)

        elif message_type == MessageType.SwimAck:
            return SwimAck.from_dict(message)

        elif message_type == MessageType.SwimPingReq:
            return SwimPingReq.from_dict(message)

        elif message_type == MessageType.GossipState:
            return GossipState.from_dict(message)

        elif message_type == MessageType.SnowQuery:
            return SnowQuery.from_dict(message)

        elif message_type == MessageType.SnowResponse:
            return SnowResponse.from_dict(message)

        elif message_type == MessageType.SnowQueryBatch:
            return SnowQueryBatch.from_dict(message)

        elif message_type == MessageType.SnowResponseBatch:
            return SnowResponseBatch.from_dict(message)

        else:
            raise MessageException(f"Unsupported Message: {message}")