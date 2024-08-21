from typing import Tuple

from swarm.comm.messages.commit import Commit
from swarm.comm.messages.heart_beat import HeartBeat
from swarm.comm.messages.message import MessageType, MessageException
from swarm.comm.messages.prepare import Prepare
from swarm.comm.messages.proposal import Proposal
from swarm.comm.messages.job_status import JobStatus


class MessageBuilder:
    @staticmethod
    def from_dict(message: dict) -> Tuple[HeartBeat, Proposal, Prepare, Commit, JobStatus]:
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

        else:
            raise MessageException(f"Unsupported Message: {message}")