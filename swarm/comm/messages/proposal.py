from typing import List, Tuple

from swarm.comm.messages.message import Message, MessageType
from swarm.models.proposal_info import ProposalInfo


class Proposal(Message):
    def __init__(self, **kwargs):
        self._proposals = []
        super().__init__(**kwargs)
        self._message_type = MessageType.Proposal

    @property
    def proposals(self) -> List[ProposalInfo]:
        return self._proposals

    @proposals.setter
    def proposals(self, values: List[Tuple[ProposalInfo, dict]]):
        if isinstance(values, list):
            for v in values:
                if isinstance(v, ProposalInfo):
                    self._proposals.append(v)
                elif isinstance(v, dict):
                    self._proposals.append(ProposalInfo.from_dict(v))
                else:
                    raise ValueError("Unsupported value type for proposals")
        else:
            raise ValueError("Unsupported value type for proposals")

    def __str__(self):
        return f"[agent: {self.agent}, proposals: {self.proposals}]"


if __name__ == '__main__':
    p_info = ProposalInfo(p_id="pid_1", job_id='t-1', seed=0.6, agent_id="0")
    print(p_info)
    print(p_info.to_dict())
    proposal = Proposal(proposals=[p_info])
    print(proposal)
    print(proposal.to_dict())
    print(proposal.to_json())

    new_p = Proposal.from_dict(proposal.to_dict())
    print(new_p)