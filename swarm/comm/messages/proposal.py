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
from typing import List, Tuple

from swarm.comm.messages.message import Message, MessageType
from swarm.models.proposal_infov2 import ProposalInfo


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
                    raise ValueError(f"Unsupported value {type(v)} for proposals")
        else:
            raise ValueError(f"Unsupported value type {type(values)} for proposals")

    def __str__(self):
        return f"[agent: {self.agents}, proposals: {self.proposals}]"


if __name__ == '__main__':
    p_info = ProposalInfo(p_id="pid_1", object_id='t-1', seed=0.6, agent_id="0")
    print(p_info)
    print(p_info.to_dict())
    proposal = Proposal(proposals=[p_info])
    print(proposal)
    print(proposal.to_dict())
    print(proposal.to_json())

    new_p = Proposal.from_dict(proposal.to_dict())
    print(new_p)
    print(new_p.proposals[0].object_id)