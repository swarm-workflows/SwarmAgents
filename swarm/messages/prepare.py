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
from swarm.messages.message import MessageType
from swarm.messages.proposal import Proposal


class Prepare(Proposal):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._message_type = MessageType.Prepare


if __name__ == '__main__':
    from swarm.models.proposal_info import ProposalInfo
    p_info = ProposalInfo(p_id="pid_1", job_id='t-1', seed=0.6, agent_id="0", commits=["1"])
    print(p_info)
    print(p_info.to_dict())
    prepare = Prepare(proposals=[p_info])
    print(prepare)
    print(prepare.to_dict())
    print(prepare.to_json())

    new_p = Proposal.from_dict(prepare.to_dict())
    print(new_p)