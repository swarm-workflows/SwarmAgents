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
import random
from typing import List

from swarm.models.json_field import JSONField


class ProposalInfo(JSONField):
    def __init__(self, **kwargs):
        self.p_id = None
        self.job_id = None
        self.prepares = []
        self.commits = []
        self.agent_id = None
        self.seed = 0
        # self.seed = round(random.random(), 5)
        # Decrease chances of collision
        #base_value = round(random.random(), 10)
        #offset = round(random.uniform(0, 1e-10), 10)
        #self.seed = base_value + offset
        # TODO try this as second option
        # secure_random = random.SystemRandom()
        # unique_value = round(secure_random.random(), 10)
        self._set_fields(**kwargs)

    def _set_fields(self, forgiving=False, **kwargs):
        """
        Universal integer setter for all fields.
        if you try to set a non-existent field.
        :param kwargs:
        :return: self to support call chaining
        """
        for k, v in kwargs.items():
            try:
                # will toss an exception if field is not defined
                self.__getattribute__(k)
                self.__setattr__(k, v)
            except AttributeError:
                report = f"Unable to set field {k} of ProposalInfo, no such field available "\
                       f"{[k for k in self.__dict__.keys()]}"
                if forgiving:
                    print(report)
                else:
                    raise ProposalException(report)
        return self


class ProposalException(Exception):
    """
    Exception with a capacity
    """
    def __init__(self, msg: str):
        assert msg is not None
        super().__init__(f"ProposalException exception: {msg}")


class ProposalContainer:
    def __init__(self):
        self.proposals_by_job_id = {}
        self.proposals_by_pid = {}

    def add_proposal(self, proposal: ProposalInfo):
        if proposal.job_id not in self.proposals_by_job_id:
            self.proposals_by_job_id[proposal.job_id] = []

        self.proposals_by_job_id[proposal.job_id].append(proposal)

        self.proposals_by_pid[proposal.p_id] = proposal

    def contains(self, job_id: str = None, p_id: str = None):
        if job_id is None and p_id is None:
            return False

        if job_id and job_id not in self.proposals_by_job_id:
            return False

        if p_id and p_id not in self.proposals_by_pid:
            return False

        if job_id and p_id:
            for p in self.proposals_by_job_id[job_id]:
                if p_id == p.p_id:
                    return True
            return False
        else:
            return True

    def get_proposal(self, job_id: str = None, p_id: str = None) -> ProposalInfo:
        if job_id is not None and p_id is not None:
            for p in self.proposals_by_job_id[job_id]:
                if p_id == p.p_id:
                    return p
        elif p_id is not None:
            return self.proposals_by_pid.get(p_id)
        elif job_id is not None:
            proposals = self.proposals_by_job_id.get(job_id)
            if proposals and len(proposals):
                return next(iter(proposals))

    def get_proposals_by_job_id(self, job_id: str) -> List[ProposalInfo]:
        return self.proposals_by_job_id.get(job_id, [])

    def size(self):
        return len(self.proposals_by_pid)

    def remove_proposal(self, p_id: str, job_id: str):
        if p_id in self.proposals_by_pid:
            self.proposals_by_pid.pop(p_id)

        if job_id in self.proposals_by_job_id:
            if p_id in self.proposals_by_job_id[job_id]:
                self.proposals_by_job_id[job_id].pop(p_id)
            if len(self.proposals_by_job_id[job_id]) == 0:
                self.proposals_by_job_id.pop(job_id)

    def remove_job(self, job_id: str):
        if job_id in self.proposals_by_job_id:
            for p in self.proposals_by_job_id[job_id]:
                self.proposals_by_pid.pop(p.p_id)
            self.proposals_by_job_id.pop(job_id)

    def jobs(self) -> List[str]:
        return list(self.proposals_by_job_id.keys())

    def has_better_proposal(self, proposal: ProposalInfo) -> ProposalInfo:
        better = None
        for p in self.get_proposals_by_job_id(proposal.job_id):
            if p.seed < proposal.seed or (p.seed == proposal.seed and p.agent_id < proposal.agent_id):
                better = p
                break

        return better
