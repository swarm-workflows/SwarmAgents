import random
from typing import List

from swarm.models.json_field import JSONField


class Proposal(JSONField):
    def __init__(self, p_id: str, task_id: str, agent_id: str,
                 prepares: int = 0, commits: int = 0, seed: float = None):
        self.p_id = p_id
        self.task_id = task_id
        self.prepares = prepares
        self.commits = commits
        self.agent_id = agent_id
        self.seed = seed if seed else round(random.random(), 2)

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
                report = f"Unable to set field {k} of capacity, no such field available "\
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
        self.proposals_by_task_id = {}
        self.proposals_by_pid = {}

    def add_proposal(self, proposal: Proposal):
        if proposal.task_id not in self.proposals_by_task_id:
            self.proposals_by_task_id[proposal.task_id] = []

        self.proposals_by_task_id[proposal.task_id].append(proposal)

        self.proposals_by_pid[proposal.p_id] = proposal

    def contains(self, task_id: str = None, p_id: str = None):
        if task_id is None and p_id is None:
            return False

        if task_id and task_id not in self.proposals_by_task_id:
            return False

        if p_id and p_id not in self.proposals_by_pid:
            return False

        if task_id and p_id:
            for p in self.proposals_by_task_id[task_id]:
                if p_id == p.p_id:
                    return True
            return False
        else:
            return True

    def get_proposal(self, task_id: str = None, p_id: str = None) -> Proposal:
        if task_id is not None and p_id is not None:
            for p in self.proposals_by_task_id[task_id]:
                if p_id == p.p_id:
                    return p
        elif p_id is not None:
            return self.proposals_by_pid.get(p_id)
        elif task_id is not None:
            proposals = self.proposals_by_task_id.get(task_id)
            if proposals and len(proposals):
                return next(iter(proposals))

    def size(self):
        return len(self.proposals_by_pid)

    def remove_proposal(self, p_id: str, task_id: str):
        if p_id in self.proposals_by_pid:
            self.proposals_by_pid.pop(p_id)

        if task_id in self.proposals_by_task_id:
            if p_id in self.proposals_by_task_id[task_id]:
                self.proposals_by_task_id[task_id].pop(p_id)
            if len(self.proposals_by_task_id[task_id]) == 0:
                self.proposals_by_task_id.pop(task_id)

    def remove_task(self, task_id: str):
        if task_id in self.proposals_by_task_id:
            for p in self.proposals_by_task_id[task_id]:
                self.proposals_by_pid.get(p.p_id)
            self.proposals_by_task_id.pop(task_id)

    def tasks(self) -> List[str]:
        return list(self.proposals_by_task_id.keys())
