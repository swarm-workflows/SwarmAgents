from swarm.comm.messages.message import MessageType
from swarm.comm.messages.proposal import Proposal


class Commit(Proposal):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.message_type = MessageType.Commit


if __name__ == '__main__':
    from swarm.models.proposal_info import ProposalInfo
    p_info = ProposalInfo(p_id="pid_1", job_id='t-1', seed=0.6, agent_id="0", commits=1)
    print(p_info)
    print(p_info.to_dict())
    commit = Commit(proposals=[p_info])
    print(commit)
    print(commit.to_dict())
    print(commit.to_json())

    new_c = Commit.from_dict(commit.to_dict())
    print(new_c)