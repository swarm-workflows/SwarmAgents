from swarm.comm.messages.message import MessageType
from swarm.comm.messages.proposal import Proposal


class Prepare(Proposal):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._message_type = MessageType.Prepare


if __name__ == '__main__':
    from swarm.models.proposal_info import ProposalInfo
    p_info = ProposalInfo(p_id="pid_1", task_id='t-1', seed=0.6, agent_id="0", commits=1)
    print(p_info)
    print(p_info.to_dict())
    prepare = Prepare(proposals=[p_info])
    print(prepare)
    print(prepare.to_dict())
    print(prepare.to_json())

    new_p = Proposal.from_dict(prepare.to_dict())
    print(new_p)