# consensus/engine.py
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
from swarm.consensus.messages.proposal_info import ProposalContainer, ProposalInfo
from swarm.consensus.messages.proposal import Proposal
from swarm.consensus.messages.prepare import Prepare
from swarm.consensus.messages.commit import Commit
from .interfaces import ConsensusHost, ConsensusTransport, TopologyRouter
from ..models.agent_info import AgentInfo
from ..models.object import ObjectState


class ConsensusEngine:
    """
    Generic PBFT-like quorum engine.
    - framework-agnostic (no direct Agent dependencies)
    - uses host+transport callbacks for I/O and side effects
    """
    def __init__(self, agent_id: int, host: ConsensusHost, transport: ConsensusTransport,
                 router: TopologyRouter):
        self.agent_id = agent_id
        self.host = host
        self.transport = transport
        self.router = router

        # Local state
        self.outgoing = ProposalContainer()  # proposals initiated by me
        self.incoming = ProposalContainer()  # proposals initiated by peers
        self.conflicts = {}

    # ---------- API exposed to the agent/framework ----------
    def propose(self, proposals: list[ProposalInfo]) -> None:
        # send PROPOSAL to peers
        msg = Proposal(source=self.agent_id,
                       agents=[AgentInfo(agent_id=self.agent_id)],
                       proposals=proposals)
        self.transport.broadcast(payload=msg)
        for proposal in proposals:
            self.outgoing.add_proposal(proposal)

    def on_proposal(self, msg: Proposal) -> None:
        proposals = []
        for proposal in msg.proposals:
            object = self.host.get_object(proposal.object_id)
            if not object or self.host.is_agreement_achieved(object.object_id):
                self.host.log_debug(f"Skip proposal {proposal.p_id} for {proposal.object_id} (missing or complete)")
                self.outgoing.remove_object(object_id=proposal.object_id)
                self.incoming.remove_object(object_id=proposal.object_id)
                continue

            # Basic dominance check using your existing helpers
            my_better = self.outgoing.has_better_proposal(proposal)
            peer_better = self.incoming.has_better_proposal(proposal)

            if my_better:
                # I think my own proposal for this object is better; ignore/forward if topology requires
                self.host.log_debug(f"Retaining my better proposal for Object {object.object_id}")
                self.conflicts[object.object_id] = self.conflicts.get(object.object_id, 0) + 1
            elif peer_better:
                # adopt better peer proposal (already handled by containers)
                self.host.log_debug(f"Already accepted better proposal for Object {object.object_id} from peer {peer_better.agent_id} Cost: {peer_better.seed}")
                self.conflicts[object.object_id] = self.conflicts.get(object.object_id, 0) + 1
            else:
                if my_better:
                    self.host.log_debug(f"Removed my Proposal: {my_better} in favor of incoming proposal {proposal}")
                    self.outgoing.remove_proposal(p_id=my_better.p_id, object_id=object.object_id)
                if peer_better:
                    self.host.log_debug(f"Removed peer Proposal: {peer_better} in favor of incoming proposal {proposal}")
                    self.incoming.remove_proposal(p_id=peer_better.p_id, object_id=object.object_id)

                proposals.append(proposal)
                if msg.agents[0].agent_id not in proposal.prepares:
                    proposal.prepares.append(msg.agents[0].agent_id)
                self.incoming.add_proposal(proposal)
                object.state = ObjectState.PREPARE
            #if not self.incoming.contains(p_id=proposal.p_id, object_id=object.object_id):
            #    self.incoming.add_proposal(proposal)

        # respond with PREPARE
        if len(proposals):
            prepare = Prepare(source=self.agent_id,
                              agents=[AgentInfo(agent_id=self.agent_id)],
                              proposals=proposals)
            self.transport.broadcast(prepare)

        if self.router.should_forward():
            self.transport.broadcast(payload=msg)

    def on_prepare(self, msg: Prepare) -> None:
        proposals = []
        for p in msg.proposals:
            object = self.host.get_object(p.object_id)
            if not object or self.host.is_agreement_achieved(object.object_id):
                self.host.log_debug(f"Skip proposal {p.p_id}/{p.object_id} (missing or complete)")
                self.outgoing.remove_object(object_id=p.object_id)
                self.incoming.remove_object(object_id=p.object_id)
                continue

            # I have sent this proposal
            if self.outgoing.contains(object_id=p.object_id, p_id=p.p_id):
                proposal = self.outgoing.get_proposal(p_id=p.p_id)
            # Received this proposal
            elif self.incoming.contains(object_id=p.object_id, p_id=p.p_id):
                proposal = self.incoming.get_proposal(p_id=p.p_id)
            # New proposal
            else:
                proposal = p
                self.incoming.add_proposal(proposal=proposal)

            #if not self.incoming.contains(p_id=proposal.p_id, object_id=object.object_id):
            #    self.incoming.add_proposal(proposal)

            if msg.agents[0].agent_id not in proposal.prepares:
                proposal.prepares.append(msg.agents[0].agent_id)

            # Commit has already been triggered
            if object.is_commit:
                continue

            quorum_count = self.host.calculate_quorum()
            object.state = ObjectState.PREPARE

            if len(proposal.prepares) >= quorum_count:
                self.host.log_debug(f"Object: {p.object_id} Agent: {self.agent_id} received quorum "
                                  f"prepares: {p.prepares}, starting commit!")

                # Increment the number of commits to count the commit being sent
                proposals.append(proposal)
                object.state = ObjectState.COMMIT

        if len(proposals):
            commit = Commit(source=self.agent_id, agents=[AgentInfo(agent_id=self.agent_id)], proposals=proposals)
            self.transport.broadcast(payload=commit)

        if self.router.should_forward():
            self.transport.broadcast(payload=msg)

    def on_commit(self, msg: Commit) -> None:
        for p in msg.proposals:
            object = self.host.get_object(p.object_id)
            if not object or self.host.is_agreement_achieved(object.object_id):
                self.host.log_debug(f"Skip proposal {p.p_id}/{p.object_id} (missing or complete)")
                self.outgoing.remove_object(object_id=p.object_id)
                self.incoming.remove_object(object_id=p.object_id)
                continue

            # I have sent this proposal
            if self.outgoing.contains(object_id=p.object_id, p_id=p.p_id):
                proposal = self.outgoing.get_proposal(p_id=p.p_id)
            # Received this proposal
            elif self.incoming.contains(object_id=p.object_id, p_id=p.p_id):
                proposal = self.incoming.get_proposal(p_id=p.p_id)
            # New proposal
            else:
                proposal = p
                self.incoming.add_proposal(proposal=proposal)
            #if not self.incoming.contains(p_id=proposal.p_id, object_id=object.object_id):
            #    self.incoming.add_proposal(proposal)

            if msg.agents[0].agent_id not in proposal.commits:
                proposal.commits.append(msg.agents[0].agent_id)

            quorum = self.host.calculate_quorum()
            if len(proposal.commits) >= quorum:
                # leader vs participant path
                if proposal.agent_id == self.agent_id and self.outgoing.contains(object_id=proposal.object_id, p_id=proposal.p_id):
                    # I am leader, do selection
                    object.leader_id = proposal.agent_id
                    self.host.log_info(f"[CON_LEADER] Object:{proposal.object_id} Leader:{self.agent_id} p:{proposal.p_id}")
                    print(f"[CON_LEADER] Object:{proposal.object_id} Leader:{self.agent_id} p:{proposal.p_id} quotum: {len(proposal.commits)}")
                    self.host.on_leader_elected(object, proposal.p_id)
                else:
                    self.host.log_info(f"[CON_PART] Object:{proposal.object_id} Leader:{proposal.agent_id} p:{proposal.p_id}")
                    self.host.on_participant_commit(object, msg.agents[0].agent_id, proposal.p_id)
                self.outgoing.remove_object(object_id=proposal.object_id)
                self.incoming.remove_object(object_id=proposal.object_id)

        if self.router.should_forward():
            self.transport.broadcast(payload=msg)
