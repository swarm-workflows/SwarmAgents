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

    def _remove_worse_proposals_from_container(self, incoming_proposal: ProposalInfo,
                                               container: ProposalContainer) -> None:
        """
        Remove all proposals for the same object_id that are worse than incoming_proposal.

        A proposal is "worse" if:
        - It has higher cost, OR
        - It has equal cost but lexicographically larger agent_id

        This prevents accumulation of inferior proposals when messages arrive out of order.
        """
        all_proposals = container.get_proposals_by_object_id(incoming_proposal.object_id)

        for existing in all_proposals:
            if existing.p_id == incoming_proposal.p_id:
                continue  # Don't compare proposal to itself

            # Determine if existing proposal is worse
            is_worse = (existing.cost > incoming_proposal.cost) or \
                       (existing.cost == incoming_proposal.cost and
                        (existing.agent_id or "") > (incoming_proposal.agent_id or ""))

            if is_worse:
                self.host.log_debug(
                    f"Removing worse proposal {existing.p_id} (cost={existing.cost}, "
                    f"agent={existing.agent_id}) in favor of {incoming_proposal.p_id} "
                    f"(cost={incoming_proposal.cost}, agent={incoming_proposal.agent_id})"
                )
                container.remove_proposal(p_id=existing.p_id, object_id=incoming_proposal.object_id)

    # ---------- API exposed to the agent/framework ----------
    def propose(self, proposals: list[ProposalInfo]) -> None:
        # send PROPOSAL to peers
        msg = Proposal(source=self.agent_id,
                       agents=[AgentInfo(agent_id=self.agent_id)],
                       proposals=proposals)
        for proposal in proposals:
            # Proposer implicitly prepares its own proposal
            if self.agent_id not in proposal.prepares:
                proposal.prepares.append(self.agent_id)
            self.outgoing.add_proposal(proposal)
        self.transport.broadcast(payload=msg)
        #for proposal in proposals:
        #    self.outgoing.add_proposal(proposal)

    def on_proposal(self, msg: Proposal) -> None:
        proposals = []
        for proposal in msg.proposals:
            object = self.host.get_object(proposal.object_id)
            if not object or self.host.is_agreement_achieved(object.object_id):
                if not object:
                    self.host.log_info(f"Enqueued proposal {proposal.p_id} for {proposal.object_id} (missing)")
                    self.host.set_pending_proposal(msg, proposal.object_id)
                else:
                    self.host.log_info(f"Skip proposal {proposal.p_id} for {proposal.object_id} (complete)")
                    self.outgoing.remove_object(object_id=proposal.object_id)
                    self.incoming.remove_object(object_id=proposal.object_id)
                continue

            # Basic dominance check using your existing helpers
            my_better = self.outgoing.has_better_proposal(proposal)
            peer_better = self.incoming.has_better_proposal(proposal)

            if my_better:
                # I think my own proposal for this object is better; ignore/forward if topology requires
                self.host.log_info(f"Retaining my better proposal for Object {proposal.object_id}")
                self.conflicts[proposal.object_id] = self.conflicts.get(proposal.object_id, 0) + 1
            elif peer_better:
                # adopt better peer proposal (already handled by containers)
                self.host.log_debug(f"Already accepted better proposal for Object {object.object_id} from peer {peer_better.agent_id} Cost: {peer_better.cost}")
                self.conflicts[object.object_id] = self.conflicts.get(object.object_id, 0) + 1
            else:
                # Incoming proposal is better - remove ALL worse existing proposals
                # FIX: Use helper method to remove ALL worse proposals, not just one arbitrary one
                self._remove_worse_proposals_from_container(proposal, self.outgoing)
                self._remove_worse_proposals_from_container(proposal, self.incoming)

                proposals.append(proposal)
                self.incoming.add_proposal(proposal)
                object.state = ObjectState.PREPARE

        # respond with PREPARE
        if len(proposals):
            prepare = Prepare(source=self.agent_id,
                              agents=[AgentInfo(agent_id=self.agent_id)],
                              proposals=proposals)
            self.host.log_info("Sending prepares")
            self.transport.broadcast(prepare)

        if self.router.should_forward():
            self.transport.broadcast(payload=msg)

    def on_prepare(self, msg: Prepare) -> None:
        proposals = []
        for p in msg.proposals:
            object = self.host.get_object(p.object_id)
            if not object or self.host.is_agreement_achieved(object.object_id):
                if not object:
                    self.host.set_pending_prepare(msg, p.object_id)
                    self.host.log_info(f"Enqueued prepare {p.p_id}/{p.object_id} (missing)")
                else:
                    self.outgoing.remove_object(object_id=p.object_id)
                    self.incoming.remove_object(object_id=p.object_id)
                    self.host.log_debug(f"Skip prepare {p.p_id}/{p.object_id} (complete)")
                continue

            # I have sent this proposal
            if self.outgoing.contains(object_id=p.object_id, p_id=p.p_id):
                proposal = self.outgoing.get_proposal(p_id=p.p_id)
            # Received this proposal
            elif self.incoming.contains(object_id=p.object_id, p_id=p.p_id):
                proposal = self.incoming.get_proposal(p_id=p.p_id)
            # New proposal arriving via PREPARE (before PROPOSAL message)
            else:
                # FIX: Check dominance before blindly adding
                my_better = self.outgoing.has_better_proposal(p)
                peer_better = self.incoming.has_better_proposal(p)

                if my_better:
                    # We have our own better proposal, ignore this PREPARE
                    self.host.log_debug(
                        f"Ignoring PREPARE for {p.p_id} (cost={p.cost}) - "
                        f"retaining my better proposal (cost={my_better.cost})"
                    )
                    self.conflicts[object.object_id] = self.conflicts.get(object.object_id, 0) + 1
                    continue
                elif peer_better:
                    # We already have a better peer proposal, ignore this PREPARE
                    self.host.log_debug(
                        f"Ignoring PREPARE for {p.p_id} (cost={p.cost}) - "
                        f"already have better peer proposal {peer_better.p_id} (cost={peer_better.cost})"
                    )
                    self.conflicts[object.object_id] = self.conflicts.get(object.object_id, 0) + 1
                    continue
                else:
                    # This is the best proposal we've seen so far
                    # Remove any worse proposals before adding
                    self._remove_worse_proposals_from_container(p, self.outgoing)
                    self._remove_worse_proposals_from_container(p, self.incoming)

                    proposal = p
                    self.incoming.add_proposal(proposal=proposal)

                    # FIX: If we accepted a better proposal while in COMMIT phase, reset to PREPARE
                    # This ensures we properly vote for the better proposal
                    if object.is_commit:
                        self.host.log_debug(
                            f"Resetting object {object.object_id} from COMMIT to PREPARE "
                            f"for better proposal {p.p_id} (cost={p.cost})"
                        )
                        object.state = ObjectState.PREPARE

                    self.host.log_debug(f"Accepted new proposal {p.p_id} via PREPARE (cost={p.cost})")

            if msg.agents[0].agent_id not in proposal.prepares:
                proposal.prepares.append(msg.agents[0].agent_id)

            # Commit has already been triggered for THIS proposal
            if object.is_commit and self.incoming.contains(object_id=object.object_id, p_id=proposal.p_id):
                # Already committed to this specific proposal, don't re-commit
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
                if not object:
                    self.host.set_pending_commit(msg, p.object_id)
                    self.host.log_info(f"Enqueued commit {p.p_id}/{p.object_id} (missing)")
                else:
                    self.outgoing.remove_object(object_id=p.object_id)
                    self.incoming.remove_object(object_id=p.object_id)
                    self.host.log_debug(f"Skipped commit {p.p_id}/{p.object_id} (missing)")
                continue

            # I have sent this proposal
            if self.outgoing.contains(object_id=p.object_id, p_id=p.p_id):
                proposal = self.outgoing.get_proposal(p_id=p.p_id)
            # Received this proposal
            elif self.incoming.contains(object_id=p.object_id, p_id=p.p_id):
                proposal = self.incoming.get_proposal(p_id=p.p_id)
            # New proposal arriving via COMMIT (before PROPOSAL/PREPARE messages)
            else:
                # FIX: Check dominance before blindly adding
                my_better = self.outgoing.has_better_proposal(p)
                peer_better = self.incoming.has_better_proposal(p)

                if my_better or peer_better:
                    # We have a better proposal, ignore this COMMIT
                    better = my_better or peer_better
                    self.host.log_debug(
                        f"Ignoring COMMIT for {p.p_id} (cost={p.cost}) - "
                        f"already have better proposal {better.p_id} (cost={better.cost})"
                    )
                    self.conflicts[object.object_id] = self.conflicts.get(object.object_id, 0) + 1
                    continue
                else:
                    # Remove worse proposals before adding
                    self._remove_worse_proposals_from_container(p, self.outgoing)
                    self._remove_worse_proposals_from_container(p, self.incoming)

                    proposal = p
                    self.incoming.add_proposal(proposal=proposal)
                    self.host.log_debug(f"Accepted new proposal {p.p_id} via COMMIT (cost={p.cost})")

            if msg.agents[0].agent_id not in proposal.commits:
                proposal.commits.append(msg.agents[0].agent_id)

            quorum = self.host.calculate_quorum()
            self.host.log_info(f"Is quorum? /{quorum}")
            if len(proposal.commits) >= quorum:
                self.host.log_info("Is quorum!!")
                # leader vs participant path
                if proposal.agent_id == self.agent_id and self.outgoing.contains(object_id=proposal.object_id, p_id=proposal.p_id):
                    # I am leader, do selection
                    object.leader_id = proposal.agent_id
                    self.host.log_info(f"[CON_LEADER] Object:{proposal.object_id} Leader:{self.agent_id} p:{proposal.p_id}")
                    print(f"[CON_LEADER] Object:{proposal.object_id} Leader:{self.agent_id} p:{proposal.p_id} quorum: {len(proposal.commits)}")
                    self.host.on_leader_elected(object, proposal.p_id)
                else:
                    self.host.log_info(f"[CON_PART] Object:{proposal.object_id} Leader:{proposal.agent_id} p:{proposal.p_id}")
                    self.host.on_participant_commit(object, msg.agents[0].agent_id, proposal.p_id)
                self.outgoing.remove_object(object_id=proposal.object_id)
                self.incoming.remove_object(object_id=proposal.object_id)

        if self.router.should_forward():
            self.transport.broadcast(payload=msg)
