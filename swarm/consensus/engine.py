# consensus/engine.py
from typing import Dict, Set, Optional
from dataclasses import dataclass, field
from swarm.models.proposal_info import ProposalContainer, ProposalInfo
from swarm.comm.messages.proposal import Proposal
from swarm.comm.messages.prepare import Prepare
from swarm.comm.messages.commit import Commit
from .interfaces import ConsensusHost, ConsensusTransport
from ..models.object import ObjectState


@dataclass
class ProposalState:
    proposal: ProposalInfo
    prepares: Set[int] = field(default_factory=set)
    commits: Set[int]  = field(default_factory=set)

class ConsensusEngine:
    """
    Generic PBFT-like quorum engine.
    - framework-agnostic (no direct Agent dependencies)
    - uses host+transport callbacks for I/O and side effects
    """
    def __init__(self, agent_id: int, host: ConsensusHost, transport: ConsensusTransport):
        self.agent_id = agent_id
        self.host = host
        self.transport = transport

        # Local state
        self.outgoing = ProposalContainer()  # proposals initiated by me
        self.incoming = ProposalContainer()  # proposals initiated by peers
        self._states: Dict[tuple[str, str], ProposalState] = {}  # (job_id, p_id) -> state
        self.conflicts = {}

    # ---------- API exposed to the agent/framework ----------
    def propose(self, object_id: str, p: Proposal) -> None:
        for proposal in p.proposals:
            key = (object_id, proposal.p_id)
            self._states[key] = ProposalState(proposal=proposal)
            self.outgoing.add_proposal(proposal)
        # send PROPOSAL to peers
        self.transport.broadcast(p, include_self=False)

    def on_proposal(self, incoming: Proposal) -> None:
        proposals = []
        for proposal in incoming.proposals:
            object = self.host.get_object(proposal.object_id)
            if not object or self.host.has_object_action_completed(object.object_id):
                self.host.log_debug(f"Skip proposal {proposal.p_id} (missing or complete)")
                continue

            key = (object.object_id, proposal.p_id)
            state = self._states.get(key)
            if not state:
                state = self._states[key] = ProposalState(proposal=proposal)
                self.incoming.add_proposal(proposal)

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

                if incoming.agents[0].agent_id not in proposal.prepares:
                    proposal.prepares.append(incoming.agents[0].agent_id)
                self.incoming.add_proposal(proposal=proposal)
                object.state = ObjectState.PREPARE

            # respond with PREPARE
            prepare = Prepare(source=self.agent_id, agents=[incoming.agents[0]], proposals=[incoming])
            self.transport.broadcast(prepare, include_self=False)

    def on_prepare(self, msg: Prepare) -> None:
        for p in msg.proposals:
            key = (p.object_id, p.p_id)
            state = self._states.get(key)
            if not state:
                state = self._states[key] = ProposalState(proposal=p)
                self.incoming.add_proposal(p)

            # mark prepare
            sender = msg.agents[0].agent_id
            if sender not in state.prepares:
                state.prepares.add(sender)

            # forward if needed (ring/star), omitted here

            # when enough prepares, issue COMMIT
            if len(state.prepares) >= self.host.calculate_quorum():
                commit = Commit(source=self.agent_id, agents=msg.agents, proposals=[p])
                self.transport.broadcast(commit, include_self=False)

    def on_commit(self, msg: Commit) -> None:
        for p in msg.proposals:
            key = (p.object_id, p.p_id)
            state = self._states.get(key)
            if not state:
                state = self._states[key] = ProposalState(proposal=p)
                self.incoming.add_proposal(p)

            sender = msg.agents[0].agent_id
            if sender not in state.commits:
                state.commits.add(sender)

            quorum = self.host.calculate_quorum()
            if len(state.commits) >= quorum:
                # leader vs participant path
                job = self.host.get_object(p.object_id)
                if not job:
                    continue
                if p.agent_id == self.agent_id and self.outgoing.contains(object_id=p.object_id, p_id=p.p_id):
                    # I am leader, do selection
                    self.host.log_info(f"[CON_LEADER] Job:{p.object_id} Leader:{self.agent_id} p:{p.p_id}")
                    self.host.on_leader_elected(job, p.p_id)
                    self.outgoing.remove_object(object_id=p.object_id)
                else:
                    self.host.log_info(f"[CON_PART] Job:{p.object_id} Leader:{p.agent_id} p:{p.p_id}")
                    self.host.on_participant_commit(job, p.agent_id, p.p_id)
                    self.incoming.remove_object(object_id=p.object_id)
