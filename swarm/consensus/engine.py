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
import time
from collections import OrderedDict

from swarm.consensus.messages.proposal_info import ProposalContainer, ProposalInfo
from swarm.consensus.messages.proposal import Proposal
from swarm.consensus.messages.prepare import Prepare
from swarm.consensus.messages.commit import Commit
from .interfaces import ConsensusHost, ConsensusTransport, TopologyRouter
from ..models.agent_info import AgentInfo
from ..models.object import ObjectState
from ..utils.instrumentation import RunningStats
from ..utils.tiebreak import dominates


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
        # conflicts is diagnostic (per-job conflict counts, read by plotting); cap it so
        # long runs with high job churn can't grow it without bound.
        self._conflicts_max = 4096

        # Finalization instrumentation (P0-4), measured at the PROPOSER only — the same
        # vantage point the Snow engine measures from, so the two protocols' finalize-time
        # distributions are comparable. A participant's clock would start when it first
        # heard of the object, which is a different and shorter quantity.
        #
        # PBFT has no round count: its phase structure is fixed at proposal->prepare->commit,
        # so what varies is how many votes a decision had to collect and how long that took.
        # `votes_` is the analogue of Snow's `rounds_`; the mechanism figure pairs it with
        # the per-agent message counts the transport records.
        # (object_id, p_id) pairs this agent has already broadcast a COMMIT for. The old
        # guard asked `self.incoming.contains(...)`, which is false for the proposer's OWN
        # proposal (it lives in `outgoing`), so a proposer re-entered the commit branch on
        # every PREPARE that arrived after quorum and broadcast another COMMIT each time —
        # up to (n - quorum) redundant COMMITs per job, biasing messages-per-job *against*
        # PBFT, which is the direction that flatters this paper's argument. Keyed by p_id
        # rather than by object so that adopting a better proposal (which resets the object
        # to PREPARE) still commits, and so does a re-proposal after a reselection timeout.
        self._commits_sent: set[tuple[str, str]] = set()
        # (object_id, p_id) pairs this agent has already FINALIZED, kept after the object
        # leaves the containers. Finalization used to erase every trace of a decision at once
        # (`_forget_object`), and that is exactly when the stragglers arrive: every tier wider
        # than its quorum produces (n - quorum) late PREPAREs and COMMITs per job. A late
        # PREPARE then found the object with empty containers and no dedupe entry, re-adopted
        # the proposal with the SENDER's wire vote list and broadcast a second COMMIT; a late
        # COMMIT re-adopted it with a wire `commits` list already at quorum and finalized the
        # decision a second time — `finalized_count` and `votes_` doubled, and the proposer ran
        # the participant branch with the straggler recorded as leader. Both bias the
        # PBFT-versus-Snow figures against PBFT. Bounded by count, not by objects in flight,
        # because the whole point is to remember decisions the containers have forgotten; the
        # host tells us when an object is genuinely up for election again (`forget_decision`),
        # and a new election carries a new p_id anyway.
        self._finalized: "OrderedDict[tuple[str, str], float]" = OrderedDict()
        self._finalized_max = 8192
        self._proposed_at: dict[str, float] = {}
        self._proposed_at_max = 8192
        self.time_to_finalize = RunningStats()
        self.votes_to_finalize = RunningStats()
        self.finalized_count = 0
        self.reproposals = 0

    def _mark_proposed(self, object_id: str, now: float) -> None:
        """Stamp the start of a proposal attempt for *object_id*.

        A re-proposal after a reselection timeout restarts the clock rather than extending
        the first attempt's: the quantity the figure needs is how long the attempt that won
        took, and carrying a timed-out attempt into it would report the reselection timeout
        as consensus latency. The re-proposals are counted separately so the churn is not
        lost — that is what `jobs_restarted` in the collector is cross-checked against.
        """
        if object_id in self._proposed_at:
            self.reproposals += 1
        elif len(self._proposed_at) >= self._proposed_at_max:
            self._proposed_at.pop(next(iter(self._proposed_at)), None)
        self._proposed_at[object_id] = now

    def _record_finalize(self, proposal: ProposalInfo) -> None:
        """Record one finalization, at the proposer only."""
        if proposal.agent_id != self.agent_id:
            return
        started = self._proposed_at.pop(proposal.object_id, None)
        self.finalized_count += 1
        self.votes_to_finalize.add(len(proposal.commits))
        if started is not None:
            self.time_to_finalize.add(time.time() - started)

    def consensus_stats(self) -> dict:
        """Per-agent finalization accounting, in the same shape the Snow engine emits.

        `rounds_*` is absent by design rather than faked as a constant 3: PBFT's phase count
        is fixed, so a column of 3s would invite a comparison of round counts across
        protocols that means nothing. Compare `finalize_s_*` and the transport's message
        counts instead.

        `abandoned` is absent for the same reason, and used to be a hard-coded 0 (code review
        §11). PBFT does not abandon: a stuck object is left to the reselection timeout, so
        there is no count to report. Zero is the answer to "how many were abandoned", and PBFT
        cannot answer it — absent, not zero, or a cross-protocol `abandoned` column reads as
        though PBFT abandoned nothing where Snow abandoned some, which is a comparison of a
        measurement against a constant. `finalize_lost` is absent here too; it is the Snow CAS
        path and PBFT has no CAS.
        """
        stats: dict = {
            "protocol": "pbft",
            "finalized": self.finalized_count,
            # PBFT counts at the winning proposer only, so every finalize it counts is a win.
            "won": self.finalized_count,
            "reproposals": self.reproposals,
            "conflict_rounds": sum(self.conflicts.values()),
            "conflict_objects": len(self.conflicts),
        }
        stats.update(self.votes_to_finalize.summary("votes_"))
        stats.update(self.time_to_finalize.summary("finalize_s_"))
        return stats

    def _bump_conflict(self, object_id: str) -> None:
        if object_id not in self.conflicts and len(self.conflicts) >= self._conflicts_max:
            # evict oldest entry (dict preserves insertion order)
            self.conflicts.pop(next(iter(self.conflicts)), None)
        self.conflicts[object_id] = self.conflicts.get(object_id, 0) + 1

    def _set_pending_safe(self, kind: str, msg, object_id: str) -> None:
        """Hand an out-of-order message to the host for later replay. Hosts that don't
        implement the set_pending_* hooks must not abort the batch — before this guard a
        missing hook raised AttributeError mid-loop, silently dropping every remaining
        proposal in the message (lost quorum votes -> re-proposal storms)."""
        fn = getattr(self.host, f"set_pending_{kind}", None)
        if fn is None:
            self.host.log_warn(
                f"host lacks set_pending_{kind}; dropping out-of-order {kind} for {object_id}")
            return
        try:
            fn(msg, object_id)
        except Exception as exc:
            self.host.log_warn(f"set_pending_{kind}({object_id}) failed: {exc}")

    def _remove_worse_proposals_from_container(self, incoming_proposal: ProposalInfo,
                                               container: ProposalContainer) -> None:
        """
        Remove all proposals for the same object_id that are worse than incoming_proposal.

        A proposal is "worse" if:
        - It has higher cost, OR
        - It has equal cost but a higher tiebreak_rank for this object

        This prevents accumulation of inferior proposals when messages arrive out of order.

        The equal-cost arm used to compare agent ids lexicographically, which hands every tie
        to the lowest id. That is only harmless if ties are rare; they are not (finding 10),
        so it is now a per-object rank that no agent wins systematically.
        """
        all_proposals = container.get_proposals_by_object_id(incoming_proposal.object_id)

        for existing in all_proposals:
            if existing.p_id == incoming_proposal.p_id:
                continue  # Don't compare proposal to itself

            # Determine if existing proposal is worse
            is_worse = dominates(incoming_proposal.object_id,
                                 incoming_proposal.cost, incoming_proposal.agent_id,
                                 existing.cost, existing.agent_id)

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
        now = time.time()
        for proposal in proposals:
            # Proposer implicitly prepares its own proposal
            self._add_vote(proposal.prepares)
            self.outgoing.add_proposal(proposal)
            self._mark_proposed(proposal.object_id, now)
        self.transport.broadcast(payload=msg)
        # A live set of one has quorum 1, and the proposer's own PREPARE already meets it.
        self._commit_where_prepared(proposals)

    def on_proposal(self, msg: Proposal) -> None:
        proposals = []
        for proposal in msg.proposals:
            object = self.host.get_object(proposal.object_id)
            if not object or self.host.is_agreement_achieved(object.object_id):
                if not object:
                    self.host.log_debug(f"Enqueued proposal {proposal.p_id} for {proposal.object_id} (missing)")
                    self._set_pending_safe("proposal", msg, proposal.object_id)
                else:
                    self.host.log_debug(f"Skip proposal {proposal.p_id} for {proposal.object_id} (complete)")
                    self.outgoing.remove_object(object_id=proposal.object_id)
                    self.incoming.remove_object(object_id=proposal.object_id)
                continue

            if self._already_finalized(proposal.object_id, proposal.p_id):
                # A straggler (or a ring/star forward) of a proposal this agent has already
                # finalized. Adopting it would re-run the decision from its wire vote lists.
                self.host.log_debug(f"Skip proposal {proposal.p_id} for {proposal.object_id} (finalized)")
                continue

            # Basic dominance check using your existing helpers
            my_better = self.outgoing.has_better_proposal(proposal)
            peer_better = self.incoming.has_better_proposal(proposal)

            if my_better:
                # I think my own proposal for this object is better; ignore/forward if topology requires
                self.host.log_debug(f"Retaining my better proposal for Object {object.object_id}")
                self._bump_conflict(object.object_id)
            elif peer_better:
                # adopt better peer proposal (already handled by containers)
                self.host.log_debug(f"Already accepted better proposal for Object {object.object_id} from peer {peer_better.agent_id} Cost: {peer_better.cost}")
                self._bump_conflict(object.object_id)
            else:
                # Incoming proposal is better - remove ALL worse existing proposals
                # FIX: Use helper method to remove ALL worse proposals, not just one arbitrary one
                self._remove_worse_proposals_from_container(proposal, self.outgoing)
                self._remove_worse_proposals_from_container(proposal, self.incoming)

                # Our own PREPARE counts toward the quorum we compute over a live set that
                # includes us (code review 2026-10-05 §5).
                self._add_vote(proposal.prepares)
                proposals.append(proposal)
                self.incoming.add_proposal(proposal)
                object.state = ObjectState.PREPARE

        # respond with PREPARE
        if len(proposals):
            prepare = Prepare(source=self.agent_id,
                              agents=[AgentInfo(agent_id=self.agent_id)],
                              proposals=proposals)
            self.host.log_debug("Sending prepares")
            self.transport.broadcast(prepare)
            # Our vote may be the one that completes the prepare quorum.
            self._commit_where_prepared(proposals)

        if self.router.should_forward():
            self.transport.broadcast(payload=msg)

    def on_prepare(self, msg: Prepare) -> None:
        proposals = []
        for p in msg.proposals:
            object = self.host.get_object(p.object_id)
            if not object or self.host.is_agreement_achieved(object.object_id):
                if not object:
                    self._set_pending_safe("prepare", msg, p.object_id)
                    self.host.log_debug(f"Enqueued prepare {p.p_id}/{p.object_id} (missing)")
                else:
                    self.outgoing.remove_object(object_id=p.object_id)
                    self.incoming.remove_object(object_id=p.object_id)
                    self.host.log_debug(f"Skip prepare {p.p_id}/{p.object_id} (complete)")
                continue

            if self._already_finalized(p.object_id, p.p_id):
                # Straggler PREPARE after our finalization. Before this check it re-adopted the
                # proposal below with the sender's wire `prepares` (>= quorum), found no
                # `_commits_sent` entry (forgotten at finalize) and broadcast a second COMMIT —
                # the 2026-09-15 COMMIT-inflation defect, reintroduced by the cleanup that
                # fixed it. Pinned by tests/test_pbft_stragglers.py.
                self.host.log_debug(f"Skip prepare {p.p_id}/{p.object_id} (finalized)")
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
                    self._bump_conflict(object.object_id)
                    continue
                elif peer_better:
                    # We already have a better peer proposal, ignore this PREPARE
                    self.host.log_debug(
                        f"Ignoring PREPARE for {p.p_id} (cost={p.cost}) - "
                        f"already have better peer proposal {peer_better.p_id} (cost={peer_better.cost})"
                    )
                    self._bump_conflict(object.object_id)
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

            # Commit has already been broadcast for THIS proposal, by us. Asked of a set we
            # own rather than of a container, so it holds for a proposal we proposed
            # ourselves as well as one we adopted from a peer.
            if (object.object_id, proposal.p_id) in self._commits_sent:
                continue

            object.state = ObjectState.PREPARE
            proposals.append(proposal)

        self._commit_where_prepared(proposals)

        if self.router.should_forward():
            self.transport.broadcast(payload=msg)

    def _forget_object(self, object_id: str) -> None:
        """Drop per-object commit bookkeeping once the object leaves the engine.

        Bounded by objects in flight, not by objects ever seen: without this the set would
        grow for the life of the agent, which is the same slow leak `_proposed_at` caps."""
        stale = [k for k in self._commits_sent if k[0] == object_id]
        for k in stale:
            self._commits_sent.discard(k)

    def _note_finalized(self, object_id: str, p_id: str, now: float) -> None:
        key = (object_id, p_id)
        self._finalized[key] = now
        self._finalized.move_to_end(key)
        while len(self._finalized) > self._finalized_max:
            self._finalized.popitem(last=False)

    def _already_finalized(self, object_id: str, p_id: str) -> bool:
        return (object_id, p_id) in self._finalized

    def forget_decision(self, object_id: str) -> None:
        """The host says *object_id* is up for election again (a reassignment after its
        assignee died, a reselection reset). Drop the memory of its PAST decisions.

        Only `_finalized` is touched. `_commits_sent` is keyed by (object, p_id) and a new
        election carries a new p_id, so the old entries are inert and are released at the next
        finalize anyway. Clearing it here was a defect: the host calls this from its periodic
        pending-job scan, which also sees every job whose election is merely IN FLIGHT (the
        record stays PENDING in Redis until the leader persists READY), so the dedupe was
        being erased mid-election every 0.5 s and the next PREPARE past quorum broadcast a
        fresh COMMIT — the very duplication this file exists to prevent."""
        for key in [k for k in self._finalized if k[0] == object_id]:
            self._finalized.pop(key, None)

    # ---------- vote counting ----------
    #
    # A vote is counted when it is CAST, by whoever casts it: a peer's arrives in a message and
    # is appended by the receiver; our own is appended here when we send it. Before 2026-10-06
    # only arriving votes were appended, and `broadcast` never delivers to self, while
    # `calculate_quorum` is a majority of `neighbor_map`, which includes self — so an agent
    # needed q votes from its n-1 peers instead of q-1. A 2-agent group never finalized, and a
    # 3-agent group with one peer down stalled for good (code review 2026-10-05 §5).
    def _add_vote(self, votes: list, voter=None) -> None:
        voter = self.agent_id if voter is None else voter
        if voter not in votes:
            votes.append(voter)

    def _commit_where_prepared(self, proposals) -> None:
        """Broadcast ONE COMMIT for every proposal whose prepare quorum is met and that we have
        not committed to yet, count our own COMMIT, and finalize any whose commit quorum that
        vote completes. The single place a COMMIT is sent, so the one-COMMIT-per-proposal rule
        (`_commits_sent`, 2026-09-15) cannot be bypassed by a new caller."""
        ready = []
        quorum = self.host.calculate_quorum()
        for proposal in proposals:
            if (proposal.object_id, proposal.p_id) in self._commits_sent:
                continue
            if len(proposal.prepares) < quorum:
                continue
            object = self.host.get_object(proposal.object_id)
            if object is None:
                continue
            self.host.log_debug(f"Object: {proposal.object_id} Agent: {self.agent_id} received "
                                f"quorum prepares: {proposal.prepares}, starting commit!")
            object.state = ObjectState.COMMIT
            ready.append((object, proposal))
        if not ready:
            return
        for _obj, proposal in ready:
            self._commits_sent.add((proposal.object_id, proposal.p_id))
            self._add_vote(proposal.commits)
        commit = Commit(source=self.agent_id, agents=[AgentInfo(agent_id=self.agent_id)],
                        proposals=[p for _o, p in ready])
        self.transport.broadcast(payload=commit)
        for object, proposal in ready:
            self._finalize_if_quorum(object, proposal)

    def _finalize_if_quorum(self, object, proposal) -> bool:
        if self._already_finalized(proposal.object_id, proposal.p_id):
            return False
        quorum = self.host.calculate_quorum()
        self.host.log_debug(f"Is quorum? /{quorum}")
        if len(proposal.commits) < quorum:
            return False
        self.host.log_debug("Is quorum!!")
        # Remembered BEFORE the containers are cleared, so the stragglers that follow
        # are recognised as such rather than re-adopted.
        self._note_finalized(proposal.object_id, proposal.p_id, time.time())
        self._record_finalize(proposal)
        # Leader vs participant. The leader is whoever PROPOSED — agent ids are unique, so a
        # proposal naming us is ours whichever container it now sits in. This used to also
        # require `outgoing.contains(...)`, and two paths remove our own proposal from
        # `outgoing` while its election is still live (`_restart_selection`,
        # `_clear_consensus_for_failed_agent`); late votes then finalized it on the
        # participant branch with leader = self, so `select_job` never ran while every peer
        # recorded us as the assignee (code review 2026-10-05 §6).
        if proposal.agent_id == self.agent_id:
            object.leader_id = proposal.agent_id
            self.host.log_info(f"[CON_LEADER] Object:{proposal.object_id} Leader:{self.agent_id} p:{proposal.p_id}")
            self.host.on_leader_elected(object, proposal.p_id)
        else:
            self.host.log_info(f"[CON_PART] Object:{proposal.object_id} Leader:{proposal.agent_id} p:{proposal.p_id}")
            # The leader is the PROPOSER, not whoever's COMMIT happened to reach quorum
            # last. `msg.agents[0]` was passed here, so every participant recorded the
            # sender of the final COMMIT as the job's assignee in `job_assignments` —
            # a different peer per participant, and the wrong one for all of them.
            self.host.on_participant_commit(object, proposal.agent_id, proposal.p_id)
        self.outgoing.remove_object(object_id=proposal.object_id)
        self.incoming.remove_object(object_id=proposal.object_id)
        self._forget_object(proposal.object_id)
        return True

    def on_commit(self, msg: Commit) -> None:
        for p in msg.proposals:
            object = self.host.get_object(p.object_id)
            if not object or self.host.is_agreement_achieved(object.object_id):
                if not object:
                    self._set_pending_safe("commit", msg, p.object_id)
                    self.host.log_debug(f"Enqueued commit {p.p_id}/{p.object_id} (missing)")
                else:
                    self.outgoing.remove_object(object_id=p.object_id)
                    self.incoming.remove_object(object_id=p.object_id)
                    self._forget_object(p.object_id)
                    self.host.log_debug(f"Skipped commit {p.p_id}/{p.object_id} (missing)")
                continue

            if self._already_finalized(p.object_id, p.p_id):
                # Straggler COMMIT after our finalization. It carries the sender's wire
                # `commits` list, already at quorum, so adopting it finalized the decision a
                # second time: `finalized_count` and `votes_` doubled, and the proposer took
                # the participant branch with the straggler recorded as leader.
                self.host.log_debug(f"Skip commit {p.p_id}/{p.object_id} (finalized)")
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
                    self._bump_conflict(object.object_id)
                    continue
                else:
                    # Remove worse proposals before adding
                    self._remove_worse_proposals_from_container(p, self.outgoing)
                    self._remove_worse_proposals_from_container(p, self.incoming)

                    proposal = p
                    self.incoming.add_proposal(proposal=proposal)
                    self.host.log_debug(f"Accepted new proposal {p.p_id} via COMMIT (cost={p.cost})")

            self._add_vote(proposal.commits, msg.agents[0].agent_id)
            self._finalize_if_quorum(object, proposal)

        if self.router.should_forward():
            self.transport.broadcast(payload=msg)
