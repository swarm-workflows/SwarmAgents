# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Author: Komal Thareja(kthare10@renci.org)
"""
Snow/Avalanche-style consensus engine for job assignment.

Designed to be drop-in compatible with ``ConsensusEngine`` from the
``ResourceAgent`` perspective: it exposes ``propose()`` taking a list of
``ProposalInfo`` and emits the same ``on_leader_elected`` /
``on_participant_commit`` callbacks once a decision is finalized. Internally
it replaces the PBFT 3-phase broadcast with repeated k-peer sampling and
Redis SET-NX finalization (exactly-once safety net).

See ``docs/GOSSIP_CONSENSUS_DESIGN.md`` for the protocol description.
"""

from __future__ import annotations

import random
import threading
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Protocol

from swarm.consensus.interfaces import ConsensusHost, TopologyRouter
from swarm.consensus.messages.proposal_info import ProposalContainer, ProposalInfo
from swarm.consensus.messages.snow_query import SnowQuery
from swarm.consensus.messages.snow_response import SnowResponse


class SnowTransport(Protocol):
    """Subset of agent transport needed by the Snow engine."""

    def send(self, dest: int, payload: object) -> None: ...
    def broadcast(self, payload: object) -> None: ...


class SnowHost(ConsensusHost, Protocol):
    """``ConsensusHost`` plus Snow-specific cost/membership accessors."""

    agent_id: int

    def live_peer_ids(self) -> List[int]: ...
    def my_cost_for_job(self, object_id: str) -> Optional[float]: ...
    def try_claim_assignment(self, object_id: str, agent_id: int) -> int: ...
    def get_assignment(self, object_id: str) -> Optional[int]: ...
    # Optional locality accessors for topology-aware sampling. Implementations that
    # don't provide site info can omit these (the engine falls back to uniform sampling).
    def my_site(self) -> Optional[str]: ...
    def peer_site(self, peer_id: int) -> Optional[str]: ...


@dataclass
class _SnowState:
    proposal: ProposalInfo
    preferred: int            # agent_id currently believed to win
    preferred_cost: float
    confidence: int = 0
    round_no: int = 0
    pending_query_id: Optional[str] = None
    pending_responses: List[SnowResponse] = field(default_factory=list)
    round_deadline: float = 0.0
    finalized: bool = False
    last_round_at: float = 0.0
    queried: int = 0          # peers actually queried in the current round
    started_at: float = 0.0   # when this proposal entered Snow (for latency instrumentation)


class GossipConsensusEngine:
    """
    Snow consensus driver.

    Concurrency model: ``propose()`` enqueues per-job state and a single daemon
    driver thread pumps rounds (sends queries, evaluates responses, finalizes).
    ``on_snow_query()`` and ``on_snow_response()`` are called from the agent's
    inbound thread and only touch ``self._states`` under ``self._lock``.

    Drop-in parity with ``ConsensusEngine``:
    - ``self.outgoing`` / ``self.incoming`` are exposed as ``ProposalContainer``
      instances so existing housekeeping in ``ResourceAgent`` (which clears them
      on job completion / restart) keeps working.
    """

    def __init__(
        self,
        agent_id: int,
        host: SnowHost,
        transport: SnowTransport,
        router: TopologyRouter,
        k: int = 20,
        alpha: float = 0.7,
        beta: int = 20,
        max_rounds: int = 100,
        round_timeout_s: float = 0.5,
        tick_interval_s: float = 0.05,
        local_sample_frac: float = 1.0,
        time_fn: Callable[[], float] = time.time,
    ):
        self.agent_id = int(agent_id)
        self.host = host
        self.transport = transport
        self.router = router
        self.k = int(k)
        self.alpha = float(alpha)
        self.alpha_k = max(1, int(round(alpha * k)))
        self.beta = int(beta)
        self.max_rounds = int(max_rounds)
        self.round_timeout_s = float(round_timeout_s)
        self.tick_interval_s = float(tick_interval_s)
        # Fraction of each k-sample drawn from same-site peers (1.0 = uniform).
        self.local_sample_frac = min(1.0, max(0.0, float(local_sample_frac)))
        self._time = time_fn

        # ProposalContainer parity with PBFT engine — agent code reads/clears these.
        self.outgoing = ProposalContainer()
        self.incoming = ProposalContainer()
        self.conflicts: Dict[str, int] = {}

        self._lock = threading.RLock()
        self._states: Dict[str, _SnowState] = {}
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _alpha_threshold(self, sample_size: int) -> int:
        """Supermajority vote count required, relative to the peers actually sampled
        this round. Basing this on the live sample (not the static k) lets small
        groups — where fewer than k peers exist — still reach a supermajority and
        commit, instead of never clearing a k-based threshold."""
        eff = min(int(self.k), int(sample_size)) if sample_size else int(self.k)
        return max(1, int(round(self.alpha * eff)))

    # ---- Lifecycle ------------------------------------------------------- #

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name=f"snow-{self.agent_id}", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=max(0.2, self.tick_interval_s * 4))
            self._thread = None

    # ---- PBFT-engine-compatible API ------------------------------------- #

    def propose(self, proposals: List[ProposalInfo]) -> None:
        """
        Begin Snow rounds for each proposal. The submitter's own proposal is
        registered as both ``outgoing`` (for housekeeping parity with PBFT)
        and seeds the initial Snow state with ``preferred=self``.
        """
        now = self._time()
        with self._lock:
            for p in proposals:
                if p.object_id in self._states:
                    continue  # already in flight
                self.outgoing.add_proposal(p)
                self._states[p.object_id] = _SnowState(
                    proposal=p,
                    preferred=self.agent_id,
                    preferred_cost=float(p.cost or 0.0),
                    round_deadline=now,  # send first query on next tick
                    started_at=now,
                )

    # PBFT-message handlers — Snow doesn't use them but keep no-op shims so
    # ResourceAgent's existing dispatch path doesn't crash if a stray PBFT
    # message arrives during a protocol switch / config rollout.
    def on_proposal(self, msg) -> None: pass
    def on_prepare(self, msg) -> None: pass
    def on_commit(self, msg) -> None: pass

    # ---- Snow-message handlers (called from inbound thread) ------------- #

    def on_snow_query(self, msg: SnowQuery) -> None:
        """Peer-side: respond with our preferred assignee for the queried job."""
        if msg.source == self.agent_id or msg.job_id is None:
            return
        winner = self.host.get_assignment(msg.job_id)
        if winner is not None:
            # Already committed — short-circuit with the decided value.
            resp = SnowResponse(
                source=self.agent_id,
                query_id=msg.query_id,
                job_id=msg.job_id,
                preferred_agent=winner,
                cost=0.0,
                already_decided=True,
            )
            self._safe_send(int(msg.source), resp)
            return

        my_cost = self.host.my_cost_for_job(msg.job_id)
        # Dominance rule: if I'm cheaper than the initiator's preferred, vote
        # for myself; otherwise yield to the initiator's candidate. Ties broken
        # by lexicographic agent_id to match the PBFT engine's tiebreak.
        preferred = self.agent_id
        if my_cost is None:
            # Can't evaluate locally — yield.
            preferred = int(msg.preferred_agent) if msg.preferred_agent is not None else self.agent_id
            cost_out = float(msg.preferred_cost or 0.0)
        else:
            init_cost = float(msg.preferred_cost) if msg.preferred_cost is not None else float("inf")
            init_agent = int(msg.preferred_agent) if msg.preferred_agent is not None else -1
            mine_dominates = (my_cost < init_cost) or (
                my_cost == init_cost and self.agent_id < init_agent
            )
            if mine_dominates:
                preferred = self.agent_id
                cost_out = my_cost
            else:
                preferred = init_agent if init_agent >= 0 else self.agent_id
                cost_out = init_cost if init_cost != float("inf") else my_cost
        resp = SnowResponse(
            source=self.agent_id,
            query_id=msg.query_id,
            job_id=msg.job_id,
            preferred_agent=preferred,
            cost=cost_out,
            already_decided=False,
        )
        self._safe_send(int(msg.source), resp)

    def on_snow_response(self, msg: SnowResponse) -> None:
        """Initiator-side: accumulate responses; the driver thread evaluates."""
        if msg.job_id is None or msg.query_id is None:
            return
        with self._lock:
            state = self._states.get(msg.job_id)
            if state is None or state.finalized:
                return
            if state.pending_query_id != msg.query_id:
                return  # response for a prior round; discard
            state.pending_responses.append(msg)
            # If a peer reports the job is already decided, fast-finalize.
            if msg.already_decided and msg.preferred_agent is not None:
                self._finalize(state, int(msg.preferred_agent), reason="peer-decided")

    # ---- Driver thread --------------------------------------------------- #

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick(self._time())
            except Exception as exc:  # pragma: no cover - defensive
                self.host.log_warn(f"snow tick error: {exc}")
            if self._stop.wait(self.tick_interval_s):
                return

    def _tick(self, now: float) -> None:
        with self._lock:
            jobs = list(self._states.values())
        for state in jobs:
            if state.finalized:
                continue
            if state.pending_query_id is None:
                self._send_round(state, now)
                continue
            # A round ends as soon as every peer we actually queried has responded
            # (capped at k), or the deadline is hit. Using min(k, queried) instead of
            # a static k means small groups (fewer than k live peers) no longer wait
            # the full round_timeout on every round — the dominant latency source.
            with self._lock:
                got = len(state.pending_responses)
                needed = min(self.k, state.queried) if state.queried else self.k
                deadline_hit = state.round_deadline <= now
            if got >= needed or deadline_hit:
                self._evaluate_round(state, now)

        # Drop finalized state entries.
        with self._lock:
            for oid in [k for k, s in self._states.items() if s.finalized]:
                self._states.pop(oid, None)

    def _send_round(self, state: _SnowState, now: float) -> None:
        peers = self._pick_query_peers(exclude=self.agent_id)
        if not peers:
            # No peers yet — degenerate single-node case: claim immediately.
            self._finalize(state, self.agent_id, reason="single-node")
            return
        query_id = uuid.uuid4().hex
        with self._lock:
            state.pending_query_id = query_id
            state.pending_responses = []
            state.round_deadline = now + self.round_timeout_s
            state.last_round_at = now
            state.round_no += 1
            state.queried = len(peers)
        msg = SnowQuery(
            source=self.agent_id,
            query_id=query_id,
            job_id=state.proposal.object_id,
            preferred_agent=state.preferred,
            preferred_cost=state.preferred_cost,
            round=state.round_no,
        )
        for dest in peers:
            self._safe_send(dest, msg)

    def _evaluate_round(self, state: _SnowState, now: float) -> None:
        with self._lock:
            responses = list(state.pending_responses)
            state.pending_query_id = None
            state.pending_responses = []
        if not responses:
            # No-one responded; reset confidence and retry.
            with self._lock:
                state.confidence = 0
            self._maybe_abort_or_continue(state, now)
            return

        counts = Counter(int(r.preferred_agent) for r in responses
                         if r.preferred_agent is not None)
        if not counts:
            with self._lock:
                state.confidence = 0
            self._maybe_abort_or_continue(state, now)
            return
        top_choice, top_count = counts.most_common(1)[0]
        # Supermajority relative to the peers actually queried this round, so small
        # groups (fewer than k peers) can still clear the threshold and commit.
        alpha_threshold = self._alpha_threshold(state.queried or len(responses))

        with self._lock:
            if top_count >= alpha_threshold:
                if top_choice == state.preferred:
                    state.confidence += 1
                else:
                    state.preferred = top_choice
                    # Adopt the new candidate's cost from the responses if known.
                    new_cost = min(
                        (r.cost for r in responses
                         if r.preferred_agent is not None
                         and int(r.preferred_agent) == top_choice
                         and r.cost is not None),
                        default=state.preferred_cost,
                    )
                    state.preferred_cost = float(new_cost)
                    state.confidence = 1
            else:
                state.confidence = 0
                self.conflicts[state.proposal.object_id] = (
                    self.conflicts.get(state.proposal.object_id, 0) + 1
                )

            ready_to_commit = state.confidence >= self.beta

        if ready_to_commit:
            self._finalize(state, state.preferred, reason=f"beta@round={state.round_no}")
        else:
            self._maybe_abort_or_continue(state, now)

    def _maybe_abort_or_continue(self, state: _SnowState, now: float) -> None:
        if state.round_no >= self.max_rounds:
            self.host.log_warn(
                f"[snow] max_rounds exhausted for {state.proposal.object_id}; "
                f"abandoning Snow attempt"
            )
            with self._lock:
                state.finalized = True
                self.outgoing.remove_object(object_id=state.proposal.object_id)
                self.incoming.remove_object(object_id=state.proposal.object_id)
        # else: leave pending_query_id=None so next tick sends a fresh round.

    def _finalize(self, state: _SnowState, candidate: int, reason: str = "") -> None:
        if state.finalized:
            return
        winner = self.host.try_claim_assignment(state.proposal.object_id, candidate)
        with self._lock:
            state.finalized = True
            self.outgoing.remove_object(object_id=state.proposal.object_id)
            self.incoming.remove_object(object_id=state.proposal.object_id)

        # Per-decision latency instrumentation: separates Snow round time from any
        # downstream queue/serialization tax so the ~9s selection cost can be diagnosed.
        elapsed = self._time() - state.started_at if state.started_at else -1.0
        self.host.log_info(
            f"[SNOW_TIMING] Object:{state.proposal.object_id} rounds={state.round_no} "
            f"queried={state.queried} elapsed={elapsed:.3f}s reason={reason}"
        )

        obj = self.host.get_object(state.proposal.object_id)
        if obj is None:
            return
        if int(winner) == self.agent_id:
            self.host.log_info(
                f"[SNOW_LEADER] Object:{state.proposal.object_id} "
                f"agent:{self.agent_id} reason={reason}"
            )
            obj.leader_id = self.agent_id
            self.host.on_leader_elected(obj, state.proposal.p_id)
        else:
            self.host.log_info(
                f"[SNOW_PART] Object:{state.proposal.object_id} "
                f"leader:{winner} reason={reason}"
            )
            self.host.on_participant_commit(obj, int(winner), state.proposal.p_id)

    # ---- Helpers --------------------------------------------------------- #

    def _pick_query_peers(self, exclude: int) -> List[int]:
        try:
            pool = [int(p) for p in self.host.live_peer_ids() if int(p) != exclude]
        except Exception:
            pool = []
        if not pool:
            return []
        if len(pool) <= self.k:
            return pool
        # Uniform sampling unless topology-aware sampling is enabled AND site info exists.
        if self.local_sample_frac >= 1.0:
            return random.sample(pool, self.k)
        my_site = self._safe_site(None)
        if my_site is None:
            return random.sample(pool, self.k)
        # Partition by locality and draw most of the sample from same-site peers so
        # rounds resolve at LAN latency; keep a cross-site remainder for global convergence.
        local = [p for p in pool if self._safe_site(p) == my_site]
        remote = [p for p in pool if self._safe_site(p) != my_site]
        n_local = min(len(local), int(round(self.local_sample_frac * self.k)))
        picks = random.sample(local, n_local) if n_local else []
        n_remote = min(len(remote), self.k - len(picks))
        if n_remote > 0:
            picks += random.sample(remote, n_remote)
        if len(picks) < self.k:  # backfill from leftovers if a partition was too small
            leftover = list(set(pool) - set(picks))
            need = min(self.k - len(picks), len(leftover))
            if need > 0:
                picks += random.sample(leftover, need)
        return picks

    def _safe_site(self, peer_id: Optional[int]) -> Optional[str]:
        """Site of a peer (or this agent when peer_id is None); None if unavailable."""
        try:
            return self.host.my_site() if peer_id is None else self.host.peer_site(int(peer_id))
        except Exception:
            return None

    def _safe_send(self, dest: int, payload: object) -> None:
        try:
            self.transport.send(dest, payload)
        except Exception as exc:
            self.host.log_debug(f"[snow] send -> {dest} failed: {exc}")
