# MIT License
#
# Copyright (c) 2025 swarm-workflows
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
# Author: Komal Thareja(kthare10@renci.org)

"""
LLM-driven Agent
================

This agent mirrors the structure of `ResourceAgent` but uses a Large Language
Model (LLM) to:

- reason about a **cost/bid score** for <job, agent> pairs,
- participate in the **consensus engine** for quorum (pre-prepare/prepare/commit),
- optionally assist with **multi-job selection** when multiple near-equal
  candidates exist.

Drop-in replacement for `ResourceAgent`: same queues, same consensus hooks,
config keys extended with an `llm:` section.

Minimal config snippet
----------------------

```yaml
llm:
  enabled: true
  provider: openai            # or "none" to disable external calls
  model: "gpt-4o-mini"       # any text model that returns JSON
  temperature: 0.1
  timeout_seconds: 6
  use_for_selection: true     # use LLM to break ties across candidates
  prompts:
    cost: |-
      You are a scheduler. Given a JSON job and an agent's resource state,
      output a single JSON object: {"score": <float in [0, 100]>, "explanation": "..."}.
      Higher score = better fit / lower cost. Consider capacity headroom,
      estimated runtime, network DTN requirements, and fairness when applicable.
```

If the LLM is disabled or errors, the agent seamlessly falls back to the
analytical cost model implemented by `ResourceAgent.compute_job_cost`.
"""
from __future__ import annotations

import json
import threading
import time
from collections import OrderedDict
from typing import Any, Dict, Optional

from swarm.agents.llm.llm_bidder import LlmBidder
from swarm.agents.llm.llm_config import LlmConfig
from swarm.agents.resource_agent import ResourceAgent
from swarm.consensus.messages.proposal_info import ProposalInfo
from swarm.database.repository import Repository
from swarm.models.job import Job
from swarm.models.agent_info import AgentInfo
from swarm.models.object import ObjectState
from swarm.selection.engine import SelectionEngine
from swarm.selection.penalties import apply_multiplicative_penalty
from swarm.agents.cost_scale import CostScale
from swarm.utils.tiebreak import tiebreak_rank
from swarm.utils.utils import generate_id


class LlmAgent(ResourceAgent):
    """LLM-enhanced agent that reuses ResourceAgent's consensus & queueing, but
    swaps the cost/bid function (and optionally tie-breaking selection) for LLM-based
    reasoning.
    """

    def __init__(self, agent_id: int, config_file: str, debug: bool = False):
        super().__init__(agent_id=agent_id, config_file=config_file, debug=debug)
        # Load LLM configuration
        self.llm_cfg = LlmConfig.from_dict(self.config.get("llm", {}))

        self.bidder: Optional[LlmBidder] = LlmBidder(self.llm_cfg, logger=self.logger)

        self._init_llm_state()

        # Ablation switch, off by default: remove the analytic safety net so a failed LLM bid
        # means no bid at all (see _llm_or_analytic_cost). Read straight from the raw config
        # rather than through LlmConfig, which is a bidder-transport dataclass and has no reason
        # to know about a scheduling experiment.
        self.llm_disable_fallback = bool((self.config.get("llm", {}) or {}).get(
            "disable_fallback", False))

        # Designated-bidder mode. Off by default: it changes who bids, so turning it on changes
        # every capture and fairness figure the campaign has measured (test plan 7) and must be
        # an explicit choice, not a silent default.
        job_cfg = self.config.get("job_selection", {}) or {}
        self.designate_bidder = bool(job_cfg.get("designate_bidder", False))
        # A DEADLINE, not a retry count — see _designate_bidders. Must stay well above one LLM
        # bid (4-7 s measured) plus consensus (~1 s), or the fallback fires before the designated
        # agent could have bid and designation buys nothing.
        self.designate_bidder_fallback_s = float(
            job_cfg.get("designate_bidder_fallback_s", 30.0))

        # ResourceAgent.__init__ has just built a selection engine over the ANALYTIC cost model.
        # Keep it before overwriting self.selector below: the cheap model is what makes
        # designation affordable (see _designate_bidders), and rebuilding it here would mean a
        # second cache for identical work.
        self.analytic_selector = self.selector

        # Re-wire the selection engine to use LLM-driven cost if available
        self.selector = SelectionEngine(
            feasible=lambda job, agent: self.is_job_feasible(job, agent),
            cost=self._llm_or_analytic_cost,
            candidate_key=self._job_sig,
            assignee_key=self._agent_sig,
            candidate_version=lambda job: int(getattr(job, "version", 0) or 0),
            assignee_version=lambda ag: int(getattr(ag, "version", 0) or getattr(ag, "updated_at", 0) or 0),
            cache_enabled=True,
            feas_cache_size=131072,
            cost_cache_size=131072,
            cache_ttl_s=60.0,  # optional, if you want time-based safety
        )

        self.logger.info(
            "LlmAgent initialized: enabled=%s provider=%s model=%s",
            self.llm_cfg.enabled, self.llm_cfg.provider, self.llm_cfg.model
        )

    # ------------------------------------------------------------------------------------------
    # Cost / selection overrides
    # ------------------------------------------------------------------------------------------
    def _llm_or_analytic_cost(self, job: Job, agent: AgentInfo) -> float:
        """Return a *lower-is-better* cost for SelectionEngine.

        If LLM bidder is available, get a score in [0,100] and convert to cost: 100 - score.
        Now includes peer context to enable load-aware scoring decisions.
        On any error, fall back to the analytic model.
        """
        try:
            payload_job = job.to_dict(compact=True)
            payload_agent = agent.to_dict() if hasattr(agent, "to_dict") else json.loads(agent.to_json())

            # Gather peer context for load-aware scoring
            peer_context = self._get_peer_context()

            # Log LLM cost computation start
            self.logger.info(
                f"[LLM_COST_START] Job={job.job_id} Agent={agent.agent_id} "
                f"GatheringPeerContext=yes Peers={len(peer_context.get('peer_agents', {}))}"
            )

            # Pass peer context to LLM bidder
            bid = self.bidder.score(
                job=payload_job,
                agent_state=payload_agent,
                peer_context=peer_context
            )

            job.reasoning_time = bid.reasoning_time
            score = float(bid.score)
            cost = self._score_to_cost(score, job, agent)
            # Only cache a verdict about THIS agent: the cache answers "what do I cost for this
            # job", and a peer's cost is a different question (and not peer-computable for the
            # LLM plane anyway).
            if getattr(agent, "agent_id", None) == self.agent_id:
                self._remember_cost(job.job_id, cost, CostScale.LLM)

            # Log successful LLM cost computation
            self.logger.info(
                f"[LLM_COST_COMPLETE] Job={job.job_id} Agent={agent.agent_id} "
                f"Score={score:.2f} Cost={cost:.2f} ReasoningTime={bid.reasoning_time:.3f}s "
                f"Explanation=\"{bid.explanation}\""
            )

            self.logger.debug("LLM bidder score=%s bid=%s peer_ctx=%s", score, bid, peer_context)

            # audit trail (best-effort)
            try:
                self.repository.save({
                    "id": getattr(job, "job_id", None),
                    "agent_id": getattr(agent, "agent_id", None),
                    "score": score,
                    "explanation": bid.explanation,
                    "ts": time.time(),
                    "reasoning_time": bid.reasoning_time,
                    #"peer_context": peer_context,  # Save peer context for analysis
                },
                    key=f"llm_score:A-{self.agent_id}:{Repository.KEY_JOB}:{job.job_id}",
                    level=self.topology.level, group=self.topology.group)
            except Exception as e:
                self.logger.exception("Failed to save LLM bidder: %s", e)
            return cost
        except Exception as e:
            if self.llm_disable_fallback:
                # Ablation: the analytic safety net is removed, so a failed bid means this agent
                # simply does not bid. An infinite cost is how SelectionEngine expresses "not a
                # candidate", so the job goes to whichever agent still has a working LLM.
                #
                # This is the experiment the campaign has been circling. §12.2 found placement is
                # decided by *when* an agent bids, §8 found corrupting *what* it bids moves
                # nothing, and §10 found cutting LLM calls costs no completion — all pointing at
                # the LLM's output doing little. The complement is untested: how much of the
                # system's resilience is the fallback rather than the model. Removing it converts
                # S05's graceful degradation into hard failure and measures the difference.
                #
                # Note this is a no-op without a fault: the gateway arm's fault-free fallback rate
                # is 0.0%, so there is nothing to disable unless something is breaking the LLM.
                self.logger.warning(
                    f"[LLM_COST_NO_BID] Job={job.job_id} Agent={agent.agent_id} "
                    f"FallbackDisabled, not bidding, due to error: %s", e
                )
                return float("inf")
            self.logger.warning(
                f"[LLM_COST_FALLBACK] Job={job.job_id} Agent={agent.agent_id} "
                f"FallingBackToAnalyticCost due to error: %s", e
            )
            analytical_cost = self._cost_job_on_agent(job, agent)
            self.logger.info(
                f"[ANALYTIC_COST] Job={job.job_id} Agent={agent.agent_id} Cost={analytical_cost:.2f}"
            )
            # Tag the verdict as analytic so instrumentation can tell a fallback from a real
            # bid. Both planes emit 0..100, so nothing is converted — but the two models do
            # CALIBRATE differently (analytic median 11.85 against a typical LLM bid of 25), so
            # a fallback bid is systematically a little cheaper. That is a real, ~2x effect for
            # E4 to measure, not a units error to correct away.
            if getattr(agent, "agent_id", None) == self.agent_id:
                self._remember_cost(job.job_id, float(analytical_cost), CostScale.ANALYTIC)
            return analytical_cost

    # ------------------------------------------------------------------------------------------
    # Optional: expose a utility to score a job for this agent explicitly
    # ------------------------------------------------------------------------------------------
    def score_job_here(self, job: Job) -> Dict[str, Any]:
        """Return the LLM (or analytic) scoring result for the *local* agent only.
        Useful for debugging or external RPCs.
        """
        agent_info = self._generate_agent_info()
        cost = self._llm_or_analytic_cost(job=job, agent=agent_info)
        return {"agent_id": self.agent_id, "cost": cost}

    def _get_peer_context(self) -> Dict[str, Any]:
        """Gather peer information for LLM context to enable load-aware scoring.

        Returns a dictionary with:
        - total_agents: Number of configured agents in the system
        - peer_agents: Dictionary mapping peer_id -> peer_agent
        - conflicts: Number of recent conflicts for this agent
        - topology: Network topology type (mesh, ring, etc.)
        """
        try:
            peer_agents = {}
            for peer_id, peer_agent in self.neighbor_map.items():
                if peer_id != self.agent_id and peer_agent:

                    peer_agents[peer_id] = {
                        'id': peer_agent.agent_id,
                        'load': round(peer_agent.load, 2) if peer_agent.load else 0,
                        'proposed_load': round(peer_agent.proposed_load, 2) if peer_agent.proposed_load else 0,
                    }

            # Get recent conflicts for this agent
            conflicts = 0
            if hasattr(self, 'metrics') and hasattr(self.metrics, 'conflicts'):
                conflicts = sum(self.metrics.conflicts.values())

            # Get topology type
            topology_type = 'unknown'
            if hasattr(self, 'topology') and self.topology:
                topology_type = str(self.topology.type.value) if hasattr(self.topology.type, 'value') else str(self.topology.type)

            '''
            child_agents = {}
            for c in self.children.values():
                child_agents[c.agent_id] = c.to_dict()
            '''

            context = {
                'total_agents': self.configured_agent_count if hasattr(self, 'configured_agent_count') else len(self.neighbor_map),
                'peer_agents': peer_agents,
                'conflicts': conflicts,
                'topology': topology_type,
                #'child_agents': child_agents,
            }

            # Log peer context summary at DEBUG level
            self.logger.debug(
                f"[PEER_CONTEXT] Agent={self.agent_id} "
                f"TotalAgents={context['total_agents']} Peers={len(peer_agents)} "
                #f"Children={len(child_agents)} Conflicts={conflicts} Topology={topology_type}"
                f"Conflicts={conflicts} Topology={topology_type}"
            )

            return context
        except Exception as e:
            self.logger.warning(f"[PEER_CONTEXT_ERROR] Agent={self.agent_id} Failed to gather peer context: {e}")
            # Return minimal context on error
            return {
                'total_agents': 0,
                'peer_agents': {},
                'conflicts': 0,
                'topology': 'unknown',
                'child_agents': {},
            }

    # ---------- Verdict cache: the LLM's opinion on the consensus path -----------------------
    COST_SCALE = CostScale.LLM

    def _init_llm_state(self) -> None:
        """Every piece of LLM state that a bid depends on, in one call.

        Construction goes through here so nothing can be half-initialised. It is also the single
        hook for tests that build an agent with `__new__` to skip Redis and gRPC: two separate
        initialisers had already broken those stubs twice, each time only when a bid path
        happened to touch the missing attribute. Add new LLM state here, not in `__init__`.
        """
        self._init_cost_cache()
        self._init_elicitation()

    # ---------- Elicitation (P0-6) -----------------------------------------------------------
    def _init_elicitation(self) -> None:
        """Set up how a model rating becomes a cost.

        The campaign's central problem with the LLM plane is that its output barely varies:
        **59% of qwen2.5:3b bids were the identical value 75.00**, and gpt-oss-20b put **92% of
        bids on two values**. Asking for a 0-100 rating gets answers in round steps, so most
        agents tie and the bid cannot order them — which is one reason inverting the model's
        reasoning changed no placements. A bigger model made it worse, not better, so the lever
        is the elicitation, not the model.

        Two arms, both off by default so the measured baseline is what ships:

        * `llm.score_scale` — the range the model is asked for. A finer range gives it room to
          discriminate. The cost is normalised back to 0..100 either way.
        * `llm.tie_break_with_analytic` — when two bids are still equal, order them by the
          analytic cost. The term is deliberately **sub-granular**: at most half of one rating
          step, so it can separate exact ties and can never reorder distinct ratings. That bound
          is what makes it safe, and `tests/test_elicitation.py` pins it.
        """
        llm_cfg = (getattr(self, "config", None) or {}).get("llm", {}) or {}
        self.llm_score_scale = int(llm_cfg.get("score_scale", 100) or 100)
        self.llm_tie_break_with_analytic = bool(llm_cfg.get("tie_break_with_analytic", False))
        # Analytic cost at which the tie-break term reaches half its (already tiny) range.
        # 11.85 is the measured median analytic cost over 400 real Pegasus jobs x the five
        # shipped flavours, so the term spreads ties across the part of the range they occupy.
        self.llm_tie_break_ref = float(llm_cfg.get("tie_break_ref_cost", 11.85))
        # Bid distribution, for E4/T5: a plane whose bids nearly all tie cannot be influencing
        # placement, whatever else a run shows.
        self._bid_scores: "OrderedDict[float, int]" = OrderedDict()
        self._bid_count = 0

    def _score_to_cost(self, score: float, job: Job, agent: AgentInfo) -> float:
        """Convert a model rating into a 0..100 cost (lower is better)."""
        scale = float(self.llm_score_scale)
        score = max(0.0, min(scale, float(score)))
        cost = 100.0 * (1.0 - score / scale)
        self._record_bid_score(score)
        if not self.llm_tie_break_with_analytic:
            return cost
        # One rating step is `100/scale` cost units; stay strictly inside half of it so two
        # distinct ratings can never swap order no matter what the analytic model says.
        step = 100.0 / scale
        try:
            analytic = float(self._cost_job_on_agent(job, agent))
        except Exception:
            return cost
        if analytic < 0 or analytic != analytic:          # negative or NaN — no opinion
            return cost
        fraction = analytic / (analytic + self.llm_tie_break_ref)   # in [0, 1)
        return cost + 0.5 * step * fraction

    def _record_bid_score(self, score: float) -> None:
        """Track the bid distribution so tie rate and distinct-value count are reportable."""
        key = round(float(score), 4)
        self._bid_count += 1
        self._bid_scores[key] = self._bid_scores.get(key, 0) + 1
        # Bounded: a degenerate plane has a handful of distinct values, and a healthy one does
        # not need every value retained to show that it is healthy.
        if len(self._bid_scores) > 4096:
            self._bid_scores.popitem(last=False)

    def bid_distribution(self) -> dict:
        """`{count, distinct, modal_share}` for the bids this agent has made."""
        if not self._bid_count:
            return {"count": 0, "distinct": 0, "modal_share": 0.0}
        modal = max(self._bid_scores.values())
        return {"count": self._bid_count,
                "distinct": len(self._bid_scores),
                "modal_share": modal / self._bid_count}

    def _init_cost_cache(self) -> None:
        """Set up the verdict cache. Called from `__init__`; a bid must never run without it.

        How the LLM's opinion reaches consensus (chaos finding 11). An inbound Snow query is
        answered on the single consumer thread, which must never block — so it cannot call the
        model. Without a cache the only non-blocking answer was the ANALYTIC cost, which is what
        `_HostAdapter.my_cost_for_job` used to return: an LlmAgent priced a job with the LLM when
        proposing and analytically when voting, so under `consensus.protocol: snow` (the shipped
        default) the LLM's verdict entered the protocol only through whoever initiated the round
        and was out-voted by every peer that had the job locally.

        `_cost_cache[job_id] = (native_cost, scale, ts)`. The scale is recorded per entry so
        instrumentation can separate a real LLM verdict from an analytic fallback; it converts
        nothing, because both planes already emit 0..100 — `compute_job_cost` ends in `* 100`
        and the LLM plane returns `100 - score`. See `swarm/agents/cost_scale.py` for the
        measured distributions and for why the per-plane rescaling that briefly lived there was
        worse than the mismatch it was meant to fix.
        """
        llm_cfg = (getattr(self, "config", None) or {}).get("llm", {}) or {}
        self._cost_cache: "OrderedDict[str, tuple]" = OrderedDict()
        self._cost_cache_lock = threading.Lock()
        self._cost_cache_ttl_s = float(llm_cfg.get("cost_cache_ttl_s", 300.0))
        self._cost_cache_max = int(llm_cfg.get("cost_cache_max", 8192))
        # What to answer a query with when this agent has no LLM verdict for the job.
        #   "yield"    — say nothing (cost None); the engine yields to the initiator. Default:
        #                answering with a different decision plane than the one we propose with
        #                is exactly the bug above.
        #   "analytic" — answer with the analytic cost, canonicalised as analytic. Kept as an
        #                ablation arm so the cost of yielding can be measured, not assumed.
        self.llm_snow_cost_fallback = str(llm_cfg.get("snow_cost_fallback", "yield")).lower()
        # Counters for E4/E8: how often the LLM's verdict actually reached a peer's vote.
        self.llm_wire_cost_hits = 0
        self.llm_wire_cost_misses = 0

    def _remember_cost(self, job_id: str, native_cost: float, scale: str) -> None:
        """Record this agent's own cost for a job, with the plane that produced it."""
        if not job_id:
            return
        with self._cost_cache_lock:
            self._cost_cache[job_id] = (float(native_cost), scale, time.time())
            self._cost_cache.move_to_end(job_id)
            while len(self._cost_cache) > self._cost_cache_max:
                self._cost_cache.popitem(last=False)

    def native_cost_for_job(self, object_id: str):
        """Answer an inbound consensus query from the LLM verdict, never by calling the model.

        This runs on the single inbound consumer thread: an LLM call here would stall every
        other peer's queries behind one 4-7s inference. A cached verdict is the only way the
        LLM's opinion can reach a vote at all.

        A miss returns None ("no opinion"), so the engine yields to the initiator rather than
        answering with the analytic model — answering with a *different* decision plane than the
        one this agent proposes with is the bug this method exists to fix. Set
        `llm.snow_cost_fallback: analytic` to measure the alternative.
        """
        entry = None
        if object_id:
            with self._cost_cache_lock:
                entry = self._cost_cache.get(object_id)
        if entry is not None:
            native, scale, ts = entry
            if self._cost_cache_ttl_s <= 0 or (time.time() - ts) <= self._cost_cache_ttl_s:
                self.llm_wire_cost_hits += 1
                return float(native), scale
            # Stale: a verdict about a job whose state has moved on is worse than no verdict.
            with self._cost_cache_lock:
                self._cost_cache.pop(object_id, None)

        self.llm_wire_cost_misses += 1
        if self.llm_snow_cost_fallback == "analytic":
            return super().native_cost_for_job(object_id)
        return None

    def _designate_bidders(self, pending_jobs: list) -> list:
        """Pick one bidder per job using the ANALYTIC cost, and keep only this agent's share.

        The problem (test plan §9): every agent scores every feasible pending job against
        itself, so ~3.7 distinct agents each pay a full LLM bid for a job that is placed once.
        Nothing partitions the pool, so an added agent is a redundant bidder rather than a new
        server, and measured throughput is flat in fleet size — 4.3x the agents bought 0.86x.

        Why the analytic model can do this and the LLM cannot: `_cost_job_on_agent(job, agent)`
        and `is_job_feasible(job, agent)` both take an arbitrary `AgentInfo`, so an agent can
        price a job *for a peer* from gossiped state. Peer LLM cost is not available (finding 11).
        Each agent therefore reaches the same designation locally, with no coordination and no
        extra inference, and only the designated agent spends an LLM call.

        This is deliberately NOT the commented-out all-agents LLM matrix a few lines below. That
        would price every job for every peer with the model — roughly 30x the inference to remove
        a 3.7x redundancy.

        **A non-designated job is skipped, not requeued.** `pending_queue.gets()` is a
        non-destructive peek at the first N PENDING jobs, so skipping leaves the job in every
        agent's window — including the designee's, which is what lets it bid promptly. Requeueing
        it would only delay the requeueing agent's own next look at it.

        To be precise about what reordering can and cannot do, because an earlier version of this
        comment overstated it: it **cannot** change who a job is designated to.
        `pick_agent_per_candidate` selects per *column*, with column-wise thresholds and no
        cross-candidate accumulation, so a job's designee depends only on its own cost column and
        the assignee set — never on which other jobs share the window. Queue order therefore
        affects *when* an agent considers a job, not *who* is designated to it. (`ResourceAgent`
        already reorders unilaterally on its own infeasible path, so this is not a new hazard.)

        **Liveness.** Agents can still disagree while gossip is stale, and a job whose designee
        never bids must not stall forever. The fallback is a **deadline, not a counter**: a
        non-designated agent bids anyway once the job has gone unclaimed for
        `designate_bidder_fallback_s`. A counter cannot work here — three loop iterations is
        ~1.5 s while the designee needs 4-7 s for its LLM bid plus ~1 s of consensus, so a
        counter-based fallback fires before the designee could possibly have bid, on every job.
        The deadline must stay comfortably above bid-plus-consensus for the same reason.

        A job infeasible for every live agent is skipped without starting its deadline, which
        leaves the existing infeasible handling exactly as it was.
        """
        agents_map = self.neighbor_map
        agents = [agents_map.get(aid) for aid in list(agents_map.keys())]
        agents = [a for a in agents if a is not None]
        if len(agents) <= 1:
            return pending_jobs          # nothing to partition against

        matrix = self.analytic_selector.compute_cost_matrix(
            assignees=agents, candidates=pending_jobs)
        matrix = apply_multiplicative_penalty(
            cost_matrix=matrix, assignees=agents, factor_fn=self._projected_load_factor)
        designations = self.analytic_selector.pick_agent_per_candidate(
            assignees=agents,
            candidates=pending_jobs,
            cost_matrix=matrix,
            objective="min",
            threshold_pct=self.selection_threshold_pct,
            tie_break_key=lambda ag, s, cand: tiebreak_rank(
                getattr(cand, "job_id", ""), getattr(ag, "agent_id", "")),
        )

        now = time.time()
        mine, deferred, forced, infeasible, requeued = [], 0, 0, 0, 0
        for job, (agent, _cost) in zip(pending_jobs, designations):
            if agent is None:
                infeasible += 1
                # "Infeasible for the whole fleet" is a LOCAL verdict, not a fact: it is computed
                # over `self.neighbor_map`, which is per-agent live membership. An agent that has
                # transiently dropped the one peer able to run this job concludes nobody can,
                # while its peers designate it normally — and SWIM churn is not hypothetical here
                # (7-9 false-fails per run, §3). Requeueing on a transient verdict would push the
                # job out of this agent's window for a full rotation on the strength of a local
                # error. It could not misdirect the designation (that is per-column, see the
                # docstring), but it delays this agent's own next look for no reason.
                #
                # Not requeueing at all is the opposite failure: `gets()` returns the first N
                # PENDING jobs, so a genuinely unschedulable job would hold a window slot forever,
                # and enough of them stall the run (§2.3's head-of-line blocking).
                #
                # Gate it on the same deadline as a deferral, which bounds both. A transient
                # disagreement clears well inside the window and never touches the queue; a
                # persistently unschedulable job is requeued after it, and since every agent then
                # reaches that verdict they requeue together and stay aligned.
                #
                # State stays PENDING, not BLOCKED: the BLOCKED path is restored by
                # `_restore_infeasible_jobs`, which only `ResourceAgent.selection_main` calls, and
                # this override never does — a BLOCKED job here would never come back. Cycling an
                # unschedulable job is a pre-existing gap in this agent (never retired via
                # `max_infeasible_retries`), but it no longer holds up anything else.
                since = getattr(job, "designation_infeasible_since", None)
                if since is None:
                    job.designation_infeasible_since = now
                elif now - since >= self.designate_bidder_fallback_s:
                    job.designation_infeasible_since = None   # fresh window if it returns
                    requeued += 1
                    self.queues.pending_queue.move_to_end(job)
                continue

            # Feasible for someone. Clear any infeasible marker so blips in separate episodes
            # cannot accumulate into a spurious requeue.
            if getattr(job, "designation_infeasible_since", None) is not None:
                job.designation_infeasible_since = None

            if agent.agent_id == self.agent_id:
                mine.append(job)
                continue
            first_seen = getattr(job, "designation_deferred_at", None)
            if first_seen is None:
                first_seen = now
                job.designation_deferred_at = first_seen
            if now - first_seen >= self.designate_bidder_fallback_s:
                forced += 1
                mine.append(job)
            else:
                deferred += 1

        if forced:
            # Not routine: it means a designated agent did not bid within the deadline. A rate
            # anywhere near the deferred count means designation is not holding and the mode is
            # buying nothing.
            self.logger.info(
                f"[DESIGNATE_FALLBACK] Agent={self.agent_id} forced={forced} "
                f"after {self.designate_bidder_fallback_s}s unclaimed"
            )
        if deferred or forced or infeasible:
            self.logger.info(
                f"[DESIGNATE] Agent={self.agent_id} candidates={len(pending_jobs)} "
                f"mine={len(mine)} deferred={deferred} forced={forced} "
                f"infeasible={infeasible} requeued={requeued} peers={len(agents)}"
            )
        return mine

    def selection_main(self):
        self.logger.info(f"Starting agent: {self}")
        while self.live_agent_count != self.configured_agent_count:
            time.sleep(0.5)
            self.logger.info(f"[SEL_WAIT] Waiting for Peer map to be populated: "
                             f"{self.live_agent_count}/{self.configured_agent_count}!")

        while not self.shutdown:
            try:
                pending_jobs = self.queues.pending_queue.gets(states=[ObjectState.PENDING],
                                                              count=self.proposal_job_batch_size)
                if not pending_jobs:
                    self.logger.debug(f"No pending jobs available for agent: {self.agent_id}")
                    time.sleep(0.5)
                    continue
                proposals = []
                jobs = []

                # Step 0 (optional): decide who *should* bid on each job using the cheap analytic
                # model, and drop the rest. This is the only step that reduces LLM calls per
                # placed job; everything below still scores only this agent.
                if self.designate_bidder:
                    pending_jobs = self._designate_bidders(pending_jobs)
                    if not pending_jobs:
                        time.sleep(0.5)
                        continue

                # Step 1: Compute cost matrix ONCE for all agents and jobs
                # Scoring the jobs on itself.
                agents = [self.neighbor_map.get(self.agent_id)]
                '''
                agents_map = self.neighbor_map
                agent_ids = list(agents_map.keys())
                agents = [agents_map.get(aid) for aid in agent_ids if agents_map.get(aid) is not None]
                '''

                # Build once
                start = time.perf_counter()
                cost_matrix = self.selector.compute_cost_matrix(
                    assignees=agents,
                    candidates=pending_jobs,
                )
                cost_computation = time.perf_counter() - start

                # Calculate LLM scoring statistics
                num_jobs = len(pending_jobs)
                num_agents = len(agents)
                total_evaluations = num_jobs * num_agents
                avg_time_per_eval = (cost_computation / total_evaluations) if total_evaluations > 0 else 0

                self.logger.info(
                    f"[COST_MATRIX_COMPLETE] Jobs={num_jobs} Agents={num_agents} "
                    f"TotalEvaluations={total_evaluations} TotalTime={cost_computation:.3f}s "
                    f"AvgTimePerEval={avg_time_per_eval:.3f}s"
                )

                cost_matrix_with_penalities = apply_multiplicative_penalty(cost_matrix=cost_matrix,
                                                                           assignees=agents,
                                                                           factor_fn=self._projected_load_factor)

                # Step 2: Use existing helper to get the best agent per job
                assignments = self.selector.pick_agent_per_candidate(
                    assignees=agents,
                    candidates=pending_jobs,
                    cost_matrix=cost_matrix_with_penalities,
                    objective="min",
                    threshold_pct=self.selection_threshold_pct,  # e.g., 10 means within +10% of best
                    tie_break_key=lambda ag, s, cand: tiebreak_rank(
                        getattr(cand, "job_id", ""), getattr(ag, "agent_id", ""))
                )

                # Step 3: If this agent is assigned, start proposal
                for job, (selected_agent, cost) in zip(pending_jobs, assignments):
                    if selected_agent and selected_agent.agent_id == self.agent_id:
                        proposal = ProposalInfo(
                            p_id=generate_id(),
                            object_id=job.job_id,
                            agent_id=self.agent_id,
                            # Real cost, not `cost + self.agent_id` — see the same change in
                            # ResourceAgent. Exact ties are broken by tiebreak_rank now.
                            cost=self.proposal_cost(job, cost)
                        )
                        proposals.append(proposal)
                        job.state = ObjectState.PRE_PREPARE

                        # Log that this agent won the bid for this job
                        reasoning_time = getattr(job, 'reasoning_time', None) or 0
                        self.logger.info(
                            f"[LLM_BID_WON] Job={job.job_id} Agent={self.agent_id} "
                            f"Cost={cost:.2f} FinalCost={proposal.cost:.2f} "
                            f"ReasoningTime={reasoning_time:.3f}s"
                        )

                if len(proposals):
                    self.logger.info(
                        f"[PROPOSALS_CREATED] Agent={self.agent_id} Count={len(proposals)} "
                        f"ProposalIDs={[p.p_id for p in proposals]}"
                    )
                    self.logger.debug(f"Identified jobs to propose: {proposals}")
                    if self.debug:
                        self.logger.info(f"Identified jobs to select: {jobs}")
                    self.engine.propose(proposals=proposals)
                    proposals.clear()

                time.sleep(0.5)
            except Exception as e:
                self.logger.exception(f"Error occurred while executing e: {e}")
        self.logger.info(f"Agent: {self} stopped with restarts: {self.metrics.restarts}!")