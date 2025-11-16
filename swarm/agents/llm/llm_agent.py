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
import time
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
            payload_job = job.to_dict()
            payload_agent = agent.to_dict() if hasattr(agent, "to_dict") else json.loads(agent.to_json())

            # Gather peer context for load-aware scoring
            peer_context = self._get_peer_context()

            # Pass peer context to LLM bidder
            bid = self.bidder.score(
                job=payload_job,
                agent_state=payload_agent,
                peer_context=peer_context
            )

            job.reasoning_time = bid.reasoning_time
            score = float(bid.score)
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
            return 100.0 - max(0.0, min(100.0, score))
        except Exception as e:
            self.logger.exception("LLM scoring failed, using analytic cost: %s", e)
            return self._cost_job_on_agent(job, agent)

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

                    peer_agents[peer_id] = peer_agent.to_dict()

            # Get recent conflicts for this agent
            conflicts = 0
            if hasattr(self, 'metrics') and hasattr(self.metrics, 'conflicts'):
                conflicts = sum(self.metrics.conflicts.values())

            # Get topology type
            topology_type = 'unknown'
            if hasattr(self, 'topology') and self.topology:
                topology_type = str(self.topology.type.value) if hasattr(self.topology.type, 'value') else str(self.topology.type)

            child_agents = {}
            for c in self.children.values():
                child_agents[c.agent_id] = c.to_dict()

            return {
                'total_agents': self.configured_agent_count if hasattr(self, 'configured_agent_count') else len(self.neighbor_map),
                'peer_agents': peer_agents,
                'conflicts': conflicts,
                'topology': topology_type,
                'child_agents': child_agents,
            }
        except Exception as e:
            self.logger.warning(f"Failed to gather peer context: {e}")
            # Return minimal context on error
            return {
                'total_agents': 0,
                'peer_agents': {},
                'conflicts': 0,
                'topology': 'unknown',
                'child_agents': {},
            }

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
                self.logger.info(f"Time for computing cost matrix: {cost_computation}")

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
                    tie_break_key=lambda ag, s: getattr(ag, "agent_id", "")
                )

                # Step 3: If this agent is assigned, start proposal
                for job, (selected_agent, cost) in zip(pending_jobs, assignments):
                    if selected_agent and selected_agent.agent_id == self.agent_id:
                        proposal = ProposalInfo(
                            p_id=generate_id(),
                            object_id=job.job_id,
                            agent_id=self.agent_id,
                            cost=round((cost + self.agent_id), 2)
                        )
                        proposals.append(proposal)
                        job.state = ObjectState.PRE_PREPARE

                if len(proposals):
                    self.logger.debug(f"Identified jobs to propose: {proposals}")
                    if self.debug:
                        self.logger.info(f"Identified jobs to select: {jobs}")
                    self.engine.propose(proposals=proposals)
                    proposals.clear()

                time.sleep(0.5)
            except Exception as e:
                self.logger.exception(f"Error occurred while executing e: {e}")
        self.logger.info(f"Agent: {self} stopped with restarts: {self.metrics.restarts}!")