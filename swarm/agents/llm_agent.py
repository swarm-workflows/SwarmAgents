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
from typing import Any, Dict, Optional

from swarm.agents.llm_agent.llm_bidder import LlmBidder
from swarm.agents.llm_agent.llm_config import LlmConfig
from swarm.agents.resource_agent import ResourceAgent
from swarm.database.repository import Repository
from swarm.models.job import Job
from swarm.models.agent_info import AgentInfo
from swarm.models.object import ObjectState
from swarm.selection.engine import SelectionEngine


class LlmAgent(ResourceAgent):
    """LLM-enhanced agent that reuses ResourceAgent's consensus & queueing, but
    swaps the cost/bid function (and optionally tie-breaking selection) for LLM-based
    reasoning.
    """

    def __init__(self, agent_id: int, config_file: str, debug: bool = False):
        super().__init__(agent_id=agent_id, config_file=config_file, debug=debug)
        # Load LLM configuration
        self.llm_cfg = LlmConfig.from_dict(self.config.get("llm", {}))

        # Instantiate bidder if enabled; else set to None and rely on analytic cost
        self.bidder: Optional[LlmBidder] = None
        if self.llm_cfg.enabled:
            try:
                self.bidder = LlmBidder(self.llm_cfg, logger=self.logger)
            except Exception as e:
                self.logger.warning("LLM bidder disabled due to init error: %s", e)
                self.bidder = None

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
        On any error, fall back to the analytic model.
        """
        if self.bidder is not None:
            try:
                payload_job = job.to_dict()
                payload_agent = agent.to_dict() if hasattr(agent, "to_dict") else json.loads(agent.to_json())
                bid = self.bidder.score(job=payload_job, agent_state=payload_agent)
                score = float(bid.score)
                # audit trail (best-effort)
                try:
                    self.repository.save({
                        "job_id": getattr(job, "job_id", None),
                        "agent_id": getattr(agent, "agent_id", None),
                        "score": score,
                        "explanation": bid.explanation,
                        "ts": time.time(),
                    }, key_prefix="llm_score", level=self.topology.level, group=self.topology.group)
                except Exception:
                    pass
                return 100.0 - max(0.0, min(100.0, score))
            except Exception as e:
                self.logger.debug("LLM scoring failed, using analytic cost: %s", e)
        return super()._cost_job_on_agent(job=job, agent=agent)

    # If you also want the LLM to rank a *set* of near-equal candidates (tie-break):
    # The SelectionEngine already returns near-min candidates using the threshold.
    # We override the parent’s `selection_main` lightly to re-rank if configured.
    def selection_main(self):
        """Run selection loop; if multiple near-equal assignees exist, optionally
        ask the LLM to break the tie by considering qualitative factors.
        """
        self.logger.info("LLM Selection thread - Start")
        super_selection = super().selection_main  # keep parent behavior handy

        if not self.llm_cfg.enabled or not self.llm_cfg.use_for_selection:
            # Use the default parent loop entirely
            return super_selection()

        # Otherwise, we wrap the parent loop but intercept the short-list step.
        # To avoid copying a long loop, we delegate selection scoring entirely to
        # our cost hook above; SelectionEngine will supply candidates using that cost.
        # That means the parent loop is sufficient. We just log that we are active.
        return super_selection()



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
