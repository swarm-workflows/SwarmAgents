# llm_delegator.py
# MIT License
#
# Author: Komal Thareja (kthare10@renci.org)
"""LLM group delegation for hierarchical coordinators (P0-1).

A coordinator holds a job and must hand it to one of its child groups. Until now that choice
was made either by the bandit (`mab.algorithm`) or not at all (delegate to every capable
group). The LLM plane only ever produced a *bid* — a cost for the coordinator itself — so an
"LLM coordinator" was a coordinator with an LLM-scored bid, not a coordinator that reasons
about where work should go. This module is the missing half: given the job and a summary of
each candidate group, the model returns a **ranked** list of group ids and a short rationale.

Three things are deliberate.

**The candidate ids are in the JSON schema, not just the prompt.** Ollama's `NativeOutput`
drives generation from the schema, so a prompt listing groups 3, 7, 11 against a schema of
bare `int` invites a confident answer of `[0, 1, 2]`. The schema is built per candidate set
(cached) with `Literal[...]` over exactly the ids on offer, which is the same lesson
`bid_model_for_scale` records for the rating range.

**The answer is sanitized anyway.** A structured-output model can still return a short list, a
repeated id, or nothing. `rank()` filters to known ids and de-duplicates; completing the
ranking (and deciding what to do with an empty one) is the caller's business, because only the
caller knows the order to fall back to.

**The call is bounded by `llm.timeout_seconds`.** This runs on the coordinator's scheduling
thread, which is the thread that delegates every subsequent job — an unbounded call there
stalls the whole subtree, not just one decision. P0-3 adds caching and an inference budget on
top; the timeout is the floor.

Two limits of that floor, both open and both P0-3's to close. It is a **per-request** timeout,
so provider-level retries and pydantic-ai output-validation retries can stack several of them
into one `rank()`; there is no wall-clock deadline around the call. And the call is
**synchronous**, so a coordinator's delegation rate is capped at 1/latency however many jobs
it holds. Before reading anything into LLM-plane throughput at scale, measure this.
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple

from pydantic import BaseModel, Field, create_model
from pydantic_ai import Agent as PydanticAgent, ModelSettings, NativeOutput

from swarm.agents.llm.llm_bidder import build_model
from swarm.agents.llm.llm_config import LlmConfig

#: How many distinct candidate-group sets keep a cached response schema. A coordinator sees a
#: handful of shapes (all groups, all-but-one, ...); this only stops a pathological run from
#: growing the cache without bound.
_SCHEMA_CACHE_MAX = 64

DEFAULT_PROMPT = (
    "You are the scheduler for one level of a hierarchical compute fleet. You are given a JOB "
    "and the current state of the child GROUPS that could run it. Rank the groups from best to "
    "worst for this job and explain your choice in one sentence.\n"
    "Each group reports: children (how many agents are alive in it), cpu/ram/gpu headroom as "
    "the free fraction in 0..1 (1.0 = idle), inflight (jobs already delegated there and not "
    "yet finished), failure_rate and type_failure_rate (recent fraction of delegated jobs that "
    "failed, overall and for this job's type), and timeout_rate (recent delegations that never "
    "reported back at all).\n"
    "Prefer a group with the headroom the job actually needs and a low failure rate for this "
    "job's type. Return every group id exactly once, best first."
)


class GroupRanking(BaseModel):
    """Ranked child-group ids, best first, with a one-line rationale."""
    ranking: List[int] = Field(default_factory=list)
    rationale: str = ""
    reasoning_time: float | None = None   # seconds


def ranking_model_for_groups(group_ids: Sequence[int]) -> type[BaseModel]:
    """Build the response schema for exactly *group_ids*.

    The ids go into the schema as a `Literal` so structured output cannot invent a group. Falls
    back to the open `GroupRanking` when there are no candidates (the caller short-circuits
    that case, but the schema builder must not raise).
    """
    ids = tuple(int(g) for g in group_ids)
    if not ids:
        return GroupRanking
    return create_model(
        "GroupRanking_" + "_".join(str(g) for g in ids),
        ranking=(List[Literal[ids]], Field(default_factory=list)),  # type: ignore[valid-type]
        rationale=(str, ""),
        reasoning_time=(Optional[float], None),
    )


class LlmDelegator:
    """Asks the model which child group should get a job.

    Construction mirrors `LlmBidder`: same provider resolution, same timeout, same
    `NativeOutput` treatment for Ollama. Kept as its own object so the delegation prompt,
    schema and latency history are separable from the bidding ones — E4 reports them apart.
    """

    def __init__(self, cfg: LlmConfig, logger: Optional[Any] = None,
                 prompt: Optional[str] = None):
        self.cfg = cfg
        self.logger = logger
        self.provider = (cfg.provider or "").strip().lower()
        self.model = build_model(cfg)
        self._schema_cache: Dict[Tuple[int, ...], type[BaseModel]] = {}

        system_prompt = prompt or (cfg.prompts or {}).get("delegate") or DEFAULT_PROMPT
        self.agent: PydanticAgent = PydanticAgent(model=self.model, system_prompt=system_prompt)
        if self.logger:
            self.logger.info(f"[LLM_DELEGATOR] System Prompt: {system_prompt}")

    def _output_type(self, group_ids: Sequence[int]):
        key = tuple(int(g) for g in group_ids)
        schema = self._schema_cache.get(key)
        if schema is None:
            if len(self._schema_cache) >= _SCHEMA_CACHE_MAX:
                self._schema_cache.clear()
            schema = ranking_model_for_groups(key)
            self._schema_cache[key] = schema
        # Small local models emit malformed tool-call args; Ollama's json_schema mode is reliable.
        return NativeOutput(schema) if self.provider == "ollama" else schema

    def rank(self, *, job: Dict[str, Any], groups: Dict[int, Dict[str, Any]]
             ) -> Tuple[List[int], str, float]:
        """Return `(ranked_group_ids, rationale, seconds)`.

        The ranking contains only ids present in *groups*, each at most once, in the model's
        order. It may be shorter than *groups* — including empty, when the model returns
        nothing usable. Raises whatever the provider raises (timeout included); the caller
        owns the fallback.
        """
        candidates = [int(g) for g in groups.keys()]
        timeout_s = float(getattr(self.cfg, "timeout_seconds", 0) or 0)

        prompt = (
            f"JOB:{json.dumps(job, ensure_ascii=False, separators=(',', ':'))}\n"
            f"GROUPS:{json.dumps(groups, ensure_ascii=False, separators=(',', ':'), default=str)}"
        )
        if self.logger:
            self.logger.debug(
                f"[LLM_DELEGATE_PROMPT] candidates={candidates} len={len(prompt)} chars\n{prompt}")

        start = time.perf_counter()
        res = self.agent.run_sync(
            prompt,
            output_type=self._output_type(candidates),
            model_settings=ModelSettings(
                temperature=float(getattr(self.cfg, "temperature", 0.0) or 0.0),
                # Same reason as the bidder: this thread delegates every job the coordinator
                # holds, so an unbounded call stalls the subtree. 0 disables.
                **({"timeout": timeout_s} if timeout_s > 0 else {}),
            ),
        )
        elapsed = time.perf_counter() - start

        out = res.output
        allowed = set(candidates)
        seen = set()
        ranking: List[int] = []
        for g in (getattr(out, "ranking", None) or []):
            try:
                gid = int(g)
            except (TypeError, ValueError):
                continue
            if gid in allowed and gid not in seen:
                seen.add(gid)
                ranking.append(gid)

        rationale = (getattr(out, "rationale", "") or "").strip()
        if len(rationale) > 200:
            rationale = rationale[:197] + "..."
        return ranking, rationale, elapsed
