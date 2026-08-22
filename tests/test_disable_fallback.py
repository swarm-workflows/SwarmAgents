"""Tests for the fallback-disabled ablation (`LlmAgent._llm_or_analytic_cost`).

`CHAOS_JUNGLE_LLM_TEST_PLAN.md` §15 figure D: the campaign has established that the LLM's *output*
barely reaches the scheduler (§12.2 placement follows bid timing; §8 all four semantic mutations
move nothing; §10 cutting LLM calls 2.16x costs no completion). The untested complement is how
much of the system's resilience is the analytic fallback rather than the model. Disabling it turns
S05's graceful degradation into hard failure, which is what makes the safety net measurable.

The behaviour under test is narrow but load-bearing: when the bidder raises and the ablation is on,
the agent must return +inf (SelectionEngine's "not a candidate") and must NOT reach the analytic
cost model. If it silently fell back anyway, the ablation would be a no-op and the resulting run
would look like a normal S05 — a false negative that is invisible in the metrics.
"""
from __future__ import annotations

import os
import sys
import types

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)


def _stub_pydantic_ai() -> None:
    """Import LlmAgent without pydantic-ai, a deployment-only dependency (see
    test_designate_bidder for the reasoning)."""
    if "pydantic_ai" in sys.modules:
        return
    try:
        import pydantic_ai  # noqa: F401
        return
    except ImportError:
        pass

    def mod(name, **attrs):
        m = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(m, k, v)
        sys.modules[name] = m

    class _Any:
        def __init__(self, *a, **k):
            pass

        def __class_getitem__(cls, _item):
            return cls

    mod("pydantic_ai", Agent=_Any, ModelSettings=_Any, NativeOutput=_Any)
    mod("pydantic_ai.models")
    mod("pydantic_ai.models.google", GoogleModel=_Any)
    mod("pydantic_ai.models.openai", OpenAIChatModel=_Any)
    mod("pydantic_ai.providers")
    mod("pydantic_ai.providers.ollama", OllamaProvider=_Any)


_stub_pydantic_ai()

llm_agent = pytest.importorskip("swarm.agents.llm.llm_agent",
                                reason="LlmAgent not importable even with pydantic-ai stubbed")
LlmAgent = llm_agent.LlmAgent


class _Job:
    job_id = "j1"

    def to_dict(self, compact=False):
        return {"id": self.job_id}


class _Agent:
    agent_id = 7

    def to_dict(self):
        return {"agent_id": self.agent_id}


class _Bidder:
    """A bidder that always fails, which is what S05's 503 produces in practice."""

    def score(self, **_kwargs):
        raise RuntimeError("503 Service Unavailable")


def make_agent(disable_fallback: bool):
    a = LlmAgent.__new__(LlmAgent)
    a.agent_id = 7
    a.bidder = _Bidder()
    a.llm_disable_fallback = disable_fallback
    a.analytic_calls = []
    a.logged = []

    def _analytic(job, agent):
        a.analytic_calls.append((job.job_id, agent.agent_id))
        return 0.42

    a._cost_job_on_agent = _analytic
    a._get_peer_context = lambda: {"peer_agents": {}}

    class _Log:
        def info(self, *args, **_k):
            a.logged.append(str(args[0]) if args else "")

        def warning(self, *args, **_k):
            a.logged.append(str(args[0]) if args else "")

        def exception(self, *args, **_k):
            a.logged.append(str(args[0]) if args else "")

        def debug(self, *args, **_k):
            # Needed: the success path calls logger.debug OUTSIDE the audit-trail try/except, so
            # a missing method here raises into the fallback and makes a passing bid look failed.
            a.logged.append(str(args[0]) if args else "")

    a.logger = _Log()
    return a


def test_default_still_falls_back_to_analytic():
    """The shipped default must be unchanged — every campaign result depends on it."""
    agent = make_agent(disable_fallback=False)
    cost = agent._llm_or_analytic_cost(_Job(), _Agent())

    assert cost == 0.42, "must return the analytic cost"
    assert agent.analytic_calls == [("j1", 7)], "analytic model must be consulted"
    assert any("LLM_COST_FALLBACK" in m for m in agent.logged)


def test_ablation_returns_infinite_cost():
    """+inf is how SelectionEngine expresses "not a candidate", i.e. this agent does not bid."""
    agent = make_agent(disable_fallback=True)
    cost = agent._llm_or_analytic_cost(_Job(), _Agent())

    assert cost == float("inf")


def test_ablation_never_reaches_the_analytic_model():
    """The whole point: if it silently fell back, the ablation would be a no-op and the run would
    look like an ordinary S05 — a false negative invisible in the metrics."""
    agent = make_agent(disable_fallback=True)
    agent._llm_or_analytic_cost(_Job(), _Agent())

    assert agent.analytic_calls == [], "the safety net must not be used"
    assert not any("LLM_COST_FALLBACK" in m for m in agent.logged), "must not claim a fallback"


def test_ablation_logs_a_distinguishable_marker():
    """The run is scored by counting these, so the marker must not collide with FALLBACK."""
    agent = make_agent(disable_fallback=True)
    agent._llm_or_analytic_cost(_Job(), _Agent())

    assert any("LLM_COST_NO_BID" in m for m in agent.logged)


def test_ablation_is_inert_when_the_bid_succeeds():
    """No fault, no effect: the gateway arm's fault-free fallback rate is 0.0%, so this flag must
    change nothing unless something is actually breaking the LLM."""

    class _Bid:
        score = 80.0
        explanation = "fine"
        reasoning_time = 1.0

    class _OkBidder:
        def score(self, **_kwargs):
            return _Bid()

    for disabled in (False, True):
        agent = make_agent(disable_fallback=disabled)
        agent.bidder = _OkBidder()
        agent.save_to_db = lambda *a, **k: None
        agent.topology = types.SimpleNamespace(level=0, group=0)
        cost = agent._llm_or_analytic_cost(_Job(), _Agent())
        assert cost == pytest.approx(20.0), f"100 - 80 regardless of the flag (disabled={disabled})"
        assert agent.analytic_calls == []
