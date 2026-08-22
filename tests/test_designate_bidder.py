"""Tests for designated-bidder mode (`LlmAgent._designate_bidders`).

The throughput problem it addresses is measured in `CHAOS_JUNGLE_LLM_TEST_PLAN.md` §4.0b: every
agent scores every feasible pending job against itself, so ~3.7 distinct agents each pay a full
LLM bid for a job placed once, and throughput is flat in fleet size.

The dangerous failure mode is not slowness, it is **losing work**. A partitioning scheme that can
leave a job with no bidder turns a throughput fix into a completion regression — the one metric no
fault in the campaign has moved. These tests therefore concentrate on liveness: every job must end
up bid on by somebody, under agreement, under disagreement, and under a fleet-wide infeasibility.

`_designate_bidders` is exercised directly against a stub rather than a live agent, so the tests
stay independent of Redis, gRPC and the LLM.
"""
from __future__ import annotations

import os
import sys
import types

import numpy as np
import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)


def _stub_pydantic_ai() -> None:
    """Make `swarm.agents.llm.llm_agent` importable without pydantic-ai installed.

    pydantic-ai is a deployment dependency, not a test one — it is absent from a plain dev
    checkout, and `importorskip` would leave these tests permanently skipped, which is the same
    as not having them. `_designate_bidders` touches none of it (analytic cost and the pending
    queue only), so a stub is honest here: it keeps the test exercising real production code
    rather than a copy of it.
    """
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
        return m

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
    def __init__(self, job_id):
        self.job_id = job_id


class _Agent:
    def __init__(self, agent_id):
        self.agent_id = agent_id


class _Queue:
    def __init__(self):
        self.requeued = []

    def move_to_end(self, job):
        self.requeued.append(job.job_id)


class _Queues:
    def __init__(self):
        self.pending_queue = _Queue()


class _Selector:
    """Stands in for the analytic SelectionEngine: returns a caller-supplied designation."""

    def __init__(self, designate):
        self._designate = designate
        self.matrix_calls = 0

    def compute_cost_matrix(self, assignees, candidates):
        self.matrix_calls += 1
        # Shape (assignees, candidates) as apply_multiplicative_penalty requires — it reads
        # .size on the matrix, so this has to be a real array, not a placeholder.
        return np.ones((len(assignees), len(candidates)), dtype=float)

    def pick_agent_per_candidate(self, assignees, candidates, cost_matrix, objective,
                                threshold_pct, tie_break_key):
        return [self._designate(job, assignees) for job in candidates]


def make_agent(agent_id, peer_ids, designate, max_defers=3):
    """A bare LlmAgent with only what _designate_bidders touches."""
    a = LlmAgent.__new__(LlmAgent)
    a.agent_id = agent_id
    a.neighbor_map = {i: _Agent(i) for i in peer_ids}
    a.analytic_selector = _Selector(designate)
    a.queues = _Queues()
    a.selection_threshold_pct = 10.0
    a.designate_bidder_max_defers = max_defers
    a._projected_load_factor = lambda ag: 1.0

    class _Log:
        def info(self, *_a, **_k):
            pass

    a.logger = _Log()
    return a


# --- the core behaviour: one bidder per job ---------------------------------------------------

def test_only_the_designated_agent_keeps_the_job():
    jobs = [_Job("j1"), _Job("j2"), _Job("j3")]
    # j1 -> agent 1, j2 -> agent 2, j3 -> agent 3
    owner = {"j1": 1, "j2": 2, "j3": 3}

    def designate(job, assignees):
        return (_Agent(owner[job.job_id]), 1.0)

    kept = {}
    for me in (1, 2, 3):
        agent = make_agent(me, [1, 2, 3], designate)
        kept[me] = [j.job_id for j in agent._designate_bidders(list(jobs))]

    assert kept == {1: ["j1"], 2: ["j2"], 3: ["j3"]}
    # Every job is bid on exactly once across the fleet — that is the whole point.
    assert sorted(j for v in kept.values() for j in v) == ["j1", "j2", "j3"]


def test_non_designated_jobs_are_requeued_not_dropped():
    jobs = [_Job("j1"), _Job("j2")]

    def designate(job, assignees):
        return (_Agent(2), 1.0)          # both belong to agent 2

    agent = make_agent(1, [1, 2], designate)
    assert agent._designate_bidders(jobs) == []
    assert agent.queues.pending_queue.requeued == ["j1", "j2"], "must go back on the queue"


# --- liveness: the failure mode that would cost completion ------------------------------------

def test_persistent_disagreement_falls_back_to_bidding():
    """If a designee never bids, every other agent must eventually bid anyway.

    Without this, a job whose designation is wrong under stale gossip circulates forever and the
    run completes fewer than 300 jobs — a throughput change paid for in lost work.
    """
    job = _Job("orphan")

    def designate(j, assignees):
        return (_Agent(99), 1.0)         # an agent that is not us and never bids

    agent = make_agent(1, [1, 2], designate, max_defers=3)

    assert agent._designate_bidders([job]) == []          # defer 1
    assert agent._designate_bidders([job]) == []          # defer 2
    forced = agent._designate_bidders([job])              # defer 3 -> bid anyway
    assert [j.job_id for j in forced] == ["orphan"]
    assert job.designation_defers == 3


def test_fleet_wide_infeasible_job_does_not_consume_defers():
    """A job nobody can run is not a designation failure and must not exhaust the fallback."""
    job = _Job("too-big")

    def designate(j, assignees):
        return (None, float("inf"))

    agent = make_agent(1, [1, 2], designate, max_defers=3)
    for _ in range(5):
        assert agent._designate_bidders([job]) == []
    assert not hasattr(job, "designation_defers"), "infeasible must not count as a deferral"
    assert agent.queues.pending_queue.requeued == ["too-big"] * 5


def test_single_agent_fleet_is_a_passthrough():
    """With no peers there is nothing to partition, and gating would strand everything."""
    jobs = [_Job("j1"), _Job("j2")]

    def designate(job, assignees):  # pragma: no cover - must not be reached
        raise AssertionError("should not designate in a one-agent fleet")

    agent = make_agent(1, [1], designate)
    assert agent._designate_bidders(jobs) is jobs
    assert agent.analytic_selector.matrix_calls == 0, "no analytic matrix needed"


def test_empty_neighbor_map_is_a_passthrough():
    jobs = [_Job("j1")]
    agent = make_agent(1, [], lambda j, a: (None, 1.0))
    assert agent._designate_bidders(jobs) is jobs


# --- it must use the analytic engine, never the LLM one ---------------------------------------

def test_designation_uses_the_analytic_selector_only():
    """The saving comes from pricing peers analytically; using the LLM engine here would mean
    ~30x the inference to remove a 3.7x redundancy."""
    jobs = [_Job("j1")]

    def designate(job, assignees):
        return (_Agent(1), 1.0)

    agent = make_agent(1, [1, 2, 3], designate)

    class _Exploding:
        def compute_cost_matrix(self, **_):
            raise AssertionError("the LLM selector must not be used for designation")

    agent.selector = _Exploding()
    assert [j.job_id for j in agent._designate_bidders(jobs)] == ["j1"]
    assert agent.analytic_selector.matrix_calls == 1


def test_all_live_peers_are_considered():
    """Designation must range over the whole live fleet, or it partitions against a stale subset."""
    seen = {}

    def designate(job, assignees):
        seen["ids"] = sorted(a.agent_id for a in assignees)
        return (_Agent(1), 1.0)

    agent = make_agent(1, [1, 2, 3, 4, 5], designate)
    agent._designate_bidders([_Job("j1")])
    assert seen["ids"] == [1, 2, 3, 4, 5]
