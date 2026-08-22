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


def make_agent(agent_id, peer_ids, designate, fallback_s=30.0):
    """A bare LlmAgent with only what _designate_bidders touches."""
    a = LlmAgent.__new__(LlmAgent)
    a.agent_id = agent_id
    a.neighbor_map = {i: _Agent(i) for i in peer_ids}
    a.analytic_selector = _Selector(designate)
    a.queues = _Queues()
    a.selection_threshold_pct = 10.0
    a.designate_bidder_fallback_s = fallback_s
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


def test_nothing_is_requeued():
    """Requeueing is what broke the first version, so this is pinned.

    gets() is a non-destructive peek at the first N PENDING jobs, so all agents share a window
    only while their queues stay in the same order. move_to_end on a job this agent does not own
    reorders its queue away from its peers', the designee stops seeing the job designated to it,
    and the liveness fallback becomes the normal path.
    """
    jobs = [_Job("j1"), _Job("j2")]

    def designate(job, assignees):
        return (_Agent(2), 1.0)          # both belong to agent 2

    agent = make_agent(1, [1, 2], designate)
    assert agent._designate_bidders(jobs) == []
    assert agent.queues.pending_queue.requeued == [], "must NOT reorder the shared window"


# --- liveness: the failure mode that would cost completion ------------------------------------

def test_unclaimed_job_falls_back_after_the_deadline():
    """If a designee never bids, others must eventually bid anyway — but only after a deadline.

    Without this, a job whose designation is wrong under stale gossip stalls and the run
    completes fewer than 300 jobs: a throughput change paid for in lost work.
    """
    job = _Job("orphan")

    def designate(j, assignees):
        return (_Agent(99), 1.0)         # an agent that is not us and never bids

    agent = make_agent(1, [1, 2], designate, fallback_s=30.0)

    assert agent._designate_bidders([job]) == [], "held while inside the deadline"
    assert agent._designate_bidders([job]) == [], "still held — a counter would have fired here"

    # Age the job past the deadline instead of sleeping.
    job.designation_deferred_at -= 31.0
    forced = agent._designate_bidders([job])
    assert [j.job_id for j in forced] == ["orphan"]


def test_deadline_is_not_a_loop_counter():
    """Many iterations inside the deadline must not force a bid.

    This is the regression that mattered: the designee needs 4-7 s for its LLM bid plus ~1 s of
    consensus, so a counter of a few ~0.5 s iterations fires before it could possibly claim the
    job — restoring full redundancy on every job while adding delay.
    """
    job = _Job("j1")
    agent = make_agent(1, [1, 2], lambda j, a: (_Agent(2), 1.0), fallback_s=30.0)

    for _ in range(50):
        assert agent._designate_bidders([job]) == []
    assert agent.queues.pending_queue.requeued == []


def test_infeasible_is_not_requeued_inside_the_deadline():
    """An "infeasible for the whole fleet" verdict is LOCAL, so it must not reorder immediately.

    It is computed over self.neighbor_map — per-agent live membership. An agent that has
    transiently dropped the one peer able to run a job concludes nobody can, while its peers
    designate it normally, and SWIM churn makes that real (7-9 false-fails per run). Requeueing on
    sight would desynchronise the shared window, which is the defect that made deferral requeues
    harmful.
    """
    job = _Job("maybe-infeasible")

    def designate(j, assignees):
        return (None, float("inf"))

    agent = make_agent(1, [1, 2], designate)
    for _ in range(20):
        assert agent._designate_bidders([job]) == []
    assert agent.queues.pending_queue.requeued == [], "a transient verdict must not reorder"
    assert job.designation_infeasible_since is not None, "but the deadline must be armed"


def test_persistently_infeasible_job_is_requeued_after_the_deadline():
    """A genuinely unschedulable job must stop holding a slot in the shared window.

    gets() returns the first N PENDING jobs, so leaving it in place forever is §2.2's
    head-of-line blocking. Past the deadline every agent reaches this verdict, so they requeue
    together and stay aligned.
    """
    job = _Job("unschedulable")
    agent = make_agent(1, [1, 2], lambda j, a: (None, float("inf")), fallback_s=30.0)

    assert agent._designate_bidders([job]) == []          # arms the deadline
    job.designation_infeasible_since -= 31.0             # age past it
    assert agent._designate_bidders([job]) == []
    assert agent.queues.pending_queue.requeued == ["unschedulable"]
    assert job.designation_infeasible_since is None, "window resets so it does not thrash"
    assert not hasattr(job, "state"), "stays PENDING: BLOCKED is never restored in this agent"


def test_becoming_feasible_clears_the_infeasible_deadline():
    """Blips in separate episodes must not accumulate into a spurious requeue."""
    job = _Job("flappy")
    verdict = {"infeasible": True}

    def designate(j, assignees):
        return (None, float("inf")) if verdict["infeasible"] else (_Agent(2), 1.0)

    agent = make_agent(1, [1, 2], designate)
    agent._designate_bidders([job])
    assert job.designation_infeasible_since is not None

    verdict["infeasible"] = False                        # a peer reappears
    agent._designate_bidders([job])
    assert job.designation_infeasible_since is None, "marker must be cleared, not carried over"
    assert agent.queues.pending_queue.requeued == []


def test_infeasible_job_does_not_block_a_feasible_one_behind_it():
    """The blocking case: an unschedulable job ahead of one this agent owns."""
    dead, live = _Job("unschedulable"), _Job("mine")

    def designate(job, assignees):
        return (None, float("inf")) if job.job_id == "unschedulable" else (_Agent(1), 1.0)

    agent = make_agent(1, [1, 2], designate)
    kept = agent._designate_bidders([dead, live])

    assert [j.job_id for j in kept] == ["mine"], "the feasible job must still be bid on"


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


# --- the invariant that makes queue order safe ------------------------------------------------

def test_designation_is_independent_of_window_composition():
    """A job's designee must not depend on which other jobs share its window.

    This is the property that makes unilateral queue reordering harmless:
    pick_agent_per_candidate selects per column with column-wise thresholds and no
    cross-candidate accumulation, so queue order changes *when* an agent considers a job, never
    *who* is designated to it. If this ever becomes false — a global assignment, or per-agent load
    accumulated across a batch — then reordering would misdirect designations and the skip/requeue
    reasoning in _designate_bidders has to be revisited.
    """
    owner = {"a": 1, "b": 2, "c": 3}

    def designate(job, assignees):
        return (_Agent(owner[job.job_id]), 1.0)

    # Same job, three different windows and orderings; the verdict for "a" must not move.
    windows = [
        [_Job("a")],
        [_Job("b"), _Job("a"), _Job("c")],
        [_Job("c"), _Job("b"), _Job("a")],
    ]
    for jobs in windows:
        agent = make_agent(1, [1, 2, 3], designate)
        kept = [j.job_id for j in agent._designate_bidders(jobs)]
        assert kept == ["a"], f"agent 1 must keep only 'a', got {kept}"


def test_requeue_does_not_change_a_designation():
    """Even after this agent reorders its own queue, the designation is unchanged."""
    job = _Job("unschedulable")
    verdict = {"infeasible": True}

    def designate(j, assignees):
        return (None, float("inf")) if verdict["infeasible"] else (_Agent(2), 1.0)

    agent = make_agent(1, [1, 2], designate, fallback_s=30.0)
    agent._designate_bidders([job])
    job.designation_infeasible_since -= 31.0
    agent._designate_bidders([job])
    assert agent.queues.pending_queue.requeued == ["unschedulable"], "it did reorder"

    # ...and the job, once feasible again, still goes to its designee and not to us.
    verdict["infeasible"] = False
    assert agent._designate_bidders([job]) == [], "still designated to agent 2, not claimed here"


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
