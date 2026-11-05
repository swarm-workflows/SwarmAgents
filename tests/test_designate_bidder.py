"""Tests for designated-bidder mode (`LlmAgent._designate_bidders`).

The throughput problem it addresses is measured in `CHAOS_JUNGLE_LLM_TEST_PLAN.md` §9: every
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

# Imported AFTER the sys.path insert above, not with the third-party imports: `swarm` is only
# importable once the repo root is on the path. Placing it higher happens to work when pytest is
# run from the repo root (cwd is on sys.path) and raises ModuleNotFoundError at COLLECTION time
# from anywhere else — which aborts the whole session, not just this file.
from swarm.utils.tiebreak import tiebreak_rank  # noqa: E402


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
                                tie_break_key):
        return [self._designate(job, assignees) for job in candidates]


def make_agent(agent_id, peer_ids, designate, fallback_s=30.0):
    """A bare LlmAgent with only what _designate_bidders touches."""
    a = LlmAgent.__new__(LlmAgent)
    a.agent_id = agent_id
    a.neighbor_map = {i: _Agent(i) for i in peer_ids}
    a.analytic_selector = _Selector(designate)
    a.queues = _Queues()
    a.designate_bidder_fallback_s = fallback_s
    a.designate_bidder = True
    a._projected_load_factor = lambda ag: 1.0
    # The shipped setup, not a hand-rolled copy of it: a double that assembles its own
    # counters stops exercising the agent that ships the moment one is added.
    a._init_bidding_stats()

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

    gets() returns the first N PENDING jobs, so leaving it in place forever is §2.3's
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
#
# These test the REAL selection engine, not the stub. The stub used elsewhere in this file is a
# hardcoded lookup by job id, so it satisfies column-independence by construction and could never
# fail — testing it would be circular. The invariant lives in
# SelectionEngine.pick_agent_per_candidate, so that is what is exercised here, with real matrices.

def _engine():
    from swarm.selection.engine import SelectionEngine
    return SelectionEngine(
        feasible=lambda job, agent: True,
        cost=lambda job, agent: 0.0,
        candidate_key=lambda job: job.job_id,
        assignee_key=lambda ag: ag.agent_id,
        cache_enabled=False,
    )


# cost[assignee][candidate]: c1 -> a2 (3.0), c2 -> a2 (2.0), c3 -> a1 (1.0)
_COSTS = np.array([
    [5.0, 9.0, 1.0],
    [3.0, 2.0, 8.0],
    [7.0, 4.0, 6.0],
])


def test_selection_is_column_independent_when_a_candidate_is_alone():
    """A candidate's chosen assignee must not depend on which other candidates are present.

    This is the property that makes unilateral queue reordering harmless in
    _designate_bidders: queue order changes WHICH jobs share a window, so if selection coupled
    candidates together, reordering could misdirect designations.
    """
    eng = _engine()
    assignees = [_Agent(1), _Agent(2), _Agent(3)]
    cands = [_Job("c1"), _Job("c2"), _Job("c3")]

    full = eng.pick_agent_per_candidate(
        assignees=assignees, candidates=cands, cost_matrix=_COSTS, objective="min")
    assert [a.agent_id for a, _ in full] == [2, 2, 1], "precondition: the expected winners"

    for i, job in enumerate(cands):
        alone = eng.pick_agent_per_candidate(
            assignees=assignees, candidates=[job],
            cost_matrix=_COSTS[:, [i]], objective="min")
        assert alone[0][0].agent_id == full[i][0].agent_id, (
            f"{job.job_id} changed assignee when considered alone")


def test_selection_is_invariant_to_candidate_order():
    eng = _engine()
    assignees = [_Agent(1), _Agent(2), _Agent(3)]
    cands = [_Job("c1"), _Job("c2"), _Job("c3")]
    full = eng.pick_agent_per_candidate(
        assignees=assignees, candidates=cands, cost_matrix=_COSTS, objective="min")

    for order in ([2, 0, 1], [1, 2, 0], [2, 1, 0]):
        shuffled = eng.pick_agent_per_candidate(
            assignees=assignees,
            candidates=[cands[i] for i in order],
            cost_matrix=_COSTS[:, order],
            objective="min",
        )
        for pos, src in enumerate(order):
            assert shuffled[pos][0].agent_id == full[src][0].agent_id, (
                f"{cands[src].job_id} changed assignee under reordering {order}")


def test_column_independence_holds_with_threshold_and_tiebreak():
    """The two options _designate_bidders actually passes must not couple candidates either."""
    eng = _engine()
    assignees = [_Agent(1), _Agent(2), _Agent(3)]
    # Deliberate exact ties, which is where a tie-break could leak cross-candidate state.
    costs = np.array([
        [4.0, 1.0, 7.0],
        [4.0, 1.0, 2.0],
        [9.0, 1.0, 2.0],
    ])
    cands = [_Job("t1"), _Job("t2"), _Job("t3")]
    kwargs = dict(
        objective="min",
        tie_break_key=lambda ag, s, cand: tiebreak_rank(
            getattr(cand, "job_id", ""), getattr(ag, "agent_id", "")),
    )

    full = eng.pick_agent_per_candidate(
        assignees=assignees, candidates=cands, cost_matrix=costs, **kwargs)
    for i, job in enumerate(cands):
        alone = eng.pick_agent_per_candidate(
            assignees=assignees, candidates=[job], cost_matrix=costs[:, [i]], **kwargs)
        assert alone[0][0].agent_id == full[i][0].agent_id, (
            f"{job.job_id} moved with threshold+tiebreak — selection is coupled across candidates")


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


# --- P0-8: measuring what designation is supposed to buy --------------------------------------
#
# The mode's whole claim is that bidders-per-job falls from ~3.7 to ~1, so an added agent is a
# new server rather than a redundant bidder. Nothing measured that: the per-iteration log line
# says how many jobs THIS agent kept, never how many agents paid for a bid on the same job. That
# left the claim unfalsifiable and, worse, made its failure mode invisible — a run where the
# liveness deadline fires on everything has designation in name only, and produced artefacts
# identical to a healthy one.

def test_bidders_per_job_counts_agents_not_bids():
    """Re-bidding a job after a lost consensus round is not a second bidder. The quantity that
    matters is how many AGENTS priced the job, which is what the fleet sum divides by."""
    a = make_agent(1, [1, 2], lambda job, assignees: (_Agent(1), 1.0))
    a._note_bid("j1")
    a._note_bid("j1")          # same job again, same agent
    a._note_bid("j2")
    stats = a.bidding_stats()
    assert stats["bid_jobs"] == 2, "distinct jobs this agent paid for"
    assert stats["bid_calls"] == 3, "and the raw call count, kept separately"


def test_the_bid_counter_can_never_change_the_bid():
    """It is called from inside the bid path's `try`, whose `except` falls back to the analytic
    cost. An AttributeError there would not show up as a broken counter — it would silently turn
    a working LLM bid into an analytic one and move placement. This is not hypothetical: adding
    the counter did exactly that to a pacing test before the guard went in."""
    a = LlmAgent.__new__(LlmAgent)          # no _init_bidding_stats: counters absent
    a._note_bid("j1")                        # must not raise
    a._bid_job_ids = set()
    a._bid_calls = 0
    a._note_bid(None)                        # a job with no id is not a bid
    assert a.bidding_stats  # attribute exists
    assert a._bid_calls == 0


def test_designation_counts_jobs_not_passes_of_the_loop():
    """`pending_queue.gets()` is a non-destructive peek, so an unplaced job comes back on every
    pass of the selection loop (~2 Hz). Counting `+= len(pending_jobs)` therefore counts
    job-PASSES, and the categories accumulate at wildly different rates: a job designated here
    leaves after ~1 pass because it is bid on at once, while a job designated elsewhere sits in
    `deferred` for the whole 30 s deadline — ~60 passes. On those numbers a fleet that is really
    50% fallback reports 67%, and `candidates` reports pending-job-passes rather than jobs."""
    jobs = [_Job("j1"), _Job("j2"), _Job("j3")]
    designate = lambda job, assignees: (_Agent(1 if job.job_id == "j1" else 2), 1.0)
    a = make_agent(1, [1, 2], designate)
    for _ in range(5):                       # five passes over the same unplaced jobs
        a._designate_bidders(jobs)
    stats = a.bidding_stats()
    assert a.designation["iterations"] == 5, "the loop really did run five times"
    assert stats["designate_candidates"] == 3, "three jobs, not fifteen job-passes"
    assert stats["designate_mine"] == 1, "j1"
    assert stats["designate_deferred"] == 2, "j2 and j3"
    assert stats["designate_forced"] == 0


def test_a_run_where_the_deadline_fires_on_everything_is_visible():
    """The failure mode. If every job hits the fallback, bidders-per-job returns to its
    undesignated value and the mode buys nothing — but completion, latency and the [DESIGNATE]
    line all look ordinary. `designate_forced_share` is what separates the two runs."""
    jobs = [_Job("j1"), _Job("j2")]
    a = make_agent(1, [1, 2], lambda job, assignees: (_Agent(2), 1.0))  # never designated to us
    a._designate_bidders(jobs)
    for job in jobs:                      # age every deferral past the deadline
        job.designation_deferred_at -= 31.0
    mine = a._designate_bidders(jobs)
    a._designate_bidders(jobs)               # and again on the next pass
    assert len(mine) == 2, "the deadline must still guarantee liveness"
    stats = a.bidding_stats()
    assert stats["designate_forced"] == 2, "two jobs, not two-jobs-times-two-passes"
    assert stats["designate_forced_share"] == 1.0, "designation held for nothing"


def test_mine_and_forced_are_counted_apart():
    """`mine` is the list actually bid on, which INCLUDES the forced ones. Counting it whole
    would report a coordinator as healthily designated while every job it took was a fallback."""
    jobs = [_Job("j1"), _Job("j2")]
    designate = lambda job, assignees: (_Agent(1 if job.job_id == "j1" else 2), 1.0)
    a = make_agent(1, [1, 2], designate)
    a._designate_bidders(jobs)
    jobs[1].designation_deferred_at -= 31.0
    a._designate_bidders(jobs)
    a._designate_bidders(jobs)               # still unplaced: forced again, same job
    stats = a.bidding_stats()
    assert stats["designate_mine"] == 1, "j1 — genuinely designated"
    assert stats["designate_forced"] == 1, "j2, once, however many passes it is forced on"
    assert stats["designate_forced_share"] == 0.5


def test_a_job_that_took_both_routes_is_not_counted_twice():
    """`mine` and `forced` overlap across reselection rounds. Within one round they are
    disjoint, but a job that loses consensus returns to PENDING and comes round again — so it
    can be designated here once and reach its fallback deadline later. Summing the two sets
    then counts it twice and DILUTES the share: a fleet where every job eventually hit the
    fallback would report 0.50 instead of 1.00, understating exactly the failure this number
    exists to expose."""
    a = make_agent(1, [1, 2], lambda job, assignees: (_Agent(1), 1.0))
    for category in ("mine", "forced"):
        for job_id in ("j1", "j2", "j3"):
            a._note_designation(category, _Job(job_id))
    stats = a.bidding_stats()
    assert stats["designate_mine"] == 3
    assert stats["designate_forced"] == 3
    assert stats["designate_claimed"] == 3, "three jobs took both routes, not six"
    assert stats["designate_forced_share"] == 1.0, "every job hit the fallback"


def test_retry_cost_is_still_visible_even_though_jobs_are_counted_once():
    """Counting a job once is right for the share and wrong for the cost, so the cost lives in
    a different pair: re-bidding the same job after a lost round is `bid_calls` above
    `bid_jobs`."""
    a = make_agent(1, [1, 2], lambda job, assignees: (_Agent(1), 1.0))
    for _ in range(3):
        a._note_bid("j1")
    stats = a.bidding_stats()
    assert stats["bid_jobs"] == 1
    assert stats["bid_calls"] == 3, "two redundant re-bids, visible"


def test_stats_stay_out_of_a_run_that_never_bid():
    """A rule-based or analytic run must keep exactly the metrics payload it always had."""
    a = make_agent(1, [1, 2], lambda job, assignees: (_Agent(1), 1.0))
    a.designate_bidder = False
    a.bidder = None
    a.delegator = None
    assert a.llm_usage_snapshot() == {}


# --- a wholesale LLM failure must not look like a healthy run ---------------------------------

def test_a_totally_dead_llm_plane_is_announced():
    """Measured on the slice 2026-09-14: the shipped config asks for `gpt-4o-mini`, the gateway
    key allows only `gpt-oss-20b` and friends, so all 924 bids returned 403 — and the run
    completed 197 of 197 jobs, drained normally, and wrote a metrics payload that looked
    entirely healthy. It was not an LLM-plane measurement, and not a clean analytic baseline
    either: a failed bid returns in ~0 s, which is the race-to-propose regime P0-7 exists for."""
    a = make_agent(1, [1, 2], lambda job, assignees: (_Agent(1), 1.0))
    warnings = []
    a.logger = type("L", (), {"info": lambda *_a, **_k: None,
                              "warning": lambda self, *args: warnings.append(args)})()
    for i in range(a._LLM_DEAD_MIN_CALLS):
        a._note_bid(f"j{i}")
        a._bid_failures += 1
        a._warn_if_the_llm_plane_is_dead("403 model access denied")
    assert warnings, "a run where every bid fails must say so while it is still running"
    assert "LLM_PLANE_DEAD" in warnings[0][0]
    assert a.bidding_stats()["bid_failures"] == a._LLM_DEAD_MIN_CALLS


def test_a_partly_working_llm_plane_is_not_called_dead():
    """Fallbacks are expected and are already counted; only a total outage is this warning."""
    a = make_agent(1, [1, 2], lambda job, assignees: (_Agent(1), 1.0))
    warnings = []
    a.logger = type("L", (), {"info": lambda *_a, **_k: None,
                              "warning": lambda self, *args: warnings.append(args)})()
    for i in range(50):
        a._note_bid(f"j{i}")
        if i % 2:
            a._bid_failures += 1
        a._warn_if_the_llm_plane_is_dead("some error")
    assert not warnings


def test_the_warning_does_not_fire_on_a_handful_of_early_failures():
    """A provider hiccup in the first few bids is not an outage, and a warning that fires on
    one is a warning that gets filtered out."""
    a = make_agent(1, [1, 2], lambda job, assignees: (_Agent(1), 1.0))
    warnings = []
    a.logger = type("L", (), {"info": lambda *_a, **_k: None,
                              "warning": lambda self, *args: warnings.append(args)})()
    for i in range(a._LLM_DEAD_MIN_CALLS - 1):
        a._note_bid(f"j{i}")
        a._bid_failures += 1
        a._warn_if_the_llm_plane_is_dead("e")
    assert not warnings


def test_a_bid_that_fails_before_the_model_call_is_still_an_attempt():
    """The attempt counter is the FIRST statement in the bid path's try, because the `except`
    counts a failure. When it sat lower — below the payload build, the peer-context gather and
    a log line — anything failing in that gap counted a failure with no attempt behind it. If
    every bid failed there, the run reported bid_calls=0, a NaN failure rate and no
    [LLM_PLANE_DEAD] warning, because that check needs a minimum number of calls: a wholly
    dead LLM plane looked untouched."""
    from swarm.agents.llm.llm_agent import LlmAgent
    import inspect
    src = inspect.getsource(LlmAgent._llm_or_analytic_cost)
    body = src[src.index("try:"):]
    note_at = body.index("self._note_bid(")
    for later in ("job.to_dict(", "self._get_peer_context()", "self.bidder.score("):
        assert note_at < body.index(later), f"_note_bid must precede {later}"


def test_failures_can_never_outnumber_attempts():
    """With the attempt counted first this is an invariant, so a violation means the counter
    moved and the failure rate cannot be trusted — say so rather than emitting a rate above 1."""
    a = make_agent(1, [1, 2], lambda job, assignees: (_Agent(1), 1.0))
    for i in range(10):
        a._note_bid(f"j{i}")
        a._bid_failures += 1
    stats = a.bidding_stats()
    assert stats["bid_failures"] == stats["bid_calls"] == 10
    assert "bid_failures_exceed_calls" not in stats

    a._bid_failures += 1                      # only reachable if the ordering regressed
    assert a.bidding_stats()["bid_failures_exceed_calls"] is True
