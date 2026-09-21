"""The smaller items from the 2026-09-18 code review, §11.

Five separate things, each small and each biasing or breaking something specific:

* A Snow decision whose CAS won but whose object could not be read produced no assignment and
  was counted as a **finalize** — a flattering overcount, with the loss visible nowhere.
* PBFT reported `abandoned: 0`, a constant standing in for a quantity PBFT does not measure,
  next to Snow's measured count.
* `baselines/scheduler.py` ported feasibility from `ResourceAgent` without the `local`
  exclusion, so E7 could not run a converted workflow at all; its cost function had the §8
  doubling too, while its docstring promised an identical formula.
* Snow's `conflicts` dict was unbounded where PBFT capped it.
* `LlmAgent.selection_main` overrides the whole selection loop and had no data-predicate gate,
  so an LLM-agent run of a DAG-gated workflow ignored the DAG.
"""
import os
import sys
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)


# --------------------------------------------------------------------------- #
# PBFT does not report a quantity it cannot measure
# --------------------------------------------------------------------------- #

def _pbft():
    from swarm.consensus.engine import ConsensusEngine
    return ConsensusEngine(1, MagicMock(), MagicMock(), router=MagicMock())


def test_pbft_does_not_report_abandoned_at_all():
    """It was hard-coded 0. PBFT leaves a stuck object to the reselection timeout, so it has
    no count to give; a constant 0 beside Snow\'s measured value reads as \'PBFT abandoned
    nothing\', which compares a measurement against a placeholder."""
    stats = _pbft().consensus_stats()
    assert "abandoned" not in stats
    assert "finalize_lost" not in stats, "the CAS path is Snow\'s; PBFT has no CAS"


def test_pbft_still_reports_what_it_does_measure():
    stats = _pbft().consensus_stats()
    assert stats["protocol"] == "pbft"
    assert "finalized" in stats and "reproposals" in stats


def test_the_collector_reads_absent_as_absent_not_zero():
    from evaluation.collect import instrumentation_metrics
    row = instrumentation_metrics(
        {"1": {"instrumentation": {"consensus": {"protocol": "pbft", "finalized": 7}}}})
    assert row["consensus_finalized"] == 7
    assert row["consensus_abandoned"] is None
    assert row["consensus_finalize_lost"] is None


def test_the_collector_sums_them_when_snow_reports_them():
    from evaluation.collect import instrumentation_metrics
    row = instrumentation_metrics({
        "1": {"instrumentation": {"consensus": {"protocol": "snow", "finalized": 5,
                                                "abandoned": 2, "finalize_lost": 1}}},
        "2": {"instrumentation": {"consensus": {"protocol": "snow", "finalized": 3,
                                                "abandoned": 0, "finalize_lost": 0}}},
    })
    assert (row["consensus_abandoned"], row["consensus_finalize_lost"]) == (2, 1)


def test_a_measured_zero_is_reported_as_zero():
    """The other half of absent-vs-zero: a Snow run that abandoned nothing must say 0, not
    disappear."""
    from evaluation.collect import instrumentation_metrics
    row = instrumentation_metrics(
        {"1": {"instrumentation": {"consensus": {"protocol": "snow", "finalized": 9,
                                                 "abandoned": 0, "finalize_lost": 0}}}})
    assert row["consensus_abandoned"] == 0 and row["consensus_finalize_lost"] == 0


# --------------------------------------------------------------------------- #
# Snow conflicts are bounded, like PBFT\'s
# --------------------------------------------------------------------------- #

def test_snow_conflicts_are_bounded_like_pbfts():
    from swarm.consensus.engine import ConsensusEngine
    from swarm.consensus.gossip_engine import GossipConsensusEngine
    snow = GossipConsensusEngine(agent_id=1, host=MagicMock(), transport=MagicMock(),
                                 router=MagicMock())
    pbft = ConsensusEngine(1, MagicMock(), MagicMock(), router=MagicMock())
    assert snow._conflicts_max == pbft._conflicts_max

    for i in range(snow._conflicts_max + 500):
        snow._bump_conflict(f"j{i}")
    assert len(snow.conflicts) == snow._conflicts_max
    assert "j0" not in snow.conflicts, "the oldest entries must be the ones evicted"
    assert f"j{snow._conflicts_max + 499}" in snow.conflicts


def test_repeat_conflicts_on_one_object_do_not_consume_the_bound():
    from swarm.consensus.gossip_engine import GossipConsensusEngine
    snow = GossipConsensusEngine(agent_id=1, host=MagicMock(), transport=MagicMock(),
                                 router=MagicMock())
    for _ in range(50):
        snow._bump_conflict("j1")
    assert snow.conflicts == {"j1": 50}


# --------------------------------------------------------------------------- #
# the baseline can run a converted workflow
# --------------------------------------------------------------------------- #

def _bundle_job():
    from swarm.models.capacities import Capacities
    from swarm.models.data_node import DataNode
    from swarm.models.job import Job
    j = Job()
    j.job_id = "w1"
    j.capacities = Capacities(core=1, ram=2, disk=10)
    j.wall_time = 1.0
    j.add_incoming_data_dep(DataNode(name="local"))
    return j


class _SimAgent:
    dtns = {}

    def can_fit(self, caps):
        return True


def test_the_baseline_can_run_an_all_local_job():
    """`--dtn-names local` is the documented converter flag for a shared mount. The baseline
    rejected every such job, so E7 could not take a workflow bundle at all."""
    from baselines.scheduler import BaselineScheduler
    assert BaselineScheduler.is_feasible(_bundle_job(), _SimAgent()) is True


def test_the_baseline_does_not_double_an_all_local_cost():
    """Its docstring promises the agent\'s formula; after §8 that means no penalty here."""
    from swarm.models.capacities import Capacities
    from swarm.models.job import Job
    from baselines.scheduler import BaselineScheduler

    from baselines.scheduler import GreedyScheduler
    sched = GreedyScheduler.__new__(GreedyScheduler)   # concrete; compute_cost is on the base
    sched.cpu_weight, sched.ram_weight, sched.disk_weight, sched.gpu_weight = .4, .3, .3, .0
    sched.long_job_threshold = 20.0
    sched.connectivity_penalty_factor = 1.0

    agent = _SimAgent()
    agent.capacities = Capacities(core=8, ram=32, disk=500)

    plain = Job()
    plain.job_id = "p1"
    plain.capacities = Capacities(core=1, ram=2, disk=10)
    plain.wall_time = 1.0

    assert sched.compute_cost(_bundle_job(), agent) == pytest.approx(
        sched.compute_cost(plain, agent), abs=0.02)


# --------------------------------------------------------------------------- #
# the LLM loop honours the DAG
# --------------------------------------------------------------------------- #

class TestTheLlmLoopGatesOnDataPredicates:
    @staticmethod
    def _agent(ready):
        from swarm.agents.resource_agent import ResourceAgent
        a = ResourceAgent.__new__(ResourceAgent)
        a.logger = MagicMock()
        a.queues = MagicMock()
        a.moved = []
        a.queues.pending_queue.move_to_end = a.moved.append
        a._data_predicate_ready = lambda j: ready(j)
        return a

    def test_an_ungated_batch_passes_through_untouched(self):
        a = self._agent(lambda j: True)
        jobs = [MagicMock(), MagicMock()]
        assert a._gate_on_data_predicates(jobs) == jobs
        assert a.moved == []

    def test_a_gated_job_is_dropped_and_requeued(self):
        blocked, ready = MagicMock(), MagicMock()
        a = self._agent(lambda j: j is ready)
        assert a._gate_on_data_predicates([blocked, ready]) == [ready]
        assert a.moved == [blocked]

    def test_two_equal_jobs_do_not_take_each_other_out(self):
        """Filtering by `in` compares with __eq__; jobs that compare equal would then both
        vanish when only one was gated."""
        class _Eq:
            def __eq__(self, other):
                return True
            def __hash__(self):
                return 1
        blocked, ready = _Eq(), _Eq()
        a = self._agent(lambda j: j is ready)
        out = a._gate_on_data_predicates([blocked, ready])
        assert len(out) == 1 and out[0] is ready

    def test_the_llm_loop_calls_the_gate_before_designation(self):
        """Parsed rather than run: the loop is a `while not self.shutdown` with live queues.
        The order matters — gating after designation would spend an LLM call on a job whose
        parents have not produced their files."""
        import ast
        import inspect
        from swarm.agents.llm.llm_agent import LlmAgent

        src = inspect.getsource(LlmAgent.selection_main)
        tree = ast.parse(src.lstrip() if not src.startswith("    ") else
                         "\n".join(line[4:] if line.startswith("    ") else line
                                   for line in src.splitlines()))
        calls = [n.func.attr for n in ast.walk(tree)
                 if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)]
        assert "_gate_on_data_predicates" in calls, "the LLM loop must gate on the DAG"
        assert calls.index("_gate_on_data_predicates") < calls.index("_designate_bidders")
