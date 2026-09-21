"""`local` is a site, not a data transfer node, and cost was the one place that forgot
(code review §8).

Feasibility, `_job_sig`, `fleet_sizing` and the runner's shape summary all subtracted `local`
from a job's required DTNs. `compute_job_cost` rebuilt the set inline and kept it, and no agent
holds a DTN by that name, so it scored 0.0 — the worst connectivity there is — on every agent.
`avg_conn` went to 0 and the penalty to `1 + connectivity_penalty_factor`: at the shipped factor
of 1.0, **every cost in a converted-workflow run was exactly doubled**. Feasibility was saying
"this is not a DTN" while cost said "this is a DTN nobody can reach".

`_get_child_groups_for_job` had the same inline set with a worse outcome: an all-local job
matched no child group at all, fell through to the "delegate to all active groups" fallback,
and logged two warnings per job — one of them blaming the feasibility check. The DTN capability
filter was therefore inert for every job in a hierarchical workflow cell, so the bandit saw the
whole fleet as candidates instead of the capable subset.

The set now has one definition, `Job.required_dtns`, and nothing reads the cache behind it.
"""
import os
import sys
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from swarm.agents.resource_agent import ResourceAgent  # noqa: E402
from swarm.models.capacities import Capacities  # noqa: E402
from swarm.models.data_node import DataNode  # noqa: E402
from swarm.models.job import Job  # noqa: E402

TOTAL = Capacities(core=8, ram=32, disk=500)


def _job(in_names=(), out_names=(), cores=2, ram=8, disk=50, wall=1.0):
    j = Job()
    j.job_id = "j1"
    j.capacities = Capacities(core=cores, ram=ram, disk=disk)
    j.wall_time = wall
    for n in in_names:
        j.add_incoming_data_dep(DataNode(name=n))
    for n in out_names:
        j.add_outgoing_data_dep(DataNode(name=n))
    return j


def _dtns(**scores):
    return {n: DataNode(name=n, connectivity_score=s) for n, s in scores.items()}


def _agent(factor=1.0):
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = MagicMock()
    a.cpu_weight, a.ram_weight, a.disk_weight = 0.4, 0.3, 0.3
    a.gpu_weight, a.qpu_weight = 0.0, 0.0
    a.long_job_threshold = 20.0
    a.connectivity_penalty_factor = factor
    a.quantum_penalty_factor = 1.0
    a.split_comm_penalty_factor = 0.0
    return a


def _cost(agent, job, dtns):
    return agent.compute_job_cost(job=job, total=TOTAL, dtns=dtns)


# --------------------------------------------------------------------------- #
# the set itself
# --------------------------------------------------------------------------- #

def test_local_is_not_a_required_dtn():
    assert _job(in_names=["local"]).required_dtns() == frozenset()


def test_real_dtns_survive_beside_local():
    assert _job(in_names=["local", "dtn1"], out_names=["dtn2"]).required_dtns() == \
        frozenset({"dtn1", "dtn2"})


def test_the_set_is_cached_on_the_job():
    j = _job(in_names=["dtn1"])
    assert j.required_dtns() is j.required_dtns()


def test_an_empty_set_is_not_recomputed():
    """`frozenset()` is falsy. A cache guarded by truthiness would rebuild it on every
    (job, agent) pair in the cost matrix."""
    j = _job(in_names=["local"])
    first = j.required_dtns()
    j.data_in.append(DataNode(name="dtn9"))      # a later mutation must not leak in
    assert j.required_dtns() is first


# --------------------------------------------------------------------------- #
# the cost
# --------------------------------------------------------------------------- #

def test_an_all_local_job_pays_no_connectivity_penalty():
    """The defect: this used to be the no-DTN cost multiplied by `1 + factor`."""
    agent = _agent(factor=1.0)
    assert _cost(agent, _job(in_names=["local"]), _dtns()) == \
        pytest.approx(_cost(agent, _job(), _dtns()))


def test_the_doubling_is_what_it_was():
    """Pin the magnitude the finding claims, so 'shifted' cannot drift into 'negligible'."""
    agent = _agent(factor=1.0)
    clean = _cost(agent, _job(), _dtns())
    old_style = clean * (1 + agent.connectivity_penalty_factor * (1 - 0.0))
    assert old_style == pytest.approx(2 * clean)


def test_a_real_dtn_is_still_priced():
    agent = _agent(factor=1.0)
    good = _cost(agent, _job(in_names=["dtn1"]), _dtns(dtn1=0.9))
    poor = _cost(agent, _job(in_names=["dtn1"]), _dtns(dtn1=0.1))
    assert poor > good


def test_a_dtn_the_agent_does_not_hold_is_still_the_worst_score():
    """Removing `local` must not soften a genuinely unreachable DTN: an unheld name still
    scores 0.0 and still doubles the cost at factor 1.0. The tolerance is one step of the
    `round(cost, 2)` the function ends with, applied once to each side."""
    agent = _agent(factor=1.0)
    held = _cost(agent, _job(in_names=["dtn1"]), _dtns(dtn1=1.0))
    unheld = _cost(agent, _job(in_names=["dtn1"]), _dtns(dtn2=1.0))
    assert unheld == pytest.approx(2 * held, abs=0.02)


def test_local_no_longer_drags_down_a_mixed_job():
    """A mixed job averaged the real DTN's score with local's 0.0, halving it. This is the
    case where the old behaviour was NOT a uniform shift: the penalty depended on the real
    score, so two agents differing in both base cost and connectivity could reorder."""
    agent = _agent(factor=1.0)
    mixed = _cost(agent, _job(in_names=["local", "dtn1"]), _dtns(dtn1=0.8))
    alone = _cost(agent, _job(in_names=["dtn1"]), _dtns(dtn1=0.8))
    assert mixed == pytest.approx(alone)


# --------------------------------------------------------------------------- #
# the child-group filter
# --------------------------------------------------------------------------- #

class TestChildGroupFilter:
    @staticmethod
    def _coordinator(groups=(1, 2)):
        a = ResourceAgent.__new__(ResourceAgent)
        a.logger = MagicMock()
        a.topology = MagicMock()
        a.topology.children = list(groups)
        a._get_active_child_groups = lambda: list(groups)
        a.children = MagicMock()
        a.children.values = lambda: [
            MagicMock(group=g, dtns={"dtn1": DataNode(name="dtn1")}) for g in groups]
        return a

    def test_an_all_local_job_reaches_every_active_group(self):
        a = self._coordinator()
        assert a._get_child_groups_for_job(_job(in_names=["local"])) == [1, 2]

    def test_and_does_not_warn_that_feasibility_was_wrong(self):
        """Two warnings per job on a 25k-job replay, one of them blaming the wrong component."""
        a = self._coordinator()
        a._get_child_groups_for_job(_job(in_names=["local"]))
        assert a.logger.warning.call_args_list == []

    def test_a_real_requirement_still_filters(self):
        a = self._coordinator()
        assert a._get_child_groups_for_job(_job(in_names=["dtn1"])) == [1, 2]
        assert a._get_child_groups_for_job(_job(in_names=["dtn7"])) == []


def test_nothing_reads_the_cache_behind_the_accessor():
    """The cache is populated lazily, so a direct reader that runs first raises AttributeError.
    Keeping every read inside `Job.required_dtns` removes that ordering dependency entirely."""
    import pathlib
    for path in pathlib.Path(REPO, "swarm").rglob("*.py"):
        if path.name == "job.py" and path.parent.name == "models":
            continue
        assert "_required_dtns_cache" not in path.read_text(), path
