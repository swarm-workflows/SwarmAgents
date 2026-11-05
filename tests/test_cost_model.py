"""`job_selection.cost_model`: the `utilization` model that replaced `capacity` on 2026-10-16.

The fc9eb220 campaign (docs/results/e0-e1-fc9eb220/README.md §9) showed that pricing a job by its
demand over an agent's TOTAL capacity makes the largest agent the cheapest whatever it holds: a
2-core leaf priced a job at 29x a 32-core leaf, the projected-load factor never exceeded 1.32x at a
coordinator, and so in every cell 54-74 of 81-87 leaves never ran a job and, under Snow, the four
smallest coordinator groups never won one.

`utilization` prices the agent's committed work (scheduled + backlog) into the cost. These tests
pin what that must and must not change:

* an idle agent prices EXACTLY as under `capacity`, so the two differ only by load;
* a free small agent beats a saturated large one, and backlog counts like scheduled work;
* resources the job does not use do not make it dearer;
* the analytic load factor is 1.0 (load is not counted twice) and the cache key sees load;
* a batch priced from one snapshot is spread, not handed whole to one agent;
* a coordinator counts delegated-but-unpicked jobs and its children's backlogs;
* gossip carries it, from the agent's full info (a coordinator's own ready queue is empty);
* the campaign's coordinator tier, replayed, reaches every group.
"""
import os
import sys
import types
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from swarm.agents.cost_scale import CostScale  # noqa: E402
from swarm.agents.resource_agent import (COST_MODEL_DEFAULT, COST_MODELS,  # noqa: E402
                                         ResourceAgent, resolve_cost_model)
from swarm.models.agent_info import AgentInfo  # noqa: E402
from swarm.models.capacities import Capacities  # noqa: E402
from swarm.models.job import Job  # noqa: E402
from swarm.selection.engine import SelectionEngine  # noqa: E402


# --------------------------------------------------------------------------- #
# fixtures
# --------------------------------------------------------------------------- #

def _agent(model=None, agent_id=1):
    """A ResourceAgent with the real cost, selection and backlog paths. `model=None` leaves the
    class attribute in place, i.e. the shipped default."""
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = MagicMock()
    a.agent_id = agent_id
    a.cpu_weight, a.ram_weight, a.disk_weight, a.gpu_weight, a.qpu_weight = 0.4, 0.3, 0.2, 0.1, 0.0
    a.long_job_threshold = 20.0
    a.connectivity_penalty_factor = 1.0
    a.quantum_penalty_factor = 1.0
    a.split_comm_penalty_factor = 0.0
    a.measurement_layer = None
    if model is not None:
        a.cost_model = model
    a.selector = SelectionEngine(
        feasible=lambda job, ag: True,
        cost=a._cost_job_on_agent,
        candidate_key=a._job_sig,
        assignee_key=a._agent_sig,
        candidate_version=lambda job: 0,
        assignee_version=lambda ag: 0,
        cache_enabled=True,
    )
    return a


def _job(job_id="j1", core=1.0, ram=4.0, disk=1.0, gpu=0, wall=10.0):
    j = Job()
    j.job_id = job_id
    j.capacities = Capacities(core=core, ram=ram, disk=disk, gpu=gpu)
    j.wall_time = wall
    return j


def _info(agent_id, core, ram=None, disk=None, gpu=0, alloc=None, backlog=None):
    info = AgentInfo(agent_id=agent_id,
                     capacities=Capacities(core=core, ram=ram if ram is not None else 4 * core,
                                           disk=disk if disk is not None else 100 * core, gpu=gpu),
                     dtns=[])
    if alloc is not None:
        info.capacity_allocations = alloc
    if backlog is not None:
        info.capacity_backlog = backlog
    return info


def _cost(agent, job, info):
    return agent._cost_job_on_agent(job, info)


# --------------------------------------------------------------------------- #
# the key
# --------------------------------------------------------------------------- #

class TestResolve:
    def test_default_is_utilization(self):
        assert resolve_cost_model({}) == "utilization" == COST_MODEL_DEFAULT
        assert resolve_cost_model(None) == "utilization"

    def test_the_class_default_is_the_resolved_default(self):
        """A test double built with __new__ must exercise the shipped model, not a stale one."""
        assert ResourceAgent.cost_model == resolve_cost_model({})

    def test_capacity_is_accepted_case_insensitively(self):
        assert resolve_cost_model({"cost_model": "Capacity"}) == "capacity"

    def test_unknown_value_is_refused(self):
        with pytest.raises(ValueError, match="cost_model"):
            resolve_cost_model({"cost_model": "utilisation"})

    def test_every_shipped_config_names_a_valid_model(self):
        import glob
        import yaml
        files = [os.path.join(REPO, "config_swarm_multi.yml")] + \
            sorted(glob.glob(os.path.join(REPO, "campaigns", "config_*.yml")))
        for f in files:
            cfg = yaml.safe_load(open(f))
            assert (cfg["job_selection"] or {}).get("cost_model") in COST_MODELS, f


# --------------------------------------------------------------------------- #
# the cost itself
# --------------------------------------------------------------------------- #

class TestCost:
    def test_an_idle_agent_prices_exactly_as_under_capacity(self):
        """The two models differ only by load, so an idle fleet places identically."""
        job = _job(core=2, ram=8, disk=5)
        for info in (_info(1, 2), _info(2, 32, gpu=4), _info(3, 8)):
            assert _cost(_agent("utilization"), job, info) == \
                pytest.approx(_cost(_agent("capacity"), job, info))

    def test_a_free_small_agent_beats_a_saturated_large_one(self):
        """The defect: under `capacity` the 32-core agent is cheapest however full it is."""
        job = _job()
        small = _info(1, 2)
        big_full = _info(2, 32, alloc=Capacities(core=30, ram=120, disk=30))
        util, cap = _agent("utilization"), _agent("capacity")
        assert _cost(util, job, small) < _cost(util, job, big_full)
        assert _cost(cap, job, small) > _cost(cap, job, big_full)

    def test_capacity_needs_implausible_load_to_flip(self):
        """What §9 measured: even with the projected-load factor at its full-load value of 2.0,
        `capacity` still prices the 2-core agent far above the 32-core one."""
        job = _job()
        cap = _agent("capacity")
        small, big = _info(1, 2), _info(2, 32)
        big.load, big.proposed_load = 100.0, 0.0
        assert _cost(cap, job, small) > _cost(cap, job, big) * cap._projected_load_factor(big) * 5

    def test_backlog_counts_like_scheduled_work(self):
        job = _job()
        a = _agent("utilization")
        scheduled = _info(1, 8, alloc=Capacities(core=4, ram=16, disk=4))
        queued = _info(1, 8, backlog=Capacities(core=4, ram=16, disk=4))
        idle = _info(1, 8)
        assert _cost(a, job, scheduled) == pytest.approx(_cost(a, job, queued))
        assert _cost(a, job, queued) > _cost(a, job, idle)

    def test_a_queue_beyond_capacity_keeps_raising_the_price(self):
        """Committed work past 100 % is a queue; the cost must keep climbing through it."""
        job = _job()
        a = _agent("utilization")
        costs = [_cost(a, job, _info(1, 8, backlog=Capacities(core=c, ram=4 * c, disk=c)))
                 for c in (0, 8, 16, 32)]
        assert costs == sorted(costs) and len(set(costs)) == 4

    def test_resources_the_job_does_not_use_do_not_make_it_dearer(self):
        job = _job(gpu=0)
        a = _agent("utilization")
        gpu_busy = _info(1, 8, gpu=4, alloc=Capacities(gpu=4))
        gpu_free = _info(1, 8, gpu=4)
        assert _cost(a, job, gpu_busy) == pytest.approx(_cost(a, job, gpu_free))

    def test_capacity_ignores_committed_work_entirely(self):
        job = _job()
        a = _agent("capacity")
        assert a._committed(_info(1, 8, alloc=Capacities(core=8))) is None
        assert _cost(a, job, _info(1, 8, alloc=Capacities(core=8, ram=32))) == \
            pytest.approx(_cost(a, job, _info(1, 8)))


class TestLoadFactorAndCache:
    def test_analytic_factor_is_one_under_utilization(self):
        info = _info(1, 8)
        info.load, info.proposed_load = 90.0, 50.0
        assert _agent("utilization")._selection_load_factor(info) == 1.0
        cap = _agent("capacity")
        assert cap._selection_load_factor(info) == cap._projected_load_factor(info) > 1.0

    def test_wire_factor_follows_the_plane(self):
        a = _agent("utilization")
        info = _info(1, 8)
        info.load = 50.0
        assert a._wire_load_factor(info, CostScale.ANALYTIC) == 1.0
        assert a._wire_load_factor(info, CostScale.LLM) == a._projected_load_factor(info)

    def test_llm_agent_answers_in_its_matrix_unit(self):
        """An LLM agent's selection matrix applies the projected factor to every entry, its
        unloaded analytic fallback included, so its wire answers must too."""
        from swarm.agents.llm.llm_agent import LlmAgent
        a = LlmAgent.__new__(LlmAgent)
        a.cost_model = "utilization"
        a.llm_snow_cost_fallback = "yield"
        info = _info(1, 8)
        info.load = 50.0
        expected = a._projected_load_factor(info)
        assert a._wire_load_factor(info, CostScale.ANALYTIC) == expected
        assert a._wire_load_factor(info, CostScale.LLM) == expected
        a.llm_snow_cost_fallback = "analytic"
        assert a._wire_load_factor(info, CostScale.ANALYTIC) == 1.0

    def test_cache_key_sees_committed_work_under_utilization(self):
        """Without this a cached idle price was served for the cache TTL (60 s)."""
        a = _agent("utilization")
        assert a._agent_sig(_info(1, 8)) != a._agent_sig(_info(1, 8, backlog=Capacities(core=1)))
        cap = _agent("capacity")
        assert cap._agent_sig(_info(1, 8)) == cap._agent_sig(_info(1, 8, backlog=Capacities(core=1)))

    def test_a_cached_price_tracks_load(self):
        a = _agent("utilization")
        job = _job()
        idle = _info(1, 8)
        before = a.selector.compute_cost_matrix(assignees=[idle], candidates=[job])[0, 0]
        busy = _info(1, 8, alloc=Capacities(core=6, ram=24, disk=6))
        after = a.selector.compute_cost_matrix(assignees=[busy], candidates=[job])[0, 0]
        assert after > before


# --------------------------------------------------------------------------- #
# batch assignment
# --------------------------------------------------------------------------- #

class TestBatch:
    def _fleet(self):
        return [_info(1, 2), _info(2, 4), _info(3, 32)]

    def test_capacity_hands_the_whole_batch_to_the_largest(self):
        a = _agent("capacity")
        jobs = [_job(f"j{i}") for i in range(20)]
        picks = [ag.agent_id for ag, _ in a._assign_batch(self._fleet(), jobs)]
        assert set(picks) == {3}

    def test_utilization_spreads_one_snapshot_over_the_fleet(self):
        a = _agent("utilization")
        jobs = [_job(f"j{i}") for i in range(20)]
        picks = [ag.agent_id for ag, _ in a._assign_batch(self._fleet(), jobs)]
        assert set(picks) == {1, 2, 3}
        assert picks.count(3) > picks.count(2) > 0      # proportional, not round-robin

    def test_a_single_job_batch_is_priced_as_one_column(self):
        """`wire_cost_for_job` mirrors a one-job pass; the sequential path must agree with it."""
        a = _agent("utilization")
        fleet = self._fleet()
        [(chosen, cost)] = a._assign_batch(fleet, [_job()])
        best = min(_cost(a, _job(), f) for f in fleet)
        assert cost == pytest.approx(best)

    def test_the_real_agent_state_is_untouched(self):
        a = _agent("utilization")
        fleet = self._fleet()
        a._assign_batch(fleet, [_job(f"j{i}") for i in range(10)])
        assert all(float(f.capacity_backlog.core or 0) == 0 for f in fleet)


# --------------------------------------------------------------------------- #
# backlog and the coordinator's view
# --------------------------------------------------------------------------- #

def _with_queues(a, selected=(), delegated=None, children=False):
    a.queues = types.SimpleNamespace(selected_queue=types.SimpleNamespace(gets=lambda: list(selected)))
    a.topology = types.SimpleNamespace(children=[1] if children else [], level=1 if children else 0,
                                       group=0)
    a.delegated_jobs = types.SimpleNamespace(items=lambda: list((delegated or {}).items()))
    return a


class TestBacklog:
    def test_a_leaf_backlog_is_its_selected_queue(self):
        a = _with_queues(_agent(), selected=[_job("a", core=2, ram=8), _job("b", core=1, ram=2)])
        b = a.compute_backlog()
        assert (b.core, b.ram) == (3, 10)

    def test_a_coordinator_counts_delegations_until_a_child_picks_them_up(self):
        delegated = {
            "x": {"groups": [0], "capacities": {"core": 4, "ram": 16, "disk": 1}},
            "y": {"groups": [0], "capacities": {"core": 8, "ram": 32, "disk": 1}, "picked": True},
            "z": {"groups": [1]},     # a record from before capacities were tracked
        }
        a = _with_queues(_agent(), selected=[_job("s", core=1, ram=4)], delegated=delegated,
                         children=True)
        b = a.compute_backlog()
        assert (b.core, b.ram) == (5, 20)

    def test_backlog_round_trips_through_redis_shape(self):
        info = _info(1, 8, backlog=Capacities(core=2, ram=4, disk=1))
        back = AgentInfo.from_dict(info.to_dict())
        assert (back.capacity_backlog.core, back.capacity_backlog.ram) == (2, 4)
        assert AgentInfo.from_dict({"agent_id": 3}).capacity_backlog.core == 0


class TestGossip:
    def test_utilization_is_published_from_the_full_info(self):
        """A coordinator's own ready queue is empty while its capacity is its children's sum;
        publishing from it read 0 % for every coordinator."""
        info = _info(9, 100, alloc=Capacities(core=25, ram=100, disk=50),
                     backlog=Capacities(core=50, ram=0, disk=0))
        got = ResourceAgent._gossip_utilization(info)
        assert got["cpu_util"] == pytest.approx(25.0)
        assert got["cpu_backlog"] == pytest.approx(50.0)
        assert got["gpu_util"] == 0.0

    def _overlay(self, model):
        a = _agent(model)
        a.agent_id = 1
        peer = _info(2, 8)
        a.neighbor_map = {2: peer}
        entry = types.SimpleNamespace(agent_id=2, load=40.0, cpu_util=50.0, ram_util=25.0,
                                      disk_util=0.0, gpu_util=0.0, cpu_backlog=100.0,
                                      ram_backlog=0.0, disk_backlog=0.0, gpu_backlog=0.0)
        a.gossip = types.SimpleNamespace(snapshot=lambda: [entry])
        a._apply_gossip_overlay()
        return peer

    def test_overlay_refreshes_allocations_and_backlog_under_utilization(self):
        peer = self._overlay("utilization")
        assert peer.load == 40.0
        assert peer.capacity_allocations.core == pytest.approx(4.0)
        assert peer.capacity_backlog.core == pytest.approx(8.0)

    def test_overlay_leaves_capacity_runs_as_they_were(self):
        peer = self._overlay("capacity")
        assert peer.load == 40.0
        assert float(peer.capacity_allocations.core or 0) == 0.0


# --------------------------------------------------------------------------- #
# the campaign, replayed
# --------------------------------------------------------------------------- #

#: fc9eb220 Hier-90 analytic: each coordinator's group as it advertises it (README §9.2).
CAMPAIGN_GROUPS = {82: (44, 176, 2650, 8), 83: (52, 208, 3150, 16), 84: (92, 368, 4700, 20),
                   85: (52, 208, 3150, 8), 86: (66, 264, 4050, 12), 87: (78, 312, 4800, 16),
                   88: (72, 288, 3400, 12), 89: (68, 272, 4200, 20), 90: (52, 208, 3150, 8)}


def _replay(model, n_jobs=300):
    """Jobs arrive one at a time and nothing completes; each goes to the cheapest coordinator,
    which is the one whose Snow instance every peer defers to. Returns jobs won per coordinator."""
    a = _agent(model)
    held = {c: Capacities() for c in CAMPAIGN_GROUPS}
    won = {c: 0 for c in CAMPAIGN_GROUPS}
    for i in range(n_jobs):
        job = _job(f"j{i}")
        infos = {c: _info(c, core, ram, disk, gpu, alloc=held[c])
                 for c, (core, ram, disk, gpu) in CAMPAIGN_GROUPS.items()}
        costs = {c: _cost(a, job, info) * a._selection_load_factor(info)
                 for c, info in infos.items()}
        best = min(costs, key=lambda c: (costs[c], c))
        won[best] += 1
        held[best] += job.capacities
    return won


def test_the_campaign_coordinator_tier_reaches_every_group_under_utilization():
    won = _replay("utilization")
    assert all(n > 0 for n in won.values()), won
    for c in (82, 83, 85, 90):                     # the four that never won in fc9eb220
        assert won[c] > 0


def test_capacity_reproduces_the_campaign_concentration():
    won = _replay("capacity")
    assert won[84] == 300
    assert all(won[c] == 0 for c in (82, 83, 85, 90))


# --------------------------------------------------------------------------- #
# reservations survive consensus (stop-time review, 2026-10-16)
# --------------------------------------------------------------------------- #

class _Ids(list):
    """Membership-only stand-in for the ready/selected queues (`job_id in queue`)."""


def _live(a, info, pending, outgoing=(), level=0, matrix="self"):
    """`a` with a pending queue, an engine whose `outgoing` holds `outgoing`, and `info` as its
    own snapshot — what `wire_cost_for_job` and the batch path read."""
    a.agent_id = info.agent_id
    a.last_agent_info = info
    a.topology = types.SimpleNamespace(level=level, group=0, children=[])
    a.coordinator_cost_matrix = matrix
    a.queues = types.SimpleNamespace(pending_queue=types.SimpleNamespace(get=pending.get),
                                     ready_queue=_Ids(), selected_queue=_Ids())
    out = list(outgoing)
    a.engine = types.SimpleNamespace(outgoing=types.SimpleNamespace(objects=lambda: list(out)))
    a.is_job_feasible = lambda job, ag: True
    return a, out


class TestReservationsSurviveConsensus:
    def test_open_proposals_raise_the_price_of_other_jobs_not_their_own(self):
        jobs = {f"j{i}": _job(f"j{i}") for i in range(20)}
        a, _ = _live(_agent(), _info(3, 32), jobs, outgoing=[f"j{i}" for i in range(17)])
        idle, _ = _live(_agent(), _info(3, 32), jobs)
        # the 18th job is dearer on an agent holding 17 open proposals ...
        assert a.wire_cost_for_job("j17") > idle.wire_cost_for_job("j17")
        # ... and a proposed job is not charged for itself (the §1 undercut)
        own = a.wire_cost_for_job("j0")
        assert own == pytest.approx(a.proposal_cost(jobs["j0"], _cost(
            _agent(), jobs["j0"], _info(3, 32, backlog=Capacities(core=16, ram=64, disk=16)))))

    def test_a_loaded_proposer_defers_to_the_small_initiator(self):
        """The review's scenario: the large agent has just proposed 17 jobs of a batch. Asked
        about the 18th, which the batch gave a 2-core agent, it must answer above the small
        agent's advertised price, so its Snow vote goes to the initiator."""
        jobs = {f"j{i}": _job(f"j{i}") for i in range(20)}
        big, _ = _live(_agent(), _info(3, 32), jobs, outgoing=[f"j{i}" for i in range(17)])
        small, _ = _live(_agent(), _info(1, 2), jobs, outgoing=["j17"])
        assert big.wire_cost_for_job("j17") > small.wire_cost_for_job("j17")

    def test_proposal_prices_equal_wire_prices_once_the_batch_is_out(self):
        """Every job this agent proposes from a batch is advertised at exactly what it will
        answer for it once all of them are open proposals."""
        jobs = {f"j{i}": _job(f"j{i}") for i in range(12)}
        own = _info(3, 32)
        a, outgoing = _live(_agent(), own, jobs)
        fleet = [_info(1, 2), _info(2, 4), own]
        out = a._assign_batch(fleet, list(jobs.values()))
        mine = [(job, cost) for job, (chosen, cost) in zip(jobs.values(), out)
                if chosen is not None and chosen.agent_id == 3]
        assert len(mine) > 1
        outgoing.extend(job.job_id for job, _ in mine)
        a.selector.clear_caches()
        for job, cost in mine:
            assert a.proposal_cost(job, cost) == pytest.approx(a.wire_cost_for_job(job.job_id))

    def test_coordinators_under_self_do_not_count_proposals(self):
        """Every coordinator proposes every job under `self`; counting them charges each the
        whole tier's pending load, which favours the largest group again."""
        jobs = {f"j{i}": _job(f"j{i}") for i in range(20)}
        coord, _ = _live(_agent(), _info(84, 92), jobs, outgoing=list(jobs), level=1)
        idle, _ = _live(_agent(), _info(84, 92), jobs, level=1)
        assert coord.wire_cost_for_job("j5") == idle.wire_cost_for_job("j5")
        peers, _ = _live(_agent(), _info(84, 92), jobs, outgoing=list(jobs), level=1,
                         matrix="peers")
        assert peers.wire_cost_for_job("j5") > idle.wire_cost_for_job("j5")

    def test_capacity_model_ignores_open_proposals(self):
        jobs = {f"j{i}": _job(f"j{i}") for i in range(20)}
        a, _ = _live(_agent("capacity"), _info(3, 32), jobs, outgoing=list(jobs))
        idle, _ = _live(_agent("capacity"), _info(3, 32), jobs)
        assert a.wire_cost_for_job("j5") == idle.wire_cost_for_job("j5")
