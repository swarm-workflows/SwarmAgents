"""Bandit herding (2026-10-12): per-arm LinUCB models and the relative load guard.

Pilot e669e79c, PBFT+LinUCB at Hier-90 G=3: coordinator 90 sent 1,151 of 1,292 jobs to one of
its three groups at 762 in flight and 5 % CPU headroom, the other two idle; every arm read
reward 1.0 (no failures injected, `reward.shaped: false`). The shared ridge model fits the
features its choices have in common — the chosen group's own load — so the idle groups score
lower and the choice reinforces itself. `mab.linucb.model: disjoint` keeps one model per arm,
`mab.load_slack` keeps the choice among the near-least-loaded groups, and the LinUCB campaign
configs turn on `reward.shaped`.
"""
from unittest.mock import MagicMock

import pytest

from swarm.models.capacities import Capacities
from swarm.models.job import Job
from swarm.rl.bandit import DisjointLinUCBPolicy, LinUCBPolicy
from swarm.rl.context import ContextExtractor, GroupSnapshot
from swarm.rl.mab_manager import MABManager


def _job(i=1):
    j = Job()
    j.job_id = str(i)
    j.capacities = Capacities(core=1, ram=2, disk=1)
    j.wall_time = 30.0
    return j


def _scores_after_feeding_one_arm(policy):
    """Score idle arm 7 before and after training only arm 6 (+1 every time, at high load)."""
    ext = ContextExtractor({})
    busy = GroupSnapshot(active_children=9, cpu_headroom=0.05, ram_headroom=0.3, inflight=500)
    idle = GroupSnapshot(active_children=9)
    ctx = ext.build(_job(0), [6, 7], {6: busy, 7: idle})
    policy._ensure_dim(ctx[7])
    before = policy._scores([7], {7: ctx[7]})[7]
    for i in range(50):
        policy.update(6, 1.0, context=ext.build(_job(i), [6], {6: busy})[6])
    return before, policy._scores([7], {7: ctx[7]})[7]


class TestDisjointModel:
    def test_the_shared_model_moves_an_untried_arms_score(self):
        """The mechanism behind the herding: one arm's outcomes re-score every other arm.
        (The herding itself emerges only as load drifts through a burst — reproduced offline,
        not in a static test.)"""
        before, after = _scores_after_feeding_one_arm(LinUCBPolicy())
        assert after != pytest.approx(before)

    def test_per_arm_models_leave_it_alone(self):
        before, after = _scores_after_feeding_one_arm(DisjointLinUCBPolicy())
        assert after == pytest.approx(before)

    def test_one_arms_history_never_moves_anothers_score(self):
        ext = ContextExtractor({})
        p = DisjointLinUCBPolicy()
        ctx = ext.build(_job(), [6, 7], {6: GroupSnapshot(), 7: GroupSnapshot()})
        before = p._scores([7], {7: ctx[7]}) if p.dim else None
        p.update(6, 1.0, context=ctx[6])
        after = p._scores([7], {7: ctx[7]})
        fresh = DisjointLinUCBPolicy()
        fresh._ensure_dim(ctx[7])
        assert after == fresh._scores([7], {7: ctx[7]})
        assert before is None

    def test_state_round_trips(self):
        ext = ContextExtractor({})
        p = DisjointLinUCBPolicy(schema_version="s")
        ctx = ext.build(_job(), [6, 7], {6: GroupSnapshot(), 7: GroupSnapshot()})
        p.update(6, 0.5, context=ctx[6])
        q = DisjointLinUCBPolicy(schema_version="s")
        q.load_state(p.get_state())
        assert q._scores([6, 7], ctx) == p._scores([6, 7], ctx)

    def test_a_shared_models_state_is_not_loaded(self):
        ext = ContextExtractor({})
        shared = LinUCBPolicy(schema_version="s")
        ctx = ext.build(_job(), [6], {6: GroupSnapshot()})
        shared.update(6, 1.0, context=ctx[6])
        p = DisjointLinUCBPolicy(schema_version="s")
        p.load_state(shared.get_state())
        assert not getattr(p, "_arm_A_inv", {})


def _mgr(slack=0, model=None, snaps=None):
    cfg = {"algorithm": "linucb", "load_slack": slack}
    if model:
        cfg["linucb"] = {"model": model}
    return MABManager(agent_id=90, child_groups=[6, 7, 8], config=cfg, repository=MagicMock(),
                      group_snapshot_provider=(lambda: snaps or {}))


class TestManager:
    def test_model_selects_the_policy(self):
        assert type(_mgr(model="disjoint").policy) is DisjointLinUCBPolicy
        assert type(_mgr().policy) is LinUCBPolicy

    def test_an_unknown_model_raises(self):
        with pytest.raises(ValueError):
            _mgr(model="per-arm")

    def test_a_negative_slack_raises(self):
        with pytest.raises(ValueError):
            _mgr(slack=-1)

    def test_the_guard_keeps_the_choice_among_the_least_loaded(self):
        snaps = {6: GroupSnapshot(inflight=700), 7: GroupSnapshot(inflight=3),
                 8: GroupSnapshot(inflight=10)}
        m = _mgr(slack=9, snaps=snaps)
        for i in range(30):
            assert m.select_groups([6, 7, 8], job=_job(i), top_k=1)[0] in (7, 8)
        assert m.load_guard_narrowed == 30

    def test_slack_zero_is_off(self):
        snaps = {6: GroupSnapshot(inflight=700), 7: GroupSnapshot(inflight=3)}
        m = _mgr(slack=0, snaps=snaps)
        assert m._load_eligible([6, 7], 1, snaps) == [6, 7]
        assert m.load_guard_decisions == 0

    def test_the_guard_always_leaves_top_k(self):
        snaps = {6: GroupSnapshot(inflight=700), 7: GroupSnapshot(inflight=3),
                 8: GroupSnapshot(inflight=400)}
        m = _mgr(slack=9, snaps=snaps)
        assert m._load_eligible([6, 7, 8], 2, snaps) == [7, 8]

    def test_stats_report_the_guard(self):
        stats = _mgr(slack=9).get_stats()
        assert (stats["load_slack"], stats["load_guard_decisions"]) == (9.0, 0)
