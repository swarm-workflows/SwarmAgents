"""Bandit herding (2026-10-12): per-arm LinUCB models and the relative load guard.

Pilot e669e79c, PBFT+LinUCB at Hier-90 G=3: coordinator 90 sent 1,151 of 1,292 jobs to one of
its three groups at 762 in flight and 5 % CPU headroom, the other two idle; every arm read
reward 1.0 (no failures injected, `reward.shaped: false`). The shared ridge model fits the
features its choices have in common — the chosen group's own load — so the idle groups score
lower and the choice reinforces itself. `mab.linucb.model: disjoint` keeps one model per arm,
`mab.load_slack` keeps the choice among the near-least-loaded groups, and the LinUCB campaign
configs turn on `reward.shaped`.
"""
import collections
import heapq
import random
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


def _burst(model, slack, shaped, seed, n=600, span=120.0, slots=36, groups=(6, 7, 8)):
    """Route a seeded burst through the REAL `MABManager.select_groups` / `report_outcome` path
    (policy, load guard, reward shaping) into three equal groups of `slots` execution slots.
    Returns (busiest group's share, p95 delegation-to-completion latency). Deterministic per
    seed; ~0.1 s."""
    random.seed(seed)
    rnd = random.Random(seed)
    running = {g: 0 for g in groups}
    inflight = {g: 0 for g in groups}
    queue = {g: collections.deque() for g in groups}

    def snaps():
        return {g: GroupSnapshot(active_children=9,
                                 cpu_headroom=max(0.0, 1 - running[g] / slots),
                                 ram_headroom=max(0.0, 1 - running[g] / slots),
                                 inflight=inflight[g]) for g in groups}

    m = MABManager(agent_id=90, child_groups=list(groups), repository=MagicMock(),
                   config={"algorithm": "linucb", "load_slack": slack,
                           "linucb": {"model": model}, "reward": {"shaped": shaped}},
                   group_snapshot_provider=snaps, delegation_timeout_s=600.0)
    events, t = [], 0.0
    for i in range(n):
        t += rnd.expovariate(n / span)
        heapq.heappush(events, (t, 0, i))
    chosen, latencies = collections.Counter(), []
    while events:
        now, kind, data = heapq.heappop(events)
        if kind == 0:
            job = _job(data)
            job.wall_time = rnd.uniform(5, 120)
            g = m.select_groups(list(groups), job=job, top_k=1)[0]
            chosen[g] += 1
            inflight[g] += 1
            item = (data, job.wall_time, now)
            if running[g] < slots:
                running[g] += 1
                heapq.heappush(events, (now + job.wall_time, 1, (g,) + item))
            else:
                queue[g].append(item)
        else:
            g, job_id, _dur, t0 = data
            running[g] -= 1
            inflight[g] -= 1
            latencies.append(now - t0)
            m.report_outcome(g, str(job_id), True, latency_s=now - t0)
            if queue[g]:
                nxt, dur, tq = queue[g].popleft()
                running[g] += 1
                heapq.heappush(events, (now + dur, 1, (g, nxt, dur, tq)))
    latencies.sort()
    return max(chosen.values()) / n, latencies[int(len(latencies) * 0.95)]


SEEDS = (1, 2, 3)


class TestBurstRouting:
    """Behaviour, not mechanism: under a saturating burst with every outcome a success (E1′
    injects no failures), does routing herd? Measured at commit (8 seeds): shared/no guard/
    binary 0.92-0.96 of jobs to one group, p95 805-871 s; the E1′ configuration 0.34, 255-280 s;
    per-arm models without the guard 0.44-0.84 — which is why the guard is on as well."""

    def test_the_shipped_bandit_herds(self):
        for seed in SEEDS:
            share, _ = _burst("shared", 0, False, seed)
            assert share >= 0.8, f"seed {seed}: {share:.2f}"

    def test_the_e1_configuration_spreads_the_burst(self):
        for seed in SEEDS:
            share, p95 = _burst("disjoint", 9, True, seed)
            _, herded_p95 = _burst("shared", 0, False, seed)
            assert share <= 0.40, f"seed {seed}: {share:.2f}"
            assert p95 <= 0.5 * herded_p95, f"seed {seed}: p95 {p95:.0f} vs {herded_p95:.0f}"

    def test_the_guard_alone_spreads_it_for_either_model(self):
        for seed in SEEDS:
            assert _burst("shared", 9, True, seed)[0] <= 0.40
            assert _burst("disjoint", 9, False, seed)[0] <= 0.40

    def test_per_arm_models_alone_herd_less_but_still_herd(self):
        shared = sum(_burst("shared", 0, False, s)[0] for s in SEEDS) / len(SEEDS)
        disjoint = sum(_burst("disjoint", 0, False, s)[0] for s in SEEDS) / len(SEEDS)
        assert disjoint < shared
        assert disjoint > 0.40, "if this ever balances on its own, revisit whether the guard is needed"
