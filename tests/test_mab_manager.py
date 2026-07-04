# Tests for MABManager contextual-bandit plumbing (Phase 2)
import random
import time

import numpy as np
import pytest

from swarm.rl.bandit import EpsilonGreedyPolicy, LinUCBPolicy
from swarm.rl.context import GroupSnapshot
from swarm.rl.mab_manager import MABManager


class FakeRepository:
    def __init__(self):
        self.store = {}

    def save(self, obj, key):
        self.store[key] = obj

    def get(self, key):
        return self.store.get(key)


class _FakeCapacities:
    def __init__(self, core=0, ram=0, disk=0, gpu=0):
        self.core, self.ram, self.disk, self.gpu = core, ram, disk, gpu


class _FakeJob:
    def __init__(self, job_id, job_type=None, core=4, ram=16, wall_time=10.0):
        self.job_id = job_id
        self.job_type = job_type
        self.capacities = _FakeCapacities(core=core, ram=ram)
        self.wall_time = wall_time
        self.data_in = []
        self.data_out = []


def linucb_config(**overrides):
    config = {
        "algorithm": "linucb",
        "linucb": {"alpha": 0.5, "discount": 1.0},
        "context": {"job_types": ["compute", "transfer"], "failure_window": 10},
    }
    config.update(overrides)
    return config


def make_manager(config, groups=(1, 2), repository=None, **kwargs):
    return MABManager(agent_id=99, child_groups=list(groups),
                      config=config, repository=repository, **kwargs)


class TestBackwardCompatibility:
    def test_default_policy_unchanged(self):
        manager = make_manager({"algorithm": "epsilon_greedy", "epsilon": 0.2})
        assert isinstance(manager.policy, EpsilonGreedyPolicy)
        assert not manager.contextual

        selected = manager.select_groups([1, 2], job=_FakeJob("j1"), top_k=1)
        assert len(selected) == 1
        manager.report_outcome(selected[0], "j1", success=True)
        assert manager.policy.arms[selected[0]].total_reward == 1.0
        # Non-contextual mode records no pending selections
        assert manager._pending == {}

    def test_top_k_covering_all_returns_all(self):
        manager = make_manager({"algorithm": "epsilon_greedy"})
        assert manager.select_groups([1, 2], top_k=5) == [1, 2]


class TestLinUCBPlumbing:
    def test_linucb_policy_created_with_schema(self):
        manager = make_manager(linucb_config())
        assert isinstance(manager.policy, LinUCBPolicy)
        assert manager.contextual
        assert manager.policy.dim == manager.extractor.dim
        assert manager.policy.schema_version == manager.extractor.schema_version

    def test_pending_recorded_and_replayed_on_outcome(self):
        manager = make_manager(linucb_config())
        job = _FakeJob("j1", job_type="compute")
        selected = manager.select_groups([1, 2], job=job, top_k=1)
        assert len(manager._pending["j1"]) == 1

        manager.report_outcome(selected[0], "j1", success=True)
        # Context was replayed into the model, not just the counters
        assert manager._pending == {}
        assert manager.policy.arms[selected[0]].pull_count == 1
        assert not np.allclose(manager.policy.A_inv,
                               np.eye(manager.extractor.dim))

    def test_select_all_still_records_pending(self):
        """top_k >= len(capable) bypasses the policy but must still stash
        context so outcomes train the model."""
        manager = make_manager(linucb_config(), groups=(1, 2, 3))
        job = _FakeJob("j1", job_type="compute")
        assert manager.select_groups([1, 2, 3], job=job, top_k=5) == [1, 2, 3]
        assert set(manager._pending["j1"]) == {1, 2, 3}

    def test_outcome_is_terminal_for_job(self):
        """Reporting one group's outcome clears sibling pending entries."""
        manager = make_manager(linucb_config(), groups=(1, 2, 3))
        job = _FakeJob("j1", job_type="compute")
        manager.select_groups([1, 2, 3], job=job, top_k=2)
        winner = list(manager._pending["j1"])[0]
        manager.report_outcome(winner, "j1", success=True)
        assert manager._pending == {}
        # Siblings dropped without update (non_winner defaults to null)
        total_pulls = sum(a.pull_count for a in manager.policy.arms.values())
        assert total_pulls == 1

    def test_non_winner_reward(self):
        config = linucb_config(reward={"non_winner": -0.1})
        manager = make_manager(config, groups=(1, 2, 3))
        job = _FakeJob("j1", job_type="compute")
        selected = manager.select_groups([1, 2, 3], job=job, top_k=2)
        winner, loser = selected
        manager.report_outcome(winner, "j1", success=True)
        assert manager.policy.arms[winner].total_reward == 1.0
        assert manager.policy.arms[loser].total_reward == -0.1

    def test_pending_ttl_sweep(self):
        manager = make_manager(linucb_config(pending_ttl_s=0.01))
        manager.select_groups([1, 2], job=_FakeJob("stale", job_type="compute"))
        time.sleep(0.05)
        manager.select_groups([1, 2], job=_FakeJob("fresh", job_type="compute"))
        assert "stale" not in manager._pending
        assert "fresh" in manager._pending


class TestRewardShaping:
    def make_shaped(self):
        return make_manager(linucb_config(reward={"shaped": True}),
                            delegation_timeout_s=60.0)

    def test_success_latency_shaped(self):
        manager = self.make_shaped()
        manager.select_groups([1, 2], job=_FakeJob("j1", job_type="compute"))
        group = list(manager._pending["j1"])[0]
        manager.report_outcome(group, "j1", success=True, latency_s=30.0)
        assert manager.policy.arms[group].total_reward == pytest.approx(0.5)

    def test_exit_failure_vs_timeout(self):
        manager = self.make_shaped()
        manager.report_outcome(1, "j-exit", success=False)
        manager.report_outcome(2, "j-timeout", success=False, timed_out=True)
        assert manager.policy.arms[1].total_reward == pytest.approx(-0.5)
        assert manager.policy.arms[2].total_reward == pytest.approx(-1.0)

    def test_unshaped_stays_binary(self):
        manager = make_manager(linucb_config())
        manager.report_outcome(1, "j1", success=True, latency_s=30.0)
        manager.report_outcome(2, "j2", success=False, timed_out=True)
        assert manager.policy.arms[1].total_reward == 1.0
        assert manager.policy.arms[2].total_reward == -1.0


class TestFailureWindows:
    def test_windows_feed_snapshots_and_stats(self):
        manager = make_manager(linucb_config())
        job = _FakeJob("j1", job_type="compute")
        manager.select_groups([1, 2], job=job, top_k=2)
        manager.report_outcome(1, "j1", success=False)

        snapshots = manager._build_snapshots([1, 2])
        assert snapshots[1].failure_rate == 1.0
        assert snapshots[1].type_failure_rates == {"compute": 1.0}
        assert snapshots[2].failure_rate == 0.0

        stats = manager.get_stats()["context"]
        assert stats["group_failure_rates"][1] == 1.0
        assert stats["type_failure_rates"]["1:compute"] == 1.0

    def test_provider_load_kept_failure_fields_overwritten(self):
        provider_snap = GroupSnapshot(active_children=7, cpu_headroom=0.3,
                                      failure_rate=0.9,
                                      type_failure_rates={"compute": 0.9})
        manager = make_manager(linucb_config(),
                               group_snapshot_provider=lambda: {1: provider_snap})
        snapshots = manager._build_snapshots([1])
        # Load/capacity data comes from the provider...
        assert snapshots[1].active_children == 7
        assert snapshots[1].cpu_headroom == 0.3
        # ...but failure rates are manager-owned (no outcomes yet -> 0)
        assert snapshots[1].failure_rate == 0.0
        assert snapshots[1].type_failure_rates == {}

    def test_provider_failure_does_not_break_selection(self):
        def broken():
            raise RuntimeError("boom")
        manager = make_manager(linucb_config(), group_snapshot_provider=broken)
        selected = manager.select_groups(
            [1, 2], job=_FakeJob("j1", job_type="compute"))
        assert len(selected) == 1


class TestPersistence:
    def test_roundtrip_via_repository(self):
        repo = FakeRepository()
        config = linucb_config(persist_to_redis=True)
        manager = make_manager(config, repository=repo)
        job = _FakeJob("j1", job_type="compute")
        selected = manager.select_groups([1, 2], job=job)
        manager.report_outcome(selected[0], "j1", success=True)
        manager.save_state()

        manager2 = make_manager(config, repository=repo)
        manager2.load_state()
        assert np.allclose(manager2.policy.A_inv, manager.policy.A_inv)
        assert np.allclose(manager2.policy.b, manager.policy.b)

    def test_schema_change_discards_persisted_state(self):
        repo = FakeRepository()
        config = linucb_config(persist_to_redis=True)
        manager = make_manager(config, repository=repo)
        job = _FakeJob("j1", job_type="compute")
        selected = manager.select_groups([1, 2], job=job)
        manager.report_outcome(selected[0], "j1", success=True)
        manager.save_state()

        changed = linucb_config(persist_to_redis=True)
        changed["context"] = {"job_types": ["compute", "transfer", "dtn"]}
        manager2 = make_manager(changed, repository=repo)
        manager2.load_state()
        # Stale layout discarded: fresh identity model, no arm history
        assert np.allclose(manager2.policy.A_inv,
                           np.eye(manager2.extractor.dim))
        assert all(a.pull_count == 0 for a in manager2.policy.arms.values())


class TestEndToEndRouting:
    def test_manager_learns_job_type_routing(self):
        """Full manager loop: group 1 fails compute, group 2 fails transfer.
        The manager's own failure windows must provide enough signal for
        LinUCB to route job types to the group that succeeds at them."""
        random.seed(11)
        manager = make_manager(linucb_config())

        correct = 0
        for t in range(400):
            job_type = "compute" if t % 2 == 0 else "transfer"
            right = 2 if job_type == "compute" else 1
            job = _FakeJob(f"j{t}", job_type=job_type)
            selected = manager.select_groups([1, 2], job=job, top_k=1)
            arm = selected[0]
            success = arm == right
            manager.report_outcome(arm, job.job_id, success=success)
            if t >= 300 and success:
                correct += 1
        assert correct >= 75


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
