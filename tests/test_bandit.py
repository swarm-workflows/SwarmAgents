# Tests for Multi-Armed Bandit algorithms
import math
import random

import numpy as np
import pytest
from swarm.rl.bandit import ArmStats, EpsilonGreedyPolicy, LinUCBPolicy, UCB1Policy
from swarm.rl.context import ContextExtractor, GroupSnapshot


class TestArmStats:
    def test_q_value_zero_pulls(self):
        arm = ArmStats(arm_id=0)
        assert arm.q_value == 0.0

    def test_q_value_calculation(self):
        arm = ArmStats(arm_id=0, total_reward=10.0, pull_count=5)
        assert arm.q_value == 2.0

    def test_serialization_roundtrip(self):
        arm = ArmStats(arm_id=1, successes=3, failures=2, total_reward=1.0, pull_count=5)
        data = arm.to_dict()
        restored = ArmStats.from_dict(data)
        assert restored.arm_id == 1
        assert restored.successes == 3
        assert restored.failures == 2
        assert restored.total_reward == 1.0
        assert restored.pull_count == 5


class TestEpsilonGreedyPolicy:
    def test_selects_from_eligible(self):
        policy = EpsilonGreedyPolicy(epsilon=0.0)  # pure greedy
        for _ in range(10):
            arm = policy.select_arm([1, 2, 3])
            assert arm in [1, 2, 3]

    def test_greedy_selects_best(self):
        # epsilon_min=0.0 too: the floor would silently raise epsilon back
        # to 0.01 after the first update, making this test flaky
        policy = EpsilonGreedyPolicy(epsilon=0.0, epsilon_min=0.0)
        # Arm 2 has highest Q-value
        policy.update(1, -1.0)
        policy.update(2, 1.0)
        policy.update(3, 0.0)

        selections = [policy.select_arm([1, 2, 3]) for _ in range(100)]
        assert all(s == 2 for s in selections)

    def test_epsilon_decay(self):
        policy = EpsilonGreedyPolicy(epsilon=1.0, epsilon_decay=0.5, epsilon_min=0.1)
        assert policy.epsilon == 1.0
        policy.update(1, 1.0)
        assert policy.epsilon == 0.5
        policy.update(1, 1.0)
        assert policy.epsilon == 0.25
        policy.update(1, 1.0)
        assert policy.epsilon == 0.125
        policy.update(1, 1.0)
        assert policy.epsilon == 0.1  # hit floor

    def test_state_persistence(self):
        policy = EpsilonGreedyPolicy(epsilon=0.5)
        policy.update(1, 1.0)
        policy.update(2, -1.0)

        state = policy.get_state()

        policy2 = EpsilonGreedyPolicy(epsilon=1.0)
        policy2.load_state(state)

        assert policy2.arms[1].successes == 1
        assert policy2.arms[2].failures == 1
        assert policy2.epsilon == policy.epsilon

    def test_empty_arms_raises(self):
        policy = EpsilonGreedyPolicy()
        with pytest.raises(ValueError):
            policy.select_arm([])


class TestUCB1Policy:
    def test_unpulled_arms_first(self):
        policy = UCB1Policy()
        policy.update(1, 1.0)  # arm 1 has been pulled

        # Arms 2 and 3 are unpulled, should be selected
        selections = set()
        for _ in range(20):
            arm = policy.select_arm([1, 2, 3])
            selections.add(arm)

        assert 2 in selections or 3 in selections

    def test_balances_exploration_exploitation(self):
        policy = UCB1Policy(exploration_weight=math.sqrt(2))

        # Arm 1 slightly better than arm 2; selections feed back into the
        # policy so the UCB bonus of the neglected arm can grow. (With a
        # static Q gap of 2.0 and no updates, arm 2 was mathematically
        # unreachable and the old version of this test always failed.)
        for _ in range(10):
            policy.update(1, 0.6)
            policy.update(2, 0.4)

        selections = []
        for _ in range(100):
            arm = policy.select_arm([1, 2])
            selections.append(arm)
            policy.update(arm, 0.6 if arm == 1 else 0.4)
        arm1_count = selections.count(1)
        arm2_count = selections.count(2)

        assert arm1_count > arm2_count  # exploitation
        assert arm2_count > 0  # exploration

    def test_state_persistence(self):
        policy = UCB1Policy(exploration_weight=2.0)
        policy.update(1, 1.0)
        policy.update(2, -1.0)

        state = policy.get_state()

        policy2 = UCB1Policy()
        policy2.load_state(state)

        assert policy2.arms[1].total_reward == 1.0
        assert policy2.arms[2].total_reward == -1.0
        assert policy2.exploration_weight == 2.0


class TestConvergence:
    def test_epsilon_greedy_converges_to_best_arm(self):
        """With enough trials, epsilon-greedy should mostly pick the best arm."""
        policy = EpsilonGreedyPolicy(epsilon=0.3, epsilon_decay=0.99, epsilon_min=0.01)

        # Simulate: arm 1 succeeds 80%, arm 2 succeeds 20%
        import random
        random.seed(42)

        for _ in range(500):
            arm = policy.select_arm([1, 2])
            if arm == 1:
                reward = 1.0 if random.random() < 0.8 else -1.0
            else:
                reward = 1.0 if random.random() < 0.2 else -1.0
            policy.update(arm, reward)

        # Arm 1 should have higher Q-value
        assert policy.arms[1].q_value > policy.arms[2].q_value

    def test_ucb1_converges_to_best_arm(self):
        """UCB1 should converge to pulling the best arm more often."""
        policy = UCB1Policy()

        import random
        random.seed(42)

        for _ in range(500):
            arm = policy.select_arm([1, 2])
            if arm == 1:
                reward = 1.0 if random.random() < 0.8 else -1.0
            else:
                reward = 1.0 if random.random() < 0.2 else -1.0
            policy.update(arm, reward)

        # Arm 1 should have higher Q-value and more pulls
        assert policy.arms[1].q_value > policy.arms[2].q_value
        assert policy.arms[1].pull_count > policy.arms[2].pull_count


class TestStepSize:
    def test_recency_weighted_estimate(self):
        policy = EpsilonGreedyPolicy(epsilon=0.0, step_size=0.5)
        policy.update(1, 1.0)   # 0 + 0.5*(1 - 0) = 0.5
        policy.update(1, 0.0)   # 0.5 + 0.5*(0 - 0.5) = 0.25
        assert policy.arms[1].q_estimate == 0.25
        assert policy.arms[1].q_value == 0.5  # lifetime average untouched

    def test_greedy_uses_recency_estimate(self):
        """With step_size, recent rewards dominate stale lifetime averages."""
        policy = EpsilonGreedyPolicy(epsilon=0.0, step_size=0.9)
        # Arm 1 good historically then bad recently; arm 2 the opposite
        for _ in range(10):
            policy.update(1, 1.0)
            policy.update(2, -1.0)
        for _ in range(3):
            policy.update(1, -1.0)
            policy.update(2, 1.0)

        # Lifetime average still favors arm 1, recency favors arm 2
        assert policy.arms[1].q_value > policy.arms[2].q_value
        assert all(policy.select_arm([1, 2]) == 2 for _ in range(20))

    def test_step_size_persistence(self):
        policy = EpsilonGreedyPolicy(step_size=0.1)
        policy.update(1, 1.0)
        state = policy.get_state()

        policy2 = EpsilonGreedyPolicy()
        policy2.load_state(state)
        assert policy2.step_size == 0.1
        assert policy2.arms[1].q_estimate == policy.arms[1].q_estimate


class TestLinUCBPolicy:
    def test_selects_from_eligible_with_context(self):
        policy = LinUCBPolicy()
        ctx = {a: np.array([0.5, 1.0]) for a in [1, 2, 3]}
        for _ in range(10):
            assert policy.select_arm([1, 2, 3], context=ctx) in [1, 2, 3]

    def test_fallback_random_without_context(self):
        policy = LinUCBPolicy()
        assert policy.select_arm([1, 2, 3]) in [1, 2, 3]
        # Partial context (arm 3 missing) also falls back
        ctx = {1: np.array([1.0]), 2: np.array([1.0])}
        assert policy.select_arm([1, 2, 3], context=ctx) in [1, 2, 3]
        # Model must remain uninitialized by fallback selections
        assert policy.A_inv is None

    def test_empty_arms_raises(self):
        with pytest.raises(ValueError):
            LinUCBPolicy().select_arm([])

    def test_update_without_context_skips_model(self):
        policy = LinUCBPolicy(dim=2)
        policy.update(1, 1.0)
        assert policy.arms[1].pull_count == 1
        assert np.allclose(policy.A_inv, np.eye(2))
        assert np.allclose(policy.b, np.zeros(2))

    def test_dimension_mismatch_raises(self):
        policy = LinUCBPolicy(dim=3)
        with pytest.raises(ValueError):
            policy.update(1, 1.0, context=np.array([1.0, 0.0]))

    def test_sherman_morrison_matches_direct_inverse(self):
        np.random.seed(0)
        d, gamma = 4, 0.98
        policy = LinUCBPolicy(discount=gamma, dim=d)
        A = np.eye(d)
        for _ in range(60):
            x = np.random.rand(d)
            r = np.random.rand() * 2 - 1
            policy.update(1, r, context=x)
            A = gamma * A + np.outer(x, x)
        assert np.allclose(policy.A_inv, np.linalg.inv(A), atol=1e-8)

    def test_discount_adapts_to_reward_flip(self):
        stationary = LinUCBPolicy(discount=1.0, dim=1)
        adaptive = LinUCBPolicy(discount=0.9, dim=1)
        x = np.array([1.0])
        for policy in (stationary, adaptive):
            for _ in range(100):
                policy.update(1, 1.0, context=x)
            for _ in range(50):
                policy.update(1, -1.0, context=x)

        # Discounted model tracks the flip; undiscounted lags on old rewards
        assert adaptive.theta[0] < 0
        assert stationary.theta[0] > 0

    def test_top_k_returns_highest_scores(self):
        random.seed(1)
        policy = LinUCBPolicy(alpha=0.0, dim=2)  # pure exploitation
        # Train theta to weight feature 0 positively
        for _ in range(30):
            policy.update(1, 1.0, context=np.array([1.0, 1.0]))
            policy.update(1, -1.0, context=np.array([0.0, 1.0]))

        ctx = {
            1: np.array([1.0, 1.0]),
            2: np.array([0.5, 1.0]),
            3: np.array([0.0, 1.0]),
        }
        assert policy.select_top_k([1, 2, 3], 2, context=ctx) == [1, 2]
        assert policy.select_top_k([1, 2, 3], 5, context=ctx) == [1, 2, 3]

    def test_state_persistence_roundtrip(self):
        np.random.seed(2)
        policy = LinUCBPolicy(alpha=0.7, discount=0.99, schema_version="v1")
        for _ in range(20):
            policy.update(1, np.random.rand(), context=np.random.rand(3))
        state = policy.get_state()

        policy2 = LinUCBPolicy(schema_version="v1")
        policy2.load_state(state)
        assert policy2.dim == 3
        assert policy2.alpha == 0.7
        assert policy2.discount == 0.99
        assert np.allclose(policy2.A_inv, policy.A_inv)
        assert np.allclose(policy2.b, policy.b)
        assert policy2.arms[1].pull_count == 20

    def test_schema_version_mismatch_discards_state(self):
        policy = LinUCBPolicy(schema_version="v1")
        policy.update(1, 1.0, context=np.array([1.0, 0.0]))
        state = policy.get_state()

        policy2 = LinUCBPolicy(schema_version="v2")
        policy2.load_state(state)
        assert policy2.A_inv is None
        assert policy2.arms == {}

    def test_dim_mismatch_discards_state(self):
        policy = LinUCBPolicy()
        policy.update(1, 1.0, context=np.array([1.0, 0.0, 0.5]))
        state = policy.get_state()

        policy2 = LinUCBPolicy(dim=5)
        policy2.load_state(state)
        assert policy2.dim == 5
        assert np.allclose(policy2.A_inv, np.eye(5))
        assert policy2.arms == {}


class TestLinUCBLearning:
    @staticmethod
    def _ctx(right_arm, arms):
        # Per-(job, arm) features: [does this arm match the job type?, bias]
        return {a: np.array([1.0 if a == right_arm else 0.0, 1.0]) for a in arms}

    def test_learns_context_dependent_routing(self):
        """When the best arm depends on job type, LinUCB routes per-type
        while a context-blind policy cannot beat coin-flipping."""
        random.seed(7)
        linucb = LinUCBPolicy(alpha=0.5, discount=1.0)
        eps = EpsilonGreedyPolicy(epsilon=0.05)
        arms = [1, 2]

        linucb_correct = eps_correct = 0
        for t in range(300):
            right = 1 if t % 2 == 0 else 2  # alternating job types
            ctx = self._ctx(right, arms)

            arm = linucb.select_arm(arms, context=ctx)
            linucb.update(arm, 1.0 if arm == right else -1.0, context=ctx[arm])
            if t >= 200 and arm == right:
                linucb_correct += 1

            arm = eps.select_arm(arms)
            eps.update(arm, 1.0 if arm == right else -1.0)
            if t >= 200 and arm == right:
                eps_correct += 1

        assert linucb_correct >= 90   # near-oracle over the last 100 jobs
        assert eps_correct <= 70      # context-blind stays near 50%
        assert linucb_correct > eps_correct

    def test_new_arm_scored_from_features(self):
        """Shared model: a never-pulled group is preferred immediately when
        its features predict success (dynamic agent addition)."""
        random.seed(3)
        policy = LinUCBPolicy(alpha=0.1)
        arms = [1, 2]
        for t in range(200):
            right = 1 if t % 2 == 0 else 2
            ctx = self._ctx(right, arms)
            arm = policy.select_arm(arms, context=ctx)
            policy.update(arm, 1.0 if arm == right else -1.0, context=ctx[arm])

        # Arm 99 has never been seen, but its features say "match"
        ctx = self._ctx(99, [1, 99])
        selections = [policy.select_arm([1, 99], context=ctx) for _ in range(20)]
        assert selections.count(99) >= 18


class _FakeCapacities:
    def __init__(self, core=0, ram=0, disk=0, gpu=0):
        self.core, self.ram, self.disk, self.gpu = core, ram, disk, gpu


class _FakeJob:
    def __init__(self, core=0, ram=0, disk=0, gpu=0, wall_time=0.0,
                 job_type=None, dtns=0):
        self.capacities = _FakeCapacities(core, ram, disk, gpu)
        self.wall_time = wall_time
        self.job_type = job_type
        self.data_in = [object()] * dtns
        self.data_out = []


class TestContextExtractor:
    CONFIG = {
        "job_types": ["compute", "transfer"],
        "max_group_size": 10,
        "max_inflight": 32,
        "max_dtns": 4,
        "long_job_threshold": 20.0,
        "max_caps": {"core": 16, "ram": 64, "disk": 500, "gpu": 4},
    }

    def test_dim_matches_feature_names_and_vectors(self):
        extractor = ContextExtractor(self.CONFIG)
        assert extractor.dim == len(extractor.feature_names)
        vectors = extractor.build(_FakeJob(core=4), [1, 2])
        assert set(vectors) == {1, 2}
        assert all(len(v) == extractor.dim for v in vectors.values())

    def test_values_in_unit_interval(self):
        extractor = ContextExtractor(self.CONFIG)
        # Demands beyond the normalization caps must clip, not explode
        job = _FakeJob(core=999, ram=9999, disk=99999, gpu=99,
                       wall_time=1e6, job_type="compute", dtns=50)
        snapshots = {1: GroupSnapshot(active_children=50, inflight=500,
                                      failure_rate=2.0)}
        vec = extractor.build(job, [1], snapshots)[1]
        assert np.all(vec >= 0.0) and np.all(vec <= 1.0)

    def test_job_type_one_hot(self):
        extractor = ContextExtractor(self.CONFIG)
        names = extractor.feature_names
        compute_idx = names.index("job_type:compute")
        transfer_idx = names.index("job_type:transfer")

        vec = extractor.build(_FakeJob(job_type="compute"), [1])[1]
        assert vec[compute_idx] == 1.0 and vec[transfer_idx] == 0.0

        # Unknown type: all one-hot slots zero
        vec = extractor.build(_FakeJob(job_type="mystery"), [1])[1]
        assert vec[compute_idx] == 0.0 and vec[transfer_idx] == 0.0

    def test_bias_term_last(self):
        extractor = ContextExtractor(self.CONFIG)
        vec = extractor.build(_FakeJob(), [1])[1]
        assert extractor.feature_names[-1] == "bias"
        assert vec[-1] == 1.0

    def test_missing_snapshot_uses_idle_defaults(self):
        extractor = ContextExtractor(self.CONFIG)
        names = extractor.feature_names
        vec = extractor.build(_FakeJob(), [7])[7]  # no snapshot for group 7
        assert vec[names.index("grp_cpu_headroom")] == 1.0
        assert vec[names.index("grp_inflight")] == 0.0
        assert vec[names.index("grp_failure_rate")] == 0.0

    def test_schema_version_stable_and_config_sensitive(self):
        a = ContextExtractor(self.CONFIG)
        b = ContextExtractor(dict(self.CONFIG))
        assert a.schema_version == b.schema_version

        changed = dict(self.CONFIG, job_types=["compute", "transfer", "dtn"])
        assert ContextExtractor(changed).schema_version != a.schema_version

    def test_real_job_model(self):
        """Smoke test against the actual Job/Capacities models to catch
        attribute drift."""
        from swarm.models.capacities import Capacities
        from swarm.models.job import Job

        job = Job()
        job.job_id = "job-ctx-1"
        job.capacities = Capacities(core=8, ram=32, disk=100, gpu=1)
        job.wall_time = 15.0
        job.job_type = "compute"

        extractor = ContextExtractor(self.CONFIG)
        vec = extractor.build(job, [1], {1: GroupSnapshot(active_children=5)})[1]
        assert len(vec) == extractor.dim
        assert np.all(vec >= 0.0) and np.all(vec <= 1.0)
        names = extractor.feature_names
        assert vec[names.index("job_core")] == 0.5     # 8 / 16
        assert vec[names.index("job_type:compute")] == 1.0

    def test_linucb_end_to_end_with_extractor(self):
        """Extractor vectors drive LinUCB end-to-end: group 1 fails compute
        jobs, group 2 fails transfer jobs. Per-type failure rates (fed back
        into GroupSnapshot, as MABManager will maintain them) let the model
        learn to route each job type away from the group that fails it."""
        from collections import defaultdict, deque

        random.seed(11)
        extractor = ContextExtractor(self.CONFIG)
        policy = LinUCBPolicy(alpha=0.5, dim=extractor.dim,
                              schema_version=extractor.schema_version)

        windows = defaultdict(lambda: deque(maxlen=20))

        def snapshot(group):
            rates = {
                t: (sum(w) / len(w)) if (w := windows[(group, t)]) else 0.0
                for t in ("compute", "transfer")
            }
            return GroupSnapshot(active_children=5, type_failure_rates=rates)

        correct = 0
        for t in range(400):
            job_type = "compute" if t % 2 == 0 else "transfer"
            right = 2 if job_type == "compute" else 1
            job = _FakeJob(core=4, ram=16, job_type=job_type)
            ctx = extractor.build(job, [1, 2], {1: snapshot(1), 2: snapshot(2)})
            arm = policy.select_arm([1, 2], context=ctx)
            success = arm == right
            policy.update(arm, 1.0 if success else -1.0, context=ctx[arm])
            windows[(arm, job_type)].append(0.0 if success else 1.0)
            if t >= 300 and success:
                correct += 1
        assert correct >= 80


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
