# Tests for Multi-Armed Bandit algorithms
import math
import pytest
from swarm.rl.bandit import ArmStats, EpsilonGreedyPolicy, UCB1Policy


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
        policy = EpsilonGreedyPolicy(epsilon=0.0)
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

        # Give arm 1 high reward, arm 2 low reward
        for _ in range(10):
            policy.update(1, 1.0)
            policy.update(2, -1.0)

        # Arm 1 should be preferred but arm 2 still gets explored
        selections = [policy.select_arm([1, 2]) for _ in range(100)]
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
