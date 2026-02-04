# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Author: Komal Thareja(kthare10@renci.org)
import math
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class ArmStats:
    arm_id: int
    successes: int = 0
    failures: int = 0
    total_reward: float = 0.0
    pull_count: int = 0

    @property
    def q_value(self) -> float:
        if self.pull_count == 0:
            return 0.0
        return self.total_reward / self.pull_count

    def to_dict(self) -> dict:
        return {
            "arm_id": self.arm_id,
            "successes": self.successes,
            "failures": self.failures,
            "total_reward": self.total_reward,
            "pull_count": self.pull_count,
            "q_value": self.q_value,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ArmStats":
        stats = cls(arm_id=data["arm_id"])
        stats.successes = data.get("successes", 0)
        stats.failures = data.get("failures", 0)
        stats.total_reward = data.get("total_reward", 0.0)
        stats.pull_count = data.get("pull_count", 0)
        return stats


class BanditPolicy(ABC):
    def __init__(self):
        self.arms: Dict[int, ArmStats] = {}

    def ensure_arm(self, arm_id: int):
        if arm_id not in self.arms:
            self.arms[arm_id] = ArmStats(arm_id=arm_id)

    @abstractmethod
    def select_arm(self, eligible_arms: List[int]) -> int:
        ...

    def update(self, arm_id: int, reward: float):
        self.ensure_arm(arm_id)
        arm = self.arms[arm_id]
        arm.pull_count += 1
        arm.total_reward += reward
        if reward > 0:
            arm.successes += 1
        else:
            arm.failures += 1

    def get_state(self) -> dict:
        return {
            "arms": {str(k): v.to_dict() for k, v in self.arms.items()},
        }

    def load_state(self, state: dict):
        self.arms.clear()
        for k, v in state.get("arms", {}).items():
            self.arms[int(k)] = ArmStats.from_dict(v)

    def reset(self):
        self.arms.clear()


class EpsilonGreedyPolicy(BanditPolicy):
    def __init__(self, epsilon: float = 0.1, epsilon_decay: float = 0.995,
                 epsilon_min: float = 0.01):
        super().__init__()
        self.epsilon = epsilon
        self.initial_epsilon = epsilon
        self.epsilon_decay = epsilon_decay
        self.epsilon_min = epsilon_min

    def select_arm(self, eligible_arms: List[int]) -> int:
        if not eligible_arms:
            raise ValueError("No eligible arms to select from")

        for arm_id in eligible_arms:
            self.ensure_arm(arm_id)

        if random.random() < self.epsilon:
            return random.choice(eligible_arms)

        # Greedy: pick arm with highest Q-value (break ties randomly)
        best_q = max(self.arms[a].q_value for a in eligible_arms)
        best_arms = [a for a in eligible_arms if self.arms[a].q_value == best_q]
        return random.choice(best_arms)

    def update(self, arm_id: int, reward: float):
        super().update(arm_id, reward)
        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)

    def get_state(self) -> dict:
        state = super().get_state()
        state["epsilon"] = self.epsilon
        return state

    def load_state(self, state: dict):
        super().load_state(state)
        self.epsilon = state.get("epsilon", self.initial_epsilon)


class UCB1Policy(BanditPolicy):
    def __init__(self, exploration_weight: float = math.sqrt(2)):
        super().__init__()
        self.exploration_weight = exploration_weight

    def select_arm(self, eligible_arms: List[int]) -> int:
        if not eligible_arms:
            raise ValueError("No eligible arms to select from")

        for arm_id in eligible_arms:
            self.ensure_arm(arm_id)

        # Arms with zero pulls get selected first
        unpulled = [a for a in eligible_arms if self.arms[a].pull_count == 0]
        if unpulled:
            return random.choice(unpulled)

        total_pulls = sum(self.arms[a].pull_count for a in eligible_arms)

        def ucb_score(arm_id: int) -> float:
            arm = self.arms[arm_id]
            exploitation = arm.q_value
            exploration = self.exploration_weight * math.sqrt(
                math.log(total_pulls) / arm.pull_count
            )
            return exploitation + exploration

        best_score = max(ucb_score(a) for a in eligible_arms)
        best_arms = [a for a in eligible_arms
                     if abs(ucb_score(a) - best_score) < 1e-9]
        return random.choice(best_arms)

    def get_state(self) -> dict:
        state = super().get_state()
        state["exploration_weight"] = self.exploration_weight
        return state

    def load_state(self, state: dict):
        super().load_state(state)
        self.exploration_weight = state.get("exploration_weight", self.exploration_weight)
