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
import logging
import math
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class ArmStats:
    arm_id: int
    successes: int = 0
    failures: int = 0
    total_reward: float = 0.0
    pull_count: int = 0
    # Exponential recency-weighted estimate, maintained only when the policy
    # has a step_size configured (non-stationary environments).
    q_estimate: float = 0.0

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
            "q_estimate": self.q_estimate,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ArmStats":
        stats = cls(arm_id=data["arm_id"])
        stats.successes = data.get("successes", 0)
        stats.failures = data.get("failures", 0)
        stats.total_reward = data.get("total_reward", 0.0)
        stats.pull_count = data.get("pull_count", 0)
        stats.q_estimate = data.get("q_estimate", 0.0)
        return stats


class BanditPolicy(ABC):
    """Base class for bandit policies over child-group arms.

    ``context`` is an optional mapping ``arm_id -> feature vector`` built by
    a ContextExtractor. Non-contextual policies ignore it; contextual policies
    (LinUCB) require it for informed selection and model updates.

    ``step_size``: when set, arm values are tracked as an exponential
    recency-weighted average (Q <- Q + step_size * (r - Q)) instead of the
    lifetime average, so estimates track non-stationary reward distributions.
    """

    def __init__(self, step_size: Optional[float] = None):
        self.arms: Dict[int, ArmStats] = {}
        self.step_size = step_size

    def ensure_arm(self, arm_id: int):
        if arm_id not in self.arms:
            self.arms[arm_id] = ArmStats(arm_id=arm_id)

    def q(self, arm_id: int) -> float:
        """Effective arm value: recency-weighted if step_size is set."""
        arm = self.arms[arm_id]
        if self.step_size is not None:
            return arm.q_estimate
        return arm.q_value

    @abstractmethod
    def select_arm(self, eligible_arms: List[int],
                   context: Optional[Dict[int, np.ndarray]] = None) -> int:
        ...

    def select_top_k(self, eligible_arms: List[int], k: int,
                     context: Optional[Dict[int, np.ndarray]] = None) -> List[int]:
        """Pick up to *k* distinct arms. Default: sequential select_arm."""
        remaining = list(eligible_arms)
        selected: List[int] = []
        for _ in range(min(k, len(remaining))):
            arm = self.select_arm(remaining, context=context)
            selected.append(arm)
            remaining.remove(arm)
        return selected

    def update(self, arm_id: int, reward: float,
               context: Optional[np.ndarray] = None):
        self.ensure_arm(arm_id)
        arm = self.arms[arm_id]
        arm.pull_count += 1
        arm.total_reward += reward
        if self.step_size is not None:
            arm.q_estimate += self.step_size * (reward - arm.q_estimate)
        if reward > 0:
            arm.successes += 1
        else:
            arm.failures += 1

    def get_state(self) -> dict:
        return {
            "arms": {str(k): v.to_dict() for k, v in self.arms.items()},
            "step_size": self.step_size,
        }

    def load_state(self, state: dict):
        self.arms.clear()
        for k, v in state.get("arms", {}).items():
            self.arms[int(k)] = ArmStats.from_dict(v)
        self.step_size = state.get("step_size", self.step_size)

    def reset(self):
        self.arms.clear()


class EpsilonGreedyPolicy(BanditPolicy):
    def __init__(self, epsilon: float = 0.1, epsilon_decay: float = 0.995,
                 epsilon_min: float = 0.01, step_size: Optional[float] = None):
        super().__init__(step_size=step_size)
        self.epsilon = epsilon
        self.initial_epsilon = epsilon
        self.epsilon_decay = epsilon_decay
        self.epsilon_min = epsilon_min

    def select_arm(self, eligible_arms: List[int],
                   context: Optional[Dict[int, np.ndarray]] = None) -> int:
        if not eligible_arms:
            raise ValueError("No eligible arms to select from")

        for arm_id in eligible_arms:
            self.ensure_arm(arm_id)

        if random.random() < self.epsilon:
            return random.choice(eligible_arms)

        # Greedy: pick arm with highest Q-value (break ties randomly)
        best_q = max(self.q(a) for a in eligible_arms)
        best_arms = [a for a in eligible_arms if self.q(a) == best_q]
        return random.choice(best_arms)

    def update(self, arm_id: int, reward: float,
               context: Optional[np.ndarray] = None):
        super().update(arm_id, reward, context=context)
        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)

    def get_state(self) -> dict:
        state = super().get_state()
        state["epsilon"] = self.epsilon
        return state

    def load_state(self, state: dict):
        super().load_state(state)
        self.epsilon = state.get("epsilon", self.initial_epsilon)


class UCB1Policy(BanditPolicy):
    def __init__(self, exploration_weight: float = math.sqrt(2),
                 step_size: Optional[float] = None):
        super().__init__(step_size=step_size)
        self.exploration_weight = exploration_weight

    def select_arm(self, eligible_arms: List[int],
                   context: Optional[Dict[int, np.ndarray]] = None) -> int:
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
            exploitation = self.q(arm_id)
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


class LinUCBPolicy(BanditPolicy):
    """Shared-model LinUCB over per-(job, group) feature vectors.

    One ridge-regression model scores every candidate arm from its feature
    vector x_a: score(a) = theta . x_a + alpha * sqrt(x_a . A^-1 . x_a).
    A single shared model (rather than per-arm models) means newly added
    child groups are scored from their features immediately, and every
    delegation outcome trains the same model.

    A^-1 is maintained incrementally via Sherman-Morrison rank-1 updates
    (O(d^2) per update, no matrix inversion). ``discount`` (gamma) applies
    exponential forgetting so the model tracks non-stationary load;
    gamma = 1.0 recovers standard LinUCB.

    When no context is provided, select_arm falls back to uniform random
    choice and update records metrics counters only (the model is untouched).
    """

    def __init__(self, alpha: float = 1.0, discount: float = 0.995,
                 dim: Optional[int] = None, schema_version: Optional[str] = None):
        super().__init__()
        self.alpha = alpha
        self.discount = discount
        self.schema_version = schema_version
        self.dim: Optional[int] = None
        self.A_inv: Optional[np.ndarray] = None
        self.b: Optional[np.ndarray] = None
        if dim is not None:
            self._init_model(dim)

    def _init_model(self, dim: int):
        self.dim = dim
        self.A_inv = np.eye(dim)
        self.b = np.zeros(dim)

    def _ensure_dim(self, x: np.ndarray):
        if self.A_inv is None:
            self._init_model(len(x))
        elif len(x) != self.dim:
            raise ValueError(
                f"Context dimension {len(x)} does not match model dimension {self.dim}"
            )

    @property
    def theta(self) -> Optional[np.ndarray]:
        if self.A_inv is None:
            return None
        return self.A_inv @ self.b

    def _scores(self, eligible_arms: List[int],
                context: Dict[int, np.ndarray]) -> Dict[int, float]:
        theta = self.theta
        scores = {}
        for arm_id in eligible_arms:
            x = np.asarray(context[arm_id], dtype=float)
            # Uncertainty term can dip epsilon-negative from float drift
            width = math.sqrt(max(0.0, float(x @ self.A_inv @ x)))
            scores[arm_id] = float(theta @ x) + self.alpha * width
        return scores

    def _has_full_context(self, eligible_arms: List[int],
                          context: Optional[Dict[int, np.ndarray]]) -> bool:
        return context is not None and all(a in context for a in eligible_arms)

    def select_arm(self, eligible_arms: List[int],
                   context: Optional[Dict[int, np.ndarray]] = None) -> int:
        if not eligible_arms:
            raise ValueError("No eligible arms to select from")

        for arm_id in eligible_arms:
            self.ensure_arm(arm_id)

        if not self._has_full_context(eligible_arms, context):
            logger.debug("LinUCB: no/partial context, falling back to random choice")
            return random.choice(eligible_arms)

        self._ensure_dim(np.asarray(context[eligible_arms[0]], dtype=float))
        scores = self._scores(eligible_arms, context)
        best_score = max(scores.values())
        best_arms = [a for a in eligible_arms
                     if abs(scores[a] - best_score) < 1e-9]
        return random.choice(best_arms)

    def select_top_k(self, eligible_arms: List[int], k: int,
                     context: Optional[Dict[int, np.ndarray]] = None) -> List[int]:
        if not eligible_arms:
            return []
        if not self._has_full_context(eligible_arms, context):
            return super().select_top_k(eligible_arms, k, context=context)

        for arm_id in eligible_arms:
            self.ensure_arm(arm_id)
        self._ensure_dim(np.asarray(context[eligible_arms[0]], dtype=float))
        scores = self._scores(eligible_arms, context)
        ranked = sorted(eligible_arms, key=lambda a: scores[a], reverse=True)
        return ranked[:min(k, len(ranked))]

    def update(self, arm_id: int, reward: float,
               context: Optional[np.ndarray] = None):
        super().update(arm_id, reward)
        if context is None:
            # Delayed reward whose selection-time context was lost (e.g.
            # coordinator restart) — keep the metrics, skip the model.
            return

        x = np.asarray(context, dtype=float)
        self._ensure_dim(x)

        # A <- gamma*A + x x^T, applied to A^-1 via Sherman-Morrison:
        # (gamma*A + x x^T)^-1 = M - (M x)(M x)^T / (1 + x^T M x), M = A^-1/gamma
        m = self.A_inv / self.discount
        mx = m @ x
        self.A_inv = m - np.outer(mx, mx) / (1.0 + float(x @ mx))
        # Re-symmetrize to stop float drift accumulating over many updates
        self.A_inv = (self.A_inv + self.A_inv.T) / 2.0
        self.b = self.discount * self.b + reward * x

    def get_state(self) -> dict:
        state = super().get_state()
        state["alpha"] = self.alpha
        state["discount"] = self.discount
        state["dim"] = self.dim
        state["schema_version"] = self.schema_version
        state["A_inv"] = self.A_inv.tolist() if self.A_inv is not None else None
        state["b"] = self.b.tolist() if self.b is not None else None
        return state

    def load_state(self, state: dict):
        stored_version = state.get("schema_version")
        stored_dim = state.get("dim")

        if self.schema_version is not None and stored_version != self.schema_version:
            logger.warning(
                f"LinUCB: discarding persisted state — schema version mismatch "
                f"(stored={stored_version}, current={self.schema_version})"
            )
            return
        if self.dim is not None and stored_dim is not None and stored_dim != self.dim:
            logger.warning(
                f"LinUCB: discarding persisted state — dimension mismatch "
                f"(stored={stored_dim}, current={self.dim})"
            )
            return

        super().load_state(state)
        self.alpha = state.get("alpha", self.alpha)
        self.discount = state.get("discount", self.discount)
        if stored_dim is not None and state.get("A_inv") is not None:
            self.dim = stored_dim
            self.A_inv = np.array(state["A_inv"], dtype=float)
            self.b = np.array(state["b"], dtype=float)

    def reset(self):
        super().reset()
        if self.dim is not None:
            self._init_model(self.dim)


class LinTSPolicy(LinUCBPolicy):
    """Linear Thompson Sampling over the same shared model as LinUCB.

    Instead of an explicit uncertainty bonus, exploration comes from
    posterior sampling: each selection draws theta_tilde ~ N(theta,
    ts_variance * A^-1) and ranks arms by theta_tilde . x_a. Arms in
    well-explored feature regions get near-deterministic scores; uncertain
    regions get noisy draws and are therefore tried occasionally. Updates,
    persistence, discounting, and fallbacks are inherited from LinUCB.
    """

    def __init__(self, ts_variance: float = 0.25, discount: float = 0.995,
                 dim: Optional[int] = None, schema_version: Optional[str] = None):
        super().__init__(alpha=0.0, discount=discount, dim=dim,
                         schema_version=schema_version)
        self.ts_variance = float(ts_variance)

    def _scores(self, eligible_arms: List[int],
                context: Dict[int, np.ndarray]) -> Dict[int, float]:
        theta = self.theta
        if self.ts_variance > 0:
            try:
                # A_inv is kept symmetric; jitter guards float drift at the
                # PD boundary
                chol = np.linalg.cholesky(
                    self.A_inv + 1e-10 * np.eye(self.dim))
                z = np.random.standard_normal(self.dim)
                theta = theta + math.sqrt(self.ts_variance) * (chol @ z)
            except np.linalg.LinAlgError:
                logger.warning("LinTS: Cholesky failed, using mean theta")
        return {a: float(theta @ np.asarray(context[a], dtype=float))
                for a in eligible_arms}

    def get_state(self) -> dict:
        state = super().get_state()
        state["ts_variance"] = self.ts_variance
        return state

    def load_state(self, state: dict):
        super().load_state(state)
        self.ts_variance = state.get("ts_variance", self.ts_variance)
