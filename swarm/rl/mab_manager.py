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
import json
import logging
import threading
import time
from typing import List, Optional

from swarm.rl.bandit import BanditPolicy, EpsilonGreedyPolicy, UCB1Policy


class MABManager:
    """Bridges the Multi-Armed Bandit policy with the ResourceAgent hierarchy.

    Each coordinator agent (level >= 1) owns one MABManager.  Arms correspond
    to child-group IDs.  The manager exposes:
      - select_groups(): pick the best child groups for a given job
      - report_outcome(): feed success/failure reward back to the policy
      - save_state() / load_state(): persist to/from Redis
    """

    REWARD_SUCCESS = 1.0
    REWARD_FAILURE = -1.0

    def __init__(self, agent_id: int, child_groups: List[int],
                 config: dict, repository, logger: Optional[logging.Logger] = None):
        self.agent_id = agent_id
        self.child_groups = list(child_groups)
        self.config = config
        self.repository = repository
        self.logger = logger or logging.getLogger(f"mab.{agent_id}")
        self._lock = threading.Lock()
        self._last_persist_time = 0.0
        self._persist_interval = config.get("persist_interval_s", 30.0)
        self._persist_to_redis = config.get("persist_to_redis", False)

        self.policy = self._create_policy(config)

        # Ensure all child groups are registered as arms
        for group_id in self.child_groups:
            self.policy.ensure_arm(group_id)

        self.logger.info(
            f"MABManager initialised for agent {agent_id} with "
            f"{len(child_groups)} arms (groups: {child_groups}), "
            f"algorithm: {config.get('algorithm', 'epsilon_greedy')}"
        )

    @staticmethod
    def _create_policy(config: dict) -> BanditPolicy:
        algorithm = config.get("algorithm", "epsilon_greedy")
        if algorithm == "ucb1":
            return UCB1Policy(
                exploration_weight=config.get("exploration_weight", 1.41),
            )
        # default: epsilon-greedy
        return EpsilonGreedyPolicy(
            epsilon=config.get("epsilon", 0.1),
            epsilon_decay=config.get("epsilon_decay", 0.995),
            epsilon_min=config.get("epsilon_min", 0.01),
        )

    def select_groups(self, capable_groups: List[int], job=None,
                      top_k: int = 1) -> List[int]:
        """Use the bandit policy to pick *top_k* groups from *capable_groups*.

        If top_k >= len(capable_groups), all capable groups are returned
        (equivalent to the pre-MAB behaviour).
        """
        if not capable_groups:
            return []

        if top_k >= len(capable_groups):
            return list(capable_groups)

        with self._lock:
            selected: List[int] = []
            remaining = list(capable_groups)

            for _ in range(min(top_k, len(remaining))):
                arm = self.policy.select_arm(remaining)
                selected.append(arm)
                remaining.remove(arm)

            self.logger.debug(
                f"MAB selected groups {selected} from capable {capable_groups} "
                f"(job={getattr(job, 'job_id', None)})"
            )
            return selected

    def report_outcome(self, group_id: int, job_id: str, success: bool):
        """Feed a reward signal back to the bandit for *group_id*."""
        reward = self.REWARD_SUCCESS if success else self.REWARD_FAILURE
        with self._lock:
            self.policy.update(group_id, reward)

        self.logger.debug(
            f"MAB update: group={group_id}, job={job_id}, "
            f"success={success}, reward={reward}, "
            f"q_value={self.policy.arms[group_id].q_value:.3f}"
        )

        # Periodic persistence
        if self._persist_to_redis:
            now = time.time()
            if now - self._last_persist_time >= self._persist_interval:
                self.save_state()
                self._last_persist_time = now

    def _redis_key(self) -> str:
        return f"mab:{self.agent_id}"

    def save_state(self):
        """Persist bandit state to Redis."""
        if not self._persist_to_redis or self.repository is None:
            return
        try:
            with self._lock:
                state = self.policy.get_state()
            self.repository.save(obj=state, key=self._redis_key())
            self.logger.debug(f"MAB state persisted to Redis key {self._redis_key()}")
        except Exception as e:
            self.logger.warning(f"Failed to persist MAB state: {e}")

    def load_state(self):
        """Load bandit state from Redis if available."""
        if not self._persist_to_redis or self.repository is None:
            return
        try:
            state = self.repository.get(key=self._redis_key())
            if state:
                with self._lock:
                    self.policy.load_state(state)
                self.logger.info(f"MAB state loaded from Redis key {self._redis_key()}")
        except Exception as e:
            self.logger.warning(f"Failed to load MAB state: {e}")

    def get_stats(self) -> dict:
        """Return arm statistics for metrics/logging."""
        with self._lock:
            return {
                "algorithm": self.config.get("algorithm", "epsilon_greedy"),
                "arms": {
                    arm_id: arm.to_dict()
                    for arm_id, arm in self.policy.arms.items()
                },
                "policy_state": self.policy.get_state(),
            }
