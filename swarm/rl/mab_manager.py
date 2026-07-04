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
import threading
import time
from collections import deque
from dataclasses import dataclass, replace
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np

from swarm.rl.bandit import (BanditPolicy, EpsilonGreedyPolicy, LinUCBPolicy,
                             UCB1Policy)
from swarm.rl.context import ContextExtractor, GroupSnapshot


@dataclass
class _PendingSelection:
    """Selection-time state stashed for delayed-reward replay."""
    context: Optional[np.ndarray]
    job_type: Optional[str]
    selected_at: float


class MABManager:
    """Bridges the Multi-Armed Bandit policy with the ResourceAgent hierarchy.

    Each coordinator agent (level >= 1) owns one MABManager.  Arms correspond
    to child-group IDs.  The manager exposes:
      - select_groups(): pick the best child groups for a given job
      - report_outcome(): feed success/failure reward back to the policy
      - save_state() / load_state(): persist to/from Redis

    Contextual mode (``algorithm: linucb``): a ContextExtractor builds one
    feature vector per (job, group) candidate. Because rewards arrive minutes
    after selection via the delegation monitor, the chosen vectors are stashed
    in a pending map keyed by (job_id, group_id) and replayed into
    policy.update() when report_outcome() fires. The manager also maintains
    per-(group, job-type) failure-rate sliding windows that feed the
    grp_type_failure_rate interaction feature — the signal that lets the
    model route job types away from groups that fail them.

    ``group_snapshot_provider``: optional zero-arg callable returning
    {group_id: GroupSnapshot} with load/capacity data (wired by the agent in
    Phase 3). Failure-rate fields are always overwritten from the manager's
    own outcome windows.
    """

    REWARD_SUCCESS = 1.0
    REWARD_FAILURE = -1.0

    def __init__(self, agent_id: int, child_groups: List[int],
                 config: dict, repository, logger: Optional[logging.Logger] = None,
                 group_snapshot_provider: Optional[Callable[[], Dict[int, GroupSnapshot]]] = None,
                 delegation_timeout_s: float = 60.0):
        self.agent_id = agent_id
        self.child_groups = list(child_groups)
        self.config = config
        self.repository = repository
        self.logger = logger or logging.getLogger(f"mab.{agent_id}")
        self._lock = threading.Lock()
        self._last_persist_time = 0.0
        self._persist_interval = config.get("persist_interval_s", 30.0)
        self._persist_to_redis = config.get("persist_to_redis", False)

        self._snapshot_provider = group_snapshot_provider
        self.delegation_timeout_s = max(delegation_timeout_s, 1e-6)
        self._pending_ttl_s = config.get("pending_ttl_s", 2 * delegation_timeout_s)

        reward_cfg = config.get("reward", {})
        self._reward_shaped = reward_cfg.get("shaped", False)
        self._reward_exit_failure = reward_cfg.get("exit_failure", -0.5)
        self._reward_timeout = reward_cfg.get("timeout", -1.0)
        self._reward_non_winner = reward_cfg.get("non_winner", None)

        self.extractor: Optional[ContextExtractor] = None
        self.policy = self._create_policy(config)

        # Delayed-reward replay: job_id -> {group_id: _PendingSelection}
        self._pending: Dict[str, Dict[int, _PendingSelection]] = {}
        # Outcome sliding windows (1.0 = failure): per group and per (group, type)
        window_len = int(config.get("context", {}).get("failure_window", 20))
        self._window_len = max(window_len, 1)
        self._group_windows: Dict[int, deque] = {}
        self._type_windows: Dict[Tuple[int, str], deque] = {}

        # Ensure all child groups are registered as arms
        for group_id in self.child_groups:
            self.policy.ensure_arm(group_id)

        self.logger.info(
            f"MABManager initialised for agent {agent_id} with "
            f"{len(child_groups)} arms (groups: {child_groups}), "
            f"algorithm: {config.get('algorithm', 'epsilon_greedy')}"
            + (f", context dim {self.extractor.dim} "
               f"(schema {self.extractor.schema_version})"
               if self.extractor else "")
        )

    def _create_policy(self, config: dict) -> BanditPolicy:
        algorithm = config.get("algorithm", "epsilon_greedy")
        step_size = config.get("step_size", None)
        if algorithm == "linucb":
            self.extractor = ContextExtractor(config.get("context", {}))
            linucb_cfg = config.get("linucb", {})
            return LinUCBPolicy(
                alpha=linucb_cfg.get("alpha", 1.0),
                discount=linucb_cfg.get("discount", 0.995),
                dim=self.extractor.dim,
                schema_version=self.extractor.schema_version,
            )
        if algorithm == "ucb1":
            return UCB1Policy(
                exploration_weight=config.get("exploration_weight", 1.41),
                step_size=step_size,
            )
        # default: epsilon-greedy
        return EpsilonGreedyPolicy(
            epsilon=config.get("epsilon", 0.1),
            epsilon_decay=config.get("epsilon_decay", 0.995),
            epsilon_min=config.get("epsilon_min", 0.01),
            step_size=step_size,
        )

    @property
    def contextual(self) -> bool:
        return self.extractor is not None

    @staticmethod
    def _window_rate(window: Optional[deque]) -> float:
        if not window:
            return 0.0
        return sum(window) / len(window)

    def _build_snapshots(self, group_ids: List[int]) -> Dict[int, GroupSnapshot]:
        """Provider load data merged with manager-owned failure windows."""
        base: Dict[int, GroupSnapshot] = {}
        if self._snapshot_provider is not None:
            try:
                base = self._snapshot_provider() or {}
            except Exception as e:
                self.logger.warning(f"Group snapshot provider failed: {e}")

        default = GroupSnapshot()
        snapshots = {}
        for g in group_ids:
            snapshots[g] = replace(
                base.get(g, default),
                failure_rate=self._window_rate(self._group_windows.get(g)),
                type_failure_rates={
                    t: self._window_rate(w)
                    for (gg, t), w in self._type_windows.items() if gg == g
                },
            )
        return snapshots

    def _sweep_pending(self, now: float):
        """Drop pending selections whose reward never arrived (lost jobs,
        lost leadership) so the map cannot grow without bound."""
        expired_jobs = []
        for job_id, entries in self._pending.items():
            stale = [g for g, e in entries.items()
                     if now - e.selected_at > self._pending_ttl_s]
            for g in stale:
                del entries[g]
            if not entries:
                expired_jobs.append(job_id)
        for job_id in expired_jobs:
            del self._pending[job_id]

    def select_groups(self, capable_groups: List[int], job=None,
                      top_k: int = 1) -> List[int]:
        """Use the bandit policy to pick *top_k* groups from *capable_groups*.

        If top_k >= len(capable_groups), all capable groups are returned
        (equivalent to the pre-MAB behaviour), but selection context is still
        recorded so their outcomes train the contextual model.
        """
        if not capable_groups:
            return []

        job_id = getattr(job, "job_id", None)
        job_type = getattr(job, "job_type", None)
        now = time.time()

        with self._lock:
            self._sweep_pending(now)

            contexts = None
            if self.contextual and job is not None:
                snapshots = self._build_snapshots(capable_groups)
                contexts = self.extractor.build(job, capable_groups, snapshots)

            if top_k >= len(capable_groups):
                selected = list(capable_groups)
            else:
                selected = self.policy.select_top_k(
                    capable_groups, top_k, context=contexts)

            if self.contextual and job_id is not None:
                entries = self._pending.setdefault(job_id, {})
                for g in selected:
                    entries[g] = _PendingSelection(
                        context=contexts[g] if contexts else None,
                        job_type=job_type,
                        selected_at=now,
                    )

            self.logger.debug(
                f"MAB selected groups {selected} from capable {capable_groups} "
                f"(job={job_id})"
            )
            return selected

    def _shape_reward(self, success: bool, latency_s: Optional[float],
                      timed_out: bool) -> float:
        if not self._reward_shaped:
            return self.REWARD_SUCCESS if success else self.REWARD_FAILURE
        if success:
            if latency_s is None:
                return self.REWARD_SUCCESS
            return 1.0 - min(1.0, max(0.0, latency_s) / self.delegation_timeout_s)
        return self._reward_timeout if timed_out else self._reward_exit_failure

    def report_outcome(self, group_id: int, job_id: str, success: bool,
                       latency_s: Optional[float] = None,
                       timed_out: bool = False):
        """Feed a reward signal back to the bandit for *group_id*.

        *latency_s* (delegation-to-completion) sharpens the reward when
        ``reward.shaped`` is on; *timed_out* distinguishes delegation
        timeouts from non-zero exit status. An outcome is terminal for the
        job: pending entries for other groups selected for the same job are
        resolved too (``reward.non_winner``, default: dropped with no update).
        """
        reward = self._shape_reward(success, latency_s, timed_out)

        with self._lock:
            entry = self._pending.get(job_id, {}).pop(group_id, None)
            siblings = self._pending.pop(job_id, {})

            self.policy.update(group_id, reward,
                               context=entry.context if entry else None)

            if self.contextual:
                outcome = 0.0 if success else 1.0
                self._group_windows.setdefault(
                    group_id, deque(maxlen=self._window_len)).append(outcome)
                job_type = entry.job_type if entry else None
                if job_type is not None:
                    self._type_windows.setdefault(
                        (group_id, job_type),
                        deque(maxlen=self._window_len)).append(outcome)

                if self._reward_non_winner is not None:
                    for g, e in siblings.items():
                        self.policy.update(g, self._reward_non_winner,
                                           context=e.context)

            q_value = self.policy.arms[group_id].q_value

        self.logger.debug(
            f"MAB update: group={group_id}, job={job_id}, "
            f"success={success}, reward={reward:.3f}, "
            f"q_value={q_value:.3f}"
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
        """Load bandit state from Redis if available.

        For LinUCB, the policy itself discards state whose schema version or
        dimension does not match the current ContextExtractor layout.
        """
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
            stats = {
                "algorithm": self.config.get("algorithm", "epsilon_greedy"),
                "arms": {
                    arm_id: arm.to_dict()
                    for arm_id, arm in self.policy.arms.items()
                },
                "policy_state": self.policy.get_state(),
            }
            if self.contextual:
                theta = self.policy.theta
                stats["context"] = {
                    "schema_version": self.extractor.schema_version,
                    "feature_names": self.extractor.feature_names,
                    "theta": theta.tolist() if theta is not None else None,
                    "pending_selections": sum(
                        len(v) for v in self._pending.values()),
                    "group_failure_rates": {
                        g: self._window_rate(w)
                        for g, w in self._group_windows.items()
                    },
                    "type_failure_rates": {
                        f"{g}:{t}": self._window_rate(w)
                        for (g, t), w in self._type_windows.items()
                    },
                }
            return stats
