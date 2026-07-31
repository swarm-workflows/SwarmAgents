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

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Author: Komal Thareja(kthare10@renci.org)
"""
Recovery state machine for failed agents (ROADMAP item 14).

Failure detection marks an agent failed and removes it from the neighbor map;
this module decides when a failed agent may rejoin. Two kinds of recovery
evidence are distinguished:

- **Passive (heartbeat freshness)**: the agent's Redis record is being updated
  again. Because a single fresh timestamp can be a dying gasp or clock jitter,
  promotion requires the record to stay fresh for a *grace window*
  (``grace_seconds``). A stale observation inside the window resets it.
- **Direct (we heard from it)**: a gRPC channel to the agent came back up, or
  SWIM received an ack/alive for it. Direct evidence promotes immediately.

The tracker manages only the recovery decision; the caller owns the side
effects of rejoin (re-adding to neighbor maps, quorum accounting, metrics).
"""

import threading
from typing import Dict, Optional


class RecoveryTracker:
    """Decides when agents in a failed set may rejoin.

    The tracker shares the caller's *failed* mapping (agent_id ->
    failure_timestamp): failure detection writes into it, and this class is
    the only thing that removes entries from it.
    """

    def __init__(self, failed_agents, grace_seconds: float = 10.0):
        """
        :param failed_agents: mapping of agent_id -> failure timestamp; must
            support ``__contains__``, ``remove(key)`` (ThreadSafeDict) or
            ``__delitem__`` (plain dict).
        :param grace_seconds: how long a failed agent's heartbeat must stay
            fresh before passive promotion.
        """
        self._failed = failed_agents
        self.grace_seconds = float(grace_seconds)
        self._recovering: Dict[int, float] = {}  # agent_id -> first fresh ts
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------

    def _remove_failed(self, agent_id: int) -> None:
        remove = getattr(self._failed, "remove", None)
        if callable(remove):
            remove(agent_id)
        else:
            del self._failed[agent_id]

    # ------------------------------------------------------------------
    # observations
    # ------------------------------------------------------------------

    def observe(self, agent_id: int, last_updated: float, fresh: bool,
                now: float) -> bool:
        """Feed one heartbeat observation for a failed agent.

        Recovery evidence is **progress**, not recency: the record must have
        been written *after* the failure was declared (``last_updated`` past
        the stored failure timestamp) and still be fresh. A dead agent's
        lingering record is recent for up to the failure threshold after the
        crash but never advances past the failure timestamp, so it can not
        promote. (Assumes the usual same-cluster clock discipline; a live
        agent heartbeats every tick, so modest skew only delays promotion.)

        :param last_updated: the agent record's own update timestamp.
        :param fresh: True when the record was updated within the failure
            threshold.
        :param now: current timestamp.
        :return: True exactly once, when the agent is promoted (removed from
            the failed set). False otherwise, including for agents that are
            not failed.
        """
        with self._lock:
            if agent_id not in self._failed:
                self._recovering.pop(agent_id, None)
                return False

            failed_at = self._failed.get(agent_id) or 0.0
            if not fresh or last_updated <= failed_at:
                # No post-failure write yet (or gone stale again): reset.
                self._recovering.pop(agent_id, None)
                return False

            first_fresh = self._recovering.get(agent_id)
            if first_fresh is None:
                self._recovering[agent_id] = now
                return False

            if (now - first_fresh) >= self.grace_seconds:
                self._recovering.pop(agent_id, None)
                self._remove_failed(agent_id)
                return True
            return False

    def recover_direct(self, agent_id: int) -> bool:
        """Promote immediately on direct evidence (channel up / SWIM ack).

        :return: True if the agent was failed and is now recovered.
        """
        with self._lock:
            if agent_id not in self._failed:
                self._recovering.pop(agent_id, None)
                return False
            self._recovering.pop(agent_id, None)
            self._remove_failed(agent_id)
            return True

    # ------------------------------------------------------------------
    # introspection
    # ------------------------------------------------------------------

    def recovering_since(self, agent_id: int) -> Optional[float]:
        """First-fresh timestamp for an agent inside its grace window."""
        with self._lock:
            return self._recovering.get(agent_id)

    def recovering_count(self) -> int:
        with self._lock:
            return len(self._recovering)
