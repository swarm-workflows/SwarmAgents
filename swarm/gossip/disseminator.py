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
"""
Epidemic (push-style) state-dissemination layer.

Each agent maintains a local cache of versioned ``AgentStateEntry`` records and
pushes its current view to ``fanout`` random peers every round. Receivers merge
by version (higher wins); entries expire after ``state_ttl_s``.

The layer is intentionally orthogonal to consensus: the Snow engine (Phase 3)
queries it for peer cost/load when scoring candidates, but ``ResourceAgent``
can also consult it directly as a faster substitute for Redis lookups.
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional, Protocol

from swarm.consensus.messages.agent_state_entry import AgentStateEntry
from swarm.consensus.messages.gossip_state import GossipState


class GossipHost(Protocol):
    """Surface the agent must expose to drive gossip."""

    agent_id: int

    def send(self, dest: int, payload: object) -> None: ...
    def live_peers(self) -> Iterable[int]: ...
    def log_debug(self, msg: str) -> None: ...
    def log_warn(self, msg: str) -> None: ...


@dataclass
class _CachedEntry:
    entry: AgentStateEntry
    received_at: float


class GossipStateDisseminator:
    """
    Push-based gossip with bounded staleness.

    Threading: one daemon thread runs the periodic push. All public APIs are
    safe to call concurrently — an internal RLock guards the cache.
    """

    def __init__(
        self,
        host: GossipHost,
        period_s: float = 1.0,
        fanout: int = 3,
        state_ttl_s: float = 30.0,
        time_fn: Callable[[], float] = time.time,
    ):
        self.host = host
        self.period_s = float(period_s)
        self.fanout = int(fanout)
        self.state_ttl_s = float(state_ttl_s)
        self._time = time_fn

        self._lock = threading.RLock()
        self._cache: Dict[int, _CachedEntry] = {}
        self._self_version: int = 0

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ---- Lifecycle ------------------------------------------------------- #

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name=f"gossip-{self.host.agent_id}", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.period_s * 2)
            self._thread = None

    # ---- Local-state publication ---------------------------------------- #

    def publish_local(self, *, cpu_util: float = 0.0, ram_util: float = 0.0,
                      disk_util: float = 0.0, gpu_util: float = 0.0,
                      load: float = 0.0) -> AgentStateEntry:
        """Register or refresh this agent's own entry. Bumps the version so
        peers' merge logic accepts the update."""
        with self._lock:
            self._self_version += 1
            entry = AgentStateEntry(
                agent_id=int(self.host.agent_id),
                cpu_util=cpu_util,
                ram_util=ram_util,
                disk_util=disk_util,
                gpu_util=gpu_util,
                load=load,
                version=self._self_version,
            )
            self._cache[int(self.host.agent_id)] = _CachedEntry(
                entry=entry, received_at=self._time()
            )
            return entry

    # ---- Read API -------------------------------------------------------- #

    def get(self, agent_id: int) -> Optional[AgentStateEntry]:
        with self._lock:
            cached = self._cache.get(int(agent_id))
            return cached.entry if cached else None

    def snapshot(self) -> List[AgentStateEntry]:
        """Return all unexpired entries (not including expired-but-not-evicted)."""
        with self._lock:
            now = self._time()
            return [
                c.entry for c in self._cache.values()
                if (now - c.received_at) <= self.state_ttl_s
            ]

    # ---- Incoming message ------------------------------------------------ #

    def on_gossip(self, msg: GossipState) -> None:
        for e in msg.entries or ():
            if e is None or e.agent_id is None:
                continue
            self._merge_entry(e)

    def _merge_entry(self, entry: AgentStateEntry) -> None:
        with self._lock:
            existing = self._cache.get(int(entry.agent_id))
            if existing is None or entry.version > existing.entry.version:
                # Don't accept stale snapshots about ourselves (we own that record).
                if int(entry.agent_id) == int(self.host.agent_id):
                    if entry.version <= self._self_version:
                        return
                    self._self_version = int(entry.version)
                self._cache[int(entry.agent_id)] = _CachedEntry(
                    entry=entry, received_at=self._time()
                )

    # ---- Protocol loop --------------------------------------------------- #

    def _run(self) -> None:
        next_tick = self._time()
        while not self._stop.is_set():
            try:
                self._evict_expired()
                self._push_round()
            except Exception as exc:  # pragma: no cover - defensive
                self.host.log_warn(f"gossip tick error: {exc}")
            next_tick += self.period_s
            sleep_for = max(0.0, next_tick - self._time())
            if self._stop.wait(timeout=sleep_for):
                return

    def _evict_expired(self) -> None:
        now = self._time()
        with self._lock:
            stale = [
                aid for aid, c in self._cache.items()
                if aid != int(self.host.agent_id)
                and (now - c.received_at) > self.state_ttl_s
            ]
            for aid in stale:
                del self._cache[aid]
        if stale:
            self.host.log_debug(f"[gossip] evicted stale entries: {stale}")

    def _push_round(self) -> None:
        targets = self._pick_targets()
        if not targets:
            return
        with self._lock:
            entries = [c.entry for c in self._cache.values()]
        if not entries:
            return
        msg = GossipState(source=self.host.agent_id, entries=entries)
        for dest in targets:
            try:
                self.host.send(dest, msg)
            except Exception as exc:
                self.host.log_debug(f"[gossip] send -> {dest} failed: {exc}")

    def _pick_targets(self) -> List[int]:
        try:
            peers = [
                int(p) for p in self.host.live_peers()
                if int(p) != int(self.host.agent_id)
            ]
        except Exception:
            peers = []
        if not peers:
            return []
        if len(peers) <= self.fanout:
            return peers
        return random.sample(peers, self.fanout)
