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
SWIM membership protocol implementation.

Reference: Das, Gupta, Motivala. "SWIM: Scalable Weakly-consistent Infection-style
Process Group Membership Protocol." DSN 2002.

This module is framework-agnostic. The host agent provides a small adapter
(see ``SwimHost``) for sending messages, listing candidate peers, scheduling time,
and receiving membership-change callbacks. The intended integration point is
``ResourceAgent``, which can run SWIM alongside the existing heartbeat mechanism
during transition (Phase 1 of GOSSIP_CONSENSUS_DESIGN.md).
"""

from __future__ import annotations

import random
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Iterable, List, Optional, Protocol, Tuple

from swarm.consensus.messages.membership_update import MembershipUpdate
from swarm.consensus.messages.swim_ack import SwimAck
from swarm.consensus.messages.swim_ping import SwimPing
from swarm.consensus.messages.swim_ping_req import SwimPingReq


# -------- Status helpers --------------------------------------------------- #

ALIVE = MembershipUpdate.STATUS_ALIVE
JOINED = MembershipUpdate.STATUS_JOINED
SUSPECT = MembershipUpdate.STATUS_SUSPECT
FAILED = MembershipUpdate.STATUS_FAILED

# Rank for merge: failed dominates suspect dominates alive/joined.
_STATUS_RANK = {ALIVE: 0, JOINED: 0, SUSPECT: 1, FAILED: 2}


def _rank(status: str) -> int:
    return _STATUS_RANK.get(status, -1)


# -------- Host adapter protocol -------------------------------------------- #

class SwimHost(Protocol):
    """
    Surface the agent must expose to drive SWIM. Mirrors the
    ``ConsensusHost`` style used by the PBFT engine.
    """

    agent_id: int

    def send(self, dest: int, payload: object) -> None: ...
    def known_peers(self) -> Iterable[int]: ...   # candidates for bootstrap; includes failed-removed
    def log_debug(self, msg: str) -> None: ...
    def log_info(self, msg: str) -> None: ...
    def log_warn(self, msg: str) -> None: ...

    # Membership-change callbacks (best-effort; should not block).
    def on_agent_alive(self, agent_id: int) -> None: ...
    def on_agent_joined(self, agent_id: int) -> None: ...
    def on_agent_suspected(self, agent_id: int) -> None: ...
    def on_agent_failed(self, agent_id: int) -> None: ...


# -------- Internal member state ------------------------------------------- #

@dataclass
class _Member:
    agent_id: int
    status: str = ALIVE
    incarnation: int = 0
    suspect_since: float = 0.0  # epoch seconds; 0 if not suspect

    def is_alive(self) -> bool:
        return self.status in (ALIVE, JOINED)


@dataclass
class _DirectProbe:
    probe_id: str
    target: int
    deadline: float


@dataclass
class _IndirectProbe:
    probe_id: str
    target: int
    deadline: float


@dataclass
class _PiggyEntry:
    update: MembershipUpdate
    remaining_broadcasts: int


# -------- SWIM membership manager ----------------------------------------- #

class SwimMembership:
    """
    SWIM failure detector with infection-style update dissemination.

    Threading model: a single daemon thread drives the protocol loop. All public
    on_* handlers (called from the agent's inbound message thread) must be
    safe to invoke concurrently with the protocol thread; an internal lock
    guards membership state and pending probes.
    """

    def __init__(
        self,
        host: SwimHost,
        period_s: float = 1.0,
        probe_timeout_s: float = 0.3,
        k_req: int = 3,
        suspect_timeout_s: float = 8.0,
        piggyback_count: int = 3,
        piggyback_max_per_msg: int = 8,
        time_fn: Callable[[], float] = time.time,
    ):
        self.host = host
        self.period_s = float(period_s)
        self.probe_timeout_s = float(probe_timeout_s)
        self.k_req = int(k_req)
        self.suspect_timeout_s = float(suspect_timeout_s)
        self.piggyback_count = int(piggyback_count)
        self.piggyback_max_per_msg = int(piggyback_max_per_msg)
        self._time = time_fn

        self._lock = threading.RLock()
        self._members: dict[int, _Member] = {}
        self._direct_probes: dict[str, _DirectProbe] = {}
        # probe_id -> set of relay agent_ids that have responded
        self._indirect_acked: dict[str, bool] = {}
        self._indirect_probes: dict[str, _IndirectProbe] = {}
        self._piggy: List[_PiggyEntry] = []
        # Relay state: probe_id -> initiator agent_id (used when this node is
        # acting as a relay for someone else's ping-req).
        self._relay_initiators: dict[str, int] = {}

        self._self_incarnation = 0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Seed self as alive.
        self._members[int(host.agent_id)] = _Member(
            agent_id=int(host.agent_id),
            status=ALIVE,
            incarnation=0,
        )

    # ---- Lifecycle -------------------------------------------------------- #

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name=f"swim-{self.host.agent_id}", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.period_s * 2)
            self._thread = None

    # ---- Public observation API ------------------------------------------ #

    def live_agents(self) -> List[int]:
        """Agents currently considered alive or joined (includes self)."""
        with self._lock:
            return [m.agent_id for m in self._members.values() if m.is_alive()]

    def failed_agents(self) -> List[int]:
        """Agents currently marked FAILED. Deliberately excludes SUSPECT — suspected
        peers must keep receiving traffic so they can refute the suspicion."""
        with self._lock:
            return [m.agent_id for m in self._members.values() if m.status == FAILED]

    def status_of(self, agent_id: int) -> Optional[str]:
        with self._lock:
            m = self._members.get(int(agent_id))
            return m.status if m else None

    def snapshot(self) -> List[Tuple[int, str, int]]:
        """(agent_id, status, incarnation) tuples for diagnostics."""
        with self._lock:
            return [(m.agent_id, m.status, m.incarnation) for m in self._members.values()]

    # ---- Incoming-message handlers (called from inbound thread) ---------- #

    def on_ping(self, msg: SwimPing) -> None:
        self._absorb_updates(msg.updates)
        sender = int(msg.source) if msg.source is not None else None
        if sender is None:
            return
        # Reply directly with an ack covering ourselves.
        ack = SwimAck(
            source=self.host.agent_id,
            probe_id=msg.probe_id,
            target_agent=self.host.agent_id,
            updates=self._fresh_piggy(),
        )
        self._safe_send(sender, ack)

    def on_ack(self, msg: SwimAck) -> None:
        self._absorb_updates(msg.updates)
        target = int(msg.target_agent) if msg.target_agent is not None else None
        with self._lock:
            direct = self._direct_probes.pop(msg.probe_id, None)
            indirect = self._indirect_probes.pop(msg.probe_id, None)
        if direct is not None:
            self._mark_alive(direct.target, reason="direct-ack")
        elif indirect is not None:
            self._indirect_acked[msg.probe_id] = True
            self._mark_alive(indirect.target, reason="indirect-ack")
        elif target is not None:
            # Late ack arriving after we already cleared the probe; still informative.
            self._mark_alive(target, reason="late-ack")

    def on_ping_req(self, msg: SwimPingReq) -> None:
        """Act as relay: ping the target on behalf of the initiator."""
        self._absorb_updates(msg.updates)
        initiator = int(msg.source) if msg.source is not None else None
        target = int(msg.target_agent) if msg.target_agent is not None else None
        if initiator is None or target is None or target == self.host.agent_id:
            return
        relay_probe_id = msg.probe_id  # reuse so the initiator can correlate
        # Send a ping to the target; if it acks we'll relay the ack.
        ping = SwimPing(
            source=self.host.agent_id,
            probe_id=relay_probe_id,
            updates=self._fresh_piggy(),
        )
        # Track the relay so a subsequent SwimAck triggers a relayed reply.
        with self._lock:
            self._indirect_probes[relay_probe_id] = _IndirectProbe(
                probe_id=relay_probe_id,
                target=target,
                deadline=self._time() + self.probe_timeout_s,
            )
            # Remember initiator so the ack-relay knows where to forward.
            self._indirect_acked.setdefault(relay_probe_id, False)
            self._relay_initiators[relay_probe_id] = initiator  # type: ignore[attr-defined]
        self._safe_send(target, ping)

    # ---- Protocol thread ------------------------------------------------- #

    def _run(self) -> None:
        next_tick = self._time()
        while not self._stop.is_set():
            now = self._time()
            try:
                self._handle_expirations(now)
                self._maybe_send_probe(now)
            except Exception as exc:  # pragma: no cover - defensive
                self.host.log_warn(f"SWIM tick error: {exc}")
            next_tick += self.period_s
            sleep_for = max(0.0, next_tick - self._time())
            if self._stop.wait(timeout=sleep_for):
                return

    def _maybe_send_probe(self, now: float) -> None:
        target = self._pick_probe_target()
        if target is None:
            return
        probe_id = uuid.uuid4().hex
        with self._lock:
            self._direct_probes[probe_id] = _DirectProbe(
                probe_id=probe_id, target=target, deadline=now + self.probe_timeout_s
            )
        ping = SwimPing(
            source=self.host.agent_id,
            probe_id=probe_id,
            updates=self._fresh_piggy(),
        )
        self.host.log_debug(f"[SWIM] ping -> {target} ({probe_id[:8]})")
        self._safe_send(target, ping)

    def _handle_expirations(self, now: float) -> None:
        # Direct probes -> ping-req fanout
        expired_direct: List[_DirectProbe] = []
        with self._lock:
            for pid, probe in list(self._direct_probes.items()):
                if probe.deadline <= now:
                    expired_direct.append(probe)
                    del self._direct_probes[pid]

        for probe in expired_direct:
            relays = self._pick_relays(exclude=probe.target)
            if not relays:
                # No relays available: mark suspect immediately.
                self._mark_suspect(probe.target, reason="no-relays")
                continue
            req = SwimPingReq(
                source=self.host.agent_id,
                probe_id=probe.probe_id,
                target_agent=probe.target,
                updates=self._fresh_piggy(),
            )
            with self._lock:
                self._indirect_probes[probe.probe_id] = _IndirectProbe(
                    probe_id=probe.probe_id,
                    target=probe.target,
                    deadline=now + self.probe_timeout_s,
                )
                self._indirect_acked[probe.probe_id] = False
            for relay in relays:
                self._safe_send(relay, req)

        # Indirect probes -> suspect on timeout
        with self._lock:
            timed_out = []
            for pid, probe in list(self._indirect_probes.items()):
                if probe.deadline <= now and not self._indirect_acked.get(pid, False):
                    timed_out.append(probe)
                    del self._indirect_probes[pid]
                    self._indirect_acked.pop(pid, None)
                    self._relay_initiators.pop(pid, None)
        for probe in timed_out:
            self._mark_suspect(probe.target, reason="indirect-timeout")

        # Suspects -> failed
        with self._lock:
            to_fail = []
            for m in self._members.values():
                if m.status == SUSPECT and m.suspect_since > 0 and \
                        now - m.suspect_since >= self.suspect_timeout_s:
                    to_fail.append(m.agent_id)
        for aid in to_fail:
            self._mark_failed(aid, reason="suspect-timeout")

        # Relay ack forwarding: any indirect-probe ack should be re-emitted to
        # the original initiator. We do this opportunistically here for any
        # relay slots that have observed a true ack.
        for pid, acked in list(self._indirect_acked.items()):
            initiator = self._relay_initiators.get(pid)
            if acked and initiator is not None and initiator != self.host.agent_id:
                ack = SwimAck(
                    source=self.host.agent_id,
                    probe_id=pid,
                    target_agent=self._indirect_probes.get(pid).target  # type: ignore[union-attr]
                    if pid in self._indirect_probes else None,
                    updates=self._fresh_piggy(),
                )
                self._safe_send(initiator, ack)
                self._relay_initiators.pop(pid, None)

    # ---- Membership transitions ----------------------------------------- #

    def _mark_alive(self, agent_id: int, reason: str = "") -> None:
        if agent_id == self.host.agent_id:
            return
        joined = False
        with self._lock:
            m = self._members.get(agent_id)
            if m is None:
                m = _Member(agent_id=agent_id, status=JOINED, incarnation=0)
                self._members[agent_id] = m
                joined = True
            else:
                if m.status in (SUSPECT, FAILED):
                    self.host.log_info(
                        f"[SWIM] {agent_id} -> alive ({reason}); was {m.status}"
                    )
                m.status = ALIVE
                m.suspect_since = 0.0
            self._enqueue_piggy(MembershipUpdate(
                agent_id=agent_id, status=ALIVE, incarnation=m.incarnation
            ))
        try:
            if joined:
                self.host.on_agent_joined(agent_id)
            self.host.on_agent_alive(agent_id)
        except Exception as exc:
            self.host.log_warn(f"SWIM callback (alive) raised: {exc}")

    def _mark_suspect(self, agent_id: int, reason: str = "") -> None:
        if agent_id == self.host.agent_id:
            return
        notify = False
        with self._lock:
            m = self._members.setdefault(
                agent_id, _Member(agent_id=agent_id, status=ALIVE)
            )
            if m.status == FAILED:
                return
            if m.status != SUSPECT:
                m.status = SUSPECT
                m.suspect_since = self._time()
                self.host.log_info(f"[SWIM] {agent_id} -> suspect ({reason})")
                notify = True
            self._enqueue_piggy(MembershipUpdate(
                agent_id=agent_id, status=SUSPECT, incarnation=m.incarnation
            ))
        if notify:
            try:
                self.host.on_agent_suspected(agent_id)
            except Exception as exc:
                self.host.log_warn(f"SWIM callback (suspect) raised: {exc}")

    def _mark_failed(self, agent_id: int, reason: str = "") -> None:
        if agent_id == self.host.agent_id:
            return
        notify = False
        with self._lock:
            m = self._members.get(agent_id)
            if m is None or m.status == FAILED:
                return
            m.status = FAILED
            self.host.log_warn(f"[SWIM] {agent_id} -> FAILED ({reason})")
            self._enqueue_piggy(MembershipUpdate(
                agent_id=agent_id, status=FAILED, incarnation=m.incarnation
            ))
            notify = True
        if notify:
            try:
                self.host.on_agent_failed(agent_id)
            except Exception as exc:
                self.host.log_warn(f"SWIM callback (failed) raised: {exc}")

    # ---- Update merge / refutation -------------------------------------- #

    def _absorb_updates(self, updates: Iterable[MembershipUpdate]) -> None:
        for upd in updates or ():
            if upd is None or upd.agent_id is None or upd.status is None:
                continue
            if int(upd.agent_id) == int(self.host.agent_id):
                self._maybe_refute(upd)
                continue
            self._merge_one(int(upd.agent_id), upd.status, int(upd.incarnation or 0))

    def _maybe_refute(self, upd: MembershipUpdate) -> None:
        """If a peer claims we're suspect/failed at an incarnation <= our current,
        bump our incarnation and emit an Alive update."""
        if upd.status not in (SUSPECT, FAILED):
            return
        with self._lock:
            current = self._self_incarnation
            if int(upd.incarnation or 0) >= current:
                self._self_incarnation = int(upd.incarnation or 0) + 1
            self._enqueue_piggy(MembershipUpdate(
                agent_id=self.host.agent_id, status=ALIVE,
                incarnation=self._self_incarnation,
            ))
        self.host.log_info(
            f"[SWIM] refuting {upd.status} claim at inc={upd.incarnation}; "
            f"bumped self incarnation to {self._self_incarnation}"
        )

    def _merge_one(self, agent_id: int, status: str, incarnation: int) -> None:
        with self._lock:
            m = self._members.get(agent_id)
            if m is None:
                self._members[agent_id] = _Member(
                    agent_id=agent_id,
                    status=status if status in (ALIVE, JOINED, SUSPECT, FAILED) else ALIVE,
                    incarnation=incarnation,
                    suspect_since=self._time() if status == SUSPECT else 0.0,
                )
                # Mirror into piggyback so neighbors learn through us.
                self._enqueue_piggy(MembershipUpdate(
                    agent_id=agent_id, status=status, incarnation=incarnation
                ))
                joined_callback = status in (ALIVE, JOINED)
            else:
                if _rank(status) > _rank(m.status) or \
                        (_rank(status) == _rank(m.status) and incarnation > m.incarnation):
                    prior = m.status
                    m.status = status if status in (ALIVE, JOINED, SUSPECT, FAILED) else m.status
                    m.incarnation = max(m.incarnation, incarnation)
                    if m.status == SUSPECT and prior != SUSPECT:
                        m.suspect_since = self._time()
                    if m.status in (ALIVE, JOINED):
                        m.suspect_since = 0.0
                    self._enqueue_piggy(MembershipUpdate(
                        agent_id=agent_id, status=m.status, incarnation=m.incarnation
                    ))
                    joined_callback = (
                        status in (ALIVE, JOINED) and prior not in (ALIVE, JOINED)
                    )
                else:
                    joined_callback = False
        try:
            if joined_callback:
                self.host.on_agent_alive(agent_id)
        except Exception as exc:
            self.host.log_warn(f"SWIM callback (alive-merge) raised: {exc}")

    # ---- Helpers --------------------------------------------------------- #

    def _pick_probe_target(self) -> Optional[int]:
        with self._lock:
            candidates = [
                m.agent_id for m in self._members.values()
                if m.agent_id != self.host.agent_id and m.status != FAILED
            ]
        # Seed from host-known peers (bootstrap before any updates arrive).
        if not candidates:
            try:
                seed = [int(p) for p in self.host.known_peers()
                        if int(p) != int(self.host.agent_id)]
            except Exception:
                seed = []
            for p in seed:
                with self._lock:
                    self._members.setdefault(
                        p, _Member(agent_id=p, status=ALIVE, incarnation=0)
                    )
            candidates = seed
        if not candidates:
            return None
        return random.choice(candidates)

    def _pick_relays(self, exclude: int) -> List[int]:
        with self._lock:
            pool = [
                m.agent_id for m in self._members.values()
                if m.agent_id not in (self.host.agent_id, exclude) and m.is_alive()
            ]
        if not pool:
            return []
        if len(pool) <= self.k_req:
            return pool
        return random.sample(pool, self.k_req)

    def _enqueue_piggy(self, update: MembershipUpdate) -> None:
        # Replace any prior entry for the same agent_id; keep at most one per agent.
        with self._lock:
            self._piggy = [
                e for e in self._piggy if e.update.agent_id != update.agent_id
            ]
            self._piggy.append(_PiggyEntry(
                update=update, remaining_broadcasts=self.piggyback_count
            ))

    def _fresh_piggy(self) -> List[MembershipUpdate]:
        with self._lock:
            picks = self._piggy[: self.piggyback_max_per_msg]
            for e in picks:
                e.remaining_broadcasts -= 1
            self._piggy = [e for e in self._piggy if e.remaining_broadcasts > 0]
            return [e.update for e in picks]

    def _safe_send(self, dest: int, payload: object) -> None:
        try:
            self.host.send(dest, payload)
        except Exception as exc:
            self.host.log_debug(f"[SWIM] send -> {dest} failed: {exc}")
