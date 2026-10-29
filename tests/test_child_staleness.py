"""A coordinator must drop a dead child group when its heartbeats stop (code review §5).

`_refresh_agent_map` applied its staleness test only when an entry was first INSERTED. An
entry already in the map was *updated* when Redis had a fresher record and *removed* only
when the key was gone — and the agent key's TTL is `2 x peer_expiry_seconds`, 600 s shipped.
So a child group that died stayed in `children` for ten minutes: its frozen record's headroom
read idle, `_get_live_child_groups` did not gate it (its docstring claimed peer expiry pruned
it), and the only thing steering the bandit away was its own per-job `delegation_timeout_s`.
F5 "time-to-re-adoption" therefore measured delegation-timeout learning plus a Redis TTL
rather than liveness detection.

The other half of the fix is that eviction must not outrun `_detect_failed_agents`, which
scans `neighbor_map` at `failure_threshold_seconds` and is the only thing that reassigns a
dead peer's jobs.
"""
import os
import sys
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from swarm.agents.resource_agent import ResourceAgent  # noqa: E402
from swarm.models.agent_info import AgentInfo  # noqa: E402
from swarm.topology.topology import Topology  # noqa: E402
from swarm.utils.thread_safe_dict import ThreadSafeDict  # noqa: E402

NOW = 10_000.0


def _info(agent_id, group, last_updated, **kw):
    a = AgentInfo(agent_id=agent_id, **kw)
    a.group = group
    a.last_updated = last_updated
    return a


def _coordinator(children_groups=(1, 2), runtime=None):
    """A level-1 coordinator with `children_groups` below it."""
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = MagicMock()
    a.agent_id = 100
    a.runtime_config = dict(runtime or {"peer_expiry_seconds": 300,
                                        "failure_threshold_seconds": 60})
    a.topology = Topology({"type": "hierarchical", "group_size": 9})
    a.topology.level = 1
    a.topology.group = 0
    a.topology.children = list(children_groups)
    a.children = ThreadSafeDict()
    a.neighbor_map = ThreadSafeDict()
    a.failed_agents = ThreadSafeDict()
    a._agent_seen_at = {}
    a.repository = MagicMock()
    return a


def _redis_returns(agent, records):
    """`get_all_objects` answers per (level, group) from `records`: {group: [AgentInfo]}."""
    def _get(key_prefix=None, level=0, group=None, state=None):
        return [i.to_dict() for i in records.get(group, [])]
    agent.repository.get_all_objects.side_effect = _get


def test_a_child_whose_heartbeats_stopped_is_evicted_while_its_key_still_exists():
    """The regression. Redis still answers with the record; it just never changes."""
    a = _coordinator()
    frozen = _info(1, group=1, last_updated=NOW - 400)   # 400s > 300s expiry
    a.children.set(1, frozen)
    a._agent_seen_at[1] = 1.0
    _redis_returns(a, {1: [frozen], 2: []})

    a._refresh_children(current_time=NOW)

    assert 1 not in a.children
    assert 1 not in a._agent_seen_at
    warning = " ".join(str(c.args[0]) for c in a.logger.warning.call_args_list)
    assert "stale agent 1" in warning and "key still in Redis" in warning


def test_the_dead_group_stops_being_offered_to_the_bandit():
    """`_get_live_child_groups` gates delegation; before the fix the dead group stayed in it."""
    a = _coordinator()
    dead = _info(1, group=1, last_updated=NOW - 400)
    alive = _info(2, group=2, last_updated=NOW - 1)
    a.children.set(1, dead)
    a.children.set(2, alive)
    _redis_returns(a, {1: [dead], 2: [alive]})

    assert a._get_live_child_groups() == {1, 2}
    a._refresh_children(current_time=NOW)
    assert a._get_live_child_groups() == {2}


def test_a_child_still_heartbeating_is_kept_and_updated():
    a = _coordinator()
    old = _info(1, group=1, last_updated=NOW - 10)
    a.children.set(1, old)
    fresh = _info(1, group=1, last_updated=NOW - 1)
    _redis_returns(a, {1: [fresh], 2: []})

    a._refresh_children(current_time=NOW)

    assert a.children.get(1).last_updated == NOW - 1
    assert 1 in a._agent_seen_at


def test_a_record_re_read_unchanged_but_fresh_is_neither_evicted_nor_re_stamped():
    """The `_note_agent_seen` invariant the context-age metric rests on must survive this."""
    a = _coordinator()
    same = _info(1, group=1, last_updated=NOW - 30)
    a.children.set(1, same)
    a._agent_seen_at[1] = 42.0
    _redis_returns(a, {1: [same], 2: []})

    a._refresh_children(current_time=NOW)

    assert 1 in a.children
    assert a._agent_seen_at[1] == 42.0


def test_an_evicted_child_rejoins_once_it_writes_a_fresh_record():
    a = _coordinator()
    dead = _info(1, group=1, last_updated=NOW - 400)
    a.children.set(1, dead)
    _redis_returns(a, {1: [dead], 2: []})
    a._refresh_children(current_time=NOW)
    assert 1 not in a.children

    back = _info(1, group=1, last_updated=NOW + 5)
    _redis_returns(a, {1: [back], 2: []})
    a._refresh_children(current_time=NOW + 6)

    assert 1 in a.children and a._agent_seen_at.get(1) is not None


def test_an_agent_never_evicts_itself():
    a = _coordinator()
    mine = _info(a.agent_id, group=0, last_updated=NOW - 5000)
    a.neighbor_map.set(a.agent_id, mine)
    _redis_returns(a, {0: [mine]})

    a._refresh_neighbors(current_time=NOW)

    assert a.agent_id in a.neighbor_map


class TestSameTierFloor:
    """Eviction must never remove a peer before `_detect_failed_agents` has judged it, or the
    peer's death is never recorded and the jobs it held are stranded for the run."""

    def test_the_child_tier_uses_peer_expiry_as_written(self):
        a = _coordinator(runtime={"peer_expiry_seconds": 300, "failure_threshold_seconds": 60})
        assert a._staleness_eviction_threshold(level=0) == 300.0

    def test_the_own_tier_is_floored_above_the_detector(self):
        a = _coordinator(runtime={"peer_expiry_seconds": 45, "failure_threshold_seconds": 60})
        assert a._staleness_eviction_threshold(level=1) > 60 * 1.1

    def test_the_floor_does_not_lower_a_larger_expiry(self):
        a = _coordinator(runtime={"peer_expiry_seconds": 300, "failure_threshold_seconds": 60})
        assert a._staleness_eviction_threshold(level=1) == 300.0

    def test_a_peer_stale_past_expiry_but_inside_the_detector_window_stays(self):
        """peer_expiry 45s < failure threshold 60s: the inverted order the duplicate-key bug
        produced. At 50s the detector has not fired yet, so the entry must still be there."""
        a = _coordinator(runtime={"peer_expiry_seconds": 45, "failure_threshold_seconds": 60})
        peer = _info(7, group=0, last_updated=NOW - 50)
        a.neighbor_map.set(7, peer)
        _redis_returns(a, {0: [peer]})

        a._refresh_neighbors(current_time=NOW)

        assert 7 in a.neighbor_map

    def test_a_peer_past_the_detector_window_is_evicted(self):
        a = _coordinator(runtime={"peer_expiry_seconds": 45, "failure_threshold_seconds": 60})
        peer = _info(7, group=0, last_updated=NOW - 400)
        a.neighbor_map.set(7, peer)
        _redis_returns(a, {0: [peer]})

        a._refresh_neighbors(current_time=NOW)

        assert 7 not in a.neighbor_map


# --------------------------------------------------------------------------- detector-window gate
# E2 at c9c980b8 (2026-10-28): a whole group was SIGKILLed for 180 s. Eviction waits for
# peer_expiry_seconds (300 s), so the dead group stayed in `children` and its coordinators
# delegated to it until ~170 s into the outage in every Snow run. The gate now reads each
# child's last-fresher-record stamp against the failure detector's window instead.

def _clock(monkeypatch, t):
    import swarm.agents.resource_agent as ra
    monkeypatch.setattr(ra.time, "monotonic", lambda: t)


def test_a_silent_group_is_gated_at_the_detector_window_not_at_peer_expiry(monkeypatch):
    a = _coordinator()
    for aid, g in ((1, 1), (2, 1), (3, 2)):
        a.children.set(aid, _info(aid, group=g, last_updated=NOW - 1))
    a._agent_seen_at.update({1: 1000.0, 2: 1000.0, 3: 1000.0})

    _clock(monkeypatch, 1000.0 + 60.0)          # inside 60 x 1.1 + 1 = 67 s
    assert a._get_live_child_groups() == {1, 2}

    a._agent_seen_at[3] = 1100.0                 # group 2 keeps writing, group 1 went silent
    _clock(monkeypatch, 1000.0 + 68.0)
    assert a._get_live_child_groups() == {2}
    assert set(a.children.keys()) == {1, 2, 3}, "the gate must not evict: nothing else changes"


def test_one_fresh_member_keeps_its_group_live(monkeypatch):
    a = _coordinator()
    a.children.set(1, _info(1, group=1, last_updated=NOW - 1))
    a.children.set(2, _info(2, group=1, last_updated=NOW - 1))
    a._agent_seen_at.update({1: 0.0, 2: 950.0})
    _clock(monkeypatch, 1000.0)
    assert a._get_live_child_groups() == {1}


def test_a_rejoined_member_reopens_the_gate_on_its_first_fresh_record(monkeypatch):
    a = _coordinator()
    old = _info(1, group=1, last_updated=NOW - 200)
    a.children.set(1, old)
    a._agent_seen_at[1] = 0.0
    _clock(monkeypatch, 200.0)
    assert a._get_live_child_groups() == set()

    fresh = _info(1, group=1, last_updated=NOW - 1)   # restarted agent writes again
    _redis_returns(a, {1: [fresh], 2: []})
    a._refresh_children(current_time=NOW)
    assert a._get_live_child_groups() == {1}


def test_a_child_with_no_stamp_counts_as_live(monkeypatch):
    a = _coordinator()
    a.children.set(1, _info(1, group=1, last_updated=NOW - 1))
    _clock(monkeypatch, 1e6)
    assert a._get_live_child_groups() == {1}


def test_the_window_follows_the_failure_threshold(monkeypatch):
    a = _coordinator(runtime={"peer_expiry_seconds": 300, "failure_threshold_seconds": 20})
    a.children.set(1, _info(1, group=1, last_updated=NOW - 1))
    a._agent_seen_at[1] = 0.0
    _clock(monkeypatch, 23.0)                    # 20 x 1.1 + 1 = 23
    assert a._get_live_child_groups() == {1}
    _clock(monkeypatch, 23.5)
    assert a._get_live_child_groups() == set()
    assert a._detector_window_s() == pytest.approx(23.0)
