"""The selection startup barrier is bounded (code review 2026-09-18, §4).

`selection_main` used to spin on `live_agent_count != configured_agent_count` with no timeout.
One agent that never registered — a crash at startup, a coordinator on a host without the LLM
API key — left every other agent in its group waiting for the whole run: no selection ever
started and the run looked busy until the cap expired. It was also an equality test, so a group
that came up one agent OVER never released either.
"""
import os
import sys
import threading
import time
from unittest.mock import MagicMock

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from swarm.agents.resource_agent import ResourceAgent  # noqa: E402
from swarm.models.agent_info import AgentInfo  # noqa: E402
from swarm.topology.topology import Topology  # noqa: E402
from swarm.utils.thread_safe_dict import ThreadSafeDict  # noqa: E402


def _agent(configured=3, live=1, runtime=None):
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = MagicMock()
    a.agent_id = 1
    a.shutdown = False
    a.runtime_config = dict(runtime or {})
    a.topology = Topology({"type": "mesh", "group_size": configured})
    a.neighbor_map = ThreadSafeDict()
    for i in range(1, live + 1):
        a.neighbor_map.set(i, AgentInfo(agent_id=i))
    return a


def test_releases_at_once_when_the_group_is_complete():
    a = _agent(configured=3, live=3, runtime={"selection_barrier_s": 5})
    started = time.monotonic()
    result = a._await_peers()
    assert time.monotonic() - started < 0.4
    assert result["short"] == 0 and result["live"] == 3


def test_a_group_one_agent_over_releases_too():
    """The old test was `!=`: one dynamic agent registering early held the group forever."""
    a = _agent(configured=3, live=4, runtime={"selection_barrier_s": 5})
    started = time.monotonic()
    assert a._await_peers()["short"] == 0
    assert time.monotonic() - started < 0.4


def test_a_missing_agent_releases_after_the_barrier_with_a_warning():
    a = _agent(configured=3, live=2, runtime={"selection_barrier_s": 0.6})
    started = time.monotonic()
    result = a._await_peers()
    elapsed = time.monotonic() - started
    assert 0.5 <= elapsed < 2.0, elapsed
    assert result["short"] == 1 and result["live"] == 2 and result["configured"] == 3
    warning = " ".join(str(c.args[0]) for c in a.logger.warning.call_args_list)
    assert "[SEL_BARRIER]" in warning and "2/3" in warning


def test_a_late_arrival_releases_before_the_deadline():
    a = _agent(configured=3, live=2, runtime={"selection_barrier_s": 10})

    def arrive():
        time.sleep(0.7)
        a.neighbor_map.set(3, AgentInfo(agent_id=3))

    threading.Thread(target=arrive, daemon=True).start()
    started = time.monotonic()
    result = a._await_peers()
    assert time.monotonic() - started < 3.0
    assert result["short"] == 0


def test_shutdown_ends_the_wait():
    a = _agent(configured=3, live=1, runtime={"selection_barrier_s": 30})

    def stop():
        time.sleep(0.3)
        a.shutdown = True

    threading.Thread(target=stop, daemon=True).start()
    started = time.monotonic()
    a._await_peers()
    assert time.monotonic() - started < 2.0


def test_the_default_bound_is_the_failure_threshold():
    """One key for both judgements: a peer that has not shown up within the time this run would
    take to declare a live peer dead is treated the same way."""
    a = _agent(runtime={"failure_threshold_seconds": 45})
    assert a.selection_barrier_s == 45.0
    b = _agent(runtime={"failure_threshold_seconds": 45, "selection_barrier_s": 7})
    assert b.selection_barrier_s == 7.0
    c = _agent(runtime={})
    assert c.selection_barrier_s == float(c.failure_threshold_seconds)
