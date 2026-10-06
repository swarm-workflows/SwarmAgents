"""Code review 2026-10-05 §G: config drift and the smaller robustness items.

Drift: a key with one default in code and another in the shipped file runs a different regime
whenever a config omits it, and nothing in the run says which. Every pair below now resolves to
the shipped value from ONE place, and these tests pin the agreement against the file itself.

Robustness: a Snow finalize run under the engine lock; a consensus stash that only drained on
PENDING; gossip re-admitting an evicted entry; an O(completed) tick; a won backlog invisible to
peers; an agent at exact capacity advertising load 0; a cache signature missing a field that
feasibility reads; claim keys inherited across runs; an agent-key TTL below the detector.
"""
import os
import sys
import threading
import time
from unittest.mock import MagicMock

import pytest
import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "tests"))

from swarm.agents import resource_agent as ra  # noqa: E402
from swarm.agents.resource_agent import ResourceAgent, _HostAdapter  # noqa: E402
from swarm.consensus.gossip_engine import SNOW_DEFAULTS, GossipConsensusEngine  # noqa: E402
from swarm.membership.swim import SWIM_DEFAULTS, SwimMembership  # noqa: E402
from swarm.models.capacities import Capacities  # noqa: E402
from swarm.models.job import Job  # noqa: E402


@pytest.fixture(scope="module")
def shipped():
    with open(os.path.join(REPO, "config_swarm_multi.yml")) as f:
        return yaml.safe_load(f)


# --------------------------------------------------------------------------- drift
class TestOneDefaultPerKey:
    def test_snow_defaults_are_the_shipped_values(self, shipped):
        snow = shipped["consensus"]["snow"]
        for key, value in SNOW_DEFAULTS.items():
            assert snow[key] == value, key

    def test_the_snow_constructor_uses_the_same_table(self):
        eng = GossipConsensusEngine(agent_id=1, host=MagicMock(), transport=MagicMock(),
                                    router=MagicMock())
        assert (eng.k, eng.beta, eng.max_inflight) == (10, 6, 16)
        assert eng.round_timeout_s == pytest.approx(0.3)
        assert not hasattr(eng, "alpha_k")                      # dead field removed

    def test_the_agent_reads_snow_through_the_table(self):
        src = open(os.path.join(REPO, "swarm/agents/resource_agent.py")).read()
        assert 'snow_cfg.get("k", 20)' not in src
        assert "return snow_cfg.get(key, SNOW_DEFAULTS[key])" in src

    def test_swim_defaults_are_the_shipped_values(self, shipped):
        swim = shipped["failure_detection"]["swim"]
        for key, value in SWIM_DEFAULTS.items():
            assert swim[key] == value, key
        m = SwimMembership(host=MagicMock())
        assert m.suspect_timeout_s == 60.0 and m.probe_timeout_s == pytest.approx(1.0)

    def test_ucb1_has_one_default(self):
        from swarm.rl.bandit import UCB1Policy
        from test_mab_manager import make_manager
        m = make_manager({"algorithm": "ucb1"})
        assert m.policy.exploration_weight == UCB1Policy.DEFAULT_EXPLORATION_WEIGHT

    def test_runtime_fallbacks_are_the_shipped_values(self, shipped):
        rt = shipped["runtime"]
        assert ra.RESELECTION_TIMEOUT_DEFAULT_S == rt["reselection_timeout_s"]
        assert ra.FAILURE_THRESHOLD_DEFAULT_S == rt["failure_threshold_seconds"]
        a = ResourceAgent.__new__(ResourceAgent)
        a.runtime_config = {}
        assert a.reselection_timeout_s == 300.0
        assert a.failure_threshold_seconds == 60

    def test_dead_orphan_reset_is_gone(self):
        assert not hasattr(ResourceAgent, "_reset_orphaned_jobs")


# --------------------------------------------------------------------------- Snow lock
def test_a_peer_decided_finalize_does_not_run_under_the_engine_lock():
    """No pool (engine not started, or after stop()): the CAS and callbacks run inline, and
    they used to run inside `_absorb_response`'s `with self._lock`."""
    from swarm.consensus.messages.proposal_info import ProposalInfo
    from test_snow import _make_engine
    eng, host, _t, cas = _make_engine(agent_id=1, peers=(2, 3, 4))
    held = []

    def probe(*_a):
        got = []

        def other_thread():                     # an RLock must be released by its owner
            ok = eng._lock.acquire(timeout=0.5)
            got.append(ok)
            if ok:
                eng._lock.release()
        t = threading.Thread(target=other_thread)
        t.start()
        t.join()
        held.append(not got[0])
        return 2
    host.get_assignment = lambda oid: probe()
    host.try_claim_assignment = lambda oid, aid: probe()
    eng.propose([ProposalInfo(p_id="p1", object_id="j1", cost=1.0, agent_id="1")])
    eng._tick(now=0.0)
    qid = eng._states["j1"].pending_query_id
    assert eng._send_pool is None
    eng._absorb_response("j1", qid, 2, 1.0, True)
    assert held and not any(held)


# --------------------------------------------------------------------------- stash
class TestStashExpires:
    def _adapter(self):
        a = ResourceAgent.__new__(ResourceAgent)
        a._pending_lock = threading.Lock()
        a.pending_proposals, a.pending_prepares, a.pending_commits = {}, {}, {}
        a.pending_consensus_dropped = 0
        a.pending_consensus_expired = 0
        a._pending_stashed_at = {}
        return a, _HostAdapter.__new__(_HostAdapter)

    def test_a_full_stash_of_stale_objects_makes_room(self, monkeypatch):
        a, h = self._adapter()
        h.agent = a
        monkeypatch.setattr(_HostAdapter, "_PENDING_MAX_OBJECTS", 3)
        for i in range(3):
            h.set_pending_prepare(object(), f"old{i}")
        for oid in list(a._pending_stashed_at):
            a._pending_stashed_at[oid] -= _HostAdapter._PENDING_STASH_TTL_S + 1
        h.set_pending_prepare(object(), "new")
        assert "new" in a.pending_prepares
        assert a.pending_consensus_dropped == 0 and a.pending_consensus_expired == 3
        assert not any(k.startswith("old") for k in a.pending_prepares)

    def test_a_full_stash_of_fresh_objects_still_drops(self, monkeypatch):
        a, h = self._adapter()
        h.agent = a
        monkeypatch.setattr(_HostAdapter, "_PENDING_MAX_OBJECTS", 2)
        for i in range(3):
            h.set_pending_prepare(object(), f"o{i}")
        assert a.pending_consensus_dropped == 1 and a.pending_consensus_expired == 0


# --------------------------------------------------------------------------- gossip
def test_an_evicted_gossip_entry_is_not_resurrected_by_a_relayed_copy():
    from swarm.gossip.disseminator import GossipStateDisseminator
    from swarm.consensus.messages.agent_state_entry import AgentStateEntry
    clock = [0.0]
    host = MagicMock(agent_id=1)
    d = GossipStateDisseminator(host=host, state_ttl_s=10.0, time_fn=lambda: clock[0])
    d._merge_entry(AgentStateEntry(agent_id=7, load=50.0, version=4))
    clock[0] = 20.0
    d._evict_expired()
    assert d.get(7) is None
    d._merge_entry(AgentStateEntry(agent_id=7, load=50.0, version=4))     # a peer's old copy
    assert d.get(7) is None
    d._merge_entry(AgentStateEntry(agent_id=7, load=10.0, version=5))     # the subject, alive
    assert d.get(7).version == 5


# --------------------------------------------------------------------------- completed sweep
def test_the_tick_only_cleans_up_newly_settled_jobs():
    a = ResourceAgent.__new__(ResourceAgent)
    a.completed_lock = threading.RLock()
    a.completed_jobs_set = {"done"}
    a._decided_jobs = {}
    a.engine = MagicMock()
    a.queues = MagicMock()
    a._update_completed_jobs(["done", "new"])
    removed = [c.kwargs["object_id"] for c in a.engine.incoming.remove_object.call_args_list]
    assert removed == ["new"] and a.completed_jobs_set == {"done", "new"}
    a.engine.reset_mock()
    a._update_completed_jobs(["done", "new"])
    a.engine.incoming.remove_object.assert_not_called()


# --------------------------------------------------------------------------- load
class TestAdvertisedLoad:
    def test_full_reads_full_not_idle(self):
        total = Capacities(core=8, ram=16, disk=100)
        assert ResourceAgent.resource_usage_score(Capacities(core=8, ram=16, disk=100),
                                                  total) == 100.0
        assert ResourceAgent.resource_usage_score(Capacities(), Capacities()) == 0.0

    def test_the_won_backlog_is_proposed_load(self):
        a = ResourceAgent.__new__(ResourceAgent)
        a._capacities = Capacities(core=8, ram=16, disk=100)
        backlog = Job()
        backlog.capacities = Capacities(core=4, ram=8, disk=50)
        a.queues = MagicMock()
        a.queues.selected_queue.gets.return_value = [backlog]
        a.engine = MagicMock()
        a.engine.outgoing.objects.return_value = []
        assert a.compute_proposed_load() == 50.0


# --------------------------------------------------------------------------- cache signature
def test_the_cache_signature_carries_delegation_failures():
    a = ResourceAgent.__new__(ResourceAgent)
    j = Job()
    j.job_id = "j1"
    j.capacities = Capacities(core=1, ram=1, disk=1)
    before = a._job_sig(j)
    j.add_delegation_failed_agents(31)
    assert a._job_sig(j) != before


# --------------------------------------------------------------------------- claim keys
def test_claim_keys_are_run_scoped():
    from swarm.database.repository import Repository

    class _R:
        def __init__(self):
            self.kv = {}

        def set(self, k, v, nx=False, ex=None):
            if nx and k in self.kv:
                return False
            self.kv[k] = v
            return True

        def get(self, k):
            return self.kv.get(k)
    r = _R()
    assert Repository(r, run_id="run-1").try_claim_assignment("j1", 4) == 4
    assert Repository(r, run_id="run-2").try_claim_assignment("j1", 9) == 9
    assert all(":run-" in k for k in r.kv)
    assert Repository(r, run_id="run-2").try_claim_reassignment("j1", 4, 1)
    assert any(k.startswith("reassign:run-2:") for k in r.kv)


# --------------------------------------------------------------------------- key TTL
def test_the_agent_key_outlives_the_failure_detector():
    a = ResourceAgent.__new__(ResourceAgent)
    a.runtime_config = {"peer_expiry_seconds": 20, "failure_threshold_seconds": 120}
    assert a._agent_key_ttl_s() >= 2 * (120 * 1.1 + 1)
    a.runtime_config = {"peer_expiry_seconds": 300, "failure_threshold_seconds": 60}
    assert a._agent_key_ttl_s() == 600
