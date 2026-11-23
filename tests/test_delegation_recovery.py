"""A coordinator killed mid-delegation must close its own tier record after it restarts.

`delegated_jobs` is in memory and only `_monitor_delegated_jobs` marks the coordinator-tier
record COMPLETE. Peers leave a dead coordinator's delegated job with its children (code review
2026-10-05 §13), so nobody closed the record: the job ran once at the leaf and the tier record
stayed READY. Frozen E7c under Snow (2026-11-17) recorded 1-2 such records in 6 of 8 attempts,
and the runner's in-flight wait ran its whole 1800 s bound on each.
"""
import os
import sys
from unittest.mock import MagicMock

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from swarm.agents.resource_agent import ResourceAgent  # noqa: E402
from swarm.models.job import ObjectState  # noqa: E402
from swarm.utils.thread_safe_dict import ThreadSafeDict  # noqa: E402

ME, PEER, LEVEL, GROUP, CHILD = 85, 86, 1, 0, 3
CAPS = {"core": 2, "ram": 4, "disk": 10, "gpu": 0}


class _Repo:
    """Job records keyed by (level, group, job_id); the state index is derived from them."""

    def __init__(self):
        self.jobs = {}
        self.fail_reads = False

    def put(self, job_id, level, group, state, leader=None, delegated=None):
        self.jobs[(level, group, job_id)] = {
            "job_id": job_id, "state": state.value, "leader_id": leader, "exit_status": 0,
            "delegated_groups": list(delegated or []), "capacities": dict(CAPS)}

    def get_all_ids_multi(self, key_prefix=None, level=0, group=0, states=None):
        if self.fail_reads:
            raise ConnectionError("redis down")
        return {s: [j for (lv, g, j), r in self.jobs.items()
                    if lv == level and g == group and r["state"] == s] for s in states}

    def get_many(self, ids, key_prefix=None, level=0, group=0):
        return {j: self.jobs.get((level, group, j)) for j in ids}

    def get_many_grouped(self, pairs, key_prefix=None, level=0):
        return {(j, g): self.jobs[(level, g, j)] for j, g in pairs if (level, g, j) in self.jobs}

    def get(self, obj_id, key_prefix=None, level=0, group=0):
        rec = self.jobs.get((level, group, obj_id))
        return dict(rec) if rec else None

    def save(self, obj, key_prefix=None, level=0, group=0):
        self.jobs[(level, group, obj["job_id"])] = dict(obj)


def _coordinator(repo, children=(CHILD,)):
    a = ResourceAgent.__new__(ResourceAgent)
    a.agent_id = ME
    a.topology = MagicMock(level=LEVEL, group=GROUP, children=list(children))
    a.repository = repo
    a.logger = MagicMock()
    a.metrics = MagicMock()
    a.delegated_jobs = ThreadSafeDict()
    a.runtime_config = {}
    a.mab_enabled = True
    a.mab_manager = MagicMock()
    a._get_active_child_groups = lambda: list(children)
    return a


def test_a_restarted_coordinator_tracks_what_it_had_delegated():
    repo = _Repo()
    repo.put("mine", LEVEL, GROUP, ObjectState.READY, leader=ME, delegated=[CHILD])
    repo.put("peers", LEVEL, GROUP, ObjectState.READY, leader=PEER, delegated=[CHILD])
    repo.put("won-not-delegated", LEVEL, GROUP, ObjectState.READY, leader=ME)
    repo.put("done", LEVEL, GROUP, ObjectState.COMPLETE, leader=ME, delegated=[CHILD])
    a = _coordinator(repo)

    a._recover_own_delegations()

    assert [j for j, _ in a.delegated_jobs.items()] == ["mine"]
    info = a.delegated_jobs.get("mine")
    assert info["groups"] == [CHILD] and info["recovered"] and info["bandit_closed"]
    assert info["capacities"] == CAPS


def test_recovery_runs_once_and_keeps_live_tracking():
    repo = _Repo()
    repo.put("mine", LEVEL, GROUP, ObjectState.READY, leader=ME, delegated=[CHILD])
    a = _coordinator(repo)
    live = {"delegated_at": 1.0, "groups": [CHILD], "capacities": CAPS}
    a.delegated_jobs.set("mine", live)

    a._recover_own_delegations()
    assert a.delegated_jobs.get("mine") is live          # this process's own entry wins

    a.delegated_jobs.remove("mine")
    a._recover_own_delegations()                         # second call: nothing is re-read
    assert a.delegated_jobs.size() == 0


def test_an_unreadable_store_is_retried_on_the_next_tick():
    repo = _Repo()
    repo.put("mine", LEVEL, GROUP, ObjectState.READY, leader=ME, delegated=[CHILD])
    a = _coordinator(repo)

    repo.fail_reads = True
    a._recover_own_delegations()
    assert a.delegated_jobs.size() == 0

    repo.fail_reads = False
    a._recover_own_delegations()
    assert a.delegated_jobs.get("mine") is not None


def test_a_leaf_never_reads_the_index():
    repo = _Repo()
    repo.fail_reads = True                               # a read would raise and be logged
    a = _coordinator(repo, children=())
    a._recover_own_delegations()
    a.logger.warning.assert_not_called()


def test_the_monitor_then_closes_the_tier_record_without_paying_the_bandit():
    """The E7c sequence: delegated, coordinator killed, the leaf finishes, coordinator back."""
    repo = _Repo()
    repo.put("j", LEVEL, GROUP, ObjectState.READY, leader=ME, delegated=[CHILD])
    repo.put("j", LEVEL - 1, CHILD, ObjectState.COMPLETE, leader=28)
    a = _coordinator(repo)

    a._recover_own_delegations()
    a._monitor_delegated_jobs()

    assert repo.jobs[(LEVEL, GROUP, "j")]["state"] == ObjectState.COMPLETE.value
    assert a.delegated_jobs.size() == 0
    a.mab_manager.report_outcome.assert_not_called()


def test_a_job_still_running_at_the_leaf_stays_tracked():
    repo = _Repo()
    repo.put("j", LEVEL, GROUP, ObjectState.READY, leader=ME, delegated=[CHILD])
    repo.put("j", LEVEL - 1, CHILD, ObjectState.RUNNING, leader=28)
    a = _coordinator(repo)

    a._recover_own_delegations()
    a._monitor_delegated_jobs()

    assert repo.jobs[(LEVEL, GROUP, "j")]["state"] == ObjectState.READY.value
    assert a.delegated_jobs.get("j") is not None
    a.mab_manager.report_outcome.assert_not_called()
