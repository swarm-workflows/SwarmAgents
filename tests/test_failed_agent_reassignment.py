"""An agent dies holding jobs: the work must go back to the pool, not disappear.

Three things had to be true together and none of them was:

* **Find the jobs.** A job that won consensus is no longer in any peer's pending queue, so
  looking there found nothing and the code logged "likely completed" and dropped it. Its
  persisted state is READY or RUNNING, not PENDING, so no agent re-added it either, and the
  reselection net only covers the three consensus states. The work was stranded for the run.
* **Release the claim.** Exactly-once rests on a Redis `SET NX` that nothing ever deleted, so
  a re-finalization was handed the dead agent forever. Reassignment could not work at all
  under the shipped protocol.
* **Let the peers vote again.** A job returning to PENDING stayed in every peer's consensus
  dedupe set, so `is_agreement_achieved` remained true and every commit for it was skipped.

P0-9 made this urgent rather than theoretical: a scheduled job used to flip to COMPLETE
immediately, so a dead agent's job merely looked finished; it is now honestly RUNNING for its
whole duration, which is the window this covers.
"""
import os
import sys
import threading
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from swarm.agents.resource_agent import ResourceAgent  # noqa: E402
from swarm.database.repository import Repository  # noqa: E402
from swarm.models.job import Job, ObjectState  # noqa: E402
from swarm.utils.metrics import Metrics  # noqa: E402
from swarm.utils.thread_safe_dict import ThreadSafeDict  # noqa: E402


class _FakeRedis:
    """Sets, strings and SET NX/EX with real semantics — enough for claims and the index."""

    def __init__(self):
        self.kv = {}
        self.sets = {}

    def set(self, key, val, nx=False, ex=None):
        if nx and key in self.kv:
            return None
        self.kv[key] = str(val)
        return True

    def get(self, key):
        return self.kv.get(key)

    def delete(self, *keys):
        n = 0
        for k in keys:
            n += 1 if self.kv.pop(k, None) is not None else 0
            n += 1 if self.sets.pop(k, None) is not None else 0
        return n

    def sadd(self, key, *vals):
        self.sets.setdefault(key, set()).update(str(v) for v in vals)

    def srem(self, key, *vals):
        self.sets.get(key, set()).difference_update(str(v) for v in vals)

    def smembers(self, key):
        return set(self.sets.get(key, set()))

    def mget(self, keys):
        return [self.kv.get(k) for k in keys]

    def pipeline(self):
        outer = self

        class _P:
            def __init__(self):
                self.ops = []

            def watch(self, *a):
                pass

            def get(self, key):
                return outer.kv.get(key)

            def multi(self):
                pass

            def set(self, k, v):
                self.ops.append(("set", k, v))

            def sadd(self, k, *v):
                self.ops.append(("sadd", k, v))

            def srem(self, k, *v):
                self.ops.append(("srem", k, v))

            def smembers(self, k):
                self.ops.append(("smembers", k, None))

            def execute(self):
                out = []
                for op in self.ops:
                    if op[0] == "set":
                        outer.kv[op[1]] = op[2]
                        out.append(True)
                    elif op[0] == "sadd":
                        outer.sadd(op[1], *op[2])
                        out.append(True)
                    elif op[0] == "srem":
                        outer.srem(op[1], *op[2])
                        out.append(True)
                    else:
                        out.append(outer.smembers(op[1]))
                self.ops = []
                return out

            def reset(self):
                self.ops = []
        return _P()


def _agent(repo, agent_id=1):
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = MagicMock()
    a.agent_id = agent_id
    a.metrics = Metrics()
    a.repository = repo
    a.topology = MagicMock(level=0, group=0)
    a.queues = MagicMock()
    a.engine = MagicMock()
    a.job_assignments = ThreadSafeDict()
    a._init_decision_state()   # the shipped dedupe state, not a hand-built copy
    a.pending_proposals = {}
    a.pending_prepares = {}
    a.pending_commits = {}
    return a


def _place(repo, job_id, leader, state):
    """Put a job in Redis as if `leader` had won it, with the exactly-once claim set."""
    j = Job()
    j.job_id = job_id
    j.wall_time = 1.0
    j.leader_id = leader
    j.state = state
    repo.save(obj=j.to_dict(), key_prefix=Repository.KEY_JOB, level=0, group=0)
    repo.try_claim_assignment(job_id, leader, level=0, group=0)
    return j


def _state_of(repo, job_id):
    rec = repo.get(job_id, key_prefix=Repository.KEY_JOB, level=0, group=0)
    return rec.get("state"), rec.get("leader_id")


# --------------------------------------------------------------------------------------
# Finding the jobs at all.
# --------------------------------------------------------------------------------------

@pytest.mark.parametrize("state", [ObjectState.READY, ObjectState.RUNNING])
def test_an_in_flight_job_on_a_dead_agent_goes_back_to_the_pool(state):
    """The regression: neither state is in any peer's pending queue, so the old lookup found
    nothing and called it 'likely completed'."""
    repo = Repository(_FakeRedis(), run_id="t")
    _place(repo, "j1", leader=7, state=state)
    a = _agent(repo)

    a._reassign_jobs_from_failed_agent(7)

    st, leader = _state_of(repo, "j1")
    assert st == ObjectState.PENDING.value
    assert leader is None, "a reassigned job must not still name the dead agent"
    assert "j1" in a.metrics.reassignments


def test_a_completed_job_is_left_alone():
    """Its work is done; resetting it would run it twice."""
    repo = Repository(_FakeRedis(), run_id="t")
    _place(repo, "done", leader=7, state=ObjectState.COMPLETE)
    a = _agent(repo)

    a._reassign_jobs_from_failed_agent(7)

    assert _state_of(repo, "done")[0] == ObjectState.COMPLETE.value
    assert a.metrics.reassignments == {}


def test_jobs_held_by_healthy_agents_are_left_alone():
    repo = Repository(_FakeRedis(), run_id="t")
    _place(repo, "mine", leader=2, state=ObjectState.RUNNING)
    _place(repo, "theirs", leader=7, state=ObjectState.RUNNING)
    a = _agent(repo)

    a._reassign_jobs_from_failed_agent(7)

    assert _state_of(repo, "mine")[0] == ObjectState.RUNNING.value
    assert _state_of(repo, "theirs")[0] == ObjectState.PENDING.value


def test_the_local_assignment_map_is_not_the_source_of_truth():
    """An agent that never witnessed the election has an empty map and must still recover."""
    repo = Repository(_FakeRedis(), run_id="t")
    _place(repo, "j1", leader=7, state=ObjectState.RUNNING)
    a = _agent(repo)
    assert len(a.job_assignments) == 0

    a._reassign_jobs_from_failed_agent(7)

    assert _state_of(repo, "j1")[0] == ObjectState.PENDING.value


# --------------------------------------------------------------------------------------
# The exactly-once claim.
# --------------------------------------------------------------------------------------

def test_the_claim_is_released_so_the_job_can_be_won_again():
    """Until this, `try_claim_assignment` kept returning the dead agent for the rest of the
    run, so a reassigned job was re-finalized straight back to the corpse."""
    repo = Repository(_FakeRedis(), run_id="t")
    _place(repo, "j1", leader=7, state=ObjectState.RUNNING)
    assert repo.get_assignment("j1", level=0, group=0) == 7

    _agent(repo)._reassign_jobs_from_failed_agent(7)

    assert repo.get_assignment("j1", level=0, group=0) is None
    assert repo.try_claim_assignment("j1", 3, level=0, group=0) == 3, \
        "a live agent must be able to win the job"


def test_release_is_idempotent_and_silent_on_an_unclaimed_job():
    repo = Repository(_FakeRedis(), run_id="t")
    assert repo.release_assignment("never-claimed", level=0, group=0) is False


# --------------------------------------------------------------------------------------
# Exactly one agent reassigns each (job, failure).
# --------------------------------------------------------------------------------------

def test_only_one_of_several_live_agents_reassigns_a_job():
    """All of them detect the same failure at the same moment. A second reset can land after
    the first reassignment has already won a fresh election, which would clobber a live
    assignment and run the job twice."""
    redis = _FakeRedis()
    repo = Repository(redis, run_id="t")
    _place(repo, "j1", leader=7, state=ObjectState.RUNNING)
    agents = [_agent(Repository(redis, run_id="t"), agent_id=i) for i in (1, 2, 3)]

    for a in agents:
        a._reassign_jobs_from_failed_agent(7)

    winners = [a.agent_id for a in agents if a.metrics.reassignments]
    assert len(winners) == 1, f"expected one reassigner, got {winners}"


def test_a_different_failure_of_the_same_job_can_still_be_reassigned():
    """The claim is per (job, failed agent): if the job's next holder also dies, it must be
    recoverable again rather than pinned by the first claim."""
    redis = _FakeRedis()
    repo = Repository(redis, run_id="t")
    _place(repo, "j1", leader=7, state=ObjectState.RUNNING)
    _agent(repo)._reassign_jobs_from_failed_agent(7)

    _place(repo, "j1", leader=8, state=ObjectState.RUNNING)     # won, then agent 8 dies
    a2 = _agent(repo)
    a2._reassign_jobs_from_failed_agent(8)

    assert _state_of(repo, "j1")[0] == ObjectState.PENDING.value
    assert "j1" in a2.metrics.reassignments


# --------------------------------------------------------------------------------------
# Peers have to be able to vote on it again.
# --------------------------------------------------------------------------------------

def test_a_job_back_in_the_pool_leaves_every_peers_dedupe_set():
    """It stayed in the consensus dedupe set, so `is_agreement_achieved` remained true and
    every commit for the reassigned job was skipped — one agent reset it and all of them
    ignored it."""
    repo = Repository(_FakeRedis(), run_id="t")
    _place(repo, "j1", leader=7, state=ObjectState.PENDING)
    peer = _agent(repo, agent_id=2)
    peer.completed_jobs_set.add("j1")

    peer._update_pending_jobs(["j1"])

    assert "j1" not in peer.completed_jobs_set
    assert peer.is_job_completed("j1") is False


def test_a_read_failure_reassigns_nothing_rather_than_guessing():
    repo = Repository(_FakeRedis(), run_id="t")
    repo.get_all_ids_multi = MagicMock(side_effect=RuntimeError("redis down"))
    a = _agent(repo)

    a._reassign_jobs_from_failed_agent(7)

    assert a.metrics.reassignments == {}
