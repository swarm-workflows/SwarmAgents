"""Fixes from the 2026-09-15 review pass, each pinned by the behaviour it restores.

Every one of these biases a number the two papers report, and all but the last two were found
by reading rather than by a failing run — which is why they need tests that fail against the
old code, not assertions that the new code does what it now does.

* PBFT proposers re-broadcast COMMIT on every PREPARE past quorum, inflating messages-per-job
  *against* PBFT — the direction that flatters the papers' own argument.
* Snow's plain-int counters are incremented from the finalize pool and the driver thread, so
  they undercount in exactly the saturated runs the mechanism figure is about; and a finalize
  whose CAS throws used to be counted as neither finalized nor abandoned.
* An unknown consensus engine name silently became PBFT, so a cell labelled Snow or Hybrid in
  the results table could have been PBFT throughout.
* SWIM's acked-probe map never shrank, and every tick walked all of it.
* `batch_tests_v2.py` floored the host count where the runner ceils it.
"""
import os
import sys
import threading
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from swarm.consensus.engine import ConsensusEngine  # noqa: E402
from swarm.consensus.messages.commit import Commit  # noqa: E402
from swarm.consensus.messages.prepare import Prepare  # noqa: E402
from swarm.consensus.messages.proposal_info import ProposalInfo  # noqa: E402
from swarm.models.agent_info import AgentInfo  # noqa: E402
from swarm.models.object import ObjectState  # noqa: E402


# ---------------------------------------------------------------------------------------
# PBFT: one COMMIT per proposal, however many PREPAREs arrive after quorum.
# ---------------------------------------------------------------------------------------

class _Obj:
    def __init__(self, object_id="j1"):
        self.object_id = object_id
        self.state = ObjectState.PENDING
        self.leader_id = None

    @property
    def is_commit(self):
        return self.state is ObjectState.COMMIT


class _Host:
    def __init__(self, obj, quorum):
        self.obj = obj
        self._quorum = quorum

    def get_object(self, oid):
        return self.obj if oid == self.obj.object_id else None

    def is_agreement_achieved(self, oid): return False
    def calculate_quorum(self): return self._quorum
    def on_leader_elected(self, obj, p_id): pass
    def on_participant_commit(self, obj, leader_id, p_id): pass
    def now(self): return 0.0
    def log_debug(self, m): pass
    def log_info(self, m): pass
    def log_warn(self, m): pass


class _Transport:
    def __init__(self):
        self.broadcasts = []

    def send(self, dest, payload): pass
    def broadcast(self, payload): self.broadcasts.append(payload)


class _Router:
    def should_forward(self): return False


def _info(p_id="p1", object_id="j1", agent_id="1", cost=1.0):
    return ProposalInfo(p_id=p_id, object_id=object_id, cost=cost, agent_id=agent_id)


def _prepare_from(sender, infos):
    return Prepare(source=sender, agents=[AgentInfo(agent_id=sender)], proposals=infos)


def _engine(obj, quorum, agent_id=1):
    return ConsensusEngine(agent_id, _Host(obj, quorum), _Transport(), router=_Router())


def _commits(eng):
    return [m for m in eng.transport.broadcasts if isinstance(m, Commit)]


def test_proposer_broadcasts_one_commit_however_many_prepares_follow():
    """The regression: the old guard asked `incoming.contains`, which is false for our own
    proposal because it lives in `outgoing` — so the proposer re-committed on every extra
    PREPARE. With 6 peers and quorum 3 that is four redundant COMMIT broadcasts per job."""
    obj = _Obj()
    eng = _engine(obj, quorum=3)
    mine = _info(agent_id="1")
    eng.outgoing.add_proposal(proposal=mine)

    for sender in (2, 3, 4, 5, 6):
        eng.on_prepare(_prepare_from(sender, [mine]))

    assert len(_commits(eng)) == 1
    assert obj.is_commit


def test_participant_also_commits_once():
    """The participant path was already correct; it must stay correct."""
    obj = _Obj()
    eng = _engine(obj, quorum=3, agent_id=1)
    peer = _info(p_id="p2", agent_id="2")
    eng.incoming.add_proposal(proposal=peer)

    for sender in (2, 3, 4, 5, 6):
        eng.on_prepare(_prepare_from(sender, [peer]))

    assert len(_commits(eng)) == 1


def test_adopting_a_better_proposal_still_commits():
    """Suppression is per proposal, not per object: a better proposal must still commit, or
    the fix would trade redundant messages for lost decisions."""
    obj = _Obj()
    eng = _engine(obj, quorum=2)
    mine = _info(p_id="p1", agent_id="1", cost=9.0)
    eng.outgoing.add_proposal(proposal=mine)
    for sender in (2, 3):
        eng.on_prepare(_prepare_from(sender, [mine]))
    assert len(_commits(eng)) == 1

    better = _info(p_id="p2", agent_id="2", cost=0.1)
    for sender in (2, 3):
        eng.on_prepare(_prepare_from(sender, [better]))

    assert len(_commits(eng)) == 2


def test_commit_bookkeeping_is_released_with_the_object():
    obj = _Obj()
    eng = _engine(obj, quorum=2)
    mine = _info(agent_id="1")
    eng.outgoing.add_proposal(proposal=mine)
    for sender in (2, 3):
        eng.on_prepare(_prepare_from(sender, [mine]))
    assert eng._commits_sent

    eng._forget_object("j1")
    assert not eng._commits_sent, "bounded by objects in flight, not objects ever seen"


# ---------------------------------------------------------------------------------------
# Snow: counters survive concurrency, and a failed finalize is counted somewhere.
# ---------------------------------------------------------------------------------------

def _snow():
    from swarm.consensus.gossip_engine import GossipConsensusEngine
    host = MagicMock()
    host.log_warn = lambda m: None
    host.log_info = lambda m: None
    host.log_debug = lambda m: None
    return GossipConsensusEngine(agent_id=1, host=host, transport=MagicMock(),
                                 router=MagicMock())


class _CountingLock:
    """A real lock that records how often it was taken.

    The race itself cannot be reproduced on demand: under CPython a lost `d += 1` needs a
    thread switch between the LOAD and the STORE, which is probabilistic and rare at low
    contention — so a threads-only test passes against the unfixed code and proves nothing.
    What *is* deterministic is whether the increment takes the lock at all, so these tests
    assert that, and run the threads as well to show the total is right when it does."""

    def __init__(self):
        self.acquisitions = 0
        self._lock = threading.Lock()

    def __enter__(self):
        self._lock.acquire()
        self.acquisitions += 1
        return self

    def __exit__(self, *exc):
        self._lock.release()
        return False


def test_dropped_send_counter_is_taken_under_the_lock():
    """`sends_dropped` is bumped from the driver tick and from send workers."""
    eng = _snow()
    eng._stats_lock = _CountingLock()
    eng._send_pool = MagicMock()
    eng._send_sem = threading.Semaphore(0)
    eng._safe_send(2, object())
    assert eng._stats_lock.acquisitions == 1, "the shed-send counter must be incremented under the lock"
    assert eng.sends_dropped == 1


def test_dropped_send_counter_totals_correctly_across_threads():
    eng = _snow()
    eng._send_pool = MagicMock()
    eng._send_sem = threading.Semaphore(0)          # every send is shed

    def hammer():
        for _ in range(500):
            eng._safe_send(2, object())

    threads = [threading.Thread(target=hammer) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert eng.sends_dropped == 4000


def test_finalize_counter_is_taken_under_the_lock():
    eng = _snow()
    eng._stats_lock = _CountingLock()
    eng.host.try_claim_assignment.return_value = 1
    eng.host.get_object.return_value = None
    state = MagicMock()
    state.proposal.object_id = "j1"
    state.started_at = 0.0
    state.round_no = 1
    state.queried = 1
    eng._finalize_work_inner(state, candidate=1, reason="t")
    assert eng._stats_lock.acquisitions == 1
    # `get_object -> None` is the LOST path since 2026-09-20 (code review §11): the CAS won but
    # no assignment was produced, so it is neither `finalized` nor an error. The subject here is
    # still the lock — one acquisition, whichever counter it guards.
    assert (eng.finalize_lost, eng.finalized_count) == (1, 0)


def test_the_success_path_takes_the_lock_once_too():
    # The companion the lock tests were missing: both of them reached the counter through
    # `get_object -> None`, which is now a different outcome from a real finalize.
    eng = _snow()
    eng._stats_lock = _CountingLock()
    eng.host.try_claim_assignment.return_value = 1
    eng.host.get_object.return_value = MagicMock()
    state = MagicMock()
    state.proposal.object_id = "j1"
    state.started_at = 0.0
    state.round_no = 1
    state.queried = 1

    eng._finalize_work_inner(state, candidate=1, reason="t")

    assert eng._stats_lock.acquisitions == 1
    assert (eng.finalized_count, eng.finalize_lost, eng.finalize_errors) == (1, 0, 0)


def test_finalize_counter_totals_correctly_across_threads():
    """`_finalize_work_inner` runs on the finalize pool — several workers at once."""
    eng = _snow()
    eng.host.try_claim_assignment.return_value = 1
    eng.host.get_object.return_value = None          # stop right after the counters

    def hammer():
        for _ in range(250):
            state = MagicMock()
            state.proposal.object_id = "j1"
            state.started_at = 0.0
            state.round_no = 1
            state.queried = 1
            eng._finalize_work_inner(state, candidate=1, reason="t")

    threads = [threading.Thread(target=hammer) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert eng.finalize_lost == 2000      # the no-object path; see the test above
    assert eng.finalized_count == 0


def test_a_finalize_that_raises_is_counted_not_just_logged():
    eng = _snow()
    eng.host.try_claim_assignment.side_effect = RuntimeError("redis down")
    state = MagicMock()
    state.proposal.object_id = "j1"
    state.started_at = 0.0

    eng._stats_lock = _CountingLock()
    eng._finalize_work(state, candidate=2, reason="test")

    assert eng._stats_lock.acquisitions == 1
    assert eng.finalize_errors == 1
    assert eng.finalized_count == 0
    assert eng.consensus_stats()["finalize_errors"] == 1, (
        "finalized + abandoned + errors must be the whole population, or a lost assignment "
        "shows up only as a warning line")


# ---------------------------------------------------------------------------------------
# An unknown engine name is refused, not quietly turned into PBFT.
# ---------------------------------------------------------------------------------------

def test_a_failed_host_callback_is_an_error_not_also_a_finalize():
    """`finalized + abandoned + finalize_errors` is the whole population, or the error counter
    is worth nothing. Counting the finalize before the callbacks reported one decision as
    both."""
    eng = _snow()
    eng.host.try_claim_assignment.return_value = 1        # we won, so on_leader_elected runs
    obj = MagicMock()
    eng.host.get_object.return_value = obj
    eng.host.on_leader_elected.side_effect = RuntimeError("callback blew up")
    state = MagicMock()
    state.proposal.object_id = "j1"
    state.started_at = 0.0
    state.round_no = 1
    state.queried = 1

    eng._finalize_work(state, candidate=1, reason="test")

    stats = eng.consensus_stats()
    assert eng.finalize_errors == 1
    assert eng.finalized_count == 0, "a decision must not be both finalized and failed"
    assert stats["finalized"] + stats["abandoned"] + stats["finalize_errors"] == 1


def test_a_finalize_with_no_object_is_neither_a_success_nor_an_error():
    """The CAS happened; the object merely went away. That is not an error — which is what
    this test asserted when it was written, and still is. But it is not a *finalize* either,
    and counting it as one was a flattering overcount: no leader was elected, no participant
    committed, and the claim key now names an agent that will not act on it, so the job is
    recoverable only if the winner re-proposes. It is its own outcome (code review §11), and
    `finalized + abandoned + errors + lost` is the whole population.
    """
    eng = _snow()
    eng.host.try_claim_assignment.return_value = 1
    eng.host.get_object.return_value = None
    state = MagicMock()
    state.proposal.object_id = "j1"
    state.started_at = 0.0
    state.round_no = 1
    state.queried = 1

    eng._finalize_work(state, candidate=1, reason="test")

    assert (eng.finalized_count, eng.finalize_errors, eng.finalize_lost) == (0, 0, 1)
    stats = eng.consensus_stats()
    assert stats["finalized"] + stats["abandoned"] + stats["finalize_errors"] \
        + stats["finalize_lost"] == 1
    assert eng.host.on_leader_elected.call_count == 0
    assert eng.host.on_participant_commit.call_count == 0


def test_a_lost_finalize_stays_out_of_the_latency_distributions():
    """`rounds_*` and `finalize_s_*` describe decisions that placed a job. A decision that
    placed none must not enter them, or the summaries describe a population they are not
    counted with."""
    eng = _snow()
    eng.host.try_claim_assignment.return_value = 1
    eng.host.get_object.return_value = None
    state = MagicMock()
    state.proposal.object_id = "j1"
    state.started_at = 0.0
    state.round_no = 9
    state.queried = 9

    eng._finalize_work(state, candidate=1, reason="test")

    assert eng.rounds_to_finalize.count == 0
    assert eng.queries_to_finalize.count == 0
    assert eng.time_to_finalize.count == 0


def test_unknown_engine_name_is_refused():
    import swarm.agents.resource_agent as ra
    src = open(ra.__file__).read()
    assert "is not one of 'pbft' or 'snow'" in src
    assert "is not one of 'pbft', 'snow' or 'hybrid'" in src


# ---------------------------------------------------------------------------------------
# SWIM: the acked-probe map is released rather than accumulated.
# ---------------------------------------------------------------------------------------

def _swim():
    from swarm.membership.swim import SwimMembership
    host = MagicMock()
    host.agent_id = 1
    host.peers.return_value = [2, 3, 4]
    for name in ("log_debug", "log_info", "log_warn", "log_error"):
        setattr(host, name, lambda m: None)
    sw = SwimMembership(host=host, period_s=0.05, probe_timeout_s=0.02)
    sw._safe_send = lambda *a, **k: None
    return sw


def test_acked_indirect_probes_do_not_accumulate():
    """Every successful indirect probe used to leave one entry behind for the life of the
    agent, and the relay-forwarding loop walked all of them every tick — so the cost of
    failure detection grew with uptime on exactly the long runs a campaign is made of."""
    sw = _swim()
    for i in range(50):                     # acked, we were the initiator: nothing to forward
        sw._indirect_acked[f"p{i}"] = True

    sw._handle_expirations(now=1.0)

    assert sw._indirect_acked == {}


def test_orphaned_unacked_entries_are_released():
    sw = _swim()
    sw._indirect_acked["gone"] = False       # probe already removed by the timeout sweep
    sw._handle_expirations(now=1.0)
    assert "gone" not in sw._indirect_acked


def test_a_relay_still_forwards_its_ack_before_releasing():
    sw = _swim()
    sent = []
    sw._safe_send = lambda dest, payload: sent.append((dest, payload))
    sw._indirect_acked["r1"] = True
    sw._relay_initiators["r1"] = 9            # we are the relay; 9 is waiting on us

    sw._handle_expirations(now=1.0)

    assert [d for d, _ in sent] == [9], "the ack must reach the initiator before cleanup"
    assert "r1" not in sw._indirect_acked


def test_an_ack_landing_during_cleanup_is_still_forwarded():
    """The regression the first version of this cleanup introduced.

    `on_ack` pops the probe and sets the flag in one step. Deciding from a snapshot taken a
    moment earlier sees "not acked" *and* "probe gone" — which is exactly the orphan branch —
    so cleanup deleted a fresh ack and its relay destination, and the initiator suspected a
    live peer. Reproduced deterministically here by landing the ack from inside the send of
    an earlier entry, which is the same interleaving without the threads."""
    from swarm.membership.swim import SwimAck, _IndirectProbe

    sw = _swim()
    sent = []

    # r1 is ready to forward; r2 is a live relay probe that has not been acked yet.
    sw._indirect_acked["r1"] = True
    sw._relay_initiators["r1"] = 8
    sw._indirect_acked["r2"] = False
    sw._relay_initiators["r2"] = 9
    sw._indirect_probes["r2"] = _IndirectProbe(probe_id="r2", target=5, deadline=1e9)

    def _send(dest, payload):
        sent.append(dest)
        if dest == 8:                      # r2's ack lands while we are forwarding r1's
            sw.on_ack(SwimAck(source=5, probe_id="r2", target_agent=5, updates=[]))
    sw._safe_send = _send

    sw._handle_expirations(now=1.0)
    assert sent == [8]
    assert sw._relay_initiators.get("r2") == 9, "a fresh ack must not be deleted by cleanup"

    sw._handle_expirations(now=1.0)        # the next tick forwards it
    assert sent == [8, 9], "the relay must still forward r2's ack to its initiator"


# ---------------------------------------------------------------------------------------
# The batch driver sizes hosts the way the runner does.
# ---------------------------------------------------------------------------------------

def test_batch_host_count_ceils_like_the_runner():
    src = open(os.path.join(REPO, "batch_tests_v2.py")).read()
    assert "math.ceil(args.agents / args.agents_per_host)" in src, (
        "270 agents at 4/VM needs 68 hosts; a floor gives 67 and the run dies after the flush")
    assert "int (args.agents / args.agents_per_host)" not in src
    head = src.split("\ndef ")[0]
    assert "import math" in head or "math," in head


def test_batch_forwards_the_strict_completeness_flag():
    src = open(os.path.join(REPO, "batch_tests_v2.py")).read()
    assert '"--expect-silent-agents", args.expect_silent_agents' in src
