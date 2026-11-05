# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""A Snow reply that misses its round (2026-10-14).

From a host whose round trip exceeds `round_timeout_ms` every reply misses its round and was
discarded, so every round read as empty: coordinator 89 (on agent-92, ~90 ms from every other
coordinator) abandoned 80 jobs per Snow/Hybrid analytic run that its peers had long decided,
each holding an inflight slot for ~37 s; leaf 21 on the AMST host abandoned 80-431. A late
decided-hint is still true of the job and now finalizes the instance (verified against the
claim, as a timely one is); a late ordinary vote answered a question the instance has moved
past and is still discarded."""

from swarm.consensus.gossip_engine import RECENT_QUERY_IDS, SNOW_DEFAULTS
from swarm.consensus.messages.proposal_info import ProposalInfo
from swarm.consensus.messages.snow_response import SnowResponse

from tests.test_snow import _latest_query_item, _make_engine


def _proposed(job="job-l"):
    eng, host, transport, cas = _make_engine(agent_id=1, peers=(2, 3, 4), k=3, beta=2)
    eng.propose([ProposalInfo(p_id="p", object_id=job, cost=1.0, agent_id="1")])
    return eng, host, transport, cas


def _rounds(eng, transport, job, n):
    """Run n rounds that nobody answers in time; return their query ids, oldest first."""
    ids = []
    for _ in range(n):
        eng._tick(now=0.0)
        ids.append(_latest_query_item(transport, job)["query_id"])
        eng._evaluate_round(eng._states[job], 0.0)
    return ids


def _reply(eng, job, query_id, preferred, decided, source=2):
    eng.on_snow_response(SnowResponse(source=source, query_id=query_id, job_id=job,
                                      preferred_agent=preferred, cost=0.0,
                                      already_decided=decided))


class TestLateDecidedHint:
    def test_a_late_hint_finalizes_on_the_claim(self):
        eng, host, transport, cas = _proposed()
        cas.claim("job-l", 3)                         # a peer's vote decided it meanwhile
        old = _rounds(eng, transport, "job-l", 2)[0]
        _reply(eng, "job-l", old, 3, True)
        assert eng._states["job-l"].finalized
        assert host.participant_events == [("job-l", 3)]
        assert eng.late_decided_hints == 1
        assert eng.consensus_stats()["late_decided_hints"] == 1

    def test_a_late_hint_is_verified_not_believed(self):
        eng, host, transport, cas = _proposed()
        old = _rounds(eng, transport, "job-l", 2)[0]
        _reply(eng, "job-l", old, 9, True)            # no claim behind it: stale
        assert not eng._states["job-l"].finalized
        assert host.participant_events == []
        assert eng.stale_decided_hints == 1

    def test_a_late_vote_is_still_discarded(self):
        eng, host, transport, cas = _proposed()
        old = _rounds(eng, transport, "job-l", 2)[0]
        eng._tick(now=0.0)                            # a third round is now pending
        _reply(eng, "job-l", old, 1, False)
        st = eng._states["job-l"]
        assert st.pending_responses == []
        assert not st.finalized and eng.late_decided_hints == 0

    def test_a_query_id_this_instance_never_sent_is_ignored(self):
        eng, host, transport, cas = _proposed()
        cas.claim("job-l", 3)
        _rounds(eng, transport, "job-l", 1)
        _reply(eng, "job-l", "someone-elses-round", 3, True)
        assert not eng._states["job-l"].finalized
        assert eng.late_decided_hints == 0

    def test_only_recent_rounds_are_remembered(self):
        eng, host, transport, cas = _proposed()
        cas.claim("job-l", 3)
        ids = _rounds(eng, transport, "job-l", RECENT_QUERY_IDS + 1)
        _reply(eng, "job-l", ids[0], 3, True)         # fell out of the window
        assert not eng._states["job-l"].finalized
        _reply(eng, "job-l", ids[1], 3, True)
        assert eng._states["job-l"].finalized

    def test_a_late_hint_respects_a_proven_stale_hint(self):
        eng, host, transport, cas = _proposed()
        old = _rounds(eng, transport, "job-l", 2)[0]
        eng._states["job-l"].ignore_decided_hints = True
        cas.claim("job-l", 3)
        _reply(eng, "job-l", old, 3, True)
        assert not eng._states["job-l"].finalized


def test_the_send_pool_covers_a_far_host():
    # 16 workers at ~90 ms RTT drain ~180 sends/s against ~320/s of queries and answers.
    assert SNOW_DEFAULTS["send_workers"] == 64
