"""P0-5: the LLM's verdict must reach consensus, on a scale peers can compare.

Two defects, one symptom (chaos finding 11):

1. `_HostAdapter.my_cost_for_job` computed the ANALYTIC cost, and `LlmAgent` did not override
   it. So an LLM agent priced a job with the model when proposing and analytically when voting:
   under `consensus.protocol: snow` — the shipped default — the LLM's opinion entered the
   protocol only through whoever initiated the round.
2. The two planes are not on the same scale (analytic ~0..1, LLM 25..75), so a peer comparing
   its 0.5 against an initiator's 45 "dominated" every time and the rule degenerated.

The same mismatch also made a *failed* LLM bid — which falls back to the analytic model — look
like the cheapest host in the fleet, a capture channel independent of the fallback's speed.
"""
import os
import sys
import threading
import time
from collections import OrderedDict

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from swarm.agents.cost_scale import (ANALYTIC_HALF, CANONICAL_MAX,  # noqa: E402
                                     LLM_HALF, CostScale, to_canonical)
from swarm.utils.tiebreak import tiebreak_rank  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestCanonicalScale:
    @pytest.mark.parametrize("scale", [CostScale.ANALYTIC, CostScale.LLM])
    def test_both_planes_are_bounded(self, scale):
        for c in (0.0, 0.01, 1.0, 10.0, 1e6):
            assert 0.0 <= to_canonical(c, scale) <= CANONICAL_MAX

    def test_each_plane_reference_cost_lands_on_the_midpoint(self):
        """The calibration: whatever each plane calls middling maps to the same canonical 50."""
        assert to_canonical(ANALYTIC_HALF, CostScale.ANALYTIC) == pytest.approx(50.0)
        assert to_canonical(LLM_HALF, CostScale.LLM) == pytest.approx(50.0)
        assert to_canonical(2.0, CostScale.ANALYTIC, analytic_half=2.0) == pytest.approx(50.0)
        assert to_canonical(30.0, CostScale.LLM, llm_half=30.0) == pytest.approx(50.0)

    def test_equivalent_costs_on_the_two_planes_agree(self):
        """Half of each plane's reference is the same canonical cost, which is the property
        that makes an analytic agent and an LLM agent comparable at all."""
        assert to_canonical(ANALYTIC_HALF / 2, CostScale.ANALYTIC) == pytest.approx(
            to_canonical(LLM_HALF / 2, CostScale.LLM))

    def test_a_penalised_llm_cost_keeps_its_ordering(self):
        """Selection multiplies by a load factor up to ~2x BEFORE canonicalising, so LLM costs
        arrive above 100. Clamping there mapped every loaded agent to the same value."""
        loaded = [to_canonical(c * 2.0, CostScale.LLM) for c in (25.0, 50.0, 75.0, 90.0)]
        assert loaded == sorted(loaded)
        assert len(set(loaded)) == 4, "loaded agents must stay distinguishable"
        assert max(loaded) < CANONICAL_MAX

    @pytest.mark.parametrize("scale", [CostScale.ANALYTIC, CostScale.LLM])
    def test_strictly_increasing_so_no_within_plane_reordering(self, scale):
        """The whole safety argument: canonicalising must not change who wins within a plane."""
        native = [0.0, 0.001, 0.01, 0.1, 0.5, 0.9, 1.0, 1.5, 3.0, 17.0, 99.0, 150.0, 1e4]
        out = [to_canonical(c, scale) for c in native]
        assert out == sorted(out)
        assert len(set(out)) == len(out), "distinct costs must stay distinct"

    def test_infinity_is_the_worst_possible_cost(self):
        """SelectionEngine uses +inf for 'not a candidate'; it must not wrap to cheap."""
        assert to_canonical(float("inf"), CostScale.ANALYTIC) == CANONICAL_MAX
        assert to_canonical(float("inf"), CostScale.LLM) == CANONICAL_MAX

    def test_negative_costs_clamp_to_zero(self):
        assert to_canonical(-5.0, CostScale.ANALYTIC) == 0.0
        assert to_canonical(-5.0, CostScale.LLM) == 0.0

    def test_nan_is_refused(self):
        with pytest.raises(ValueError):
            to_canonical(float("nan"), CostScale.ANALYTIC)

    def test_unknown_scale_is_refused(self):
        with pytest.raises(ValueError):
            to_canonical(1.0, "vibes")


class TestScaleMismatchWasTheBug:
    """Pins the degeneracy, so nobody 'simplifies' the canonical scale back out."""

    ANALYTIC_IDLE = 0.5      # a lightly loaded agent, analytic model
    LLM_GOOD_BID = 25.0      # score 75 — the modal LLM bid in the campaign
    LLM_GREAT_BID = 5.0      # score 95 — the modal gateway bid

    @staticmethod
    def _dominates(job_id, cost_a, agent_a, cost_b, agent_b):
        """The Snow dominance rule, as gossip_engine applies it."""
        if cost_a != cost_b:
            return cost_a < cost_b
        return tiebreak_rank(job_id, agent_a) < tiebreak_rank(job_id, agent_b)

    def test_raw_costs_let_any_analytic_peer_beat_every_llm_bid(self):
        """Before the fix: a peer's analytic 0.5 beat even a near-perfect LLM bid."""
        assert self._dominates("j1", self.ANALYTIC_IDLE, 2, self.LLM_GOOD_BID, 1)
        assert self._dominates("j1", self.ANALYTIC_IDLE, 2, self.LLM_GREAT_BID, 1)

    def test_canonical_costs_make_the_comparison_meaningful(self):
        """After: a strong LLM bid can beat a half-utilised analytic peer, and a nearly idle
        peer still beats a mediocre bid. Either way the comparison reflects the costs."""
        peer = to_canonical(self.ANALYTIC_IDLE, CostScale.ANALYTIC)   # 33.3
        great = to_canonical(self.LLM_GREAT_BID, CostScale.LLM)       # 9.1
        good = to_canonical(self.LLM_GOOD_BID, CostScale.LLM)         # 33.3
        assert self._dominates("j1", great, 1, peer, 2), "a great bid must be able to win"
        idle = to_canonical(0.05, CostScale.ANALYTIC)                 # 4.8 — nearly idle peer
        assert self._dominates("j1", idle, 2, good, 1), "a genuinely idle peer still wins"

    def test_a_failed_llm_bid_no_longer_looks_like_the_best_host(self):
        """The second, unmeasured capture channel: an agent whose LLM is down falls back to the
        analytic model. Advertised raw, its 0.5 beat every healthy peer's real bid."""
        broken_raw = self.ANALYTIC_IDLE
        healthy_raw = self.LLM_GOOD_BID
        assert self._dominates("j1", broken_raw, 9, healthy_raw, 1), "the old behaviour"

        broken = to_canonical(self.ANALYTIC_IDLE, CostScale.ANALYTIC)   # 33.3
        healthy = to_canonical(self.LLM_GREAT_BID, CostScale.LLM)       # 9.1
        assert self._dominates("j1", healthy, 1, broken, 9), \
            "a good bid must beat a fallback bid from an equally-loaded agent"
        # And the equally-middling case is now a genuine tie on cost rather than a walkover.
        assert to_canonical(self.ANALYTIC_IDLE, CostScale.ANALYTIC) == pytest.approx(
            to_canonical(self.LLM_GOOD_BID, CostScale.LLM))


class _FakeCache:
    """Minimal stand-in for LlmAgent's verdict cache, exercising the real methods."""

    def __init__(self, ttl=300.0, maxsize=4):
        self._cost_cache = OrderedDict()
        self._cost_cache_lock = threading.Lock()
        self._cost_cache_ttl_s = ttl
        self._cost_cache_max = maxsize
        self.llm_wire_cost_hits = 0
        self.llm_wire_cost_misses = 0
        self.llm_snow_cost_fallback = "yield"
        self.agent_id = 1

    _remember_cost = None          # bound below from the real class
    native_cost_for_job = None


@pytest.fixture
def cache():
    """Bind LlmAgent's real cache methods onto a bare object, so the test exercises the
    shipped code without constructing an agent (which needs Redis and gRPC)."""
    from swarm.agents.llm.llm_agent import LlmAgent
    obj = _FakeCache()
    obj._remember_cost = LlmAgent._remember_cost.__get__(obj)
    obj.native_cost_for_job = LlmAgent.native_cost_for_job.__get__(obj)
    return obj


class TestVerdictCache:
    def test_a_recorded_verdict_is_returned_with_its_plane(self, cache):
        cache._remember_cost("j1", 25.0, CostScale.LLM)
        assert cache.native_cost_for_job("j1") == (25.0, CostScale.LLM)
        assert cache.llm_wire_cost_hits == 1

    def test_a_fallback_verdict_keeps_the_analytic_plane(self, cache):
        """It is an analytic number; labelling it LLM is what made a broken agent cheapest."""
        cache._remember_cost("j1", 0.5, CostScale.ANALYTIC)
        assert cache.native_cost_for_job("j1") == (0.5, CostScale.ANALYTIC)

    def test_a_miss_abstains_rather_than_answering_with_another_plane(self, cache):
        assert cache.native_cost_for_job("nope") is None
        assert cache.llm_wire_cost_misses == 1

    def test_a_stale_verdict_is_dropped_not_voted_with(self, cache):
        cache._cost_cache_ttl_s = 0.05
        cache._remember_cost("j1", 25.0, CostScale.LLM)
        time.sleep(0.06)
        assert cache.native_cost_for_job("j1") is None
        assert "j1" not in cache._cost_cache, "the stale entry must be evicted, not re-read"

    def test_zero_ttl_disables_expiry(self, cache):
        cache._cost_cache_ttl_s = 0.0
        cache._remember_cost("j1", 25.0, CostScale.LLM)
        time.sleep(0.02)
        assert cache.native_cost_for_job("j1") == (25.0, CostScale.LLM)

    def test_the_cache_is_bounded_and_evicts_oldest_first(self, cache):
        for i in range(10):
            cache._remember_cost(f"j{i}", float(i), CostScale.LLM)
        assert len(cache._cost_cache) == cache._cost_cache_max
        assert cache.native_cost_for_job("j0") is None
        assert cache.native_cost_for_job("j9") == (9.0, CostScale.LLM)

    def test_a_rewritten_verdict_replaces_the_old_one(self, cache):
        cache._remember_cost("j1", 25.0, CostScale.LLM)
        cache._remember_cost("j1", 5.0, CostScale.LLM)
        assert cache.native_cost_for_job("j1") == (5.0, CostScale.LLM)

    def test_an_empty_job_id_is_ignored(self, cache):
        cache._remember_cost("", 25.0, CostScale.LLM)
        assert len(cache._cost_cache) == 0

    def test_concurrent_writes_do_not_corrupt_the_cache(self, cache):
        """The write side is the selection thread; the read side is the inbound consumer."""
        cache._cost_cache_max = 512

        def writer(base):
            for i in range(200):
                cache._remember_cost(f"j{base}-{i}", float(i), CostScale.LLM)

        threads = [threading.Thread(target=writer, args=(b,)) for b in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(cache._cost_cache) <= cache._cost_cache_max


class TestWiring:
    """The paths that carry the cost, asserted against the source so they cannot regress."""

    def test_the_adapter_delegates_instead_of_inlining_the_analytic_cost(self):
        src = open(os.path.join(REPO, "swarm/agents/resource_agent.py")).read()
        body = src[src.index("def my_cost_for_job"):src.index("def try_claim_assignment")]
        assert "wire_cost_for_job" in body
        assert "_cost_job_on_agent" not in body, \
            "inlining the analytic cost here is exactly finding 11"

    def test_the_base_reports_the_analytic_plane_explicitly(self):
        """Returning self.COST_SCALE would let a subclass label an analytic number as its own."""
        src = open(os.path.join(REPO, "swarm/agents/resource_agent.py")).read()
        body = src[src.index("def native_cost_for_job"):src.index("def wire_cost_for_job")]
        code = "\n".join(ln.split("#")[0] for ln in body.splitlines())
        assert "CostScale.ANALYTIC" in code
        assert "self.COST_SCALE" not in code

    def test_both_agents_advertise_a_canonical_proposal_cost(self):
        for path in ("swarm/agents/resource_agent.py", "swarm/agents/llm/llm_agent.py"):
            src = open(os.path.join(REPO, path)).read()
            assert "cost=self.proposal_cost(job, cost)" in src, path
            assert "cost=round(cost, 2)" not in src, f"{path} still advertises a native cost"

    def test_the_llm_agent_declares_its_own_plane(self):
        from swarm.agents.llm.llm_agent import LlmAgent
        from swarm.agents.resource_agent import ResourceAgent
        assert ResourceAgent.COST_SCALE == CostScale.ANALYTIC
        assert LlmAgent.COST_SCALE == CostScale.LLM

    def test_the_query_path_never_calls_the_model(self):
        """An LLM call on the inbound consumer thread stalls every other peer's queries."""
        src = open(os.path.join(REPO, "swarm/agents/llm/llm_agent.py")).read()
        body = src[src.index("def native_cost_for_job"):src.index("def _designate_bidders")]
        for forbidden in ("self.bidder", "_llm_or_analytic_cost", "score("):
            assert forbidden not in body, f"{forbidden} would block the inbound thread"


class TestThroughTheRealSnowEngine:
    """End to end through `GossipConsensusEngine._answer_query`, which is where it bit."""

    @staticmethod
    def _engine(my_cost):
        """A Snow engine whose host answers queries with `my_cost` (None = abstain)."""
        sys.path.insert(0, os.path.join(REPO, "tests"))
        from test_snow import _make_engine
        return _make_engine(agent_id=2, peers=(1, 3, 4), my_cost=my_cost)[0]

    def test_a_peer_with_no_verdict_yields_to_the_initiator(self):
        """The LLM plane's miss path: abstaining must hand the round to the initiator."""
        eng = self._engine(my_cost=None)
        ans = eng._answer_query("q1", "j1", q_preferred=1, q_cost=25.0)
        assert ans["preferred_agent"] == 1
        assert ans["cost"] == pytest.approx(25.0)

    def test_raw_analytic_cost_beat_a_good_llm_bid(self):
        """The bug, driven through the real engine: peer 2's analytic 0.5 takes the job from
        agent 1's LLM bid of 25 purely because the two are on different scales."""
        eng = self._engine(my_cost=0.5)
        ans = eng._answer_query("q1", "j1", q_preferred=1, q_cost=25.0)
        assert ans["preferred_agent"] == 2, "pins the pre-fix behaviour"

    def test_canonical_costs_let_a_good_llm_bid_stand(self):
        """After canonicalisation the peer answers 33.3 and a score-95 bid (9.1) wins."""
        peer = to_canonical(0.5, CostScale.ANALYTIC)                         # 33.3
        bid = to_canonical(5.0, CostScale.LLM)                               # 9.1
        eng = self._engine(my_cost=peer)
        ans = eng._answer_query("q1", "j1", q_preferred=1, q_cost=bid)
        assert ans["preferred_agent"] == 1
        assert ans["cost"] == pytest.approx(bid)

    def test_a_genuinely_cheaper_peer_still_wins(self):
        """The fix must not simply hand every round to the initiator."""
        eng = self._engine(my_cost=to_canonical(0.05, CostScale.ANALYTIC))   # 4.8
        ans = eng._answer_query("q1", "j1", q_preferred=1,
                                q_cost=to_canonical(25.0, CostScale.LLM))    # 33.3
        assert ans["preferred_agent"] == 2

    def test_two_llm_agents_compare_directly(self):
        """The comparison the campaign has never actually made: bid against bid."""
        eng = self._engine(my_cost=to_canonical(5.0, CostScale.LLM))         # score 95 -> 9.1
        ans = eng._answer_query("q1", "j1", q_preferred=1,
                                q_cost=to_canonical(25.0, CostScale.LLM))    # score 75 -> 33.3
        assert ans["preferred_agent"] == 2
        assert ans["cost"] == pytest.approx(to_canonical(5.0, CostScale.LLM))


class TestProposalCostUsesTheProducingPlane:
    """The advertised cost must be canonicalised on the plane that produced the bid.

    Selection multiplies the native cost by a live load factor (up to ~2x) between the bid and
    the proposal, so the value reaching `proposal_cost` never equals the cached one. An earlier
    version required them to be equal before trusting the cached scale; every fallback therefore
    fell through to the LLM plane and a 0.5 analytic cost was advertised as 0.5 — the exact
    "broken agent looks cheapest" bug the canonical scale exists to remove.
    """

    @pytest.fixture
    def agent(self):
        from swarm.agents.llm.llm_agent import LlmAgent
        obj = _FakeCache(maxsize=64)
        for name in ("_remember_cost", "proposal_cost", "to_wire"):
            setattr(obj, name, getattr(LlmAgent, name).__get__(obj))
        obj.COST_SCALE = LlmAgent.COST_SCALE
        obj.analytic_cost_half = ANALYTIC_HALF
        obj.llm_cost_half = LLM_HALF
        return obj

    class _Job:
        def __init__(self, job_id):
            self.job_id = job_id

    LOAD_FACTOR = 1.35        # 1 + (50/100)**1.5, an agent at 50% projected load

    def test_a_penalised_fallback_is_canonicalised_as_analytic(self, agent):
        agent._remember_cost("j1", 0.5, CostScale.ANALYTIC)
        advertised = agent.proposal_cost(self._Job("j1"), 0.5 * self.LOAD_FACTOR)
        assert advertised == pytest.approx(
            to_canonical(0.5 * self.LOAD_FACTOR, CostScale.ANALYTIC), abs=0.01)
        assert advertised > 30.0, "a raw ~0.68 here is the bug: it beats every real bid"

    def test_a_penalised_llm_bid_is_canonicalised_as_llm(self, agent):
        agent._remember_cost("j1", 25.0, CostScale.LLM)
        advertised = agent.proposal_cost(self._Job("j1"), 25.0 * self.LOAD_FACTOR)
        assert advertised == pytest.approx(
            to_canonical(25.0 * self.LOAD_FACTOR, CostScale.LLM), abs=0.01)

    def test_a_broken_agent_no_longer_undercuts_a_healthy_one(self, agent):
        """Both agents equally loaded; one's LLM is down. The healthy bid must win."""
        agent._remember_cost("broken", 0.5, CostScale.ANALYTIC)     # fallback
        agent._remember_cost("healthy", 25.0, CostScale.LLM)        # real bid
        broken = agent.proposal_cost(self._Job("broken"), 0.5 * self.LOAD_FACTOR)
        healthy = agent.proposal_cost(self._Job("healthy"), 25.0 * self.LOAD_FACTOR)
        assert healthy == pytest.approx(broken, abs=0.01), \
            "an idle fallback and a middling bid are genuinely comparable now"
        agent._remember_cost("healthy", 5.0, CostScale.LLM)
        better = agent.proposal_cost(self._Job("healthy"), 5.0 * self.LOAD_FACTOR)
        assert better < broken, "a strong bid must beat a fallback from an equal agent"

    def test_with_no_cached_verdict_it_uses_the_agents_own_plane(self, agent):
        advertised = agent.proposal_cost(self._Job("unknown"), 25.0)
        assert advertised == pytest.approx(to_canonical(25.0, CostScale.LLM), abs=0.01)

    def test_the_scale_survives_any_penalty_factor(self, agent):
        """Behavioural guard on the regression: whatever selection multiplied by, an analytic
        fallback stays on the analytic plane. A value-equality gate fails every one of these."""
        agent._remember_cost("j1", 0.5, CostScale.ANALYTIC)
        for factor in (1.0, 1.01, 1.35, 1.9, 2.0):
            advertised = agent.proposal_cost(self._Job("j1"), 0.5 * factor)
            assert advertised == pytest.approx(
                to_canonical(0.5 * factor, CostScale.ANALYTIC), abs=0.01), factor
            assert advertised > 30.0, f"raw analytic value leaked at factor {factor}"

