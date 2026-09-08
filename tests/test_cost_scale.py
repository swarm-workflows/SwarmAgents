"""P0-5: the LLM's verdict must reach consensus.

`_HostAdapter.my_cost_for_job` computed the ANALYTIC cost and `LlmAgent` did not override it, so
an LLM agent priced a job with the model when proposing and analytically when voting. Under
`consensus.protocol: snow` — the shipped default — the LLM's opinion entered the protocol only
through whoever initiated the round (chaos finding 11).

Finding 11 also claims the two costs are "not on the same scale ... analytic roughly 0-1 ...
LLM 25-75". `TestBothPlanesShareOneRange` refutes that and pins the refutation: the analytic
model ends in `* 100`, so both planes emit 0..100 and are compared raw. A rescaling built on
the 0-1 claim shipped briefly and was far worse than the mismatch it addressed — it sent a
median analytic cost of 11.85 to 92.2 — so the assumption is now a test rather than a comment.
"""
import os
import sys
import threading
import time
from collections import OrderedDict

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from swarm.agents.cost_scale import CostScale, NOMINAL_MAX  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


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

    def test_a_dearer_peer_lets_the_initiator_keep_the_job(self):
        """Both planes emit 0..100, so this is a like-for-like comparison: the peer's analytic
        cost sits at the 75th percentile of what that model produces, above a good LLM bid."""
        eng = self._engine(my_cost=28.40)          # analytic p75
        ans = eng._answer_query("q1", "j1", q_preferred=1, q_cost=5.0)   # LLM score 95
        assert ans["preferred_agent"] == 1
        assert ans["cost"] == pytest.approx(5.0)

    def test_a_genuinely_cheaper_peer_still_wins(self):
        """The fix must not simply hand every round to the initiator."""
        eng = self._engine(my_cost=5.41)           # analytic p25
        ans = eng._answer_query("q1", "j1", q_preferred=1, q_cost=25.0)  # LLM score 75
        assert ans["preferred_agent"] == 2

    def test_two_llm_agents_compare_directly(self):
        """The comparison the campaign has never actually made: bid against bid."""
        eng = self._engine(my_cost=5.0)                                  # score 95
        ans = eng._answer_query("q1", "j1", q_preferred=1, q_cost=25.0)  # score 75
        assert ans["preferred_agent"] == 2
        assert ans["cost"] == pytest.approx(5.0)

    def test_a_loaded_agent_above_the_nominal_max_still_loses_properly(self):
        """Post-penalty costs exceed 100; without clamping they still order correctly."""
        eng = self._engine(my_cost=150.0)
        ans = eng._answer_query("q1", "j1", q_preferred=1, q_cost=120.0)
        assert ans["preferred_agent"] == 1


class TestBothPlanesShareOneRange:
    """The assumption that lets costs be compared raw. If either model's range moves, this
    fails — which is the point: finding 11's "analytic is 0-1" was never checked."""

    def test_the_analytic_model_scales_to_0_100(self):
        """`compute_job_cost` ends in `* 100`. Drop that and the planes really do diverge."""
        src = open(os.path.join(REPO, "swarm/agents/resource_agent.py")).read()
        body = src[src.index("def compute_job_cost"):src.index("def selection_main")]
        assert "* 100)" in body, "the analytic cost must stay on the 0..100 range"

    def test_the_llm_plane_is_a_0_100_complement(self):
        """Whatever range the model is asked for, the cost it becomes is 0..100 — that is what
        keeps it comparable with the analytic plane. See `_score_to_cost` and test_elicitation."""
        from swarm.agents.llm.llm_agent import LlmAgent
        a = LlmAgent.__new__(LlmAgent)
        for scale in (100, 1000):
            a.config = {"llm": {"score_scale": scale}}
            a._init_llm_state()
            assert a._score_to_cost(0, None, None) == pytest.approx(NOMINAL_MAX)
            assert a._score_to_cost(scale, None, None) == pytest.approx(0.0)
            assert a._score_to_cost(scale * 0.25, None, None) == pytest.approx(75.0)

    def test_measured_analytic_costs_overlap_the_llm_range(self):
        """Measured over 400 real Pegasus jobs x the 5 shipped flavours (see cost_scale.py).
        Kept as fixed quantiles so a change to weights or penalties surfaces here."""
        measured = {"p10": 2.59, "p25": 5.41, "p50": 11.85, "p75": 28.40, "p90": 71.85}
        llm_typical = (5.0, 25.0)          # scores 95 and 75, the two modal bids
        assert measured["p25"] <= llm_typical[1] <= measured["p75"], \
            "a typical LLM bid must sit inside the analytic interquartile range"
        assert all(0.0 <= v <= NOMINAL_MAX for v in measured.values())

    def test_nothing_rescales_a_cost_on_the_way_to_the_wire(self):
        """The rescaling is gone; a reintroduced one must be a deliberate, reviewed change."""
        from swarm.agents import cost_scale
        assert not hasattr(cost_scale, "to_canonical")
        for path in ("swarm/agents/resource_agent.py", "swarm/agents/llm/llm_agent.py"):
            assert "to_canonical" not in open(os.path.join(REPO, path)).read(), path

    def test_costs_above_the_nominal_max_are_not_clamped(self):
        """1.6% of analytic costs exceed 100, and the load penalty (up to ~2x) pushes either
        plane past it. Clamping would map every loaded agent to the same value."""
        from swarm.agents.resource_agent import ResourceAgent
        agent = ResourceAgent.__new__(ResourceAgent)
        assert agent.proposal_cost(None, 150.0) == 150.0
        assert agent.proposal_cost(None, 959.25) == 959.25
        assert agent.proposal_cost(None, 11.857) == 11.86, "rounded to 2dp, not transformed"

    def test_the_scale_tag_survives_for_instrumentation(self):
        """The tag still distinguishes a real LLM verdict from an analytic fallback."""
        assert CostScale.ANALYTIC != CostScale.LLM

