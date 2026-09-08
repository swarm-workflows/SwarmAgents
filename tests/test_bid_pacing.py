"""P0-7: stop a failed bid from out-racing a real one.

The campaign's sharpest result is that placement is decided by **when** an agent bids, not what
it bids:

* slowing 15 of 30 agents by +3s on a ~10s bid changed placement not at all — capture ratio
  1.00x, and no trend out to +60s;
* taking the LLM away from 8 of 30 gave those agents **280 of 300 jobs**, 38.5x the healthy
  rate, because a failed bid skips inference and returns in ~0s.

Regime, not degree. A 30% slowdown leaves both groups racing on the same timescale; a fallback
bid is two orders of magnitude faster and wins outright. So the pathology is that the fallback
path is far cheaper than the path it stands in for, and failing is rewarded with the work.

`fallback_parity` holds a fallback until it has cost what a real bid costs. `uniform` holds every
bid to the same target, so arrival time carries no information about the agent — the direct test
of the race-to-propose finding.
"""
import os
import sys
import time
from collections import deque

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from swarm.agents.llm.llm_agent import LlmAgent  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class _Log:
    """A real agent always has a logger; the __new__ stub has to supply one."""

    def __getattr__(self, _name):
        return lambda *a, **k: None


_OMIT = object()


def make_agent(mode="none", target=0.0, quantile=0.5, max_s=30.0,
               min_samples=8, bootstrap=0.0, timeout_s=0.0):
    """`timeout_s=_OMIT` leaves the key out of the config entirely, which is the shape a config
    that never mentions it has — and the shape that used to leave pacing inert."""
    a = LlmAgent.__new__(LlmAgent)
    llm = {"bid_pacing": mode, "bid_pacing_target_s": target,
           "bid_pacing_quantile": quantile, "bid_pacing_max_s": max_s,
           "bid_pacing_min_samples": min_samples,
           "bid_pacing_bootstrap_s": bootstrap}
    if timeout_s is not _OMIT:
        llm["timeout_seconds"] = timeout_s
    a.config = {"llm": llm}
    a._init_llm_state()
    a.shutdown = False
    a.logger = _Log()
    return a


@pytest.fixture
def clock(monkeypatch):
    """A fake clock: `_pace_bid` sleeps by advancing it, so tests are exact and instant."""
    import swarm.agents.llm.llm_agent as mod

    state = {"now": 1000.0, "slept": 0.0}
    monkeypatch.setattr(mod.time, "time", lambda: state["now"])

    def _sleep(seconds):
        state["slept"] += seconds
        state["now"] += seconds

    monkeypatch.setattr(mod.time, "sleep", _sleep)
    return state


class TestTargetSelection:
    def test_an_explicit_target_wins(self):
        a = make_agent(mode="fallback_parity", target=7.5)
        a._record_bid_latency(1.0)
        assert a._pace_target_s() == pytest.approx(7.5)

    def test_the_target_is_derived_from_observed_latencies(self):
        """Self-calibrating: a gateway bid and a local-Ollama bid differ several-fold, so one
        config has to work for every cell of E4."""
        a = make_agent(mode="fallback_parity", min_samples=5)
        for v in (4.0, 5.0, 6.0, 7.0, 20.0):
            a._record_bid_latency(v)
        assert a._pace_target_s() == pytest.approx(6.0)          # median

    def test_a_higher_quantile_paces_to_a_slower_bid(self):
        """`uniform` needs this: at the median, half the fleet still finishes early and races."""
        a = make_agent(mode="uniform", quantile=0.9, min_samples=5)
        for v in (4.0, 5.0, 6.0, 7.0, 20.0):
            a._record_bid_latency(v)
        assert a._pace_target_s() == pytest.approx(20.0)

    def test_a_quantile_over_too_few_samples_is_not_used(self):
        """One or two observations are noise, not a distribution."""
        a = make_agent(mode="fallback_parity", min_samples=8, bootstrap=6.0)
        a._record_bid_latency(0.2)
        assert a._pace_target_s() == pytest.approx(6.0), "bootstrap holds until there are enough"
        for _ in range(8):
            a._record_bid_latency(9.0)
        assert a._pace_target_s() == pytest.approx(9.0)

    def test_only_successful_bids_are_recorded(self):
        a = make_agent(mode="fallback_parity")
        for bad in (0.0, None, -1.0):
            a._record_bid_latency(bad)
        assert len(a._bid_latencies) == 0

    def test_the_latency_window_is_bounded(self):
        a = make_agent(mode="fallback_parity")
        for i in range(1000):
            a._record_bid_latency(float(i + 1))
        assert len(a._bid_latencies) == 256

    def test_an_unknown_mode_is_refused_at_construction(self):
        with pytest.raises(ValueError, match="bid_pacing"):
            make_agent(mode="sometimes")


class TestTheAlwaysFailingAgent:
    """The population P0-7 exists for, and the one a naive derivation misses entirely.

    In S05 the faulted agents have `LLMUnavailable` for the WHOLE run: they never complete a
    bid, so a target derived only from successful bids never exists, and pacing silently does
    nothing on exactly the agents it is meant to slow down. That is not a corner case — it is
    100% of the faulted population and the entire measured 38.5x capture.
    """

    def test_it_paces_from_the_bootstrap_with_no_successful_bid_ever(self):
        a = make_agent(mode="fallback_parity", bootstrap=6.0)
        assert a._bid_latencies == deque(), "precondition: the LLM has never succeeded"
        assert a._pace_target_s() == pytest.approx(6.0)

    def test_the_bootstrap_defaults_to_the_enforced_timeout(self):
        """With the timeout enforced, it is an upper bound on a successful bid, so pacing to it
        guarantees a fallback cannot arrive before a real bid could have."""
        a = make_agent(mode="fallback_parity", timeout_s=6.0)
        assert a._pace_target_s() == pytest.approx(6.0)

    def test_an_explicit_bootstrap_beats_the_timeout(self):
        a = make_agent(mode="fallback_parity", bootstrap=12.0, timeout_s=6.0)
        assert a._pace_target_s() == pytest.approx(12.0)

    def test_a_broken_agent_is_actually_held_with_the_shipped_style_config(self, clock):
        """No explicit target anywhere — only `timeout_seconds`, as the shipped config has."""
        a = make_agent(mode="fallback_parity", timeout_s=6.0)
        a._pace_bid(started_at=clock["now"], reason="fallback")
        assert clock["slept"] == pytest.approx(6.0), \
            "an always-503 agent must still be slowed, or P0-7 does nothing in S05"

    def test_an_omitted_timeout_key_still_bootstraps(self):
        """The key resolves through LlmConfig, which defaults it to 6s — the same value the
        bidder enforces. Reading the raw dict with a default of 0 meant that omitting the key
        gave the bidder a 6s bound and pacing a 0s one, so pacing was inert on the config most
        likely to be in use."""
        a = make_agent(mode="fallback_parity", timeout_s=_OMIT)
        assert "timeout_seconds" not in a.config["llm"], "precondition: the key is absent"
        assert a._pace_target_s() == pytest.approx(6.0)

    def test_the_bootstrap_matches_the_deadline_the_bidder_enforces(self):
        """One key, one default. Two defaults for one setting is how this broke, and how
        runtime.peer_expiry_seconds broke before it."""
        from swarm.agents.llm.llm_config import LlmConfig
        for cfg in ({}, {"timeout_seconds": 9}, {"timeout_seconds": 0}):
            a = LlmAgent.__new__(LlmAgent)
            a.config = {"llm": dict(cfg, bid_pacing="fallback_parity")}
            a._init_llm_state()
            assert a.bid_pacing_bootstrap_s == pytest.approx(
                float(LlmConfig.from_dict(cfg).timeout_seconds)), cfg

    def test_an_explicitly_zeroed_timeout_really_means_no_bound(self):
        """Setting it to 0 disables the bid deadline, so there genuinely is nothing to pace to."""
        a = make_agent(mode="fallback_parity", timeout_s=0.0)
        a.logger = type("L", (), {"__getattr__": lambda s, n: lambda *x, **k: None})()
        assert a._pace_target_s() == 0.0

    def test_with_no_target_available_at_all_it_warns_once(self):
        """Pacing on but inert is the dangerous state: it looks configured and does nothing."""
        a = make_agent(mode="fallback_parity")          # no target, no bootstrap, no timeout
        warned = []
        a.logger = type("L", (), {"__getattr__": lambda s, n: (
            (lambda *x, **k: warned.append(x)) if n == "warning" else (lambda *x, **k: None))})()
        assert a._pace_target_s() == 0.0
        assert a._pace_target_s() == 0.0
        assert len(warned) == 1, "warn once, not on every bid"
        assert "INERT" in str(warned[0])


class TestPacing:
    def test_a_fallback_is_held_to_the_target(self, clock):
        a = make_agent(mode="fallback_parity", target=6.0)
        a._pace_bid(started_at=clock["now"], reason="t")
        assert clock["slept"] == pytest.approx(6.0)
        assert a.bid_pacing_waits == 1

    def test_it_tops_up_rather_than_adding(self, clock):
        """A timeout failure has already burned `timeout_seconds`; it must not then wait the
        full target on top, or a slow failure is punished twice."""
        a = make_agent(mode="fallback_parity", target=6.0)
        a._pace_bid(started_at=clock["now"] - 4.0, reason="t")
        assert clock["slept"] == pytest.approx(2.0)

    def test_a_bid_already_over_the_target_waits_not_at_all(self, clock):
        a = make_agent(mode="fallback_parity", target=6.0)
        a._pace_bid(started_at=clock["now"] - 30.0, reason="t")
        assert clock["slept"] == 0.0
        assert a.bid_pacing_waits == 0

    def test_the_wait_is_capped(self, clock):
        """A slow outlier in a short window must not stall the selection loop."""
        a = make_agent(mode="fallback_parity", target=600.0, max_s=5.0)
        a._pace_bid(started_at=clock["now"], reason="t")
        assert clock["slept"] == pytest.approx(5.0)

    def test_no_target_means_no_wait(self, clock):
        a = make_agent(mode="fallback_parity")
        a._pace_bid(started_at=clock["now"], reason="t")
        assert clock["slept"] == 0.0

    def test_shutdown_interrupts_the_wait(self, monkeypatch):
        """A 30s cap per outstanding bid must not become a 30s shutdown."""
        a = make_agent(mode="fallback_parity", target=30.0)
        slept = []
        import swarm.agents.llm.llm_agent as mod

        def _sleep(seconds):
            slept.append(seconds)
            a.shutdown = True          # the agent is told to stop mid-wait

        monkeypatch.setattr(mod.time, "sleep", _sleep)
        started = time.time()
        a._pace_bid(started_at=started, reason="t")
        assert sum(slept) < 1.0, "the wait must abandon promptly, not run to the target"

    def test_the_wait_is_sliced_not_one_long_sleep(self, clock):
        """So the shutdown check above is reached often enough to matter."""
        a = make_agent(mode="fallback_parity", target=3.0)
        slices = []
        import swarm.agents.llm.llm_agent as mod
        monkeypatch_sleep = mod.time.sleep

        def _sleep(seconds):
            slices.append(seconds)
            monkeypatch_sleep(seconds)

        mod.time.sleep = _sleep
        try:
            a._pace_bid(started_at=clock["now"], reason="t")
        finally:
            mod.time.sleep = monkeypatch_sleep
        assert len(slices) > 1
        assert max(slices) <= 0.25 + 1e-9

    def test_seconds_held_are_accumulated_for_reporting(self, clock):
        a = make_agent(mode="fallback_parity", target=2.0)
        for _ in range(3):
            a._pace_bid(started_at=clock["now"], reason="t")
        assert a.bid_pacing_waits == 3
        assert a.bid_pacing_stats()["seconds"] == pytest.approx(6.0)


class TestModesReachTheRightBranches:
    """Which branches call the pacing, asserted against the source."""

    @staticmethod
    def _cost_fn_source():
        src = open(os.path.join(REPO, "swarm/agents/llm/llm_agent.py")).read()
        start = src.index("def _llm_or_analytic_cost")
        return src[start:src.index("def ", start + 10)]

    def test_the_fallback_branch_paces(self):
        body = self._cost_fn_source()
        tail = body[body.index("[LLM_COST_FALLBACK]"):]
        assert "_pace_bid" in tail
        assert "PACING_FALLBACK" in tail and "PACING_UNIFORM" in tail

    def test_the_success_branch_paces_only_under_uniform(self):
        body = self._cost_fn_source()
        head = body[:body.index("except Exception")]
        assert "_pace_bid" in head
        assert "PACING_UNIFORM" in head
        assert "PACING_FALLBACK" not in head, "a successful bid is not the S05 pathology"

    def test_the_no_bid_branch_is_left_instant(self):
        """An agent that is not a candidate gains nothing by being quick to say so."""
        body = self._cost_fn_source()
        no_bid = body[body.index("[LLM_COST_NO_BID]"):body.index("[LLM_COST_FALLBACK]")]
        assert "_pace_bid" not in no_bid

    def test_a_successful_bid_records_its_latency(self):
        assert "_record_bid_latency" in self._cost_fn_source()

    def test_pacing_is_off_by_default(self):
        from swarm.utils.yaml_strict import safe_load
        cfg = safe_load(open(os.path.join(REPO, "config_swarm_multi.yml")))["llm"]
        assert cfg["bid_pacing"] == "none", "the campaign baseline is what ships"
        assert make_agent().bid_pacing == LlmAgent.PACING_NONE

    def test_the_stats_line_reports_pacing(self):
        src = open(os.path.join(REPO, "swarm/agents/resource_agent.py")).read()
        body = src[src.index("def _log_perf_stats"):src.index("def compute_job_cost")]
        assert "bid_pacing_stats" in body


class TestTheEffectItIsFor:
    """End to end through `_llm_or_analytic_cost` with a bidder that fails instantly — the S05
    shape — comparing arrival times with pacing off and on."""

    @staticmethod
    def _agent(mode, target):
        import types

        a = make_agent(mode=mode, target=target)
        a.agent_id = 7
        a.llm_disable_fallback = False
        a.topology = types.SimpleNamespace(level=0, group=0)
        a.repository = types.SimpleNamespace(save=lambda *x, **k: None)
        a._get_peer_context = lambda: {"peer_agents": {}}
        a._cost_job_on_agent = lambda job, agent: 11.85
        a.bidder = types.SimpleNamespace(
            score=lambda **_k: (_ for _ in ()).throw(RuntimeError("503 Service Unavailable")))
        return a

    class _Job:
        job_id = "j1"
        reasoning_time = None

        def to_dict(self, compact=False):
            return {"id": "j1"}

    class _Peer:
        agent_id = 7

        def to_dict(self):
            return {"agent_id": 7}

    def _arrival(self, mode, target, clock):
        a = self._agent(mode, target)
        start = clock["now"]
        cost = a._llm_or_analytic_cost(self._Job(), self._Peer())
        return cost, clock["now"] - start

    def test_without_pacing_a_broken_agent_bids_instantly(self, clock):
        """The pathology: a 503 becomes an analytic bid in ~0s and out-races real reasoning."""
        cost, arrival = self._arrival("none", 10.0, clock)
        assert cost == pytest.approx(11.85), "it still falls back, as the baseline does"
        assert arrival == 0.0

    def test_with_parity_the_same_failure_takes_as_long_as_a_real_bid(self, clock):
        cost, arrival = self._arrival("fallback_parity", 10.0, clock)
        assert cost == pytest.approx(11.85), "the bid itself is unchanged — only its timing"
        assert arrival == pytest.approx(10.0)

    def test_uniform_paces_the_fallback_too(self, clock):
        _cost, arrival = self._arrival("uniform", 10.0, clock)
        assert arrival == pytest.approx(10.0)

    def test_a_successful_bid_is_paced_only_under_uniform(self, clock):
        import types

        class _Bid:
            score = 80.0
            explanation = "ok"
            reasoning_time = 2.0

        for mode, expected in (("none", 0.0), ("fallback_parity", 0.0), ("uniform", 10.0)):
            a = self._agent(mode, 10.0)
            a.bidder = types.SimpleNamespace(score=lambda **_k: _Bid())
            start = clock["now"]
            cost = a._llm_or_analytic_cost(self._Job(), self._Peer())
            assert cost == pytest.approx(20.0), mode
            assert clock["now"] - start == pytest.approx(expected), mode
