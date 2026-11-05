"""P0-6: make the LLM's bid a signal that can actually order agents.

The campaign's central negative result is that the LLM's *output* barely reaches placement.
The measured reason is that it barely varies: **59% of qwen2.5:3b bids were the identical value
75.00**, and **gpt-oss-20b put 92% of its bids on two values**, both near the top of the range.
Asking for a 0-100 rating gets answers in round steps, so most agents tie on cost and the bid
cannot order them. A 6x larger model emitted *half* as many distinct values, so the lever is the
elicitation, not the model.

Two arms, both defaulting to the measured baseline:

* `llm.score_scale` — the range the model is asked for, normalised back to a 0..100 cost.
* `llm.tie_break_with_analytic` — order surviving ties by the analytic cost, with a term capped
  at half of one rating step so it can never reorder distinct ratings. That bound is the whole
  safety argument, and it is what these tests pin.
"""
import os
import sys
from collections import OrderedDict

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from swarm.agents.llm.llm_agent import LlmAgent  # noqa: E402
from swarm.agents.llm.llm_bidder import Bid, bid_model_for_scale  # noqa: E402
from swarm.agents.llm.llm_config import LlmConfig  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class _Job:
    def __init__(self, job_id="j1"):
        self.job_id = job_id


class _Agent:
    def __init__(self, agent_id=1):
        self.agent_id = agent_id


def make_agent(score_scale=100, tie_break=False, analytic=11.85, ref=11.85):
    """A bare LlmAgent with only the elicitation state initialised."""
    a = LlmAgent.__new__(LlmAgent)
    a.config = {"llm": {"score_scale": score_scale,
                        "tie_break_with_analytic": tie_break,
                        "tie_break_ref_cost": ref}}
    a._init_llm_state()
    a._cost_job_on_agent = lambda job, agent: analytic
    return a


class TestScoreScale:
    def test_the_default_conversion_is_unchanged(self):
        """Every campaign number was measured with cost = 100 - score."""
        a = make_agent()
        for score in (0.0, 25.0, 75.0, 95.0, 100.0):
            assert a._score_to_cost(score, _Job(), _Agent()) == pytest.approx(100.0 - score)

    def test_a_finer_scale_still_yields_a_0_100_cost(self):
        """The wire scale is fixed at 0..100 whatever the model was asked for."""
        a = make_agent(score_scale=1000)
        assert a._score_to_cost(1000.0, _Job(), _Agent()) == pytest.approx(0.0)
        assert a._score_to_cost(750.0, _Job(), _Agent()) == pytest.approx(25.0)
        assert a._score_to_cost(0.0, _Job(), _Agent()) == pytest.approx(100.0)

    def test_a_finer_scale_gives_a_whole_number_answer_more_room(self):
        """What the arm actually changes. The conversion does not round — the *model* does:
        the campaign's bids came in whole, mostly multiple-of-5 steps. On 0-100 a whole-number
        answer has 101 distinct costs available and neighbours sit 1.0 apart; on 0-1000 it has
        1001, ten times finer, so two agents it rates slightly differently stay separated."""
        coarse, fine = make_agent(score_scale=100), make_agent(score_scale=1000)
        gap_coarse = abs(coarse._score_to_cost(75, _Job(), _Agent())
                         - coarse._score_to_cost(76, _Job(), _Agent("b")))
        gap_fine = abs(fine._score_to_cost(750, _Job(), _Agent())
                       - fine._score_to_cost(751, _Job(), _Agent("b")))
        assert gap_coarse == pytest.approx(1.0)
        assert gap_fine == pytest.approx(0.1)

        # And the coarse scale genuinely cannot express what the fine one can: a rating of 754
        # has no whole-number equivalent on 0-100.
        coarse_costs = {coarse._score_to_cost(s, _Job(), _Agent()) for s in range(70, 81)}
        fine_costs = {fine._score_to_cost(s, _Job(), _Agent()) for s in range(700, 811)}
        assert len(fine_costs) > len(coarse_costs)

    def test_out_of_range_scores_are_clamped(self):
        a = make_agent()
        assert a._score_to_cost(-10.0, _Job(), _Agent()) == pytest.approx(100.0)
        assert a._score_to_cost(500.0, _Job(), _Agent()) == pytest.approx(0.0)

    def test_the_schema_carries_the_bound_not_just_the_prompt(self):
        """Ollama's NativeOutput drives the model from the JSON schema, so a prompt saying
        0-1000 against a schema saying 0-100 gets silently clamped answers."""
        assert bid_model_for_scale(100) is Bid
        schema = bid_model_for_scale(1000).model_json_schema()["properties"]["score"]
        assert schema["maximum"] == 1000.0
        assert schema["minimum"] == 0.0

    def test_the_config_carries_the_scale(self):
        assert LlmConfig.from_dict({}).score_scale == 100
        assert LlmConfig.from_dict({"score_scale": 1000}).score_scale == 1000

    def test_the_shipped_prompt_states_the_range_it_is_configured_for(self):
        """A prompt hardcoded to 0-100 would lie to the model whenever the scale changes."""
        from swarm.utils.yaml_strict import safe_load
        prompt = safe_load(open(os.path.join(REPO, "config_swarm_multi.yml")))["llm"]["prompts"]["cost"]
        assert "{scale}" in prompt
        rendered = prompt.format(scale=1000)
        assert "0-1000" in rendered and "0-100)" not in rendered


class TestAnalyticTieBreak:
    """The safety property: it separates exact ties and nothing else."""

    def test_off_by_default(self):
        a = make_agent()
        assert a._score_to_cost(75.0, _Job(), _Agent()) == pytest.approx(25.0)

    def test_it_separates_two_agents_that_tie(self):
        busy = make_agent(tie_break=True, analytic=71.85)     # analytic p90
        idle = make_agent(tie_break=True, analytic=5.41)      # analytic p25
        assert idle._score_to_cost(75.0, _Job(), _Agent()) < \
            busy._score_to_cost(75.0, _Job(), _Agent()), "the idler agent must win the tie"

    @pytest.mark.parametrize("scale", [100, 1000])
    @pytest.mark.parametrize("analytic", [0.0, 2.59, 11.85, 71.85, 959.25, 1e6])
    def test_it_can_never_reorder_two_distinct_ratings(self, scale, analytic):
        """The bound that makes this safe: at most half a rating step. Even the most extreme
        analytic cost must not let a worse rating overtake a better one."""
        step = 100.0 / scale
        extreme = make_agent(score_scale=scale, tie_break=True, analytic=analytic)
        clean = make_agent(score_scale=scale, tie_break=True, analytic=0.0)
        better = clean._score_to_cost(scale * 0.75 + 1, _Job(), _Agent())      # one step better
        worse = extreme._score_to_cost(scale * 0.75, _Job(), _Agent("b"))
        assert better < worse, f"scale={scale} analytic={analytic} step={step}"

    @pytest.mark.parametrize("scale", [100, 1000])
    def test_distinct_grid_points_are_never_reordered(self, scale):
        """The exact invariant, swept over fractional scores with the analytic costs stacked
        the wrong way. `Bid.score` is a float, so a model may answer 75.1; capping the term at
        half a step is not enough on its own, because two scores 0.1 apart differ by only 0.1
        in cost. Quantising to the rating grid is what closes it: whenever two scores land on
        different grid points the higher one wins outright, however bad its analytic cost."""
        handicapped = make_agent(score_scale=scale, tie_break=True, analytic=1e9)
        favoured = make_agent(score_scale=scale, tie_break=True, analytic=0.0)
        base = scale * 0.75
        scores = [base + i * 0.1 for i in range(40)]
        compared = 0
        for a in scores:
            for b in scores:
                if a <= b or round(a) == round(b):
                    continue                     # same grid point: tied by design, see below
                compared += 1
                assert handicapped._score_to_cost(a, _Job(), _Agent()) < \
                    favoured._score_to_cost(b, _Job(), _Agent("b")), \
                    f"scale={scale}: rating {a} lost to {b}"
        assert compared > 0, "the sweep must actually cross grid points"

    def test_scores_inside_one_grid_cell_are_treated_as_tied(self):
        """The deliberate trade: sub-grid precision is discarded so the guarantee is exact.
        Ask for a finer grid with score_scale instead."""
        idle = make_agent(score_scale=100, tie_break=True, analytic=2.59)
        busy = make_agent(score_scale=100, tie_break=True, analytic=71.85)
        # 75.1 and 75.4 both round to 75, so the analytic cost decides — and it decides sanely.
        assert idle._score_to_cost(75.4, _Job(), _Agent()) < \
            busy._score_to_cost(75.1, _Job(), _Agent("b"))

    def test_quantisation_only_happens_on_the_tie_break_branch(self):
        """The default path must stay byte-identical to what the campaign measured."""
        a = make_agent(tie_break=False)
        assert a._score_to_cost(75.4, _Job(), _Agent()) == pytest.approx(24.6)
        assert a._score_to_cost(75.6, _Job(), _Agent()) == pytest.approx(24.4)

    def test_the_term_is_bounded_by_half_a_step(self):
        base = make_agent(score_scale=100, tie_break=False)._score_to_cost(75.0, _Job(), _Agent())
        for analytic in (0.0, 1.0, 11.85, 100.0, 1e9):
            a = make_agent(score_scale=100, tie_break=True, analytic=analytic)
            delta = a._score_to_cost(75.0, _Job(), _Agent()) - base
            assert 0.0 <= delta < 0.5, analytic

    def test_it_is_monotone_in_the_analytic_cost(self):
        costs = [make_agent(tie_break=True, analytic=x)._score_to_cost(75.0, _Job(), _Agent())
                 for x in (0.0, 1.0, 5.41, 11.85, 28.4, 71.85, 959.25)]
        assert costs == sorted(costs)
        assert len(set(costs)) == len(costs)

    def test_an_unavailable_analytic_cost_leaves_the_bid_alone(self):
        a = make_agent(tie_break=True)
        a._cost_job_on_agent = lambda job, agent: (_ for _ in ()).throw(RuntimeError("no info"))
        assert a._score_to_cost(75.0, _Job(), _Agent()) == pytest.approx(25.0)

    def test_a_nan_analytic_cost_is_ignored(self):
        a = make_agent(tie_break=True, analytic=float("nan"))
        assert a._score_to_cost(75.0, _Job(), _Agent()) == pytest.approx(25.0)


class TestBidDistribution:
    """Instrumentation for E4/T5: a plane whose bids all tie cannot be ordering agents."""

    def test_empty_before_any_bid(self):
        assert make_agent().bid_distribution() == {"count": 0, "distinct": 0, "modal_share": 0.0}

    def test_it_reproduces_the_campaign_shape(self):
        """59% of 1064 bids at one value is what the counter has to make visible."""
        a = make_agent()
        for _ in range(626):
            a._score_to_cost(75.0, _Job(), _Agent())
        for i in range(438):
            a._score_to_cost(50.0 + (i % 45), _Job(), _Agent())
        d = a.bid_distribution()
        assert d["count"] == 1064
        assert d["modal_share"] == pytest.approx(626 / 1064, abs=0.01)
        assert d["distinct"] > 1

    def test_a_finer_scale_shows_up_as_more_distinct_values(self):
        coarse = make_agent(score_scale=100)
        fine = make_agent(score_scale=1000)
        for i in range(200):
            coarse._score_to_cost(70.0 + (i % 10) * 0.4, _Job(), _Agent())
            fine._score_to_cost(700.0 + (i % 10) * 4, _Job(), _Agent())
        assert fine.bid_distribution()["distinct"] >= coarse.bid_distribution()["distinct"]

    def test_the_tracker_is_bounded(self):
        a = make_agent(score_scale=1000)
        for i in range(9000):
            a._score_to_cost(float(i % 8000), _Job(), _Agent())
        assert a.bid_distribution()["distinct"] <= 4096
        assert a.bid_distribution()["count"] == 9000


class TestStatsExposure:
    def test_the_stats_line_reports_the_bid_spread(self):
        src = open(os.path.join(REPO, "swarm/agents/resource_agent.py")).read()
        body = src[src.index("def _log_perf_stats"):src.index("def compute_job_cost")]
        assert "bid_distribution" in body
        assert "modal_share" in body
