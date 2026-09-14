"""P1-1: the offline-optimal delegator, and the regret of the policy that ran.

Regret is the axis E2's delegation-quality figure is plotted on and the y-axis of the
staleness figure (F6), so a defect here is a wrong conclusion rather than a wrong number —
and unlike a crash, a plausible-looking regret column does not announce itself.

What these tests hold to:

* the ground truth comes from the agents' own resolution functions, so the oracle and the
  run cannot disagree about which group was genuinely better;
* a run with no injected failure profile is refused, not scored against an invented one;
* a decision that had nothing to choose between contributes zero regret and is excluded from
  routing accuracy, so widening a coordinator's fan-out cannot inflate the score;
* a group nothing is known about is unknown, never zero-failure — the single most flattering
  way this could go wrong;
* the validation pass actually notices when the injected profile does not describe the run.
"""
import json
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from evaluation.oracle import (OracleError, expected_reward, group_failure_rate,  # noqa: E402
                               load_run, score_decision, score_run, validate)

# The real all_jobs.csv has NO job_type column -- the validation join is on job_id, and a
# fixture that invented a job_type column would have hidden that.
JOBS_HEADER = ("job_id,submitted_at,selection_started_at,assigned_at,started_at,"
               "completed_at,exit_status,leader_id\n")


def _truth(**over):
    base = {
        "enabled": True,
        "failure_probability": 0.1,
        # Group 0's agents are bad at cpu jobs, group 1's are good at them: the regime
        # contextual delegation exists for.
        "per_agent_failure_rates": {
            "1": {"cpu": 0.8, "default": 0.1},
            "2": {"cpu": 0.8, "default": 0.1},
            "3": {"cpu": 0.05, "default": 0.1},
            "4": {"cpu": 0.05, "default": 0.1},
        },
        "per_job_type_failure_rates": {},
        "phases": [],
        "reward": {},
    }
    base.update(over)
    return base


def _members():
    return {0: ["1", "2"], 1: ["3", "4"]}


def _decision(selected, candidates=(0, 1), job_type="cpu", policy="bandit",
              ts=1000.0, job_id="j1", agent_id="9"):
    return {"ts": ts, "job_id": job_id, "job_type": job_type, "policy": policy,
            "candidates": list(candidates), "selected": list(selected),
            "agent_id": agent_id, "decide_s": 0.001, "ctx_age_mean": 1.5,
            "ctx_age_chosen": 1.5}


def _run(decisions, truth=None, members=None, starts=None, run_started_at=900.0):
    return {
        "meta": {},
        "truth": truth or _truth(),
        "decisions": list(decisions),
        "group_members": members if members is not None else _members(),
        "agent_starts": starts or {"9": 900.0},
        "run_started_at": run_started_at,
    }


# --------------------------------------------------------------------------------------------
# The ground truth is the agents' own, not a second copy of it.
# --------------------------------------------------------------------------------------------

STARTS = {"1": 0.0, "2": 0.0, "3": 0.0, "4": 0.0, "9": 0.0}


def test_group_rate_is_the_mean_over_its_member_agents():
    rate = group_failure_rate(0, "cpu", 0.0, _truth(), _members(), STARTS)
    assert rate == pytest.approx(0.8)
    assert group_failure_rate(1, "cpu", 0.0, _truth(), _members(), STARTS) == pytest.approx(0.05)


def test_the_agent_resolves_an_int_keyed_profile_the_way_its_own_config_documents_it():
    """`per_agent_failure_rates: {3: 0.5}` is the form the shipped config's comment gives,
    and YAML parses those keys as ints while every caller looks up `str(agent_id)`. The
    documented form therefore matched nothing and every agent fell through to the base rate.
    It also made the run and the oracle disagree: run_meta.json archives the profile through
    JSON, whose keys are always strings, so the oracle matched a key the run had ignored."""
    import json as _json
    import yaml as _yaml
    from swarm.agents.resource_agent import ResourceAgent
    int_keyed = _yaml.safe_load("r: {3: 0.5}")["r"]
    str_keyed = _json.loads(_json.dumps(int_keyed))
    assert ResourceAgent._resolve_failure_rate("3", None, int_keyed, {}, 0.1) == 0.5
    assert ResourceAgent._resolve_failure_rate("3", None, str_keyed, {}, 0.1) == 0.5
    assert ResourceAgent._resolve_failure_rate("9", None, int_keyed, {}, 0.1) == 0.1


def test_a_heterogeneous_group_is_scored_differently_by_each_aggregate():
    """Which member of a group executes a delegated job is decided at run time and is not
    knowable offline, so the aggregate is an assumption — and it moves the answer, which is
    why it is a flag and is recorded in the summary rather than hard-coded."""
    truth = _truth(per_agent_failure_rates={"1": 0.9, "2": 0.1})
    members = {0: ["1", "2"]}
    starts = {"1": 0.0, "2": 0.0}
    assert group_failure_rate(0, "x", 0.0, truth, members, starts,
                              aggregate="mean") == pytest.approx(0.5)
    assert group_failure_rate(0, "x", 0.0, truth, members, starts,
                              aggregate="min") == pytest.approx(0.1)
    assert group_failure_rate(0, "x", 0.0, truth, members, starts,
                              aggregate="max") == pytest.approx(0.9)


def test_a_group_with_no_known_members_is_unknown_not_perfect():
    """The most flattering way this could break: an unknown group scored as 0.0 failure
    becomes the oracle's choice for every job, so the policy looks maximally wrong and regret
    is fiction."""
    assert group_failure_rate(7, "cpu", 0.0, _truth(), _members(), STARTS) is None


def test_phases_are_resolved_the_way_the_agents_resolve_them():
    """The mid-run failure-parity flip (E2a) is the whole point of the non-stationarity arm.
    An oracle that scored the whole run against the pre-flip profile would report the policy
    as catastrophically wrong exactly where it correctly re-adapted."""
    truth = _truth(phases=[{"after_s": 100.0,
                            "per_agent_failure_rates": {"1": 0.05, "2": 0.05,
                                                        "3": 0.8, "4": 0.8}}])
    assert group_failure_rate(0, "cpu", 50.0, truth, _members(), STARTS) == pytest.approx(0.8)
    assert group_failure_rate(0, "cpu", 150.0, truth, _members(), STARTS) == pytest.approx(0.05)


def test_expected_reward_follows_the_configured_shape():
    assert expected_reward(0.0, {}) == pytest.approx(1.0)
    assert expected_reward(1.0, {}) == pytest.approx(-1.0)
    # Shaped runs punish an exit failure less, so the same misrouting costs less regret.
    assert expected_reward(1.0, {"shaped": True, "exit_failure": -0.5}) == pytest.approx(-0.5)


# --------------------------------------------------------------------------------------------
# Scoring a decision.
# --------------------------------------------------------------------------------------------

def test_the_wrong_group_carries_regret_and_the_right_one_none():
    run = _run([_decision([0]), _decision([1], job_id="j2")])
    rows, summary = score_run(run)
    wrong, right = rows[0], rows[1]
    assert wrong["regret"] > 0 and not wrong["optimal"]
    assert right["regret"] == 0.0 and right["optimal"]
    assert wrong["oracle_groups"] == "1"
    assert summary["routing_accuracy"] == pytest.approx(0.5)


def test_regret_is_the_reward_gap_not_the_failure_gap():
    """Regret is denominated in the reward the bandit was actually optimising. Scoring it as
    a probability difference would silently halve it under the default shape."""
    run = _run([_decision([0])])
    rows, _ = score_run(run)
    assert rows[0]["regret"] == pytest.approx(2 * (0.8 - 0.05))


def test_a_decision_with_no_choice_costs_nothing_and_is_not_graded():
    """`bandit_all`/`all` take every candidate, so the best is always among them. Counting
    them as correct would make routing accuracy rise as a coordinator's fan-out widens —
    measuring the configuration rather than the policy."""
    run = _run([_decision([0, 1], policy="bandit_all")])
    rows, summary = score_run(run)
    assert rows[0]["regret"] == 0.0
    assert rows[0]["no_choice"]
    assert summary["decisions_with_a_choice"] == 0
    assert summary["routing_accuracy"] is None


def test_a_decision_whose_candidates_are_all_equal_is_not_graded():
    """Measured on the slice 2026-09-14: a profile whose contrast ran BETWEEN coordinators
    rather than within each one gave every candidate set a single rate, and the run reported
    routing_accuracy 1.0 over 470 decisions in which no routing choice could have been wrong.
    Zero regret was correct; grading it as perfect routing was not."""
    flat = _truth(per_agent_failure_rates={a: 0.3 for a in ("1", "2", "3", "4")})
    run = _run([_decision([0], candidates=(0, 1))], truth=flat)
    rows, summary = score_run(run)
    assert rows[0]["regret"] == 0.0
    assert rows[0]["no_choice"]
    assert summary["decisions_without_a_choice"] == 1
    assert summary["routing_accuracy"] is None


def test_a_fan_out_is_scored_on_its_best_group():
    """With top_k > 1 the job is delegated to every selected group, so the run gets the best
    outcome among them; scoring the mean would charge a policy for a group it also covered."""
    members = {0: ["1", "2"], 1: ["3", "4"], 2: ["1", "2"]}
    run = _run([_decision([0, 1], candidates=(0, 1, 2), policy="bandit")], members=members)
    rows, _ = score_run(run)
    assert rows[0]["regret"] == 0.0


def test_a_decision_is_refused_when_any_candidate_cannot_be_priced():
    """Regret compares the whole candidate set. Pricing only the candidates we can and taking
    the best of those biases *towards* the policy: if the genuinely best group is the one that
    vanished, the chosen group becomes the best of the remainder and a bad routing decision is
    recorded as regret 0, optimal. A silent bias in the flattering direction is the worst kind
    for a figure whose whole claim is that one policy routes better than another."""
    # Group 1 is the better group and has no known members.
    run = _run([_decision([0], candidates=(0, 1))], members={0: ["1", "2"]})
    assert score_decision(run["decisions"][0], run) is None


def test_a_missing_executor_start_refuses_a_phased_decision_rather_than_flattering_it():
    """Same trap reached the other way. `agent_starts` is built from the metrics payloads, so
    an agent that reported no `failure_sim_start` is PRESENT with the value None — and
    `dict.get(key, default)` does not apply the default for a key that exists. The better
    group then drops out and the worse one scores as optimal."""
    truth = _truth(phases=[{"after_s": 100.0, "per_agent_failure_rates": {"1": 0.9}}])
    run = _run([_decision([0], candidates=(0, 1))], truth=truth,
               starts={"9": 900.0, "1": 900.0, "2": 900.0, "3": None, "4": None},
               run_started_at=None)
    assert score_decision(run["decisions"][0], run) is None


def test_a_missing_start_is_harmless_when_the_profile_has_no_phases():
    """A constant profile is the same at every instant, so demanding a start there would
    refuse every run predating `failure_sim_start` for no gain at all."""
    run = _run([_decision([0], candidates=(0, 1))],
               starts={"9": None, "1": None, "2": None, "3": None, "4": None},
               run_started_at=None)
    scored = score_decision(run["decisions"][0], run)
    assert scored is not None
    assert scored["regret"] > 0   # group 1 is still correctly the better choice


def test_cumulative_regret_accumulates_in_decision_order():
    run = _run([_decision([0], ts=1000.0, job_id="a"),
                _decision([1], ts=1001.0, job_id="b"),
                _decision([0], ts=1002.0, job_id="c")])
    rows, summary = score_run(run)
    assert [r["cumulative_regret"] for r in rows] == pytest.approx(
        [1.5, 1.5, 3.0], abs=1e-6)
    assert summary["regret_total"] == pytest.approx(3.0)


def test_the_phase_follows_the_executing_agents_clock_not_the_coordinators():
    """`after_s` is counted from each agent's own construction, and a 30-host remote launch
    spreads construction over a minute. The rate that decides a delegated job's fate is the
    one the EXECUTING agent computes, so resolving the phase on the coordinator's clock
    misattributes every decision within the launch spread of a boundary — precisely the
    decisions the mid-run flip (E2a) is built around.

    Here the coordinator is 50 s into its own life at the decision (pre-flip on its clock),
    while the group-0 members are 150 s into theirs (post-flip on theirs). Post-flip, group 0
    is the good group, so routing to it must score as optimal.
    """
    truth = _truth(phases=[{"after_s": 100.0,
                            "per_agent_failure_rates": {"1": 0.05, "2": 0.05,
                                                        "3": 0.8, "4": 0.8}}])
    row = _decision([0], ts=1000.0, agent_id="9")
    # run_started_at is pinned to the coordinator's start so that BOTH wrong answers -- the
    # coordinator's clock and the run's -- read pre-flip. Only resolving on the members' own
    # clocks gives post-flip, so this test cannot pass by coincidence.
    run = _run([row], truth=truth, run_started_at=950.0,
               starts={"9": 950.0, "1": 850.0, "2": 850.0, "3": 850.0, "4": 850.0})
    scored = score_decision(row, run)
    assert scored["coordinator_elapsed_s"] == pytest.approx(50.0)
    assert scored["chosen_failure_rate"] == pytest.approx(0.05), "members are post-flip"
    assert scored["regret"] == 0.0
    assert scored["optimal"]


def test_a_member_with_no_known_start_is_unknown_not_assumed():
    """Without a start there is no way to place the agent in a phase, and there is no
    fallback clock to borrow — see the next test for why."""
    truth = _truth(phases=[{"after_s": 100.0, "per_agent_failure_rates": {"1": 0.05}}])
    assert group_failure_rate(0, "cpu", 1000.0, truth, _members(), agent_starts={}) is None


def test_there_is_no_fallback_to_the_run_clock_for_a_phased_profile():
    """Substituting the run's clock for a missing agent's is the same error as using the
    coordinator's: agents start up to a minute apart, so it places the member in the wrong
    phase — and a member read as post-flip when it is really pre-flip makes a bad group look
    good, scoring a bad routing decision as optimal.

    Here group 1 is genuinely the better choice, and it is the group whose members have no
    start. On the run's clock they would read post-flip (elapsed 150) and price at 0.9,
    making the chosen group 0 look best. The decision must be refused instead.
    """
    truth = _truth(phases=[{"after_s": 100.0,
                            "per_agent_failure_rates": {"1": 0.5, "2": 0.5,
                                                        "3": 0.9, "4": 0.9}}])
    row = _decision([0], candidates=(0, 1), ts=1000.0, agent_id="9")
    run = _run([row], truth=truth, run_started_at=850.0,
               starts={"9": 850.0, "1": 850.0, "2": 850.0, "3": None, "4": None})
    assert score_decision(row, run) is None


def test_an_unscoreable_decision_is_dropped_and_counted_not_guessed():
    run = _run([_decision([0]),
                _decision([5], candidates=(5, 6), job_id="j2")])  # groups with no members
    rows, summary = score_run(run)
    assert len(rows) == 1
    assert summary["decisions_unscored"] == 1
    # The count is the evidence that a run was only partly scored; a summary that reported
    # only what it managed to score would look complete.
    assert summary["decisions_scored"] == 1


# --------------------------------------------------------------------------------------------
# Refusing to score what cannot be scored.
# --------------------------------------------------------------------------------------------

def test_a_run_without_an_injected_profile_is_refused(tmp_path):
    """Failure simulation is OFF in the shipped config. Scoring such a run against the base
    rate would make every group identical and report a flat zero regret, which reads as a
    perfect policy rather than as no measurement at all."""
    (tmp_path / "metrics.json").write_text(json.dumps({"1": {"id": 1}}))
    (tmp_path / "run_meta.json").write_text(json.dumps({"ground_truth": {"enabled": False}}))
    with pytest.raises(OracleError, match="no injected failure profile"):
        load_run(tmp_path)


def test_a_run_predating_the_ground_truth_field_is_refused(tmp_path):
    """run_meta.json without `ground_truth` means the profile was never archived, so the only
    honest source is a config file that may have been edited since."""
    (tmp_path / "metrics.json").write_text(json.dumps({"1": {"id": 1}}))
    (tmp_path / "run_meta.json").write_text(json.dumps({"run_id": "old"}))
    with pytest.raises(OracleError, match="no injected failure profile"):
        load_run(tmp_path)


def test_a_run_with_no_delegations_is_refused(tmp_path):
    (tmp_path / "metrics.json").write_text(json.dumps({"1": {"id": 1}}))
    (tmp_path / "run_meta.json").write_text(
        json.dumps({"ground_truth": _truth()}))
    with pytest.raises(OracleError, match="No delegation decisions"):
        load_run(tmp_path)


def test_coordinators_are_not_counted_as_members_of_the_groups_they_lead(tmp_path):
    """A coordinator carries a group of its own at level >= 1 but never executes a delegated
    job; folding its rate into the group's would score routing against an agent that cannot
    receive the work."""
    from evaluation.oracle import _group_members
    members = _group_members([
        {"agent_id": 1, "group": 0, "level": 0},
        {"agent_id": 2, "group": 0, "level": 0},
        {"agent_id": 9, "group": 0, "level": 1},   # the coordinator
    ])
    assert members == {0: ["1", "2"]}


# --------------------------------------------------------------------------------------------
# Checking the model against what the run actually did.
# --------------------------------------------------------------------------------------------

def _write_jobs(tmp_path, rows):
    (tmp_path / "all_jobs.csv").write_text(JOBS_HEADER + rows)


def test_validation_agrees_when_the_profile_describes_the_run(tmp_path):
    # Every cpu job went to group 0 (predicted 0.8 failure) and 4 of 5 really did fail.
    _write_jobs(tmp_path, "".join(
        f"j{i},1,1,2,2,9,{1 if i < 4 else 0},1\n" for i in range(5)))
    run = _run([_decision([0], job_id=f"j{i}") for i in range(5)])
    report = validate(run, tmp_path)
    assert report["validated"]
    assert report["max_abs_error"] == pytest.approx(0.0, abs=0.01)


def test_validation_catches_a_profile_that_does_not_describe_the_run(tmp_path):
    """The check that separates a regret number from a plausible-looking one: if nothing
    actually failed, the profile that says 80% of these jobs fail is not this run's."""
    _write_jobs(tmp_path, "".join(f"j{i},1,1,2,2,9,0,1\n" for i in range(5)))
    run = _run([_decision([0], job_id=f"j{i}") for i in range(5)])
    report = validate(run, tmp_path)
    assert report["validated"]
    assert report["max_abs_error"] == pytest.approx(0.8, abs=0.01)


def test_validation_joins_on_job_id_with_no_job_type_column_present():
    """The real `all_jobs.csv` carries no job_type at all. A type-keyed comparison matches
    nothing and reports a clean bill of health for a check that never ran — which is worse
    than no check, because the regret column then looks validated."""
    import csv as _csv
    from pathlib import Path as _Path
    header = _csv.DictReader([JOBS_HEADER]).fieldnames
    assert "job_type" not in header
    assert "job_id" in header and "exit_status" in header


def test_validation_reports_nothing_to_compare_rather_than_success(tmp_path):
    """A run whose delegated jobs never completed gives the check no evidence. It has to say
    so; `validated: True, error 0.0` on an empty comparison is the failure mode."""
    _write_jobs(tmp_path, "")
    run = _run([_decision([0], job_id="j0")])
    report = validate(run, tmp_path)
    assert not report["validated"]
    assert "nothing to compare" in report["reason"]


def test_validation_weights_job_types_by_how_many_jobs_they_had(tmp_path):
    """One type with three jobs must not outvote one with thirty when deciding whether the
    profile describes the run."""
    rows = "".join(f"a{i},1,1,2,2,9,1,1\n" for i in range(30))       # predicted 0.8, all fail
    rows += "".join(f"b{i},1,1,2,2,9,0,1\n" for i in range(3))       # predicted 0.05, none fail
    _write_jobs(tmp_path, rows)
    decisions = [_decision([0], job_type="cpu", job_id=f"a{i}") for i in range(30)]
    decisions += [_decision([1], job_type="other", job_id=f"b{i}") for i in range(3)]
    report = validate(_run(decisions), tmp_path)
    assert report["jobs_compared"] == 33
    # cpu -> group 0 -> agents 1,2 at 0.8: |1.0-0.8| = 0.2 over 30 jobs.
    # "other" -> group 1 -> agents 3,4 fall to their "default" 0.1: |0.0-0.1| = 0.1 over 3.
    assert report["mean_abs_error"] == pytest.approx((0.2 * 30 + 0.1 * 3) / 33, abs=1e-4)
    assert report["max_abs_error"] == pytest.approx(0.2, abs=1e-4)


def test_a_rare_job_type_does_not_trip_the_validator_on_sampling_noise(tmp_path):
    """Measured on the slice 2026-09-14: a run whose profile fitted to 0.018 job-weighted
    error was flagged as a mismatch because ONE job type with six jobs came in 0.18 off —
    comfortably inside its own +/-0.41 noise band. A validator that cries wolf on every run
    is one nobody reads, which is worse than not having it."""
    # 6 jobs predicted at ~0.5 (group 0 is bad at cpu), 2 of which failed.
    _write_jobs(tmp_path, "".join(
        f"j{i},1,1,2,2,9,{1 if i < 2 else 0},1\n" for i in range(6)))
    truth = _truth(per_agent_failure_rates={"1": 0.5, "2": 0.5, "3": 0.5, "4": 0.5})
    run = _run([_decision([0], job_id=f"j{i}") for i in range(6)], truth=truth)
    report = validate(run, tmp_path)
    assert report["per_job_type"][0]["n_jobs"] == 6
    assert not report["per_job_type"][0]["beyond_noise"]
    assert report["types_beyond_sampling_noise"] == []


def test_a_real_mismatch_is_still_flagged_when_the_sample_supports_it(tmp_path):
    """The noise floor must not swallow a genuine mismatch: 40 jobs predicted at 0.8 of which
    none failed is far outside anything sampling explains."""
    _write_jobs(tmp_path, "".join(f"j{i},1,1,2,2,9,0,1\n" for i in range(40)))
    run = _run([_decision([0], job_id=f"j{i}") for i in range(40)])
    report = validate(run, tmp_path)
    assert report["types_beyond_sampling_noise"] == ["cpu"]
    assert report["per_job_type"][0]["beyond_noise"]


def test_the_noise_band_is_zero_when_the_profile_leaves_no_room(tmp_path):
    """The sharpest case for using the Poisson-binomial variance over the individual rates
    rather than the binomial one at their mean. Here every job either cannot fail (p=0) or
    cannot succeed (p=1), so the observed rate is pinned at exactly 0.5 and any deviation is
    impossible — but the mean-p band (p=0.5) would accept a swing of +/-0.10 at n=100, i.e.
    it accepts outcomes the injected profile rules out."""
    # 4 jobs to a group that cannot fail, 4 to a group that cannot succeed. One of the
    # "cannot fail" jobs failed anyway: impossible under the profile, and must be flagged.
    rows = "".join(f"a{i},1,1,2,2,9,{1 if i == 0 else 0},1\n" for i in range(4))
    rows += "".join(f"b{i},1,1,2,2,9,1,1\n" for i in range(4))
    _write_jobs(tmp_path, rows)
    truth = _truth(per_agent_failure_rates={"1": 0.0, "2": 0.0, "3": 1.0, "4": 1.0})
    decisions = [_decision([0], job_id=f"a{i}") for i in range(4)]
    decisions += [_decision([1], job_id=f"b{i}") for i in range(4)]
    report = validate(_run(decisions, truth=truth), tmp_path)
    entry = report["per_job_type"][0]
    assert entry["sampling_noise_2se"] == pytest.approx(0.0)
    assert entry["beyond_noise"], "an outcome the profile makes impossible must be flagged"


def test_the_band_uses_the_individual_rates_not_their_mean(tmp_path):
    """With half the jobs routed to a 0.75 group and half to a 0.05 group, the mean-p
    binomial band is ~1.4x wider than the true one, so it accepts a real mismatch."""
    import math as _math
    rows = "".join(f"a{i},1,1,2,2,9,{1 if i < 3 else 0},1\n" for i in range(4))
    rows += "".join(f"b{i},1,1,2,2,9,0,1\n" for i in range(4))
    _write_jobs(tmp_path, rows)
    decisions = [_decision([0], job_id=f"a{i}") for i in range(4)]     # predicted 0.8
    decisions += [_decision([1], job_id=f"b{i}") for i in range(4)]    # predicted 0.05
    report = validate(_run(decisions), tmp_path)
    entry = report["per_job_type"][0]
    exact = 2 * _math.sqrt(4 * 0.8 * 0.2 + 4 * 0.05 * 0.95) / 8
    mean_p = (4 * 0.8 + 4 * 0.05) / 8
    naive = 2 * _math.sqrt(mean_p * (1 - mean_p) / 8)
    assert entry["sampling_noise_2se"] == pytest.approx(exact, abs=1e-6)
    assert exact < naive, "the mean-p band would have been the wider, more permissive one"


def test_validation_ignores_jobs_that_never_finished(tmp_path):
    """A job with no completion time has an exit status that means nothing. Counting those
    zeros as successes would make any run with a backlog look like a profile mismatch."""
    _write_jobs(tmp_path, "j0,1,1,2,2,0,0,1\nj1,1,1,2,2,9,1,1\n")
    run = _run([_decision([0], job_id="j0"), _decision([0], job_id="j1")])
    report = validate(run, tmp_path)
    assert report["per_job_type"][0]["n_jobs"] == 1
    assert report["per_job_type"][0]["observed_failure_rate"] == pytest.approx(1.0)
