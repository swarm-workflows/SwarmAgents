# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""Unit tests for the post-hoc run-metric collector (evaluation/collect.py).

These guard the properties that would silently corrupt paper numbers: dropping a
livelocked run, guessing a completion denominator, and mis-parsing hyphenated factors.
"""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from evaluation.collect import (  # noqa: E402
    aggregate, dedup_jobs, discover_runs, parse_factors, read_jobs_csv, run_metrics,
)

import pandas as pd  # noqa: E402

HEADER = ("job_id,submitted_at,selection_started_at,assigned_at,started_at,"
          "completed_at,exit_status,leader_id,reasoning_time,scheduling_latency\n")


def write_run(root: Path, name: str, rows: str) -> Path:
    run_dir = root / name
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "all_jobs.csv").write_text(HEADER + rows)
    return run_dir


class TestFactorParsing(unittest.TestCase):
    def test_topology_and_scale(self):
        root = Path("/campaign")
        factors = parse_factors(root / "run-mesh-30-500" / "run03", root)
        self.assertEqual(factors["topology"], "mesh")
        self.assertEqual(factors["agents"], 30)
        self.assertEqual(factors["jobs_planned"], 500)
        self.assertEqual(factors["run"], 3)

    def test_unseparated_scale_and_engine(self):
        root = Path("/campaign")
        factors = parse_factors(root / "demo-mesh120-snowloc-fixed" / "run01", root)
        self.assertEqual(factors["topology"], "mesh")
        self.assertEqual(factors["agents"], 120)
        self.assertEqual(factors["engine"], "snow")
        self.assertTrue(factors["snow_locality"])
        self.assertEqual(factors["code_rev"], "post-fix")

    def test_hyphenated_token_survives_splitting(self):
        # "gpt-oss" must not be split into "gpt" + "oss" before lookup.
        root = Path("/campaign")
        factors = parse_factors(root / "gpt-oss" / "hier-110" / "run02", root)
        self.assertEqual(factors["llm_model"], "gpt-oss")
        self.assertEqual(factors["topology"], "hierarchical")
        self.assertEqual(factors["agents"], 110)

    def test_run_defaults_to_one(self):
        root = Path("/campaign")
        self.assertEqual(parse_factors(root / "hier-30", root)["run"], 1)


class TestJobLoading(unittest.TestCase):
    def test_dedup_prefers_completed_record(self):
        df = pd.DataFrame({
            "job_id": [1, 1, 2],
            "completed_at": [0.0, 500.0, 0.0],
            "leader_id": [7, 9, 3],
        })
        deduped = dedup_jobs(df)
        self.assertEqual(len(deduped), 2)
        row = deduped[deduped.job_id == 1].iloc[0]
        self.assertEqual(row.completed_at, 500.0)
        self.assertEqual(row.leader_id, 9)

    def test_missing_file_is_none_but_empty_file_is_a_frame(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertIsNone(read_jobs_csv(root / "absent.csv"))
            empty = root / "empty.csv"
            empty.write_bytes(b"")
            frame = read_jobs_csv(empty)
            self.assertIsNotNone(frame)
            self.assertTrue(frame.empty)


class TestRunMetrics(unittest.TestCase):
    def test_livelocked_run_is_zero_percent_not_dropped(self):
        """A header-only jobs file means zero jobs were ever assigned -- the key
        PBFT-at-scale data point. It must produce a 0% row, not disappear."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = write_run(Path(tmp), "demo-mesh120-pbft/run01", "")
            metrics = run_metrics(run_dir, expected_jobs=2188)
            self.assertTrue(metrics, "livelocked run must still yield metrics")
            self.assertEqual(metrics["jobs_completed"], 0)
            self.assertEqual(metrics["completion_pct"], 0.0)

    def test_completion_denominator_is_never_guessed(self):
        """Without a declared job count, completion% is NaN -- reporting completed/seen
        would turn a run that placed 206 of 5000 jobs into a triumphant 100%."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = write_run(Path(tmp), "hier-250/run01",
                                "1,100,100.5,101,101,102,0,5,0,1.0\n")
            metrics = run_metrics(run_dir, expected_jobs=None)
            self.assertNotEqual(metrics["completion_basis"], "declared")
            self.assertTrue(pd.isna(metrics["completion_pct"]))
            self.assertEqual(metrics["completion_pct_of_seen"], 100.0)

            declared = run_metrics(run_dir, expected_jobs=5000)
            self.assertEqual(declared["completion_basis"], "declared")
            self.assertEqual(declared["completion_pct"], 0.02)

    def test_selection_and_scheduling_latency_are_distinct(self):
        """Two different quantities have historically both been called 'selection time';
        the collector must keep them apart."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = write_run(Path(tmp), "hier-30/run01",
                                "1,100,110,114,115,120,0,5,2.0,9.0\n")
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertEqual(metrics["selection_mean"], 4.0)      # assigned - selection_started
            self.assertEqual(metrics["sched_latency_mean"], 9.0)  # reported column
            self.assertEqual(metrics["job_latency_mean"], 20.0)   # completed - submitted

    def test_a_metrics_shortfall_is_carried_into_the_row(self):
        """A cell whose agents did not all report has partial per-agent aggregates; the
        collector must say so rather than let it average in as a good cell."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = write_run(root, "run01", "1,100,100.1,100.2,100.3,105,0,3,0.1,0.2\n")
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertTrue(metrics["metrics_complete"])
            self.assertEqual(metrics["agents_missing_metrics"], 0)

            (run_dir / "metrics_shortfall.json").write_text(
                '{"run_id": "x", "missing_agents": [2, 3, 4]}')
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertFalse(metrics["metrics_complete"])
            self.assertEqual(metrics["agents_missing_metrics"], 3)

    def test_reselection_multiplier_counts_repeat_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = write_run(Path(tmp), "hier-30/run01",
                                "1,100,110,114,115,0,0,5,0,9.0\n"
                                "1,100,130,134,135,140,0,6,0,9.0\n")
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertEqual(metrics["job_records"], 2)
            self.assertEqual(metrics["jobs_seen"], 1)
            self.assertEqual(metrics["reselection_multiplier"], 2.0)


class TestDiscoveryAndAggregation(unittest.TestCase):
    def test_discover_nested_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_run(root, "hier-30/run01", "")
            write_run(root, "hier-30/run02", "")
            (root / "not-a-run").mkdir()
            self.assertEqual(len(discover_runs(root)), 2)

    def test_aggregate_reports_n_and_spread(self):
        wide = pd.DataFrame({
            "engine": ["snow", "snow", "pbft"],
            "run": [1, 2, 1],
            "selection_mean": [10.0, 20.0, 100.0],
        })
        agg = aggregate(wide, ["engine"]).set_index("engine")
        self.assertEqual(agg.loc["snow", "n_runs"], 2)
        self.assertEqual(agg.loc["snow", "selection_mean_mean"], 15.0)
        self.assertAlmostEqual(agg.loc["snow", "selection_mean_std"], 7.0710678, places=5)
        self.assertEqual(agg.loc["pbft", "n_runs"], 1)


if __name__ == "__main__":
    unittest.main()


class TestInstrumentationColumns(unittest.TestCase):
    """P0-4 columns, read out of the per-agent metrics.json payloads.

    The wide row must carry the fleet totals the message-complexity figure predicts and a
    context-age distribution weighted by DECISIONS, not by agents — a coordinator that made
    400 decisions and one that made 4 are not equal evidence.
    """

    ROWS = "j1,1,1,2,2,9,0,1,0.1,0.5\n"

    def _run_with_metrics(self, root: Path, payload: dict) -> Path:
        import json
        run_dir = write_run(root, "hier-30/run01", self.ROWS)
        (run_dir / "metrics.json").write_text(json.dumps(payload))
        return run_dir

    def _payload(self, agent_id, decisions, **instr):
        base = {"id": agent_id, "instrumentation": instr}
        if decisions:
            base["delegation_decisions"] = decisions

        return base

    @staticmethod
    def _decision(ts, age, chosen=None, policy="bandit", job="j",
                  skewed=0, skew_max=0.0):
        return {"ts": ts, "job_id": job, "job_type": "cpu", "policy": policy,
                "n_candidates": 2, "candidates": [1, 2], "selected": [1],
                "decide_s": 0.01, "ctx_age_mean": age, "ctx_age_min": age,
                "ctx_age_max": age, "ctx_age_chosen": chosen if chosen is not None else age,
                "ctx_age_oldest_max": age, "ctx_age_remote_mean": age,
                "ctx_age_remote_chosen": age, "ctx_age_unknown": 0,
                "ctx_age_remote_unknown": 0, "ctx_age_skewed": skewed,
                "ctx_age_skew_max_s": skew_max}

    def test_message_counts_are_summed_across_agents(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = self._run_with_metrics(root, {
                "1": self._payload(1, [], messages={"sent_msgs": 10, "recv_msgs": 8,
                                                    "sent_bytes": 100, "recv_bytes": 80,
                                                    "dropped_msgs": 1}),
                "2": self._payload(2, [], messages={"sent_msgs": 5, "recv_msgs": 4,
                                                    "sent_bytes": 50, "recv_bytes": 40,
                                                    "dropped_msgs": 0}),
            })
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertEqual(metrics["msgs_sent"], 15)
            self.assertEqual(metrics["msg_bytes_recv"], 120)
            self.assertEqual(metrics["msgs_dropped"], 1)
            self.assertEqual(metrics["agents_reporting"], 2)

    def test_context_age_is_weighted_by_decisions_not_by_agents(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            busy = [self._decision(i, 10.0, job=f"a{i}") for i in range(9)]
            quiet = [self._decision(0, 2.0, job="b0")]
            run_dir = self._run_with_metrics(root, {
                "1": self._payload(1, busy),
                "2": self._payload(2, quiet),
            })
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertEqual(metrics["delegations"], 10)
            # An unweighted mean of the two agents would be 6.0.
            self.assertAlmostEqual(metrics["ctx_age_mean"], 9.2, places=3)
            self.assertEqual(metrics["delegations_bandit"], 10)

    def test_clock_skew_is_surfaced_with_its_magnitude(self):
        """A count alone cannot separate a 1 ms artefact from a 1.1 s free-running clock, and
        that is exactly the judgement a reader has to make about the remote-age series. The
        magnitude is carried as a MAX across decisions, never summed or averaged."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = [self._decision(0, 1.0, job="a", skewed=3, skew_max=0.002),
                    self._decision(1, 1.0, job="b", skewed=1, skew_max=1.1)]
            rows[0]["ctx_age_unknown"] = 1
            run_dir = self._run_with_metrics(root, {"1": self._payload(1, rows)})
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertEqual(metrics["ctx_skewed_ages"], 4)
            self.assertEqual(metrics["ctx_unknown_groups"], 1)
            self.assertAlmostEqual(metrics["ctx_skew_max_s"], 1.1)

    def test_the_headline_and_remote_age_series_are_both_reported(self):
        """They answer different questions and only one survives an unsynchronised fleet, so
        a figure has to be able to say which it used."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run_with_metrics(Path(tmp), {
                "1": self._payload(1, [self._decision(0, 2.0)])})
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertAlmostEqual(metrics["ctx_age_mean"], 2.0)
            self.assertAlmostEqual(metrics["ctx_age_remote_mean"], 2.0)

    def test_a_run_without_metrics_json_keeps_its_other_columns(self):
        """Every archived run predating P0-4 has no metrics.json to read; the collector must
        yield those runs unchanged rather than dropping them or filling NaN columns."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = write_run(Path(tmp), "hier-30/run01", self.ROWS)
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertEqual(metrics["jobs_completed"], 1)
            self.assertNotIn("msgs_sent", metrics)
            self.assertNotIn("ctx_age_mean", metrics)

    def test_an_unreadable_metrics_file_does_not_kill_the_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = write_run(Path(tmp), "hier-30/run01", self.ROWS)
            (run_dir / "metrics.json").write_text("{not json")
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertEqual(metrics["jobs_completed"], 1)
            self.assertNotIn("msgs_sent", metrics)


class TestRegretColumns(unittest.TestCase):
    """P1-1 regret, folded into the same wide row as the context age it is plotted against."""

    ROWS = "j1,1,1,2,2,9,0,1,0.1,0.5\n"

    def _run(self, root: Path, ground_truth: dict, decisions: list) -> Path:
        import json
        run_dir = write_run(root, "hier-30/run01", self.ROWS)
        (run_dir / "run_meta.json").write_text(
            json.dumps({"started_at": 900.0, "ground_truth": ground_truth}))
        (run_dir / "metrics.json").write_text(json.dumps({
            "9": {"id": 9, "failure_sim_start": 900.0,
                  "delegation_decisions": decisions}}))
        (run_dir / "all_agents.csv").write_text(json.dumps([
            {"agent_id": 1, "group": 0, "level": 0},
            {"agent_id": 2, "group": 1, "level": 0},
        ]))
        return run_dir

    @staticmethod
    def _truth():
        return {"enabled": True, "failure_probability": 0.1,
                "per_agent_failure_rates": {"1": 0.8, "2": 0.05},
                "per_job_type_failure_rates": {}, "phases": [], "reward": {}}

    @staticmethod
    def _decision(selected, job="j", ts=1000.0, age=1.0):
        return {"ts": ts, "job_id": job, "job_type": "cpu", "policy": "bandit",
                "candidates": [0, 1], "selected": selected, "decide_s": 0.001,
                "ctx_age_mean": age, "ctx_age_chosen": age}

    def test_regret_lands_on_the_wide_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run(Path(tmp), self._truth(), [
                self._decision([0], job="a"), self._decision([1], job="b", ts=1001.0)])
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertAlmostEqual(metrics["regret_total"], 1.5)
            self.assertAlmostEqual(metrics["routing_accuracy"], 0.5)
            self.assertEqual(metrics["regret_decisions_scored"], 2)

    def test_a_run_with_no_injected_profile_gets_no_regret_columns(self):
        """Absent, not zero. A zero-filled regret column on a run that had no optimum to be
        short of reads as a perfect policy rather than as no measurement, and it would be
        averaged into `config_agg.csv` alongside runs that were really scored."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run(Path(tmp), {"enabled": False}, [self._decision([0])])
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertNotIn("regret_total", metrics)
            self.assertNotIn("routing_accuracy", metrics)

    def test_regret_against_context_age_is_correlated_on_the_same_row(self):
        """F6 is regret vs context age; carrying the correlation here means a plot of it does
        not begin by rejoining two files."""
        with tempfile.TemporaryDirectory() as tmp:
            decisions = [self._decision([0] if i % 2 else [1], job=f"j{i}",
                                        ts=1000.0 + i, age=float(i))
                         for i in range(8)]
            run_dir = self._run(Path(tmp), self._truth(), decisions)
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertIn("regret_ctx_age_corr", metrics)
            self.assertTrue(-1.0 <= metrics["regret_ctx_age_corr"] <= 1.0)


class TestBiddersPerJob(unittest.TestCase):
    """P0-8: the number designated bidding exists to move, and the failure it can hide."""

    ROWS = ("j1,1,1,2,2,9,0,1,0.1,0.5\n"
            "j2,1,1,3,3,10,0,2,0.1,0.5\n")

    def _run(self, root: Path, agents: dict) -> Path:
        import json
        run_dir = write_run(root, "hier-30/run01", self.ROWS)
        (run_dir / "metrics.json").write_text(json.dumps(agents))
        return run_dir

    @staticmethod
    def _agent(aid, bid_jobs, designate=None, bid_calls=None, claimed=None):
        # bid_calls deliberately differs from bid_jobs by default: a job re-bid after a lost
        # consensus round is not a second bidder, and the two must not be interchangeable.
        bidding = {"designate_bidder": designate is not None,
                   "bid_jobs": bid_jobs,
                   "bid_calls": bid_jobs * 2 if bid_calls is None else bid_calls}
        if designate is not None:
            mine, forced = designate
            bidding["designate_mine"], bidding["designate_forced"] = mine, forced
            # The agent reports the UNION, which is what the collector must divide by. Default
            # to a disjoint fleet; `claimed` overrides it for the retry-overlap case.
            bidding["designate_claimed"] = (claimed if claimed is not None
                                            else mine + forced)
        return {"id": aid, "instrumentation": {"llm": {"bidding": bidding}}}

    def test_bidders_per_job_is_the_fleet_sum_over_the_jobs_seen(self):
        """Undesignated: several agents each pay for a bid on a job placed once. This is the
        ~3.7 the mode exists to cut."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run(Path(tmp), {
                "1": self._agent(1, 2), "2": self._agent(2, 2), "3": self._agent(3, 1)})
            metrics = run_metrics(run_dir, expected_jobs=2)
            self.assertEqual(metrics["llm_bid_jobs"], 5)
            self.assertEqual(metrics["llm_bid_calls"], 10, "raw calls are tracked apart")
            # Distinct-jobs, not calls: re-bidding a job does not make a second bidder.
            self.assertAlmostEqual(metrics["bidders_per_job"], 2.5)  # 5 agent-jobs / 2 jobs

    def test_the_denominator_is_jobs_the_run_saw_not_the_declared_count(self):
        """A job never distributed was never available to bid on. Dividing by the declared
        count would make a stalled run look well partitioned."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run(Path(tmp), {"1": self._agent(1, 2)})
            metrics = run_metrics(run_dir, expected_jobs=5000)
            self.assertAlmostEqual(metrics["bidders_per_job"], 1.0)

    def test_a_deadline_that_fires_on_everything_is_visible_in_the_row(self):
        """The mode's failure is silent in every other artefact: completion, latency and the
        per-iteration log all look ordinary while designation buys nothing."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run(Path(tmp), {
                "1": self._agent(1, 2, designate=(0, 2)),
                "2": self._agent(2, 2, designate=(0, 2))})
            metrics = run_metrics(run_dir, expected_jobs=2)
            self.assertTrue(metrics["designate_bidder"])
            self.assertEqual(metrics["designate_forced"], 4)
            self.assertAlmostEqual(metrics["designate_forced_share"], 1.0)
            self.assertAlmostEqual(metrics["bidders_per_job"], 2.0)

    def test_a_healthy_designated_run_reports_one_bidder_per_job(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run(Path(tmp), {
                "1": self._agent(1, 1, designate=(1, 0)),
                "2": self._agent(2, 1, designate=(1, 0))})
            metrics = run_metrics(run_dir, expected_jobs=2)
            self.assertAlmostEqual(metrics["bidders_per_job"], 1.0)
            self.assertAlmostEqual(metrics["designate_forced_share"], 0.0)

    def test_a_job_that_took_both_routes_does_not_dilute_the_share(self):
        """The agent resolves the mine/forced overlap into a union; the collector must use it
        rather than adding the two back up. A job designated in one round and forced in a later
        one is one job — summing counts it twice and reports 0.5 where the truth is 1.0."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run(Path(tmp), {
                "1": self._agent(1, 2, designate=(2, 2), claimed=2)})
            metrics = run_metrics(run_dir, expected_jobs=2)
            self.assertEqual(metrics["designate_claimed"], 2)
            self.assertAlmostEqual(metrics["designate_forced_share"], 1.0)

    def test_an_analytic_run_gets_no_bidding_columns(self):
        """A run with no LLM bidding keeps exactly the row it always had."""
        with tempfile.TemporaryDirectory() as tmp:
            import json
            run_dir = write_run(Path(tmp), "hier-30/run01", self.ROWS)
            (run_dir / "metrics.json").write_text(json.dumps({"1": {"id": 1}}))
            metrics = run_metrics(run_dir, expected_jobs=2)
            self.assertNotIn("bidders_per_job", metrics)
            self.assertNotIn("designate_forced_share", metrics)


class TestDeadLlmPlaneIsFlaggedWhateverTheRunsAge(unittest.TestCase):
    """A run whose every LLM call failed must not read as an ordinary row.

    Measured on the slice 2026-09-14: the shipped config asked for `gpt-4o-mini` while the
    gateway key allowed only `gpt-oss-20b` and friends, so all 924 bids returned 403 — and the
    run completed 197/197 jobs and drained normally. It was neither an LLM-plane measurement
    nor a clean analytic baseline (a failed bid returns in ~0 s, which is the race-to-propose
    regime), yet nothing in the row said so.
    """

    ROWS = "j1,1,1,2,2,9,0,1,0.1,0.5\n"

    def _run(self, root: Path, instr: dict) -> Path:
        import json
        run_dir = write_run(root, "hier-30/run01", self.ROWS)
        (run_dir / "metrics.json").write_text(
            json.dumps({"1": {"id": 1, "instrumentation": instr}}))
        return run_dir

    def test_a_legacy_payload_with_only_the_usage_block_is_still_flagged(self):
        """The `bidding` block exists only in payloads written after P0-8. Deriving the rate
        from it alone left every earlier LLM run with two unremarkable integers and no ratio."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run(Path(tmp), {
                "llm": {"bid": {"calls": 924, "failures": 924,
                                "input_tokens": 0, "output_tokens": 0}}})
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertAlmostEqual(metrics["llm_failure_rate"], 1.0)
            self.assertTrue(metrics["llm_plane_dead"])

    def test_a_current_payload_is_flagged_from_the_bidding_block(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run(Path(tmp), {
                "llm": {"bidding": {"designate_bidder": False, "bid_jobs": 40,
                                    "bid_calls": 40, "bid_failures": 40}}})
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertAlmostEqual(metrics["llm_bid_failure_rate"], 1.0)
            self.assertTrue(metrics["llm_plane_dead"])

    def test_a_working_plane_is_explicitly_not_dead(self):
        """False, not absent: a missing column reads the same as a column nobody computed."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run(Path(tmp), {
                "llm": {"bid": {"calls": 900, "failures": 12,
                                "input_tokens": 1, "output_tokens": 1}}})
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertFalse(metrics["llm_plane_dead"])
            self.assertAlmostEqual(metrics["llm_failure_rate"], round(12 / 900, 6))

    def test_a_handful_of_early_failures_is_not_an_outage(self):
        """A run that made five bids and lost them all is noise, not an outage; a flag that
        fires on that is a flag that gets ignored."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run(Path(tmp), {
                "llm": {"bid": {"calls": 5, "failures": 5,
                                "input_tokens": 0, "output_tokens": 0}}})
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertFalse(metrics["llm_plane_dead"])

    def test_an_analytic_run_has_no_opinion_either_way(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run(Path(tmp), {"messages": {"sent_msgs": 10}})
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertNotIn("llm_plane_dead", metrics)
            self.assertNotIn("llm_failure_rate", metrics)


class TestAnAbsentFailureCounterIsUnknownNotZero(unittest.TestCase):
    """The `bidding` block shipped before `bid_failures` existed. Reading its absence as 0
    makes a run where EVERY bid failed report a 0.0 failure rate — a perfectly healthy plane.
    That is exactly the payload shape of the invalid run measured on the slice 2026-09-14; it
    was caught only because the separate `llm` usage block happened to exist alongside it.
    """

    ROWS = "j1,1,1,2,2,9,0,1,0.1,0.5\n"

    def _run(self, root: Path, instr: dict) -> Path:
        import json
        run_dir = write_run(root, "hier-30/run01", self.ROWS)
        (run_dir / "metrics.json").write_text(
            json.dumps({"1": {"id": 1, "instrumentation": instr}}))
        return run_dir

    def test_a_bidding_block_without_the_counter_is_not_reported_as_healthy(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run(Path(tmp), {          # the exact shipped intermediate shape
                "llm": {"bidding": {"designate_bidder": True, "bid_jobs": 924,
                                    "bid_calls": 924, "designate_mine": 924}}})
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertNotIn("llm_bid_failure_rate", metrics,
                             "a rate computed from a counter that does not exist is a lie")
            self.assertNotIn("llm_plane_dead", metrics)
            self.assertTrue(metrics["llm_plane_unchecked"])

    def test_checked_and_fine_is_distinguishable_from_could_not_check(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run(Path(tmp), {
                "llm": {"bidding": {"designate_bidder": True, "bid_jobs": 900,
                                    "bid_calls": 900, "bid_failures": 3}}})
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertFalse(metrics["llm_plane_unchecked"])
            self.assertFalse(metrics["llm_plane_dead"])
            self.assertAlmostEqual(metrics["llm_bid_failure_rate"], round(3 / 900, 6))

    def test_the_usage_block_alone_is_enough_to_check(self):
        """A legacy payload with no bidding block at all is still checkable, so it must not be
        lumped in with the genuinely unknowable ones."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run(Path(tmp), {
                "llm": {"bid": {"calls": 924, "failures": 924,
                                "input_tokens": 0, "output_tokens": 0}}})
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertFalse(metrics["llm_plane_unchecked"])
            self.assertTrue(metrics["llm_plane_dead"])


class TestPartialCounterCoverage(unittest.TestCase):
    """A fleet on mixed revisions must not dilute one agent's failures across everyone's calls.

    Real scenario, not hypothetical: a partial code push, or dynamic agents launched from an
    older image. `bid_calls` was summed from every agent while `bid_failures` came only from
    those that carried the counter, so with 1 of 30 agents reporting, a fleet in which EVERY
    bid fails reads as a 3.3% failure rate — comfortably healthy.
    """

    ROWS = "j1,1,1,2,2,9,0,1,0.1,0.5\n"

    def _run(self, root: Path, agents: dict) -> Path:
        import json
        run_dir = write_run(root, "hier-30/run01", self.ROWS)
        (run_dir / "metrics.json").write_text(json.dumps(agents))
        return run_dir

    @staticmethod
    def _bidding(calls, failures=None):
        block = {"designate_bidder": False, "bid_jobs": calls, "bid_calls": calls}
        if failures is not None:
            block["bid_failures"] = failures
        return {"id": 0, "instrumentation": {"llm": {"bidding": block}}}

    def test_the_rate_is_computed_only_over_agents_that_reported_failures(self):
        """Paired: one agent's failures over that agent's calls, never over the fleet's."""
        with tempfile.TemporaryDirectory() as tmp:
            agents = {"1": self._bidding(35, 35)}          # reports: all its bids failed
            for i in range(2, 31):
                agents[str(i)] = self._bidding(35)         # silent about failures
            run_dir = self._run(Path(tmp), agents)
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertAlmostEqual(metrics["llm_bid_failure_rate"], 1.0,
                                   msg="one agent's failures over one agent's calls")

    def test_a_thin_slice_does_not_condemn_the_whole_fleet(self):
        """The pairing fix, taken alone, swaps one error for its mirror image. If the single
        agent carrying the counter is the broken one — a host with a bad key — the covered
        rate is 1.0 and the WHOLE run gets classified dead while 29 of 30 agents bid fine.
        Both errors are wrong, so a verdict needs coverage behind it: the rate is still
        reported over the subset it measures, but no fleet conclusion is drawn from it."""
        with tempfile.TemporaryDirectory() as tmp:
            agents = {"1": self._bidding(35, 35)}
            for i in range(2, 31):
                agents[str(i)] = self._bidding(35)
            run_dir = self._run(Path(tmp), agents)
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertLess(metrics["llm_failure_coverage"], 0.1)
            self.assertNotIn("llm_plane_dead", metrics, "3% of the fleet cannot condemn it")
            self.assertTrue(metrics["llm_plane_unchecked"])

    def test_a_well_covered_outage_is_still_called(self):
        """Coverage must gate the verdict without suppressing it: when the counter really does
        speak for the fleet, a total outage is still a total outage."""
        with tempfile.TemporaryDirectory() as tmp:
            agents = {str(i): self._bidding(35, 35) for i in range(1, 31)}
            run_dir = self._run(Path(tmp), agents)
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertAlmostEqual(metrics["llm_failure_coverage"], 1.0)
            self.assertTrue(metrics["llm_plane_dead"])

    def test_coverage_says_how_much_of_the_fleet_the_rate_speaks_for(self):
        """Below 1.0 the rate describes a subset, so a healthy value says nothing about the
        rest — the reader has to be able to see that."""
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run(Path(tmp), {
                "1": self._bidding(100, 2), "2": self._bidding(300)})
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertAlmostEqual(metrics["llm_failure_coverage"], 0.25)
            self.assertAlmostEqual(metrics["llm_bid_failure_rate"], 0.02)

    def test_full_coverage_reports_one(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run(Path(tmp), {
                "1": self._bidding(100, 2), "2": self._bidding(300, 6)})
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertAlmostEqual(metrics["llm_failure_coverage"], 1.0)
            self.assertAlmostEqual(metrics["llm_bid_failure_rate"], 0.02)
            self.assertFalse(metrics["llm_plane_dead"])


class TestAgentCoverage(unittest.TestCase):
    """Call coverage is blind to an agent that reported nothing at all.

    Such an agent contributes to neither side of the calls ratio, so a mixed-revision fleet
    where a third of the agents carry the new blocks reads as 1.00 call coverage while two
    thirds of the bidding was never measured. `run_meta.json`'s declared agent type is what
    distinguishes "analytic by design" from "unmeasured".
    """

    ROWS = "j1,1,1,2,2,9,0,1,0.1,0.5\n"

    def _run(self, root: Path, agents: dict, meta: dict) -> Path:
        import json
        run_dir = write_run(root, "hier-30/run01", self.ROWS)
        (run_dir / "metrics.json").write_text(json.dumps(agents))
        (run_dir / "run_meta.json").write_text(json.dumps(meta))
        return run_dir

    @staticmethod
    def _instrumented(calls, failures):
        return {"id": 0, "instrumentation": {"llm": {"bid": {
            "calls": calls, "failures": failures,
            "input_tokens": 0, "output_tokens": 0}}}}

    @staticmethod
    def _bare():
        """An agent from a revision that predates the instrumentation entirely."""
        return {"id": 0, "instrumentation": {}}

    def test_unmeasured_agents_block_the_verdict_on_a_declared_llm_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            agents = {str(i): self._instrumented(35, 0) for i in range(1, 11)}
            agents.update({str(i): self._bare() for i in range(11, 31)})
            run_dir = self._run(Path(tmp), agents, {"agent_type": "llm"})
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertAlmostEqual(metrics["llm_failure_agent_coverage"], 10 / 30, places=5)
            self.assertNotIn("llm_plane_dead", metrics,
                             "two thirds of the fleet was never measured")
            self.assertTrue(metrics["llm_plane_unchecked"])

    def test_a_fully_instrumented_llm_run_still_gets_a_verdict(self):
        with tempfile.TemporaryDirectory() as tmp:
            agents = {str(i): self._instrumented(35, 0) for i in range(1, 31)}
            run_dir = self._run(Path(tmp), agents, {"agent_type": "llm"})
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertAlmostEqual(metrics["llm_failure_agent_coverage"], 1.0)
            self.assertFalse(metrics["llm_plane_dead"])

    def test_analytic_agents_in_a_resource_run_are_not_counted_as_unmeasured(self):
        """In a hierarchical run only the coordinators may be LLM agents. Without the declared
        type, every level-0 agent would look like a hole in the coverage and no LLM run of
        that shape could ever be judged."""
        with tempfile.TemporaryDirectory() as tmp:
            agents = {str(i): self._instrumented(35, 35) for i in range(1, 4)}
            agents.update({str(i): self._bare() for i in range(4, 31)})
            run_dir = self._run(Path(tmp), agents, {"agent_type": "resource"})
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertAlmostEqual(metrics["llm_failure_agent_coverage"], 1.0,
                                   msg="all three agents that used the LLM plane reported")
            self.assertTrue(metrics["llm_plane_dead"])


class TestMixedRoleRuns(unittest.TestCase):
    """A run may legitimately put the two agent types at different levels.

    `--agent-type llm --hierarchical-level1-agent-type resource` gives LLM workers under
    analytic coordinators; the reverse gives analytic workers under LLM coordinators. Treating
    every agent as LLM because the run declared `agent_type: llm` rejects the first outright —
    27 of 30 reporting is 0.90 coverage and a deliberately analytic coordinator tier looks like
    a hole in the measurement.
    """

    ROWS = "j1,1,1,2,2,9,0,1,0.1,0.5\n"

    def _run(self, root: Path, agents: dict, meta: dict, levels: list) -> Path:
        import json
        run_dir = write_run(root, "hier-30/run01", self.ROWS)
        (run_dir / "metrics.json").write_text(json.dumps(agents))
        (run_dir / "run_meta.json").write_text(json.dumps(meta))
        (run_dir / "all_agents.csv").write_text(json.dumps(levels))
        return run_dir

    @staticmethod
    def _llm_agent(calls=35, failures=0):
        return {"instrumentation": {"llm": {"bid": {
            "calls": calls, "failures": failures,
            "input_tokens": 0, "output_tokens": 0}}}}

    def _fleet(self, llm_levels: set):
        """27 workers at level 0, 3 coordinators at level 1."""
        agents, levels = {}, []
        for i in range(1, 31):
            level = 0 if i <= 27 else 1
            levels.append({"agent_id": i, "level": level})
            agents[str(i)] = self._llm_agent() if level in llm_levels else {
                "instrumentation": {}}
        return agents, levels

    def test_llm_workers_under_analytic_coordinators_are_fully_covered(self):
        agents, levels = self._fleet(llm_levels={0})
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run(Path(tmp), agents,
                                {"agent_type": "llm", "topology": "hierarchical",
                                 "hierarchical_level1_agent_type": "resource"}, levels)
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertAlmostEqual(metrics["llm_failure_agent_coverage"], 1.0,
                                   msg="the analytic coordinators are not holes")
            self.assertFalse(metrics["llm_plane_dead"])

    def test_analytic_workers_under_llm_coordinators_are_fully_covered(self):
        agents, levels = self._fleet(llm_levels={1})
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run(Path(tmp), agents,
                                {"agent_type": "resource", "topology": "hierarchical",
                                 "hierarchical_level1_agent_type": "llm"}, levels)
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertAlmostEqual(metrics["llm_failure_agent_coverage"], 1.0)

    def test_a_silent_worker_in_an_llm_tier_is_still_a_hole(self):
        """The gate must keep working for the case it was built for."""
        agents, levels = self._fleet(llm_levels={0})
        for i in range(1, 15):                      # half the LLM workers report nothing
            agents[str(i)] = {"instrumentation": {}}
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run(Path(tmp), agents,
                                {"agent_type": "llm", "topology": "hierarchical",
                                 "hierarchical_level1_agent_type": "resource"}, levels)
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertLess(metrics["llm_failure_agent_coverage"], 0.9)
            self.assertTrue(metrics["llm_plane_unchecked"])

    def test_a_run_predating_the_level1_field_falls_back_to_what_was_observed(self):
        """Guessing either way is wrong: 'llm' rejects a supported run, 'resource' hides
        unmeasured agents. Fall back to observed activity and say nothing about the rest."""
        agents, levels = self._fleet(llm_levels={0})
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = self._run(Path(tmp), agents,
                                {"agent_type": "llm", "topology": "hierarchical"}, levels)
            metrics = run_metrics(run_dir, expected_jobs=1)
            self.assertAlmostEqual(metrics["llm_failure_agent_coverage"], 1.0)
