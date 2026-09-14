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
