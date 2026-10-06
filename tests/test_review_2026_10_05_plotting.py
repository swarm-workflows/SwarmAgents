"""Code review 2026-10-05 §42-§43 (and §39 in comparison.py): the figure code's numbers.

§42 comparison.py read a hierarchical all_jobs.csv raw: the coordinator-tier copy of every
    job, with a coordinator-only scheduling latency, entered the SWARM latency CDF; jobs were
    double-counted; coordinators counted as leaders. SWARM biased fast against the baselines.
§43 multi_run.py reported a run with no assignments as 0 s selection time (safe_mean returned
    0.0 on all-NaN) and counted READY/RUNNING jobs as successes (missing exit status exported
    as 0).
§39 comparison.py's fairness ignored idle agents, as collect.py's did.
"""
import math
import os
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "tests"))

from plotting.comparison import compute_stats  # noqa: E402
from plotting.data import load_jobs_csv  # noqa: E402
from plotting.stats import safe_mean, safe_median, safe_quantile  # noqa: E402
from test_collect import HEADER  # noqa: E402


def _csv(tmp, rows):
    p = Path(tmp) / "all_jobs.csv"
    p.write_text(HEADER + rows)
    return str(p)


# job_id,submitted_at,selection_started_at,assigned_at,started_at,completed_at,exit_status,
# leader_id,reasoning_time,scheduling_latency
COORD = "j1,100,101,102,0,0,0,30,0,2\n"          # coordinator tier: 2 s, never completes
LEAF = "j1,100,105,110,111,150,0,4,0,10\n"        # leaf tier: the job's real record


class TestComparisonDedup:
    def test_tier_copies_collapse_to_the_leaf_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            df = load_jobs_csv(_csv(tmp, COORD + LEAF))
        assert len(df) == 1
        assert int(df.iloc[0]["leader_id"]) == 4
        assert df.iloc[0]["scheduling_latency"] == 10

    def test_stats_count_jobs_once_and_ignore_coordinator_latency(self):
        with tempfile.TemporaryDirectory() as tmp:
            stats = compute_stats(load_jobs_csv(_csv(tmp, COORD + LEAF)))
        assert stats["jobs_total"] == 1
        assert stats["mean_latency"] == 10

    def test_raw_loading_is_still_available(self):
        with tempfile.TemporaryDirectory() as tmp:
            assert len(load_jobs_csv(_csv(tmp, COORD + LEAF), dedup=False)) == 2


class TestComparisonFairness:
    def test_idle_agents_count_when_the_fleet_is_known(self):
        rows = "".join(f"j{i},100,101,102,103,150,0,{1 + i % 10},0,1\n" for i in range(30))
        with tempfile.TemporaryDirectory() as tmp:
            df = load_jobs_csv(_csv(tmp, rows))
        assert compute_stats(df)["fairness_jain"] == pytest.approx(1.0)
        assert compute_stats(df, fleet_ids=list(range(1, 31)))["fairness_jain"] == \
            pytest.approx(1 / 3, abs=1e-3)


class TestNoDataIsNotZero:
    def test_safe_helpers_return_nan_on_nothing(self):
        empty = pd.Series([np.nan, np.nan])
        assert math.isnan(safe_mean(empty))
        assert math.isnan(safe_median(empty))
        assert math.isnan(safe_quantile(empty, 0.95))

    def test_values_are_unchanged(self):
        s = pd.Series([1.0, 3.0, np.nan])
        assert safe_mean(s) == 2.0 and safe_median(s) == 2.0


class TestMultiRunCompletion:
    def test_running_jobs_are_not_successes(self):
        from plotting.multi_run import MultiRunAnalyzer
        rows = ("a,100,101,102,103,150,0,1,0,1\n"     # completed, success
                "b,100,101,102,103,150,2,2,0,1\n"     # completed, failed
                "c,100,101,102,103,0,0,3,0,1\n")      # still RUNNING — exit exported as 0
        with tempfile.TemporaryDirectory() as tmp:
            run = Path(tmp) / "mesh-3-3" / "run01"
            run.mkdir(parents=True)
            (run / "all_jobs.csv").write_text(HEADER + rows)
            an = MultiRunAnalyzer(tmp, output_dir=str(Path(tmp) / "plots"))
            an.data = {"mesh-3-3": {"topology": "mesh", "agents": 3, "jobs": 3,
                                    "runs": [an.load_single_run(run)]}}
            rec = an.aggregate_metrics().iloc[0]
        assert rec["completed_jobs"] == 2
        assert rec["failed_jobs"] == 1
        assert rec["success_rate"] == pytest.approx(1 / 3)
