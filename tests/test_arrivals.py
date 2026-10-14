# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""Job arrival patterns (2026-10-14).

Every run before this released `--jobs-per-interval` jobs every second, 20/s at every scale:
a Hier-90 fleet drains ~4.2 jobs/s, so each run was a backlog draining and queueing hid the
decision time being compared. `--arrival poisson` adds one-at-a-time Poisson arrivals at a
constant rate or a piecewise-constant schedule (a compressed work day); batch stays the
default so no earlier run changes."""
import json
import os
import statistics
from itertools import islice
from types import SimpleNamespace
from unittest import mock

import pytest

import job_distributor
from job_distributor import JobDistributor, load_rate_schedule, poisson_offsets


def _offsets(schedule, n, seed=7):
    return list(islice(poisson_offsets(schedule, seed), n))


class TestPoissonOffsets:
    def test_a_constant_rate_has_the_right_mean_gap(self):
        t = _offsets([(0.0, 4.0)], 20000)
        gaps = [b - a for a, b in zip([0.0] + t, t)]
        assert statistics.mean(gaps) == pytest.approx(0.25, rel=0.03)
        # exponential gaps: the standard deviation equals the mean
        assert statistics.pstdev(gaps) == pytest.approx(0.25, rel=0.05)

    def test_times_increase_and_repeat_with_the_seed(self):
        a, b = _offsets([(0.0, 3.0)], 500, seed=42), _offsets([(0.0, 3.0)], 500, seed=42)
        assert a == b
        assert all(x < y for x, y in zip(a, a[1:]))
        assert a != _offsets([(0.0, 3.0)], 500, seed=43)

    def test_a_schedule_sets_the_rate_per_segment(self):
        t = _offsets([(0.0, 1.0), (200.0, 10.0)], 4000)
        first = sum(1 for x in t if x < 200)
        second = sum(1 for x in t if 200 <= x < 400)
        assert first == pytest.approx(200, rel=0.2)
        assert second == pytest.approx(2000, rel=0.08)

    def test_nothing_arrives_in_a_zero_rate_segment(self):
        t = _offsets([(0.0, 5.0), (10.0, 0.0), (30.0, 5.0)], 300)
        assert not any(10.0 <= x < 30.0 for x in t)
        assert any(x < 10 for x in t) and any(x >= 30 for x in t)


class TestRateSchedule:
    def _write(self, tmp_path, payload):
        p = tmp_path / "s.json"
        p.write_text(json.dumps(payload))
        return str(p)

    def test_reads_a_list_or_a_segments_object(self, tmp_path):
        assert load_rate_schedule(self._write(tmp_path, [[0, 1], [60, 3]])) == [(0, 1), (60, 3)]
        assert load_rate_schedule(self._write(tmp_path, {"segments": [[0, 2]]})) == [(0, 2)]

    @pytest.mark.parametrize("bad", [[], [[5, 1]], [[0, 1], [0, 2]], [[0, 1], [10, 0]],
                                     [[0, -1], [10, 1]], [[0]], {"x": 1}])
    def test_refuses_a_schedule_that_cannot_mean_what_it_says(self, tmp_path, bad):
        with pytest.raises(ValueError):
            load_rate_schedule(self._write(tmp_path, bad))


class _Repo:
    def __init__(self):
        self.saved = []

    def save(self, obj, key_prefix, level, group):
        self.saved.append(obj)


def _jobs_dir(tmp_path, n):
    d = tmp_path / "jobs"
    d.mkdir()
    for i in range(1, n + 1):
        (d / f"job_{i}.json").write_text(json.dumps({"id": str(i), "wall_time": 1.0}))
    return str(d)


class TestDistributor:
    def test_poisson_submits_every_job_in_order_and_records_the_times(self, tmp_path):
        out = str(tmp_path / "arrivals.json")
        d = JobDistributor("localhost", 6379, _jobs_dir(tmp_path, 12), None, 1.0, 1, 0,
                           arrival="poisson", schedule=[(0.0, 400.0)], arrival_seed=3,
                           arrivals_out=out)
        d.job_repo = _Repo()
        d.run()
        assert [j["id"] for j in d.job_repo.saved] == [str(i) for i in range(1, 13)]
        rec = json.load(open(out))
        assert rec["arrival"] == "poisson" and rec["seed"] == 3
        planned = [j["planned_s"] for j in rec["jobs"]]
        assert planned == [round(x, 4) for x in _offsets([(0.0, 400.0)], 12, seed=3)]
        assert all(j["submitted_s"] >= j["planned_s"] - 1e-3 for j in rec["jobs"])

    def test_batch_is_unchanged_and_also_recorded(self, tmp_path):
        out = str(tmp_path / "arrivals.json")
        d = JobDistributor("localhost", 6379, _jobs_dir(tmp_path, 5), 2, 0.0, 1, 0,
                           arrivals_out=out)
        d.job_repo = _Repo()
        d.run()
        assert len(d.job_repo.saved) == 5
        rec = json.load(open(out))
        assert rec["arrival"] == "batch" and rec["jobs_per_interval"] == 2
        assert all(j["planned_s"] is None for j in rec["jobs"])

    @pytest.mark.parametrize("kw", [dict(arrival="poisson"),
                                    dict(arrival="batch", schedule=[(0.0, 1.0)]),
                                    dict(arrival="burst")])
    def test_refuses_a_mode_without_its_rate(self, tmp_path, kw):
        with pytest.raises(ValueError):
            JobDistributor("localhost", 6379, str(tmp_path), 1, 1.0, 1, 0, **kw)


class TestRunTestWiring:
    def _args(self, **kw):
        base = dict(arrival="batch", arrival_rate=None, arrival_schedule=None, arrival_seed=None,
                    seed=42, jobs_per_interval=20, job_interval=1.0, db_host="database",
                    topology="hierarchical", agents=90, run_dir="/tmp/rd", split_hybrid=False)
        base.update(kw)
        return SimpleNamespace(**base)

    def _cmd(self, args):
        import run_test
        seen = {}
        def fake_run(cmd, check):
            seen["cmd"] = cmd
            return SimpleNamespace(returncode=0)

        with mock.patch.object(run_test, "run_blocking", side_effect=fake_run), \
                mock.patch.object(run_test, "jobs_dir", return_value="/jobs"), \
                mock.patch.object(run_test, "log"):
            run_test.produce_jobs(args)
        return seen["cmd"]

    def test_batch_forwards_what_it_always_did(self):
        cmd = self._cmd(self._args())
        assert cmd[cmd.index("--jobs-per-interval") + 1] == "20"
        assert cmd[cmd.index("--interval") + 1] == "1.0"
        assert "--arrival" not in cmd
        assert cmd[cmd.index("--arrivals-out") + 1] == os.path.join("/tmp/rd", "arrivals.json")

    def test_poisson_forwards_rate_and_defaults_its_seed_to_the_run_seed(self):
        cmd = self._cmd(self._args(arrival="poisson", arrival_rate=3.0))
        assert cmd[cmd.index("--arrival") + 1] == "poisson"
        assert cmd[cmd.index("--rate") + 1] == "3.0"
        assert cmd[cmd.index("--arrival-seed") + 1] == "42"
        assert "--jobs-per-interval" not in cmd

    @pytest.mark.parametrize("kw", [dict(arrival="poisson"),
                                    dict(arrival="poisson", arrival_rate=0.0),
                                    dict(arrival="poisson", arrival_rate=2.0,
                                         arrival_schedule="x.json"),
                                    dict(arrival="batch", arrival_rate=2.0),
                                    dict(arrival="poisson", arrival_schedule="/no/such.json")])
    def test_refuses_an_arrival_it_would_not_run(self, kw):
        import run_test
        with pytest.raises(SystemExit):
            run_test.check_arrival_args(self._args(**kw))


def test_the_default_is_still_batch():
    assert job_distributor.ARRIVAL_MODES[0] == "batch"
