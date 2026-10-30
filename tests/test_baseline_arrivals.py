"""Baselines take the SWARM cell's arrivals (E7, 2026-10-30).

The baseline runners could release jobs only in fixed batches, so they could sit beside E1′'s burst
cells but not its Poisson 3 jobs/s cells. Both runners now take `run_test.py`'s arrival options and
hand them to the same `JobDistributor`; a Poisson baseline needs an explicit seed, because one
drawing different arrival times from the cell it is compared with compares two workloads.
"""
import os
import sys
import threading

import fakeredis
import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

import job_distributor  # noqa: E402
from baselines.common import arrival_kwargs  # noqa: E402
from baselines.run_baseline_remote import parse_args as central_args  # noqa: E402
from baselines.run_sparrow import parse_args as sparrow_args  # noqa: E402
from baselines.scheduler import GreedyScheduler  # noqa: E402
from job_distributor import poisson_offsets  # noqa: E402

BASE = ["--agents", "9", "--jobs", "10", "--db-host", "x", "--agent-hosts-file", "h",
        "--run-dir", "r"]


def _central(*extra):
    return central_args(["--scheduler", "greedy", *BASE, *extra])


def _sparrow(*extra):
    return sparrow_args([*BASE, "--use-profiles", "p.json", "--use-jobs-dir", "j", *extra])


@pytest.mark.parametrize("parse", [_central, _sparrow])
def test_batch_stays_the_default(parse, tmp_path):
    kw = arrival_kwargs(parse(), tmp_path)
    assert kw == {"arrival": "batch", "arrivals_out": str(tmp_path / "arrivals.json")}


@pytest.mark.parametrize("parse", [_central, _sparrow])
def test_poisson_at_a_rate_with_the_cells_seed(parse, tmp_path):
    kw = arrival_kwargs(parse("--arrival", "poisson", "--arrival-rate", "3.0",
                              "--arrival-seed", "42"), tmp_path)
    assert kw["arrival"] == "poisson" and kw["schedule"] == [(0.0, 3.0)]
    assert kw["arrival_seed"] == 42


def test_the_baseline_sees_the_swarm_cells_arrival_times(tmp_path):
    """run_test.py hands job_distributor `--rate 3.0 --arrival-seed 42` for a poisson3 cell, which
    becomes schedule [(0.0, 3.0)] (job_distributor.py); the baseline must draw the same times."""
    kw = arrival_kwargs(_central("--arrival", "poisson", "--arrival-rate", "3.0",
                                 "--arrival-seed", "42"), tmp_path)
    ours = poisson_offsets(kw["schedule"], kw["arrival_seed"])
    swarm = poisson_offsets([(0.0, 3.0)], 42)
    assert [next(ours) for _ in range(200)] == [next(swarm) for _ in range(200)]


@pytest.mark.parametrize("extra, why", [
    (["--arrival", "poisson", "--arrival-rate", "3.0"], "arrival-seed"),
    (["--arrival", "poisson", "--arrival-seed", "1"], "exactly one"),
    (["--arrival", "poisson", "--arrival-rate", "3", "--arrival-schedule", "s.json",
      "--arrival-seed", "1"], "exactly one"),
    (["--arrival", "poisson", "--arrival-rate", "0", "--arrival-seed", "1"], "positive"),
    (["--arrival", "poisson", "--arrival-rate", "inf", "--arrival-seed", "1"], "finite"),
    (["--arrival", "poisson", "--arrival-rate", "nan", "--arrival-seed", "1"], "finite"),
    (["--arrival-rate", "3.0"], "poisson only"),
    (["--arrival-seed", "42"], "poisson only"),
])
def test_a_configuration_that_would_run_something_else_is_refused(extra, why, tmp_path):
    with pytest.raises(SystemExit, match=why):
        arrival_kwargs(_central(*extra), tmp_path)


def test_a_rate_schedule_file_is_read(tmp_path):
    sched = tmp_path / "day.json"
    sched.write_text("[[0, 1.95], [300, 3.9]]")
    kw = arrival_kwargs(_sparrow("--arrival", "poisson", "--arrival-schedule", str(sched),
                                 "--arrival-seed", "7"), tmp_path)
    assert kw["schedule"] == [(0.0, 1.95), (300.0, 3.9)]


def test_the_centralized_scheduler_hands_them_to_the_distributor(tmp_path, monkeypatch):
    got = {}

    class _Distributor:
        def __init__(self, **kw):
            got.update(kw)
            self.daemon = False
            self.shutdown_flag = threading.Event()

        def start(self):
            pass

        def stop(self):
            pass

    monkeypatch.setattr(job_distributor, "JobDistributor", _Distributor)
    arrival = {"arrival": "poisson", "schedule": [(0.0, 3.0)], "arrival_seed": 42,
               "arrivals_out": str(tmp_path / "arrivals.json")}
    s = GreedyScheduler(db_host="x", db_port=0, agent_profiles_path="", jobs_dir="j",
                        jobs_per_interval=1, run_dir=str(tmp_path / "run"), total_jobs=0,
                        timeout=2.0, run_id="t", redis_client=fakeredis.FakeStrictRedis(
                            decode_responses=True), arrival=arrival)
    s.agents = []
    t = threading.Thread(target=s.run, daemon=True)
    t.start()
    t.join(10)
    assert {k: got[k] for k in arrival} == arrival
