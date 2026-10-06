"""Code review 2026-10-05 §55: one seed, one workload — at every rung of a ladder.

The job generator drew from the global RNG after generate_configs had consumed it for every
agent, and modelled jobs on whatever fleet it was given, coordinators included. So "Hier-30
and Hier-270 at the same seed" scheduled different workloads, and a job sized to a
coordinator's flavour could be infeasible for every leaf.
"""
import json
import os
import random
import sys
import tempfile

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from job_generator import JobGenerator  # noqa: E402


def _profiles(n, coordinators=()):
    """Agent i has flavour i — so a ladder's rungs share agents 1..K exactly, as
    --master-fleet-size guarantees."""
    return {str(i): {"core": 2 + i % 7, "ram": 8 + i % 5, "disk": 100 + i, "gpu": 0,
                     "level": 1 if i in coordinators else 0, "dtns": []}
            for i in range(1, n + 1)}


def _jobs(profiles, seed, **kw):
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(profiles, f)
    try:
        g = JobGenerator(job_count=0, agent_profile_path=f.name, seed=seed, **kw)
        return [g.generate_job(i, enable_dtns=False) for i in range(40)]
    finally:
        os.unlink(f.name)


def test_the_stream_does_not_depend_on_prior_global_draws():
    random.seed(1)
    a = _jobs(_profiles(30), seed=7)
    random.seed(999)
    for _ in range(12345):            # generate_configs drawing for a bigger fleet
        random.random()
    b = _jobs(_profiles(30), seed=7)
    assert a == b


def test_two_rungs_with_the_same_targets_get_the_same_workload():
    small = _jobs(_profiles(30), seed=7, target_agents=30)
    large = _jobs(_profiles(270), seed=7, target_agents=30)
    assert small == large


def test_without_a_target_cap_rungs_differ_as_before():
    assert _jobs(_profiles(30), seed=7) != _jobs(_profiles(270), seed=7)


def test_coordinators_are_never_targets():
    profiles = _profiles(12, coordinators={10, 11, 12})
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(profiles, f)
    try:
        g = JobGenerator(job_count=0, agent_profile_path=f.name, seed=3)
    finally:
        os.unlink(f.name)
    assert set(g.agent_profiles) == {str(i) for i in range(1, 10)}
    assert len(g.leaf_profiles) == 9


def test_a_target_cap_that_leaves_no_executor_is_refused():
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(_profiles(5, coordinators={1, 2}), f)
    try:
        with pytest.raises(ValueError, match="no executing agent"):
            JobGenerator(job_count=0, agent_profile_path=f.name, target_agents=2)
    finally:
        os.unlink(f.name)


def test_generate_configs_records_each_agents_level():
    src = open(os.path.join(REPO, "generate_configs.py")).read()
    assert '"level": int(topo.get("level", 0) or 0),' in src


# --------------------------------------------------------------------------- §58
def test_jobs_are_published_in_job_number_order(tmp_path):
    from job_distributor import JobDistributor
    for n in (10, 2, 1, 100):
        (tmp_path / f"job_{n}.json").write_text("{}")
    (tmp_path / "conversion_summary.json").write_text("{}")
    d = JobDistributor.__new__(JobDistributor)
    d.jobs_dir = str(tmp_path)
    assert [os.path.basename(p) for p in d._job_file_generator()] == \
        ["job_1.json", "job_2.json", "job_10.json", "job_100.json"]


# --------------------------------------------------------------------------- §56
def test_the_job_interval_reaches_the_distributor(monkeypatch):
    import argparse
    import run_test
    seen = []

    class _P:
        returncode = 0
    monkeypatch.setattr(run_test, "run_blocking", lambda cmd, **k: seen.append(cmd) or _P())
    monkeypatch.setattr(run_test, "jobs_dir", lambda a: "jobs")
    run_test.produce_jobs(argparse.Namespace(jobs_per_interval=10, db_host="localhost",
                                             topology="mesh", split_hybrid=False, debug=False,
                                             job_interval=2.5))
    cmd = seen[0]
    assert cmd[cmd.index("--interval") + 1] == "2.5"


def test_the_runner_default_is_the_cadence_runs_actually_had():
    src = open(os.path.join(REPO, "run_test.py")).read()
    assert 'ap.add_argument("--job-interval", type=float, default=1.0,' in src


def test_a_batch_with_failed_runs_exits_non_zero():
    src = open(os.path.join(REPO, "batch_tests_v2.py")).read()
    assert "failed_runs.append((run_name, rc))" in src and "return 1" in src


# --------------------------------------------------------------------------- §54
class TestJobsDirUnderUseConfigDir:
    def _args(self, tmp_path, **over):
        import argparse
        base = dict(use_config_dir=True, jobs=3, pegasus_profiles=None, pegasus_jobs_dir=None)
        base.update(over)
        return argparse.Namespace(**base)

    def test_a_count_mismatch_is_refused(self, tmp_path, monkeypatch):
        import run_test
        for n in range(5):
            (tmp_path / f"job_{n}.json").write_text("{}")
        monkeypatch.setattr(run_test, "jobs_dir", lambda a: str(tmp_path))
        with pytest.raises(SystemExit, match="holds 5 job record"):
            run_test.check_jobs_dir_matches(self._args(tmp_path))

    def test_an_empty_dir_is_refused(self, tmp_path, monkeypatch):
        import run_test
        monkeypatch.setattr(run_test, "jobs_dir", lambda a: str(tmp_path / "missing"))
        with pytest.raises(SystemExit):
            run_test.check_jobs_dir_matches(self._args(tmp_path))

    def test_a_match_passes_and_generation_runs_are_unaffected(self, tmp_path, monkeypatch):
        import run_test
        for n in range(3):
            (tmp_path / f"job_{n}.json").write_text("{}")
        monkeypatch.setattr(run_test, "jobs_dir", lambda a: str(tmp_path))
        run_test.check_jobs_dir_matches(self._args(tmp_path))
        run_test.check_jobs_dir_matches(self._args(tmp_path, use_config_dir=False, jobs=99))
        run_test.check_jobs_dir_matches(self._args(tmp_path, pegasus_jobs_dir="x", jobs=99))
