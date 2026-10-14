# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""The drain under steady arrivals (2026-10-14).

The drain called a run done once fewer than `--threshold` (5) jobs were PENDING for
`--stable-seconds`. Under the 20 jobs/s burst the pool stayed far above 5 until the end; under a
steady Poisson rate it sits at 0-7 the whole run, so the rule could fire between arrivals, and a
pilot stopped with 3 jobs PENDING three minutes after their re-election — before the 300 s
reselection timeout could act — and recorded `drained`."""
import argparse

import pytest

import run_test


@pytest.fixture(autouse=True)
def _fresh():
    run_test._DRAIN.clear()
    run_test._PRODUCER_RC.clear()
    yield
    run_test._DRAIN.clear()
    run_test._PRODUCER_RC.clear()


class _Clock:
    def __init__(self):
        self.t = 1000.0

    def time(self):
        return self.t

    def sleep(self, s):
        self.t += max(s, 1.0)


def _run(tmp_path, monkeypatch, sizes, producing=None, **over):
    clock = _Clock()
    seq = iter(sizes)
    last = {"n": None}

    def fake_once(cmd):
        try:
            last["n"] = next(seq)
        except StopIteration:
            pass
        return "\n".join(f"state:1:0:1: {{'job:{i}'}}" for i in range(last["n"])) \
            or "No objects found in queue 'state'."

    monkeypatch.setattr(run_test, "run_once", fake_once)
    monkeypatch.setattr(run_test.time, "time", clock.time)
    monkeypatch.setattr(run_test.time, "sleep", clock.sleep)
    monkeypatch.setattr(run_test, "_wait_for_inflight", lambda a: None)
    monkeypatch.setattr(run_test, "check_all_jobs_infeasible", lambda a, b: False)
    if producing is not None:
        run_test._PRODUCER_RC["running"] = producing
    args = argparse.Namespace(runtime=0, watch_bucket=1, threshold=5, stable_seconds=90,
                              stranded_wait_s=420, check_interval=5.0, max_misses=3,
                              grace_seconds=0, run_dir=str(tmp_path), db_host="localhost",
                              mode="remote")
    for k, v in over.items():
        setattr(args, k, v)
    start = clock.t
    run_test.wait_runtime(args)
    return clock.t - start


def test_an_empty_pool_is_done_after_stable_seconds(tmp_path, monkeypatch):
    waited = _run(tmp_path, monkeypatch, [0] * 1000)
    assert run_test._DRAIN["status"] == "drained"
    assert run_test._DRAIN["pending_at_stop"] == 0
    assert 90 <= waited < 120


def test_a_few_pending_jobs_get_the_protocols_fallbacks_first(tmp_path, monkeypatch):
    waited = _run(tmp_path, monkeypatch, [3] * 1000)
    assert run_test._DRAIN["status"] == "drained"
    assert run_test._DRAIN["pending_at_stop"] == 3
    assert waited >= 420


def test_jobs_that_clear_inside_the_wait_end_it_early(tmp_path, monkeypatch):
    waited = _run(tmp_path, monkeypatch, [3] * 40 + [0] * 1000)
    assert run_test._DRAIN["pending_at_stop"] == 0
    assert waited < 420


def test_never_drained_while_jobs_are_still_arriving(tmp_path, monkeypatch):
    waited = _run(tmp_path, monkeypatch, [0] * 1000, producing=True, runtime=600)
    assert run_test._DRAIN["status"] == "cap"
    assert waited >= 600
