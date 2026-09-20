"""What `all_jobs.csv` means on a hierarchical run (review finding §3, 2026-09-18).

A job's timestamps are per LEVEL. The combined export used to take `submitted_at` from level 0
— which on a hierarchical run is stamped at DELEGATION, when the coordinator moves the job into
its child pool — and the leaf tier's selection stamps. So scheduling latency, job latency,
makespan and throughput all excluded the coordinator tier, which is where PBFT collapses; the
collapse was visible only in completion % and `l1_selection_*`. Now `submitted_at` is the
earliest stamp (arrival at the top tier) and `selection_total` sums consensus time over every
tier; `selection_*` stays the leaf tier's, as documented.
"""
import csv
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from plotting.data import save_jobs  # noqa: E402
from swarm.models.object import ObjectState  # noqa: E402

T0 = 1_700_000_000.0


def _hier_job(job_id="j1"):
    """Submitted at the top tier at T0; delegated 50 s later; leaf consensus took 1 s."""
    return {
        "id": job_id, "state": ObjectState.COMPLETE.value, "leader_id": 7, "exit_status": 0,
        "wall_time": 2.0,
        "submitted_at": {"1": T0, "0": T0 + 50},
        "selection_started_at": {"1": T0 + 1, "0": T0 + 51},
        "assigned_at": {"1": T0 + 3, "0": T0 + 52},
        "started_at": {"0": T0 + 53}, "completed_at": {"0": T0 + 55},
    }


def _flat_job(job_id="f1"):
    return {
        "id": job_id, "state": ObjectState.COMPLETE.value, "leader_id": 3, "exit_status": 0,
        "wall_time": 2.0,
        "submitted_at": {"0": T0}, "selection_started_at": {"0": T0 + 1},
        "assigned_at": {"0": T0 + 2}, "started_at": {"0": T0 + 3}, "completed_at": {"0": T0 + 5},
    }


def _rows(tmp_path, jobs):
    save_jobs(jobs, str(tmp_path), level=None)
    with open(tmp_path / "all_jobs.csv", newline="") as fh:
        return {r["job_id"]: r for r in csv.DictReader(fh)}


def test_submitted_at_is_the_arrival_at_the_top_tier(tmp_path):
    row = _rows(tmp_path, [_hier_job()])["j1"]
    assert float(row["submitted_at"]) == T0, "was the delegation time (T0 + 50)"
    assert float(row["scheduling_latency"]) == 52.0, "assigned at the leaf minus TOP-tier arrival"


def test_selection_columns_stay_the_leaf_tier(tmp_path):
    """Documented: level-0 is the headline selection number; per-tier files carry the rest."""
    row = _rows(tmp_path, [_hier_job()])["j1"]
    assert float(row["selection_started_at"]) == T0 + 51
    assert float(row["assigned_at"]) == T0 + 52


def test_selection_total_sums_every_tier(tmp_path):
    row = _rows(tmp_path, [_hier_job()])["j1"]
    assert float(row["selection_total"]) == 3.0, "2 s at the coordinator tier + 1 s at the leaf"


def test_a_flat_run_is_unchanged(tmp_path):
    row = _rows(tmp_path, [_flat_job()])["f1"]
    assert float(row["submitted_at"]) == T0
    assert float(row["scheduling_latency"]) == 2.0
    assert float(row["selection_total"]) == 1.0 == float(row["assigned_at"]) - float(row["selection_started_at"])


def test_a_tier_missing_one_stamp_contributes_nothing(tmp_path):
    job = _hier_job()
    job["assigned_at"] = {"0": T0 + 52}          # the coordinator tier never recorded assignment
    row = _rows(tmp_path, [job])["j1"]
    assert float(row["selection_total"]) == 1.0
