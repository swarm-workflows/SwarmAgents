"""T-5: context ERROR at delegation time — what the coordinator's in-flight view got wrong.

Context age is stamped on the neighbor refresh whichever engine runs, so it cannot vary with
the consensus protocol and F6 could not be asked on it. These tests pin the replacement:

* the agent records the per-candidate view the policy decided on, taken BEFORE the policy
  runs, and leaves the key off a row that has none;
* the export says which group each delegated copy was in, from the Redis key;
* the collector reconstructs the true in-flight count per group at each decision, including
  copies withdrawn at a delegation timeout (in no export), and REFUSES to score a decision
  whose truth it cannot reconstruct rather than substituting a value.
"""
import json
import os
import sys
import tempfile
import time
from pathlib import Path

import pandas as pd
import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "tests"))

from evaluation import context_error  # noqa: E402
from evaluation.collect import decision_rows, run_metrics  # noqa: E402
from swarm.database.repository import Repository  # noqa: E402
from swarm.models.job import Job, ObjectState  # noqa: E402
from swarm.rl.context import GroupSnapshot  # noqa: E402
from swarm.utils.instrumentation import DecisionRecord, snapshot_view  # noqa: E402

from test_failed_agent_reassignment import _FakeRedis  # noqa: E402


# --------------------------------------------------------------------------- agent side

def test_the_view_records_each_candidate_with_a_snapshot_and_omits_the_rest():
    snaps = {1: GroupSnapshot(inflight=3, cpu_headroom=0.25, active_children=9),
             2: GroupSnapshot(inflight=0)}
    view = snapshot_view(snaps, [1, 2, 3])
    assert set(view) == {"1", "2"}, "a candidate with no snapshot is unknown, not idle"
    assert view["1"]["inflight"] == 3
    assert view["1"]["cpu_headroom"] == 0.25
    assert view["1"]["active_children"] == 9


def test_a_row_without_a_view_carries_no_view_key():
    rec = DecisionRecord(ts=1.0, job_id="j", job_type=None, policy="bandit",
                         n_candidates=2, candidates=[1, 2], selected=[1], decide_s=0.0)
    assert "ctx_view" not in rec.as_dict()
    rec.view = {"1": {"inflight": 2}}
    row = rec.as_dict()
    assert row["ctx_view"] == {"1": {"inflight": 2}}
    assert json.loads(json.dumps(row))["ctx_view"]["1"]["inflight"] == 2


def test_the_decision_records_the_view_before_the_policy_runs():
    from test_delegation import _Job, make_agent
    a = make_agent()
    snaps = {g: GroupSnapshot(inflight=g, active_children=3) for g in (1, 2, 3)}
    a._build_group_snapshots = lambda: snaps

    original = a._select_child_groups

    def mutating_policy(job, groups, top_k=None, snapshots=None):
        for s in (snapshots or {}).values():
            s.inflight = 99          # a policy (or the manager) writing into the objects
        return original(job, groups, top_k=top_k, snapshots=snapshots)

    a._select_child_groups = mutating_policy
    a._delegate_child_groups(_Job(), [1, 2, 3])
    view = a.decision_log.records()[0]["ctx_view"]
    assert {g: v["inflight"] for g, v in view.items()} == {"1": 1, "2": 2, "3": 3}


def test_the_snapshot_layout_the_bandit_sees_is_unchanged():
    """A changed ContextExtractor schema discards every persisted LinUCB model. T-5 reads
    the snapshot; it must not add to it."""
    from dataclasses import fields
    assert [f.name for f in fields(GroupSnapshot)] == [
        "active_children", "cpu_headroom", "ram_headroom", "gpu_headroom", "inflight",
        "failure_rate", "type_failure_rates", "timeout_rate", "observed_at",
        "oldest_observed_at", "received_at", "oldest_received_at"]


# --------------------------------------------------------------------------- export side

def test_the_export_tags_each_copy_with_the_group_its_key_names():
    repo = Repository(_FakeRedis(), run_id="t")
    redis = repo.redis
    redis.scan_iter = lambda pattern: [k for k in list(redis.kv) if k.startswith(pattern[:-1])]
    for g in (2, 5):
        j = Job(); j.job_id = "j1"; j.wall_time = 1.0; j.state = ObjectState.PENDING
        repo.save(obj=j.to_dict(), key_prefix=Repository.KEY_JOB, level=0, group=g)
    tagged = repo.get_all_objects_with_group(key_prefix=Repository.KEY_JOB, level=0)
    assert sorted(o[Repository.KEY_GROUP_TAG] for o in tagged) == [2, 5]

    from plotting.data import save_jobs
    with tempfile.TemporaryDirectory() as tmp:
        save_jobs(jobs=tagged, path=tmp, level=0)
        df = pd.read_csv(Path(tmp) / "pending_level0_jobs.csv")
        assert sorted(df["group"].tolist()) == [2, 5]


# --------------------------------------------------------------------------- collector side

L0_HEADER = ("job_id,submitted_at,selection_started_at,assigned_at,started_at,completed_at,"
             "exit_status,leader_id,reasoning_time,scheduling_latency,selection_total,refused,"
             "group\n")   # column order is irrelevant to the reader, which goes by name


def _run(tmp: Path, level0: str = "", pending: str = "", agents=None, metrics=None,
         with_group_column=True) -> Path:
    run = tmp / "hier-30" / "run01"
    run.mkdir(parents=True)
    header = L0_HEADER if with_group_column else L0_HEADER.replace(",group\n", "\n")
    (run / "level0_jobs.csv").write_text(header + level0)
    (run / "pending_level0_jobs.csv").write_text(header + pending)
    (run / "all_agents.csv").write_text(json.dumps(agents if agents is not None else [
        {"agent_id": 30, "level": 1, "group": 0},
        {"agent_id": 1, "level": 0, "group": 1},
        {"agent_id": 2, "level": 0, "group": 2},
    ]))
    (run / "all_jobs.csv").write_text(
        "job_id,submitted_at,selection_started_at,assigned_at,started_at,completed_at,"
        "exit_status,leader_id,reasoning_time,scheduling_latency\n"
        "jx,1,1,2,2,9,0,1,0.1,0.5\n")
    (run / "metrics.json").write_text(json.dumps(metrics or {}))
    return run


def _decision(ts, job, selected, seen, candidates=(1, 2), agent=30):
    return {"agent_id": str(agent), "ts": ts, "job_id": job, "policy": "bandit",
            "candidates": list(candidates), "selected": list(selected),
            "ctx_view": {str(g): {"inflight": n} for g, n in seen.items()}}


def _annotate(run, rows, agents=None):
    return context_error.annotate(rows, run, agents or {})


def test_a_stale_view_is_measured_against_the_copies_actually_in_flight():
    with tempfile.TemporaryDirectory() as tmp:
        # Group 1 has two copies; one finished at t=105, the other runs until 200.
        run = _run(Path(tmp), level0=(
            "a,100,0,101,101,105,0,1,0,0,0,0,1\n"
            "b,100,0,101,101,200,0,1,0,0,0,0,1\n"))
        rows = [_decision(100.0, "a", [1], {1: 0, 2: 0}),
                _decision(100.0, "b", [1], {1: 1, 2: 0}),
                # At t=110 the coordinator still counts both as in flight; only b is.
                _decision(110.0, "c", [2], {1: 2, 2: 0})]
        out = _annotate(run, rows)[2]
        assert out["ctx_err_scored"] == 1
        assert out["ctx_err_mean"] == pytest.approx(0.5)          # |2-1| and |0-0|
        assert out["ctx_err_signed_mean"] == pytest.approx(0.5)   # over-counted: stale
        assert out["ctx_err_max"] == pytest.approx(1.0)
        assert out["ctx_err_chosen"] == pytest.approx(0.0)        # group 2 was right


def test_the_decisions_own_job_is_not_part_of_its_truth():
    with tempfile.TemporaryDirectory() as tmp:
        run = _run(Path(tmp), level0="a,100,0,101,101,200,0,1,0,0,0,0,1\n")
        out = _annotate(run, [_decision(100.0, "a", [1], {1: 0, 2: 0})])[0]
        assert out["ctx_err_mean"] == 0.0


def test_a_withdrawn_copy_ends_at_the_re_delegation_that_followed_it():
    with tempfile.TemporaryDirectory() as tmp:
        # j was delegated to group 1 at 100, withdrawn, re-delegated to group 2 at 400; only
        # the group-2 copy survives in the export.
        run = _run(Path(tmp), level0=("j,400,0,401,401,500,0,2,0,0,0,0,2\n"
                                      "k,300,0,301,301,420,0,2,0,0,0,0,2\n"
                                      "m,450,0,451,451,460,0,1,0,0,0,0,1\n"))
        rows = [_decision(100.0, "j", [1], {1: 0, 2: 0}),
                _decision(300.0, "k", [2], {1: 1, 2: 0}),     # j still in group 1 here
                _decision(400.0, "j", [2], {1: 0, 2: 0}),
                _decision(450.0, "m", [1], {1: 0, 2: 1})]     # j now in group 2
        out = _annotate(run, rows)
        assert out[1]["ctx_err_mean"] == 0.0, "the withdrawn copy was in flight at 300"
        assert out[3]["ctx_err_mean"] == 0.0


def test_a_withdrawn_copy_ends_at_the_coordinators_reassignment_record():
    with tempfile.TemporaryDirectory() as tmp:
        run = _run(Path(tmp), level0=("k,200,0,201,201,220,0,2,0,0,0,0,2\n"
                                      "m,300,0,301,301,320,0,2,0,0,0,0,2\n"))
        agents = {"30": {"delegation_reassignments": {
            "j": {"reassigned_at": 250.0, "child_groups": [1]}}}}
        rows = [_decision(100.0, "j", [1], {1: 0, 2: 0}),
                _decision(200.0, "k", [2], {1: 1, 2: 0}),
                _decision(300.0, "m", [2], {1: 0, 2: 0})]
        out = _annotate(run, rows, agents)
        assert out[1]["ctx_err_mean"] == 0.0
        assert out[2]["ctx_err_mean"] == 0.0, "withdrawn at 250, so not in flight at 300"


def test_a_copy_with_no_knowable_end_refuses_the_decisions_it_could_affect():
    with tempfile.TemporaryDirectory() as tmp:
        run = _run(Path(tmp))
        rows = [_decision(100.0, "j", [1], {1: 0, 2: 0}),     # vanished, no record
                _decision(50.0, "early", [2], {1: 0, 2: 0}),
                _decision(200.0, "k", [2], {1: 1, 2: 0})]
        out = _annotate(run, rows)
        assert out[1]["ctx_err_scored"] == 1, "a decision before the copy existed is fine"
        assert out[2]["ctx_err_scored"] == 0
        assert out[2]["ctx_err_reason"] == "copy_with_unknown_end"
        assert "ctx_err_mean" not in out[2]


def test_an_old_export_attributes_by_leader_and_refuses_an_unpicked_copy():
    with tempfile.TemporaryDirectory() as tmp:
        run = _run(Path(tmp), with_group_column=False,
                   level0="a,100,0,101,101,200,0,1,0,0,0,0\n",
                   pending="b,100,0,0,0,0,0,,0,0,0,0\n")
        rows = [_decision(100.0, "a", [1], {1: 0, 2: 0}),
                _decision(150.0, "c", [1], {1: 1, 2: 0})]
        out = _annotate(run, rows)
        assert out[1]["ctx_err_scored"] == 0
        assert out[1]["ctx_err_reason"] == "unattributed_copies"


def test_rows_without_a_view_and_non_level1_deciders_are_refused_by_name():
    with tempfile.TemporaryDirectory() as tmp:
        run = _run(Path(tmp), agents=[{"agent_id": 30, "level": 1, "group": 0},
                                      {"agent_id": 40, "level": 2, "group": 0},
                                      {"agent_id": 1, "level": 0, "group": 1},
                                      {"agent_id": 2, "level": 0, "group": 2}])
        old = _decision(100.0, "a", [1], {})
        old.pop("ctx_view")
        top = _decision(100.0, "b", [1], {1: 0, 2: 0}, agent=40)
        out = _annotate(run, [old, top])
        assert out[0]["ctx_err_reason"] == "no_view"
        assert out[1]["ctx_err_reason"] == "not_a_level1_decision"


def test_run_level_columns_count_unscored_decisions_as_measured_zeros():
    with tempfile.TemporaryDirectory() as tmp:
        run = _run(Path(tmp), level0="a,100,0,101,101,105,0,1,0,0,0,0,1\n")
        rows = _annotate(run, [_decision(100.0, "a", [1], {1: 0, 2: 0}),
                               _decision(110.0, "b", [2], {1: 1, 2: 0})])
        cols = context_error.run_metrics(rows, {}, metrics_complete=True)
        assert cols["ctx_err_decisions_scored"] == 2
        assert cols["ctx_err_decisions_unscored"] == 0
        assert cols["ctx_err_mean"] == pytest.approx(0.25)
        assert cols["ctx_err_truth_complete"] is True
        dropped = {"30": {"instrumentation": {"delegation": {"records_dropped": 4}}}}
        assert context_error.run_metrics(rows, dropped, True)["ctx_err_truth_complete"] is False
        assert context_error.run_metrics([], {}, True) == {}


def test_regret_is_rank_correlated_with_error_on_the_same_decisions():
    annotated = [{"agent_id": "30", "job_id": f"j{i}", "ts": float(i),
                  "ctx_err_scored": 1, "ctx_err_mean": float(i)} for i in range(5)]
    regret = [{"agent_id": "30", "job_id": f"j{i}", "ts": float(i),
               "regret": float(i) ** 2, "no_choice": False} for i in range(5)]
    out = context_error.regret_correlation(regret, annotated)
    assert out["regret_ctx_err_pairs"] == 5
    assert out["regret_ctx_err_spearman"] == pytest.approx(1.0)


def test_the_collector_row_carries_the_error_columns():
    with tempfile.TemporaryDirectory() as tmp:
        decisions = [
            {"ts": 100.0, "job_id": "a", "job_type": "cpu", "policy": "bandit",
             "n_candidates": 2, "candidates": [1, 2], "selected": [1], "decide_s": 0.0,
             "ctx_view": {"1": {"inflight": 0}, "2": {"inflight": 0}}},
            {"ts": 110.0, "job_id": "b", "job_type": "cpu", "policy": "bandit",
             "n_candidates": 2, "candidates": [1, 2], "selected": [2], "decide_s": 0.0,
             "ctx_view": {"1": {"inflight": 1}, "2": {"inflight": 0}}},
        ]
        run = _run(Path(tmp), level0="a,100,0,101,101,105,0,1,0,0,0,0,1\n",
                   metrics={"30": {"id": 30, "delegation_decisions": decisions}})
        row = run_metrics(run, expected_jobs=1)
        assert row["ctx_err_decisions_scored"] == 2
        assert row["ctx_err_mean"] == pytest.approx(0.25)
        assert len(decision_rows(json.loads((run / "metrics.json").read_text()))) == 2


def test_an_untaken_fan_out_copy_ends_at_its_recorded_withdrawal():
    """Fan-out 2: group 1 picks the job up, so the coordinator deletes group 2's untaken copy.
    Without the withdrawal record that copy has no end and every later decision on group 2
    would be refused."""
    with tempfile.TemporaryDirectory() as tmp:
        run = _run(Path(tmp), level0=("j,100,0,101,101,300,0,1,0,0,0,0,1\n"
                                      "x,101.5,0,102,102,120,0,1,0,0,0,0,1\n"
                                      "k,150,0,151,151,160,0,2,0,0,0,0,2\n"))
        agents = {"30": {"delegation_withdrawals": [["j", 2, 102.0]]}}
        rows = [_decision(100.0, "j", [1, 2], {1: 0, 2: 0}),
                _decision(101.5, "x", [1], {1: 1, 2: 1}, candidates=(1, 2)),
                _decision(150.0, "k", [2], {1: 1, 2: 0})]
        out = _annotate(run, rows, agents)
        assert out[1]["ctx_err_scored"] == 1
        assert out[1]["ctx_err_mean"] == 0.0, "j's group-2 copy was still there at 101.5"
        assert out[2]["ctx_err_scored"] == 1, "withdrawn at 102, so known not in flight at 150"
        assert out[2]["ctx_err_mean"] == 0.0


def test_the_coordinator_records_each_withdrawn_copy():
    from unittest.mock import MagicMock
    from swarm.agents.resource_agent import ResourceAgent
    from swarm.utils.metrics import Metrics
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = MagicMock()
    a.metrics = Metrics()
    a.topology = MagicMock(level=1, group=0)
    a.repository = MagicMock()
    a.repository.delete_if_state.side_effect = [True, False]   # group 2 withdrawn, 3 kept
    kept = a._withdraw_child_copies("j", [2, 3])
    assert kept == [3]
    assert [(j, g) for j, g, _ in a.metrics.delegation_withdrawals] == [("j", 2)]


def test_an_upper_tier_decision_is_not_counted_as_leaf_work():
    """A level-2 coordinator selecting level-1 group 1 must not add a copy to level-0 group 1."""
    with tempfile.TemporaryDirectory() as tmp:
        run = _run(Path(tmp), agents=[{"agent_id": 30, "level": 1, "group": 0},
                                      {"agent_id": 40, "level": 2, "group": 0},
                                      {"agent_id": 1, "level": 0, "group": 1},
                                      {"agent_id": 2, "level": 0, "group": 2}])
        rows = [_decision(100.0, "top", [1], {1: 0, 2: 0}, agent=40),
                _decision(150.0, "a", [2], {1: 0, 2: 0})]
        out = _annotate(run, rows)
        assert out[1]["ctx_err_scored"] == 1, "the level-2 copy must not make group 1 unknown"
        assert out[1]["ctx_err_mean"] == 0.0


def test_a_decider_missing_from_the_agent_list_is_refused():
    with tempfile.TemporaryDirectory() as tmp:
        run = _run(Path(tmp))
        rows = [_decision(100.0, "a", [1], {1: 0, 2: 0}, agent=99),
                _decision(150.0, "b", [2], {1: 0, 2: 0})]
        out = _annotate(run, rows)
        assert out[0]["ctx_err_reason"] == "decider_tier_unknown"
        assert out[1]["ctx_err_reason"] == "unattributed_copies"
