"""Code review 2026-10-05 §34-§39, §41: the collector's numbers.

Every one of these moved a paper number in the flattering direction — a collapsed run averaged
away, a fan-out inflated, idle agents ignored, background traffic billed to consensus — and none
needed a new run to fix: re-collecting existing data is enough.
"""
import json
import math
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "tests"))

from evaluation.collect import aggregate, instrumentation_metrics, run_metrics  # noqa: E402
from test_collect import HEADER  # noqa: E402

# job_id,submitted_at,selection_started_at,assigned_at,started_at,completed_at,exit_status,
# leader_id,reasoning_time,scheduling_latency
def _row(job_id, leader, submitted=100.0, completed=200.0):
    return f"{job_id},{submitted},101,102,103,{completed},0,{leader},0,1\n"


def _pending_row(job_id, submitted=100.0):
    return f"{job_id},{submitted},0,0,0,0,,,0,0\n"


def _run(tmp, rows, pending="", metrics=None, agents=None, extra=None):
    run_dir = Path(tmp) / "mesh-30" / "run01"
    run_dir.mkdir(parents=True)
    (run_dir / "all_jobs.csv").write_text(HEADER + rows)
    (run_dir / "pending_jobs.csv").write_text(HEADER + pending)
    if metrics is not None:
        (run_dir / "metrics.json").write_text(json.dumps(metrics))
    if agents is not None:
        (run_dir / "all_agents.csv").write_text(json.dumps(agents))
    for name, text in (extra or {}).items():
        (run_dir / name).write_text(text)
    return run_dir


# --------------------------------------------------------------------------- §34
class TestChurn:
    def test_restarts_come_from_the_agents_counters(self):
        out = instrumentation_metrics({
            "1": {"restarts": {"j1": 2, "j2": 1}},
            "2": {"restarts": {"j1": 1}},
        })
        assert out["jobs_restarted"] == 2
        assert out["restarts_total"] == 4

    def test_no_restarts_is_zero_not_absent(self):
        assert instrumentation_metrics({"1": {"restarts": {}}})["jobs_restarted"] == 0

    def test_the_tier_copy_count_is_named_for_what_it_is(self):
        with tempfile.TemporaryDirectory() as tmp:
            m = run_metrics(_run(tmp, _row("j1", 1)), expected_jobs=1)
        assert m["tier_copies_per_job"] == 1.0
        assert "reselection_multiplier" not in m


# --------------------------------------------------------------------------- §35
class TestCollapsedRunsStayInTheMean:
    def test_zero_completions_is_zero_throughput(self):
        with tempfile.TemporaryDirectory() as tmp:
            m = run_metrics(_run(tmp, "", pending=_pending_row("j1")), expected_jobs=1)
        assert m["throughput_jobs_per_s"] == 0.0
        assert math.isnan(m["makespan_s"])

    def test_aggregate_reports_n_per_metric(self):
        wide = pd.DataFrame({"topology": ["mesh"] * 5,
                             "makespan_s": [10.0, 12.0, 11.0, float("nan"), float("nan")],
                             "throughput_jobs_per_s": [1.0, 1.0, 1.0, 0.0, 0.0]})
        agg = aggregate(wide, ["topology"]).iloc[0]
        assert agg["n_runs"] == 5
        assert agg["makespan_s_n"] == 3          # the survivors — now visible
        assert agg["throughput_jobs_per_s_n"] == 5
        assert agg["throughput_jobs_per_s_mean"] == pytest.approx(0.6)


# --------------------------------------------------------------------------- §36
class TestOfferedDenominators:
    def test_fan_out_divides_by_jobs_offered_not_assigned(self):
        """27 coordinators each propose 200 jobs; 2 were assigned, 198 still pending at
        teardown. Over assigned jobs this read 27 × 200 / 2 = 2700."""
        def payload():
            return {"instrumentation": {"selection": {
                "proposals_issued": 200, "jobs_proposed": 200, "reproposals": 0,
                "matrix_assignees_mean": 1.0, "matrix_assignees_max": 1.0, "level": 1}}}
        assigned = _row("a1", 1) + _row("a2", 1)
        pending = "".join(_pending_row(f"p{i}") for i in range(198))
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = _run(tmp, assigned, metrics={str(i): payload() for i in range(1, 28)},
                           extra={"level1_jobs.csv": HEADER + assigned,
                                  "pending_level1_jobs.csv": HEADER + pending})
            m = run_metrics(run_dir, expected_jobs=200)
        assert m["l1_jobs"] == 2
        assert m["l1_jobs_offered"] == 200
        assert m["proposers_per_job_l1"] == pytest.approx(27.0)

    def test_jobs_offered_counts_pending(self):
        with tempfile.TemporaryDirectory() as tmp:
            m = run_metrics(_run(tmp, _row("j1", 1), pending=_pending_row("j2")), expected_jobs=2)
        assert m["jobs_seen"] == 1
        assert m["jobs_offered"] == 2


# --------------------------------------------------------------------------- §37
class TestConsensusMessages:
    def test_only_consensus_types_are_counted(self):
        out = instrumentation_metrics({"1": {"instrumentation": {"messages": {
            "sent_msgs": 1000, "sent_bytes": 0, "recv_msgs": 0, "recv_bytes": 0,
            "dropped_msgs": 0,
            "sent_by_type": {
                "Prepare": {"msgs": 30, "bytes": 3000},
                "Commit": {"msgs": 20, "bytes": 2000},
                "SnowQueryBatch": {"msgs": 5, "bytes": 500},
                "SwimPing": {"msgs": 600, "bytes": 60},
                "GossipState": {"msgs": 300, "bytes": 30},
                "HeartBeat": {"msgs": 45, "bytes": 4},
            }}}}})
        assert out["msgs_sent"] == 1000
        assert out["msgs_consensus_sent"] == 55
        assert out["msg_bytes_consensus_sent"] == 5500

    def test_per_job_is_over_offered_jobs(self):
        payload = {"instrumentation": {"messages": {
            "sent_msgs": 40, "sent_bytes": 0, "recv_msgs": 0, "recv_bytes": 0,
            "dropped_msgs": 0, "sent_by_type": {"Prepare": {"msgs": 40, "bytes": 0}}}}}
        with tempfile.TemporaryDirectory() as tmp:
            m = run_metrics(_run(tmp, _row("j1", 1), pending=_pending_row("j2"),
                                 metrics={"1": payload}), expected_jobs=2)
        assert m["consensus_msgs_per_job"] == pytest.approx(20.0)


# --------------------------------------------------------------------------- §38
class TestFinalizeColumns:
    def test_won_is_summed_and_named_tails_are_gone(self):
        out = instrumentation_metrics({
            "1": {"instrumentation": {"consensus": {"protocol": "snow", "finalized": 5,
                                                    "won": 1, "finalize_s_p50": 0.2,
                                                    "finalize_s_p95": 0.9}}},
            "2": {"instrumentation": {"consensus": {"protocol": "snow", "finalized": 5,
                                                    "won": 4, "finalize_s_p50": 0.3,
                                                    "finalize_s_p95": 1.4}}},
        })
        assert out["consensus_finalized"] == 10
        assert out["consensus_won"] == 5
        assert "finalize_s_p95" not in out
        assert out["finalize_s_p95_worst_agent"] == pytest.approx(1.4)
        assert "finalize_s_agent_median_p50" in out

    def test_snow_counts_a_loss_as_finalized_but_not_won(self):
        from test_snow import _make_engine
        from swarm.consensus.messages.proposal_info import ProposalInfo
        eng, host, transport, cas = _make_engine(agent_id=1, peers=(2, 3))
        cas.claim("job-x", 2)                      # someone else already won
        eng.propose([ProposalInfo(p_id="p", object_id="job-x", cost=1.0, agent_id="1")])
        eng._finalize_work_inner(eng._states["job-x"], 1, "test")
        stats = eng.consensus_stats()
        assert stats["finalized"] == 1
        assert stats["won"] == 0
        assert stats["finalize_s_n"] == 0          # a loss is not in the time distribution


# --------------------------------------------------------------------------- §39
class TestFairnessOverTheFleet:
    def test_idle_agents_count(self):
        """30 level-0 agents, 10 share 30 jobs evenly: 1.0 over leaders, 0.33 over the fleet."""
        rows = "".join(_row(f"j{i}", 1 + i % 10) for i in range(30))
        agents = [{"agent_id": i, "level": 0} for i in range(1, 31)]
        with tempfile.TemporaryDirectory() as tmp:
            m = run_metrics(_run(tmp, rows, agents=agents), expected_jobs=30)
        assert m["fairness_jain_active"] == pytest.approx(1.0)
        assert m["fairness_jain"] == pytest.approx(1 / 3, abs=1e-3)
        assert m["executing_agents"] == 30
        assert m["fairness_basis"] == "level0"

    def test_coordinators_are_not_counted_as_idle(self):
        rows = _row("j1", 1) + _row("j2", 2)
        agents = [{"agent_id": 1, "level": 0}, {"agent_id": 2, "level": 0},
                  {"agent_id": 9, "level": 1}]
        with tempfile.TemporaryDirectory() as tmp:
            m = run_metrics(_run(tmp, rows, agents=agents), expected_jobs=2)
        assert m["fairness_jain"] == pytest.approx(1.0)
        assert m["executing_agents"] == 2

    def test_a_leader_missing_from_the_agent_list_still_counts(self):
        """all_agents.csv is read from Redis at teardown; an expired key drops the agent."""
        rows = _row("j1", 1) + _row("j2", 7)
        with tempfile.TemporaryDirectory() as tmp:
            m = run_metrics(_run(tmp, rows, agents=[{"agent_id": 1, "level": 0}]),
                            expected_jobs=2)
        assert m["executing_agents"] == 2


# --------------------------------------------------------------------------- §41
def test_makespan_starts_at_the_first_submission_of_any_job():
    """j0 was submitted at t=10 and never assigned; j1 ran 100→200. Makespan is 190, not 100."""
    with tempfile.TemporaryDirectory() as tmp:
        m = run_metrics(_run(tmp, _row("j1", 1, submitted=100, completed=200),
                             pending=_pending_row("j0", submitted=10)), expected_jobs=2)
    assert m["makespan_s"] == pytest.approx(190.0)
