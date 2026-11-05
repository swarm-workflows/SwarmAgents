"""Code review 2026-10-05 §10, §12, §45, §46: the last items."""
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "tests"))


# --------------------------------------------------------------------------- §10
def test_a_straggler_for_a_decided_job_costs_no_redis_read():
    from swarm.consensus.engine import ConsensusEngine
    from swarm.consensus.messages.commit import Commit
    from swarm.consensus.messages.prepare import Prepare
    from swarm.consensus.messages.proposal import Proposal
    from swarm.consensus.messages.proposal_info import ProposalInfo
    from swarm.models.agent_info import AgentInfo
    host = MagicMock()
    host.is_agreement_achieved.return_value = True
    eng = ConsensusEngine(1, host, MagicMock(), MagicMock(should_forward=lambda: False))
    for cls, handler in ((Proposal, eng.on_proposal), (Prepare, eng.on_prepare),
                         (Commit, eng.on_commit)):
        msg = cls(source=2, agents=[AgentInfo(agent_id=2)],
                  proposals=[ProposalInfo(p_id="p", object_id="j1", cost=1.0, agent_id=2)])
        handler(msg)
    host.get_object.assert_not_called()


def test_a_finalized_straggler_costs_no_redis_read():
    from swarm.consensus.engine import ConsensusEngine
    from swarm.consensus.messages.commit import Commit
    from swarm.consensus.messages.proposal_info import ProposalInfo
    from swarm.models.agent_info import AgentInfo
    host = MagicMock()
    host.is_agreement_achieved.return_value = False
    eng = ConsensusEngine(1, host, MagicMock(), MagicMock(should_forward=lambda: False))
    eng._note_finalized("j1", "p", 0.0)
    eng.on_commit(Commit(source=2, agents=[AgentInfo(agent_id=2)],
                         proposals=[ProposalInfo(p_id="p", object_id="j1", cost=1.0, agent_id=2)]))
    host.get_object.assert_not_called()


# --------------------------------------------------------------------------- §12
def test_snow_samples_only_deliverable_peers():
    from swarm.agents.resource_agent import _HostAdapter
    from swarm.utils.thread_safe_dict import ThreadSafeDict
    agent = MagicMock()
    agent.agent_id = 1
    agent.swim.live_agents.return_value = [1, 2, 3, 9]       # 9: SWIM-alive, heartbeat-evicted
    agent.neighbor_map = ThreadSafeDict()
    for a in (1, 2, 3):
        agent.neighbor_map.set(a, object())
    assert sorted(_HostAdapter(agent).live_peer_ids()) == [2, 3]


# --------------------------------------------------------------------------- §45
def test_infeasible_retired_jobs_count_as_seen_and_failed():
    from evaluation.collect import run_metrics
    from test_collect import HEADER
    with tempfile.TemporaryDirectory() as tmp:
        run = Path(tmp) / "mesh-3" / "run01"
        run.mkdir(parents=True)
        (run / "all_jobs.csv").write_text(HEADER + "a,1,1,2,2,3,0,1,0,1\nb,1,1,2,2,3,0,1,0,1\n")
        (run / "metrics.json").write_text(json.dumps({"1": {"infeasible_retired": ["x", "y"]},
                                                      "2": {"infeasible_retired": ["y"]}}))
        m = run_metrics(run, expected_jobs=4)
    assert m["jobs_retired_infeasible"] == 2
    assert m["jobs_failed_total"] == 2
    assert m["completion_pct_of_seen"] == pytest.approx(50.0)     # 2 of 4, not 2 of 2


def test_without_the_field_retirement_is_absent_not_zero():
    from evaluation.collect import run_metrics
    from test_collect import HEADER
    with tempfile.TemporaryDirectory() as tmp:
        run = Path(tmp) / "mesh-3" / "run01"
        run.mkdir(parents=True)
        (run / "all_jobs.csv").write_text(HEADER + "a,1,1,2,2,3,0,1,0,1\n")
        m = run_metrics(run, expected_jobs=1)
    assert "jobs_retired_infeasible" not in m


# --------------------------------------------------------------------------- §46
def test_a_decision_near_a_phase_boundary_is_unscoreable():
    from evaluation.oracle import PHASE_BOUNDARY_MARGIN_S, group_failure_rate
    from test_oracle import _truth
    truth = _truth(phases=[{"after_s": 100, "per_agent_failure_rates": {"1": {"default": 0.9}}}])
    members = {0: ["1"]}
    starts = {"1": 1000.0}
    near = group_failure_rate(0, "cpu", 1000.0 + 100 + PHASE_BOUNDARY_MARGIN_S / 2,
                              truth, members, starts)
    far = group_failure_rate(0, "cpu", 1000.0 + 100 + PHASE_BOUNDARY_MARGIN_S * 3,
                             truth, members, starts)
    assert near is None
    assert far == pytest.approx(0.9)


def test_the_collector_runs_the_profile_validation():
    src = open(os.path.join(REPO, "evaluation/collect.py")).read()
    assert "verdict = validate(run, run_dir)" in src
    assert 'out["regret_profile_validated"]' in src


# --------------------------------------------------------------------------- §46 leftovers
def test_validate_takes_one_outcome_per_job_any_success_wins(tmp_path, monkeypatch):
    import evaluation.oracle as oracle
    jobs = tmp_path / "all_jobs.csv"
    jobs.write_text("job_id,completed_at,exit_status\n"
                    "j1,10,0\n"          # the leaf copy that succeeded
                    "j1,11,1\n")         # a later fan-out copy that failed
    monkeypatch.setattr(oracle, "score_decision",
                        lambda d, run, agg: {"chosen_failure_rate": 0.1})
    run = {"decisions": [{"job_id": "j1", "job_type": "cpu"}]}
    out = oracle.validate(run, tmp_path)
    assert out["validated"] is True
    assert out["per_job_type"][0]["observed_failure_rate"] == 0.0


def test_the_redis_job_loader_reads_every_group(monkeypatch):
    import plotting.data as data

    class _R:
        store = {f"job:0:{g}:j{g}": json.dumps({"id": f"j{g}"}) for g in range(27)}
        store["job:1:0:c1"] = json.dumps({"id": "c1"})

        def __init__(self, **k):
            pass

        def scan_iter(self, match=None, count=None):
            return list(self.store)

        def mget(self, keys):
            return [self.store[k] for k in keys]
    monkeypatch.setattr(data.redis, "StrictRedis", _R)
    df = data.load_jobs_from_redis("h")
    assert len(df) == 28                                  # 27 leaf groups + a coordinator


def test_from_csv_reads_all_jobs(tmp_path):
    from plotting.data import load_jobs_from_csv
    from test_collect import HEADER
    (tmp_path / "all_jobs.csv").write_text(HEADER + "a,1,1,2,2,3,0,1,0,1\n")
    assert list(load_jobs_from_csv(str(tmp_path))["job_id"]) == ["a"]


def test_mab_plots_default_to_the_output_dirs_run_id():
    src = open(os.path.join(REPO, "plotting/mab.py")).read()
    assert 'run_id = (json.load(fh) or {}).get("run_id")' in src


# --------------------------------------------------------------------------- §56
def test_dag_gating_is_on_by_default_with_an_opt_out():
    src = open(os.path.join(REPO, "run_test.py")).read()
    assert '"--pegasus-dag-gating", action=argparse.BooleanOptionalAction, default=True,' in src
    from batch_tests_v2 import forwarded_run_flags
    import argparse
    base = dict(pegasus_jobs_dir=None, pegasus_data_nodes=None, pegasus_dtn_names=None,
                pegasus_bundle_source_root=None, textfile_dir=None, quantum_agents_pct=None,
                quantum_fraction=None, hybrid_fraction=None, job_target_agents=None,
                split_hybrid=False)
    assert forwarded_run_flags(argparse.Namespace(**base, pegasus_dag_gating=None)) == []
    assert forwarded_run_flags(argparse.Namespace(**base, pegasus_dag_gating=False)) == \
        ["--no-pegasus-dag-gating"]
