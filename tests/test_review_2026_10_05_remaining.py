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
