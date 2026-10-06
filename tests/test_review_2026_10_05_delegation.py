"""Code review 2026-10-05 §22-§23: the delegation plane before any E4/C1 run.

§22 The bandit kept a decision's context for 2 × delegation_timeout_s — sized for the reward
    being paid at scheduling time. Since P0-9 it is paid at completion, up to
    timeout + exec_grace later, so a long job's context was swept first: the model update was
    skipped while the arm's counters moved, and the per-type failure feature (C1) trained on
    short jobs only.
§23 `delegation.policy: llm` was read only by LlmAgent. On a resource coordinator — the default
    since 2026-09-18 — it ran the bandit with no warning, under the LLM arm's label.
"""
import argparse
import os
import sys
import time
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "tests"))

import run_test  # noqa: E402
from swarm.agents.llm.llm_agent import LlmAgent  # noqa: E402
from swarm.agents.resource_agent import ResourceAgent  # noqa: E402
from test_mab_manager import _FakeJob, linucb_config, make_manager  # noqa: E402


# --------------------------------------------------------------------------- §22
class TestPendingContextOutlivesTheOutcome:
    def test_ttl_covers_the_outcome_horizon(self):
        m = make_manager(linucb_config(), delegation_timeout_s=120.0, outcome_horizon_s=240.0)
        assert m._pending_ttl_s >= 240.0

    def test_long_horizon_is_not_capped_at_twice_the_timeout(self):
        """No wall-time cap (or real execution): grace 1800 s. 2 × 120 = 240 s swept it."""
        m = make_manager(linucb_config(), delegation_timeout_s=120.0, outcome_horizon_s=1920.0)
        assert m._pending_ttl_s > 1920.0

    def test_an_explicit_key_still_wins(self):
        m = make_manager(linucb_config(pending_ttl_s=5.0), delegation_timeout_s=120.0,
                         outcome_horizon_s=1920.0)
        assert m._pending_ttl_s == 5.0

    def test_a_late_outcome_still_updates_the_model(self):
        """An outcome arriving after 2 × timeout but inside the horizon keeps its context."""
        m = make_manager(linucb_config(), delegation_timeout_s=0.01, outcome_horizon_s=10.0)
        m.select_groups([1, 2], job=_FakeJob("long", job_type="compute"), top_k=1)
        time.sleep(0.05)                       # > 2 × timeout
        m.select_groups([1, 2], job=_FakeJob("next", job_type="compute"), top_k=1)  # sweeps
        assert "long" in m._pending

    def test_the_agent_passes_its_horizon(self):
        src = open(os.path.join(REPO, "swarm/agents/resource_agent.py")).read()
        assert ("outcome_horizon_s=self.delegation_timeout_s + self.delegation_exec_grace_s"
                in src)


# --------------------------------------------------------------------------- §23
def _agent(cls, policy, children):
    a = cls.__new__(cls)
    a.agent_id = 31
    a.config = {"delegation": {"policy": policy}}
    a.topology = MagicMock(children=children)
    return a


class TestAgentRefusesWhatItCannotDo:
    def test_a_resource_coordinator_refuses_llm_delegation(self):
        with pytest.raises(ValueError, match="cannot delegate by LLM"):
            _agent(ResourceAgent, "llm", [1, 2])._refuse_unsupported_delegation_policy()

    def test_an_llm_coordinator_accepts_it(self):
        _agent(LlmAgent, "llm", [1, 2])._refuse_unsupported_delegation_policy()

    def test_a_leaf_is_unaffected(self):
        """Leaves never delegate; the shared config may still carry the key."""
        _agent(ResourceAgent, "llm", [])._refuse_unsupported_delegation_policy()

    def test_bandit_is_unaffected(self):
        _agent(ResourceAgent, "bandit", [1, 2])._refuse_unsupported_delegation_policy()
        _agent(ResourceAgent, None, [1, 2])._refuse_unsupported_delegation_policy()


class TestRunnerRefusesBeforeLaunch:
    def _args(self, **over):
        base = dict(topology="hierarchical", delegation_policy="llm", use_config_dir=False,
                    hierarchical_level1_agent_type="resource")
        base.update(over)
        return argparse.Namespace(**base)

    def test_llm_policy_with_resource_coordinators_is_refused(self):
        with pytest.raises(SystemExit, match="coordinator tier is resource"):
            run_test.check_delegation_policy_is_honoured(self._args())

    def test_llm_policy_with_llm_coordinators_passes(self):
        run_test.check_delegation_policy_is_honoured(
            self._args(hierarchical_level1_agent_type="llm"))

    def test_flat_topologies_and_bandit_are_unaffected(self):
        run_test.check_delegation_policy_is_honoured(self._args(topology="mesh"))
        run_test.check_delegation_policy_is_honoured(self._args(delegation_policy="bandit"))
