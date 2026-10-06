"""Code review 2026-10-05 §24, §25, §30, §33, §44: config values that silently did something else.

Each of these let a run carry a label for a regime it was not in: an "LLM agent" not using
the LLM, an "epsilon-greedy" bandit labelled LinUCB, a timeout of 0.5 s that meant none, a
fan-out of 0 that dropped every job, a correct kill run reported incomplete.
"""
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

from swarm.agents.llm.llm_agent import LlmAgent  # noqa: E402
from swarm.agents.llm.llm_config import LlmConfig  # noqa: E402
from test_mab_manager import make_manager  # noqa: E402


# --------------------------------------------------------------------------- §24
def test_a_fractional_timeout_survives():
    assert LlmConfig.from_dict({"timeout_seconds": 0.5}).timeout_seconds == pytest.approx(0.5)
    assert LlmConfig.from_dict({"timeout_seconds": 2.9}).timeout_seconds == pytest.approx(2.9)
    assert LlmConfig.from_dict({}).timeout_seconds == pytest.approx(6.0)


# --------------------------------------------------------------------------- §25
class TestLlmAgentWithTheLlmSwitchedOff:
    @pytest.mark.parametrize("key", ["enabled", "use_for_selection"])
    def test_false_is_refused(self, key):
        with pytest.raises(ValueError, match=f"llm.{key}: false"):
            LlmAgent._refuse_llm_switched_off({"provider": "openai", key: False})

    def test_provider_none_is_refused_with_a_clear_message(self):
        with pytest.raises(ValueError, match="provider is 'none'"):
            LlmAgent._refuse_llm_switched_off({"provider": "none"})
        with pytest.raises(ValueError, match="provider is 'none'"):
            LlmAgent._refuse_llm_switched_off({})

    def test_the_shipped_block_passes(self):
        import yaml
        with open(os.path.join(REPO, "config_swarm_multi.yml")) as f:
            LlmAgent._refuse_llm_switched_off(yaml.safe_load(f)["llm"])

    def test_absent_keys_are_not_false(self):
        LlmAgent._refuse_llm_switched_off({"provider": "ollama"})


# --------------------------------------------------------------------------- §30
class TestBanditAlgorithmName:
    def test_an_unknown_name_is_refused(self):
        with pytest.raises(ValueError, match="not one of"):
            make_manager({"algorithm": "thompson"})

    def test_case_does_not_matter(self):
        """CLAUDE.md wrote `LinUCB`; that ran epsilon-greedy under the LinUCB label."""
        from swarm.rl.bandit import LinUCBPolicy
        m = make_manager({"algorithm": "LinUCB"})
        assert isinstance(m.policy, LinUCBPolicy)

    @pytest.mark.parametrize("name", ["epsilon_greedy", "ucb1", "linucb", "lin_ts"])
    def test_every_implemented_name_builds(self, name):
        make_manager({"algorithm": name})


# --------------------------------------------------------------------------- §33
def test_a_zero_fan_out_is_refused():
    src = open(os.path.join(REPO, "swarm/agents/resource_agent.py")).read()
    assert 'raise ValueError(f"mab.top_k must be at least 1' in src


# --------------------------------------------------------------------------- §44
def _run_with_shortfall(tmp, shortfall):
    from test_collect import HEADER
    run_dir = Path(tmp) / "mesh-3" / "run01"
    run_dir.mkdir(parents=True)
    (run_dir / "all_jobs.csv").write_text(HEADER + "j1,1,1,2,2,3,0,1,0,1\n")
    (run_dir / "metrics_shortfall.json").write_text(json.dumps(shortfall))
    return run_dir


class TestDeclaredKillRunsAreComplete:
    def test_an_accounted_kill_run_reads_complete(self):
        from evaluation.collect import run_metrics
        with tempfile.TemporaryDirectory() as tmp:
            m = run_metrics(_run_with_shortfall(
                tmp, {"missing_agents": [2, 3], "accounted": True}), expected_jobs=1)
        assert m["metrics_complete"] is True
        assert m["agents_missing_metrics"] == 2

    def test_an_unaccounted_shortfall_still_reads_incomplete(self):
        from evaluation.collect import run_metrics
        with tempfile.TemporaryDirectory() as tmp:
            m = run_metrics(_run_with_shortfall(
                tmp, {"missing_agents": [2], "accounted": False}), expected_jobs=1)
        assert m["metrics_complete"] is False

    def test_an_older_shortfall_without_the_field_reads_incomplete(self):
        from evaluation.collect import run_metrics
        with tempfile.TemporaryDirectory() as tmp:
            m = run_metrics(_run_with_shortfall(tmp, {"missing_agents": [2]}), expected_jobs=1)
        assert m["metrics_complete"] is False
