"""The no-bandit delegation path hands a job to `mab.top_k` groups, not to every capable group.

Fan-out-to-all was the pre-MAB behaviour and was harmless while every coordinator led exactly one
group. When `--groups-per-coordinator` moved to 2 (2026-09-18) it became the default for every
analytic hierarchical run: the job is saved once per selected group under a group-scoped key and
each group executes its own copy — E0 and the analytic half of E1' would have run every job
twice (`FGCS_EVAL_PLAN.md` §0.10: 400 jobs, 952 leaf completions on `p11-oracle3`). A random
pick at the configured fan-out is the honest context-blind arm; the oracle scores it, where it
excludes `all`.
"""
import os
import sys
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from swarm.agents.resource_agent import ResourceAgent  # noqa: E402


def _coordinator(mab_enabled=False, top_k=1):
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = MagicMock()
    a.agent_id = 28
    a.config = {}
    a.topology = MagicMock(level=1, children=[0, 1, 2])
    a.mab_enabled = mab_enabled
    a.mab_manager = MagicMock() if mab_enabled else None
    a.mab_top_k = top_k
    a.metrics = MagicMock()
    a.metrics.mab_selections = {}
    a._init_instrumentation()
    return a


def test_without_a_bandit_one_group_gets_the_job_by_default():
    a = _coordinator()
    picks = [tuple(a._select_child_groups(MagicMock(job_id=f"j{i}"), [0, 1, 2])) for i in range(40)]
    assert all(len(p) == 1 for p in picks), picks
    assert a._decision_ctx.policy == "random"


def test_the_pick_is_random_not_the_lowest_group_id():
    """`capable_groups[:k]` would send every job of a run to group 0, and that hot spot would
    read as a placement effect."""
    a = _coordinator()
    picks = {a._select_child_groups(MagicMock(job_id=f"j{i}"), [0, 1, 2])[0] for i in range(60)}
    assert len(picks) > 1, picks


def test_mab_top_k_is_the_one_fan_out_key_for_both_paths():
    a = _coordinator(top_k=2)
    assert len(a._select_child_groups(MagicMock(job_id="j"), [0, 1, 2])) == 2


def test_a_fan_out_covering_every_candidate_is_still_all():
    """Not a decision, and recorded as such so it stays out of the routing-accuracy denominator."""
    a = _coordinator(top_k=3)
    assert a._select_child_groups(MagicMock(job_id="j"), [0, 1, 2]) == [0, 1, 2]
    assert a._decision_ctx.policy == "all"


def test_a_single_capable_group_is_all_too():
    a = _coordinator(top_k=1)
    assert a._select_child_groups(MagicMock(job_id="j"), [1]) == [1]
    assert a._decision_ctx.policy == "all"


def test_the_bandit_path_is_untouched():
    a = _coordinator(mab_enabled=True, top_k=1)
    a.mab_manager.select_groups.return_value = [2]
    assert a._select_child_groups(MagicMock(job_id="j"), [0, 1, 2]) == [2]
    assert a._decision_ctx.policy == "bandit"


def test_an_explicit_top_k_from_a_falling_back_policy_is_honoured():
    a = _coordinator(top_k=1)
    assert len(a._select_child_groups(MagicMock(job_id="j"), [0, 1, 2], top_k=2)) == 2
