"""`job_selection.selection_threshold_pct` never did anything (code review §7).

`pick_agent_per_candidate` selects the column's argmin and then compared that selection against
`per_cand_best`, which is the same column's minimum — the very value it had just returned. The
test `sel_cost > best * (1 + pct/100)` therefore asked whether `best > best`: false for every
non-negative cost and every non-negative percentage. Meanwhile `config_swarm_multi.yml`, both
CLAUDE.md files and the README described it as *the* candidate-pool control, and 57 generated
config files carry it.

It is removed rather than implemented. A tolerance around the best is only meaningful for a
function that returns a pool of assignees, and this one returns one winner per column; giving
the shipped default a real effect would also have changed the bidding regime of every existing
config that carries the value. `designate_bidder` is the knob that actually decides how many
agents bid, and unlike this one it is measured.
"""
import os
import sys
from unittest.mock import MagicMock

import numpy as np
import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from swarm.selection.engine import SelectionEngine  # noqa: E402


class _Job:
    def __init__(self, jid):
        self.job_id = jid


class _Agent:
    def __init__(self, aid):
        self.agent_id = aid


def _engine():
    return SelectionEngine(
        feasible=lambda c, a: True,
        cost=lambda c, a: 0.0,
        candidate_key=lambda c: c.job_id,
        assignee_key=lambda a: a.agent_id,
    )


def _pick(costs, **kw):
    eng = _engine()
    costs = np.asarray(costs, dtype=float)
    assignees = [_Agent(i + 1) for i in range(costs.shape[0])]
    cands = [_Job(f"j{i}") for i in range(costs.shape[1])]
    return eng.pick_agent_per_candidate(
        assignees=assignees, candidates=cands, cost_matrix=costs, **kw)


def test_the_dead_parameter_is_refused_not_ignored():
    """A no-op argument is how this survived. Passing it now fails loudly."""
    with pytest.raises(TypeError):
        _pick([[1.0], [2.0]], objective="min", threshold_pct=10.0)


def test_the_best_assignee_is_still_selected():
    assert _pick([[5.0], [1.0], [9.0]], objective="min")[0][0].agent_id == 2


def test_a_wide_spread_no_longer_looks_like_a_knob_that_did_something():
    """Costs 1 and 100 are 9900% apart. Under any reading of a 10% pool the loser is out and
    the winner is in — which is exactly what the removed code produced, for every input."""
    (agent, cost), = _pick([[1.0], [100.0]], objective="min")
    assert agent.agent_id == 1 and cost == 1.0


def test_the_absolute_gate_still_works():
    """`accept_if` is the gate that was never broken: it reads the selected score without
    reference to the value it was selected by."""
    assert _pick([[5.0]], objective="min", accept_if=lambda s: s < 1.0)[0] == (None, float("inf"))
    assert _pick([[5.0]], objective="min", accept_if=lambda s: s < 10.0)[0][0].agent_id == 1


def test_the_removed_comparison_was_worse_than_inert_at_a_negative_cost():
    """Why 'inert' understates it. The formula is reproduced here because it no longer exists
    to call: `sel_cost` and `best` are the same number by construction, so the test reduces to
    `best > best * (1 + pct/100)` — false for a non-negative best, but TRUE for a negative one,
    where multiplying by 1.1 moves the bound further down. It would have discarded every
    assignment. Costs are 0-100 today so this was latent, and it is now unreachable."""
    def removed(sel_cost, best, pct):
        return sel_cost > best * (1.0 + pct / 100.0)

    assert not removed(4.0, 4.0, 10.0)      # the inert case: rejects nothing, ever
    assert removed(-4.0, -4.0, 10.0)        # the flipped case: rejects everything

    (agent, cost), = _pick([[-4.0], [-1.0]], objective="min")
    assert agent.agent_id == 1 and cost == -4.0


def test_maximise_is_unaffected_too():
    assert _pick([[1.0], [7.0], [3.0]], objective="max")[0][0].agent_id == 2


class TestTheConfigKeyIsAnnounced:
    """57 generated config files still carry the key. Silence is what let it survive.

    These call the shipped method, not a copy of it: a test that re-states the statement under
    test stops testing the code that ships the moment the two drift.
    """

    @staticmethod
    def _warn_for(job_cfg):
        from swarm.agents.resource_agent import ResourceAgent
        a = ResourceAgent.__new__(ResourceAgent)
        a.logger = MagicMock()
        a._warn_removed_job_selection_keys(job_cfg)
        return a.logger.warning.call_args_list

    def test_an_old_config_is_told(self):
        calls = self._warn_for({"selection_threshold_pct": 10.0})
        assert len(calls) == 1
        rendered = str(calls[0].args[0]) % tuple(calls[0].args[1:])
        assert "selection_threshold_pct=10.0" in rendered and "IGNORED" in rendered

    def test_the_advice_is_not_a_second_no_op(self):
        """`designate_bidder` is read only by `LlmAgent`, so answering a dead key by telling a
        resource-agent operator to set it would have replaced one silently-inert knob with
        another — and since the coordinator default became `resource` on 2026-09-18, that is
        most of a hierarchical fleet. The message must say there is no replacement, and may
        name `designate_bidder` only with its restriction attached."""
        from swarm.agents.llm.llm_agent import LlmAgent
        from swarm.agents.resource_agent import ResourceAgent

        # The premise, checked rather than asserted from memory.
        assert hasattr(LlmAgent, "_designate_bidders")
        assert not hasattr(ResourceAgent, "_designate_bidders")

        rendered = str(
            ResourceAgent._REMOVED_JOB_SELECTION_KEYS["selection_threshold_pct"])
        assert "no replacement" in rendered
        if "designate_bidder" in rendered:
            assert "LlmAgent" in rendered, "named without its restriction"

    def test_a_clean_config_is_silent(self):
        assert self._warn_for({}) == []

    def test_a_zero_is_still_a_value_that_was_set(self):
        """`0.0` is falsy; reading presence with a truth test would skip exactly the config
        that thought it had disabled the pool."""
        assert len(self._warn_for({"selection_threshold_pct": 0.0})) == 1


def test_no_call_site_still_passes_the_dead_argument():
    """The parameter now raises, so a missed call site is not a stale no-op — it is an agent
    that starts and then throws on its first selection pass. Parsed rather than grepped: the
    warning text and the comments explaining the removal both name the key, and a substring
    search cannot tell those from a live keyword argument."""
    import ast
    import pathlib

    offenders = []
    for path in pathlib.Path(REPO, "swarm").rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = getattr(node.func, "attr", getattr(node.func, "id", None))
            if name != "pick_agent_per_candidate":
                continue
            if any(kw.arg == "threshold_pct" for kw in node.keywords):
                offenders.append(f"{path}:{node.lineno}")
    assert not offenders, offenders
