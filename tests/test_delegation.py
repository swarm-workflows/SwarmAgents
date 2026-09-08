"""P0-1: the coordinator's own decision — which child group gets the job.

Before this, an "LLM coordinator" was a coordinator whose *bid* was LLM-scored. The choice it
actually owns — routing a job to one of its child groups — was made by the bandit, or by nobody
at all (delegate to every capable group). So the LLM was never a decision plane at the level
where the hierarchy makes decisions, which is the claim E1/E4 rest on.

What these tests hold to:

* the default config, with no `delegation` key at all, behaves exactly as before;
* an unusable answer — provider error, timeout, invented group ids, an empty ranking — falls
  back to the bandit instead of stalling the subtree or silently passing candidates through;
* a decision that cannot change the outcome (one candidate, or top_k covering them all) does
  not spend an inference;
* the candidate ids reach the JSON schema, not just the prompt, and `llm.timeout_seconds` is
  actually applied to the call.
"""
import os
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from swarm.agents.llm.llm_agent import LlmAgent  # noqa: E402
from swarm.agents.llm.llm_config import LlmConfig  # noqa: E402
from swarm.agents.llm.llm_delegator import LlmDelegator, ranking_model_for_groups  # noqa: E402
from swarm.rl.context import GroupSnapshot  # noqa: E402
from swarm.rl.mab_manager import MABManager  # noqa: E402
from swarm.utils.metrics import Metrics  # noqa: E402


class _Log:
    """A real agent always has a logger; the __new__ stub has to supply one."""

    def __getattr__(self, _name):
        return lambda *a, **k: None


class _Job:
    def __init__(self, job_id="j1", job_type="cpu_short_low"):
        self.job_id = job_id
        self.job_type = job_type

    def to_dict(self, compact=False):
        return {"id": self.job_id, "job_type": self.job_type}


class _Repo:
    def __init__(self):
        self.saved = {}

    def save(self, obj, key=None, **kwargs):
        self.saved[key] = obj

    def get(self, key=None, **kwargs):
        return None


class _Topology:
    level = 1
    group = 0


class _FakeDelegator:
    """Stands in for the provider round-trip: returns (or raises) whatever the test wants."""

    def __init__(self, ranking=None, error=None):
        self.ranking = ranking if ranking is not None else []
        self.error = error
        self.calls = 0

    def rank(self, *, job, groups):
        self.calls += 1
        if self.error:
            raise self.error
        allowed = set(int(g) for g in groups)
        seen, out = set(), []
        for g in self.ranking:
            if g in allowed and g not in seen:
                seen.add(g)
                out.append(g)
        return out, "because", 0.5


def make_agent(delegation=None, mab=None, ranking=None, error=None, with_delegator=True):
    a = LlmAgent.__new__(LlmAgent)
    cfg = {"llm": {}}
    if delegation is not None:
        cfg["delegation"] = delegation
    a.config = cfg
    a._init_llm_state()
    a.logger = _Log()
    a.agent_id = 1
    a.metrics = Metrics()
    a.repository = _Repo()
    a.topology = _Topology()
    a.shutdown = False

    a.mab_enabled = bool(mab)
    a.mab_top_k = (mab or {}).get("top_k", 1)
    a.mab_manager = None
    if mab:
        a.mab_manager = MABManager(
            agent_id=1, child_groups=mab.get("groups", [1, 2, 3]),
            config=mab.get("config", {"algorithm": "epsilon_greedy", "epsilon": 0.0}),
            repository=a.repository, logger=None,
            group_snapshot_provider=lambda: {},
        )
    if a.delegation_policy == a.DELEGATE_LLM and with_delegator:
        a.delegator = _FakeDelegator(ranking=ranking, error=error)
    return a


def _snapshots(a, groups):
    a._build_group_snapshots = lambda: {
        g: GroupSnapshot(active_children=2, cpu_headroom=0.5) for g in groups}


# --------------------------------------------------------------------------------------------
# The default is the shipped config, which mentions no delegation at all.
# --------------------------------------------------------------------------------------------

def test_absent_delegation_key_is_the_old_behaviour():
    """No `delegation:` section: every capable group, no delegator, no inference."""
    a = make_agent()
    assert a.delegation_policy == a.DELEGATE_BANDIT
    assert a.delegator is None
    assert a._select_child_groups(_Job(), [1, 2, 3]) == [1, 2, 3]


def test_default_policy_still_defers_to_the_bandit():
    a = make_agent(mab={"top_k": 1, "groups": [1, 2, 3]})
    selected = a._select_child_groups(_Job(), [1, 2, 3])
    assert len(selected) == 1 and selected[0] in (1, 2, 3)
    assert sum(a.metrics.mab_selections.values()) == 1
    assert not a.metrics.llm_delegations


def test_unknown_policy_warns_and_falls_back():
    a = make_agent(delegation={"policy": "magic"})
    assert a.delegation_policy == a.DELEGATE_BANDIT
    assert a._delegation_policy_warning and "magic" in a._delegation_policy_warning
    assert a._select_child_groups(_Job(), [1, 2]) == [1, 2]


# --------------------------------------------------------------------------------------------
# The LLM in the delegation seat.
# --------------------------------------------------------------------------------------------

def test_llm_ranking_picks_the_top_group():
    a = make_agent(delegation={"policy": "llm", "top_k": 1}, ranking=[3, 1, 2])
    _snapshots(a, [1, 2, 3])
    assert a._select_child_groups(_Job(), [1, 2, 3]) == [3]
    assert a.delegation_calls == 1
    assert a.metrics.llm_delegations == {3: 1}
    assert a.delegation_stats()["mean_s"] == 0.5


def test_llm_ranking_respects_top_k():
    a = make_agent(delegation={"policy": "llm", "top_k": 2}, ranking=[3, 1, 2])
    _snapshots(a, [1, 2, 3])
    assert a._select_child_groups(_Job(), [1, 2, 3]) == [3, 1]


def test_short_ranking_is_completed_from_the_candidates():
    """A model that names only its favourite still made a decision; top_k must be satisfiable."""
    a = make_agent(delegation={"policy": "llm", "top_k": 3}, ranking=[3])
    _snapshots(a, [1, 2, 3, 4])
    assert a._select_child_groups(_Job(), [1, 2, 3, 4]) == [3, 1, 2]
    assert a.delegation_calls == 1
    # Only group 3 was the model's choice; 1 and 2 are tail-fill. Crediting them would put
    # placements the model never made into the E2/E4 delegation counts.
    assert a.metrics.llm_delegations == {3: 1}
    assert a.delegation_stats()["filled"] == 2


def test_invented_group_ids_are_dropped():
    a = make_agent(delegation={"policy": "llm", "top_k": 1}, ranking=[99, 2])
    _snapshots(a, [1, 2, 3])
    assert a._select_child_groups(_Job(), [1, 2, 3]) == [2]


def test_delegation_record_is_persisted_for_audit():
    a = make_agent(delegation={"policy": "llm", "top_k": 1}, ranking=[2, 1, 3])
    _snapshots(a, [1, 2, 3])
    a._select_child_groups(_Job("job-7"), [1, 2, 3])
    key = [k for k in a.repository.saved if "job-7" in k]
    assert key and key[0].startswith("llm_score:delegate:A-1:")
    rec = a.repository.saved[key[0]]
    assert rec["ranking"] == [2, 1, 3] and rec["selected"] == [2]
    assert rec["candidates"] == [1, 2, 3] and rec["rationale"] == "because"
    # The audited group state is what the model was shown, not a re-read after the fact.
    assert set(rec["groups"]) == {"1", "2", "3"}


# --------------------------------------------------------------------------------------------
# Everything that can go wrong with a model in a scheduling loop.
# --------------------------------------------------------------------------------------------

def test_provider_error_falls_back_to_the_bandit():
    a = make_agent(delegation={"policy": "llm", "top_k": 1},
                   mab={"top_k": 1, "groups": [1, 2, 3]},
                   error=TimeoutError("boom"))
    _snapshots(a, [1, 2, 3])
    selected = a._select_child_groups(_Job(), [1, 2, 3])
    assert len(selected) == 1 and selected[0] in (1, 2, 3)
    assert a.delegation_fallbacks == 1 and a.delegation_calls == 0
    # The bandit made this one, and the counters must say so.
    assert sum(a.metrics.mab_selections.values()) == 1
    assert not a.metrics.llm_delegations


def test_empty_ranking_falls_back_and_is_counted_separately():
    """Structured output that names nothing we offered is a failed decision, not a pass-through."""
    a = make_agent(delegation={"policy": "llm", "top_k": 1}, ranking=[])
    _snapshots(a, [1, 2, 3])
    selected = a._select_child_groups(_Job(), [1, 2, 3])
    assert len(selected) == 1 and selected[0] in (1, 2, 3)
    assert a.delegation_empty == 1 and a.delegation_calls == 0


def test_a_failed_decision_does_not_change_the_fan_out():
    """Fan-out is a load and fairness variable. A coordinator whose model errs must hand the
    job to as many groups as one that succeeded — otherwise failing is rewarded a second way,
    with the whole subtree instead of one group, and E4's load figures move for the wrong
    reason. With no bandit to ask, the fallback picks at random rather than by list order: a
    sustained outage must not park every job on the lowest group id."""
    seen = set()
    for _ in range(40):
        a = make_agent(delegation={"policy": "llm", "top_k": 1}, error=TimeoutError("boom"))
        _snapshots(a, [1, 2, 3])
        selected = a._select_child_groups(_Job(), [1, 2, 3])
        assert len(selected) == 1, "the configured fan-out must survive the failure"
        seen.update(selected)
    assert seen == {1, 2, 3}, "a blind fallback must not always choose the same group"


def test_a_timed_out_call_is_charged_for_the_time_it_burned():
    """The expensive failure is the timeout — it spent the full `llm.timeout_seconds` before
    raising. Charging it nothing made `mean_s` cheapest in exactly the fallback-heavy runs E4
    exists to price."""
    import time as _time

    a = make_agent(delegation={"policy": "llm", "top_k": 1})
    _snapshots(a, [1, 2, 3])

    def slow_then_fail(**kwargs):
        _time.sleep(0.05)
        raise TimeoutError("deadline exceeded")

    a.delegator.rank = slow_then_fail
    a._select_child_groups(_Job(), [1, 2, 3])
    stats = a.delegation_stats()
    assert stats["fallbacks"] == 1 and stats["attempts"] == 1 and stats["calls"] == 0
    assert stats["mean_s"] >= 0.05, "a failed call must still appear in the cost of reasoning"


def test_mean_latency_counts_the_calls_that_answered_nothing():
    """An unusable answer still cost a full inference. E4 prices the reasoning, not the wins."""
    a = make_agent(delegation={"policy": "llm", "top_k": 1}, ranking=[])
    _snapshots(a, [1, 2, 3])
    a._select_child_groups(_Job(), [1, 2, 3])
    stats = a.delegation_stats()
    assert stats["calls"] == 0 and stats["empty"] == 1 and stats["attempts"] == 1
    assert stats["mean_s"] == 0.5


def test_a_delegator_that_could_not_be_built_still_reports_as_an_llm_run():
    """An unsupported `llm.provider` leaves `policy: llm` configured with nothing behind it.
    Rewriting the policy to `bandit` would make the run's own statistics claim it was a bandit
    run — the operator would have to diff configs to discover the decision plane never ran."""
    a = make_agent(delegation={"policy": "llm", "top_k": 1}, with_delegator=False)
    a.delegation_disabled_reason = "ValueError: Unsupported provider: 'none'"
    assert a.delegator is None
    assert a.delegation_policy == a.DELEGATE_LLM
    selected = a._select_child_groups(_Job(), [1, 2, 3])
    assert len(selected) == 1, "the configured fan-out still applies"
    stats = a.delegation_stats()
    assert stats["policy"] == "llm" and stats["attempts"] == 0
    assert "Unsupported provider" in stats["disabled"]


def test_no_inference_when_the_decision_cannot_matter():
    """One candidate, or a top_k that covers them all: nothing to decide, nothing to spend."""
    a = make_agent(delegation={"policy": "llm", "top_k": 1}, ranking=[2])
    _snapshots(a, [2])
    assert a._select_child_groups(_Job(), [2]) == [2]

    b = make_agent(delegation={"policy": "llm", "top_k": 3}, ranking=[3, 2, 1])
    _snapshots(b, [1, 2, 3])
    assert b._select_child_groups(_Job(), [1, 2, 3]) == [1, 2, 3]

    assert a.delegator.calls == 0 and b.delegator.calls == 0
    assert a.delegation_trivial == 1 and b.delegation_trivial == 1


def test_trivial_path_uses_the_delegation_fan_out_not_the_bandits():
    """`delegation.top_k: 3` with 3 candidates needs no inference — but it still means three
    groups. Deferring to `mab.top_k: 1` here delegated to one, so the fan-out silently came
    from the bandit's config in a run where the bandit was not choosing."""
    a = make_agent(delegation={"policy": "llm", "top_k": 3},
                   mab={"top_k": 1, "groups": [1, 2, 3]}, ranking=[3, 2, 1])
    _snapshots(a, [1, 2, 3])
    assert a._select_child_groups(_Job(), [1, 2, 3]) == [1, 2, 3]
    assert a.delegation_trivial == 1 and a.delegator.calls == 0


def test_top_k_zero_follows_mab_top_k():
    """One fan-out default, resolved in one place — `delegation.top_k: 0` means `mab.top_k`."""
    a = make_agent(delegation={"policy": "llm"},
                   mab={"top_k": 2, "groups": [1, 2, 3]}, ranking=[3, 1, 2])
    _snapshots(a, [1, 2, 3])
    assert a._delegation_top_k() == 2
    assert a._select_child_groups(_Job(), [1, 2, 3]) == [3, 1]


# --------------------------------------------------------------------------------------------
# Keeping the bandit's books honest when it is not the one choosing.
# --------------------------------------------------------------------------------------------

def test_llm_choice_is_recorded_with_the_bandit_context():
    """The delegation monitor reports outcomes to the bandit whichever policy chose. Without
    the selection-time context the contextual model silently skips the update while the arm's
    pull and reward counters keep climbing — statistics describing a run it did not steer."""
    mab_cfg = {"algorithm": "linucb", "context": {"job_types": ["cpu_short_low"]}}
    a = make_agent(delegation={"policy": "llm", "top_k": 1},
                   mab={"top_k": 1, "groups": [1, 2, 3], "config": mab_cfg},
                   ranking=[3, 1, 2])
    _snapshots(a, [1, 2, 3])
    job = _Job("job-9")
    assert a._select_child_groups(job, [1, 2, 3]) == [3]

    pending = a.mab_manager._pending.get("job-9", {})
    assert set(pending) == {3}
    assert pending[3].context is not None
    assert pending[3].job_type == "cpu_short_low"

    before = a.mab_manager.policy.b.copy()
    a.mab_manager.report_outcome(group_id=3, job_id="job-9", success=True)
    assert not (a.mab_manager.policy.b == before).all(), \
        "the contextual model must actually learn from an LLM-routed outcome"


def test_recorded_context_is_the_state_the_decision_was_made_on():
    """An LLM call takes seconds. Rebuilding the context after it would train the model on a
    world nobody chose from — and the inflight count in particular moves during the call."""
    mab_cfg = {"algorithm": "linucb", "context": {"job_types": ["cpu_short_low"]}}
    a = make_agent(delegation={"policy": "llm", "top_k": 1},
                   mab={"top_k": 1, "groups": [1, 2], "config": mab_cfg},
                   ranking=[2, 1])
    state = {1: GroupSnapshot(active_children=2, cpu_headroom=0.9, inflight=0),
             2: GroupSnapshot(active_children=2, cpu_headroom=0.9, inflight=0)}
    a.mab_manager._snapshot_provider = lambda: dict(state)

    at_decision = a.mab_manager.snapshots_for([1, 2])
    expected = a.mab_manager.extractor.build(_Job(), [1, 2], at_decision)[2]

    inner = a.delegator.rank

    def rank_then_the_world_moves(**kwargs):
        out = inner(**kwargs)
        state[2] = GroupSnapshot(active_children=2, cpu_headroom=0.1, inflight=30)
        return out

    a.delegator.rank = rank_then_the_world_moves
    assert a._select_child_groups(_Job("job-m"), [1, 2]) == [2]

    recorded = a.mab_manager._pending["job-m"][2].context
    assert (recorded == expected).all()
    # ...and the two really are distinguishable, or the assertion above proves nothing.
    stale = a.mab_manager.extractor.build(
        _Job(), [1, 2], a.mab_manager.snapshots_for([1, 2]))[2]
    assert not (stale == expected).all()


def test_snapshots_for_merges_the_managers_own_history():
    """Both policies must see the same group state, or E4 compares information, not rules."""
    mgr = MABManager(agent_id=1, child_groups=[1, 2],
                     config={"algorithm": "linucb"}, repository=_Repo(), logger=None,
                     group_snapshot_provider=lambda: {
                         1: GroupSnapshot(active_children=3, cpu_headroom=0.25)})
    mgr.report_outcome(group_id=1, job_id="x", success=False)
    snaps = mgr.snapshots_for([1, 2])
    assert snaps[1].active_children == 3 and snaps[1].cpu_headroom == 0.25
    assert snaps[1].failure_rate == 1.0
    assert snaps[2].failure_rate == 0.0


def test_group_summary_carries_the_failure_history_the_prompt_describes():
    mab_cfg = {"algorithm": "linucb", "context": {"job_types": ["cpu_short_low"]}}
    a = make_agent(delegation={"policy": "llm"},
                   mab={"top_k": 1, "groups": [1, 2], "config": mab_cfg})
    a.mab_manager._snapshot_provider = lambda: {
        1: GroupSnapshot(active_children=4, cpu_headroom=0.1, inflight=7)}
    a.mab_manager.report_outcome(group_id=1, job_id="x", success=False)
    summaries = a._group_summaries(_Job(), [1, 2])
    assert summaries[1]["children"] == 4 and summaries[1]["inflight"] == 7
    assert summaries[1]["failure_rate"] == 1.0
    # No per-type history for this job type yet, so it reads the aggregate rather than 0.
    assert summaries[1]["type_failure_rate"] == 1.0
    assert summaries[2]["failure_rate"] == 0.0


# --------------------------------------------------------------------------------------------
# The call itself: schema and timeout.
# --------------------------------------------------------------------------------------------

def test_candidate_ids_are_in_the_json_schema():
    """Ollama's NativeOutput generates from the schema; a prompt-only id list invites 0,1,2."""
    schema = ranking_model_for_groups([3, 7])
    assert schema.model_json_schema()["properties"]["ranking"]["items"]["enum"] == [3, 7]
    with pytest.raises(Exception):
        schema(ranking=[5])
    assert schema(ranking=[7, 3]).ranking == [7, 3]


class _Out:
    def __init__(self, ranking, rationale=""):
        self.ranking = ranking
        self.rationale = rationale


class _Res:
    def __init__(self, output):
        self.output = output


class _RecordingAgent:
    def __init__(self, output):
        self._output = output
        self.settings = None
        self.output_type = None

    def run_sync(self, prompt, output_type=None, model_settings=None):
        self.settings = model_settings
        self.output_type = output_type
        self.prompt = prompt
        return _Res(self._output)


def make_delegator(output, timeout_seconds=6):
    d = LlmDelegator.__new__(LlmDelegator)
    d.cfg = LlmConfig.from_dict({"provider": "openai", "timeout_seconds": timeout_seconds})
    d.logger = _Log()
    d.provider = "openai"
    d._schema_cache = {}
    d.agent = _RecordingAgent(output)
    return d


def test_rank_applies_the_configured_timeout():
    """`llm.timeout_seconds` reaches the provider call at all — the bidder's version of this
    key was parsed and never passed, and bids of 14.9s were observed under `timeout_seconds:
    6`. Note what this does NOT establish: it is a *per-request* timeout, so SDK retries and
    output-validation retries can still stack several of them into one `rank()`. There is no
    wall-clock deadline around the call yet; see the module docstring and P0-3."""
    d = make_delegator(_Out([2, 1]))
    d.rank(job={"id": "j"}, groups={1: {}, 2: {}})
    assert d.agent.settings.get("timeout") == 6.0


def test_rank_omits_the_timeout_when_it_is_disabled():
    d = make_delegator(_Out([2, 1]), timeout_seconds=0)
    d.rank(job={"id": "j"}, groups={1: {}, 2: {}})
    assert "timeout" not in d.agent.settings


def test_rank_sanitizes_the_answer():
    d = make_delegator(_Out([2, 2, 99, 1, "3"], rationale=" spacious  "))
    ranking, rationale, elapsed = d.rank(job={"id": "j"}, groups={1: {}, 2: {}, 3: {}})
    assert ranking == [2, 1, 3]          # de-duplicated, unknown dropped, "3" coerced
    assert rationale == "spacious"
    assert elapsed >= 0.0


def test_rank_survives_a_model_that_answers_with_nothing():
    d = make_delegator(_Out(None))
    assert d.rank(job={"id": "j"}, groups={1: {}, 2: {}})[0] == []


def test_response_schema_is_cached_per_candidate_set():
    d = make_delegator(_Out([1]))
    d.rank(job={"id": "j"}, groups={1: {}, 2: {}})
    first = d.agent.output_type
    d.rank(job={"id": "j"}, groups={1: {}, 2: {}})
    assert d.agent.output_type is first
    d.rank(job={"id": "j"}, groups={1: {}, 2: {}, 3: {}})
    assert d.agent.output_type is not first


# --------------------------------------------------------------------------------------------
# Can the policy ever fire? The shipped hierarchical topology says no.
# --------------------------------------------------------------------------------------------

class _WarnLog:
    def __init__(self):
        self.warnings = []

    def warning(self, msg, *args):
        self.warnings.append(msg % args if args else msg)

    def __getattr__(self, _name):
        return lambda *a, **k: None


class _Topo:
    def __init__(self, children):
        self.children = children
        self.level = 1
        self.group = 0


def _reach_agent(children, policy="llm", mab_enabled=False):
    a = LlmAgent.__new__(LlmAgent)
    a.config = {"delegation": {"policy": policy}}
    a._init_llm_state()
    a.logger = _WarnLog()
    a.topology = _Topo(children)
    a.mab_enabled = mab_enabled
    a.mab_manager = None
    return a


def test_a_coordinator_with_one_child_group_says_so():
    """The failure this guards is silent: the run completes, the numbers look plausible, and
    `policy: llm` reports zero calls because there was never a choice to make."""
    a = _reach_agent([0])
    a._warn_if_delegation_cannot_choose()
    assert len(a.logger.warnings) == 1
    w = a.logger.warnings[0]
    assert "can never choose" in w and "--co-parents" in w
    a._warn_if_delegation_cannot_choose()
    assert len(a.logger.warnings) == 1, "warn once, not once per heartbeat"


def test_the_warning_covers_the_bandit_too():
    """Not an LLM problem — one candidate leaves the bandit with one arm just the same."""
    a = _reach_agent([0], policy="bandit", mab_enabled=True)
    a._warn_if_delegation_cannot_choose()
    assert len(a.logger.warnings) == 1


def test_no_warning_when_there_is_an_actual_choice():
    a = _reach_agent([0, 4])
    a._warn_if_delegation_cannot_choose()
    assert a.logger.warnings == []


def test_no_warning_for_a_leaf_or_an_unconfigured_coordinator():
    leaf = _reach_agent(None)
    leaf._warn_if_delegation_cannot_choose()
    assert leaf.logger.warnings == []

    plain = _reach_agent([0], policy="bandit", mab_enabled=False)
    plain._warn_if_delegation_cannot_choose()
    assert plain.logger.warnings == []


def test_shipped_hierarchical_topology_gives_a_coordinator_one_child_group(tmp_path):
    """End-to-end on generated configs, because this is the fact the whole feature depends on
    and it is not visible from the agent code.

    `--co-parents` defaults to 1, so each Level-1 coordinator leads exactly one child group and
    a Level-2 super-coordinator's `children` is the single Level-1 group it manages. Every
    delegation is then trivial and neither the bandit nor the LLM ever chooses anything. Any
    campaign cell that means to measure delegation must pass `--co-parents 2` or more.
    """
    from swarm.utils.yaml_strict import safe_load

    def coordinators(out_dir, *extra):
        cmd = [sys.executable, "generate_configs.py", "30", "10", "./config_swarm_multi.yml",
               str(out_dir), "hierarchical", "localhost", "100", "--seed", "42", "--skip-jobs",
               *extra]
        proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr[-2000:]
        found = []
        for name in sorted(os.listdir(out_dir)):
            if not name.endswith(".yml"):
                continue
            cfg = safe_load(open(os.path.join(out_dir, name)))
            topo = cfg.get("topology") or {}
            if (topo.get("level") or 0) >= 1:
                found.append((cfg.get("agent_type"), topo.get("children") or []))
        return found

    default = coordinators(tmp_path / "default")
    assert default, "Hier-30 must produce coordinators at all"
    assert all(t == "llm" for t, _ in default), "Level-1 coordinators are the LLM agents"
    assert all(len(c) == 1 for _, c in default), \
        "if this ever passes, the shipped topology changed and the co-parents requirement " \
        "documented for LLM delegation should be revisited"

    shared = coordinators(tmp_path / "shared", "--co-parents", "2")
    assert all(len(c) == 2 for _, c in shared), \
        "--co-parents 2 is what gives a delegation policy something to choose between"
