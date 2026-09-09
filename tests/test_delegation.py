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


def test_two_problems_in_one_section_are_both_reported():
    """`policy: magic` downgrades to bandit, which in turn makes `top_k` dead config. Both are
    true and the operator needs both — a single warning slot reported only the second, so the
    louder problem (a policy name that silently is not what the config says) was the one that
    got overwritten."""
    a = make_agent(delegation={"policy": "magic", "top_k": 5})
    assert len(a._delegation_warnings) == 2
    assert any("magic" in w for w in a._delegation_warnings)
    assert any("ignored under" in w for w in a._delegation_warnings)


def test_a_clean_section_warns_about_nothing():
    assert make_agent(delegation={"policy": "llm", "top_k": 2})._delegation_warnings == []
    assert make_agent()._delegation_warnings == []


def test_unknown_policy_warns_and_falls_back():
    a = make_agent(delegation={"policy": "magic"})
    assert a.delegation_policy == a.DELEGATE_BANDIT
    assert any("magic" in w for w in a._delegation_warnings)
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
    assert a._effective_delegation_top_k() == 2
    assert a._select_child_groups(_Job(), [1, 2, 3]) == [3, 1]


def test_delegation_top_k_is_ignored_by_the_bandit_path_and_says_so():
    """`delegation.top_k` is an LLM-policy knob. Under `policy: bandit` the base implementation
    runs and never reads it, so the guard must resolve `mab.top_k` — resolving the LLM knob
    would warn about a healthy bandit run and, with the numbers the other way round, stay quiet
    about an inert one. A key that is parsed and then ignored gets said out loud."""
    a = make_agent(delegation={"policy": "bandit", "top_k": 5},
                   mab={"top_k": 1, "groups": [1, 2, 3]})
    assert a._effective_delegation_top_k() == 1
    assert any("ignored under" in w for w in a._delegation_warnings)

    # ...and the reverse: the LLM knob would have hidden a genuinely inert bandit fan-out.
    b = make_agent(delegation={"policy": "bandit", "top_k": 1},
                   mab={"top_k": 3, "groups": [1, 2, 3]})
    assert b._effective_delegation_top_k() == 3


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


def _reach_agent(children, led=None, policy="llm", mab_enabled=False, top_k=0, mab_top_k=1):
    """*children* is what the topology ASSIGNS; *led* is what this agent actively leads.
    They differ under co-parenting, which is the whole point of the guard."""
    a = LlmAgent.__new__(LlmAgent)
    a.config = {"delegation": {"policy": policy, "top_k": top_k}}
    a._init_llm_state()
    a.logger = _WarnLog()
    a.topology = _Topo(children)
    a.mab_enabled = mab_enabled
    a.mab_top_k = mab_top_k
    a.mab_manager = None
    a._get_active_child_groups = lambda: (
        list(children or []) if led is None else list(led))
    return a


def test_a_coordinator_with_one_child_group_says_so():
    """The failure this guards is silent: the run completes, the numbers look plausible, and
    `policy: llm` reports zero calls because there was never a choice to make."""
    a = _reach_agent([0])
    a._warn_if_delegation_cannot_choose()
    assert len(a.logger.warnings) == 1
    assert "can never choose" in a.logger.warnings[0]
    a._warn_if_delegation_cannot_choose()
    assert len(a.logger.warnings) == 1, "do not repeat it on every heartbeat"


def test_the_guard_judges_led_groups_not_assigned_ones():
    """Two assigned groups is not two candidates. `scheduling_main` filters through
    `_get_active_child_groups()`, and every group goes to its lowest-ID live co-parent — so at
    `--co-parents 2` the measured leadership is [0, 1, 1, 1, 2] across five coordinators. A
    check against `topology.children` calls all five healthy and misses four."""
    standby = _reach_agent([0, 4], led=[])
    standby._warn_if_delegation_cannot_choose()
    assert len(standby.logger.warnings) == 1

    one_of_two = _reach_agent([0, 1], led=[1])
    one_of_two._warn_if_delegation_cannot_choose()
    assert len(one_of_two.logger.warnings) == 1
    assert "leads 1 of its 2 assigned" in one_of_two.logger.warnings[0]

    leader = _reach_agent([0, 4], led=[0, 4])
    leader._warn_if_delegation_cannot_choose()
    assert leader.logger.warnings == []


def test_the_guard_is_not_latched_by_the_empty_startup_view():
    """Before any heartbeat arrives every co-parent believes it leads all its groups. A
    once-only check would record that optimistic first reading and never correct itself."""
    led = [0, 4]
    a = _reach_agent([0, 4], led=led)
    a._warn_if_delegation_cannot_choose()
    assert a.logger.warnings == []
    led[:] = [4]                      # heartbeats arrive; a lower-ID co-parent takes group 0
    a._warn_if_delegation_cannot_choose()
    assert len(a.logger.warnings) == 1, "the corrected view must still be reported"


def test_a_fan_out_that_covers_every_candidate_is_reported_too():
    """The same inertness reached through config instead of topology: with `top_k >= len(
    candidates)` every candidate is delegated to however the policy ranks them, so the bandit
    returns them all and the LLM path short-circuits. Two led groups look healthy until you
    notice the fan-out is also two."""
    a = _reach_agent([0, 4], led=[0, 4], top_k=2)
    a._warn_if_delegation_cannot_choose()
    assert len(a.logger.warnings) == 1
    assert "fan-out of 2 covering all 2" in a.logger.warnings[0]

    # Same shape via the bandit's own key, which is what `delegation.top_k: 0` defers to.
    b = _reach_agent([0, 4], led=[0, 4], policy="bandit", mab_enabled=True, mab_top_k=2)
    b._warn_if_delegation_cannot_choose()
    assert len(b.logger.warnings) == 1

    ok = _reach_agent([0, 4], led=[0, 4], top_k=1)
    ok._warn_if_delegation_cannot_choose()
    assert ok.logger.warnings == []


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


def _generated_coordinators(out_dir, *extra, agents=30):
    """Run the real generator and return `{agent_id: (agent_type, children, co_parent_groups)}`
    for every Level-1 coordinator."""
    from swarm.utils.yaml_strict import safe_load

    cmd = [sys.executable, "generate_configs.py", str(agents), "10", "./config_swarm_multi.yml",
           str(out_dir), "hierarchical", "localhost", "100", "--seed", "42", "--skip-jobs",
           *extra]
    proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr[-2000:]
    found = {}
    for name in sorted(os.listdir(out_dir)):
        if not name.endswith(".yml"):
            continue
        cfg = safe_load(open(os.path.join(out_dir, name)))
        topo = cfg.get("topology") or {}
        if (topo.get("level") or 0) == 1:
            agent_id = int(name.rsplit("_", 1)[1].split(".")[0])
            found[agent_id] = (cfg.get("agent_type"), topo.get("children") or [],
                               topo.get("co_parent_groups") or {})
    return found


def _led_counts(coords):
    """Groups each coordinator actively leads with the whole fleet alive, by the rule in
    `_is_leader_for_group`: the lowest-ID live co-parent leads."""
    counts = []
    for agent_id, (_type, children, cpg) in sorted(coords.items()):
        led = 0
        for g in children:
            co_parents = cpg.get(g) or cpg.get(str(g)) or []
            if not co_parents or min(co_parents) == agent_id:
                led += 1
        counts.append(led)
    return sorted(counts)


def test_shipped_topology_gives_no_coordinator_a_routing_choice(tmp_path):
    """End-to-end on generated configs: the fact the whole feature depends on, invisible from
    the agent code.

    `--co-parents` defaults to 1, so each Level-1 coordinator leads exactly one child group
    (and a Level-2 super-coordinator's `children` is the single Level-1 group it manages).
    Every delegation is then trivial and neither the bandit nor the LLM chooses anything.
    """
    coords = _generated_coordinators(tmp_path / "default")
    assert coords, "Hier-30 must produce coordinators at all"
    assert all(t == "llm" for t, _c, _m in coords.values()), \
        "Level-1 coordinators are the LLM agents"
    assert _led_counts(coords) == [1, 1, 1, 1, 1]


def test_groups_per_coordinator_gives_coordinators_a_real_choice(tmp_path):
    """The fix for the blocker in section 0.6 of the plan: one coordinator exclusively parents
    G groups, so `_get_active_child_groups()` returns G candidates and the delegation policy
    finally has something to decide. Unlike `--co-parents`, the choice is spread across
    coordinators rather than concentrated on the lowest-ID one, and no coordinator is idled.

    Coordinator slots freed by the larger fan-out become Level-0 agents, so the fleet is still
    exactly the size that was asked for — `run_test.py` launches ids 1..N and would otherwise
    start agents with no config.
    """
    for fan_out, expected_led in ((2, [1, 2, 2]), (3, [2, 3]), (5, [5])):
        coords = _generated_coordinators(
            tmp_path / f"g{fan_out}", "--groups-per-coordinator", str(fan_out))
        assert _led_counts(coords) == expected_led, f"G={fan_out}"
        assert all(t == "llm" for t, _c, _m in coords.values())
        assert sum(1 for n in _led_counts(coords) if n > 1) >= 1, \
            f"G={fan_out} must leave at least one coordinator with a routing decision"


def test_fan_out_keeps_the_fleet_the_size_it_was_asked_for(tmp_path):
    """`run_test.py` starts agent ids 1..N, so a topology that quietly drops agents would
    launch processes with no config file."""
    from swarm.utils.yaml_strict import safe_load

    for fan_out in (1, 2, 3, 5):
        out = tmp_path / f"size{fan_out}"
        extra = () if fan_out == 1 else ("--groups-per-coordinator", str(fan_out))
        _generated_coordinators(out, *extra)
        ids = sorted(int(n.rsplit("_", 1)[1].split(".")[0])
                     for n in os.listdir(out) if n.endswith(".yml"))
        assert ids == list(range(1, 31)), f"G={fan_out} produced ids {ids[:3]}..{ids[-3:]}"
        # Every Level-0 agent still belongs to a group, and sizes differ by at most one.
        sizes = {}
        for name in os.listdir(out):
            if not name.endswith(".yml"):
                continue
            topo = safe_load(open(os.path.join(out, name))).get("topology") or {}
            if (topo.get("level") or 0) == 0:
                sizes[topo["group"]] = sizes.get(topo["group"], 0) + 1
        assert max(sizes.values()) - min(sizes.values()) <= 1, f"G={fan_out}: {sizes}"


def test_fan_out_leaves_the_default_topology_untouched(tmp_path):
    """G=1 must take the original code path, not a recomputation that happens to agree — the
    scale ladder and every prior run depend on the shipped presets."""
    baseline = _generated_coordinators(tmp_path / "baseline")
    explicit = _generated_coordinators(tmp_path / "explicit", "--groups-per-coordinator", "1")
    assert baseline == explicit
    assert _led_counts(baseline) == [1, 1, 1, 1, 1]


def test_fan_out_is_refused_on_three_level_hierarchies(tmp_path):
    """A three-level hierarchy sizes its super-groups in Level-1 agents, so changing how many
    there are would need restructuring too. Refuse loudly rather than emit a broken topology."""
    out = tmp_path / "three_level"
    out.mkdir()
    proc = subprocess.run(
        [sys.executable, "generate_configs.py", "100", "10", "./config_swarm_multi.yml",
         str(out), "hierarchical", "localhost", "100", "--seed", "42", "--skip-jobs",
         "--groups-per-coordinator", "2"],
        cwd=REPO, capture_output=True, text=True)
    assert proc.returncode != 0, "a refused topology must not report success"
    assert "only supported for two-level" in proc.stdout + proc.stderr
    assert not [n for n in os.listdir(out) if n.endswith(".yml")]


def test_co_parents_concentrates_leadership_instead_of_spreading_choice(tmp_path):
    """`--co-parents 2` is NOT the fix it looks like, and this test exists to stop that claim
    coming back. Every group is led by its lowest-ID live co-parent, so with the fleet healthy
    the *assigned* groups are even while the *led* groups are not: at K=2 one coordinator of
    five has two candidates, three have one, and one is an idle standby. Raising K makes it
    worse — at K=5 a single coordinator leads all five groups and the other four do nothing,
    which would skew every load and fairness figure as well.

    Co-parenting is failover, not fan-out. Measuring delegation on this topology needs a
    coordinator that exclusively parents several groups, which the generator cannot express.
    """
    k2 = _generated_coordinators(tmp_path / "k2", "--co-parents", "2")
    assert all(len(c) == 2 for _t, c, _m in k2.values()), "K=2 assigns two groups to each"
    assert _led_counts(k2) == [0, 1, 1, 1, 2], \
        "assignment is even; leadership is not — this is why the guard checks led groups"
    assert sum(1 for n in _led_counts(k2) if n > 1) == 1

    k5 = _generated_coordinators(tmp_path / "k5", "--co-parents", "5")
    assert _led_counts(k5) == [0, 0, 0, 0, 5], \
        "raising K concentrates every group on the lowest-ID coordinator"


def test_the_plans_fleet_sizes_build_a_hierarchy(tmp_path):
    """Hier-90 and Hier-270 are named all through the evaluation plan and neither was a
    supported preset. `--agents 270` errored; `--agents 90` fell into the `<= 110` branch,
    built a 110-agent topology with coordinators at ids 101-110, and then wrote only ids 1..90
    — 90 leaf agents, zero coordinators, every `parent` dangling, no delegation at all, and
    nothing in the output saying so. The ladder scales the group count, not the group shape."""
    from swarm.utils.yaml_strict import safe_load

    for agents, expected_groups in ((30, 5), (90, 9), (270, 27)):
        out = tmp_path / f"h{agents}"
        coords = _generated_coordinators(out, agents=agents)
        assert len(coords) == expected_groups, f"Hier-{agents}"
        assert all(t == "llm" for t, _c, _m in coords.values())
        ids = sorted(int(n.rsplit("_", 1)[1].split(".")[0])
                     for n in os.listdir(out) if n.endswith(".yml"))
        assert ids == list(range(1, agents + 1)), f"Hier-{agents} must be exactly that size"
        levels = [((safe_load(open(os.path.join(out, n))).get("topology") or {}).get("level"))
                  for n in os.listdir(out) if n.endswith(".yml")]
        assert levels.count(1) == expected_groups, f"Hier-{agents} lost its coordinators"


def test_the_ladder_gives_every_coordinator_a_choice_at_g3(tmp_path):
    """The point of the exercise: at G=3 the two upper rungs put three groups under every
    coordinator, so no cell is silently measuring a policy that cannot choose."""
    for agents, expected in ((90, [3, 3, 3]), (270, [3] * 9)):
        coords = _generated_coordinators(
            tmp_path / f"g3_{agents}", "--groups-per-coordinator", "3", agents=agents)
        assert _led_counts(coords) == expected, f"Hier-{agents}"


def test_a_fleet_size_with_no_preset_is_refused_not_truncated(tmp_path):
    """The general form of the Hier-90 bug: any hierarchical size whose topology does not total
    the requested agent count used to have the overflow silently dropped."""
    out = tmp_path / "unsupported"
    out.mkdir()
    proc = subprocess.run(
        [sys.executable, "generate_configs.py", "95", "10", "./config_swarm_multi.yml",
         str(out), "hierarchical", "localhost", "100", "--seed", "42", "--skip-jobs"],
        cwd=REPO, capture_output=True, text=True)
    combined = proc.stdout + proc.stderr
    assert proc.returncode != 0, "a refused topology must not report success"
    assert "would need" in combined and "silently dropping" in combined
    assert not [n for n in os.listdir(out) if n.endswith(".yml")]


def test_every_refused_topology_exits_non_zero(tmp_path):
    """`run_test.py` and `batch_tests_v2.py` invoke the generator with `check=True`. Printing a
    refusal and returning 0 told the driver the configs were ready, so an overnight campaign
    cell would launch agents against an empty config directory — or against the previous
    cell's leftovers, quietly producing a run of the wrong shape."""
    cases = [
        (["20"], "below the hierarchical minimum"),
        (["95"], "a size with no preset"),
        (["300"], "a size off the supported list"),
        (["100", "--groups-per-coordinator", "2"], "fan-out on a three-level hierarchy"),
    ]
    for i, (extra, why) in enumerate(cases):
        out = tmp_path / f"refused{i}"
        out.mkdir()
        agents, rest = extra[0], extra[1:]
        proc = subprocess.run(
            [sys.executable, "generate_configs.py", agents, "10", "./config_swarm_multi.yml",
             str(out), "hierarchical", "localhost", "100", "--seed", "42", "--skip-jobs", *rest],
            cwd=REPO, capture_output=True, text=True)
        assert proc.returncode != 0, f"{why}: exited 0"
        assert not [n for n in os.listdir(out) if n.endswith(".yml")], f"{why}: wrote configs"


def test_a_buildable_topology_still_exits_zero(tmp_path):
    """The other half: refusing must not become the default answer."""
    for agents in ("30", "90", "270"):
        out = tmp_path / f"ok{agents}"
        proc = subprocess.run(
            [sys.executable, "generate_configs.py", agents, "10", "./config_swarm_multi.yml",
             str(out), "hierarchical", "localhost", "100", "--seed", "42", "--skip-jobs",
             "--groups-per-coordinator", "3"],
            cwd=REPO, capture_output=True, text=True)
        assert proc.returncode == 0, proc.stdout[-1500:] + proc.stderr[-1500:]
        assert len([n for n in os.listdir(out) if n.endswith(".yml")]) == int(agents)


def test_a_fan_out_larger_than_the_group_count_clamps(tmp_path):
    """Asking for more groups per coordinator than exist is not an error — one coordinator
    parents them all. It is clamped so the topology and the log line agree; before, Hier-30
    with G=99 announced "1 coordinator x 99 groups" for a fleet with five."""
    coords = _generated_coordinators(
        tmp_path / "clamped", "--groups-per-coordinator", "99")
    assert _led_counts(coords) == [5]


def test_openai_provider_honours_a_configured_base_url(monkeypatch):
    """`llm.base_url` was parsed into LlmConfig and read only by the ollama branch, so a config
    naming an OpenAI-compatible gateway silently talked to api.openai.com. That is live for this
    campaign: the FABRIC endpoint at https://ai.fabric-testbed.net/v1 is the only one routable
    from the slice, and a run pointed at it by config alone would have gone somewhere else."""
    from swarm.agents.llm.llm_bidder import build_model

    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    gateway = build_model(LlmConfig.from_dict(
        {"provider": "openai", "model": "gpt-oss-20b",
         "base_url": "https://ai.fabric-testbed.net/v1"}))
    assert "ai.fabric-testbed.net" in str(gateway.client.base_url)

    stock = build_model(LlmConfig.from_dict({"provider": "openai", "model": "gpt-4o-mini"}))
    assert "api.openai.com" in str(stock.client.base_url), "default must be unchanged"

    monkeypatch.setenv("OPENAI_BASE_URL", "https://env-wins.example/v1")
    env = build_model(LlmConfig.from_dict(
        {"provider": "openai", "model": "m", "base_url": "https://config.example/v1"}))
    assert "env-wins.example" in str(env.client.base_url), "env must win over config"
