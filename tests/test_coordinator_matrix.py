# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""The coordinator-tier cost matrix, and the measurement that makes it separable.

`selection_main` built the level>0 cost matrix over `[self]` alone behind a `# TEMP HACK`,
so every coordinator holding a job proposed itself for it and the coordinator tier ran one
consensus decision per coordinator per job. That is a second explanation for the Hier-250
PBFT collapse besides PBFT's own O(n^2) cost, and a messages-per-job curve cannot tell the
two apart.

These tests pin both halves of the fix agreed for it:

* the behaviour is now a named, validated config key (`job_selection.coordinator_cost_matrix`)
  whose default reproduces the shipped behaviour exactly, with `peers` as the E5 arm;
* proposals are counted per tier, as DISTINCT JOBS, so `proposers_per_job_l1` says which
  regime a run was in instead of a reader inferring it from the config.
"""
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from swarm.agents.resource_agent import (  # noqa: E402
    ResourceAgent, resolve_coordinator_cost_matrix,
)
from swarm.utils.instrumentation import SelectionCounters  # noqa: E402
from swarm.utils.yaml_strict import safe_load  # noqa: E402

from evaluation.collect import run_metrics, selection_by_tier  # noqa: E402

REPO = Path(__file__).resolve().parent.parent


class _Info:
    """Just enough AgentInfo for the assignee list."""

    def __init__(self, agent_id):
        self.agent_id = agent_id


class _Topology:
    def __init__(self, level):
        self.level = level


class _Proposal:
    def __init__(self, object_id):
        self.object_id = object_id


def make_agent(agent_id=1, level=1, mode="self", peers=(1, 2, 3)):
    """A bare ResourceAgent carrying only what the assignee choice and the counters touch.

    `_init_instrumentation` is the shipped setup rather than a hand-rolled copy: a double
    that assembles its own counters stops exercising the agent that ships the moment one is
    added (the trap recorded in `swarm-test-the-default-not-your-example`).
    """
    a = ResourceAgent.__new__(ResourceAgent)
    a.agent_id = agent_id
    a.topology = _Topology(level)
    a.coordinator_cost_matrix = mode
    a.neighbor_map = {p: _Info(p) for p in peers}
    a.config = {}
    a._init_instrumentation()
    return a


# --------------------------------------------------------------- the key and its default

class TestConfigKey(unittest.TestCase):
    def test_absent_key_is_the_shipped_behaviour(self):
        """Every hierarchical number measured so far is self-only; the key must not move it."""
        self.assertEqual(resolve_coordinator_cost_matrix({}), "self")
        self.assertEqual(resolve_coordinator_cost_matrix(None), "self")

    def test_shipped_config_is_self(self):
        """Test the default the campaign runs, not the example in the test."""
        cfg = safe_load((REPO / "config_swarm_multi.yml").open())
        self.assertEqual(cfg["job_selection"]["coordinator_cost_matrix"], "self")

    def test_peers_is_accepted(self):
        self.assertEqual(
            resolve_coordinator_cost_matrix({"coordinator_cost_matrix": "PEERS"}), "peers")

    def test_unknown_value_raises_rather_than_defaulting(self):
        """A typo that silently selected the other regime would mislabel a whole cell — the
        same failure the consensus engine name used to have."""
        with self.assertRaises(ValueError) as ctx:
            resolve_coordinator_cost_matrix({"coordinator_cost_matrix": "peer"})
        self.assertIn("coordinator_cost_matrix", str(ctx.exception))


# ------------------------------------------------------------------- who gets scored

class TestAssignees(unittest.TestCase):
    def test_leaf_scores_every_peer_in_both_modes(self):
        """The key is coordinator-only: level 0 must be byte-identical under either value,
        or an ablation arm would quietly change the leaf tier too."""
        for mode in ("self", "peers"):
            agents = make_agent(agent_id=1, level=0, mode=mode)._selection_assignees()
            self.assertEqual(sorted(i.agent_id for i in agents), [1, 2, 3], mode)

    def test_coordinator_self_only_scores_itself(self):
        agents = make_agent(agent_id=2, level=1, mode="self")._selection_assignees()
        self.assertEqual([i.agent_id for i in agents], [2])

    def test_coordinator_peers_scores_the_tier(self):
        agents = make_agent(agent_id=2, level=1, mode="peers")._selection_assignees()
        self.assertEqual(sorted(i.agent_id for i in agents), [1, 2, 3])

    def test_self_only_keeps_its_none_when_the_agent_is_not_in_the_map_yet(self):
        """Verbatim from the hack. An agent whose own record has not landed in neighbor_map
        scores nothing this pass and retries; changing that here would be an untested
        behaviour change smuggled in beside a measurement."""
        a = make_agent(agent_id=9, level=1, mode="self", peers=(1, 2))
        self.assertEqual(a._selection_assignees(), [None])

    def test_peers_drops_missing_entries_instead_of_scoring_none(self):
        a = make_agent(agent_id=1, level=1, mode="peers", peers=(1, 2))
        a.neighbor_map[3] = None
        self.assertEqual(sorted(i.agent_id for i in a._selection_assignees()), [1, 2])


# ------------------------------------------------------------------------- the counters

class TestSelectionCounters(unittest.TestCase):
    def test_distinct_jobs_and_reproposals_are_separate(self):
        c = SelectionCounters(level=1)
        c.record_proposals(["j1", "j2"], assignees=1)
        c.record_proposals(["j1"], assignees=1)          # reselection timeout re-proposes j1
        snap = c.snapshot()
        self.assertEqual(snap["proposals_issued"], 3)
        self.assertEqual(snap["jobs_proposed"], 2)
        self.assertEqual(snap["reproposals"], 1)
        self.assertEqual(snap["level"], 1)

    def test_matrix_width_is_recorded(self):
        c = SelectionCounters(level=0)
        c.record_proposals(["j1"], assignees=30)
        c.record_proposals(["j2"], assignees=20)
        snap = c.snapshot()
        self.assertEqual(snap["matrix_assignees_mean"], 25.0)
        self.assertEqual(snap["matrix_assignees_min"], 20)
        self.assertEqual(snap["matrix_assignees_max"], 30)

    def test_an_empty_batch_is_not_a_proposal(self):
        c = SelectionCounters(level=0)
        c.record_proposals([], assignees=30)
        self.assertEqual(c.snapshot()["proposals_issued"], 0)
        self.assertIsNone(c.snapshot()["matrix_assignees_mean"])

    def test_agent_records_the_batch_it_proposes(self):
        a = make_agent(level=1, mode="self")
        a._note_proposals([_Proposal("j1"), _Proposal("j2")], assignees=1)
        snap = a.selection_counters.snapshot()
        self.assertEqual((snap["proposals_issued"], snap["jobs_proposed"]), (2, 2))
        self.assertEqual(snap["level"], 1)

    def test_the_counter_can_never_stop_a_proposal(self):
        """`_note_proposals` runs inside `selection_main`'s per-pass `try`, whose `except`
        abandons the batch. A missing or broken counter must therefore be silent, not
        raising — bookkeeping about a job may not change what happens to the job."""
        a = ResourceAgent.__new__(ResourceAgent)          # no _init_instrumentation
        a._note_proposals([_Proposal("j1")], assignees=1)  # must not raise

        class _Boom:
            def record_proposals(self, *_a, **_k):
                raise RuntimeError("counter is broken")

        a.selection_counters = _Boom()
        a.logger = None
        a._note_proposals([_Proposal("j1")], assignees=1)  # must not raise either

    def test_snapshot_carries_selection_only_once_something_was_proposed(self):
        a = make_agent(level=1, mode="self")
        a.transport = None
        a.engine = None
        a.logger = type("L", (), {"debug": lambda *_a, **_k: None})()
        self.assertNotIn("selection", a.instrumentation_snapshot())
        a._note_proposals([_Proposal("j1")], assignees=1)
        self.assertEqual(a.instrumentation_snapshot()["selection"]["jobs_proposed"], 1)


# ------------------------------------------------------------------- the collector columns

def _payload(level, proposals, jobs, width, reproposals=0):
    return {"instrumentation": {"selection": {
        "proposals_issued": proposals, "jobs_proposed": jobs, "reproposals": reproposals,
        "matrix_assignees_mean": width, "matrix_assignees_max": width, "level": level}}}


class TestSelectionByTier(unittest.TestCase):
    def test_sums_per_tier(self):
        agents = {"1": _payload(0, 10, 10, 30.0), "2": _payload(0, 5, 4, 30.0, reproposals=1),
                  "3": _payload(1, 20, 20, 1.0)}
        out = selection_by_tier(agents, {})
        self.assertEqual(out["sel_proposals_l0"], 15)
        self.assertEqual(out["sel_proposer_pairs_l0"], 14)
        self.assertEqual(out["sel_reproposals_l0"], 1)
        self.assertEqual(out["sel_agents_l1"], 1)
        self.assertEqual(out["sel_matrix_width_l1"], 1.0)

    def test_the_agents_own_level_beats_the_csv(self):
        """The agent stamps its tier from its own topology; all_agents.csv is the fallback."""
        out = selection_by_tier({"7": _payload(1, 4, 4, 1.0)}, {"7": 0})
        self.assertEqual(out["sel_proposals_l1"], 4)
        self.assertNotIn("sel_proposals_l0", out)

    def test_the_csv_fills_in_a_payload_with_no_level(self):
        payload = _payload(1, 4, 4, 1.0)
        payload["instrumentation"]["selection"].pop("level")
        out = selection_by_tier({"7": payload}, {"7": 1})
        self.assertEqual(out["sel_proposals_l1"], 4)

    def test_an_unattributable_agent_is_counted_not_folded_into_level_zero(self):
        """Putting a coordinator's proposals in the leaf tier is exactly the confusion the
        measurement exists to remove."""
        payload = _payload(1, 4, 4, 1.0)
        payload["instrumentation"]["selection"].pop("level")
        out = selection_by_tier({"7": payload}, {})
        self.assertEqual(out["sel_unattributed_agents"], 1)
        self.assertNotIn("sel_proposals_l0", out)


HEADER = ("job_id,submitted_at,selection_started_at,assigned_at,started_at,"
          "completed_at,exit_status,leader_id,reasoning_time,scheduling_latency\n")


def _rows(ids):
    return "".join(f"{j},1,1,2,2,3,0,1,0,1\n" for j in ids)


class TestProposersPerJobColumn(unittest.TestCase):
    def _run(self, tmp, agents_payload, l1_jobs=("j1", "j2")):
        run_dir = Path(tmp) / "hier-30" / "run01"
        run_dir.mkdir(parents=True)
        (run_dir / "all_jobs.csv").write_text(HEADER + _rows(["j1", "j2"]))
        (run_dir / "level0_jobs.csv").write_text(HEADER + _rows(["j1", "j2"]))
        if l1_jobs:
            (run_dir / "level1_jobs.csv").write_text(HEADER + _rows(l1_jobs))
        import json
        (run_dir / "metrics.json").write_text(json.dumps(agents_payload))
        return run_metrics(run_dir, expected_jobs=2)

    def test_self_only_shows_one_proposer_per_coordinator(self):
        """Five coordinators each proposing both jobs is proposers_per_job_l1 = 5.0 — the
        number that says the tier ran five decisions per job, whatever the protocol."""
        with tempfile.TemporaryDirectory() as tmp:
            agents = {str(i): _payload(1, 2, 2, 1.0) for i in range(1, 6)}
            metrics = self._run(tmp, agents)
        self.assertEqual(metrics["proposers_per_job_l1"], 5.0)
        self.assertEqual(metrics["proposals_per_job_l1"], 5.0)
        self.assertEqual(metrics["sel_matrix_width_l1"], 1.0)

    def test_peers_mode_shows_one_proposer_per_job(self):
        with tempfile.TemporaryDirectory() as tmp:
            agents = {"1": _payload(1, 1, 1, 5.0), "2": _payload(1, 1, 1, 5.0)}
            metrics = self._run(tmp, agents)
        self.assertEqual(metrics["proposers_per_job_l1"], 1.0)
        self.assertEqual(metrics["sel_matrix_width_l1"], 5.0)

    def test_reproposals_separate_the_two_ratios(self):
        with tempfile.TemporaryDirectory() as tmp:
            agents = {"1": _payload(1, 3, 2, 1.0, reproposals=1)}
            metrics = self._run(tmp, agents)
        self.assertEqual(metrics["proposers_per_job_l1"], 1.0)
        self.assertEqual(metrics["proposals_per_job_l1"], 1.5)

    def test_level_zero_falls_back_to_the_runs_job_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "mesh-30" / "run01"
            run_dir.mkdir(parents=True)
            (run_dir / "all_jobs.csv").write_text(HEADER + _rows(["j1", "j2"]))
            import json
            (run_dir / "metrics.json").write_text(json.dumps({"1": _payload(0, 2, 2, 30.0)}))
            metrics = run_metrics(run_dir, expected_jobs=2)
        self.assertEqual(metrics["proposers_per_job_l0"], 1.0)

    def test_a_higher_tier_never_falls_back_to_all_jobs(self):
        """all_jobs.csv is every tier at once; dividing by it would understate the fan-out by
        exactly the factor being measured, so the column is omitted instead."""
        with tempfile.TemporaryDirectory() as tmp:
            agents = {"1": _payload(1, 2, 2, 1.0)}
            metrics = self._run(tmp, agents, l1_jobs=())
        self.assertNotIn("proposers_per_job_l1", metrics)
        self.assertEqual(metrics["sel_proposals_l1"], 2)


if __name__ == "__main__":
    unittest.main()
