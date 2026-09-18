"""Workflow DAG gating: a job waits for the files another job produces.

A Pegasus workflow converts to SwarmAgents jobs that carry real wall times, capacities and
file names — but until now nothing carried the *order*. All 11 jobs of the earthquake
workflow would be proposed at once, which is not that workflow.

The edges are recoverable without any new extraction, because a DAG's dependencies are file
dependencies: job B depends on job A exactly when B lists an input that A lists as an output.
`apply_dag_gating` does that matching and writes a `data_predicate` naming the files; the
agent's existing gate (`_data_predicate_ready`) holds the job out of selection until
`Repository.data_available` says those names exist, and a job publishes its outputs when it
finishes successfully.

What these tests hold to:

* the whole DAG is recovered, and only edges *inside* the job set become predicates — gating a
  root on its staged-in inputs would hold the workflow at its root forever;
* per-site data nodes lose edges, so `--dag-gating` forces per-file;
* the gate releases a job only when every input exists, and a failed parent releases nothing;
* an unreachable registry keeps jobs gated rather than releasing the whole DAG at once;
* the quantum predicate, which shares this gate, is unaffected.
"""
import os
import sys
from unittest.mock import MagicMock

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from pegasus_to_swarm_converter import apply_dag_gating  # noqa: E402
from swarm.agents.resource_agent import ResourceAgent  # noqa: E402
from swarm.models.job import Job  # noqa: E402


def _job(jid, ins=(), outs=()):
    return {"id": jid,
            "data_in": [{"name": "local", "file": f} for f in ins],
            "data_out": [{"name": "local", "file": f} for f in outs]}


# --------------------------------------------------------------------------------------
# Recovering the graph from file names alone.
# --------------------------------------------------------------------------------------

def test_edges_are_recovered_from_file_names():
    jobs = [
        _job("fetch", ins=["seed.txt"], outs=["catalog.csv"]),
        _job("analyze", ins=["catalog.csv"], outs=["patterns.json"]),
        _job("viz", ins=["catalog.csv", "patterns.json"], outs=["plot.png"]),
    ]
    edges, roots = apply_dag_gating(jobs)

    assert edges == 3
    assert roots == ["fetch"], "a job with no produced input is a root"
    assert jobs[1]["data_predicate"] == {"kind": "files", "files": ["catalog.csv"]}
    assert jobs[2]["data_predicate"]["files"] == ["catalog.csv", "patterns.json"]


def test_a_root_is_not_gated_on_its_staged_in_inputs():
    """`seed.txt` comes from outside the workflow; nothing in the run will ever produce it,
    so gating on it would hold the DAG at its root for the whole cell."""
    jobs = [_job("fetch", ins=["seed.txt"], outs=["catalog.csv"])]
    edges, roots = apply_dag_gating(jobs)

    assert edges == 0
    assert roots == ["fetch"]
    assert "data_predicate" not in jobs[0]


def test_a_job_consuming_its_own_output_is_not_gated_on_itself():
    jobs = [_job("solo", ins=["x.dat"], outs=["x.dat"])]
    edges, _ = apply_dag_gating(jobs)
    assert edges == 0 and "data_predicate" not in jobs[0]


def test_the_earthquake_workflow_shape():
    """The real thing: one fan-out to nine, plus three viz pairs = 13 edges, one root."""
    cat = "california_catalog.csv"
    jobs = [_job("fetch", ins=["usgs"], outs=[cat])]
    for n in ("analyze", "visualize", "anomalies", "cluster"):
        jobs.append(_job(n, ins=[cat], outs=[f"{n}.json"]))
    for n in ("aftershock", "hazard", "gaps"):
        jobs.append(_job(n, ins=[cat], outs=[f"{n}.json"]))
        jobs.append(_job(f"{n}_viz", ins=[cat, f"{n}.json"], outs=[f"{n}.png"]))

    edges, roots = apply_dag_gating(jobs)
    assert roots == ["fetch"]
    assert edges == 13
    assert jobs[-1]["data_predicate"]["files"] == [cat, "gaps.json"]


# --------------------------------------------------------------------------------------
# The gate.
# --------------------------------------------------------------------------------------

def _agent(available):
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = MagicMock()
    a.measurement_layer = MagicMock()
    a.repository = MagicMock()
    a.repository.data_available.side_effect = lambda names: all(n in available for n in names)
    return a


def _gated_job(files):
    j = Job()
    j.job_id = "j1"
    j.data_predicate = {"kind": "files", "files": list(files)}
    return j


def test_a_job_waits_until_every_input_exists():
    have = {"a.csv"}
    a = _agent(have)
    job = _gated_job(["a.csv", "b.json"])

    assert a._data_predicate_ready(job) is False, "one of two inputs is not enough"
    have.add("b.json")
    assert a._data_predicate_ready(job) is True


def test_an_ungated_job_is_always_ready():
    j = Job()
    j.job_id = "root"
    assert _agent(set())._data_predicate_ready(j) is True


def test_a_registry_that_cannot_be_reached_keeps_jobs_gated():
    """The one error this gate exists to prevent is releasing the whole DAG at once."""
    a = _agent(set())
    a.repository.data_available.side_effect = RuntimeError("redis down")
    assert a._data_predicate_ready(_gated_job(["a.csv"])) is False


def test_the_quantum_predicate_still_uses_the_measurement_layer():
    """Two kinds of predicate share this gate; the older one must be untouched."""
    a = _agent(set())
    a.measurement_layer.predicate_satisfied.return_value = True
    j = Job()
    j.job_id = "q"
    j.data_predicate = {"experiment_id": "e1", "min_snapshots": 2, "total_snapshots": 4}

    assert a._data_predicate_ready(j) is True
    a.measurement_layer.predicate_satisfied.assert_called_once_with("e1", 2)
    a.repository.data_available.assert_not_called()


def test_the_file_predicate_is_part_of_the_cost_cache_signature():
    """Two jobs identical but for the files they wait on are not interchangeable, and a cache
    that conflated them would price one with the other's cost."""
    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = MagicMock()

    from swarm.models.capacities import Capacities

    def sig(files):
        j = Job()
        j.job_id = "same-id"
        j.wall_time = 1.0
        j.capacities = Capacities(core=1, ram=1, disk=1)
        j.data_predicate = {"kind": "files", "files": files}
        return a._job_sig(j)

    assert sig(["a.csv"]) != sig(["b.csv"])
    assert sig(["a.csv"]) == sig(["a.csv"]), "and the same predicate must still cache-hit"


def test_the_file_predicate_does_not_trip_the_split_hybrid_comm_penalty():
    """That penalty looks up a measurement producer's site. A DAG edge has none, and the
    lookup would have been made on an experiment id that does not exist."""
    import inspect
    src = inspect.getsource(ResourceAgent.compute_job_cost) \
        if hasattr(ResourceAgent, "compute_job_cost") else ""
    assert 'pred.get("kind") == "files"' in src, (
        "the cost path must skip the measurement-stream penalty for a file predicate")


# --------------------------------------------------------------------------------------
# Publishing what a job produced — the other half of the edge.
# --------------------------------------------------------------------------------------

def _leaf_agent():
    """A leaf far enough along to run `execute_job` without a fleet behind it."""
    import threading
    from swarm.utils.metrics import Metrics

    a = ResourceAgent.__new__(ResourceAgent)
    a.logger = MagicMock()
    a.agent_id = 1
    a.config = {"runtime": {}}
    a.runtime_config = {}
    a.metrics = Metrics()
    a.repository = MagicMock()
    a.topology = MagicMock(level=0, group=0)
    a.queues = MagicMock()
    a.engine = MagicMock()
    a._init_decision_state()   # the shipped dedupe state, not a hand-built copy
    a.failure_sim_enabled = False
    a.measurement_layer = None
    a.consumer_timeout_s = 1.0
    a.site = None
    a.shutdown = False
    a._unpublished_data = set()
    a._unpersisted_completions = {}
    a._unpublished_lock = threading.RLock()
    return a


def _producing_job(should_fail=False):
    from swarm.models.data_node import DataNode
    j = Job()
    j.job_id = "producer"
    j.wall_time = 0.0
    j.data_out = [DataNode(name="local", file="catalog.csv"),
                  DataNode(name="local", file="index.json")]
    j._should_fail = should_fail
    return j


def test_completion_and_publication_are_one_write():
    """The gap this closes: as two writes, an agent dying between them gated every descendant
    of the job for the rest of the run, and no retry queue can cover that — the queue dies
    with the agent. The names therefore ride along inside the transaction that records the
    job as COMPLETE."""
    a = _leaf_agent()
    a.execute_job(_producing_job())

    a.repository.mark_data_available.assert_not_called()
    kw = a.repository.save.call_args.kwargs
    assert sorted(kw["produced_data"]) == ["catalog.csv", "index.json"]
    assert kw["obj"]["state"] == 8, "the same write records COMPLETE"


def test_a_failed_job_publishes_nothing():
    """The safety property the whole gate rests on: releasing a child on its parent's failure
    would run it against inputs that were never written."""
    a = _leaf_agent()
    a.execute_job(_producing_job(should_fail=True))

    assert a.repository.save.call_args.kwargs["produced_data"] is None
    a.repository.mark_data_available.assert_not_called()


def test_a_job_with_no_outputs_publishes_nothing():
    a = _leaf_agent()
    j = Job()
    j.job_id = "sink"
    j.wall_time = 0.0
    a.execute_job(j)
    assert not a.repository.save.call_args.kwargs["produced_data"]


def test_a_persistence_failure_does_not_become_an_execution_failure():
    """The regression: sharing one `try` meant a Redis blip on the completion write rewrote a
    successful job as `exit_status=1`, persisted it COMPLETE — so reselection would never
    revisit it — and published nothing. Every descendant stayed gated for the rest of the run.
    """
    a = _leaf_agent()
    a.repository.save.side_effect = RuntimeError("redis blip")
    job = _producing_job()

    a.execute_job(job)

    assert job.exit_status == 0, "a failure to persist is not a failure to execute"
    assert a._unpersisted_completions, "the completion must be kept for retry"
    payload, produced = a._unpersisted_completions["producer"]
    assert payload["exit_status"] == 0
    assert sorted(produced) == ["catalog.csv", "index.json"], \
        "the outputs are retried WITH the completion, not separately"


def test_the_queued_completion_is_re_persisted_with_its_outputs():
    a = _leaf_agent()
    a.repository.save.side_effect = RuntimeError("redis blip")
    a.execute_job(_producing_job())

    a.repository.save.side_effect = None                 # Redis comes back
    a._retry_unpersisted_completions()

    assert a._unpersisted_completions == {}
    kw = a.repository.save.call_args.kwargs
    assert kw["obj"]["exit_status"] == 0
    assert sorted(kw["produced_data"]) == ["catalog.csv", "index.json"]


def test_a_retry_that_fails_again_keeps_the_completion():
    a = _leaf_agent()
    a.repository.save.side_effect = RuntimeError("still down")
    a.execute_job(_producing_job())

    a._retry_unpersisted_completions()

    assert "producer" in a._unpersisted_completions


def test_a_genuinely_failed_job_is_still_recorded_as_failed():
    """Separating the two concerns must not stop a real execution failure being recorded."""
    a = _leaf_agent()
    job = _producing_job()
    job.execute = MagicMock(side_effect=RuntimeError("the task blew up"))

    a.execute_job(job)

    assert job.exit_status == 1
    kw = a.repository.save.call_args.kwargs
    assert kw["obj"]["exit_status"] == 1
    assert kw["produced_data"] is None, "a failed job publishes nothing"


# --------------------------------------------------------------------------------------
# Readiness must not outlive the run that produced it.
# --------------------------------------------------------------------------------------

class _FakeRedis:
    """Enough of a Redis for the registry: sets, SCAN and DELETE with real key semantics."""

    def __init__(self):
        self.sets = {}

    def sadd(self, key, *vals):
        self.sets.setdefault(key, set()).update(str(v) for v in vals)

    def smismember(self, key, names):
        have = self.sets.get(key, set())
        return [n in have for n in names]

    def smembers(self, key):
        return set(self.sets.get(key, set()))

    def scan_iter(self, match):
        import fnmatch
        return [k for k in list(self.sets) if fnmatch.fnmatch(k, match)]

    def delete(self, *keys):
        for k in keys:
            self.sets.pop(k, None)


def _repo(client, run_id):
    from swarm.database.repository import Repository
    return Repository(client, run_id=run_id)


def test_a_second_run_does_not_inherit_the_first_runs_readiness():
    """The regression: a bare key survived `delete_all`, so a repeat of the same workflow
    found every file name already present and released the whole DAG on the first tick."""
    from swarm.database.repository import Repository
    client = _FakeRedis()

    r1 = _repo(client, "run-1")
    r1.mark_data_available(["california_catalog.csv"])
    assert r1.data_available(["california_catalog.csv"]) is True

    Repository(client, run_id="run-1").delete_all(key_prefix="*")   # what cleanup.py runs

    r2 = _repo(client, "run-1")
    assert r2.data_available(["california_catalog.csv"]) is False, \
        "cleanup must reach the registry — its key has to contain a colon"


def test_readiness_is_scoped_to_the_run_even_without_cleanup():
    """Cleanup is not the only path: a cell that skipped it must still not inherit."""
    client = _FakeRedis()
    _repo(client, "run-1").mark_data_available(["catalog.csv"])

    assert _repo(client, "run-2").data_available(["catalog.csv"]) is False
    assert _repo(client, "run-1").data_available(["catalog.csv"]) is True


def test_the_registry_key_has_the_shape_cleanup_scans_for():
    client = _FakeRedis()
    key = _repo(client, "run-1")._data_ready_key()
    import fnmatch
    assert fnmatch.fnmatch(key, "*:*"), f"{key!r} would survive delete_all('*')"


# --------------------------------------------------------------------------------------
# A publish that fails must not gate the subtree for the rest of the run.
# --------------------------------------------------------------------------------------

def test_an_out_of_band_publish_that_fails_is_retried_until_it_lands():
    """`_publish_produced` is the route for a caller with no completion write to ride on."""
    a = _leaf_agent()
    a.repository.mark_data_available.side_effect = RuntimeError("redis down")

    assert a._publish_produced("j1", ["catalog.csv", "index.json"]) is False
    assert a._unpublished_data == {"catalog.csv", "index.json"}, "the names must be kept"

    a.repository.mark_data_available.side_effect = None            # Redis comes back
    a._retry_unpublished_data()

    assert a._unpublished_data == set()
    assert sorted(a.repository.mark_data_available.call_args.args[0]) == \
        ["catalog.csv", "index.json"]


def test_a_retry_that_fails_again_keeps_the_names():
    a = _leaf_agent()
    a.repository.mark_data_available.side_effect = RuntimeError("still down")
    a._publish_produced("j1", ["catalog.csv", "index.json"])

    a._retry_unpublished_data()

    assert a._unpublished_data == {"catalog.csv", "index.json"}, \
        "names are only dropped once they have actually landed"


def test_an_out_of_band_publish_error_never_raises():
    """It runs inside `execute_job`, whose handler would mark a completed job FAILED."""
    a = _leaf_agent()
    a.repository.mark_data_available.side_effect = RuntimeError("down")
    a._unpublished_lock = None                       # make the recovery path itself break
    assert a._publish_produced("j1", ["x"]) is False


def test_retrying_with_nothing_pending_does_not_touch_redis():
    a = _leaf_agent()
    a._retry_unpublished_data()
    a.repository.mark_data_available.assert_not_called()


def test_a_failed_transaction_leaves_neither_the_job_nor_its_outputs():
    """Atomicity is the whole point: a partial write is what created the gap. If EXEC never
    runs, the job must not be COMPLETE *and* its names must not be readable — the gated
    descendants then wait for a reselection, which is the correct outcome."""
    from swarm.database.repository import Repository

    class _Exploding(_FakeRedis):
        def pipeline(self):
            outer = self

            class _P:
                def watch(self, *a): pass
                def get(self, *a): return None
                def multi(self): pass
                def set(self, *a): pass
                def sadd(self, *a): pass
                def srem(self, *a): pass
                def execute(self):
                    raise RuntimeError("connection lost mid-transaction")
                def reset(self): pass
            return _P()

    client = _Exploding()
    repo = Repository(client, run_id="run-1")
    with pytest.raises(RuntimeError):
        repo.save(obj={"id": "j1", "state": 8}, produced_data=["catalog.csv"])

    assert client.sets == {}, "a transaction that did not commit must publish nothing"
    assert repo.data_available(["catalog.csv"]) is False


def test_the_predicate_survives_a_round_trip_through_redis():
    j = Job()
    j.job_id = "j1"
    j.data_predicate = {"kind": "files", "files": ["a.csv", "b.json"]}
    back = Job()
    back.from_dict(j.to_dict())
    assert back.data_predicate == {"kind": "files", "files": ["a.csv", "b.json"]}


class TestCollidingOutputsAcrossWorkflows:
    """Converting several workflows into one bundle shares one flat working directory and one
    producer map, and generic names recur across unrelated workflows.
    """

    def test_a_name_produced_by_two_jobs_is_reported(self):
        from pegasus_to_swarm_converter import colliding_outputs
        jobs = [
            {"id": "wfA_split", "data_in": [], "data_out": [{"file": "output.csv"}]},
            {"id": "wfB_extract", "data_in": [], "data_out": [{"file": "output.csv"}]},
            {"id": "wfB_merge", "data_in": [{"file": "output.csv"}], "data_out": []},
        ]
        assert colliding_outputs(jobs) == {"output.csv": ["wfA_split", "wfB_extract"]}

    def test_disjoint_workflows_report_nothing(self):
        from pegasus_to_swarm_converter import colliding_outputs
        jobs = [
            {"id": "wfA_split", "data_in": [], "data_out": [{"file": "a.csv"}]},
            {"id": "wfB_split", "data_in": [], "data_out": [{"file": "b.csv"}]},
        ]
        assert colliding_outputs(jobs) == {}

    def test_the_edge_it_warns_about_is_real(self):
        """Not hypothetical: gating keys the producer map by name, so the consumer of a
        colliding name is gated on whichever job was seen last."""
        from pegasus_to_swarm_converter import apply_dag_gating
        jobs = [
            {"id": "wfA_split", "data_in": [], "data_out": [{"file": "output.csv"}]},
            {"id": "wfB_extract", "data_in": [], "data_out": [{"file": "output.csv"}]},
            {"id": "wfB_merge", "data_in": [{"file": "output.csv"}], "data_out": []},
        ]
        apply_dag_gating(jobs)
        assert jobs[2]["data_predicate"] == {"kind": "files", "files": ["output.csv"]}


class TestNamesAreDecidedByTheWorkDir:
    """The working directory is flat and `stage_inputs` reduces a declared name to its
    basename, so the basename is the name a file actually gets."""

    def test_two_directories_one_file_name_collide(self):
        from pegasus_to_swarm_converter import colliding_outputs
        jobs = [
            {"id": "wfA_x", "data_out": [{"file": "runA/out.csv"}]},
            {"id": "wfB_y", "data_out": [{"file": "runB/out.csv"}]},
        ]
        # Keyed by the recorded string these are two names and nothing is reported; keyed by
        # the basename they are one file, written twice.
        assert colliding_outputs(jobs) == {"out.csv": ["wfA_x", "wfB_y"]}


class TestStagedInputsThatSomeJobProduces:
    def test_a_cross_workflow_conflict_is_flagged(self):
        from pegasus_to_swarm_converter import conflicting_replicas
        pairs = [
            ({"id": "wfA_split", "workflow": "wfA", "data_out": [{"file": "data.csv"}]}, {}),
            ({"id": "wfB_load", "workflow": "wfB", "data_in": [{"file": "data.csv"}]},
             {"replicas_db": {"data.csv": "/wfB/data.csv"}}),
        ]
        out = conflicting_replicas(pairs)
        assert out["data.csv"]["cross_workflow"] is True
        assert out["data.csv"]["produced_by"] == ["wfA_split"]

    def test_within_one_workflow_it_is_reported_but_not_cross_workflow(self):
        from pegasus_to_swarm_converter import conflicting_replicas
        pairs = [
            ({"id": "wfA_fetch", "workflow": "wfA", "data_out": [{"file": "data.csv"}]}, {}),
            ({"id": "wfA_load", "workflow": "wfA", "data_in": [{"file": "data.csv"}]},
             {"replicas_db": {"data.csv": "/wfA/data.csv"}}),
        ]
        assert conflicting_replicas(pairs)["data.csv"]["cross_workflow"] is False

    def test_a_replica_nothing_produces_is_not_a_conflict(self):
        from pegasus_to_swarm_converter import conflicting_replicas
        pairs = [({"id": "wfA_load", "workflow": "wfA", "data_in": [{"file": "seed.json"}]},
                  {"replicas_db": {"seed.json": "/wfA/seed.json"}})]
        assert conflicting_replicas(pairs) == {}


class TestCrossWorkflowEdges:
    """Two workflows have no data relationship, so a name they share is a coincidence. Both
    mechanisms that act on names act on it anyway."""

    def test_a_name_one_workflow_writes_and_another_reads(self):
        from pegasus_to_swarm_converter import cross_workflow_edges
        jobs = [
            {"id": "wfA_split", "workflow": "wfA", "data_out": [{"file": "out.csv"}]},
            {"id": "wfB_load", "workflow": "wfB", "data_in": [{"file": "out.csv"}]},
        ]
        assert cross_workflow_edges(jobs) == {
            "out.csv": {"read_by": ["wfB_load"], "produced_by": ["wfA_split"]}}

    def test_a_real_edge_inside_one_workflow_is_not_reported(self):
        from pegasus_to_swarm_converter import cross_workflow_edges
        jobs = [
            {"id": "wfA_split", "workflow": "wfA", "data_out": [{"file": "out.csv"}]},
            {"id": "wfA_load", "workflow": "wfA", "data_in": [{"file": "out.csv"}]},
        ]
        assert cross_workflow_edges(jobs) == {}

    def test_it_does_not_depend_on_a_replica_catalog(self):
        """The check this replaced compared declared replicas, which most profiles do not
        carry — the replica catalog is not in the stampede DB — so the common case reported
        nothing at all."""
        from pegasus_to_swarm_converter import cross_workflow_edges, conflicting_replicas
        jobs = [
            {"id": "wfA_split", "workflow": "wfA", "data_out": [{"file": "out.csv"}]},
            {"id": "wfB_load", "workflow": "wfB", "data_in": [{"file": "out.csv"}]},
        ]
        assert conflicting_replicas([(j, {}) for j in jobs]) == {}      # no replicas_db
        assert cross_workflow_edges(jobs)                               # still caught

    def test_directories_do_not_hide_it(self):
        from pegasus_to_swarm_converter import cross_workflow_edges
        jobs = [
            {"id": "wfA_split", "workflow": "wfA", "data_out": [{"file": "a/out.csv"}]},
            {"id": "wfB_load", "workflow": "wfB", "data_in": [{"file": "b/out.csv"}]},
        ]
        assert "out.csv" in cross_workflow_edges(jobs)
