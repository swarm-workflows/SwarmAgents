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
    a.completed_jobs_set = set()
    a.completed_lock = threading.RLock()
    a.failure_sim_enabled = False
    a.measurement_layer = None
    a.consumer_timeout_s = 1.0
    a.site = None
    a.shutdown = False
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


def test_a_successful_job_publishes_what_it_produced():
    a = _leaf_agent()
    a.execute_job(_producing_job())

    a.repository.mark_data_available.assert_called_once()
    assert sorted(a.repository.mark_data_available.call_args.args[0]) == \
        ["catalog.csv", "index.json"]


def test_a_failed_job_publishes_nothing():
    """The safety property the whole gate rests on: releasing a child on its parent's failure
    would run it against inputs that were never written."""
    a = _leaf_agent()
    a.execute_job(_producing_job(should_fail=True))

    a.repository.mark_data_available.assert_not_called()


def test_a_publishing_error_does_not_turn_a_completed_job_into_a_failed_one():
    a = _leaf_agent()
    a.repository.mark_data_available.side_effect = RuntimeError("redis down")
    job = _producing_job()

    a.execute_job(job)

    assert job.exit_status == 0, "bookkeeping must not rewrite the job's outcome"


def test_the_predicate_survives_a_round_trip_through_redis():
    j = Job()
    j.job_id = "j1"
    j.data_predicate = {"kind": "files", "files": ["a.csv", "b.json"]}
    back = Job()
    back.from_dict(j.to_dict())
    assert back.data_predicate == {"kind": "files", "files": ["a.csv", "b.json"]}
