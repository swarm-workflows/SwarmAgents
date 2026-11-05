"""
Tests for classical-shadow batch execution.

These tests verify that the worker correctly validates job
specifications, invokes the supplied algorithm, and normalizes
the resulting estimates.

A fake algorithm is used so that the tests do not require
KrMM-shadows, quantum hardware, or measurement datasets.
"""

import pytest

from swarm.workloads.hybrid.classical_shadows.worker import (
    execute_shadow_batch,
)


def fake_algorithm(
    dataset_ref,
    batch_id,
    shots,
    observables,
):
    """
    Simulate a classical-shadow algorithm.

    The function returns deterministic estimates for each observable.

    Deterministic results are useful in unit tests because they allow
    exact verification of the worker's output without introducing
    randomness from measurement sampling.
    """

    return {
        "estimates": {
            observable: {
                "mean": 0.5,
                "variance": 0.02,
            }
            for observable in observables
        }
    }


@pytest.fixture
def valid_job():
    """
    Provide a valid classical-shadow batch job.

    This fixture represents the minimum information expected by
    execute_shadow_batch().

    It is reused across tests so that individual test cases can
    modify only the field relevant to the scenario being tested.
    """

    return {
        "id": "wf-1-batch-0",
        "job_type": "classical_shadow_batch",
        "workflow_id": "wf-1",
        "batch_id": 0,
        "dataset_ref": "dataset-1",
        "shots_per_batch": 100,
        "observables": ["Z0", "Z1"],
    }


def test_execute_valid_batch(valid_job):
    """
    Verify that a valid batch job executes successfully.

    The returned result must preserve:
    - The workflow identifier.
    - The batch identifier.
    - The number of shots.
    - The observable estimates.

    This checks the basic contract between the workload worker
    and the aggregation stage.
    """

    result = execute_shadow_batch(valid_job, fake_algorithm)

    assert result["workflow_id"] == "wf-1"
    assert result["batch_id"] == 0
    assert result["shots"] == 100
    assert result["estimates"]["Z0"]["mean"] == 0.5


def test_algorithm_receives_correct_parameters(valid_job):
    """
    Verify that the worker forwards the correct parameters
    to the underlying classical-shadow algorithm.

    This is important because the worker is the boundary between
    SwarmAgents and the scientific implementation.

    An incorrect dataset reference, batch index, or shot count
    could produce scientifically invalid results without necessarily
    causing a software exception.
    """

    received = {}

    def recording_algorithm(**kwargs):
        """
        Record the parameters received by the algorithm.

        This test implementation allows the test to inspect exactly
        what the worker forwards to the computation.
        """
        received.update(kwargs)
        return {"estimates": {}}

    execute_shadow_batch(valid_job, recording_algorithm)

    assert received["dataset_ref"] == "dataset-1"
    assert received["batch_id"] == 0
    assert received["shots"] == 100
    assert received["observables"] == ["Z0", "Z1"]


def test_reject_unsupported_job_type(valid_job):
    """
    Verify that the worker rejects jobs belonging to other workloads.

    The classical-shadow worker should not accidentally execute
    arbitrary SwarmAgents jobs.

    This protects the workload boundary when the worker is eventually
    integrated into the general ResourceAgent execution path.
    """

    valid_job["job_type"] = "unknown"

    with pytest.raises(ValueError):
        execute_shadow_batch(valid_job, fake_algorithm)


def test_reject_missing_field(valid_job):
    """
    Verify that missing mandatory job metadata is detected.

    Without a dataset reference, the worker cannot determine which
    measurement data it should process.

    The worker should fail immediately rather than pass incomplete
    information to the algorithm implementation.
    """

    del valid_job["dataset_ref"]

    with pytest.raises(ValueError):
        execute_shadow_batch(valid_job, fake_algorithm)


def test_algorithm_exception_propagates(valid_job):
    """
    Verify that exceptions raised by the underlying algorithm
    are propagated to the caller.

    Silently swallowing algorithm failures would be dangerous:
    the distributed scheduler might incorrectly mark an unsuccessful
    computation as successful.

    The ResourceAgent integration will later be responsible for
    converting such failures into the appropriate job failure state.
    """

    def failing_algorithm(**kwargs):
        raise RuntimeError("Algorithm failed")

    with pytest.raises(RuntimeError, match="Algorithm failed"):
        execute_shadow_batch(valid_job, failing_algorithm)


def test_reject_non_dictionary_result(valid_job):
    """
    Verify that the worker rejects results that do not follow
    the expected dictionary-based result interface.

    This catches integration errors where an algorithm returns
    an unexpected object or fails to normalize its output.
    """

    def invalid_algorithm(**kwargs):
        return None

    with pytest.raises(TypeError):
        execute_shadow_batch(valid_job, invalid_algorithm)


def test_reject_missing_estimates(valid_job):
    """
    Verify that the worker rejects algorithm results that do not
    contain the required 'estimates' field.

    Without this field, the aggregation stage has no observable
    estimates to process.
    """

    def invalid_algorithm(**kwargs):
        return {"unexpected": 123}

    with pytest.raises(ValueError):
        execute_shadow_batch(valid_job, invalid_algorithm)


def test_reject_invalid_estimates_type(valid_job):
    """
    Verify that the estimates field has the expected dictionary type.

    A list or another incompatible object would make it impossible
    for the aggregation stage to identify estimates by observable.

    The worker should reject such results before returning them.
    """

    def invalid_algorithm(**kwargs):
        return {"estimates": []}

    with pytest.raises(TypeError):
        execute_shadow_batch(valid_job, invalid_algorithm)