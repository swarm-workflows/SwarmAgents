"""
Tests for classical-shadow job construction.

These tests verify that the workload correctly creates batch jobs
and aggregation jobs, and rejects invalid job configurations.

No SwarmAgents scheduler or Redis instance is required because
job construction is tested independently.
"""

import pytest

from swarm.workloads.hybrid.classical_shadows.jobs import (
    create_shadow_batch_jobs,
    create_shadow_aggregation_job,
)


@pytest.fixture
def observables():
    """
    Provide a small set of observables shared by the tests.

    Using a fixture avoids repeating the same observable definition
    in every test and makes the test cases easier to read.
    """
    return ["Z0", "Z1"]


def test_create_one_batch(observables):
    """
    Verify that requesting one batch produces exactly one job.

    The generated job must:
    - Have the expected identifier.
    - Be classified as a classical-shadow batch at workload level.
    - Preserve the requested number of measurement shots.

    This checks the simplest valid configuration.
    """

    jobs = create_shadow_batch_jobs(
        workflow_id="wf-1",
        dataset_ref="dataset-1",
        num_batches=1,
        shots_per_batch=100,
        observables=observables,
    )

    assert len(jobs) == 1
    assert jobs[0]["id"] == "wf-1-batch-0"
    assert jobs[0]["job_type"] == "classical_shadow_batch"
    assert jobs[0]["shots_per_batch"] == 100


def test_create_multiple_batches(observables):
    """
    Verify that multiple independent batch jobs are generated correctly.

    Each batch must have:
    - A unique identifier.
    - A distinct batch index.
    - The same workflow identifier.

    This is important for distributed execution because different
    workers may execute these jobs concurrently.
    """

    jobs = create_shadow_batch_jobs(
        workflow_id="wf-1",
        dataset_ref="dataset-1",
        num_batches=4,
        shots_per_batch=100,
        observables=observables,
    )

    assert len(jobs) == 4

    # Ensure that every generated job has a unique identifier.
    assert len({job["id"] for job in jobs}) == 4

    # Verify that batch indices are sequential and unambiguous.
    assert [job["batch_id"] for job in jobs] == [0, 1, 2, 3]


@pytest.mark.parametrize(
    "kwargs",
    [
        # A workflow must have an identifier.
        {"workflow_id": "", "dataset_ref": "d"},

        # A dataset reference is required to locate measurement data.
        {"workflow_id": "wf", "dataset_ref": ""},

        # A workflow cannot contain zero batches.
        {"workflow_id": "wf", "dataset_ref": "d", "num_batches": 0},

        # Every batch must process at least one measurement shot.
        {"workflow_id": "wf", "dataset_ref": "d", "shots_per_batch": 0},

        # At least one observable must be specified.
        {"workflow_id": "wf", "dataset_ref": "d", "observables": []},
    ],
)
def test_invalid_batch_parameters(kwargs):
    """
    Verify that invalid batch configurations are rejected.

    Parameterization allows several invalid configurations to be
    tested using the same assertion.

    The expected behaviour is an immediate ValueError, rather than
    allowing an invalid job to enter the distributed scheduler.
    """

    # Start with a valid configuration and override individual fields
    # with invalid values supplied by the parameterized test cases.
    params = {
        "workflow_id": "wf",
        "dataset_ref": "dataset",
        "num_batches": 2,
        "shots_per_batch": 100,
        "observables": ["Z0"],
    }

    params.update(kwargs)

    with pytest.raises(ValueError):
        create_shadow_batch_jobs(**params)


def test_invalid_resource_requirements(observables):
    """
    Verify that invalid resource requirements are rejected.

    A job requesting zero CPU capacity cannot be treated as a valid
    resource request under the current workload specification.

    This test prevents nonsensical resource requirements from being
    passed to the SwarmAgents scheduler.
    """

    with pytest.raises(ValueError):
        create_shadow_batch_jobs(
            workflow_id="wf",
            dataset_ref="dataset",
            num_batches=2,
            shots_per_batch=100,
            observables=observables,
            cpu=0,
        )


def test_create_aggregation_job(observables):
    """
    Verify that an aggregation job is constructed correctly.

    The aggregation job must:
    - Have a unique workflow-level identifier.
    - Use the aggregation job type.
    - Record the identifiers of all batch jobs whose results
      it must collect.

    This checks that the information needed for dependency tracking
    is present in the generated job specification.
    """

    job = create_shadow_aggregation_job(
        workflow_id="wf-1",
        batch_job_ids=["wf-1-batch-0", "wf-1-batch-1"],
        observables=observables,
    )

    assert job["id"] == "wf-1-aggregate"
    assert job["job_type"] == "classical_shadow_aggregate"
    assert len(job["batch_job_ids"]) == 2


def test_aggregation_rejects_duplicate_batch_ids(observables):
    """
    Verify that an aggregation job cannot depend on the same batch
    identifier more than once.

    Duplicate dependencies could lead to accidental double counting
    or ambiguous workflow state.

    The job constructor should detect this configuration before
    creating the aggregation job.
    """

    with pytest.raises(ValueError):
        create_shadow_aggregation_job(
            workflow_id="wf-1",
            batch_job_ids=["batch-0", "batch-0"],
            observables=observables,
        )