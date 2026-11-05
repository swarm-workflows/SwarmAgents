"""
Tests for classical-shadow workflow construction.
Author: Vincenzo De Maio (v.demaio@leicester.ac.uk)

These tests verify that the workflow builder creates the correct
collection of jobs and describes their dependency relationships.

The tests do not execute the jobs or interact with the SwarmAgents
scheduler. They validate the workflow specification itself.
"""

import pytest

from swarm.workloads.hybrid.classical_shadows.workflow import (
    build_classical_shadows_workflow,
)


def test_build_workflow():
    """
    Verify that a valid classical-shadow workflow is constructed.

    The workflow should contain:
    - The correct workflow identifier.
    - The correct workload name.
    - The requested number of batch jobs.
    - Exactly one aggregation job.

    For four batches, the total number of jobs must therefore be five.
    """

    workflow = build_classical_shadows_workflow(
        workflow_id="wf-1",
        dataset_ref="dataset-1",
        num_batches=4,
        shots_per_batch=100,
        observables=["Z0", "Z1"],
    )

    assert workflow["workflow_id"] == "wf-1"
    assert workflow["workload"] == "classical_shadows"

    # Four independent batch jobs plus one aggregation job.
    assert len(workflow["jobs"]) == 5


def test_aggregation_depends_on_all_batches():
    """
    Verify that the aggregation job depends on every generated batch.

    The workflow description must explicitly identify all batch jobs
    whose results are needed by aggregation.

    This test validates the dependency metadata, but does not prove
    that the scheduler enforces the dependencies. That requires
    a separate integration test.
    """

    workflow = build_classical_shadows_workflow(
        workflow_id="wf-1",
        dataset_ref="dataset-1",
        num_batches=3,
        shots_per_batch=100,
        observables=["Z0"],
    )

    dependencies = workflow["dependencies"]

    assert dependencies["wf-1-aggregate"] == [
        "wf-1-batch-0",
        "wf-1-batch-1",
        "wf-1-batch-2",
    ]


def test_workflow_job_ids_are_unique():
    """
    Verify that every job in the workflow has a unique identifier.

    Job identifiers are used by the distributed infrastructure
    to track execution, associate results, and manage retries.

    Identifier collisions could cause one job to overwrite another
    job's state or result.
    """

    workflow = build_classical_shadows_workflow(
        workflow_id="wf-1",
        dataset_ref="dataset-1",
        num_batches=5,
        shots_per_batch=100,
        observables=["Z0"],
    )

    ids = [
        job["id"]
        for job in workflow["jobs"]
    ]

    assert len(ids) == len(set(ids))


def test_invalid_workflow_id():
    """
    Verify that a workflow cannot be created without an identifier.

    The workflow ID is essential for associating batch jobs,
    intermediate results, and final aggregation output.

    An empty identifier should therefore be rejected immediately.
    """

    with pytest.raises(ValueError):
        build_classical_shadows_workflow(
            workflow_id="",
            dataset_ref="dataset-1",
            num_batches=2,
            shots_per_batch=100,
            observables=["Z0"],
        )


def test_zero_batches_is_rejected():
    """
    Verify that the workflow builder rejects a configuration
    containing zero batch jobs.

    A workflow with no batch jobs cannot produce a meaningful
    aggregation result.

    The error should be detected during workflow construction,
    before any job is submitted to SwarmAgents.
    """

    with pytest.raises(ValueError):
        build_classical_shadows_workflow(
            workflow_id="wf-1",
            dataset_ref="dataset-1",
            num_batches=0,
            shots_per_batch=100,
            observables=["Z0"],
        )