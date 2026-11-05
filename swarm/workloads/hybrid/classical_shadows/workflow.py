"""
Classical Shadows Workflow Construction
========================================

This module defines the overall execution structure of a
classical-shadow workload.

The workflow decomposes the computation into two logical stages:

Stage 1: Batch processing
-------------------------

The measurement dataset is divided into independent batches.

Each batch is represented by a classical_shadow_batch job.

These jobs can be distributed across multiple SwarmAgents workers
and executed concurrently.

Stage 2: Result aggregation
---------------------------

Once every batch has completed successfully, a single
classical_shadow_aggregate job combines their partial estimates.

Workflow dependencies
---------------------

The aggregation job depends on every batch job.

Conceptually:

    batch_0 ─────┐
    batch_1 ─────┤
    batch_2 ─────┼──> aggregation
    batch_3 ─────┤
    batch_n ─────┘

The dependencies are recorded in the returned workflow description.

Important
---------

This module constructs a workflow specification.

It does not submit jobs, monitor worker execution, or enforce
dependencies itself.

Those operations must be integrated with the existing SwarmAgents
job distributor and execution infrastructure.
"""


from typing import Any, Dict, List

from .jobs import (
    create_shadow_batch_jobs,
    create_shadow_aggregation_job,
)


def build_classical_shadows_workflow(
    workflow_id: str,
    dataset_ref: str,
    num_batches: int,
    shots_per_batch: int,
    observables: List[Any],
) -> Dict[str, Any]:
    """
    Construct a complete classical-shadow workflow specification.

    Parameters
    ----------
    workflow_id : str
        Unique identifier for the workflow.

    dataset_ref : str
        Reference to the measurement dataset.

    num_batches : int
        Number of independent processing batches.

    shots_per_batch : int
        Number of shots processed by each batch.

    observables : list
        Observables whose expectation values are to be estimated.

    Returns
    -------
    dict
        Workflow specification containing:

        - workflow metadata;
        - batch job specifications;
        - aggregation job specification;
        - job dependency information.

    Notes
    -----
    This function only constructs the workflow.

    A separate integration layer must translate the job specifications
    into SwarmAgents Job objects and submit them to the scheduler.
    """

    if not workflow_id:
        raise ValueError("workflow_id cannot be empty")

    # Construct all independent batch jobs.
    #
    # These jobs can be scheduled independently because each processes
    # a distinct batch of measurement data.
    batch_jobs = create_shadow_batch_jobs(
        workflow_id=workflow_id,
        dataset_ref=dataset_ref,
        num_batches=num_batches,
        shots_per_batch=shots_per_batch,
        observables=observables,
    )

    # Extract the identifiers of all batch jobs.
    #
    # The aggregation stage uses these identifiers to determine
    # which results it must collect.
    batch_job_ids = [
        job["id"]
        for job in batch_jobs
    ]

    # Construct the final aggregation job.
    #
    # This job must wait until all batch jobs have completed.
    aggregation_job = create_shadow_aggregation_job(
        workflow_id=workflow_id,
        batch_job_ids=batch_job_ids,
        observables=observables,
    )

    # Assemble the workflow specification.
    #
    # The dependencies dictionary maps each job to the jobs that
    # must finish before it can execute.
    #
    # In this initial workflow, only aggregation has dependencies.
    return {
        "workflow_id": workflow_id,

        "workload": "classical_shadows",

        # All jobs belonging to this workflow.
        "jobs": batch_jobs + [aggregation_job],

        # Job dependency graph.
        "dependencies": {
            aggregation_job["id"]: batch_job_ids
        },
    }