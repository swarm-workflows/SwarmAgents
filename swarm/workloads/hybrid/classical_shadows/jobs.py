"""
Classical Shadows Job Construction
===================================

This module defines the jobs required to execute a classical-shadow
computation using SwarmAgents.

A classical-shadow computation can be divided into independent batches.
Each batch processes a subset of the available measurement data and
produces partial estimates of the requested observables.

The results of all batches are subsequently combined by an aggregation
job.

This decomposition is particularly suitable for distributed execution
because the batch jobs do not depend on one another.

Job types
---------

1. classical_shadow_batch

   Processes one batch of measurement data.

   Multiple instances of this job can execute concurrently on
   different SwarmAgents workers.

2. classical_shadow_aggregate

   Collects the results produced by all batch jobs and calculates
   the final observable estimates.

   This job must execute only after all required batch jobs have
   completed successfully.

Important design considerations
--------------------------------

- Job construction is independent of job submission.
- No direct Redis or scheduler interaction takes place here.
- Dataset references must identify data accessible to the selected
  worker, either through shared storage or a data-transfer mechanism.
- Resource requirements are expressed using the capacity terminology
  expected by the SwarmAgents scheduling infrastructure.

The dictionaries produced here are workload-level specifications.
They may need an adapter before being converted into SwarmAgents'
actual Job objects.

Author: Vincenzo De Maio (v.demaio@leicester.ac.uk)
"""

from typing import Any, Dict, List


def create_shadow_batch_jobs(
    workflow_id: str,
    dataset_ref: str,
    num_batches: int,
    shots_per_batch: int,
    observables: List[Any],
    cpu: float = 1.0,
    ram: float = 2.0,
) -> List[Dict[str, Any]]:
    """
    Construct independent classical-shadow batch jobs.

    Each generated job represents one unit of classical-shadow
    post-processing.

    Parameters
    ----------
    workflow_id : str
        Unique identifier of the complete classical-shadow workflow.

        This identifier allows jobs and their results to be associated
        with the same distributed computation.

    dataset_ref : str
        Reference identifying the measurement dataset.

        This should not necessarily be a local filesystem path.
        In a distributed environment, the dataset must be accessible
        to whichever worker receives the job.

    num_batches : int
        Number of independent batches into which the computation
        should be divided.

    shots_per_batch : int
        Number of measurement shots assigned to each batch.

    observables : list
        Observables whose expectation values should be estimated.

        The representation of an observable depends on the classical-
        shadow algorithm implementation.

    cpu : float
        Estimated CPU capacity required by each batch job.

    ram : float
        Estimated memory capacity required by each batch job.

    Returns
    -------
    list of dict
        A list containing one job specification per batch.

    Notes
    -----
    The batch identifiers are deterministic within a workflow.
    This makes it easier to associate results with their original
    jobs and to handle retries.

    The actual execution of these jobs is handled elsewhere.
    """

    # Validate the basic workflow configuration before constructing
    # any jobs. Invalid configurations should fail early rather than
    # produce jobs that will eventually fail on remote workers.
    if not workflow_id:
        raise ValueError("workflow_id cannot be empty")

    if not dataset_ref:
        raise ValueError("dataset_ref cannot be empty")

    if num_batches < 1:
        raise ValueError("num_batches must be >= 1")

    if shots_per_batch < 1:
        raise ValueError("shots_per_batch must be >= 1")

    if not observables:
        raise ValueError("At least one observable must be provided")

    if cpu <= 0 or ram <= 0:
        raise ValueError("CPU and RAM requirements must be positive")

    jobs = []

    # Every batch is independent and can therefore be scheduled
    # on a different agent.
    for batch_id in range(num_batches):

        job = {
            # Unique identifier for this batch within the workflow.
            "id": f"{workflow_id}-batch-{batch_id}",

            # Job type used by the workload execution adapter.
            "job_type": "classical_shadow_batch",

            # Workflow metadata allows results to be grouped later.
            "workflow_id": workflow_id,
            "batch_id": batch_id,

            # Reference to the input measurement dataset.
            "dataset_ref": dataset_ref,

            # Number of measurement shots processed by this job.
            "shots_per_batch": shots_per_batch,

            # Observables to estimate from the measurement data.
            "observables": observables,

            # Estimated resource requirements.
            #
            # These fields should be mapped to the exact resource
            # representation expected by the SwarmAgents Job model.
            #
            # GPU demand is zero because this stage is intended to
            # perform classical post-processing.
            "capacities": {
                "core": cpu,
                "ram": ram,
                "disk": 1.0,
                "gpu": 0,
            },

            # Initial wall-time estimate.
            #
            # This should eventually be calibrated using measurements
            # from actual executions rather than treated as a constant.
            "wall_time": 1.0,

            # Placeholder for execution metadata and result information.
            "state_data": {},
        }

        jobs.append(job)

    return jobs


def create_shadow_aggregation_job(
    workflow_id: str,
    batch_job_ids: List[str],
    observables: List[Any],
    cpu: float = 1.0,
    ram: float = 2.0,
) -> Dict[str, Any]:
    """
    Construct the aggregation job for a classical-shadow workflow.

    The aggregation job consumes the partial estimates generated by
    all batch jobs and calculates the final estimates.

    Parameters
    ----------
    workflow_id : str
        Identifier of the workflow to which this aggregation belongs.

    batch_job_ids : list of str
        Identifiers of all batch jobs whose results are required.

    observables : list
        Observables included in the final estimation.

    cpu : float
        Estimated CPU capacity required for aggregation.

    ram : float
        Estimated memory capacity required for aggregation.

    Returns
    -------
    dict
        Aggregation job specification.

    Notes
    -----
    The presence of batch_job_ids describes the dependency relation.
    It does not enforce that dependency.

    The workflow orchestration layer must ensure that aggregation
    is submitted or executed only after all required batch results
    are available.
    """

    if not workflow_id:
        raise ValueError("workflow_id cannot be empty")

    if not batch_job_ids:
        raise ValueError("At least one batch job is required")

    if len(batch_job_ids) != len(set(batch_job_ids)):
        raise ValueError("Duplicate batch job identifiers")

    if not observables:
        raise ValueError("At least one observable must be provided")

    if cpu <= 0 or ram <= 0:
        raise ValueError("CPU and RAM requirements must be positive")

    return {
        # Unique identifier for the aggregation stage.
        "id": f"{workflow_id}-aggregate",

        # Job type identifying this as a result aggregation task.
        "job_type": "classical_shadow_aggregate",

        "workflow_id": workflow_id,

        # The aggregation stage needs the results from every batch.
        "batch_job_ids": batch_job_ids,

        # The expected observable set.
        "observables": observables,

        # Resource requirements for the aggregation stage.
        "capacities": {
            "core": cpu,
            "ram": ram,
            "disk": 1.0,
            "gpu": 0,
        },

        "wall_time": 1.0,

        "state_data": {},
    }