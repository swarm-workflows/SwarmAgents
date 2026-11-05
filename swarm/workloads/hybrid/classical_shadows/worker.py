"""
Classical Shadows Batch Worker
==============================

This module implements the execution adapter for classical-shadow
batch jobs.

A batch worker receives a job specification, extracts its parameters,
invokes the classical-shadow algorithm, and returns a structured
result.

The worker is responsible for connecting SwarmAgents job execution
with the underlying scientific computation.

Algorithm separation
--------------------

The actual classical-shadow algorithm is supplied as a callable.

This is intentional.

SwarmAgents should not need to know whether the underlying algorithm
comes from:

- KrMM-shadows;
- another Python package;
- a local implementation;
- a remote computational service.

As long as the implementation follows the expected interface,
the same worker can invoke it.

Expected algorithm interface
----------------------------

The algorithm callable must accept:

    dataset_ref
    batch_id
    shots
    observables

It must return a dictionary containing an "estimates" field.

Expected result format
---------------------

{
    "estimates": {
        "Z0": {
            "mean": 0.42,
            "variance": 0.03
        }
    }
}

The variance must describe the variance of the batch estimator,
not merely the variance of individual measurement outcomes.
"""


from typing import Any, Callable, Dict


def execute_shadow_batch(
    job: Dict[str, Any],
    algorithm: Callable[..., Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Execute one classical-shadow batch job.

    Parameters
    ----------
    job : dict
        Job specification produced by jobs.py.

    algorithm : callable
        Classical-shadow implementation responsible for processing
        the measurement data and estimating the requested observables.

    Returns
    -------
    dict
        Structured result containing workflow metadata, batch metadata,
        shot count, and partial observable estimates.

    Raises
    ------
    ValueError
        If the job type or required parameters are invalid.

    TypeError
        If the algorithm returns an invalid result structure.

    Notes
    -----
    This function currently performs the computation synchronously.

    Integration with SwarmAgents requires calling this function from
    the actual worker execution path.
    """

    # Ensure that the worker receives a job type it knows how to execute.
    if job.get("job_type") != "classical_shadow_batch":
        raise ValueError("Unsupported job type")

    # Verify that all mandatory parameters are available.
    required_fields = [
        "workflow_id",
        "batch_id",
        "dataset_ref",
        "shots_per_batch",
        "observables",
    ]

    for field in required_fields:
        if field not in job:
            raise ValueError(f"Missing job field: {field}")

    # Extract job parameters.
    #
    # Keeping this extraction explicit makes the interface between
    # SwarmAgents and the algorithm implementation easy to inspect.
    workflow_id = job["workflow_id"]
    batch_id = job["batch_id"]
    dataset_ref = job["dataset_ref"]
    shots = job["shots_per_batch"]
    observables = job["observables"]

    if shots <= 0:
        raise ValueError("shots_per_batch must be positive")

    # Invoke the actual classical-shadow implementation.
    #
    # At this point, the algorithm is responsible for loading or
    # accessing the dataset and processing the assigned batch.
    algorithm_result = algorithm(
        dataset_ref=dataset_ref,
        batch_id=batch_id,
        shots=shots,
        observables=observables,
    )

    # Check the result type before attempting to access its contents.
    if not isinstance(algorithm_result, dict):
        raise TypeError("Algorithm must return a dictionary")

    if "estimates" not in algorithm_result:
        raise ValueError("Algorithm result must contain 'estimates'")

    estimates = algorithm_result["estimates"]

    if not isinstance(estimates, dict):
        raise TypeError("'estimates' must be a dictionary")

    # Return a normalized result.
    #
    # The output contains enough metadata to associate the result
    # with its originating workflow and batch.
    return {
        "workflow_id": workflow_id,
        "batch_id": batch_id,
        "shots": shots,
        "estimates": estimates,
    }