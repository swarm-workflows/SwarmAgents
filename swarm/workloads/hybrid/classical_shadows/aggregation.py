"""
Classical Shadows Result Aggregation
====================================

This module combines partial observable estimates produced by
independent classical-shadow batch jobs.

Each batch worker produces estimates for a collection of observables.
The aggregation stage combines these estimates into a final result.

Statistical assumptions
-----------------------

The implementation assumes:

1. Batch estimates are statistically independent.

2. Each batch reports the variance of its estimated observable.

3. Each batch contributes a known number of measurement shots.

4. The estimates are unbiased estimates of the same quantity.

For batch i, let:

    n_i = number of shots in batch i
    mu_i = estimated observable expectation value
    v_i = variance of the batch estimator

The total number of shots is:

    N = sum_i n_i

The combined estimate is:

    mu = sum_i (n_i / N) * mu_i

Assuming independence, its variance is:

    v = sum_i (n_i / N)^2 * v_i

The standard error is:

    SE = sqrt(v)

These formulas should be revisited if the underlying estimator
uses correlated samples, adaptive measurement allocation, or
non-independent batches.
"""


from typing import Any, Dict, List
import math


def aggregate_shadow_results(
    batch_results: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Aggregate partial estimates from multiple shadow batches.

    Parameters
    ----------
    batch_results : list of dict
        Results returned by execute_shadow_batch().

    Returns
    -------
    dict
        Final aggregated estimates and workflow metadata.

    Raises
    ------
    ValueError
        If results are missing, inconsistent, duplicated, or invalid.
    """

    if not batch_results:
        raise ValueError("No batch results provided")

    # All batch results must belong to the same workflow.
    workflow_ids = {
        result["workflow_id"]
        for result in batch_results
    }

    if len(workflow_ids) != 1:
        raise ValueError("Results belong to different workflows")

    # Ensure that no batch result has been included more than once.
    #
    # Duplicate results can occur when distributed jobs are retried.
    # Aggregation should not silently count the same batch twice.
    batch_ids = [
        result["batch_id"]
        for result in batch_results
    ]

    if len(batch_ids) != len(set(batch_ids)):
        raise ValueError("Duplicate batch results detected")

    # Determine the total number of measurement shots represented
    # by the supplied batch results.
    total_shots = sum(
        int(result["shots"])
        for result in batch_results
    )

    if total_shots <= 0:
        raise ValueError("Total number of shots must be positive")

    # All batches must estimate the same set of observables.
    observable_names = set(
        batch_results[0]["estimates"].keys()
    )

    for result in batch_results:

        if int(result["shots"]) <= 0:
            raise ValueError("Batch shot count must be positive")

        if set(result["estimates"].keys()) != observable_names:
            raise ValueError(
                "Inconsistent observable sets across batches"
            )

    final_estimates = {}

    # Process each observable independently.
    for observable in observable_names:

        weighted_mean = 0.0
        combined_variance = 0.0

        # Combine the contribution of each independent batch.
        for result in batch_results:

            shots = int(result["shots"])

            # Fraction of the total measurement shots represented
            # by this particular batch.
            weight = shots / total_shots

            estimate = result["estimates"][observable]

            mean = float(estimate["mean"])
            variance = float(estimate["variance"])

            # Validate the statistical quantities.
            if not math.isfinite(mean):
                raise ValueError("Non-finite observable estimate")

            if not math.isfinite(variance) or variance < 0:
                raise ValueError("Invalid estimator variance")

            # Weighted contribution to the final expectation value.
            weighted_mean += weight * mean

            # Under the independence assumption, variances combine
            # using the square of the corresponding weights.
            combined_variance += weight**2 * variance

        # Store the final estimate and its uncertainty.
        final_estimates[observable] = {
            "mean": weighted_mean,
            "variance": combined_variance,
            "standard_error": math.sqrt(combined_variance),
        }

    return {
        "workflow_id": next(iter(workflow_ids)),
        "num_batches": len(batch_results),
        "total_shots": total_shots,
        "estimates": final_estimates,
    }