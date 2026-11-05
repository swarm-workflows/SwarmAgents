"""
Tests for classical-shadow result aggregation.

These tests verify the numerical correctness of the aggregation
procedure and check that inconsistent or invalid batch results
are rejected.

The tests use analytically known values so that the expected
weighted means and variances can be calculated independently.
"""

import pytest

from swarm.workloads.hybrid.classical_shadows.aggregation import (
    aggregate_shadow_results,
)


@pytest.fixture
def batch_results():
    """
    Provide two independent batch results for one observable.

    Both batches contain 100 shots.

    Their estimates are:
        Batch 0: mean = 0.4, variance = 0.04
        Batch 1: mean = 0.6, variance = 0.04

    The expected combined mean is 0.5.

    The expected combined variance, assuming independence, is 0.02.
    """

    return [
        {
            "workflow_id": "wf-1",
            "batch_id": 0,
            "shots": 100,
            "estimates": {
                "Z0": {"mean": 0.4, "variance": 0.04},
            },
        },
        {
            "workflow_id": "wf-1",
            "batch_id": 1,
            "shots": 100,
            "estimates": {
                "Z0": {"mean": 0.6, "variance": 0.04},
            },
        },
    ]


def test_aggregate_single_batch():
    """
    Verify that aggregation works when the workflow contains
    only one completed batch.

    In this case, the final mean and variance must be identical
    to those reported by the original batch.

    This checks that the aggregation function handles the
    simplest possible input without changing its result.
    """

    result = aggregate_shadow_results([
        {
            "workflow_id": "wf-1",
            "batch_id": 0,
            "shots": 100,
            "estimates": {
                "Z0": {"mean": 0.4, "variance": 0.04},
            },
        }
    ])

    assert result["estimates"]["Z0"]["mean"] == pytest.approx(0.4)
    assert result["estimates"]["Z0"]["variance"] == pytest.approx(0.04)


def test_aggregate_equal_batches(batch_results):
    """
    Verify the weighted mean for two batches with equal shot counts.

    Since both batches contain the same number of shots, their
    estimates contribute equally to the final mean:

        mean = (0.4 + 0.6) / 2 = 0.5

    The total shot count must also be reported correctly.
    """

    result = aggregate_shadow_results(batch_results)

    assert result["estimates"]["Z0"]["mean"] == pytest.approx(0.5)
    assert result["total_shots"] == 200


def test_aggregate_variance(batch_results):
    """
    Verify the variance propagation formula for independent batches.

    For equal weights of 0.5 and batch variances of 0.04:

        combined_variance =
            (0.5 ** 2 * 0.04) + (0.5 ** 2 * 0.04)
            = 0.02

    The standard error must be the square root of this variance.

    This test is particularly important because an incorrect
    uncertainty calculation could make the final scientific
    results misleading.
    """

    result = aggregate_shadow_results(batch_results)

    assert result["estimates"]["Z0"]["variance"] == pytest.approx(0.02)

    assert result["estimates"]["Z0"]["standard_error"] == pytest.approx(
        0.02 ** 0.5
    )


def test_aggregate_unequal_shots():
    """
    Verify that batches with different shot counts receive
    different weights.

    The first batch contains 100 shots and estimates 0.2.
    The second batch contains 300 shots and estimates 0.6.

    The correct combined mean is:

        (100 / 400) * 0.2 + (300 / 400) * 0.6 = 0.5

    This test ensures that aggregation does not simply calculate
    an unweighted average of the batch means.
    """

    results = [
        {
            "workflow_id": "wf-1",
            "batch_id": 0,
            "shots": 100,
            "estimates": {
                "Z0": {"mean": 0.2, "variance": 0.04},
            },
        },
        {
            "workflow_id": "wf-1",
            "batch_id": 1,
            "shots": 300,
            "estimates": {
                "Z0": {"mean": 0.6, "variance": 0.02},
            },
        },
    ]

    result = aggregate_shadow_results(results)

    assert result["estimates"]["Z0"]["mean"] == pytest.approx(0.5)


def test_reject_empty_results():
    """
    Verify that aggregation rejects an empty collection of results.

    Without any completed batches, no final estimate can be produced.

    This is also relevant to distributed execution, where a workflow
    may fail before any worker has completed successfully.
    """

    with pytest.raises(ValueError):
        aggregate_shadow_results([])


def test_reject_different_workflows(batch_results):
    """
    Verify that results belonging to different workflows cannot
    be combined.

    This prevents accidental mixing of independent computations,
    which could otherwise produce plausible-looking but meaningless
    estimates.
    """

    batch_results[1]["workflow_id"] = "wf-2"

    with pytest.raises(ValueError):
        aggregate_shadow_results(batch_results)


def test_reject_duplicate_batches(batch_results):
    """
    Verify that duplicate batch identifiers are rejected.

    A distributed worker may execute a job more than once because
    of retries or communication failures.

    Aggregation must not silently count the same batch result twice.
    """

    batch_results[1]["batch_id"] = 0

    with pytest.raises(ValueError):
        aggregate_shadow_results(batch_results)


def test_reject_inconsistent_observables(batch_results):
    """
    Verify that every batch reports the same observable set.

    If one batch estimates Z0 while another estimates Z1, their
    results cannot be combined as though they described the same
    collection of requested observables.
    """

    batch_results[1]["estimates"] = {
        "Z1": {"mean": 0.5, "variance": 0.02},
    }

    with pytest.raises(ValueError):
        aggregate_shadow_results(batch_results)


def test_reject_negative_variance(batch_results):
    """
    Verify that negative estimator variance is rejected.

    Variance is mathematically non-negative.

    A negative value indicates an invalid result from the underlying
    algorithm or a problem in the result serialization process.
    """

    batch_results[0]["estimates"]["Z0"]["variance"] = -0.1

    with pytest.raises(ValueError):
        aggregate_shadow_results(batch_results)


def test_reject_nan_estimate(batch_results):
    """
    Verify that non-finite observable estimates are rejected.

    NaN values can propagate through numerical aggregation and
    produce invalid final results without necessarily raising
    an exception.

    The aggregation stage should therefore explicitly reject
    non-finite numerical estimates.
    """

    batch_results[0]["estimates"]["Z0"]["mean"] = float("nan")

    with pytest.raises(ValueError):
        aggregate_shadow_results(batch_results)