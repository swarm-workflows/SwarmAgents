"""Shared statistics helpers used across plotting modules."""

import warnings

import numpy as np
import pandas as pd


def jains_fairness(values: np.ndarray) -> float:
    """Compute Jain's Fairness Index for a set of values."""
    n = len(values)
    if n == 0:
        return 0.0
    s = np.sum(values)
    ss = np.sum(values ** 2)
    if ss == 0:
        return 1.0
    return (s ** 2) / (n * ss)


def safe_mean(series: pd.Series) -> float:
    """Calculate mean without warnings; NaN if there are no values.

    It returned 0.0 for an all-NaN series, so a run that assigned nothing reported a 0 s
    selection time — a livelocked cell read as the fastest one (code review 2026-10-05 §43).
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return float(series.mean())


def safe_median(series: pd.Series) -> float:
    """Calculate median without warnings; NaN if there are no values.

    It returned 0.0 for an all-NaN series, so a run that assigned nothing reported a 0 s
    selection time — a livelocked cell read as the fastest one (code review 2026-10-05 §43).
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return float(series.median())


def safe_quantile(series: pd.Series, q: float) -> float:
    """Calculate a quantile without warnings; NaN if there are no values (see safe_mean)."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return float(series.quantile(q))


def safe_sum(series: pd.Series) -> float:
    """Calculate sum, returning 0 if all values are NaN."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = series.sum()
        return result if not pd.isna(result) else 0.0


def calculate_entropy(series: pd.Series) -> float:
    """Calculate Shannon entropy for load distribution."""
    value_counts = series.value_counts()
    probabilities = value_counts / len(series)
    return -np.sum(probabilities * np.log2(probabilities + 1e-10))


def boxplot(ax, data, labels, **kwargs):
    """`ax.boxplot` with tick labels on any matplotlib.

    The keyword was renamed `labels` -> `tick_labels` in 3.9 and the old name removed in 3.11,
    which is what the database node runs: every single-run plotting step raised TypeError
    there (2026-10-07). One shim instead of a version pin, so the laptop (3.10) and the slice
    (3.11) both draw the same figure.
    """
    import inspect
    if "tick_labels" in inspect.signature(ax.boxplot).parameters:
        return ax.boxplot(data, tick_labels=labels, **kwargs)
    return ax.boxplot(data, labels=labels, **kwargs)
