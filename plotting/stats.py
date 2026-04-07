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
    """Calculate mean, returning 0 if all values are NaN."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = series.mean()
        return result if not pd.isna(result) else 0.0


def safe_median(series: pd.Series) -> float:
    """Calculate median, returning 0 if all values are NaN."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = series.median()
        return result if not pd.isna(result) else 0.0


def safe_quantile(series: pd.Series, q: float) -> float:
    """Calculate quantile, returning 0 if all values are NaN."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = series.quantile(q)
        return result if not pd.isna(result) else 0.0


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
