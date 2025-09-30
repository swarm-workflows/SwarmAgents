# MIT License
#
# Copyright (c) 2024 swarm-workflows

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Author: Komal Thareja(kthare10@renci.org)
"""
Live (non-cached) penalty helpers for selection.

These utilities adjust a precomputed cost/score matrix *without* touching the
engine's internal caches. Use them for fast-changing signals (e.g., load,
queue length, affinity) that you don’t want cached.
"""

from __future__ import annotations
from typing import Sequence, Callable, Any
import numpy as np

Assignee = Any


def apply_multiplicative_penalty(
    cost_matrix: np.ndarray,
    assignees: Sequence[Assignee],
    *,
    factor_fn: Callable[[Assignee], float],
) -> np.ndarray:
    """
    Multiply each row in the cost matrix by a live, per-assignee factor.

    This does **not** cache anything. Intended to run immediately after the
    cached base matrix is built, so the final adjusted matrix reflects the
    latest signals (e.g., load, queue time).

    :param cost_matrix: Base matrix with shape (len(assignees), num_candidates).
    :param assignees: Row-aligned sequence of assignees.
    :param factor_fn: Callable returning a multiplicative factor (>= 0.0) for each assignee.
                      For example: 1.0 = no penalty, >1.0 = penalize, <1.0 = boost.
    :returns: A **new** adjusted matrix; the input is not modified.
    """
    if cost_matrix.size == 0:
        return cost_matrix
    factors = np.array([float(factor_fn(a)) for a in assignees], dtype=float).reshape(-1, 1)
    # Safety: replace NaN/inf with neutral factor 1.0
    factors[~np.isfinite(factors)] = 1.0
    return cost_matrix * factors
