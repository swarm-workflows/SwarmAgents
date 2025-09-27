# swarm/selection/engine.py
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
Generic selection engine with cost matrix support, caching, and configurable
decision policies.

This module provides a reusable framework for assigning *candidates* (e.g.,
jobs, flows, tickets) to *assignees* (e.g., agents, servers, resources) based
on feasibility and cost functions.
"""

from __future__ import annotations
from typing import Callable, Sequence, Hashable, Protocol, Any, Literal
import time
import numpy as np
from collections import OrderedDict

Candidate = Any
Assignee  = Any
Objective = Literal["min", "max"]


def _argbest(values: np.ndarray, objective: Objective) -> int:
    """
    Return the index of the best value in an array according to the objective.

    :param values: Numpy array of values to evaluate.
    :param objective: Either ``"min"`` (choose smallest value) or ``"max"`` (choose largest).
    :returns: Index of the best value.
    """
    return int(np.argmin(values)) if objective == "min" else int(np.argmax(values))


def _best(values: np.ndarray, objective: Objective) -> float:
    """
    Return the best value in an array according to the objective.

    :param values: Numpy array of values to evaluate.
    :param objective: Either ``"min"`` or ``"max"``.
    :returns: Best value (minimum or maximum).
    """
    return float(np.min(values)) if objective == "min" else float(np.max(values))


class FeasibleFn(Protocol):
    """Callable that determines if a candidate can be assigned to an assignee."""

    def __call__(self, cand: Candidate, asg: Assignee) -> bool: ...


class CostFn(Protocol):
    """Callable that computes the cost (or utility) of a candidate–assignee assignment."""

    def __call__(self, cand: Candidate, asg: Assignee) -> float: ...


KeyFn = Callable[[Any], Hashable]
VersionFn = Callable[[Any], int]


class _LRU:
    """
    Tiny least-recently-used cache with optional TTL.

    :param maxsize: Maximum number of entries to keep in cache.
    :param ttl_s: Optional time-to-live in seconds; expired entries are evicted on access.
    """

    def __init__(self, maxsize: int = 65536, ttl_s: float | None = None) -> None:
        self.maxsize = maxsize
        self.ttl_s = ttl_s
        self._d: OrderedDict[tuple, tuple[float, Any]] = OrderedDict()

    def get(self, key: tuple) -> Any | None:
        """
        Retrieve a cached value if present and not expired.

        :param key: Cache key tuple.
        :returns: Cached value or ``None`` if not present or expired.
        """
        now = time.time()
        item = self._d.get(key)
        if item is None:
            return None
        ts, val = item
        if self.ttl_s is not None and (now - ts) > self.ttl_s:
            try:
                del self._d[key]
            except KeyError:
                pass
            return None
        self._d.move_to_end(key, last=True)
        return val

    def set(self, key: tuple, val: Any) -> None:
        """
        Insert or update a cache entry.

        :param key: Cache key tuple.
        :param val: Value to store.
        """
        now = time.time()
        if key in self._d:
            self._d.move_to_end(key, last=True)
        self._d[key] = (now, val)
        if len(self._d) > self.maxsize:
            self._d.popitem(last=False)

    def clear(self) -> None:
        """Clear all entries."""
        self._d.clear()


class SelectionEngine:
    """
    Generic engine for candidate-to-assignee selection.

    :param feasible: Callable that decides if a candidate can be assigned to an assignee.
    :param cost: Callable that computes cost/utility for a candidate–assignee pair.
    :param candidate_key: Function returning a stable hashable key for a candidate.
    :param assignee_key: Function returning a stable hashable key for an assignee.
    :param candidate_version: Optional function returning a monotonically increasing version
        number for a candidate (used for cache invalidation).
    :param assignee_version: Optional function returning a monotonically increasing version
        number for an assignee (used for cache invalidation).
    :param cache_enabled: If True, enable memoization for feasibility and cost calls.
    :param feas_cache_size: Maximum entries for feasibility cache.
    :param cost_cache_size: Maximum entries for cost cache.
    :param cache_ttl_s: Optional TTL for cached values.
    :param inf: Value used to represent infeasible assignments in the cost matrix.
    """

    def __init__(
        self,
        feasible: FeasibleFn,
        cost: CostFn,
        *,
        candidate_key: KeyFn,
        assignee_key: KeyFn,
        candidate_version: VersionFn | None = None,
        assignee_version: VersionFn | None = None,
        cache_enabled: bool = True,
        feas_cache_size: int = 131072,
        cost_cache_size: int = 131072,
        cache_ttl_s: float | None = None,
        inf: float = float("inf"),
    ) -> None:
        self.feasible = feasible
        self.cost = cost
        self.inf = inf

        self.candidate_key = candidate_key
        self.assignee_key = assignee_key
        self.candidate_version = candidate_version or (lambda _c: 0)
        self.assignee_version = assignee_version or (lambda _a: 0)

        self.cache_enabled = cache_enabled
        self._feas_cache = _LRU(maxsize=feas_cache_size, ttl_s=cache_ttl_s) if cache_enabled else None
        self._cost_cache = _LRU(maxsize=cost_cache_size, ttl_s=cache_ttl_s) if cache_enabled else None

    def clear_caches(self) -> None:
        """
        Clear all cached feasibility and cost results.

        Use this if global state changes (e.g., capacities or requirements)
        and versioning/TTL are not sufficient for invalidation.
        """
        if self._feas_cache: self._feas_cache.clear()
        if self._cost_cache: self._cost_cache.clear()

    def compute_cost_matrix(
        self,
        assignees: Sequence[Assignee],
        candidates: Sequence[Candidate],
    ) -> np.ndarray:
        """
        Compute a cost matrix of shape (len(assignees), len(candidates)).

        :param assignees: Sequence of assignees (rows).
        :param candidates: Sequence of candidates (columns).
        :returns: Numpy array with costs; infeasible pairs are filled with ``inf``.
        """
        A, C = len(assignees), len(candidates)
        M = np.full((A, C), self.inf, dtype=float)
        for ai, asg in enumerate(assignees):
            for ci, cand in enumerate(candidates):
                if self._memo_feasible(cand, asg):
                    M[ai, ci] = self._memo_cost(cand, asg)
        return M

    def pick_agent_per_candidate(
            self,
            assignees: Sequence[Assignee],
            candidates: Sequence[Candidate],
            cost_matrix: np.ndarray,
            *,
            objective: Objective = "min",
            threshold_pct: float | None = None,
            accept_if: Callable[[float], bool] | None = None,
            tie_break_key: Callable[[Assignee, float], Any] | None = None,
    ) -> list[tuple[Assignee | None, float]]:
        """
        Select the best assignee for each candidate (column) given a cost/score matrix.

        Robust to +inf (infeasible) entries:
          - For ``objective="min"``, ``np.argmin`` is safe directly on the column.
          - For ``objective="max"``, we restrict to finite entries to avoid picking +inf.

        :param assignees: Row sequence aligned with matrix rows.
        :param candidates: Column sequence aligned with matrix cols.
        :param cost_matrix: 2D array [rows=assignees, cols=candidates]; use +inf for infeasible.
        :param objective: ``"min"`` to minimize cost or ``"max"`` to maximize score.
        :param threshold_pct: Optional tolerance around the column-wise best:
            - For ``"min"``: keep only if selected <= best * (1 + pct/100).
            - For ``"max"``: keep only if selected >= best * (1 - pct/100).
            Note: if the selected index is the true best, this threshold never rejects it.
        :param accept_if: Optional final predicate on the selected score (e.g., ``lambda s: s < 1e9``).
        :param tie_break_key: Optional deterministic key for breaking exact-score ties.
            To match your old (cost, agent_id) behavior, pass:
            ``tie_break_key=lambda ag, s: getattr(ag, "agent_id", "")``.
        :returns: List of (assignee-or-None, score) with length == number of candidates.
        """
        A, C = cost_matrix.shape
        out: list[tuple[Assignee | None, float]] = []
        if A == 0 or C == 0:
            return [(None, float("inf")) for _ in range(C)]

        # Column-wise best values for threshold checks (min or max).
        # You *could* use _best on each column; vector ops are faster:
        per_cand_best = cost_matrix.min(axis=0) if objective == "min" else cost_matrix.max(axis=0)

        for ci in range(C):
            col = cost_matrix[:, ci]

            # Consider only finite entries. If none, keep alignment with a (None, inf) placeholder.
            finite_idx = np.where(np.isfinite(col))[0]
            if finite_idx.size == 0:
                out.append((None, float("inf")))
                continue

            # Pick the best index, guarding "max" against +inf
            if objective == "min":
                best_idx = int(np.argmin(col))  # +inf won't be chosen
            else:
                best_idx = int(finite_idx[np.argmax(col[finite_idx])])  # avoid +inf

            # Optional tie-break when multiple indices have exactly the same best value
            if tie_break_key is not None:
                best_val = col[best_idx]
                tied = [i for i in finite_idx if col[i] == best_val]
                if len(tied) > 1:
                    best_idx = min(tied, key=lambda i: tie_break_key(assignees[i], float(col[i])))

            sel_cost = float(col[best_idx])
            sel_agent = assignees[best_idx]

            # Threshold relative to the per-column best (objective-aware)
            if threshold_pct is not None and np.isfinite(sel_cost):
                best = float(per_cand_best[ci])
                if objective == "min":
                    if not np.isfinite(best) or sel_cost > best * (1.0 + threshold_pct / 100.0):
                        sel_agent, sel_cost = None, float("inf")
                else:
                    if not np.isfinite(best) or sel_cost < best * (1.0 - threshold_pct / 100.0):
                        sel_agent, sel_cost = None, float("inf")

            # Optional absolute acceptance gate
            if accept_if is not None and sel_agent is not None:
                if not accept_if(sel_cost):
                    sel_agent, sel_cost = None, float("inf")

            out.append((sel_agent, sel_cost))

        return out

    # ---- internal helpers for caching ----
    def _ck(self, cand: Candidate) -> Hashable: return self.candidate_key(cand)
    def _ak(self, asg: Assignee) -> Hashable: return self.assignee_key(asg)
    def _cv(self, cand: Candidate) -> int: return self.candidate_version(cand)
    def _av(self, asg: Assignee) -> int: return self.assignee_version(asg)

    def _memo_feasible(self, cand: Candidate, asg: Assignee) -> bool:
        """Memoized feasibility check."""
        if not self.cache_enabled or self._feas_cache is None:
            return self.feasible(cand, asg)
        key = (self._ak(asg), self._ck(cand), self._av(asg), self._cv(cand), "feas")
        hit = self._feas_cache.get(key)
        if hit is not None:
            return bool(hit)
        val = self.feasible(cand, asg)
        self._feas_cache.set(key, bool(val))
        return val

    def _memo_cost(self, cand: Candidate, asg: Assignee) -> float:
        """Memoized cost computation."""
        if not self.cache_enabled or self._cost_cache is None:
            return self.cost(cand, asg)
        key = (self._ak(asg), self._ck(cand), self._av(asg), self._cv(cand), "cost")
        hit = self._cost_cache.get(key)
        if hit is not None:
            return float(hit)
        val = self.cost(cand, asg)
        self._cost_cache.set(key, float(val))
        return val
