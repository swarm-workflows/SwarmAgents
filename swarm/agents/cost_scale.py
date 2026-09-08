"""One cost scale for everything that crosses the wire.

Agents run different decision planes, and their costs are not on the same scale:

* the **analytic** model returns a weighted utilisation plus penalties — roughly 0..1 for an
  idle agent, a few units under load;
* the **LLM** plane returns ``100 - score`` for a 0..100 score, so 25..75 in practice.

Both used to be advertised raw. A peer answering a Snow query therefore compared its own 0.5
against an LLM initiator's 45, concluded it dominated, and voted for itself — every time,
regardless of which agent was actually the better host. The dominance rule degenerated, and the
reasoning the fleet spent seconds per bid producing lost to a model it was meant to replace
(chaos finding 11).

The same mismatch has a second edge that nobody has measured. When an LLM bid fails, the agent
falls back to the analytic model; advertised raw, that fallback bid is ~0.5 against healthy peers'
25..75, so a *broken* agent looks like the best host in the fleet. The campaign attributed that
capture entirely to the fallback being fast (S05: a 503 becomes an instant bid that out-races real
reasoning). Scale is an independent second channel, and it is removed here.

So: **native units stay inside selection, and everything that leaves an agent is canonical.**
Selection is untouched on purpose — `selection_threshold_pct` is a *relative* window ("within
+10% of best"), so pushing a non-linear transform into the selector would change which candidates
fall inside it and silently move every analytic result.

`to_canonical` is strictly increasing on each scale, so it reorders nothing within a decision
plane; it only makes two planes commensurable.
"""
from __future__ import annotations

from typing import Final


class CostScale:
    """The decision plane a native cost came from."""

    ANALYTIC: Final[str] = "analytic"
    LLM: Final[str] = "llm"


#: Top of the canonical range. 100 because the LLM plane already speaks in 0..100, so its costs
#: pass through unchanged and its logs stay readable against the pre-canonical campaign.
CANONICAL_MAX: Final[float] = 100.0

#: Analytic cost that maps to the canonical midpoint. The analytic model is unbounded above, so
#: it needs a reference point rather than a linear rescale; 1.0 sits at "fully utilised, no
#: penalties", which is the natural middle of the range that model produces.
ANALYTIC_HALF: Final[float] = 1.0


def to_canonical(cost: float, scale: str, analytic_half: float = ANALYTIC_HALF) -> float:
    """Map a native cost onto the canonical 0..`CANONICAL_MAX` wire scale. Lower is better.

    Strictly increasing in `cost` on both scales, so the ordering of two costs from the *same*
    plane is preserved exactly and only cross-plane comparisons change.

    :param cost: native cost, as the agent's own decision plane produced it.
    :param scale: which plane produced it — a `CostScale` value.
    :param analytic_half: analytic cost mapped to the midpoint; ignored for the LLM scale.
    """
    c = float(cost)
    if c != c:                                  # NaN — no opinion is safer than a wrong one
        raise ValueError("cost is NaN")
    if c == float("inf"):
        return CANONICAL_MAX
    c = max(0.0, c)
    if scale == CostScale.LLM:
        # Already 0..100 by construction (100 - score, score clamped to 0..100).
        return min(c, CANONICAL_MAX)
    if scale == CostScale.ANALYTIC:
        # Unbounded above; a monotone squash keeps the ordering and bounds the range.
        half = float(analytic_half) if analytic_half and analytic_half > 0 else ANALYTIC_HALF
        return CANONICAL_MAX * c / (c + half)
    raise ValueError(f"unknown cost scale {scale!r}")
