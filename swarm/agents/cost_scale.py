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


#: Top of the canonical range.
CANONICAL_MAX: Final[float] = 100.0

#: Native cost that maps to the canonical midpoint, per plane. This is the whole calibration:
#: each plane names the cost it considers middling, and the same curve does the rest.
#:
#: * analytic — 1.0 is "fully utilised, no penalties", the natural middle of what that model
#:   produces (it is unbounded above, so it needs a reference rather than a rescale).
#: * llm — 50 is the midpoint of the 0..100 the plane is asked for (``100 - score``).
ANALYTIC_HALF: Final[float] = 1.0
LLM_HALF: Final[float] = 50.0


def _half_for(scale: str, analytic_half: float, llm_half: float) -> float:
    if scale == CostScale.ANALYTIC:
        half = analytic_half if analytic_half and analytic_half > 0 else ANALYTIC_HALF
    elif scale == CostScale.LLM:
        half = llm_half if llm_half and llm_half > 0 else LLM_HALF
    else:
        raise ValueError(f"unknown cost scale {scale!r}")
    return float(half)


def to_canonical(cost: float, scale: str,
                 analytic_half: float = ANALYTIC_HALF,
                 llm_half: float = LLM_HALF) -> float:
    """Map a native cost onto the canonical 0..`CANONICAL_MAX` wire scale. Lower is better.

    One curve for both planes — ``CANONICAL_MAX * c / (c + half)`` — differing only in the
    native cost each calls middling. So a plane's reference cost always lands on 50, and half
    of it lands on 33.3, which is what makes the two commensurable rather than merely bounded.

    Strictly increasing in `cost` on every plane and over the whole non-negative range, so the
    ordering of two costs from the *same* plane is preserved exactly and only cross-plane
    comparisons change.

    The LLM branch is a squash rather than a pass-through for a reason that is easy to miss:
    the cost reaching this function is the cost *after* selection's multiplicative load penalty
    (up to ~2x), so an LLM bid of 75 arrives as 150. Clamping at `CANONICAL_MAX` mapped every
    loaded agent to the same 100 and destroyed exactly the ordering this scale exists to keep.

    :param cost: native cost, as the agent's own decision plane produced it.
    :param scale: which plane produced it — a `CostScale` value.
    :param analytic_half: analytic cost mapped to the midpoint.
    :param llm_half: LLM cost mapped to the midpoint.
    """
    c = float(cost)
    if c != c:                                  # NaN — no opinion is safer than a wrong one
        raise ValueError("cost is NaN")
    half = _half_for(scale, analytic_half, llm_half)
    if c == float("inf"):
        return CANONICAL_MAX
    c = max(0.0, c)
    return CANONICAL_MAX * c / (c + half)
