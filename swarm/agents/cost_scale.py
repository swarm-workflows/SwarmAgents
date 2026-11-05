"""Which decision plane produced a cost — and why no rescaling happens between them.

`CostScale` tags a cost with the plane that produced it. `LlmAgent` records the tag alongside
each verdict so instrumentation can separate a real LLM bid from an analytic fallback; nothing
transforms a cost on the way to the wire.

Why no transform
----------------
`docs/SWARMAGENTS_FINDINGS.md` finding 11 reports two problems with LLM costs under Snow:

1. peers answered a query with the **analytic** cost while proposing with the **LLM** cost, so
   the LLM's verdict never entered the protocol; and
2. "the two costs are not on the same scale. The analytic cost is roughly 0-1 (weighted
   utilisations plus penalties); the LLM cost is `100 - score`, so 25-75."

**The first is real and is fixed** (see `LlmAgent.native_cost_for_job`). **The second is not.**
`ResourceAgent.compute_job_cost` ends in `* 100`, so the analytic cost is already on the same
0..100 range as the LLM's. Measured over 2000 (job, flavour) pairs — 400 real Pegasus jobs
against the five shipped instance flavours, `analytic_cost_half`-free:

===========  ======  ======  ======  ======  ======  ======
statistic    p10     p25     p50     p75     p90     max
===========  ======  ======  ======  ======  ======  ======
cost          2.59    5.41   11.85   28.40   71.85  959.25
===========  ======  ======  ======  ======  ======  ======

against an LLM plane whose costs are `100 - score`, concentrated at 25 (score 75) and 5
(score 95). Comparable units, overlapping ranges. 1.6% of analytic costs exceed 100, and a
multiplicative load penalty (up to ~2x) can push either plane above it, which is harmless —
dominance is a relative comparison and needs no upper bound.

An earlier version of this module mapped each plane through `100*c/(c+half)` with `half` at 1.0
for the analytic plane. On the real distribution that sent a median analytic cost of 11.85 to
92.2 and made every analytic agent look nearly worthless — an inversion far worse than the one
it was meant to remove. Any per-plane rescaling has that hazard, because it is a claim about the
two distributions that has to be re-earned whenever either model changes. Comparing the raw
costs makes no such claim.

What is left, and is a real (smaller) effect: the two models *calibrate* differently. A fallback
analytic bid has a median of 11.85 while a typical LLM bid is 25, so an agent whose LLM is down
does bid lower on average — roughly 2x, not the 50x finding 11 implies. That is a difference in
how two models rate the same job, not a units error, and rescaling it away would be putting a
thumb on the scale. It belongs in E4 as a measurement: report bid distributions per plane.

`tests/test_cost_scale.py` pins the assumption that keeps this true — that both planes emit
0..100 — so a change to either model fails a test instead of silently reintroducing the mismatch.
"""
from __future__ import annotations

from typing import Final


class CostScale:
    """The decision plane a cost came from. A tag for instrumentation, not a conversion."""

    ANALYTIC: Final[str] = "analytic"
    LLM: Final[str] = "llm"


#: Nominal top of the range both planes emit. Costs may exceed it (a long analytic tail, or
#: either plane after the multiplicative load penalty) and are NOT clamped: dominance is a
#: relative comparison, and clamping would map every loaded agent to the same value.
NOMINAL_MAX: Final[float] = 100.0
