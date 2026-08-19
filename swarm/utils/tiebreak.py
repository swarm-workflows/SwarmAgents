"""Deterministic, id-neutral tie-breaking for job placement.

Selection and consensus both need a total order over agents so that every agent, looking at
the same costs, names the same winner. That order used to be the agent id itself — lowest id
wins a tie — on the assumption that exact ties are rare. They are not: in a 30-agent LLM run,
**59% of all bids were the identical value 75.00**, because a small model asked for a 0-100
score answers in round steps. Whole cost columns therefore tied, agent 1 won them, and
placement stopped tracking cost at all: agents 1-10 took half of every workload, and inverting
the LLM's reasoning (chaos scenario S09) changed the bids without changing the schedule.

This module keeps the determinism and drops the bias. `tiebreak_rank` is a fixed pseudorandom
permutation of agents *per object*: still identical on every agent, but uncorrelated with the
id, so tied jobs spread across the fleet instead of piling onto the lowest ids.

Not the built-in `hash()`: on a str it is salted per process (PYTHONHASHSEED), so two agents
would rank the same tie differently and the cluster would disagree about who won.

Not `zlib.crc32` either, though it is stable. CRC32 is linear and mixes short, near-identical
keys poorly, and the rank is consumed as a *minimum over every agent* — which concentrates
exactly the structure a CRC leaves behind. Measured over 3000 jobs and 30 agents it gave a 4x
spread between the luckiest and unluckiest agent (166 wins vs 42, against a fair 100), with
two-digit ids visibly favoured. blake2b costs a microsecond and leaves no such pattern;
`tests/test_tiebreak.py` holds the line.
"""
from __future__ import annotations

import hashlib
from functools import lru_cache
from typing import Any, Optional


@lru_cache(maxsize=1 << 16)
def tiebreak_rank(object_id: Any, agent_id: Any) -> int:
    """Rank an agent's claim on one object. Lower wins; identical on every agent.

    Keyed on the object as well as the agent, so no agent is globally lucky — it wins some ties
    and loses others, which is what spreads load. Cached because selection re-ranks the same
    (job, agent) pairs on every pass of the scheduling loop.
    """
    key = f"{object_id}\x00{agent_id}".encode()
    return int.from_bytes(hashlib.blake2b(key, digest_size=8).digest(), "big")


def dominates(object_id: Any, cost_a: Optional[float], agent_a: Any,
              cost_b: Optional[float], agent_b: Any) -> bool:
    """True if (cost_a, agent_a) beats (cost_b, agent_b) for `object_id`.

    Cheaper wins; exact ties fall to the lower `tiebreak_rank`. A `None` cost means "cannot
    evaluate" and loses to any real cost, so a peer that cannot price a job yields rather
    than claiming it.
    """
    if cost_a is None:
        return False
    if cost_b is None:
        return True
    if cost_a != cost_b:
        return cost_a < cost_b
    return tiebreak_rank(object_id, agent_a) < tiebreak_rank(object_id, agent_b)
