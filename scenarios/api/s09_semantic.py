#!/usr/bin/env python3.11
"""
S09 — SemanticCorrupt (SwarmAgents variant of Chaos Jungle's Layer-4 semantic fault)

What
    The LLM request is mutated on the way to the model — entities swapped, context
    truncated, a contradictory instruction appended, or a false fact injected — while
    HTTP and JSON stay intact. The call succeeds, the reply parses, and the agent's
    fallback never fires. Only the *content* of the bid is wrong.

Why
    S01 and S05 both land on the same safety net: any exception drops the agent onto the
    analytic cost model, so the swarm degrades instead of breaking. A semantic fault
    removes that net by construction — there is no exception to catch. Consensus itself
    has to absorb a corrupted cost signal, which makes this the tier that tests whether
    distribution is a defence or just a wider blast radius.

    Mode matters here in a way it does not for CJ's single-call reference scenarios,
    because the swarm's prompt is a scheduling prompt:
      entity_swap        flips "higher = better fit" to "lower = better fit" in the
                         system prompt — a poisoned agent inverts its own bid polarity
      rag_poison         injects a false context line into the JOB/AGENT/PEERS turn
      inject_distractor  appends a contradictory instruction to the system prompt
      context_truncate   cuts the JOB/AGENT payload in half — the agent bids on a job
                         it can only half see

How
    scenarios/api/s09_semantic.py [mode] [fraction] [suffix]
        mode     — entity_swap | rag_poison | inject_distractor | context_truncate
                   (default entity_swap)
        fraction — share of the 30 hosts to poison (default 1.0)
        suffix   — appended to the run dir, so a repeat under changed code lands beside the
                   original instead of overwriting it (e.g. `fixedtb`)
        Sweep the fraction to find the tolerance threshold: how much of the fleet can bid
        on corrupted reasoning before completion or fairness gives way.

Results
    Compared against the stored fault-free reference (scenarios/reference_baseline.json).
    Watch score_mean / score_sd and the faulted-vs-healthy split, not fallback_rate:
    fallback_rate staying at 0 is the fault working, not the fault missing.
"""
from __future__ import annotations

import math
import sys

sys.path.insert(0, "/root/SwarmAgents/scenarios")
import helpers as h  # noqa: E402

NAME = "S09"
TITLE = "SemanticCorrupt"
MODES = ("entity_swap", "rag_poison", "inject_distractor", "context_truncate")


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else "entity_swap"
    if mode not in MODES:
        raise SystemExit(f"mode must be one of {MODES}, got {mode!r}")
    fraction = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0
    suffix = f"-{sys.argv[3]}" if len(sys.argv) > 3 else ""
    all_hosts = h.hosts()
    n = max(1, math.ceil(len(all_hosts) * fraction))
    faulted = all_hosts[:n]
    tag = f"cj-s09-{mode.replace('_', '')}-{int(fraction * 100)}pct{suffix}"

    print(f"\n{NAME} — {TITLE}({mode})  |  poisoning {n}/{len(all_hosts)} hosts")
    h.assert_clean()
    h.health_gate()
    h.cleanup()
    try:
        h.start_fault(faulted, "semantic", mode=mode)
        print(f"  running:        {h.JOBS} jobs, {h.AGENTS} agents -> runs/{tag}")
        h.run_swarm(f"runs/{tag}")
    finally:
        h.stop_fault()

    fault = h.collect(f"runs/{tag}")
    h.report(NAME, f"{TITLE}({mode}) on {n}/{len(all_hosts)} hosts", h.load_reference(), fault, [
        "fallback rate stays at 0 — the fault is silent, and that is the point",
        "LLM score mean/sd shifts: the corrupted reasoning surfacing in the bid",
        "jobs completed and jobs stuck are the correctness claim — consensus absorbing "
        "a wrong cost signal without losing or duplicating work",
        "fairness is where a partial poisoning shows up, as in S05",
    ])
    h.print_split(h.load_split(f"runs/{tag}", n))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
