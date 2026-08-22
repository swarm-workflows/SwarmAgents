#!/usr/bin/env python3.11
"""
S05 — LLMUnavailable (SwarmAgents variant of Chaos Jungle's S05)

What
    Every LLM call from the faulted agents returns HTTP 503. CJ's own S05 checks that a
    single client surfaces the error; here the question is what a 30-agent scheduler does
    when some or all of its agents lose the model mid-run.

Why
    LlmAgent falls back to the analytical cost model on any scoring exception. A total
    outage should therefore degrade the swarm into a purely analytical scheduler with no
    loss of correctness — jobs still complete, nothing is double-assigned. That is the
    graceful-degradation claim, and this is the scenario that proves or breaks it.

How
    scenarios/api/s05_unavailable.py [fraction] [suffix]
        fraction — share of the 30 hosts to fault (default 1.0 = full outage).
        suffix   — appended to the run dir, so a repeat under a different LLM arm or code
                   version lands beside the original instead of overwriting it.
        Run it at 0.25 / 0.5 / 1.0 to get the blast-radius curve.

Results
    Compared against the stored fault-free reference (scenarios/reference_baseline.json).
"""
from __future__ import annotations

import math
import os
import sys

sys.path.insert(0, "/root/SwarmAgents/scenarios")
import helpers as h  # noqa: E402

NAME = "S05"
TITLE = "LLMUnavailable (503)"


def main() -> int:
    fraction = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    suffix = f"-{sys.argv[2]}" if len(sys.argv) > 2 else ""
    all_hosts = h.hosts()
    n = max(1, math.ceil(len(all_hosts) * fraction))
    faulted = all_hosts[:n]
    tag = f"cj-s05-{int(fraction * 100)}pct{suffix}"

    # Figure D: `CJ_DISABLE_FALLBACK=1` removes the analytic safety net, so a faulted agent does
    # not bid at all instead of bidding an instant analytic cost. Set unconditionally (True or
    # False) so a plain S05 cannot inherit the flag from an earlier ablation run — it is a mutation
    # of the frozen fleet's configs, and one that no fleet-wide metric would reveal.
    nofb = os.getenv("CJ_DISABLE_FALLBACK", "").strip().lower() in ("1", "true", "yes")

    print(f"\n{NAME} — {TITLE}  |  faulting {n}/{len(all_hosts)} hosts")
    h.assert_clean()
    h.health_gate()
    # The ablation is armed INSIDE the try, because it mutates the frozen fleet from that line
    # onward: arming it before the block leaves cleanup() — a fan-out that can time out — as a
    # path that exits with the flag still set. Everything that can fail after the first byte is
    # written belongs under the same teardown.
    try:
        h.set_disable_fallback(nofb)
        h.cleanup()
        h.start_fault(faulted, "unavailable")
        print(f"  running:        {h.JOBS} jobs, {h.AGENTS} agents -> runs/{tag}")
        h.run_swarm(f"runs/{tag}")
    finally:
        # Two teardowns, nested so NEITHER can suppress the other. Flat, the config restore would
        # be able to abort before stop_fault() and leak a proxy plus an OLLAMA_BASE_URL — the
        # worse of the two leaks, and one that silently faults every later run.
        #
        # Restore first and unconditionally, not `if nofb`: the invariant worth holding is "no S05
        # run ends with the ablation armed", and a partially-applied arm (which raises) is exactly
        # the case a `nofb` guard would reason about correctly and still need to clean up.
        #
        # A SIGKILL skips both, so `assert_clean()` in the next scenario is the real backstop —
        # it refuses to measure anything while the flag is still set.
        try:
            h.set_disable_fallback(False)
        finally:
            h.stop_fault()

    fault = h.collect(f"runs/{tag}")
    if nofb:
        # The safety net is gone, so the default expectations are not just weaker here, they are
        # wrong: fallback rate goes to 0 BY CONSTRUCTION, and "300 completed" is the open question
        # rather than the prediction — at 100% radius no agent can bid at all.
        expectations = [
            f"no-bid rate rises toward {fraction:.0%} — this is the ablation's only direct "
            f"evidence, and 0 means the flag never reached the agents",
            "fallback rate is 0 by construction, not because the fault missed",
            f"jobs completed: {'unknown — nothing can be proposed if every agent refuses' if n >= len(all_hosts) else 'still 300, absorbed by the healthy agents'}",
            "the faulted group should place ~0 jobs; capture ratio collapses toward 0",
        ]
    else:
        expectations = [
            f"fallback rate rises toward {fraction:.0%} (faulted agents lose the model)",
            "jobs completed stays at 300 — the analytic cost model absorbs the outage",
            "jobs stuck stays 0; no job is left unschedulable",
            "fallback rate near 0 would mean the proxy is not intercepting",
            "the per-agent split below is the actual finding — fleet-wide totals hold steady "
            "while the faulted group takes the healthy group's work",
        ]
    h.report(NAME, f"{TITLE} on {n}/{len(all_hosts)} hosts"
                   f"{' — fallback DISABLED' if nofb else ''}",
             h.load_reference(), fault, expectations)
    # The split IS S05's result. Fleet totals barely move (300/300 either way); what the fault
    # does is redistribute, and that is only visible per agent. This was computed by hand for
    # three runs before it was wired in, which is how the two denominator bugs survived so long.
    h.print_split(h.load_split(f"runs/{tag}", n))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
