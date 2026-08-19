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

    print(f"\n{NAME} — {TITLE}  |  faulting {n}/{len(all_hosts)} hosts")
    h.assert_clean()
    h.health_gate()
    h.cleanup()
    try:
        h.start_fault(faulted, "unavailable")
        print(f"  running:        {h.JOBS} jobs, {h.AGENTS} agents -> runs/{tag}")
        h.run_swarm(f"runs/{tag}")
    finally:
        h.stop_fault()

    fault = h.collect(f"runs/{tag}")
    h.report(NAME, f"{TITLE} on {n}/{len(all_hosts)} hosts", h.load_reference(), fault, [
        f"fallback rate rises toward {fraction:.0%} (faulted agents lose the model)",
        "jobs completed stays at 300 — the analytic cost model absorbs the outage",
        "jobs stuck stays 0; no job is left unschedulable",
        "fallback rate near 0 would mean the proxy is not intercepting",
    ])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
