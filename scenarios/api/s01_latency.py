#!/usr/bin/env python3.11
"""
S01 — LLMLatency (SwarmAgents variant of Chaos Jungle's S01)

What
    Adds a fixed delay to every LLM call on the faulted agents. CJ's S01 measures the
    delay on one reply; here the delay lands inside a scheduling loop where each agent
    already spends ~10 s per bid, so the interesting effect is on throughput and on how
    the swarm redistributes work, not on one call.

Why
    Sweeping the delay gives the dose-response curve. It also probes a second-order
    effect worth reporting: a slow agent answers SWIM probes late, so LLM-plane latency
    can surface as membership churn (CJ Layer 1 leaking into Layer 8). The fault-free
    reference already carries ~9 such events, so compare against that, not against zero.

How
    scenarios/api/s01_latency.py [delay_s] [fraction]
        delay_s  — seconds added per call (default 3.0)
        fraction — share of the 30 hosts to fault (default 1.0)

Results
    Compared against the stored fault-free reference (scenarios/reference_baseline.json).
"""
from __future__ import annotations

import math
import sys

sys.path.insert(0, "/root/SwarmAgents/scenarios")
import helpers as h  # noqa: E402

NAME = "S01"
TITLE = "LLMLatency"


def main() -> int:
    delay = float(sys.argv[1]) if len(sys.argv) > 1 else 3.0
    fraction = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0
    all_hosts = h.hosts()
    n = max(1, math.ceil(len(all_hosts) * fraction))
    faulted = all_hosts[:n]
    tag = f"cj-s01-d{int(delay)}-{int(fraction * 100)}pct"

    print(f"\n{NAME} — {TITLE} +{delay}s  |  faulting {n}/{len(all_hosts)} hosts")
    h.assert_clean()
    h.health_gate()
    h.cleanup()
    try:
        h.start_fault(faulted, "latency", delay=delay)
        print(f"  running:        {h.JOBS} jobs, {h.AGENTS} agents -> runs/{tag}")
        h.run_swarm(f"runs/{tag}")
    finally:
        h.stop_fault()

    fault = h.collect(f"runs/{tag}")
    h.report(NAME, f"{TITLE} +{delay}s on {n}/{len(all_hosts)} hosts", h.load_reference(), fault, [
        f"bid latency mean rises by roughly +{delay}s on the faulted share",
        "jobs completed stays at 300 — latency slows scheduling, it does not break it",
        "SWIM false-fails may rise above the ~9 baseline: latency leaking into membership",
        "a delta near 0 would mean the proxy is not intercepting",
    ])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
