#!/usr/bin/env python3.11
"""
Fault-free control run — same gate, cleanup, fleet and trace as every scenario, no fault.

Why it is a scenario file and not a one-off command: the stored reference
(`scenarios/reference_baseline.json`) is only valid for the code that produced it. Change the
scheduler and every delta in every scenario is measured against a baseline that no longer
exists. This re-establishes the control on demand, through exactly the same path a faulted run
takes, so the comparison stays honest.

    scenarios/api/baseline.py [suffix]
        suffix — run-dir suffix, e.g. `fixedtb`; defaults to a plain `cj-baseline-run`.
        --save  overwrite scenarios/reference_baseline.json with this run's metrics.

`--save` is deliberately explicit: overwriting the reference silently would rewrite the meaning
of every result already reported against it.
"""
from __future__ import annotations

import sys

sys.path.insert(0, "/root/SwarmAgents/scenarios")
import helpers as h  # noqa: E402


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    suffix = args[0] if args else "run"
    tag = f"cj-baseline-{suffix}"

    print(f"\nBASELINE — no fault  |  {h.AGENTS} agents, {h.JOBS} jobs")
    h.assert_clean()
    h.health_gate()
    h.cleanup()
    print(f"  running:        {h.JOBS} jobs, {h.AGENTS} agents -> runs/{tag}")
    h.run_swarm(f"runs/{tag}")

    metrics = h.collect(f"runs/{tag}")
    h.report("BASE", f"fault-free control -> runs/{tag}", h.load_reference(), metrics, [
        "every delta here is drift, not fault: fleet, trace and gate are identical",
        "load fairness and the per-decile split are what a scheduler change moves",
    ])
    # Half the fleet vs the other half, with no fault anywhere: the positional bias itself.
    h.print_split(h.load_split(f"runs/{tag}", h.AGENTS // 2))

    if "--save" in sys.argv:
        h.save_reference(metrics)
        print(f"\n  reference_baseline.json now holds runs/{tag}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
