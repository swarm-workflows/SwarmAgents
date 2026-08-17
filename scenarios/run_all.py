#!/usr/bin/env python3.11
"""
Run a set of SwarmAgents chaos scenarios and print a summary table.

Each scenario is one full 30-agent scheduling run (~11 min) plus fleet setup, so unlike
Chaos Jungle's own run_all.py this is a batch job, not an interactive tool. Budget
roughly 15 minutes per entry and prefer an explicit subset over the whole catalogue.

    scenarios/run_all.py                      # the default set below
    scenarios/run_all.py s05 s01              # a named subset
    scenarios/run_all.py --list
"""
from __future__ import annotations

import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))

# name -> (script, argv). Fractions give the blast-radius curve for the tolerance figure.
SCENARIOS: dict[str, tuple[str, list[str]]] = {
    "s05":      ("api/s05_unavailable.py", ["1.0"]),
    "s05-50":   ("api/s05_unavailable.py", ["0.5"]),
    "s05-25":   ("api/s05_unavailable.py", ["0.25"]),
    "s01":      ("api/s01_latency.py", ["3.0", "1.0"]),
    "s01-10":   ("api/s01_latency.py", ["10.0", "1.0"]),
    "s01-50":   ("api/s01_latency.py", ["3.0", "0.5"]),
}

DEFAULT = ["s05", "s01"]


def main() -> int:
    args = sys.argv[1:]
    if "--list" in args:
        for name, (script, argv) in SCENARIOS.items():
            print(f"  {name:<10} {script} {' '.join(argv)}")
        return 0

    selected = [a for a in args if not a.startswith("-")] or DEFAULT
    unknown = [s for s in selected if s not in SCENARIOS]
    if unknown:
        raise SystemExit(f"unknown scenario(s): {', '.join(unknown)} (try --list)")

    print(f"Running {len(selected)} scenario(s): {', '.join(selected)}")
    print(f"Estimated wall clock: ~{15 * len(selected)} min\n")

    results = []
    for name in selected:
        script, argv = SCENARIOS[name]
        rc = subprocess.run(["python3.11", os.path.join(HERE, script), *argv]).returncode
        results.append((name, "ok" if rc == 0 else f"FAILED (rc={rc})"))

    print("\n" + "=" * 40)
    print("  summary")
    print("=" * 40)
    for name, status in results:
        print(f"  {name:<12} {status}")
    return 0 if all(s == "ok" for _, s in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
