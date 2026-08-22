#!/usr/bin/env python3.11
"""Return the slice to idle: clear leaked fault proxies and OLLAMA_BASE_URL overrides,
stop leftover agents, flush Redis, and restore the frozen fleet's configs.

Scenarios call cleanup() at their start, so a run tidies up after its predecessor but never
after itself; use this when a batch finishes."""
import sys
sys.path.insert(0, "/root/SwarmAgents/scenarios")
import helpers as h  # noqa: E402

h.stop_fault()
h.cleanup()          # stray agents + Redis, which stop_fault does not touch
# Before the assertion, not after: assert_clean() *refuses* a leaked figure-D ablation, and this
# script is what the operator is told to run to clear one. Asserting first would make the
# recovery tool the thing that needs recovering.
h.set_disable_fallback(False)
h.assert_clean(strict=True)
print("slice idle")
