#!/usr/bin/env python3.11
"""Return the slice to idle: clear leaked fault proxies and OLLAMA_BASE_URL overrides,
stop leftover agents, and flush Redis.

Scenarios call cleanup() at their start, so a run tidies up after its predecessor but never
after itself; use this when a batch finishes."""
import sys
sys.path.insert(0, "/root/SwarmAgents/scenarios")
import helpers as h  # noqa: E402

h.stop_fault()
h.cleanup()          # stray agents + Redis, which stop_fault does not touch
h.assert_clean(strict=True)
print("slice idle")
