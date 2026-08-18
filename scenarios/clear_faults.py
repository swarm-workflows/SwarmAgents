#!/usr/bin/env python3.11
"""Clear any leaked fault proxy or OLLAMA_BASE_URL override across the fleet."""
import sys
sys.path.insert(0, "/root/SwarmAgents/scenarios")
import helpers as h  # noqa: E402

h.stop_fault()
h.assert_clean()
print("fleet clean")
