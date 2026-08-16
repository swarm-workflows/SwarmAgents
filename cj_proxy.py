#!/usr/bin/env python3.11
"""
Chaos Jungle per-host LLM fault proxy for SwarmAgents.

Runs on an agent host. Starts a CJ fault proxy in front of the host's local Ollama
endpoint and stays alive so the proxy subprocess persists; the agent process is
pointed at it via OLLAMA_BASE_URL (env overrides llm.base_url in the YAML config).

    ollama-backed agent --OLLAMA_BASE_URL--> cj_proxy (fault) --upstream--> 127.0.0.1:11434

Usage (on an agent host, backgrounded):
    nohup python3.11 cj_proxy.py --fault latency --delay 3.0 --port 18011 &
    export OLLAMA_BASE_URL=http://127.0.0.1:18011/v1     # for the agent process

Stop with SIGTERM/SIGINT; the fault is reverted on the way out.
"""
from __future__ import annotations

import argparse
import signal
import sys
import threading

from chaos_jungle import ChaosRunner, Scenario
from chaos_jungle.faults.llm import (
    LLMLatency,
    LLMRateLimit,
    LLMResponseCorrupt,
    LLMTimeout,
    LLMUnavailable,
    SemanticCorrupt,
)
from chaos_jungle.targets import LocalTarget

DEFAULT_UPSTREAM = "http://127.0.0.1:11434/v1"


def build_fault(args):
    """Map a CLI fault name onto a configured Chaos Jungle fault instance."""
    common = {"port": args.port, "upstream": args.upstream, "base_url_env": args.base_url_env}
    if args.fault == "latency":
        return LLMLatency(delay_s=args.delay, **common)
    if args.fault == "unavailable":
        return LLMUnavailable(**common)
    if args.fault == "timeout":
        return LLMTimeout(timeout_s=args.timeout_s, **common)
    if args.fault == "ratelimit":
        return LLMRateLimit(n=args.after, **common)
    if args.fault == "corrupt":
        return LLMResponseCorrupt(mode=args.mode, **common)
    if args.fault == "semantic":
        return SemanticCorrupt(mode=args.mode, **common)
    raise SystemExit(f"unknown fault: {args.fault}")


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--fault", required=True,
                   choices=["latency", "unavailable", "timeout", "ratelimit", "corrupt", "semantic"])
    p.add_argument("--delay", type=float, default=3.0, help="latency: seconds added per call")
    p.add_argument("--after", type=int, default=5, help="ratelimit: allow N calls, then 429")
    p.add_argument("--timeout-s", type=float, default=30.0, help="timeout: seconds to hang before 504")
    p.add_argument("--mode", default="rag_poison",
                   help="corrupt: truncate|empty|invalid_json; semantic: "
                        "entity_swap|context_truncate|inject_distractor|rag_poison")
    p.add_argument("--port", type=int, default=18011)
    p.add_argument("--upstream", default=DEFAULT_UPSTREAM)
    p.add_argument("--base-url-env", default="OLLAMA_BASE_URL")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    fault = build_fault(args)
    runner = ChaosRunner(Scenario(f"swarm-{args.fault}", [fault]), LocalTarget())

    runner.start()
    print(f"[cj_proxy] {args.fault} active on :{args.port} -> {args.upstream} "
          f"({args.base_url_env})", flush=True)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    stop.wait()

    runner.stop()
    print("[cj_proxy] reverted", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
