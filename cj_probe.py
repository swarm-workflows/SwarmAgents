#!/usr/bin/env python3.11
"""
Semantic-fault probe for the SwarmAgents Chaos Jungle scenarios.

Runs on an agent host once a `semantic` fault proxy is up. Sends one identical
request twice — straight to Ollama, and through the proxy — and reports how many
prompt tokens the model actually received in each case.

Why token counts rather than the reply text: SemanticCorrupt mutates the
*request*, and the reply still comes from the real model, so a corrupted call is
indistinguishable from a healthy one at the HTTP layer — that is the whole point
of the fault, and also why "the proxy process is alive" proves nothing here.
`usage.prompt_tokens` is what the model was actually fed, so a delta proves the
mutation reached upstream, without depending on a 3B model choosing to obey a
probe instruction.

The payload is built so that every mode moves the count in a known direction:
entity swaps expand, rag_poison and inject_distractor append text, and
context_truncate drops half the user turn at the ".\n" boundary near its midpoint.
The expansion has to be engineered: a swap can be token-neutral ("New York" ->
"Los Angeles" is +0, and on the real scheduling prompt entity_swap flips exactly
one word, "higher" -> "lower"), so the payload leans on "today" -> "a decade ago",
which is +2 apiece. This probe proves the proxy is rewriting requests; what the
rewrite does to the scheduling prompt is a separate, offline check.

Usage (on an agent host):
    python3.11 cj_probe.py --port 18011 [--model qwen2.5:3b]
    -> {"direct_code": 200, "proxied_code": 200, "direct": 96, "proxied": 108, "delta": 12}
"""
from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request

# Entities chosen from Chaos Jungle's swap map so entity_swap has something to bite on;
# the ".\n\n" boundary and the >80 char length are what rag_poison and context_truncate
# require before they will do anything at all.
SYSTEM = "You are a probe. Reply with the single word OK."
USER = (
    "New York New York New York New York.\n\n"
    "today today today today today today today today.\n\n"
    "The report from yesterday said yes, and the value in New York should increase.\n\n"
    "Reply with OK."
)


def call(url: str, model: str, timeout: float, api_key: str = "") -> tuple[int, int]:
    """POST the probe payload; return (http_code, prompt_tokens)."""
    body = json.dumps({
        "model": model,
        "messages": [{"role": "system", "content": SYSTEM},
                     {"role": "user", "content": USER}],
        "max_tokens": 1,
        "temperature": 0,
    }).encode()
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    req = urllib.request.Request(url, data=body, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.load(resp)
            return resp.status, int(payload.get("usage", {}).get("prompt_tokens", -1))
    except urllib.error.HTTPError as exc:
        return exc.code, -1
    except Exception:
        return 0, -1


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--port", type=int, default=18011, help="fault proxy port")
    p.add_argument("--upstream-port", type=int, default=11434, help="unfaulted Ollama port")
    p.add_argument("--upstream-url", default="",
                   help="full unfaulted endpoint, e.g. https://ollama.com/v1/chat/completions. "
                        "Needed on the cloud arm, where local Ollama is stopped and there is no "
                        "127.0.0.1 endpoint to compare against.")
    p.add_argument("--api-key-file", default="", help="bearer token file for --upstream-url")
    p.add_argument("--model", default="qwen2.5:3b")
    p.add_argument("--timeout", type=float, default=120.0)
    args = p.parse_args()

    path = "/v1/chat/completions"
    key = ""
    if args.api_key_file:
        with open(args.api_key_file) as fh:
            key = fh.read().strip()
    direct_url = args.upstream_url or f"http://127.0.0.1:{args.upstream_port}{path}"
    d_code, direct = call(direct_url, args.model, args.timeout, key)
    t0 = time.time()
    # The proxied call carries the key too: the agent sends it, so the proxy must forward it.
    p_code, proxied = call(f"http://127.0.0.1:{args.port}{path}", args.model, args.timeout, key)
    print(json.dumps({
        "direct_code": d_code,
        "proxied_code": p_code,
        "direct": direct,
        "proxied": proxied,
        "delta": (proxied - direct) if direct > 0 and proxied > 0 else None,
        "ms": int((time.time() - t0) * 1000),
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
