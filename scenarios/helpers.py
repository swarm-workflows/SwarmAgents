#!/usr/bin/env python3.11
"""
Shared helpers for the SwarmAgents Chaos Jungle scenarios.

Runs on the orchestrator (`database`), which has passwordless root SSH to every agent
host. Scenario scripts stay declarative: pick the faulted hosts, name the fault, and
compare the resulting run against the stored fault-free reference.

Scenario numbering follows Chaos Jungle's own LLM_SCENARIOS.md (S01 latency, S05
unavailable, …) so results are directly comparable with the framework's catalogue.

Unlike CJ's reference scenarios, the workload here is not a single LLM call but a full
30-agent scheduling run over a frozen job trace — so the signal is scheduler behaviour
(completion, fallback rate, load fairness), not one reply.
"""
from __future__ import annotations

import os
import re
import shlex
import subprocess
import sys
import time
from glob import glob
from typing import Iterable

REPO = "/root/SwarmAgents"
HOSTS_FILE = f"{REPO}/agent_hosts_cj.txt"
REFERENCE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reference_baseline.json")

# Frozen experiment parameters — every scenario must use these so runs stay comparable.
AGENTS = 30
JOBS = 300
TOPOLOGY = "mesh"
PROXY_PORT = 18011
# Bracketed so the pattern never matches the shell command carrying it — an unbracketed
# `pkill -f cj_proxy.py` kills its own parent before the rest of the command runs.
_PROXY_PAT = "cj_prox[y].py"
# CJ spawns its own long-lived proxy script; killing only our driver leaves this bound to
# PROXY_PORT, still serving the PREVIOUS fault. One survived ~12h and silently answered a
# later latency scenario with the earlier scenario's 503s.
_CJ_PROXY_PAT = "llm_prox[y].py"
OLLAMA_UPSTREAM = "http://127.0.0.1:11434/v1"


def hosts() -> list[str]:
    with open(HOSTS_FILE) as fh:
        return [h.strip() for h in fh if h.strip()]


def _sh(cmd: str, timeout: int = 900) -> str:
    p = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True, timeout=timeout)
    return p.stdout


def _fan_out(host_list: Iterable[str], remote_cmd: str, timeout: int = 900,
             per_host_timeout: int = 0) -> str:
    """Run remote_cmd on each host in parallel; returns concatenated stdout.

    per_host_timeout bounds each connection. Needed when the remote command backgrounds a
    long-lived process: ssh does not return even with -n and </dev/null, because the child
    keeps the channel open. Bound the wait and confirm the effect by polling instead.
    """
    # shlex.quote, not json.dumps: double quotes let the LOCAL shell expand $(...) before
    # ssh ever runs, so a remote state check silently reports the orchestrator's state.
    pre = f"timeout {per_host_timeout} " if per_host_timeout else ""
    parts = " ".join(
        f'({pre}ssh -n -o ConnectTimeout=10 -o StrictHostKeyChecking=no {h} {shlex.quote(remote_cmd)} 2>/dev/null) &'
        for h in host_list
    )
    return _sh(parts + " wait", timeout=timeout)


# ---------------------------------------------------------------------------
# Fleet preparation
# ---------------------------------------------------------------------------

def health_gate() -> None:
    """Every host must prove it can infer. A host whose Ollama lost the model keeps
    running and its agent silently falls back to analytic cost for the whole run."""
    out = _fan_out(hosts(), "bash /root/fixmodels.sh")
    ok = out.count(" OK")
    if ok != len(hosts()):
        raise SystemExit(f"health gate failed: only {ok}/{len(hosts())} hosts can infer\n{out}")
    print(f"  health gate:    {ok}/{len(hosts())} hosts inferring")


def cleanup() -> None:
    """Kill agents and stale logs everywhere, flush Redis. Leftover agents register into
    the shared Redis and stall the next run at [SEL_WAIT] live != configured."""
    _fan_out(hosts(), f"pkill -9 -f main[.]py; rm -f {REPO}/swarm-multi/agent-*.log")
    _sh(f"pkill -9 -f main[.]py; rm -f {REPO}/swarm-multi/agent-*.log; "
        "docker exec redis redis-cli flushall >/dev/null")


# ---------------------------------------------------------------------------
# Fault injection (per host, so blast radius is a host subset)
# ---------------------------------------------------------------------------

def start_fault(faulted: list[str], fault: str, **params) -> None:
    """Start a CJ fault proxy on `faulted` hosts and point their agents at it.

    The agent resolves its endpoint as OLLAMA_BASE_URL -> config -> default, so exporting
    the env var on a host redirects only that host's agent.
    """
    # The driver lives on the orchestrator; agent hosts need their own copy. Without this
    # the proxy never starts, agents hit a dead port, and "connection refused" masquerades
    # as an injected fault — the failure looks like a successful outage experiment.
    for host in faulted:
        _sh(f"scp -o ConnectTimeout=10 -o StrictHostKeyChecking=no "
            f"{REPO}/cj_proxy.py {host}:{REPO}/cj_proxy.py >/dev/null 2>&1")

    args = " ".join(f"--{k.replace('_', '-')} {v}" for k, v in params.items())
    cmd = (
        f"cd {REPO} && setsid nohup python3.11 cj_proxy.py --fault {fault} {args} "
        f"--port {PROXY_PORT} --upstream {OLLAMA_UPSTREAM} --base-url-env OLLAMA_BASE_URL "
        f"> /var/log/cj_proxy.log 2>&1 < /dev/null & disown; "
        f"grep -q OLLAMA_BASE_URL /root/.profile || "
        f"echo export OLLAMA_BASE_URL=http://127.0.0.1:{PROXY_PORT}/v1 >> /root/.profile"
    )
    _fan_out(faulted, cmd, timeout=300, per_host_timeout=20)

    # Poll for readiness rather than trusting the ssh return.
    started, deadline = 0, time.time() + 120
    while time.time() < deadline:
        live = _fan_out(faulted, f"pgrep -fc '{_PROXY_PAT}' || true").split()
        started = sum(1 for n in live if n.strip().isdigit() and int(n) > 0)
        if started >= len(faulted):
            break
        time.sleep(5)
    if started < len(faulted):
        raise SystemExit(f"only {started}/{len(faulted)} fault proxies started")

    # Assert the fault actually behaves as named. A live process proves nothing: a stale
    # proxy from an earlier scenario answers on the same port, so a latency run once
    # measured the previous run's 503s.
    observed = _probe_fault(faulted[0])
    _assert_semantics(fault, observed, params)
    print(f"  fault injected: {fault} on {len(faulted)}/{len(hosts())} hosts "
          f"({started} proxies up, probe: {observed['code']} in {observed['ms']}ms)")


def _probe_fault(host: str) -> dict:
    """Send one real request through a faulted host's proxy and time it."""
    body = '{"model":"qwen2.5:3b","messages":[{"role":"user","content":"hi"}],"max_tokens":5}'
    cmd = (f"curl -s -o /dev/null -w %{{http_code}} --max-time 120 "
           f"http://127.0.0.1:{PROXY_PORT}/v1/chat/completions "
           f"-H 'Content-Type: application/json' -d '{body}'")
    t0 = time.time()
    out = _fan_out([host], cmd, timeout=180).strip()
    return {"code": out or "none", "ms": int((time.time() - t0) * 1000)}


def _assert_semantics(fault: str, obs: dict, params: dict) -> None:
    code, ms = obs["code"], obs["ms"]
    if fault == "unavailable":
        if code != "503":
            raise SystemExit(f"expected 503 from LLMUnavailable, got {code}")
    elif fault == "latency":
        want = float(params.get("delay", 0))
        if code != "200":
            raise SystemExit(
                f"LLMLatency should slow a call, not fail it — got HTTP {code} after {ms}ms. "
                f"A stale proxy from a previous scenario may still own :{PROXY_PORT}.")
        if ms < want * 1000 * 0.8:
            raise SystemExit(f"latency fault not applied: {ms}ms < expected ~{want*1000:.0f}ms")


def _state_probe() -> str:
    """env-var count, our driver count, and whether PROXY_PORT is still bound."""
    return (f"echo $(grep -c OLLAMA_BASE_URL /root/.profile) "
            f"$(pgrep -fc '{_PROXY_PAT}' || true) "
            f"$(ss -lnt 2>/dev/null | grep -c ':{PROXY_PORT} ' || true)")


def assert_clean() -> None:
    """Fail before a run rather than after. Checks the port too: a stale CJ proxy keeps
    serving the previous fault, so a later scenario measures the earlier one."""
    out = _fan_out(hosts(), _state_probe())
    dirty = [ln for ln in out.splitlines() if ln.strip() and ln.split() != ["0", "0", "0"]]
    if dirty:
        raise SystemExit(
            f"fleet is dirty on {len(dirty)} host(s) — leaked env var, driver, or a proxy "
            f"still bound to :{PROXY_PORT}.\nrun scenarios/clear_faults.py before measuring.")
    print(f"  clean check:    no leaked env vars / drivers / :{PROXY_PORT} listeners "
          f"on {len(hosts())} hosts")


def stop_fault() -> None:
    """Always run, even on failure — a leaked proxy or env var silently faults later runs.

    The env var is removed *first*: `pkill -f cj_proxy.py` matches the very shell running it,
    so an unbracketed pattern kills this command before later statements execute, which is how
    a leaked env var once made three scenarios report an identical 100% fallback rate.
    """
    _fan_out(hosts(), f"sed -i /OLLAMA_BASE_URL/d /root/.profile; "
                      f"pkill -f '{_PROXY_PAT}'; pkill -f '{_CJ_PROXY_PAT}'; "
                      f"fuser -k {PROXY_PORT}/tcp 2>/dev/null")
    time.sleep(3)
    out = _fan_out(hosts(), _state_probe())
    dirty = [ln for ln in out.splitlines() if ln.strip() and ln.split() != ["0", "0", "0"]]
    if dirty:
        print(f"  !! teardown incomplete on {len(dirty)} host(s) — next run will be invalid")


# ---------------------------------------------------------------------------
# Run + measure
# ---------------------------------------------------------------------------

def run_swarm(run_dir: str, runtime: int = 3000) -> None:
    log = f"{REPO}/runs_{os.path.basename(run_dir)}.log"
    cmd = (
        f"cd {REPO} && nohup python3.11 run_test.py --mode remote --agent-type llm "
        f"--agents {AGENTS} --agents-per-host 1 --topology {TOPOLOGY} --jobs {JOBS} "
        f"--db-host database --agent-hosts-file agent_hosts_cj.txt --use-config-dir "
        f"--jobs-per-interval 30 --stable-seconds 120 --runtime {runtime} "
        f"--generate-plots --run-dir {run_dir} > {log} 2>&1"
    )
    _sh(cmd, timeout=runtime + 900)


def collect(run_dir: str) -> dict:
    """Parse per-host agent logs and the orchestrator log into scenario metrics."""
    m = {"llm_complete": 0, "llm_fallback": 0, "swim_failed": 0}
    lat, per_agent = [], []
    for path in sorted(glob(f"{REPO}/{run_dir}/**/agent-*.log", recursive=True)):
        text = open(path, errors="ignore").read()
        c = text.count("LLM_COST_COMPLETE")
        m["llm_complete"] += c
        m["llm_fallback"] += text.count("LLM_COST_FALLBACK")
        m["swim_failed"] += text.count("FAILED (suspect-timeout)")
        per_agent.append(c)
        # Only LLM_COST_COMPLETE marks a real call. LLM_BID_WON also carries a
        # ReasoningTime, but logs 0.000s when the bid came from the analytic fallback,
        # which would otherwise read as "instant LLM" instead of "no LLM".
        lat += [float(x) for x in
                re.findall(r"LLM_COST_COMPLETE.*?ReasoningTime=([0-9.]+)s", text)]

    calls = m["llm_complete"] + m["llm_fallback"]
    m["fallback_rate"] = round(m["llm_fallback"] / calls, 4) if calls else 0.0
    # Latency comes from ReasoningTime, which only exists on successful LLM calls. Under a
    # full outage there are no samples — report n/a rather than 0, which would read as
    # "instant" instead of "never happened".
    if lat:
        lat.sort()
        m["latency_mean_s"] = round(sum(lat) / len(lat), 2)
        m["latency_p95_s"] = round(lat[int(len(lat) * 0.95)], 2)

    # Orchestrator logs written by hand use underscores where run dirs use hyphens.
    base = os.path.basename(run_dir)
    run_log = next((p for p in (f"{REPO}/runs_{base}.log",
                                f"{REPO}/runs_{base.replace('-', '_')}.log")
                    if os.path.isfile(p)), "")
    if run_log:
        text = open(run_log, errors="ignore").read()
        placed = [int(x) for x in re.findall(r"Agent \d+: (\d+) jobs", text)]
        m["jobs_completed"] = sum(placed)
        # Fairness over jobs actually placed, not scoring effort: placement is what a fault
        # can degrade, and it stays measurable when every LLM call fails.
        if placed and sum(placed):
            total = sum(placed)
            m["jains_fairness"] = round(total * total / (len(placed) * sum(x * x for x in placed)), 3)
        fa = re.search(r"Total failed agents: (\d+)", text)
        m["failed_agents"] = int(fa.group(1)) if fa else None
        inf = re.search(r"Infeasible/Failed jobs: (\d+) retired, (\d+) still", text)
        m["jobs_stuck"] = int(inf.group(2)) if inf else None
    return m


def load_reference() -> dict:
    if not os.path.isfile(REFERENCE):
        raise SystemExit(f"no reference baseline at {REFERENCE}; run save_reference() first")
    return json.load(open(REFERENCE))


def save_reference(metrics: dict) -> None:
    json.dump(metrics, open(REFERENCE, "w"), indent=2)


# ---------------------------------------------------------------------------
# Reporting — mirrors the baseline / fault / delta shape of CJ's own scenarios
# ---------------------------------------------------------------------------

_KEYS = [
    ("jobs_completed", "jobs completed", "{}"),
    ("llm_complete", "LLM calls OK", "{}"),
    ("llm_fallback", "LLM fallbacks", "{}"),
    ("fallback_rate", "fallback rate", "{:.1%}"),
    ("latency_mean_s", "bid latency mean", "{}s"),
    ("latency_p95_s", "bid latency p95", "{}s"),
    ("jains_fairness", "load fairness", "{}"),
    ("swim_failed", "SWIM false-fails", "{}"),
    ("failed_agents", "failed agents", "{}"),
    ("jobs_stuck", "jobs stuck", "{}"),
]


def report(name: str, title: str, baseline: dict, fault: dict, expectations: list[str]) -> None:
    bar = "─" * 74
    print(f"\n{bar}\n  {name} — {title}\n{bar}")
    print(f"  {'metric':<20}{'baseline':>14}{'fault':>14}{'delta':>16}")
    for key, label, fmt in _KEYS:
        b, f = baseline.get(key), fault.get(key)
        if b is None and f is None:
            continue
        bs = fmt.format(b) if b is not None else "-"
        fs = fmt.format(f) if f is not None else "-"
        if isinstance(b, (int, float)) and isinstance(f, (int, float)):
            d = f - b
            ds = f"{d:+.1%}" if fmt.endswith("%}") else f"{d:+g}"
        else:
            ds = "-"
        print(f"  {label:<20}{bs:>14}{fs:>14}{ds:>16}")
    print("\n  expected signals:")
    for line in expectations:
        print(f"    - {line}")
    print(bar)
