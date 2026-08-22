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

import csv
import json
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
REFERENCE = os.getenv("CJ_REFERENCE") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "reference_baseline.json")

# Which LLM arm the fleet is running. "local" = per-host Ollama on 11434. "cloud" = Ollama
# Cloud over IPv6, with local Ollama stopped. Deltas are only meaningful within one arm, so a
# cloud run must be compared against a cloud baseline — set CJ_REFERENCE to point at it.
ARM = os.getenv("CJ_ARM", "local").strip().lower()
# "cloud" means any remote OpenAI-compatible endpoint, not Ollama Cloud specifically. Two are in
# use: Ollama Cloud, and the FABRIC LiteLLM gateway (reachable only over the FABNet dataplane at
# 10.141.1.2, mapped in /etc/hosts so TLS/SNI stays valid). Both are driven through the `ollama`
# provider, because LlmBidder only honours llm.base_url on that path — the `openai` path takes
# its endpoint from OPENAI_BASE_URL, which would collide with fault injection.
CLOUD_ORIGIN = os.getenv("CJ_CLOUD_ORIGIN", "https://ollama.com").rstrip("/")
CLOUD_KEY_FILE = os.getenv("CJ_CLOUD_KEY_FILE", "/root/.ollama_cloud_key")  # root-only, never in the repo
# gpt-oss:120b is the only cloud model measured to both honour json_schema (which LlmBidder
# requires) and not reason by default — qwen3.5:397b answers in prose and would fall back on
# every bid. See the test plan, section 2.1b.
CLOUD_MODEL = os.getenv("CJ_CLOUD_MODEL", "gpt-oss:120b")
# The cloud endpoint is configured through llm.base_url in the YAML, deliberately NOT through
# OLLAMA_BASE_URL. That env var is the fault-injection channel: start_fault only sets it when
# absent, and assert_clean requires it absent, so parking the cloud URL there would make every
# injection a silent no-op — the exact failure mode this harness exists to prevent.
# The run the reference metrics came from; kept so a scenario can also compare *shapes*
# (which agents took the work) and not only fleet-wide totals.
#
# This MUST be switched alongside CJ_REFERENCE when changing arms. The id-bias control it
# supplies is arm-specific and not a small difference: at n=8 the local arm's fault-free split
# is 1.86x but the gateway arm's is 1.27x, because the bias comes from low-numbered hosts
# inferring faster (see the test plan, 4f.2) and that disappears once inference leaves the
# hosts. Comparing a gateway capture ratio against the local control silently reintroduces the
# exact confound load_split() exists to remove.
REFERENCE_RUN = os.getenv("CJ_REFERENCE_RUN") or "runs/cj-baseline-ref"

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
# Origin only: CJ appends the request path, so a /v1 suffix yields /v1/v1/... -> 404.
OLLAMA_UPSTREAM = CLOUD_ORIGIN if ARM == "cloud" else "http://127.0.0.1:11434"


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

# A host below this has no room to run an agent alongside llama-server, and there is no swap.
# Measured: at 79 MB and 102 MB two hosts bid at 139 s and 150 s and won zero jobs all run,
# while a host at 504 MB was the fastest bidder in the fleet — so this is a cliff, not a slope,
# and the threshold sits well under the healthy fleet's 1-4 GB.
MIN_AVAILABLE_MB = 300


def health_gate(min_available_mb: int = MIN_AVAILABLE_MB) -> None:
    """Every host must prove it can infer, and that it has room to.

    Inference alone is not enough. A host whose Ollama lost the model keeps running and its
    agent silently falls back to analytic cost for the whole run — that is what the model probe
    catches. But a host whose long-lived `llama-server` has grown to ~7 GB of its 7.9 GB still
    passes that probe: it answers a single idle request in 0.4 s. Put an agent process next to
    it and both thrash, bids take minutes, the agent wins nothing and SWIM declares it failed.
    Two hosts sat in exactly that state through three runs before anyone measured memory.
    """
    if ARM == "cloud":
        return _cloud_health_gate()

    out = _fan_out(hosts(), "bash /root/fixmodels.sh")
    ok = out.count(" OK")
    if ok != len(hosts()):
        raise SystemExit(f"health gate failed: only {ok}/{len(hosts())} hosts can infer\n{out}")

    mem = _fan_out(hosts(), "echo $(hostname) $(free -m | awk '/Mem:/{print $7}')")
    starved = []
    for line in mem.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1].isdigit() and int(parts[1]) < min_available_mb:
            starved.append((parts[0], int(parts[1])))
    if starved:
        detail = ", ".join(f"{h}={mb}MB" for h, mb in sorted(starved, key=lambda x: x[1]))
        raise SystemExit(
            f"health gate failed: {len(starved)} host(s) under {min_available_mb}MB "
            f"available — {detail}\n"
            f"Restart Ollama there to release llama-server ('systemctl restart ollama', or kill "
            f"'ollama serve' and restart it on hosts where it is not a systemd unit).")
    print(f"  health gate:    {ok}/{len(hosts())} hosts inferring, all >= {min_available_mb}MB free")


def _cloud_health_gate() -> None:
    """Cloud arm: every host must reach ollama.com and hold a working key.

    Three ways a host can be silently wrong here, all of which end as
    `[LLM_COST_FALLBACK]` for the whole run rather than as an error anyone sees: no IPv6 route
    (the slice has no IPv4 egress), no `OLLAMA_API_KEY` in root's profile, or a key the service
    rejects. Each is indistinguishable from "the LLM arm is working" unless it is probed.

    Local Ollama must also be *down* — if it is still listening, a stale `OLLAMA_BASE_URL` or a
    config fallback would quietly route some hosts to the 3B model, and the run would be a
    mixture of two arms.
    """
    body = ('{"model":"' + CLOUD_MODEL + '","messages":[{"role":"user","content":"hi"}],'
            '"max_tokens":4}')
    probe = (f". /root/.profile 2>/dev/null; "
             f"echo $(hostname) "
             f"$(curl -s -o /dev/null -w %{{http_code}} --max-time 90 "
             f"{CLOUD_ORIGIN}/v1/chat/completions "
             f"-H \"Authorization: Bearer $OLLAMA_API_KEY\" "
             f"-H 'Content-Type: application/json' -d '{body}') "
             f"$(pgrep -c 'ollama' || true)")
    # Probed in small batches, not all 30 at once. Firing the whole fleet at the endpoint
    # rate-limits the *health check itself* — 5 hosts came back 429 on the first attempt — which
    # would report a reachability failure that is really a concurrency failure. Batching keeps
    # the gate measuring what it is supposed to measure. It does not fix the run: the agents
    # will hit the same ceiling, and every 429 there becomes a fallback.
    all_hosts = hosts()
    batch = int(os.getenv("CJ_CLOUD_PROBE_BATCH", "4"))
    bad, serving = [], []
    for i in range(0, len(all_hosts), batch):
        for line in _fan_out(all_hosts[i:i + batch], probe, timeout=600).splitlines():
            parts = line.split()
            if len(parts) != 3:
                continue
            host, code, local = parts
            if code != "200":
                bad.append(f"{host}={code}")
            if local.isdigit() and int(local) > 0:
                serving.append(host)
    if bad:
        raise SystemExit(
            f"cloud health gate failed on {len(bad)} host(s): {', '.join(bad[:8])}\n"
            f"Check IPv6 egress and that OLLAMA_API_KEY is exported in /root/.profile.")
    if serving:
        raise SystemExit(
            f"local Ollama still running on {len(serving)} host(s): {', '.join(serving[:8])}\n"
            f"Stop it, or the run silently mixes the remote and 3B arms.")
    print(f"  health gate:    {len(all_hosts)}/{len(all_hosts)} hosts reach {CLOUD_ORIGIN} "
          f"({CLOUD_MODEL}), local Ollama down everywhere")


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
            f"{REPO}/cj_proxy.py {REPO}/cj_probe.py {host}:{REPO}/ >/dev/null 2>&1")
        # The probes run ON the agent host, so the key has to be there too. It was only on the
        # orchestrator once, and the probe duly sent `Authorization: Bearer ` — an empty token,
        # a 401, and an S01 abort that read as "the proxy is not forwarding auth". The proxy was
        # fine. Ship the key with the probe that needs it.
        if ARM == "cloud":
            _sh(f"scp -o ConnectTimeout=10 -o StrictHostKeyChecking=no "
                f"{CLOUD_KEY_FILE} {host}:{CLOUD_KEY_FILE} >/dev/null 2>&1; "
                f"ssh -n -o StrictHostKeyChecking=no {host} 'chmod 600 {CLOUD_KEY_FILE}' "
                f">/dev/null 2>&1")

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
    if fault == "semantic":
        # Two independent checks, because a semantic fault is invisible at the HTTP layer:
        # the listener must be the mode we asked for, and the mutation must actually reach
        # the model. Either alone would pass with a stale proxy of a different mode.
        _assert_proxy_mode(faulted[0], params.get("mode", "entity_swap"))
        observed = _probe_semantic(faulted[0])
        detail = f"prompt tokens {observed['direct']}->{observed['proxied']} " \
                 f"({observed['delta']:+d})"
    else:
        observed = _probe_fault(faulted[0])
        detail = f"{observed['code']} in {observed['ms']}ms"
    _assert_semantics(fault, observed, params)
    print(f"  fault injected: {fault} on {len(faulted)}/{len(hosts())} hosts "
          f"({started} proxies up, probe: {detail})")


def _probe_fault(host: str) -> dict:
    """Send one real request through a faulted host's proxy and time it."""
    model = CLOUD_MODEL if ARM == "cloud" else "qwen2.5:3b"
    body = f'{{"model":"{model}","messages":[{{"role":"user","content":"hi"}}],"max_tokens":5}}'
    # On the cloud arm the agent authenticates, so the probe must too — otherwise the proxy
    # forwards an unauthenticated request, gets 401, and a latency assertion reads that as the
    # fault failing to apply.
    auth = (f"-H \"Authorization: Bearer $(cat {CLOUD_KEY_FILE})\" " if ARM == "cloud" else "")
    cmd = (f"curl -s -o /dev/null -w %{{http_code}} --max-time 120 "
           f"http://127.0.0.1:{PROXY_PORT}/v1/chat/completions {auth}"
           f"-H 'Content-Type: application/json' -d '{body}'")
    t0 = time.time()
    out = _fan_out([host], cmd, timeout=180).strip()
    return {"code": out or "none", "ms": int((time.time() - t0) * 1000)}


def _probe_semantic(host: str) -> dict:
    """Measure how many prompt tokens the model receives with and without the proxy.

    A semantic fault leaves HTTP and JSON intact, so the usual "did it return an error"
    probe cannot see it. cj_probe.py sends the same payload both ways; the delta in
    usage.prompt_tokens is the mutation, measured at the only place it is observable.
    """
    extra = ""
    if ARM == "cloud":
        extra = (f" --upstream-url {CLOUD_ORIGIN}/v1/chat/completions"
                 f" --api-key-file {CLOUD_KEY_FILE} --model {CLOUD_MODEL}")
    out = _fan_out([host], f"cd {REPO} && python3.11 cj_probe.py --port {PROXY_PORT}{extra}",
                   timeout=300).strip()
    try:
        return json.loads(out.splitlines()[-1])
    except (ValueError, IndexError):
        raise SystemExit(f"semantic probe returned no JSON from {host}: {out!r}")


def _assert_proxy_mode(host: str, mode: str) -> None:
    """The listener on PROXY_PORT must be a semantic_corrupt proxy in the mode we asked for.

    CJ spawns its own long-lived llm_proxy.py; a leftover one from an earlier mode answers
    on the same port and would otherwise be measured as this scenario.
    """
    out = _fan_out([host], "ps -eo args= | grep llm_prox[y].py || true", timeout=120)
    line = next((ln for ln in out.splitlines() if "llm_proxy.py" in ln), "")
    if "--fault semantic_corrupt" not in line:
        raise SystemExit(f"no semantic_corrupt proxy on {host}; found: {line.strip()[:200]!r}")
    if f"--semantic-mode {mode}" not in line:
        raise SystemExit(f"proxy on {host} is not in mode {mode!r}: {line.strip()[:200]!r}")


def _assert_semantics(fault: str, obs: dict, params: dict) -> None:
    if fault == "semantic":
        mode = params.get("mode", "entity_swap")
        if obs["proxied_code"] != 200 or obs["direct_code"] != 200:
            raise SystemExit(
                f"semantic corruption must stay silent — got HTTP {obs['proxied_code']} "
                f"through the proxy, {obs['direct_code']} direct. A visible error means the "
                f"agent would fall back, which is a different experiment.")
        delta = obs["delta"]
        if delta is None:
            raise SystemExit(f"probe could not read prompt_tokens: {obs}")
        # context_truncate drops half the user turn; every other mode adds text. A delta of
        # 0 means the request reached the model unmutated — the proxy is a pass-through.
        want_negative = mode == "context_truncate"
        if want_negative and delta > -3:
            raise SystemExit(f"context_truncate did not shorten the prompt: {delta:+d} tokens")
        if not want_negative and delta < 3:
            raise SystemExit(f"{mode} did not alter the prompt: {delta:+d} tokens")
        return

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


def assert_clean(strict: bool = False) -> None:
    """Fail before a run rather than after. Checks the port too: a stale CJ proxy keeps
    serving the previous fault, so a later scenario measures the earlier one.

    strict also rejects leftover agents and Redis state. Those are normally cleared by
    cleanup() at the start of a run, which means a scenario tidies up after its predecessor
    but never after itself — so the slice is left dirty whenever a batch ends.
    """
    out = _fan_out(hosts(), _state_probe())
    dirty = [ln for ln in out.splitlines() if ln.strip() and ln.split() != ["0", "0", "0"]]
    if dirty:
        raise SystemExit(
            f"fleet is dirty on {len(dirty)} host(s) — leaked env var, driver, or a proxy "
            f"still bound to :{PROXY_PORT}.\nrun scenarios/clear_faults.py before measuring.")
    print(f"  clean check:    no leaked env vars / drivers / :{PROXY_PORT} listeners "
          f"on {len(hosts())} hosts")
    if strict:
        agents = sum(int(n) for n in _fan_out(hosts(), 'pgrep -fc "mai[n].py" || true').split()
                     if n.strip().isdigit())
        keys = int((_sh("docker exec redis redis-cli dbsize").strip() or "0").split()[-1])
        if agents or keys:
            raise SystemExit(f"slice not idle: {agents} stray agent process(es), "
                             f"{keys} Redis key(s). Run scenarios/clear_faults.py.")
        print("  idle check:     0 stray agents, 0 Redis keys")


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

def run_swarm(run_dir: str, runtime: int | None = None) -> None:
    """Launch the fleet. `CJ_RUNTIME` raises the cap for scenarios that are meant to be slow.

    The default 3000 s comfortably fits a fault-free run (~11 min). A large injected delay does
    not fit: at +30 s on a ~5 s bid the fleet is 7x slower, and a run that hits the cap reports
    fewer than 300 jobs completed — which reads as "the fault broke scheduling" when it actually
    means "the harness stopped watching". Raise the cap for the slow points of a sweep so the
    completion figure keeps meaning what it means everywhere else.
    """
    runtime = runtime if runtime is not None else int(os.getenv("CJ_RUNTIME", "3000"))
    log = f"{REPO}/runs_{os.path.basename(run_dir)}.log"
    cmd = (
        f"cd {REPO} && nohup python3.11 run_test.py --mode remote --agent-type llm "
        f"--agents {AGENTS} --agents-per-host 1 --topology {TOPOLOGY} --jobs {JOBS} "
        f"--db-host database --agent-hosts-file agent_hosts_cj.txt --use-config-dir "
        f"--jobs-per-interval 30 --stable-seconds 120 --runtime {runtime} "
        f"--generate-plots --run-dir {run_dir} > {log} 2>&1"
    )
    _sh(cmd, timeout=runtime + 900)


def _restarts_and_conflicts(run_dir: str) -> dict:
    """Job restarts and consensus conflicts, from the run's own metrics.json.

    Section 4 lists both as *primary* metrics and nothing collected them, so every result table in
    the campaign was silent about them. That silence is not the same as a zero, and the difference
    matters: a scheduling latency of 140-560 s invites the reading "jobs are being reselected",
    and only a measured restart count can refute it.

    Two independent sources, because each can be absent:
      * `<run>/metrics.json` — the Metrics export, `{agent_id: {restarts: {...}, conflicts: {...}}}`.
        Values may be counts or per-job collections, so both are handled.
      * the agent logs — `RESTART: Job:` is emitted by `print()` in resource_agent (not the
        logger, so it only lands here because run_test redirects stdout into the log), and
        `leaving for reselection` by the Snow engine when a decision exhausts max_rounds. These
        are the *actual* strings in the source; a plausible-looking guess at a marker is how an
        earlier version of this check concluded "zero" without evidence.

    The log markers are a cross-check on metrics.json, not a substitute: they are reported under
    separate keys so a disagreement is visible rather than averaged away.
    """
    out: dict = {}

    def _count(container) -> int:
        n = 0
        for v in (container or {}).values():
            if isinstance(v, bool):
                n += int(v)
            elif isinstance(v, (int, float)):
                n += int(v)
            elif isinstance(v, (list, tuple, set, dict)):
                n += len(v)
            else:
                n += 1
        return n

    path = f"{REPO}/{run_dir}/metrics.json"
    if os.path.isfile(path):
        try:
            data = json.load(open(path))
        except ValueError:
            data = None
        if isinstance(data, dict):
            restarts = conflicts = 0
            for entry in data.values():
                if isinstance(entry, dict):
                    restarts += _count(entry.get("restarts"))
                    conflicts += _count(entry.get("conflicts"))
            out["restarts"] = restarts
            out["conflicts"] = conflicts

    marks = {"restart_log_lines": 0, "reselection_log_lines": 0}
    for p in sorted(glob(f"{REPO}/{run_dir}/**/agent-*.log", recursive=True)):
        body = open(p, errors="ignore").read()
        marks["restart_log_lines"] += body.count("RESTART: Job:")
        marks["reselection_log_lines"] += body.count("leaving for reselection")
    out.update(marks)

    # If metrics.json was missing, fall back to the logs so the metric is never simply absent.
    if "restarts" not in out:
        out["restarts"] = marks["restart_log_lines"]
        return out

    # Two sources are only worth having if they are compared. Both are printed by report(), but a
    # reader has to notice two rows disagree; say it out loud too.
    #
    # These are TWO INDEPENDENT checks, deliberately not chained. An earlier version made the
    # reselection check an `elif` on `restarts == 0`, which meant that a run with restarts
    # agreeing at a nonzero value hit neither branch and its reselection evidence disappeared —
    # the same "collected but never surfaced" bug one level down.
    if out["restarts"] != marks["restart_log_lines"]:
        print(f"  !! restart sources disagree for {run_dir}: metrics.json="
              f"{out['restarts']}, 'RESTART: Job:' log lines={marks['restart_log_lines']}. "
              f"Trust neither until reconciled — metrics.json is what the `restarts` row reports.")

    # Unconditional, and never folded into `restarts`: the Snow engine's max_rounds path logs
    # this without touching the restart counter, so it is evidence no value of `restarts` can
    # account for. Gating it on restarts==0 hid it exactly when restarts was also happening,
    # i.e. when a run was most disturbed.
    if marks["reselection_log_lines"]:
        print(f"  !! {marks['reselection_log_lines']} 'leaving for reselection' line(s) in "
              f"{run_dir}, from the Snow engine's max_rounds path. That path does not increment "
              f"the restart counter, so the `job restarts` row ({out['restarts']}) does NOT "
              f"include them. Any latency attributed to queueing (4.0) needs re-checking.")
    return out


def _orchestrator_log(run_dir: str) -> str:
    """Orchestrator logs written by hand use underscores where run dirs use hyphens."""
    base = os.path.basename(run_dir)
    return next((p for p in (f"{REPO}/runs_{base}.log",
                             f"{REPO}/runs_{base.replace('-', '_')}.log")
                 if os.path.isfile(p)), "")


_BLOCK_HEADER = re.compile(r"^\[(\w+)\] Jobs per agent:")
_AGENT_LINE = re.compile(r"^\s*Agent (\d+): (\d+) jobs\s*$")


def _job_blocks(text: str) -> list:
    """Every "[label] Jobs per agent:" block in the orchestrator log, in file order.

    The producer is `plotting/single_run.py:plot_scheduling_latency_and_jobs`, which prints one
    such block per invocation — and the flat pipeline invokes it **twice whenever a run contains
    restarts**: once over all jobs (`[all]`) and once over the restart-filtered set
    (`[no_restarts]`). Those are two different populations of the same run, not a summary and a
    correction, so they must never be merged or summed.
    """
    blocks: list = []
    current = None
    for line in text.splitlines():
        header = _BLOCK_HEADER.match(line)
        if header:
            current = (header.group(1), {})
            blocks.append(current)
            continue
        if current is None:
            continue
        agent = _AGENT_LINE.match(line)
        if agent:
            current[1][int(agent.group(1))] = int(agent.group(2))
        elif line.strip():
            current = None      # a block ends at the first non-agent, non-blank line
    return blocks


def placement(run_dir: str) -> tuple[dict, int]:
    """Jobs placed per agent id, and the fleet size every per-agent metric must divide by.

    THE single parser for "Agent N: M jobs". collect() and load_split() both go through it so
    they cannot drift apart, which they had: collect() summed a *list* of matches while
    load_split() built a *dict*, so on a multi-block log one double-counted and the other did not.

    **Block-aware, because a log can hold several complete blocks for one run.** Reading them as
    one flat stream of matches corrupts any run containing restarts, in either direction:
    summing every match double-counts placements, while deduplicating by id silently substitutes
    the restart-filtered population for the real one. `[all]` is the block these experiments mean
    — a restarted job really was placed, and jobs_completed is meant to reconcile with the 300
    submitted. **Exactly one `[all]` block is required**; anything else is refused rather than
    guessed at:

      * several blocks with no `[all]` among them — what a **hierarchical** run produces, since
        `label_suffix="_level0"` is truthy and every level is therefore mislabelled
        `[no_restarts]` (SwarmAgents finding 12). Picking one would silently report a single
        level's placement as the whole fleet's.
      * a *lone* non-`[all]` block, which one level-filtered or restart-filtered invocation
        emits on its own. Being the only block in the file makes it unambiguous as a block, not
        the fleet's placement — it still describes a subset.
      * a legacy log with no block header at all *and* repeated agent ids, where there is no
        evidence for which occurrence is authoritative.

    Refusing matters more than it looks: every earlier bug here was silent, and a wrong choice
    does not fail, it just moves the number.

    The fleet size is the CONFIGURED count, never the number of ids the log happens to mention:
    an agent that wins no jobs emits no line at all, and dropping it from a denominator inflates
    fairness and deflates capture (see collect() and load_split()). It only exceeds AGENTS if the
    log actually names a higher id, as a dynamic-agent run would.
    """
    log = _orchestrator_log(run_dir)
    if not log:
        return {}, AGENTS
    text = open(log, errors="ignore").read()

    blocks = _job_blocks(text)
    if blocks:
        # Exactly one block labelled `all`, or nothing. No special case for a *lone* block:
        # being the only block makes it unambiguous as a block, not the whole fleet's placement.
        # A single `[no_restarts]` is what one level-filtered invocation emits, and accepting it
        # would report that level as the fleet — the same corruption this guard exists to stop,
        # with one block instead of three.
        canonical = [b for lbl, b in blocks if lbl == "all"]
        if len(canonical) != 1:
            labels = ", ".join(lbl for lbl, _ in blocks)
            raise SystemExit(
                f"{log}: {len(blocks)} 'Jobs per agent' block(s) [{labels}], "
                f"{len(canonical)} of them labelled 'all' — no unambiguous whole-fleet "
                f"placement. Exactly one 'all' block is required. A hierarchical run labels "
                f"every level 'no_restarts' (finding 12), and a restart-filtered invocation on "
                f"its own emits only 'no_restarts'; in both cases the block describes a subset, "
                f"so parse the population you want explicitly rather than letting this guess.")
        placed = canonical[0]
    else:
        pairs = re.findall(r"Agent (\d+): (\d+) jobs", text)
        ids = [int(a) for a, _ in pairs]
        if len(ids) != len(set(ids)):
            raise SystemExit(
                f"{log}: no '[label] Jobs per agent:' header, and agent ids repeat "
                f"({len(ids)} lines, {len(set(ids))} distinct). Cannot tell whether the repeats "
                f"are separate populations or a revised summary.")
        placed = {int(a): int(j) for a, j in pairs}

    fleet = max(AGENTS, max(placed, default=0))
    return placed, fleet


def collect(run_dir: str) -> dict:
    """Parse per-host agent logs and the orchestrator log into scenario metrics."""
    m = {"llm_complete": 0, "llm_fallback": 0, "swim_failed": 0}
    lat, scores = [], []
    for path in sorted(glob(f"{REPO}/{run_dir}/**/agent-*.log", recursive=True)):
        text = open(path, errors="ignore").read()
        c = text.count("LLM_COST_COMPLETE")
        m["llm_complete"] += c
        m["llm_fallback"] += text.count("LLM_COST_FALLBACK")
        m["swim_failed"] += text.count("FAILED (suspect-timeout)")
        # Only LLM_COST_COMPLETE marks a real call. LLM_BID_WON also carries a
        # ReasoningTime, but logs 0.000s when the bid came from the analytic fallback,
        # which would otherwise read as "instant LLM" instead of "no LLM".
        lat += [float(x) for x in
                re.findall(r"LLM_COST_COMPLETE.*?ReasoningTime=([0-9.]+)s", text)]
        # The bid itself. Under a semantic fault the call succeeds and nothing falls back,
        # so the score is the only place the corruption is visible before it reaches
        # consensus — it is to Tier 2 what fallback_rate is to Tier 1.
        scores += [float(x) for x in re.findall(r"LLM_COST_COMPLETE.*?Score=([0-9.]+)", text)]

    calls = m["llm_complete"] + m["llm_fallback"]
    m["fallback_rate"] = round(m["llm_fallback"] / calls, 4) if calls else 0.0
    # Latency comes from ReasoningTime, which only exists on successful LLM calls. Under a
    # full outage there are no samples — report n/a rather than 0, which would read as
    # "instant" instead of "never happened".
    if lat:
        lat.sort()
        m["latency_mean_s"] = round(sum(lat) / len(lat), 2)
        m["latency_p95_s"] = round(lat[int(len(lat) * 0.95)], 2)
    if scores:
        mean = sum(scores) / len(scores)
        m["score_mean"] = round(mean, 1)
        m["score_sd"] = round((sum((s - mean) ** 2 for s in scores) / len(scores)) ** 0.5, 1)

    # Queue-drain quality. A fault that leaves every count intact can still schedule badly,
    # and under a semantic fault this is the only place the damage can land: the bids are
    # wrong but nothing errors, so completion and fallback_rate both stay clean. Read from
    # all_jobs.csv rather than the plotting step's stdout, which not every run has.
    jobs_csv = f"{REPO}/{run_dir}/all_jobs.csv"
    if os.path.isfile(jobs_csv):
        sl, wait, sel = [], [], []
        with open(jobs_csv, newline="") as fh:
            for r in csv.DictReader(fh):
                if r.get("scheduling_latency"):
                    sl.append(float(r["scheduling_latency"]))
                # Split the latency into the two phases it is actually made of. Measured
                # 2026-08-22: selection is ~1.0 s flat while the pool wait is 78-311 s, and
                # scheduling_latency is exactly their sum. Without the split, a 140-560 s figure
                # reads as slow consensus or a reselection restart when it is neither — it is a
                # job queueing behind the other 299, all of which arrive within ~9 s.
                try:
                    sub = float(r["submitted_at"])
                    started = float(r["selection_started_at"])
                    assigned = float(r["assigned_at"])
                except (TypeError, ValueError, KeyError):
                    continue
                wait.append(started - sub)
                sel.append(assigned - started)
        if sl:
            sl.sort()
            m["sched_latency_mean_s"] = round(sum(sl) / len(sl), 1)
            m["sched_latency_p95_s"] = round(sl[int(len(sl) * 0.95)], 1)
        if wait:
            m["pool_wait_mean_s"] = round(sum(wait) / len(wait), 1)
        if sel:
            m["selection_mean_s"] = round(sum(sel) / len(sel), 1)

    run_log = _orchestrator_log(run_dir)
    if run_log:
        text = open(run_log, errors="ignore").read()
        placed_by_id, fleet = placement(run_dir)
        placed = list(placed_by_id.values())
        m["jobs_completed"] = sum(placed)
        m["agents_placing"] = len(placed)
        m["fleet_size"] = fleet
        # Fairness over jobs actually placed, not scoring effort: placement is what a fault
        # can degrade, and it stays measurable when every LLM call fails.
        #
        # n is the fleet size from placement(), NOT len(placed). An agent that wins no jobs never
        # appears in an "Agent N: M jobs" line, and Jain's index is (sum x)^2 / (n * sum x^2):
        # dropping a zero-load agent leaves both sums untouched while shrinking n, so it
        # *inflates* fairness by exactly n_logged/fleet. The inflation is largest in the runs
        # where agents are starved of work — the partial-outage runs whose whole finding is that
        # work is unevenly captured. Using len(placed) understated every collapse it measured.
        if placed and sum(placed):
            total = sum(placed)
            m["jains_fairness"] = round(total * total / (fleet * sum(x * x for x in placed)), 3)
        m.update(_restarts_and_conflicts(run_dir))
        fa = re.search(r"Total failed agents: (\d+)", text)
        m["failed_agents"] = int(fa.group(1)) if fa else None
        inf = re.search(r"Infeasible/Failed jobs: (\d+) retired, (\d+) still", text)
        m["jobs_stuck"] = int(inf.group(2)) if inf else None
    return m


def load_split(run_dir: str, n_faulted: int, reference: bool = False) -> dict:
    """Split placement and bidding between the faulted hosts and the healthy ones.

    Agent i runs on hosts()[i-1] (--agents-per-host 1, hosts consumed in file order), so
    the faulted hosts are agent ids 1..n_faulted. Verified against the S05 runs, where
    every fallback landed on exactly those ids and none outside them.

    This is the measurement that carried the S05 story: the fleet-wide totals held steady
    while one group quietly took the other group's work.

    A raw capture ratio is not usable on its own. SwarmAgents already favours low agent
    ids — `tie_break_key=agent_id` in the selection, plus `cost + self.agent_id` on the
    proposal — so *any* split at id n shows the low group ahead even with no fault at all
    (1.86x at n=8, 1.59x at n=15 in the fault-free reference). The faulted hosts are always
    the low ids, so the confound points the same way as the effect. Every split therefore
    carries the reference run's ratio at the same split point: only the gap between them is
    the fault.
    """
    placed, fleet = placement(run_dir)
    if not placed:
        return {}

    scores: dict[int, list[float]] = {}
    for path in sorted(glob(f"{REPO}/{run_dir}/**/agent-*.log", recursive=True)):
        aid = int(re.search(r"agent-(\d+)\.log$", path).group(1))
        body = open(path, errors="ignore").read()
        scores[aid] = [float(x) for x in re.findall(r"LLM_COST_COMPLETE.*?Score=([0-9.]+)", body)]

    def group(ids: list[int]) -> dict:
        jobs = [placed.get(i, 0) for i in ids]
        sc = [s for i in ids for s in scores.get(i, [])]
        return {"agents": len(ids), "jobs": sum(jobs),
                "jobs_per_agent": round(sum(jobs) / len(ids), 2) if ids else 0.0,
                "score_mean": round(sum(sc) / len(sc), 1) if sc else None}

    # Every agent in the fleet, not just those the orchestrator log mentions. An agent that won no
    # jobs never appears in a "Agent N: M jobs" line, so keying off the log drops it from its
    # group's denominator and *understates* capture — precisely backwards, since losing every job
    # is the strongest evidence of being out-raced. The S05 25% gateway run lists 25 of 30 agents
    # while its jobs still sum to 300: the 5 absent ones won zero, and excluding them reported
    # that run's capture as 13.1x when it is really 16.9x.
    ids = list(range(1, fleet + 1))
    out = {"faulted": group([i for i in ids if i <= n_faulted]),
           "healthy": group([i for i in ids if i > n_faulted])}
    fpa, hpa = out["faulted"]["jobs_per_agent"], out["healthy"]["jobs_per_agent"]
    out["capture_ratio"] = round(fpa / hpa, 2) if hpa else None
    if not reference:
        # Never quote another arm's control: a wrong control does not look wrong, it just moves
        # the verdict, which is worse than having no control at all. Warn and omit rather than
        # raise — this runs after a ~15 min run has already succeeded, and a metadata problem
        # should not discard the measurement.
        if os.getenv("CJ_REFERENCE") and not os.getenv("CJ_REFERENCE_RUN"):
            print(f"  !! CJ_REFERENCE is set but CJ_REFERENCE_RUN is not — the id-bias control "
                  f"would come from {REFERENCE_RUN}, a different arm than the metrics baseline. "
                  f"Reporting the raw ratio with no control; set CJ_REFERENCE_RUN to this arm's "
                  f"fault-free run dir and re-read with load_split().")
            out["reference_ratio"] = None
            out["reference_run"] = None
            return out
        ref = load_split(REFERENCE_RUN, n_faulted, reference=True)
        out["reference_ratio"] = ref.get("capture_ratio")
        out["reference_run"] = REFERENCE_RUN
    return out


def print_split(split: dict) -> None:
    if not split or not split.get("healthy", {}).get("agents"):
        return
    print(f"\n  {'group':<12}{'agents':>8}{'jobs':>8}{'jobs/agent':>13}{'LLM score':>12}")
    for name in ("faulted", "healthy"):
        g = split[name]
        sc = "-" if g["score_mean"] is None else g["score_mean"]
        print(f"  {name:<12}{g['agents']:>8}{g['jobs']:>8}{g['jobs_per_agent']:>13}{sc:>12}")
    ratio, ref = split.get("capture_ratio"), split.get("reference_ratio")
    if ratio is None:
        return
    if ref:
        verdict = "no capture beyond the id bias" if ratio <= ref * 1.15 else \
                  f"{round(ratio / ref, 2)}x beyond the id bias"
        print(f"  capture ratio {ratio}x vs {ref}x for the same split with no fault "
              f"-> {verdict}")
    else:
        print(f"  capture ratio {ratio}x (no reference split available)")


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
    ("score_mean", "LLM score mean", "{}"),
    ("score_sd", "LLM score sd", "{}"),
    ("sched_latency_mean_s", "sched latency mean", "{}s"),
    ("sched_latency_p95_s", "sched latency p95", "{}s"),
    # sched latency is pool wait + selection, and the two are wildly different sizes: selection
    # is ~1 s while the wait is 78-311 s. Reporting only the total invites reading a long latency
    # as slow consensus or a reselection restart, when it is a job queueing for its turn.
    ("pool_wait_mean_s", "  of which pool wait", "{}s"),
    ("selection_mean_s", "  of which selection", "{}s"),
    ("jains_fairness", "load fairness", "{}"),
    ("swim_failed", "SWIM false-fails", "{}"),
    ("failed_agents", "failed agents", "{}"),
    # Section 4 lists these as PRIMARY metrics, but nothing captured them until 2026-08-22, so no
    # result table in the campaign reported them. Always print them, even at zero: "0 restarts"
    # is a claim worth making explicitly, and it is the control for any latency the reader might
    # otherwise attribute to reselection.
    ("restarts", "job restarts", "{}"),
    # The cross-check has to be PRINTED to be a cross-check. These were collected but left out
    # of this table, so a report could show "job restarts 0" from metrics.json while the logs
    # held nonzero evidence, with nothing on screen to reveal the disagreement — the exact
    # failure mode the two-source design was meant to prevent.
    ("restart_log_lines", "  restart log lines", "{}"),
    ("reselection_log_lines", "  reselect log lines", "{}"),
    ("conflicts", "consensus conflicts", "{}"),
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
