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
from datetime import datetime
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


def _probe_hosts(remote_cmd: str, fields: int = 1, timeout: int = 900,
                 host_list: Iterable[str] | None = None) -> tuple[dict, list, list]:
    """Run a per-host probe that echoes `$(hostname) <n integers>`; return answers, SILENT, BAD.

    Three outcomes, not two, and the third is why this parses strictly. `_fan_out` discards stderr
    and returns only what came back, so an unreachable host contributes no line — and any check
    written as "no bad lines means fine" passes it. An unreachable host is the one most likely to
    still be holding whatever is being checked for.

    A MALFORMED answer is just as dangerous and less obvious. A probe whose command substitution
    failed returns a short line or a non-numeric field, and a caller that reads values with
    `if x.isdigit()` silently skips it — so "0 stray agents" and "enough memory" were both
    reachable by a host that answered nothing meaningful. Parsing lives here, once, so a caller
    cannot accidentally treat unparseable as fine: answers are ints or they are not answers.
    """
    wanted = list(host_list) if host_list is not None else hosts()
    # Collected per host FIRST, then judged. Judging line by line let a host that emitted one
    # unreadable line and one good one be accepted on the good one — a later answer cannot cancel
    # an earlier malformed one, because whatever produced the garbage was also running when the
    # good line was written. Exactly one well-formed answer per host is the only way through.
    seen: dict[str, list[list[str]]] = {}
    bad: list[str] = []
    for line in _fan_out(wanted, remote_cmd, timeout=timeout).splitlines():
        parts = line.split()
        if not parts:
            continue
        if parts[0] not in wanted:
            # A stray stdout line. Named rather than dropped: if its first token ever did collide
            # with a hostname it would masquerade as that host's answer.
            bad.append(f"{line.strip()[:60]!r} (not a known host)")
            continue
        seen.setdefault(parts[0], []).append(parts[1:])

    answers: dict[str, list[int]] = {}
    for host, rows in seen.items():
        if len(rows) != 1:
            bad.append(f"{host} answered {len(rows)} times")
        # `isdigit()` with no sign stripping: every probe here returns a COUNT (`wc -l`,
        # `pgrep -c`, `grep -c`, `ss | grep -c`, `free -m`), none of which can legitimately be
        # negative. Accepting a minus sign let `-1` through as a valid answer, and the one caller
        # that decides on a total would then have it cancel a real count elsewhere.
        elif len(rows[0]) != fields or not all(v.isdigit() for v in rows[0]):
            bad.append(f"{host} -> {' '.join(rows[0])[:40]!r}")
        else:
            answers[host] = [int(v) for v in rows[0]]
    return answers, [h for h in wanted if h not in seen], bad


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

    mem, quiet, bad = _probe_hosts("echo $(hostname) $(free -m | awk '/Mem:/{print $7}')")
    starved = [(h, f[0]) for h, f in mem.items() if f[0] < min_available_mb]
    # A host that did not report its memory, or reported something unparseable, has not been
    # cleared by this gate. That is the failure this gate was written for: a starved host answers a
    # single inference probe in 0.4 s, places zero jobs for a whole run, and looks healthy in every
    # other check (4f.3).
    if starved or quiet or bad:
        why = []
        if starved:
            why.append(f"{len(starved)} host(s) under {min_available_mb}MB available — "
                       + ", ".join(f"{h}={mb}MB" for h, mb in sorted(starved, key=lambda x: x[1])))
        if quiet:
            why.append(f"{len(quiet)} host(s) did not report memory ({', '.join(quiet[:8])})")
        if bad:
            why.append(f"{len(bad)} unreadable answer(s): {'; '.join(bad[:4])}")
        raise SystemExit(
            f"health gate failed: {'; '.join(why)}\n"
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
    bad, serving, answered = [], [], set()
    for i in range(0, len(all_hosts), batch):
        for line in _fan_out(all_hosts[i:i + batch], probe, timeout=600).splitlines():
            parts = line.split()
            # A short or garbled line used to be skipped, which left the host looking unprobed and
            # then — before `answered` existed — silently healthy. Now it is a named failure: the
            # probe either produced a hostname, an HTTP code and a process count, or it failed.
            if len(parts) != 3 or parts[0] not in all_hosts:
                if line.strip():
                    bad.append(f"unreadable: {line.strip()[:40]!r}")
                continue
            host, code, local = parts
            answered.add(host)
            if code != "200":
                bad.append(f"{host}={code}")
            if not local.isdigit():
                # Cannot tell whether local Ollama is running, and "not running" is the answer
                # that lets a host mix the 3B arm into a gateway run.
                bad.append(f"{host}=local-count-unreadable")
            elif int(local) > 0:
                serving.append(host)
    # A host that never answered has not passed the gate — it has skipped it. The whole point of
    # the gate is that an unprobed host runs an agent that falls back for the entire run.
    quiet = [h for h in all_hosts if h not in answered]
    if quiet:
        bad.extend(f"{h}=no-answer" for h in quiet)
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
    the shared Redis and stall the next run at [SEL_WAIT] live != configured.

    The delete is VERIFIED, not assumed. `_fan_out` swallows per-host failure (`2>/dev/null`),
    so a host that is briefly unreachable here keeps last run's log — and if it is reachable at
    collection time, that log is copied into this run dir with a fresh mtime and counted as this
    run's evidence. No freshness rule downstream can see that, because the file genuinely was
    copied now; only refusing to start fixes it.
    """
    for attempt in (1, 2):
        _fan_out(hosts(), f"pkill -9 -f main[.]py; rm -f {REPO}/swarm-multi/agent-*.log")
        _sh(f"pkill -9 -f main[.]py; rm -f {REPO}/swarm-multi/agent-*.log; "
            "docker exec redis redis-cli flushall >/dev/null")
        answers, silent, bad = _probe_hosts(
            f"echo $(hostname) $(ls {REPO}/swarm-multi/agent-*.log 2>/dev/null | wc -l)")
        left = [h for h, fields in answers.items() if fields[0] != 0]
        if not left and not silent and not bad:
            return
        if attempt == 2:
            detail = []
            if left:
                detail.append(f"stale agent logs still present on {len(left)}: "
                              f"{', '.join(sorted(left)[:8])}")
            if silent:
                detail.append(f"{len(silent)} host(s) did not answer the check "
                              f"({', '.join(silent[:8])})")
            if bad:
                detail.append(f"{len(bad)} unreadable answer(s): {'; '.join(bad[:4])}")
            raise SystemExit(
                "; ".join(detail) + ". A leftover log is collected into this run with a fresh copy "
                "time, which no freshness check downstream can detect — and an unreachable host is "
                "the most likely to be holding one, so silence cannot be read as clean. Clear the "
                "hosts before measuring.")
        print(f"  cleanup:        retrying — {len(left)} host(s) still holding logs, "
              f"{len(silent)} silent, {len(bad)} unreadable")


def set_disable_fallback(on: bool) -> int:
    """Write `llm.disable_fallback` into every per-agent config, and prove it landed.

    The figure-D ablation lives in the config the agents actually load, which under
    `--use-config-dir` is `configs/config_swarm_multi_<id>.yml` — editing the base
    `config_swarm_multi.yml` does nothing, because generate_configs.py is never called.

    Always called, with True or False, for the same reason `stop_fault()` always runs: the flag
    is a run-scoped mutation of a frozen fleet, and a leftover `true` from an earlier ablation is
    invisible in every metric except the one it changes. Called with False it strips the key, so a
    normal scenario cannot silently inherit it.

    Returns the number of configs carrying the flag, and refuses to continue if that is not the
    whole fleet (or zero, when turning it off) — a partially applied ablation would look like a
    smaller blast radius rather than like a broken setup.
    """
    paths = sorted(glob(f"{REPO}/configs/config_swarm_multi_*.yml"))
    if not paths:
        # Fatal when arming the ablation — there is nothing to write it into, so the run would
        # be an ordinary S05. Harmless when clearing it: no configs means nothing to clear, and
        # clear_faults.py must not fail on a fleet that has not been generated yet.
        if on:
            raise SystemExit(
                f"no per-agent configs in {REPO}/configs — restore the frozen fleet first")
        return 0
    for path in paths:
        lines = [ln for ln in open(path).read().splitlines(True)
                 if not ln.startswith("  disable_fallback:")]
        if on:
            # Anchored on the top-level `llm:` key, not on a sibling setting: the block's other
            # keys move between config revisions, and a missing anchor has to fail loudly here
            # rather than write the flag into some other section where it would be ignored.
            try:
                at = next(i for i, ln in enumerate(lines) if ln.rstrip("\n") == "llm:")
            except StopIteration:
                raise SystemExit(f"{path}: no top-level 'llm:' block to write disable_fallback into")
            lines.insert(at + 1, "  disable_fallback: true\n")
        open(path, "w").write("".join(lines))

    applied = sum(1 for p in paths
                  if any(ln.startswith("  disable_fallback: true")
                         for ln in open(p).read().splitlines()))
    want = len(paths) if on else 0
    if applied != want:
        raise SystemExit(f"disable_fallback={on} applied to {applied}/{len(paths)} configs, "
                         f"expected {want}")
    print(f"  ablation:       llm.disable_fallback={'true' if on else 'absent'} "
          f"in {len(paths)}/{len(paths)} per-agent configs")
    return applied


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
    # $(hostname) first: without it a returned line cannot be attributed, and a host that never
    # answered cannot be distinguished from one that answered "clean".
    return (f"echo $(hostname) $(grep -c OLLAMA_BASE_URL /root/.profile) "
            f"$(pgrep -fc '{_PROXY_PAT}' || true) "
            f"$(ss -lnt 2>/dev/null | grep -c ':{PROXY_PORT} ' || true)")


def assert_clean(strict: bool = False) -> None:
    """Fail before a run rather than after. Checks the port too: a stale CJ proxy keeps
    serving the previous fault, so a later scenario measures the earlier one.

    strict also rejects leftover agents and Redis state. Those are normally cleared by
    cleanup() at the start of a run, which means a scenario tidies up after its predecessor
    but never after itself — so the slice is left dirty whenever a batch ends.
    """
    answers, silent, bad = _probe_hosts(_state_probe(), fields=3)
    dirty = [h for h, fields in answers.items() if any(fields)]
    if dirty or silent or bad:
        why = []
        if dirty:
            why.append(f"dirty on {len(dirty)} host(s) ({', '.join(sorted(dirty)[:8])}) — leaked "
                       f"env var, driver, or a proxy still bound to :{PROXY_PORT}")
        # A host that did not answer is NOT a clean host, and neither is one whose answer cannot be
        # read. Either may be running the previous scenario's proxy, which will serve this run's
        # bids on the same port.
        if silent:
            why.append(f"{len(silent)} host(s) did not answer the probe "
                       f"({', '.join(silent[:8])}), so their state is unknown")
        if bad:
            why.append(f"{len(bad)} unreadable answer(s): {'; '.join(bad[:4])}")
        raise SystemExit(f"fleet is {'; '.join(why)}.\n"
                         f"run scenarios/clear_faults.py before measuring.")
    print(f"  clean check:    no leaked env vars / drivers / :{PROXY_PORT} listeners, "
          f"all {len(hosts())} hosts answering")

    # A config-side leak, which every probe above is blind to. `set_disable_fallback()` mutates
    # the frozen fleet's per-agent configs, so a leftover `true` from a killed ablation run
    # changes what the NEXT scenario measures while looking like a clean slice: an S01 latency
    # run would silently convert slow bids into refusals, and a baseline would stop being a
    # baseline. S05 writes the flag *after* this check, so the deliberate case still works and
    # only a leak trips it.
    leaked = [os.path.basename(p)
              for p in sorted(glob(f"{REPO}/configs/config_swarm_multi_*.yml"))
              if any(ln.startswith("  disable_fallback: true")
                     for ln in open(p).read().splitlines())]
    if leaked:
        raise SystemExit(
            f"llm.disable_fallback is still true in {len(leaked)} per-agent config(s) "
            f"({', '.join(leaked[:4])}) — the figure-D ablation leaked from an earlier run, and "
            f"every scenario reading these configs would measure it. Clear it with "
            f"scenarios/clear_faults.py, or helpers.set_disable_fallback(False).")
    print(f"  ablation check: llm.disable_fallback absent from every per-agent config")
    if strict:
        # Same rule as above, and it was missing here: this branch summed a fan-out with no
        # hostnames, so a host that never answered contributed 0 stray agents and the slice was
        # certified idle. clear_faults.py's "slice idle" is that certification, and a missed host
        # keeps agents that register into the shared Redis and stall the next run at
        # [SEL_WAIT] live != configured — the exact symptom this check exists to prevent.
        procs, quiet, bad = _probe_hosts('echo $(hostname) $(pgrep -fc "mai[n].py" || true)')
        # The decision is PER HOST, not on the total. A total can be cancelled — one host reporting
        # a negative against another's real count sums to zero — and "0 stray agents" is then a
        # false certification rather than a missing one. The sum is only reported, never trusted.
        # (_probe_hosts also refuses a negative outright now; this holds even if that changes.)
        busy = sorted(h for h, f in procs.items() if f[0] != 0)
        agents = sum(f[0] for f in procs.values())
        keys = int((_sh("docker exec redis redis-cli dbsize").strip() or "0").split()[-1])
        if busy or keys or quiet or bad:
            why = []
            if busy:
                why.append(f"{agents} stray agent process(es) on {', '.join(busy[:8])}")
            if keys:
                why.append(f"{keys} Redis key(s)")
            if quiet:
                why.append(f"{len(quiet)} host(s) did not answer ({', '.join(quiet[:8])}), so "
                           f"their agents are unaccounted for")
            if bad:
                why.append(f"{len(bad)} unreadable answer(s): {'; '.join(bad[:4])}")
            raise SystemExit(f"slice not idle: {'; '.join(why)}. Run scenarios/clear_faults.py.")
        print(f"  idle check:     0 stray agents across {len(procs)} hosts, 0 Redis keys")


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
    answers, silent, bad = _probe_hosts(_state_probe(), fields=3)
    dirty = [h for h, fields in answers.items() if any(fields)]
    if dirty:
        print(f"  !! teardown incomplete on {len(dirty)} host(s) "
              f"({', '.join(sorted(dirty)[:8])}) — next run will be invalid")
    # Reported separately and just as loudly: an unverified teardown is what leaks a fault into
    # the next scenario, and the host that cannot be reached — or whose answer cannot be read — is
    # the likely leaker.
    if silent or bad:
        print(f"  !! teardown UNVERIFIED on {len(silent) + len(bad)} host(s) "
              f"({', '.join((silent + bad)[:8])}) — no readable answer; assume the fault may "
              f"still be live there")


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

    # `CJ_SHUTDOWN_AFTER` bounds the run in wall-clock time. Needed because `--runtime` does NOT:
    # run_test.py parses it and never reads it (SwarmAgents finding 14). With it absent, run_test
    # takes the `wait_runtime()` branch, an UNBOUNDED loop that exits only when the pool bucket
    # drains below its threshold — so a run that cannot place jobs at all never exits on its own.
    # `--shutdown-after-seconds` switches to the deadline branch, which stops the agents and
    # collects their logs on the way out, instead of being killed with both still on the hosts.
    #
    # Only for runs expected NOT to drain. Leave it unset for everything else: a healthy run exits
    # on the drain condition well before any deadline, and a deadline would silently truncate the
    # slow points of a sweep into "the fault broke scheduling".
    bound = int(os.getenv("CJ_SHUTDOWN_AFTER", "0"))
    cmd = (
        f"cd {REPO} && nohup python3.11 run_test.py --mode remote --agent-type llm "
        f"--agents {AGENTS} --agents-per-host 1 --topology {TOPOLOGY} --jobs {JOBS} "
        f"--db-host database --agent-hosts-file agent_hosts_cj.txt --use-config-dir "
        f"--jobs-per-interval 30 --stable-seconds 120 --runtime {runtime} "
        + (f"--shutdown-after-seconds {bound} " if bound else "")
        + f"--generate-plots --run-dir {run_dir} > {log} 2>&1"
    )
    # The figure-D ablation is armed HERE, not in a scenario, so every scenario gets the same
    # lifecycle: written immediately before run_test copies the configs to the hosts, cleared in
    # the finally whatever happens. It used to live in s05_unavailable.py alone, which meant only
    # one scenario had the arm/disarm guarantee while baseline.py could quietly build a reference
    # under a set CJ_DISABLE_FALLBACK.
    ablation = os.getenv("CJ_DISABLE_FALLBACK", "").strip().lower() in ("1", "true", "yes")

    # Stamped BEFORE the run, and it is what tells this run's evidence from an earlier run's into
    # the same run dir. Agents start ~60-90 s after this point, so every log they write is newer.
    started = time.time()
    try:
        set_disable_fallback(ablation)
        _sh(cmd, timeout=(bound or runtime) + 900)
    finally:
        # Provenance of the run itself, next to its evidence: the window its logs must fall in,
        # and whether the ablation was on. Without this a run dir cannot say what produced it, and
        # a reference built under the ablation is indistinguishable from a normal one.
        try:
            os.makedirs(f"{REPO}/{run_dir}", exist_ok=True)
            json.dump({"started": started, "ended": time.time(), "disable_fallback": ablation},
                      open(f"{REPO}/{run_dir}/.run_window", "w"))
        except OSError as exc:
            print(f"  !! could not write {run_dir}/.run_window ({exc}) — log content cannot be "
                  f"checked against this run's time window")
        # Two teardowns, and the config restore must not be able to suppress the log collection:
        # the evidence is the only thing here that cannot be recreated.
        try:
            set_disable_fallback(False)
        finally:
            # Whatever happened to the run — finished, hung and killed, crashed — the agent logs
            # are the per-agent evidence and they live on the agent hosts until the next cleanup()
            # erases them. Pull them here rather than trusting run_test to reach its own step.
            snapshot_agent_logs(run_dir, since=started)


def snapshot_agent_logs(run_dir: str, since: float) -> None:
    """Copy each host's agent log into the run dir, and say exactly what was collected.

    run_test.py collects these itself, but only if it reaches the end of its wait. The S05 100%
    no-fallback run is what this exists for: nothing could be placed, the run sat in the poll
    loop, and by the time it was cleaned up the logs had been deleted from the hosts — leaving no
    evidence of the one thing the experiment was measuring, whether agents refused to bid.

    `since` is the run's start time and is REQUIRED, because every weaker rule ends up publishing
    another run's log as this one's. A log in the run dir is this run's evidence only if it was
    written after the run began; an older one came from an EARLIER run into the same dir. That is
    not cosmetic — collect(), load_split() and _restarts_and_conflicts() all glob the whole
    directory, so one stale file mixes two runs into every per-agent metric with nothing on screen
    to say so.

    Three rules, and the third is the one that is easy to get wrong:

      * **fresh stays.** run_test's own copy wins the race, and re-copying would only add a way to
        replace a complete log with a partial one.
      * **stale is set aside** as `*.log.stale` — renamed, not deleted, since it is still some
        run's evidence — which also drops it out of the `agent-*.log` glob those readers use.
      * **a failed refresh leaves the host EMPTY, never falling back to the file it displaced.**
        Setting a stale file aside and then failing to fetch a replacement must report a missing
        host, because "we could not collect this, so here is last time's" is the original bug
        wearing a different hat. For the same reason the `mv` out of `.incoming/` is gated on scp
        *succeeding*: a transfer that dies mid-file leaves a truncated log, and publishing that as
        complete is the same lie by a different mechanism.
    """
    if not run_dir or ".." in run_dir:
        raise SystemExit(f"refusing to snapshot into suspicious run_dir {run_dir!r}")
    dest_root = f"{REPO}/{run_dir}"
    all_hosts = hosts()

    stale, wanted = [], []
    for host in all_hosts:
        dest = f"{dest_root}/{host}"
        os.makedirs(dest, exist_ok=True)
        keep = []
        for path in sorted(glob(f"{dest}/agent-*.log")):
            # CONTENT decides, with the copy time only as a fallback. mtime is when the file was
            # scp'd (no -p), so a log an earlier run left on a host and this run fetched arrives
            # looking brand new; its dated lines are the only thing that disagrees. A file can also
            # be removed between the glob and the read — treated as absent, never raised, since
            # this runs in a `finally` after a ~20 minute run.
            body = _read_log(path)
            if body is None:
                continue
            span = _log_span(body)
            fresh = span[1] >= since if span else os.path.getmtime(path) >= since
            if fresh:
                keep.append(path)
                continue
            try:
                os.rename(path, f"{path}.stale")
            except OSError:
                continue
            stale.append(f"{host}/{os.path.basename(path)}"
                         + ("" if span else " (undated)"))
        if not keep:
            wanted.append(host)

    if wanted:
        parts = " ".join(
            # `&&` before the mv, not `;`: scp exits nonzero on a dead or half-finished transfer,
            # and an unconditional mv would publish the truncated file it left behind as this
            # run's complete log.
            f"(rm -rf {dest_root}/{h}/.incoming && mkdir -p {dest_root}/{h}/.incoming && "
            f"scp -q -o ConnectTimeout=10 -o StrictHostKeyChecking=no "
            f"{h}:{REPO}/swarm-multi/agent-*.log {dest_root}/{h}/.incoming/ 2>/dev/null && "
            f"mv {dest_root}/{h}/.incoming/agent-*.log {dest_root}/{h}/ 2>/dev/null; "
            f"rm -rf {dest_root}/{h}/.incoming) &"
            for h in wanted
        )
        _sh(parts + " wait", timeout=600)

    # Freshness, not existence, for the final accounting too. Transferred files carry the current
    # time (scp without -p), so this is equivalent today — but it stays correct if the transfer
    # ever preserves mtimes, and it means the reported count and the metric population are decided
    # by the same rule rather than by two rules that happen to agree.
    def _fresh(path: str) -> bool:
        body = _read_log(path)
        if body is None:     # removed under us; not evidence we hold
            return False
        span = _log_span(body)
        try:
            return span[1] >= since if span else os.path.getmtime(path) >= since
        except OSError:
            return False

    have = [h for h in all_hosts
            if any(_fresh(p) for p in glob(f"{dest_root}/{h}/agent-*.log"))]
    print(f"  agent logs:     {len(have)}/{len(all_hosts)} hosts collected under {run_dir}")
    # Both of these are printed because silence would read as complete evidence. A run missing a
    # host's log is not a run with a quiet host: every per-agent metric is then computed over a
    # smaller population than the fleet it divides by.
    if stale:
        print(f"  !! {len(stale)} log(s) predate this run and were set aside as *.log.stale "
              f"({', '.join(stale[:4])}) — an earlier run wrote into {run_dir}. They are excluded "
              f"from every metric; use a unique run dir per run.")
    missing = [h for h in all_hosts if h not in have]
    if missing:
        print(f"  !! no log collected from {len(missing)} host(s) ({', '.join(missing[:6])}) — "
              f"per-agent metrics are incomplete for this run.")


_LOG_TS = re.compile(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d+")


def _log_span(text: str) -> tuple[float, float] | None:
    """(first, last) log-line timestamps as epoch seconds, or None if the log carries none.

    This is the only thing that can tell a stale log from a current one. A file's mtime is its
    COPY time — scp writes it without -p — so a log left on a host by an earlier run and fetched
    during this one arrives looking brand new. Its contents give it away: the lines are dated.

    Assumes the orchestrator and the agent hosts share a timezone, which they do on the slice
    (both UTC). A skew would misdate every log at once and flag the whole fleet, which is loud;
    the failure mode being avoided here is the silent one.
    """
    head = _LOG_TS.search(text[:8192])
    tail = None
    for hit in _LOG_TS.finditer(text[-8192:]):
        tail = hit
    if not head:
        return None
    fmt = "%Y-%m-%d %H:%M:%S"
    first = datetime.strptime(head.group(1), fmt).timestamp()
    last = datetime.strptime(tail.group(1), fmt).timestamp() if tail else first
    return first, max(first, last)


def _read_log(path: str) -> str | None:
    """An agent log's text, or None if it is no longer there.

    Every reader globs the run dir and then opens what it found, and the two steps are not atomic:
    a concurrent cleanup, or a run dir being tidied, removes a file in between. These readers all
    run after a ~20 minute run, so raising would discard a completed measurement over one missing
    file. Callers count the None instead.
    """
    try:
        return open(path, errors="ignore").read()
    except OSError:
        return None


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
            # Which source the `restarts` row actually came from. report() needs this to say
            # whether the row survives an incomplete log collection: metrics.json is written from
            # Redis and does not, the fallback below is a sum over the logs and does.
            out["restarts_source"] = "metrics.json"

    marks = {"restart_log_lines": 0, "reselection_log_lines": 0}
    for p in sorted(glob(f"{REPO}/{run_dir}/**/agent-*.log", recursive=True)):
        body = _read_log(p)
        if body is None:
            continue
        marks["restart_log_lines"] += body.count("RESTART: Job:")
        marks["reselection_log_lines"] += body.count("leaving for reselection")
    out.update(marks)

    # If metrics.json was missing, fall back to the logs so the metric is never simply absent.
    # Recorded as such: on this path `restarts` IS log-derived, so an incomplete collection
    # undercounts it exactly like every other log sum, and report() must not advertise it as
    # independent of log collection.
    if "restarts" not in out:
        out["restarts"] = marks["restart_log_lines"]
        out["restarts_source"] = "agent logs"
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
    m = {"llm_complete": 0, "llm_fallback": 0, "llm_no_bid": 0, "swim_failed": 0}
    lat, scores = [], []
    # Every metric below this line is a sum over the logs that are PRESENT, and a log can be
    # absent — an unreachable host at collection time, or one whose log was deleted before it was
    # fetched. Counting them makes the population part of the measurement instead of a footnote:
    # 22 logs summed and reported as a 30-agent fleet understates every LLM count by a quarter,
    # and nothing else in the table reveals it.
    # The run's own provenance, written by run_swarm: the time window its logs must fall in, and
    # whether the ablation was armed. Absent for runs collected before this existed, and absence is
    # reported as "unknown", never as "fine".
    window = None
    try:
        window = json.load(open(f"{REPO}/{run_dir}/.run_window"))
    except (OSError, ValueError):
        pass
    if window:
        m["ablation_disable_fallback"] = bool(window.get("disable_fallback"))

    log_paths = sorted(glob(f"{REPO}/{run_dir}/**/agent-*.log", recursive=True))
    m["agent_logs"] = len(log_paths)
    # WHICH agents, not how many logs. A count matching the fleet proves nothing about coverage:
    # two logs for one agent and none for another is 30 files for 30 agents, and both the missing
    # and excess counters read zero while the sums double one agent and omit another. The readers
    # then disagree with each other, too — collect() sums every file, while load_split() keys
    # scores by id and lets the second file for an id overwrite the first.
    #
    # And WHICH agent is decided by the log's CONTENT, not its filename. A name is a label applied
    # by whoever copied the file; the agent writes its own identity into every line
    # ("… - agent-7 - INFO - …"). They disagree when a log is fetched into the wrong host dir or a
    # shard is off by one, and then a filename check certifies a population that is not the one
    # being summed. Only the body can settle it.
    log_ids: dict[int, int] = {}
    unattributable, mislabelled, mixed, vanished = 0, [], [], 0
    predating, outside, undated = [], [], []
    for path in log_paths:
        base = os.path.basename(path)
        text = _read_log(path)
        if text is None:
            # Globbed and then removed — a concurrent cleanup, or a run dir being tidied
            # underneath us. Counted, never raised.
            vanished += 1
            continue
        # Content against the run's window. A log copied during this run can still be an earlier
        # run's — mtime is the copy time — and a log the fleet APPENDED to across runs holds both,
        # inflating every count with lines that were never part of this measurement.
        if window:
            span = _log_span(text)
            if span is None:
                undated.append(base)
            elif span[1] < float(window["started"]):
                outside.append(base)
            elif span[0] < float(window["started"]):
                predating.append(base)

        named = re.search(r"agent-(\d+)\.log$", base)
        # EVERY id in the body, not the first one. A log can hold lines from more than one agent —
        # two agents started with the same log path, or a stale log appended across runs, which the
        # runbook already warns inflates counters. Trusting the first match attributes the whole
        # file, and all its counts, to whichever agent happened to write first.
        wrote = sorted({int(x) for x in re.findall(r"- agent-(\d+) - ", text)})
        if len(wrote) > 1:
            mixed.append(f"{base} holds agents {', '.join(str(i) for i in wrote[:4])}")
            unattributable += 1
        elif named and wrote and wrote[0] != int(named.group(1)):
            mislabelled.append(f"{base} written by agent-{wrote[0]}")
            unattributable += 1
        elif named:
            log_ids[int(named.group(1))] = log_ids.get(int(named.group(1)), 0) + 1
        elif wrote:
            log_ids[wrote[0]] = log_ids.get(wrote[0], 0) + 1
        else:
            unattributable += 1

        c = text.count("LLM_COST_COMPLETE")
        m["llm_complete"] += c
        m["llm_fallback"] += text.count("LLM_COST_FALLBACK")
        # The `llm.disable_fallback` ablation's marker: a failed call that returned +inf instead
        # of an analytic bid. It has to be counted here, because under that flag a failed call
        # leaves NO trace in llm_fallback — so the run reports "fallback rate 0.0%" and looks
        # like a healthy fleet, which is exactly what a silently inert ablation also looks like.
        m["llm_no_bid"] += text.count("LLM_COST_NO_BID")
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
    # Deliberately a SEPARATE rate rather than folding no-bids into fallback_rate: every result in
    # the campaign was measured with the old denominator, and changing it would silently move
    # numbers that are already published. Under the ablation fallback_rate goes to 0 by
    # construction and this is where the failed calls appear.
    attempts = calls + m["llm_no_bid"]
    m["no_bid_rate"] = round(m["llm_no_bid"] / attempts, 4) if attempts else 0.0
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
        # The fleet the run ACTUALLY launched, from its own log ("Launched 30 initial 'llm' agents
        # …", plus a line per dynamic batch). This is the only non-guessed configured count
        # available: `fleet_size` above is max(AGENTS, highest placing id), so for any run smaller
        # than the module constant — this repo has 10-, 14- and 60-agent sweep runs — it reports 30
        # and a population check against it invents phantom missing agents. Kept SEPARATE from
        # fleet_size, which is the published fairness denominator and must not move.
        launched = [int(n) for n in re.findall(r"Launched (\d+) \w+ '[^']*' agents", text)]
        if launched:
            m["fleet_configured"] = sum(launched)

    # The gap between the fleet and the logs actually read. Reported as a metric so it appears in
    # the table next to the numbers it qualifies, rather than only in the collection step's stdout
    # — which is a different screen, often a different day, and lost the moment anyone quotes the
    # table on its own.
    #
    # Both counters are computed from the ID SETS, not from the file count. `fleet - len(logs)`
    # would report a clean population for any run whose gaps and duplicates cancel out
    # numerically, which is the one case where every per-agent sum is wrong in two directions at
    # once. "0 missing, 0 extra" now means the logs are exactly agents 1..n, one each.
    #
    # But it can only mean that when the fleet is KNOWN, and known means READ, not defaulted. The
    # only non-guessed source is the run's own "Launched N … agents" line; `fleet_size` is
    # max(AGENTS, highest placing id) and would certify a 14-agent run against 30. With no such
    # line the verdict is withheld rather than guessed, and report() says it was withheld.
    m["agent_ids_mislabelled"] = mislabelled
    m["agent_ids_mixed"] = mixed
    m["agent_logs_vanished"] = vanished
    # None, not [], when there is no window to check against: an empty list would read as
    # "checked, nothing wrong" for exactly the runs that could not be checked.
    m["agent_logs_predating_run"] = predating if window else None
    m["agent_logs_outside_run"] = outside if window else None
    m["agent_logs_undated"] = undated if window else None
    m["run_window_known"] = bool(window)
    if not m.get("fleet_configured"):
        m["agent_population_verified"] = False
        for key in ("agent_ids_missing", "agent_ids_unexpected", "agent_ids_duplicated",
                    "agent_logs_missing", "agent_logs_extra"):
            m[key] = None
        return m

    fleet = m["fleet_configured"]
    expected = set(range(1, fleet + 1))
    m["agent_population_verified"] = True
    m["agent_ids_missing"] = sorted(expected - set(log_ids))
    m["agent_ids_unexpected"] = sorted(set(log_ids) - expected)
    m["agent_ids_duplicated"] = sorted(i for i, n in log_ids.items() if n > 1)
    m["agent_logs_missing"] = len(m["agent_ids_missing"])
    # Anything summed that is not one-log-per-fleet-agent: duplicate copies, ids outside the
    # fleet, files no id can be read from, and logs whose body names a different agent than their
    # name does. The last are excluded from log_ids rather than credited to either id — attributing
    # a mislabelled log to its filename is what a filename-only check silently does.
    m["agent_logs_extra"] = (sum(n - 1 for n in log_ids.values())
                             + len(m["agent_ids_unexpected"]) + unattributable)
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
        body = _read_log(path)
        if body is None:
            continue
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
    # First, because it is the population every log-derived row below is summed over. A run that
    # collected 22 of 30 logs is not a quieter fleet, it is a partly unmeasured one. Both gaps are
    # computed from agent IDS, not from the file count — the two disagree exactly when a run is
    # short and over-counted at once, which is the case a file-count check calls healthy.
    ("agent_logs", "agent logs read", "{}"),
    # Provenance of the run, not a measurement of it: whether the figure-D ablation was armed.
    # A reference built under it would otherwise be indistinguishable from a normal one.
    ("ablation_disable_fallback", "  fallback disabled", "{}"),
    # The run's own launched count, which is what the population check compares against. Printed so
    # a reader can see the check had a real fleet to check against, rather than a default.
    ("fleet_configured", "  fleet launched", "{}"),
    ("agent_logs_missing", "  agents with no log", "{}"),
    ("agent_logs_extra", "  logs in excess", "{}"),
    ("llm_complete", "LLM calls OK", "{}"),
    ("llm_fallback", "LLM fallbacks", "{}"),
    ("fallback_rate", "fallback rate", "{:.1%}"),
    # Only nonzero under `llm.disable_fallback: true`, and load-bearing exactly there: it is the
    # only evidence that agents tried to bid and refused. Without it, a run where the flag never
    # reached the agents is indistinguishable from one where it did.
    ("llm_no_bid", "LLM no-bids", "{}"),
    ("no_bid_rate", "no-bid rate", "{:.1%}"),
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

    # An incomplete population must be stated where the numbers are read, not just where they were
    # collected. But the consequence is NOT uniform, and saying "treat them as lower bounds" of
    # everything would be its own false claim: a sum over fewer logs really is a lower bound, while
    # a mean or a rate over fewer logs is simply a different population's statistic, wrong in
    # whichever direction the absent hosts differed. A missing slow bidder pulls the latency mean
    # DOWN, so quoting it as a floor is exactly backwards.
    for label, metrics in (("fault", fault), ("baseline", baseline)):
        # An unverifiable population is its own state, and it must not read as a clean one. This
        # fires only for runs collected by the current code that had no orchestrator log; a stored
        # reference from before the check simply has no opinion, which is not the same as False.
        if metrics.get("agent_population_verified") is False:
            print(f"\n  !! {label}: the agent population could not be checked — the run's log has "
                  f"no \"Launched N … agents\" line, so the configured fleet is unknown and there "
                  f"is nothing to compare {metrics.get('agent_logs', 0)} log(s) against. It is NOT "
                  f"assumed to be {AGENTS}: that constant is this campaign's fleet, not this run's. "
                  f"Every per-agent row above may be over or under the true figure; the population "
                  f"rows are blank rather than zero for that reason.")
        if metrics.get("run_window_known") is False:
            print(f"\n  !! {label}: no .run_window in the run dir, so the logs cannot be checked "
                  f"against the time this run actually covered. A log an earlier run left on a "
                  f"host and this one fetched arrives with a fresh copy time and is indetectable "
                  f"here; only its dated lines would have given it away.")
        for key, what in (
            ("agent_logs_outside_run",
             "dated entirely BEFORE this run — an earlier run's log, fetched during this one and "
             "counted in every sum above"),
            ("agent_logs_predating_run",
             "starting before this run and continuing into it — appended across runs, so their "
             "pre-run lines are counted in every sum above"),
            ("agent_logs_undated",
             "carrying no timestamps at all, so nothing can place them in time"),
        ):
            bad = metrics.get(key) or []
            if bad:
                print(f"\n  !! {label}: {len(bad)} log(s) {what} ({', '.join(bad[:4])}). "
                      f"Re-collect: cleanup() is supposed to make this impossible.")
        if metrics.get("agent_ids_mixed"):
            bad = metrics["agent_ids_mixed"]
            print(f"\n  !! {label}: {len(bad)} log(s) containing lines from MORE THAN ONE agent "
                  f"({'; '.join(bad[:4])}) — two agents sharing a log path, or a stale log appended "
                  f"across runs. Excluded from the population check, since attributing the file to "
                  f"any one of them would be a guess, but every line in it is counted in the sums "
                  f"above.")
        if metrics.get("agent_ids_mislabelled"):
            bad = metrics["agent_ids_mislabelled"]
            print(f"\n  !! {label}: {len(bad)} log(s) whose body names a different agent than "
                  f"their filename ({'; '.join(bad[:4])}). Collected into the wrong place, or a "
                  f"shard is off by one. They are excluded from the population check rather than "
                  f"credited to either id, but their contents ARE in every sum above.")
        if metrics.get("agent_logs_vanished"):
            print(f"\n  !! {label}: {metrics['agent_logs_vanished']} log(s) disappeared between "
                  f"being listed and being read — something is deleting this run dir while it is "
                  f"being measured. Re-collect before trusting any per-agent row.")
        gap = metrics.get("agent_logs_missing") or 0
        extra = metrics.get("agent_logs_extra") or 0
        if not gap and not extra:
            continue
        fleet = metrics.get("fleet_configured") or metrics.get("fleet_size", AGENTS)
        got = metrics.get("agent_logs", 0)
        # "agent logs", never "hosts": the count is log files, which equals agents. They coincide
        # with hosts only at --agents-per-host 1, which is this campaign's setup but not the
        # harness's only one.
        print(f"\n  !! {label}: {got} agent log(s) for a fleet of {fleet}, covering "
              f"{fleet - gap} of {fleet} agents. The per-agent rows above do not all degrade the "
              f"same way:")
        # Named, because the counts alone cannot be acted on and because a run can be short and
        # over-counted at the same time — the case a file-count check reads as healthy.
        def _ids(key: str) -> str:
            got_ids = metrics.get(key) or []
            return ", ".join(str(i) for i in got_ids[:8]) + ("…" if len(got_ids) > 8 else "")
        if metrics.get("agent_ids_missing"):
            print(f"     * no log for agent(s): {_ids('agent_ids_missing')}")
        if metrics.get("agent_ids_duplicated"):
            print(f"     * more than one log for agent(s): {_ids('agent_ids_duplicated')} — "
                  f"double-counted in every sum here, and silently deduplicated in load_split()")
        if metrics.get("agent_ids_unexpected"):
            print(f"     * log(s) for agent(s) outside the fleet: "
                  f"{_ids('agent_ids_unexpected')} — a stray log from a larger run")
        if extra and not (metrics.get("agent_ids_duplicated")
                          or metrics.get("agent_ids_unexpected")):
            print(f"     * {extra} log file(s) whose name carries no agent id, so nothing can "
                  f"attribute them")
        # `restarts` has two possible sources and only one of them survives a partial collection,
        # so which list it belongs in is decided per run, not written into the message.
        log_sourced_restarts = metrics.get("restarts_source") == "agent logs"
        sums = ("LLM calls OK, LLM fallbacks, LLM no-bids, SWIM false-fails, restart/reselect log "
                "lines" + (", and `job restarts` (this run has no metrics.json, so that row is a "
                           "log sum too)" if log_sourced_restarts else ""))
        # Three cases, not two. A run can be short AND over-counted at the same time — a duplicate
        # log for one agent and none for another — and then the sums are inflated by the duplicate
        # while missing the absent agent, so they bound the truth from NEITHER side. Calling that
        # a lower bound is the same species of false claim as calling a mean one.
        if gap and extra:
            verdict = ("wrong in BOTH directions (sums)", f"{sums}. Inflated by the excess log(s) "
                       f"and short by the uncovered agent(s) — not a bound in either direction.")
        elif gap:
            verdict = ("lower bounds (sums)", f"{sums}. The fleet's true figure is at least this.")
        else:
            verdict = ("over-counted (sums)",
                       f"{sums}. Each is inflated by whatever the excess log(s) contain.")
        print(f"     * {verdict[0]}: {verdict[1]}")
        print(f"     * biased in an UNKNOWN direction (rates and means): fallback rate, no-bid "
              f"rate, bid latency mean/p95, LLM score mean/sd, and any split's score means. These "
              f"describe the {got} logs read, not the fleet — not floors, not ceilings.")
        independent = ("jobs completed/stuck, placement, fairness, failed agents (orchestrator "
                       "log); scheduling-latency percentiles (all_jobs.csv)")
        if not log_sourced_restarts:
            independent += "; restart/conflict counts (metrics.json, written from Redis)"
        print(f"     * independent of log collection: {independent}.")
        if label == "fault" and "agent_logs" not in baseline:
            print(f"     * the stored baseline records no log population of its own, so the delta "
                  f"column compares this run's {fleet - gap}-agent coverage against a baseline "
                  f"whose own coverage is unverified.")

    print("\n  expected signals:")
    for line in expectations:
        print(f"    - {line}")
    print(bar)
