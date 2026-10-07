#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""Measure the host-to-host RTT matrix a run's consensus traffic actually crosses (T-2).

E3a / F3 bins every job by the RTT between the agent that ran it and the coordinator that
delegated it. Nothing measured RTT before this; the matrix is captured at run start, before any
agent is launched, so the probes neither load nor are loaded by the run.

**What is measured, and from where.** Each agent ADVERTISES `grpc.host` in its config, and that
is the address every peer dials. So the probe runs ON each agent's placement host (over the root
ssh mesh from `database`, as every launch does) and pings every other agent's advertised address
— the same name, resolved on the same machine, that its consensus messages go to. A row is also
taken from `database` itself, where Redis lives.

* ICMP, `samples` echo requests at `interval_s`, all destinations of one source in parallel;
  the **median** of the replies is the pair's RTT and `1 - replies/samples` its loss.
* A pair with no reply is `null`, never 0 — a 0 would bin an unmeasured pair as LAN.
* Hosts are de-duplicated: several agents on one VM share a row, and an agent's RTT to an agent
  on its own VM is 0 (loopback), which is what co-location really costs.

    python rtt_matrix.py --hosts agent-1,agent-2,agent-3 --out rtt_matrix.json
"""
from __future__ import annotations

import argparse
import json
import re
import shlex
import statistics
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, Iterable, List, Optional

SSH_OPTS = ["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
            "-o", "BatchMode=yes", "-o", "ConnectTimeout=10"]

_TIME = re.compile(r"time[=<]\s*([\d.]+)\s*ms")
_MARK = "### "


def probe_script(targets: Iterable[str], samples: int, interval_s: float) -> str:
    """A shell script that pings every target in parallel and prints each result in a block
    headed `### <target>`. Each ping writes its own file, so outputs cannot interleave."""
    names = " ".join(shlex.quote(t) for t in targets)
    return (
        "tmp=$(mktemp -d); "
        f"for d in {names}; do "
        f"ping -n -c {int(samples)} -i {interval_s} -W 2 \"$d\" > \"$tmp/$d\" 2>&1 & "
        "done; wait; "
        f"for d in {names}; do echo \"{_MARK}$d\"; cat \"$tmp/$d\"; done; "
        "rm -rf \"$tmp\""
    )


def parse_probe_output(text: str, samples: int) -> Dict[str, dict]:
    """`{target: {"rtt_ms": median | None, "loss": fraction, "replies": n}}`."""
    out: Dict[str, dict] = {}
    current: Optional[str] = None
    times: List[float] = []

    def close():
        if current is None:
            return
        out[current] = {
            "rtt_ms": round(statistics.median(times), 3) if times else None,
            "loss": round(1.0 - len(times) / samples, 3) if samples else None,
            "replies": len(times),
        }

    for line in text.splitlines():
        if line.startswith(_MARK):
            close()
            current, times = line[len(_MARK):].strip(), []
            continue
        m = _TIME.search(line)
        if m and current is not None:
            times.append(float(m.group(1)))
    close()
    return out


def _ssh_runner(host: str, script: str, timeout_s: float) -> str:
    result = subprocess.run(["ssh", *SSH_OPTS, host, script], text=True,
                            capture_output=True, timeout=timeout_s)
    if result.returncode != 0 and not result.stdout:
        raise RuntimeError(result.stderr.strip() or f"ssh {host} exited {result.returncode}")
    return result.stdout


def _local_runner(script: str, timeout_s: float) -> str:
    return subprocess.run(["bash", "-c", script], text=True, capture_output=True,
                          timeout=timeout_s).stdout


def measure(agent_hosts: Dict[int, str], placement: Dict[int, str], samples: int = 20,
            interval_s: float = 0.2, parallel: int = 32, include_database: bool = True,
            ssh: Callable[[str, str, float], str] = _ssh_runner,
            local: Callable[[str, float], str] = _local_runner) -> dict:
    """The RTT matrix over the advertised addresses of *agent_hosts*.

    *agent_hosts*: `{agent_id: advertised grpc.host}` — the probe targets.
    *placement*: `{agent_id: host the agent runs on}` — where each row is measured from (the
    ssh target). Usually the same name; kept separate because the question is "from where
    this agent runs, to the address its peers advertise".
    """
    started = time.time()
    targets = sorted(set(agent_hosts.values()))
    # One source per distinct advertised host; measured from that agent's placement host.
    source_for: Dict[str, str] = {}
    for aid in sorted(agent_hosts):
        source_for.setdefault(agent_hosts[aid], placement.get(aid, agent_hosts[aid]))
    timeout_s = samples * interval_s + 60.0
    script = probe_script(targets, samples, interval_s)

    rows: Dict[str, Dict[str, Optional[float]]] = {}
    loss: Dict[str, Dict[str, Optional[float]]] = {}
    failed: Dict[str, str] = {}

    def one(adv: str):
        try:
            return adv, parse_probe_output(ssh(source_for[adv], script, timeout_s), samples), None
        except Exception as exc:          # a source that cannot be reached is a null row
            return adv, {}, str(exc)

    with ThreadPoolExecutor(max_workers=max(1, int(parallel))) as pool:
        for adv, parsed, err in pool.map(one, targets):
            if err:
                failed[adv] = err
            rows[adv] = {t: (0.0 if t == adv else (parsed.get(t) or {}).get("rtt_ms"))
                         for t in targets}
            loss[adv] = {t: (0.0 if t == adv else (parsed.get(t) or {}).get("loss"))
                         for t in targets}
            if err:
                for t in targets:
                    if t != adv:
                        rows[adv][t] = None
                        loss[adv][t] = None

    database = None
    if include_database:
        try:
            parsed = parse_probe_output(local(script, timeout_s), samples)
            database = {t: (parsed.get(t) or {}).get("rtt_ms") for t in targets}
        except Exception as exc:
            failed["database"] = str(exc)

    pairs = [(s, t) for s in targets for t in targets if s != t]
    missing = sum(1 for s, t in pairs if rows[s][t] is None)
    return {
        # captured: every pair answered; partial: some did not (their entries are null);
        # failed: none did — nothing to bin a job by.
        "status": ("captured" if missing == 0 else
                   "partial" if missing < len(pairs) else "failed"),
        "method": "icmp", "samples": int(samples), "interval_s": float(interval_s),
        "captured_at": started, "duration_s": round(time.time() - started, 2),
        "agents": {str(a): h for a, h in sorted(agent_hosts.items())},
        "hosts": targets,
        "rtt_ms": rows, "loss": loss, "database_rtt_ms": database,
        "pairs": len(pairs), "pairs_missing": missing,
        "unreachable_sources": failed,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hosts", required=True,
                    help="Comma-separated hosts; agent i is the i-th (1-based).")
    ap.add_argument("--out", required=True)
    ap.add_argument("--samples", type=int, default=20)
    ap.add_argument("--interval", type=float, default=0.2)
    ap.add_argument("--parallel", type=int, default=32)
    args = ap.parse_args()
    hosts = [h.strip() for h in args.hosts.split(",") if h.strip()]
    agents = {i: h for i, h in enumerate(hosts, start=1)}
    result = measure(agents, agents, samples=args.samples, interval_s=args.interval,
                     parallel=args.parallel)
    with open(args.out, "w") as fh:
        json.dump(result, fh, indent=2)
    print(f"{result['status']}: {result['pairs'] - result['pairs_missing']}/{result['pairs']} "
          f"pairs in {result['duration_s']} s -> {args.out}")
    return 0 if result["status"] != "failed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
