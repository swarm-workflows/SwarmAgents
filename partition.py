#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""Partition the fleet by site for a timed interval, then heal it (T-3, E6).

What is cut from what (plan §4.2 E6): **agent <-> agent traffic between two site groups**, both
directions, every protocol — consensus, SWIM, gossip, staging all cross that boundary — with
**Redis on `database` reachable from both sides throughout**. That is the only partition this
system can live through at all (an agent cut from Redis can do nothing), and it is the case the
eScience reviewer raised: each side still reads every agent's heartbeat from Redis, so each
infers its own live count, and only the consensus traffic between them is gone. A Redis-side
partition is not produced and the paper does not claim it.

Run on the `database` node as root:

    # standalone: cut now, hold 180 s, heal
    python3.11 partition.py run --hosts-file agent_hosts.txt --sites-file agent_sites.txt \\
        --agents 30 --duration 180 --out runs/x/partition.json

    # as a campaign companion: wait for the run to start distributing jobs, then cut
    python3.11 partition.py run ... --start-after-log runs/x/run01.log --delay 120

    python3.11 partition.py heal --token <token> --hosts-file agent_hosts.txt   # by hand

**Mechanism.** On each host of side A, one iptables chain `SWP_<token>` DROPs packets to and from
every advertised address of side B (and vice versa), hooked first in INPUT and OUTPUT. The
`database` address is never in either side, so Redis and the ssh control path are untouched.
One chain per partition, named by a random token: healing removes exactly that chain, never
another run's.

**It always heals.** The driver heals on normal exit, on SIGTERM/SIGINT/SIGHUP, and on any
error; and every host additionally arms a **dead-man timer** (`systemd-run --on-active`) that
removes the chain on its own `--deadman-grace` seconds after the planned heal, so a driver
killed with SIGKILL cannot leave the fleet partitioned. The timer is cancelled when the driver
heals normally.

**Verified, not assumed.** After applying, a host on each side pings an address across the cut
(must fail), one on its own side (must answer), and `database` (must answer); after healing, the
cross-cut ping must answer again. The verdicts go into the output JSON beside the sides, the
addresses, and the apply/heal timestamps that analysis needs to window the run. A partition
whose probes disagree with the plan is reported as such and the exit status is non-zero.

**Exit status:** 0 the partition was applied to every host, verified on every host, held for
the full duration, healed and verified; 1 a host failed to apply or heal, or a probe disagreed;
2 the run never started; 3 the partition never applied, or was held for less than planned
(`held_s` in the record) — a cell that did not get the partition it claims.
"""
from __future__ import annotations

import argparse
import json
import math
import secrets
import shlex
import signal
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

SSH_OPTS = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
            "-o", "StrictHostKeyChecking=accept-new"]


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def ssh_run(host: str, script: str, timeout: float = 60.0) -> Tuple[int, str]:
    proc = subprocess.run(["ssh", *SSH_OPTS, host, script], capture_output=True, text=True,
                          timeout=timeout)
    return proc.returncode, (proc.stdout + proc.stderr).strip()


def read_lines(path: str) -> List[str]:
    return [l.strip() for l in Path(path).read_text().splitlines()
            if l.strip() and not l.startswith("#")]


# --------------------------------------------------------------------------- planning

def cell_hosts(hosts: List[str], sites: List[str], agents: int,
               agents_per_host: int) -> List[Tuple[str, str]]:
    """The (host, site) pairs the cell runs on: the first ceil(agents/per_host) lines."""
    need = math.ceil(agents / agents_per_host)
    if need > len(hosts):
        raise SystemExit(f"{agents} agents at {agents_per_host}/host need {need} hosts; the "
                         f"hosts file lists {len(hosts)}")
    if len(sites) < need:
        raise SystemExit(f"the sites file has {len(sites)} labels for {need} hosts")
    return list(zip(hosts[:need], sites[:need]))


def split_by_site(pairs: List[Tuple[str, str]],
                  side_a_sites: Optional[List[str]] = None) -> Tuple[List[str], List[str]]:
    """Two host lists, never splitting a site. With no explicit side A, sites are assigned
    greedily, largest first, to the smaller side — as even a cut as whole sites allow."""
    by_site: Dict[str, List[str]] = {}
    for host, site in pairs:
        by_site.setdefault(site, []).append(host)
    if len(by_site) < 2:
        raise SystemExit("the cell's hosts span one site; there is nothing to partition")
    if side_a_sites:
        unknown = set(side_a_sites) - set(by_site)
        if unknown:
            raise SystemExit(f"--side-a-sites names sites the cell does not use: {sorted(unknown)}")
        a_sites = set(side_a_sites)
    else:
        a_sites, a_n, b_n = set(), 0, 0
        for site in sorted(by_site, key=lambda s: (-len(by_site[s]), s)):
            if a_n <= b_n:
                a_sites.add(site)
                a_n += len(by_site[site])
            else:
                b_n += len(by_site[site])
    side_a = [h for s in sorted(a_sites) for h in by_site[s]]
    side_b = [h for s in sorted(set(by_site) - a_sites) for h in by_site[s]]
    if not side_a or not side_b:
        raise SystemExit("a side is empty; choose --side-a-sites that leave hosts on both sides")
    return side_a, side_b


def resolve(hosts: List[str], resolver: Optional[Callable[[str], str]] = None
            ) -> Dict[str, str]:
    resolver = resolver or socket.gethostbyname
    return {h: resolver(h) for h in hosts}


# --------------------------------------------------------------------------- iptables

def chain(token: str) -> str:
    return f"SWP_{token}"        # iptables chain names are limited to 28 characters


def apply_script(token: str, block_ips: List[str], deadman_s: int) -> str:
    c = chain(token)
    rules = " ".join(f"iptables -A {c} -s {ip} -j DROP && iptables -A {c} -d {ip} -j DROP &&"
                     for ip in block_ips)
    heal = heal_script(token, cancel_timer=False)
    return (f"set -e; iptables -N {c} 2>/dev/null || iptables -F {c}; {rules} true; "
            f"iptables -C INPUT -j {c} 2>/dev/null || iptables -I INPUT 1 -j {c}; "
            f"iptables -C OUTPUT -j {c} 2>/dev/null || iptables -I OUTPUT 1 -j {c}; "
            # Dead-man: removes this chain on its own if nobody heals it.
            f"systemd-run --quiet --unit swp-{token} --on-active={int(deadman_s)} "
            f"/bin/sh -c {shlex.quote(heal)}; "
            f"echo applied $(iptables -S {c} | grep -c DROP)")


def heal_script(token: str, cancel_timer: bool = True) -> str:
    c = chain(token)
    cancel = (f"systemctl stop swp-{token}.timer >/dev/null 2>&1 || true; "
              if cancel_timer else "")
    return (f"{cancel}iptables -D INPUT -j {c} 2>/dev/null || true; "
            f"iptables -D OUTPUT -j {c} 2>/dev/null || true; "
            f"iptables -F {c} 2>/dev/null || true; iptables -X {c} 2>/dev/null || true; "
            f"iptables -S {c} >/dev/null 2>&1 && echo still-present || echo healed")


def probe_script(target: str) -> str:
    return f"ping -n -c 2 -W 1 {shlex.quote(target)} >/dev/null 2>&1 && echo up || echo down"


# --------------------------------------------------------------------------- the driver

class Partition:
    def __init__(self, side_a: List[str], side_b: List[str], addresses: Dict[str, str],
                 database: str, duration_s: float, deadman_grace_s: float = 120.0,
                 runner: Optional[Callable[[str, str], Tuple[int, str]]] = None,
                 token: Optional[str] = None):
        self.side_a, self.side_b = side_a, side_b
        self.addr, self.database = addresses, database
        self.duration_s, self.grace = float(duration_s), float(deadman_grace_s)
        self.run = runner or ssh_run
        self.token = token or secrets.token_hex(4)
        self.applied_hosts: List[str] = []
        self.record: dict = {"token": self.token, "chain": chain(self.token),
                             "side_a": side_a, "side_b": side_b,
                             "addresses": addresses, "database": database,
                             "duration_s": duration_s}

    def _fan(self, fn, hosts):
        with ThreadPoolExecutor(max_workers=40) as pool:
            return dict(zip(hosts, pool.map(fn, hosts)))

    def apply(self) -> bool:
        deadman = int(self.duration_s + self.grace)
        a_ips = sorted({self.addr[h] for h in self.side_a})
        b_ips = sorted({self.addr[h] for h in self.side_b})

        def one(host):
            other = b_ips if host in self.side_a else a_ips
            return self.run(host, apply_script(self.token, other, deadman))

        results = self._fan(one, self.side_a + self.side_b)
        self.record["applied_at"] = time.time()
        self.applied_hosts = [h for h, (rc, _) in results.items() if rc == 0]
        failed = {h: out[-200:] for h, (rc, out) in results.items() if rc != 0}
        # A zero exit is not enough: every host must hold exactly two DROP rules per address on
        # the other side. A host with fewer is a partition with a hole in it.
        for h, (rc, out) in results.items():
            if rc != 0:
                continue
            want = 2 * len(b_ips if h in self.side_a else a_ips)
            got = next((int(w) for line in out.splitlines() if line.startswith("applied")
                        for w in line.split()[1:2] if w.isdigit()), None)
            if got != want:
                failed[h] = f"{got} DROP rules installed, {want} required"
        self.record["apply_failed"] = failed
        self.record["deadman_s"] = deadman
        if failed:
            log(f"apply failed on {len(failed)} host(s): {sorted(failed)[:8]}")
        return not failed

    def verify(self, expect_cut: bool) -> dict:
        """EVERY host probes one address across the cut (rotating through the other side) and
        `database`; one probe per side would accept a partition with holes in it."""
        want_cross = "down" if expect_cut else "up"

        def one(host):
            other = self.side_b if host in self.side_a else self.side_a
            mine = self.side_a if host in self.side_a else self.side_b
            idx = (self.side_a + self.side_b).index(host)
            target = other[idx % len(other)]
            same = [h for h in mine if h != host]
            res = {"cross": self.run(host, probe_script(self.addr[target]))[1],
                   "database": self.run(host, probe_script(self.database))[1]}
            if same:
                res["same"] = self.run(host, probe_script(self.addr[same[0]]))[1]
            return res

        per_host = self._fan(one, self.side_a + self.side_b)
        bad = {h: r for h, r in per_host.items()
               if r["cross"] != want_cross or r["database"] != "up"
               or r.get("same", "up") != "up"}
        return {"expect_cut": expect_cut, "hosts": len(per_host), "bad": bad,
                "checks": {"cross_" + want_cross: sum(r["cross"] == want_cross
                                                      for r in per_host.values()),
                           "database_up": sum(r["database"] == "up" for r in per_host.values())},
                "ok": not bad}

    def heal(self) -> bool:
        hosts = self.side_a + self.side_b
        results = self._fan(lambda h: self.run(h, heal_script(self.token)), hosts)
        self.record["healed_at"] = time.time()
        bad = {h: out for h, (rc, out) in results.items() if "healed" not in out}
        self.record["heal_failed"] = bad
        if bad:
            log(f"heal NOT confirmed on {len(bad)} host(s): {sorted(bad)[:8]} — the dead-man "
                f"timer removes the chain {self.record.get('deadman_s')} s after it was applied")
        return not bad


def wait_for_log(path: str, marker: str, timeout_s: float) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            if marker in Path(path).read_text(errors="replace"):
                return True
        except OSError:
            pass
        time.sleep(2.0)
    return False


class _Stop(Exception):
    pass


def cmd_run(args) -> int:
    hosts = read_lines(args.hosts_file)
    sites = read_lines(args.sites_file)
    pairs = cell_hosts(hosts, sites, args.agents, args.agents_per_host)
    side_a, side_b = split_by_site(
        pairs, [s for s in (args.side_a_sites or "").split(",") if s] or None)
    addresses = resolve(side_a + side_b)
    database = socket.gethostbyname(args.database)
    if database in addresses.values():
        raise SystemExit(f"{args.database} ({database}) is one of the cell's hosts; the cut "
                         f"would take Redis with it")
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    part = Partition(side_a, side_b, addresses, database, args.duration,
                     deadman_grace_s=args.deadman_grace)
    part.record.update({"sites_a": sorted({s for h, s in pairs if h in side_a}),
                        "sites_b": sorted({s for h, s in pairs if h in side_b})})

    def _term(*_):
        raise _Stop()

    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, _term)

    def write():
        out.write_text(json.dumps(part.record, indent=2))

    status = 0
    applied = False
    held_from = None          # set only once the cut is in place on every host
    try:
        if args.start_after_log:
            log(f"waiting for {args.start_marker!r} in {args.start_after_log}")
            if not wait_for_log(args.start_after_log, args.start_marker, args.start_timeout):
                part.record["outcome"] = "run_never_started"
                write()
                return 2
        if args.delay:
            log(f"partition in {args.delay:.0f} s")
            time.sleep(args.delay)
        log(f"cutting {len(side_a)} host(s) [{','.join(part.record['sites_a'])}] from "
            f"{len(side_b)} [{','.join(part.record['sites_b'])}] for {args.duration:.0f} s "
            f"(chain {chain(part.token)})")
        applied = True
        if not part.apply():
            status = 1
        # The hold starts when the cut is IN PLACE on every host, not when applying began:
        # counting the apply (and the verification below it is measured from) toward the hold
        # let a partition stopped short of its duration pass as complete (stop-time review).
        held_from = time.time()
        part.record["verify_cut"] = part.verify(expect_cut=True)
        write()
        if not part.record["verify_cut"]["ok"]:
            log(f"cut NOT as planned: {part.record['verify_cut']['checks']}")
            status = 1
        else:
            log("cut verified: cross-site down, own side and database up")
        time.sleep(args.duration)
    except _Stop:
        log("stopped early; healing now")
        part.record["stopped_early"] = True
    finally:
        # Incomplete is not success. A partition that never applied (the run ended first, or
        # the companion was stopped while waiting) or was held for less than planned did not
        # produce the experiment the cell claims, so it must not exit 0 — the campaign would
        # count the cell ok (stop-time review). Exit 3, with what was actually held recorded.
        held = round(max(0.0, time.time() - held_from), 1) if held_from is not None else 0.0
        part.record["held_s"] = held
        if not applied:
            part.record["outcome"] = "not_applied"
            status = status or 3
        elif held_from is None:
            # Stopped while the rules were going on: some hosts may have been cut, none for
            # the planned hold. Not a partition the cell can claim.
            part.record["outcome"] = "interrupted_during_apply"
            status = status or 3
        elif held + 1.0 < args.duration:
            part.record["outcome"] = "held_short"
            status = status or 3
        if applied:
            if not part.heal():
                status = 1
            part.record["verify_heal"] = part.verify(expect_cut=False)
            if not part.record["verify_heal"]["ok"]:
                log(f"heal NOT verified: {part.record['verify_heal']['checks']}")
                status = 1
            else:
                log("healed and verified")
        part.record.setdefault("outcome", "ok" if status == 0 else "problems")
        if status:
            log(f"partition {part.record['outcome']}: held {part.record.get('held_s')} s of "
                f"{args.duration:.0f}; exit {status}")
        write()
    return status


def cmd_heal(args) -> int:
    hosts = read_lines(args.hosts_file)
    results = {}
    with ThreadPoolExecutor(max_workers=40) as pool:
        for h, (rc, out) in zip(hosts, pool.map(lambda h: ssh_run(h, heal_script(args.token)),
                                                 hosts)):
            results[h] = out
    bad = [h for h, out in results.items() if "healed" not in out]
    log(f"healed on {len(hosts) - len(bad)}/{len(hosts)} host(s)"
        + (f"; unconfirmed: {bad[:10]}" if bad else ""))
    return 0 if not bad else 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="apply, hold, heal")
    r.add_argument("--hosts-file", required=True)
    r.add_argument("--sites-file", required=True,
                   help="one site label per hosts-file line (make_agent_hosts.py --sites-out)")
    r.add_argument("--agents", type=int, required=True)
    r.add_argument("--agents-per-host", type=int, default=1)
    r.add_argument("--side-a-sites", default="",
                   help="comma-separated site labels for side A (default: balanced by hosts)")
    r.add_argument("--duration", type=float, default=180.0)
    r.add_argument("--delay", type=float, default=0.0,
                   help="seconds to wait (after --start-after-log, if given) before cutting")
    r.add_argument("--start-after-log", default="",
                   help="wait for --start-marker to appear in this file first (the run's log)")
    r.add_argument("--start-marker", default="Job distribution started")
    r.add_argument("--start-timeout", type=float, default=1800.0)
    r.add_argument("--deadman-grace", type=float, default=120.0,
                   help="each host heals itself this long after the planned heal")
    r.add_argument("--database", default="database")
    r.add_argument("--out", required=True, help="partition record (JSON)")
    h = sub.add_parser("heal", help="remove a partition by token, on every host listed")
    h.add_argument("--token", required=True)
    h.add_argument("--hosts-file", required=True)
    args = ap.parse_args(argv)
    return cmd_run(args) if args.cmd == "run" else cmd_heal(args)


if __name__ == "__main__":
    sys.exit(main())
