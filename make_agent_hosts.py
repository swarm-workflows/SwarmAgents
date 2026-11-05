#!/usr/bin/env python3.11
"""Emit an agent hosts file (and matching sites file) for a remote run.

Two things this exists to get right, both of which were done by hand and wrongly:

**Reachability.** A node returning from a rebuild has a new host key, and plain
`ssh` under `BatchMode` fails it with "Host key verification failed" — which a
pass/fail sweep cannot tell from a timeout. AMST (agent-68..74) was recorded as
down for a day that way while all seven nodes were up and answering, which would
have written a whole site out of the fleet. `StrictHostKeyChecking=accept-new`
trusts a first key but still refuses a *changed* one, so a genuinely swapped host
is reported (KEY) rather than waved through the way `=no` would wave it through.

**Placement.** `run_test.py` assigns agents to hosts in contiguous blocks in the
order this file lists them, and agent ids map to sites in contiguous blocks too.
So the obvious `sort -V` ordering *maximises* co-location: a whole hierarchical
group lands at one site and its consensus traffic never crosses the WAN — in a
testbed whose entire point is that it does. `--order interleaved` (the default)
round-robins across sites so each group spans sites; `--order sequential` keeps
the clustered ordering, which is what a deliberate site-outage scenario wants.

Sites are inferred from the /24 of each host's address, which is how the slice is
built: one subnet per site.

Usage:
    python3.11 make_agent_hosts.py --count 80 --out agent_hosts.txt \\
        --sites-out agent_sites.txt
    python3.11 make_agent_hosts.py --count 30 --order sequential --no-probe
"""
from __future__ import annotations

import argparse
import ipaddress
import socket
import subprocess
import sys
from collections import OrderedDict, defaultdict

SSH_OPTS = [
    "-o", "BatchMode=yes",
    "-o", "ConnectTimeout=6",
    # See the module docstring: not `=no`, which would also accept a CHANGED key.
    "-o", "StrictHostKeyChecking=accept-new",
]


def resolve(host: str) -> str | None:
    try:
        return socket.gethostbyname(host)
    except OSError:
        return None


def site_of(ip: str | None) -> str:
    """Site label for an address: the /24 it sits in, which is one site per subnet."""
    if not ip:
        return "unknown"
    try:
        return str(ipaddress.ip_network(f"{ip}/24", strict=False).network_address)
    except ValueError:
        return "unknown"


def probe(host: str) -> str:
    """'UP', 'KEY' (reachable but its host key changed) or 'DOWN'."""
    proc = subprocess.run(["ssh", *SSH_OPTS, host, "hostname"],
                          capture_output=True, text=True)
    if proc.returncode == 0:
        return "UP"
    if "host key" in (proc.stderr or "").lower():
        return "KEY"
    return "DOWN"


def interleave(by_site: dict[str, list[str]]) -> list[str]:
    """Order hosts so consecutive entries come from different sites where possible.

    At each step take from the site with the most hosts left, *excluding the one
    just used* — largest-remaining alone is not enough: with 6 hosts at one site
    and 2 at another it drains the big site first and emits a solid run of it,
    which is exactly the clustering this ordering exists to avoid. Taking the
    largest of the others keeps the big site from accumulating a tail it cannot
    spread later, and when one site holds more than half the fleet some adjacency
    is unavoidable — this reaches that floor rather than beating it.
    """
    pools = {s: list(h) for s, h in by_site.items()}
    ordered: list[str] = []
    previous: str | None = None
    while any(pools.values()):
        candidates = [s for s, hosts in pools.items() if hosts and s != previous]
        if not candidates:
            # Only the previous site has hosts left; adjacency is forced from here on.
            candidates = [s for s, hosts in pools.items() if hosts]
        site = min(candidates, key=lambda s: (-len(pools[s]), s))
        ordered.append(pools[site].pop(0))
        previous = site
    return ordered


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--prefix", default="agent-", help="Hostname prefix (default: agent-)")
    ap.add_argument("--first", type=int, default=1, help="First host index (default: 1)")
    ap.add_argument("--last", type=int, default=92, help="Last host index (default: 92)")
    ap.add_argument("--count", type=int, default=0,
                    help="Emit only this many hosts (0 = every reachable one)")
    ap.add_argument("--order", choices=["interleaved", "sequential"], default="interleaved",
                    help="interleaved: round-robin across sites, so a hierarchical group spans "
                         "the WAN (default). sequential: numeric order, which clusters each "
                         "group at one site — use it only for a deliberate site-outage test.")
    ap.add_argument("--out", default="agent_hosts.txt", help="Hosts file to write")
    ap.add_argument("--sites-out", default=None,
                    help="Also write site labels, one per line, parallel to the hosts file "
                         "(feed it to run_test.py --agent-sites-file for locality-weighted "
                         "Snow sampling)")
    ap.add_argument("--no-probe", action="store_true",
                    help="Skip the ssh reachability check and include every host that resolves")
    ap.add_argument("--jobs", type=int, default=40, help="Parallel probes (default: 40)")
    args = ap.parse_args()

    hosts = [f"{args.prefix}{i}" for i in range(args.first, args.last + 1)]

    if args.no_probe:
        states = {h: ("UP" if resolve(h) else "DOWN") for h in hosts}
    else:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max(1, args.jobs)) as pool:
            states = dict(zip(hosts, pool.map(probe, hosts)))

    up = [h for h in hosts if states[h] == "UP"]
    key = [h for h in hosts if states[h] == "KEY"]
    down = [h for h in hosts if states[h] == "DOWN"]

    by_site: dict[str, list[str]] = defaultdict(list)
    for h in up:
        by_site[site_of(resolve(h))].append(h)
    # Deterministic site order regardless of dict insertion.
    by_site = OrderedDict(sorted(by_site.items()))

    print(f"reachable: {len(up)}  key-changed: {len(key)}  down: {len(down)}  "
          f"sites: {len(by_site)}", file=sys.stderr)
    if key:
        print(f"  KEY (reachable, host key changed — verify before use): {' '.join(key)}",
              file=sys.stderr)
    if down:
        print(f"  DOWN: {' '.join(down)}", file=sys.stderr)
    for site, members in by_site.items():
        print(f"  site {site}: {len(members)} host(s)", file=sys.stderr)

    ordered = interleave(by_site) if args.order == "interleaved" else up

    if args.count:
        if args.count > len(ordered):
            print(f"ERROR: asked for {args.count} hosts but only {len(ordered)} are reachable. "
                  f"Size the run to the fleet you have, or bring hosts back — a short hosts "
                  f"file makes run_test.py fail at agent start, not here.", file=sys.stderr)
            return 1
        ordered = ordered[:args.count]

    with open(args.out, "w") as f:
        for h in ordered:
            f.write(h + "\n")
    print(f"wrote {len(ordered)} host(s) to {args.out} ({args.order})", file=sys.stderr)

    if args.sites_out:
        with open(args.sites_out, "w") as f:
            for h in ordered:
                f.write(site_of(resolve(h)) + "\n")
        print(f"wrote site labels to {args.sites_out}", file=sys.stderr)

    # How well the ordering spread the sites: with one agent per host, consecutive agent ids
    # land on consecutive lines, so this is the share of neighbouring agents at the same site.
    if len(ordered) > 1:
        sites = [site_of(resolve(h)) for h in ordered]
        same = sum(1 for a, b in zip(sites, sites[1:]) if a == b)
        print(f"adjacent same-site pairs: {same}/{len(ordered) - 1} "
              f"({100.0 * same / (len(ordered) - 1):.0f}%)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
