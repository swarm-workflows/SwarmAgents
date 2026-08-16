#!/usr/bin/env python3
"""
Generate the db-node setup plan for the SWARM two-slice deployment.

Run this LOCALLY (laptop / JupyterHub) in the same environment as
SWARM-2slice.ipynb. It talks ONLY to the FABRIC control plane (orchestrator
API) -- it never SSHes to any node -- so it works even when direct SSH to the
VMs times out.

It replicates the notebook's deterministic IP assignment (skip first two
hosts of each subnet, then assign in interface order) and emits everything
the on-database-node scripts need:

    plan/inventory.json      full structured inventory (reference)
    plan/nodes.tsv           name  user  mgmt_ip  swarm(1/0)  site
    plan/nics.tsv            name  mac  ip/prefix  gateway  lan_net
    plan/etc_hosts.txt       hosts block (agent-N + agent-N-mon + database + monitor)
    plan/prometheus.yml      scrape config for the monitor VM
    plan/config.env          BRANCH / DB_NODE / MONITOR_NODE / AGENT_COUNT
    plan/bastion.env         BASTION_HOST / BASTION_USER (for SSH fallback)
    plan/upload.env          local key paths, used by upload_to_db.sh

Then: ./upload_to_db.sh swarm    (see README.md)
"""

import argparse
import json
import os
import sys


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--project-id", default="3a05ccb3-a4b9-4bc8-9bc8-4c8eb65c9d3e")
    ap.add_argument("--total-agents", type=int, default=100)
    ap.add_argument("--base-slice-name", default="SWARM-MULTI")
    ap.add_argument("--slice1", default=None, help="override slice 1 name")
    ap.add_argument("--slice2", default=None, help="override slice 2 name")
    ap.add_argument("--db-node", default="database")
    ap.add_argument("--monitor-node", default="monitor")
    ap.add_argument("--network-prefix", default="fabv4")
    ap.add_argument("--mon-network-prefix", default="fabv4mon")
    ap.add_argument("--branch", default="agent-recovery")
    ap.add_argument("--out", default=os.path.join(here, "plan"))
    args = ap.parse_args()

    from fabrictestbed_extensions.fablib.fablib import FablibManager as fablib_manager
    fablib = fablib_manager(project_id=args.project_id)

    slice_name_1 = args.slice1 or f"{args.base_slice_name}-{args.total_agents}-p1"
    slice_name_2 = args.slice2 or f"{args.base_slice_name}-{args.total_agents}-p2"

    print(f"Loading slices: {slice_name_1}, {slice_name_2}")
    slice1 = fablib.get_slice(slice_name_1)
    slice2 = fablib.get_slice(slice_name_2)

    nodes = list(slice1.get_nodes()) + list(slice2.get_nodes())
    networks = list(slice1.get_networks()) + list(slice2.get_networks())
    node_by_name = {n.get_name(): n for n in nodes}
    nw_by_name = {nw.get_name(): nw for nw in networks
                  if nw.get_name().startswith(args.network_prefix)}

    assert args.db_node in node_by_name, "database node not found -- is slice 1 up?"
    assert args.monitor_node in node_by_name, "monitor node not found -- is slice 1 up?"
    print(f"Combined fleet: {len(nodes)} nodes, {len(nw_by_name)} networks")

    monitor_site = node_by_name[args.monitor_node].get_site()
    monitor_mon_nw = nw_by_name[f"{args.mon_network_prefix}-{monitor_site}"]
    monitor_mon_subnet = monitor_mon_nw.get_subnet()

    # ------------------------------------------------------------------
    # Per-node management info (control plane only)
    # ------------------------------------------------------------------
    node_info = {}
    for n in nodes:
        name = n.get_name()
        node_info[name] = {
            "user": n.get_username(),
            "mgmt_ip": str(n.get_management_ip()),
            "site": n.get_site(),
            "swarm": name != args.monitor_node,
            "nics": [],
        }

    # ------------------------------------------------------------------
    # Deterministic IP assignment -- EXACT replica of the notebook cell:
    # skip the first two hosts of each subnet, then assign in the order
    # nw.get_interfaces() returns.
    # ------------------------------------------------------------------
    assigned_ip = {}   # (network_name, node_name) -> ip str
    for nw_name in sorted(nw_by_name):
        nw = nw_by_name[nw_name]
        subnet = nw.get_subnet()
        gateway = str(nw.get_gateway())
        is_mon = nw_name.startswith(args.mon_network_prefix)
        hiter = subnet.hosts()
        ip = next(hiter)                 # skip first host
        ip = next(hiter)                 # skip second host (first assigned)
        for iface in nw.get_interfaces():
            node_name = iface.get_node().get_name()
            mac = (iface.get_mac() or "").lower()
            if not mac:
                sys.exit(f"ERROR: no MAC for {node_name} on {nw_name}; "
                         f"is the slice fully instantiated?")

            if is_mon and node_name == args.monitor_node:
                lan_net = "10.128.0.0/10"          # monitor reaches ALL mon subnets
            elif is_mon:
                lan_net = str(monitor_mon_subnet)  # agents: only monitor's subnet
            else:
                lan_net = "10.128.0.0/10"          # nic1 carries full FABNetv4

            node_info[node_name]["nics"].append({
                "network": nw_name,
                "mac": mac,
                "ip": f"{ip}/{subnet.prefixlen}",
                "gateway": gateway,
                "lan_net": lan_net,
                "mon": is_mon,
            })
            assigned_ip[(nw_name, node_name)] = str(ip)
            ip = next(hiter)
        print(f"  {nw_name} {subnet}: "
              f"{sum(1 for k in assigned_ip if k[0] == nw_name)} interfaces")

    # ------------------------------------------------------------------
    # /etc/hosts block (notebook cell 19 logic)
    # ------------------------------------------------------------------
    host_to_ip = {}
    for (nw, host), ip in assigned_ip.items():
        if nw.startswith(args.mon_network_prefix) and host != args.monitor_node:
            name = f"{host}-mon"
        else:
            name = host
        host_to_ip.setdefault(name, ip)
    etc_hosts = "\n".join(f"{ip} {host}"
                          for host, ip in sorted(host_to_ip.items())) + "\n"

    # ------------------------------------------------------------------
    # prometheus.yml (notebook cell 24 logic)
    # ------------------------------------------------------------------
    mon_targets = sorted(
        (host, ip) for (nw, host), ip in assigned_ip.items()
        if nw.startswith(args.mon_network_prefix) and host != args.monitor_node
    )
    lines = [
        "global:",
        "  scrape_interval: 15s",
        "  evaluation_interval: 15s",
        "",
        "scrape_configs:",
        "  - job_name: prometheus",
        "    static_configs:",
        "      - targets: ['localhost:9090']",
        "",
        "  - job_name: node",
        "    static_configs:",
        "      - targets:",
        "          - 'localhost:9100'",
        "        labels:",
        "          node: monitor",
    ]
    for host, ip in mon_targets:
        lines += [
            "      - targets:",
            f"          - '{ip}:9100'",
            "        labels:",
            f"          node: {host}",
        ]
    prometheus_yml = "\n".join(lines) + "\n"

    # ------------------------------------------------------------------
    # Bastion + local key paths (for the SSH fallback + upload script)
    # ------------------------------------------------------------------
    def _try(*fns):
        for fn in fns:
            try:
                v = fn()
                if v:
                    return str(v)
            except Exception:
                pass
        return ""

    bastion_host = _try(lambda: fablib.get_bastion_host(),
                        lambda: fablib.get_bastion_public_addr()) \
        or "bastion.fabric-testbed.net"
    bastion_user = _try(lambda: fablib.get_bastion_username())
    slice_key = _try(lambda: fablib.get_default_slice_private_key_file())
    bastion_key = _try(lambda: fablib.get_bastion_key_filename(),
                       lambda: fablib.get_bastion_key_location())

    # ------------------------------------------------------------------
    # Write plan/
    # ------------------------------------------------------------------
    out = args.out
    os.makedirs(out, exist_ok=True)

    agent_count = sum(1 for n in node_info if n.startswith("agent-"))

    with open(os.path.join(out, "inventory.json"), "w") as f:
        json.dump({
            "slices": [slice_name_1, slice_name_2],
            "db_node": args.db_node,
            "monitor_node": args.monitor_node,
            "branch": args.branch,
            "agent_count": agent_count,
            "bastion": {"host": bastion_host, "user": bastion_user},
            "nodes": node_info,
        }, f, indent=2)

    with open(os.path.join(out, "nodes.tsv"), "w") as f:
        for name in sorted(node_info):
            i = node_info[name]
            f.write(f"{name}\t{i['user']}\t{i['mgmt_ip']}\t"
                    f"{1 if i['swarm'] else 0}\t{i['site']}\n")

    with open(os.path.join(out, "nics.tsv"), "w") as f:
        for name in sorted(node_info):
            # nic1 (primary) rows before nic2 (monitoring) rows per node
            for nic in sorted(node_info[name]["nics"], key=lambda x: x["mon"]):
                f.write(f"{name}\t{nic['mac']}\t{nic['ip']}\t"
                        f"{nic['gateway']}\t{nic['lan_net']}\n")

    with open(os.path.join(out, "etc_hosts.txt"), "w") as f:
        f.write(etc_hosts)
    with open(os.path.join(out, "prometheus.yml"), "w") as f:
        f.write(prometheus_yml)
    with open(os.path.join(out, "config.env"), "w") as f:
        f.write(f'BRANCH="{args.branch}"\n'
                f'DB_NODE="{args.db_node}"\n'
                f'MONITOR_NODE="{args.monitor_node}"\n'
                f'AGENT_COUNT="{agent_count}"\n')
    with open(os.path.join(out, "bastion.env"), "w") as f:
        f.write(f'BASTION_HOST="{bastion_host}"\n'
                f'BASTION_USER="{bastion_user}"\n')
    with open(os.path.join(out, "upload.env"), "w") as f:
        f.write(f'SLICE_KEY_PATH="{slice_key}"\n'
                f'BASTION_KEY_PATH="{bastion_key}"\n')

    print(f"\nPlan written to {out}/")
    print(f"  nodes: {len(node_info)} ({agent_count} agents + database + monitor)")
    print(f"  slice key:   {slice_key or 'NOT FOUND -- set SLICE_KEY when uploading'}")
    print(f"  bastion:     {bastion_user}@{bastion_host} (key: {bastion_key or 'n/a'})")
    print("\nNext: ./upload_to_db.sh swarm   (then run setup_all.sh on the db node)")


if __name__ == "__main__":
    main()
