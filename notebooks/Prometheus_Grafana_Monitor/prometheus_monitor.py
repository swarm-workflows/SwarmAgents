#!/usr/bin/env python3
"""
Prometheus Grafana Monitor - Slice lifecycle and configuration manager.

This script handles all phases of the monitoring stack:
  - start:     Create the 3-node slice, submit, wait for ready, run boot configs
  - configure: Set up FABNetv4 routes, Prometheus targets, Grafana dashboard, tunnel
  - deploy:    Run start + configure in one shot (reuses slice object, no re-fetch)
  - stop:      Delete the slice and free resources
  - monitor:   Check slice health and service availability

Usage:
    python3 prometheus_monitor.py deploy    SLICE_NAME
    python3 prometheus_monitor.py start     SLICE_NAME
    python3 prometheus_monitor.py configure SLICE_NAME
    python3 prometheus_monitor.py stop      SLICE_NAME
    python3 prometheus_monitor.py monitor   SLICE_NAME

The weave.sh script calls these commands automatically.
"""
import os, sys, json, time, ipaddress, random, tempfile, urllib.request, concurrent.futures

FABNETV4_SUBNET = ipaddress.ip_network("10.128.0.0/10")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BUILD_DIR = os.path.join(SCRIPT_DIR, "build")
RESULTS_DIR = os.path.join(SCRIPT_DIR, "results")

from fabrictestbed_extensions.fablib.fablib import FablibManager


def start(slice_name):
    """Create the 3-node monitoring slice and wait for it to be ready."""
    fablib = FablibManager()

    AVOID_SITES = {"EDUKY", "EDC"}

    # Per-node resource requirements and site env vars
    NODE_SPECS = [
        {"name": "monitor", "cores": 4, "ram": 16, "disk": 50, "env": "SITE_MONITOR"},
        {"name": "worker1", "cores": 2, "ram": 8,  "disk": 10, "env": "SITE_WORKER1"},
        {"name": "worker2", "cores": 2, "ram": 8,  "disk": 10, "env": "SITE_WORKER2"},
    ]

    resources = fablib.get_resources()

    def _eligible_sites(need_cores, need_ram, need_disk, exclude=None):
        """Return every site that currently has enough headroom for a node of this size.

        Queries FABRIC's live resource view via fablib.get_resources() so the
        answer reflects the *current* state of the testbed, not a cached list.
        """
        exclude = exclude or set()
        eligible = []
        for sn in resources.get_site_names():
            if sn.upper() in AVOID_SITES or sn in exclude:
                continue
            try:
                ac = resources.get_core_available(sn)
                ar = resources.get_ram_available(sn)
                ad = resources.get_disk_available(sn)
            except Exception:
                continue
            if ac >= need_cores and ar >= need_ram and ad >= need_disk:
                eligible.append(sn)
        return eligible

    def _pick_available_site(need_cores, need_ram, need_disk, exclude=None):
        """Pick a random site that can support this node's requirements.

        We deliberately *randomize* among all eligible sites (instead of always
        taking the top-headroom one) so consecutive runs of this weave don't
        keep landing on the same few sites. Every returned site is guaranteed
        to currently have enough cores/RAM/disk.
        """
        eligible = _eligible_sites(need_cores, need_ram, need_disk, exclude=exclude)
        if not eligible:
            return None, 0
        return random.choice(eligible), len(eligible)

    # Resolve site for each node: explicit site or auto-select
    print("### PROGRESS: Querying FABRIC for sites that can support this slice...")
    node_sites = {}
    auto_sites_used = set()  # track auto-assigned sites so no two auto nodes share a site

    for spec in NODE_SPECS:
        site_val = os.environ.get(spec["env"], "auto").strip()
        if not site_val or site_val.lower() == "auto":
            chosen, n_eligible = _pick_available_site(
                spec["cores"], spec["ram"], spec["disk"], exclude=auto_sites_used,
            )
            if not chosen:
                print(f"ERROR: No site found with enough resources for {spec['name']} "
                      f"({spec['cores']} cores, {spec['ram']}GB RAM, {spec['disk']}GB disk)")
                sys.exit(1)
            print(f"### PROGRESS: {spec['name']}: {n_eligible} eligible sites, randomly picked '{chosen}'")
            node_sites[spec["name"]] = chosen
            auto_sites_used.add(chosen)
        else:
            # User specified a site — verify resources but proceed regardless
            try:
                ac = resources.get_core_available(site_val)
                ar = resources.get_ram_available(site_val)
                ad = resources.get_disk_available(site_val)
                if ac < spec["cores"] or ar < spec["ram"] or ad < spec["disk"]:
                    print(f"WARNING: Site '{site_val}' may not have enough resources for {spec['name']} "
                          f"(available: {ac} cores, {ar}GB RAM, {ad}GB disk; "
                          f"need: {spec['cores']} cores, {spec['ram']}GB RAM, {spec['disk']}GB disk)")
            except Exception as e:
                print(f"WARNING: Could not verify resources at '{site_val}': {e}")
            node_sites[spec["name"]] = site_val

    print(f"### PROGRESS: Final site assignment: " +
          ", ".join(f"{n}={s}" for n, s in node_sites.items()))

    print(f"### PROGRESS: Creating slice '{slice_name}'...")
    s = fablib.new_slice(name=slice_name)

    # Monitor node: Prometheus + Grafana + node_exporter
    monitor = s.add_node(name="monitor", site=node_sites["monitor"], cores=4, ram=16, disk=50,
                         image="default_ubuntu_22")
    monitor.add_fabnet(net_type="IPv4")

    # Worker nodes: node_exporter only
    worker1 = s.add_node(name="worker1", site=node_sites["worker1"], cores=2, ram=8, disk=10,
                         image="default_ubuntu_22")
    worker1.add_fabnet(net_type="IPv4")

    worker2 = s.add_node(name="worker2", site=node_sites["worker2"], cores=2, ram=8, disk=10,
                         image="default_ubuntu_22")
    worker2.add_fabnet(net_type="IPv4")

    print("### PROGRESS: Submitting slice to FABRIC...")
    s.submit()

    print("### PROGRESS: Waiting for SSH access (this may take several minutes)...")
    s.wait_ssh(progress=True)

    print(f"### PROGRESS: Slice '{slice_name}' is ready!")
    for node in s.get_nodes():
        print(f"  {node.get_name()}: {node.get_management_ip()}")

    # Bring up dataplane interfaces before running setup scripts
    print("### PROGRESS: Configuring dataplane interfaces...")
    try:
        s.post_boot_config()
        print("  post_boot_config complete")
    except Exception as e:
        print(f"  post_boot_config warning: {e}")

    # Run boot configs (upload tools and execute setup scripts)
    print("### PROGRESS: Running boot configurations...")
    _run_boot_configs(s)

    return s


def _run_boot_configs(s):
    """Upload tools/ scripts and execute boot_config commands on all nodes in parallel."""
    tools_dir = os.path.join(SCRIPT_DIR, "tools")

    def _setup_node(node):
        name = node.get_name()
        node.execute("mkdir -p ~/tools", quiet=True)
        if name == "monitor":
            script = os.path.join(tools_dir, "setup-monitor.sh")
            if os.path.exists(script):
                node.upload_file(script, "tools/setup-monitor.sh")
                print(f"### PROGRESS: Running setup on {name}...")
                node.execute("chmod +x ~/tools/setup-monitor.sh && ~/tools/setup-monitor.sh",
                             quiet=False)
        else:
            script = os.path.join(tools_dir, "setup-exporter.sh")
            if os.path.exists(script):
                node.upload_file(script, "tools/setup-exporter.sh")
                print(f"### PROGRESS: Running setup on {name}...")
                node.execute("chmod +x ~/tools/setup-exporter.sh && ~/tools/setup-exporter.sh",
                             quiet=False)
        return name

    nodes = s.get_nodes()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes)) as pool:
        futures = {pool.submit(_setup_node, node): node.get_name() for node in nodes}
        for future in concurrent.futures.as_completed(futures):
            name = futures[future]
            try:
                future.result()
                print(f"### PROGRESS: Setup complete on {name}")
            except Exception as e:
                print(f"ERROR: Setup failed on {name}: {e}")
                raise


def _save_topology_layout(slice_name, node_nets):
    """Save a purposeful Topology-view layout for this slice via the LoomAI API.

    The layout makes the monitoring intent obvious at a glance and avoids the
    overlapping nodes/edges produced by the default auto-layout:

        monitor                 <- Prometheus + Grafana, prominent at the top
           |
      (monitor gateway)         <- per-site FABNetv4 gateway
           |
      FABRIC Internet           <- shared FABNetv4 cloud Prometheus scrapes across
         /      \\
    (gw)          (gw)          <- worker gateways
      |             |
   worker1       worker2        <- observed node_exporter targets, fanned out below

    Positions are keyed by LoomAI logical topology IDs (``node:<name>``,
    ``network:<name>``, and the synthetic ``fabnet-internet-v4`` cloud), so the
    same intent holds regardless of which sites were auto-selected. The
    per-site FABNet gateway names are read live from the deployed slice.
    """
    # Stable anchors: the three VMs and the shared FABNetv4 cloud. Even 160px
    # rows and a wide worker spread keep edges/labels from overlapping, and
    # mirror the static slice-layout.json shipped with the weave so the template
    # preview and the live slice render identically.
    positions = {
        "node:monitor": {"x": 400, "y": 0},
        # The synthetic FABRIC Internet hub has no node/network name, so the
        # frontend keys it by raw id — the layout key must be "id:<rawId>",
        # not the bare id, or it won't be positioned (drifts off to the side).
        "id:fabnet-internet-v4": {"x": 400, "y": 320},
        "node:worker1": {"x": 130, "y": 640},
        "node:worker2": {"x": 670, "y": 640},
    }
    # Each node's FABNet gateway sits on the path between the node and the cloud.
    gateway_pos = {
        "monitor": {"x": 400, "y": 160},
        "worker1": {"x": 130, "y": 480},
        "worker2": {"x": 670, "y": 480},
    }
    for node_name, net_name in node_nets.items():
        if net_name and node_name in gateway_pos:
            positions[f"network:{net_name}"] = gateway_pos[node_name]

    payload = json.dumps({
        "version": "slice-layout.v1",
        "generated_by": "prometheus-grafana-weave",
        "slice_name": slice_name,
        "positions": positions,
    }).encode()
    try:
        req = urllib.request.Request(
            f"http://localhost:8000/api/slices/{slice_name}/layout",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="PUT",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            json.loads(resp.read())
        print(f"  Topology layout saved for {len(positions)} elements")
    except Exception as e:
        print(f"  WARNING: Could not save topology layout: {e}")


def deploy(slice_name):
    """Create slice and configure monitoring in one shot."""
    s = start(slice_name)
    configure(slice_name, slice_obj=s)


def configure(slice_name, slice_obj=None):
    """Configure FABNetv4 routes, Prometheus targets, Grafana dashboard, and create tunnel."""
    if slice_obj:
        s = slice_obj
    else:
        fablib = FablibManager()
        print(f"### PROGRESS: Loading slice '{slice_name}'")
        s = fablib.get_slice(name=slice_name)
    os.makedirs(RESULTS_DIR, exist_ok=True)

    # Collect FABNetv4 IPs (network already configured during start)
    print("### PROGRESS: Collecting node IPs for Prometheus targets")
    node_ips = {}
    node_nets = {}  # node name -> its FABNetv4 network name (for the Topology layout)

    for node in s.get_nodes():
        for iface in node.get_interfaces():
            net = iface.get_network()
            if net and str(net.get_type()) in ("FABNetv4", "FABNetv4Ext"):
                node_nets[node.get_name()] = net.get_name()
                fd = iface.get_fablib_data()
                ip = fd.get("addr")
                if not ip:
                    ip = iface.get_ip_addr()
                if ip:
                    node_ips[node.get_name()] = ip

    # Fallback: SSH to nodes and detect dataplane IP
    for node in s.get_nodes():
        name = node.get_name()
        if name not in node_ips:
            try:
                stdout, _ = node.execute(
                    "ip -4 addr show | grep -v '127.0.0.1' | grep 'inet ' "
                    "| grep -v -E 'enp3s0|ens3|eth0' | grep -oP '\\d+\\.\\d+\\.\\d+\\.\\d+(?=/\\d+)' "
                    "| head -1",
                    quiet=True,
                )
                if stdout and stdout.strip():
                    node_ips[name] = stdout.strip()
                    print(f"  {name}: detected dataplane IP {node_ips[name]} via SSH")
            except Exception as e:
                print(f"  WARNING: could not detect IP for {name}: {e}")

    print("")
    for name, ip in node_ips.items():
        role = "monitor" if name == "monitor" else "worker"
        print(f"  {name} ({role}): FABNet IP = {ip}")

    # Save a purposeful Topology-view layout for this slice
    print("\n### PROGRESS: Saving custom Topology layout (monitor-centric scrape view)")
    _save_topology_layout(slice_name, node_nets)

    # Phase 2: Generate prometheus.yml with all node targets
    print("\n### PROGRESS: Generating Prometheus config with static targets")
    monitor_ip = node_ips.get("monitor", "localhost")
    all_targets = [f"{ip}:9100" for ip in node_ips.values()]

    prometheus_yml = f"""global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets: ['localhost:9090']

  - job_name: node
    static_configs:
      - targets: {json.dumps(all_targets)}
"""

    # Phase 3: Upload everything to the monitor node
    print("### PROGRESS: Uploading monitoring config to monitor node")
    monitor_node = s.get_node("monitor")
    monitor_node.execute(
        "mkdir -p ~/monitoring-config/grafana-provisioning/datasources "
        "~/monitoring-config/grafana-provisioning/dashboards",
        quiet=True,
    )

    def _upload_text(text, remote_path, suffix=".yml"):
        fd, tmp = tempfile.mkstemp(suffix=suffix)
        try:
            with os.fdopen(fd, "w") as f:
                f.write(text)
            monitor_node.upload_file(tmp, remote_path)
        finally:
            os.unlink(tmp)

    _upload_text(prometheus_yml, "monitoring-config/prometheus.yml")
    monitor_node.upload_file(
        os.path.join(BUILD_DIR, "resource-utilization.json"),
        "monitoring-config/grafana-provisioning/dashboards/resource-utilization.json",
    )
    monitor_node.upload_file(
        os.path.join(BUILD_DIR, "grafana-datasource.yml"),
        "monitoring-config/grafana-provisioning/datasources/datasource.yml",
    )
    monitor_node.upload_file(
        os.path.join(BUILD_DIR, "grafana-dashboard-provider.yml"),
        "monitoring-config/grafana-provisioning/dashboards/dashboard.yml",
    )

    # Phase 4: Reload Prometheus and Grafana with new config
    print("### PROGRESS: Reloading Prometheus with new targets")
    monitor_node.execute(
        "sudo cp ~/monitoring-config/prometheus.yml /etc/prometheus/prometheus.yml && "
        "sudo chown nobody: /etc/prometheus/prometheus.yml && "
        "sudo systemctl restart prometheus",
        quiet=True,
    )

    print("### PROGRESS: Reloading Grafana with dashboard and datasource")
    monitor_node.execute(
        # Remove any existing datasource files to avoid duplicate isDefault conflict
        "sudo rm -f /etc/grafana/provisioning/datasources/*.yml "
        "/etc/grafana/provisioning/datasources/*.yaml && "
        "sudo cp ~/monitoring-config/grafana-provisioning/datasources/datasource.yml "
        "/etc/grafana/provisioning/datasources/ && "
        "sudo cp ~/monitoring-config/grafana-provisioning/dashboards/dashboard.yml "
        "/etc/grafana/provisioning/dashboards/ && "
        "sudo cp ~/monitoring-config/grafana-provisioning/dashboards/resource-utilization.json "
        "/etc/grafana/provisioning/dashboards/ && "
        "sudo systemctl restart grafana-server",
        quiet=True,
    )

    # Set Grafana home dashboard
    print("### PROGRESS: Setting Grafana home dashboard")
    stdout, _ = monitor_node.execute(
        'for i in $(seq 1 30); do '
        '  curl -sf http://localhost:3000/api/health > /dev/null 2>&1 && break; '
        '  sleep 2; '
        'done && '
        'DASH_UID=$(curl -sf "http://localhost:3000/api/search?type=dash-db" '
        '  -u admin:admin 2>/dev/null | '
        '  grep -oP \'\"uid\"\\s*:\\s*\"\\K[^\"]+\' | head -1) && '
        'if [ -n "$DASH_UID" ]; then '
        '  curl -sf -X PUT http://localhost:3000/api/org/preferences '
        '    -H "Content-Type: application/json" '
        '    -u admin:admin '
        '    -d "{\\\"homeDashboardUID\\\":\\\"$DASH_UID\\\"}" > /dev/null && '
        '  echo "Home dashboard set: $DASH_UID"; '
        'fi',
        quiet=True,
    )
    if stdout:
        print(f"  {stdout.strip()}")

    # Phase 5: Wait for Prometheus targets to come up
    print("\n### PROGRESS: Waiting for Prometheus to start scraping")
    active = []
    for attempt in range(12):
        stdout, _ = monitor_node.execute(
            "curl -sf http://localhost:9090/api/v1/targets 2>/dev/null || echo '{}'",
            quiet=True,
        )
        try:
            targets_data = json.loads(stdout.strip())
            active = targets_data.get("data", {}).get("activeTargets", [])
            up_count = sum(1 for t in active if t.get("health") == "up")
            if up_count >= len(node_ips):
                print(f"  All {up_count} targets are up")
                break
            print(f"  {up_count}/{len(node_ips)} targets up, waiting...")
        except json.JSONDecodeError:
            print("  Prometheus not ready yet, waiting...")
        time.sleep(10)

    for t in active:
        labels = t.get("labels", {})
        health = t.get("health", "unknown")
        instance = labels.get("instance", "?")
        job = labels.get("job", "?")
        print(f"  {job}/{instance}: {health}")

    # Verify Grafana health
    print(f"\n### PROGRESS: Checking Grafana health")
    stdout, _ = monitor_node.execute(
        "curl -sf http://localhost:3000/api/health 2>/dev/null || echo '{}'",
        quiet=True,
    )
    try:
        health = json.loads(stdout.strip())
        db_status = health.get("database", "unknown")
        print(f"  Grafana: database={db_status}")
    except json.JSONDecodeError:
        print("  WARNING: Could not parse Grafana health response")

    # Phase 6: Create web tunnel for Grafana
    print(f"\n### PROGRESS: Creating Grafana web tunnel for Apps tab")
    grafana_tunnel = None
    try:
        req = urllib.request.Request("http://localhost:8000/api/tunnels")
        with urllib.request.urlopen(req, timeout=10) as resp:
            tunnels = json.loads(resp.read())

        for t in tunnels:
            if (t.get("slice_name") == slice_name and
                t.get("node_name") == "monitor" and
                t.get("remote_port") == 3000):
                grafana_tunnel = t
                break

        if grafana_tunnel:
            print(f"  Grafana tunnel already exists (port {grafana_tunnel.get('local_port')})")
        else:
            tunnel_data = json.dumps({
                "slice_name": slice_name,
                "node_name": "monitor",
                "remote_port": 3000,
                "label": "Grafana Dashboard"
            }).encode()
            req = urllib.request.Request(
                "http://localhost:8000/api/tunnels",
                data=tunnel_data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                grafana_tunnel = json.loads(resp.read())
            local_port = grafana_tunnel.get("local_port", "?")
            print(f"  Grafana tunnel created on port {local_port}")
    except Exception as e:
        print(f"  WARNING: Could not create tunnel: {e}")
        print(f"  Create manually in the Apps tab: slice={slice_name}, node=monitor, port=3000")

    # Save status report
    status = {
        "slice_name": slice_name,
        "node_ips": node_ips,
        "monitor_ip": monitor_ip,
        "prometheus_url": f"http://{monitor_ip}:9090",
        "grafana_url": f"http://{monitor_ip}:3000",
        "tunnel": grafana_tunnel,
    }
    status_file = os.path.join(RESULTS_DIR, "monitoring-status.json")
    with open(status_file, "w") as f:
        json.dump(status, f, indent=2)

    print(f"\n### PROGRESS: Monitoring stack ready")
    print(f"\n=== Monitoring Stack Ready ===")
    print(f"  Prometheus: http://{monitor_ip}:9090")
    print(f"  Grafana:    http://{monitor_ip}:3000")
    if grafana_tunnel:
        lp = grafana_tunnel.get("local_port", "?")
        print(f"  Grafana tunnel: port {lp} — open the Apps tab to view the dashboard")
    print(f"\n  Dashboard: FABRIC Node Resource Utilization")
    print(f"  Panels: CPU, Memory, Load, Disk, Network I/O, Disk I/O")
    print(f"  Anonymous viewer access enabled (no login needed)")


def stop(slice_name):
    """Delete the slice and free all resources."""
    fablib = FablibManager()

    try:
        s = fablib.get_slice(name=slice_name)
        print(f"### PROGRESS: Deleting slice '{slice_name}'...")
        s.delete()
        print(f"### PROGRESS: Slice '{slice_name}' deleted.")
    except Exception as e:
        print(f"### PROGRESS: Slice not found or already deleted: {e}")


def monitor(slice_name):
    """Check slice health and service availability.

    Exits with code 0 if OK, code 1 if something is wrong.
    Tolerates transient auth/connection errors (e.g. token refresh).
    """
    try:
        fablib = FablibManager()
        s = fablib.get_slice(name=slice_name)
    except Exception as e:
        print(f"WARNING: Could not connect to FABRIC (transient): {e}")
        print("### PROGRESS: Skipping health check (will retry next cycle)")
        return

    state = str(s.get_state())

    if "StableOK" not in state:
        print(f"ERROR: Slice state is {state} (expected StableOK)")
        sys.exit(1)

    for node in s.get_nodes():
        try:
            stdout, stderr = node.execute("echo ok", quiet=True)
            if "ok" not in stdout:
                raise Exception("unexpected output from test command")
        except Exception as e:
            print(f"WARNING: Node {node.get_name()} health check issue: {e}")
            print("### PROGRESS: Skipping node check (will retry next cycle)")
            return

    print(f"### PROGRESS: All nodes healthy (state: {state})")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: prometheus_monitor.py {start|configure|deploy|stop|monitor} SLICE_NAME")
        sys.exit(1)

    action = sys.argv[1]
    slice_name = sys.argv[2]

    if action == "start":
        start(slice_name)
    elif action == "configure":
        configure(slice_name)
    elif action == "deploy":
        deploy(slice_name)
    elif action == "stop":
        stop(slice_name)
    elif action == "monitor":
        monitor(slice_name)
    else:
        print(f"Unknown action: {action}")
        print("Usage: prometheus_monitor.py {start|configure|deploy|stop|monitor} SLICE_NAME")
        sys.exit(1)
