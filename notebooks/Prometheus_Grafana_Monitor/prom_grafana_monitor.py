#!/usr/bin/env python3
"""
Prometheus + Grafana Monitor - FABRIC Slice Lifecycle

This script manages a 3-node monitoring stack on FABRIC:
  - monitor node:  Prometheus + Grafana + node_exporter  (4 cores, 16 GB RAM)
  - worker1 node:  node_exporter                         (2 cores, 8 GB RAM)
  - worker2 node:  node_exporter                         (2 cores, 8 GB RAM)

All nodes connect via FABNetv4 so Prometheus can scrape metrics across nodes.

Commands:
    python3 prom_grafana_monitor.py start   SLICE_NAME
    python3 prom_grafana_monitor.py stop    SLICE_NAME
    python3 prom_grafana_monitor.py monitor SLICE_NAME

Called by weave.sh — you don't usually run this directly.
"""
import os
import sys
import json
import subprocess
import urllib.request

# Directory where this script lives (contains tools/, build/, etc.)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_LOOMAI_URL = "http://localhost:8000"

from fabrictestbed_extensions.fablib.fablib import FablibManager


def _with_api_suffix(url):
    url = url.rstrip("/")
    return url if url.endswith("/api") else f"{url}/api"


def loomai_api_base():
    configured = os.environ.get("LOOMAI_API_URL") or os.environ.get("LOOMAI_URL") or DEFAULT_LOOMAI_URL
    return _with_api_suffix(configured)


def loomai_auth_headers():
    session_cookie = os.environ.get("LOOMAI_SESSION_COOKIE", "")
    if not session_cookie or "\n" in session_cookie or "\r" in session_cookie:
        return {}
    return {"Cookie": f"loomai_session={session_cookie}"}


# =====================================================================
#  START — Create the slice, install software, configure monitoring
# =====================================================================

def start(slice_name):
    """Create and provision the full monitoring stack."""
    print("")
    print("=" * 60)
    print("  Prometheus + Grafana Monitor")
    print("=" * 60)
    print("")
    print("  This weave will:")
    print("    1. Create a 3-node FABRIC slice")
    print("    2. Install Prometheus + Grafana on the monitor node")
    print("    3. Install node_exporter on all nodes")
    print("    4. Configure monitoring targets and dashboards")
    print("    5. Create a Grafana tunnel for the Apps tab")
    print("")
    print("  This typically takes 10-15 minutes.")
    print("  Click Stop at any time to tear everything down.")
    print("")

    print("### PROGRESS: Step 1/5 — Creating FABRIC slice...")
    my_slice = create_slice(slice_name)

    print("")
    print("### PROGRESS: Step 2/5 — Installing monitoring software...")
    install_software(my_slice)

    print("")
    print("### PROGRESS: Step 3/5 — Configuring monitoring stack...")
    configure_monitoring(slice_name)

    print("")
    print("### PROGRESS: Step 4/5 — Verifying services...")
    verify_and_create_tunnel(slice_name)

    print("")
    print("=" * 60)
    print("  Step 5/5 — READY!")
    print("")
    print(f"  Slice '{slice_name}' is fully provisioned.")
    print("  Grafana dashboard is available in the Apps tab.")
    print("  Click Stop when you're done to delete the slice.")
    print("=" * 60)
    print("")


def create_slice(slice_name):
    """Create a 3-node slice with FABNetv4 and wait for SSH."""
    fablib = FablibManager()

    print(f"  Creating slice '{slice_name}' with 3 nodes:")
    print(f"    - monitor (4 cores, 16 GB RAM, 50 GB disk)")
    print(f"    - worker1 (2 cores, 8 GB RAM, 10 GB disk)")
    print(f"    - worker2 (2 cores, 8 GB RAM, 10 GB disk)")
    print(f"    - FABNetv4 network connecting all nodes")
    my_slice = fablib.new_slice(name=slice_name)

    # Monitor node — runs Prometheus + Grafana (needs more resources)
    monitor = my_slice.add_node(
        name="monitor", cores=4, ram=16, disk=50,
        image="default_ubuntu_22",
    )
    monitor_nic = monitor.add_component(model="NIC_Basic", name="FABNET")

    # Worker nodes — run node_exporter only
    worker1 = my_slice.add_node(
        name="worker1", cores=2, ram=8, disk=10,
        image="default_ubuntu_22",
    )
    worker1_nic = worker1.add_component(model="NIC_Basic", name="FABNET")

    worker2 = my_slice.add_node(
        name="worker2", cores=2, ram=8, disk=10,
        image="default_ubuntu_22",
    )
    worker2_nic = worker2.add_component(model="NIC_Basic", name="FABNET")

    # Give each node its own FABNetv4 network service.
    # FABRIC assigns IPs from 10.128.0.0/10 and handles routing between them.
    for name, nic in [("monitor", monitor_nic), ("worker1", worker1_nic),
                      ("worker2", worker2_nic)]:
        iface = nic.get_interfaces()[0]
        my_slice.add_l3network(
            name=f"fabnet-{name}", interfaces=[iface], type="IPv4",
        )

    # Submit to FABRIC and wait for all nodes to be SSH-accessible
    print("### PROGRESS: Submitting slice to FABRIC...")
    print("  (This typically takes 3-5 minutes while FABRIC provisions the VMs)")
    my_slice.submit()

    print("### PROGRESS: Waiting for SSH access on all 3 nodes...")
    my_slice.wait_ssh(progress=True)

    print(f"### PROGRESS: All 3 nodes are up and SSH-accessible!")
    for node in my_slice.get_nodes():
        print(f"  {node.get_name()}: {node.get_management_ip()}")

    return my_slice


def install_software(my_slice):
    """Upload setup scripts to nodes and run them to install software.

    - monitor node: Prometheus + Grafana + node_exporter (setup-monitor.sh)
    - worker nodes: node_exporter only (setup-exporter.sh)
    """
    tools_dir = os.path.join(SCRIPT_DIR, "tools")

    # Install on the monitor node first (takes the longest)
    monitor = my_slice.get_node("monitor")
    print("### PROGRESS: [2a/5] Installing Prometheus + Grafana + node_exporter on monitor node...")
    print("  (This is the longest step — downloads and installs 3 services)")
    monitor.execute("mkdir -p ~/tools", quiet=True)
    monitor.upload_file(
        os.path.join(tools_dir, "setup-monitor.sh"), "tools/setup-monitor.sh",
    )
    monitor.execute("chmod +x ~/tools/setup-monitor.sh && ~/tools/setup-monitor.sh")
    print("### PROGRESS: [2a/5] Monitor node software installed.")

    # Install node_exporter on each worker
    for i, name in enumerate(["worker1", "worker2"], start=1):
        worker = my_slice.get_node(name)
        print(f"### PROGRESS: [2b/5] Installing node_exporter on {name} ({i}/2)...")
        worker.execute("mkdir -p ~/tools", quiet=True)
        worker.upload_file(
            os.path.join(tools_dir, "setup-exporter.sh"), "tools/setup-exporter.sh",
        )
        worker.execute("chmod +x ~/tools/setup-exporter.sh && ~/tools/setup-exporter.sh")
        print(f"  {name} node_exporter installed.")
    print("### PROGRESS: [2/5] All software installed on all nodes.")


def configure_monitoring(slice_name):
    """Configure FABNetv4 routes, Prometheus targets, and Grafana dashboards.

    This calls the build/configure-monitor.py script which:
    1. Runs post_boot_config() to bring up dataplane interfaces
    2. Adds FABNetv4 routes on all nodes
    3. Generates prometheus.yml with all node IPs as targets
    4. Uploads Grafana dashboard and datasource configs
    5. Restarts Prometheus and Grafana with the new config
    """
    print("  Remaining steps:")
    print("    3. Configure FABNetv4 routes, Prometheus targets, Grafana dashboard")
    print("    4. Verify all services are running")
    print("    5. Ready!")
    print("")
    build_dir = os.path.join(SCRIPT_DIR, "build")

    env = os.environ.copy()
    env["SLICE_NAME"] = slice_name
    env["BUILD_DIR"] = build_dir

    result = subprocess.run(
        [sys.executable, os.path.join(build_dir, "configure-monitor.py")],
        env=env,
    )
    if result.returncode != 0:
        print("ERROR: Monitoring configuration failed")
        sys.exit(1)


def verify_and_create_tunnel(slice_name):
    """Verify Prometheus and Grafana are running, then create a Grafana tunnel."""
    fablib = FablibManager()
    my_slice = fablib.get_slice(name=slice_name)
    monitor = my_slice.get_node("monitor")

    # Check Prometheus targets
    print("### PROGRESS: [4a/5] Verifying Prometheus...")
    stdout, _ = monitor.execute(
        "curl -sf http://localhost:9090/api/v1/targets 2>/dev/null || echo '{}'",
        quiet=True,
    )
    try:
        data = json.loads(stdout.strip())
        targets = data.get("data", {}).get("activeTargets", [])
        up_count = sum(1 for t in targets if t.get("health") == "up")
        print(f"  Prometheus: {up_count}/{len(targets)} targets up")
        for t in targets:
            instance = t.get("labels", {}).get("instance", "?")
            health = t.get("health", "unknown")
            print(f"    {instance}: {health}")
    except json.JSONDecodeError:
        print("  WARNING: Prometheus not responding yet (may need a minute)")

    # Check Grafana
    print("### PROGRESS: [4b/5] Verifying Grafana...")
    stdout, _ = monitor.execute(
        "curl -sf http://localhost:3000/api/health 2>/dev/null || echo '{}'",
        quiet=True,
    )
    try:
        health = json.loads(stdout.strip())
        print(f"  Grafana: database={health.get('database', 'unknown')}")
    except json.JSONDecodeError:
        print("  WARNING: Grafana not responding yet")

    # Create a Grafana tunnel so users can view dashboards in the Apps tab
    print("### PROGRESS: [4c/5] Creating Grafana web tunnel...")
    try:
        # Check if tunnel already exists
        req = urllib.request.Request(
            f"{loomai_api_base()}/tunnels",
            headers=loomai_auth_headers(),
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            tunnels = json.loads(resp.read())

        existing = None
        for t in tunnels:
            if (t.get("slice_name") == slice_name
                    and t.get("node_name") == "monitor"
                    and t.get("remote_port") == 3000):
                existing = t
                break

        if existing:
            print(f"  Tunnel already exists (port {existing.get('local_port')})")
        else:
            tunnel_data = json.dumps({
                "slice_name": slice_name,
                "node_name": "monitor",
                "remote_port": 3000,
                "label": "Grafana Dashboard",
            }).encode()
            req = urllib.request.Request(
                f"{loomai_api_base()}/tunnels",
                data=tunnel_data,
                headers={**loomai_auth_headers(), "Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                tunnel = json.loads(resp.read())
            print(f"  Grafana tunnel created on port {tunnel.get('local_port', '?')}")
    except Exception as e:
        print(f"  WARNING: Could not create tunnel: {e}")

    print("\n### PROGRESS: [4/5] All services verified.")


# =====================================================================
#  STOP — Delete the slice
# =====================================================================

def stop(slice_name):
    """Delete the monitoring slice and free all FABRIC resources."""
    fablib = FablibManager()
    try:
        my_slice = fablib.get_slice(name=slice_name)
        print(f"### PROGRESS: Deleting slice '{slice_name}'...")
        my_slice.delete()
        print(f"### PROGRESS: Slice '{slice_name}' deleted.")
    except Exception as e:
        print(f"### PROGRESS: Slice not found or already deleted: {e}")


# =====================================================================
#  MONITOR — Health check (called every 30s by weave.sh)
# =====================================================================

def monitor(slice_name):
    """Check that the monitoring stack is healthy.

    Exit code 0 = healthy, exit code 1 = problem detected.
    When weave.sh sees exit code 1, it triggers cleanup.
    """
    fablib = FablibManager()
    my_slice = fablib.get_slice(name=slice_name)

    # Check 1: Is the slice in a good state?
    state = str(my_slice.get_state())
    if "StableOK" not in state:
        print(f"ERROR: Slice state is {state} (expected StableOK)")
        sys.exit(1)

    # Check 2: Can we SSH into every node?
    for node in my_slice.get_nodes():
        try:
            stdout, _ = node.execute("echo ok", quiet=True)
            if "ok" not in stdout:
                raise Exception("unexpected output")
        except Exception as e:
            print(f"ERROR: Node {node.get_name()} SSH check failed: {e}")
            sys.exit(1)

    # Check 3: Is Prometheus scraping targets?
    monitor_node = my_slice.get_node("monitor")
    try:
        stdout, _ = monitor_node.execute(
            "curl -sf http://localhost:9090/api/v1/targets 2>/dev/null || echo '{}'",
            quiet=True,
        )
        data = json.loads(stdout.strip())
        targets = data.get("data", {}).get("activeTargets", [])
        down = [t for t in targets if t.get("health") != "up"]
        if down:
            names = [t.get("labels", {}).get("instance", "?") for t in down]
            print(f"WARNING: Prometheus targets down: {', '.join(names)}")
    except Exception:
        pass  # Prometheus check is non-fatal

    print(f"### PROGRESS: All nodes healthy (state: {state})")


# =====================================================================
#  Main — parse command-line arguments
# =====================================================================

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: prom_grafana_monitor.py {start|stop|monitor} SLICE_NAME")
        sys.exit(1)

    action = sys.argv[1]      # "start", "stop", or "monitor"
    slice_name = sys.argv[2]

    if action == "start":
        start(slice_name)
    elif action == "stop":
        stop(slice_name)
    elif action == "monitor":
        monitor(slice_name)
    else:
        print(f"Unknown action: {action}")
        print("Usage: prom_grafana_monitor.py {start|stop|monitor} SLICE_NAME")
        sys.exit(1)
