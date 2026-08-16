# Prometheus Grafana Monitor

A LoomAI weave that deploys a complete Prometheus + Grafana monitoring stack
on FABRIC. Three nodes are provisioned on a single site: one monitor node
running Prometheus and Grafana, and two worker nodes running node_exporter.
All nodes communicate over the FABNetv4 overlay network. A pre-built Grafana
dashboard provides real-time visibility into CPU, memory, disk, and network
metrics across all nodes.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [What Gets Installed](#what-gets-installed)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Deployment Phases](#deployment-phases)
- [Grafana Dashboard](#grafana-dashboard)
- [Accessing Grafana](#accessing-grafana)
- [Prometheus Metrics](#prometheus-metrics)
- [Monitoring Loop](#monitoring-loop)
- [Included Notebooks](#included-notebooks)
- [File Structure](#file-structure)
- [Troubleshooting](#troubleshooting)
- [Extending the Stack](#extending-the-stack)

---

## Overview

Monitoring is essential for any FABRIC experiment — whether you're running
network benchmarks, distributed applications, or long-running workloads. This
weave gives you a production-quality monitoring stack in minutes, without
needing to manually install or configure anything.

Once deployed, you get:

- **Real-time metrics** from all nodes via Prometheus (scraped every 15 seconds)
- **Pre-built Grafana dashboard** with CPU, memory, disk, network, and load panels
- **Web tunnel** for accessing Grafana directly from the LoomAI Apps tab
- **Anonymous access** to Grafana — no login required
- **Automatic cleanup** — clicking Stop tears down the entire slice

The stack is designed as a standalone monitoring environment, but the patterns
here (node_exporter installation, Prometheus configuration, Grafana
provisioning) can be adapted for monitoring any FABRIC experiment.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     FABRIC Site (auto-selected)                 │
│                                                                 │
│  ┌──────────────────────┐    FABNetv4    ┌───────────────────┐  │
│  │   monitor             │  (10.x.x.x)  │   worker1          │  │
│  │   4 cores / 16 GB RAM │◄────────────►│   2 cores / 8 GB   │  │
│  │   50 GB disk          │              │   10 GB disk        │  │
│  │                       │              │                     │  │
│  │   Prometheus :9090    │  ◄── scrape  │   node_exporter    │  │
│  │   Grafana    :3000    │     /metrics │   :9100             │  │
│  │   node_exporter :9100 │              └───────────────────┘  │
│  │                       │                                     │
│  │                       │    FABNetv4    ┌───────────────────┐  │
│  │                       │  (10.x.x.x)  │   worker2          │  │
│  │                       │◄────────────►│   2 cores / 8 GB   │  │
│  │                       │  ◄── scrape  │   10 GB disk        │  │
│  │                       │     /metrics │                     │  │
│  │   ┌──────────────┐   │              │   node_exporter    │  │
│  │   │ Dashboard:    │   │              │   :9100             │  │
│  │   │ CPU, Memory,  │   │              └───────────────────┘  │
│  │   │ Disk, Network │   │                                     │
│  │   └──────────────┘   │                                     │
│  └──────────────────────┘                                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
         │
         │ SSH tunnel (auto-created)
         ▼
┌─────────────────────┐
│  LoomAI Container   │
│  Web tunnel → :3000 │
│  Apps tab: Grafana  │
└─────────────────────┘
```

### Key Design Decisions

- **Single site**: All three nodes are placed on the same FABRIC site. This
  avoids cross-site latency for metric scraping and simplifies FABNetv4
  connectivity. The script auto-selects the first available site with enough
  resources (8 cores, 32 GB RAM, 70 GB disk).

- **FABNetv4 overlay**: Prometheus scrapes node_exporter over the FABNetv4
  dataplane network (10.128.0.0/10), not the management network. This keeps
  monitoring traffic on the experiment network.

- **Anonymous Grafana access**: Grafana is configured with anonymous viewer
  access and iframe embedding enabled. No login is needed to view dashboards.

- **Web tunnel**: A tunnel is automatically created to forward Grafana's
  port 3000 from the monitor node to a local port accessible from the LoomAI
  Apps tab.

---

## What Gets Installed

### Monitor Node

| Component | Version | Port | Purpose |
|-----------|---------|------|---------|
| Prometheus | 2.53.3 | 9090 | Time-series database, scrapes all nodes every 15s |
| Grafana | 11+ (latest) | 3000 | Dashboard UI with pre-provisioned panels |
| node_exporter | 1.8.2 | 9100 | Exposes hardware/OS metrics for the monitor itself |

### Worker Nodes (worker1, worker2)

| Component | Version | Port | Purpose |
|-----------|---------|------|---------|
| node_exporter | 1.8.2 | 9100 | Exposes hardware/OS metrics |

### Software Details

**Prometheus** is installed from the official GitHub release as a standalone
binary. It runs as a systemd service under the `nobody` user with:
- 15-second scrape interval
- 30-day data retention
- Storage at `/var/lib/prometheus`
- Config at `/etc/prometheus/prometheus.yml`

**Grafana** is installed from the official APT repository. It runs as a
systemd service with:
- Anonymous viewer access (no login required)
- Iframe embedding enabled (for LoomAI Apps tab)
- Pre-provisioned Prometheus datasource
- Pre-provisioned resource utilization dashboard set as home

**node_exporter** is installed from the official GitHub release as a
standalone binary. It runs as a systemd service under the `nobody` user,
listening on port 9100 on all interfaces. It exposes hundreds of hardware
and OS metrics including CPU, memory, disk, network, filesystem, load
average, and more.

---

## Quick Start

1. Open the **Artifacts** panel in the LoomAI WebUI
2. Find **Prometheus Grafana Monitor** and click **Run**
3. Enter a slice name (or keep the default `prometheus-monitor`)
4. Click **Start** and wait ~10–15 minutes for provisioning
5. Watch the log output for `### PROGRESS:` status updates
6. When you see **"Monitoring stack ready"**, open the **Apps** tab
7. Click the **Grafana Dashboard** tunnel to view metrics
8. When done, click **Stop** to delete the slice and free resources

### What Happens During Deployment

The deployment takes approximately 10–15 minutes and goes through these
phases (visible in the log output):

```
Finding a site with available resources...        (~5 seconds)
Creating slice 'prometheus-monitor'...            (~1 second)
Submitting slice to FABRIC...                     (~2-5 minutes)
Waiting for SSH access...                         (~1-3 minutes)
Configuring dataplane interfaces...               (~30 seconds)
Running boot configurations...                    (~3-5 minutes)
  ├─ Installing node_exporter on all 3 nodes      (parallel)
  ├─ Installing Prometheus on monitor              (parallel)
  └─ Installing Grafana on monitor                (parallel)
Collecting node IPs for Prometheus targets...     (~10 seconds)
Generating Prometheus config with static targets...
Uploading monitoring config to monitor node...
Reloading Prometheus with new targets...
Reloading Grafana with dashboard and datasource...
Waiting for Prometheus to start scraping...       (~30 seconds)
Creating Grafana web tunnel for Apps tab...
Monitoring stack ready!                           ← Done!
```

---

## Configuration

This weave has a single configurable parameter:

### SLICE_NAME

- **Type**: string
- **Default**: `prometheus-monitor`
- **Required**: yes
- **Description**: Name for the FABRIC slice. Only letters, numbers, and
  hyphens are allowed — other characters are replaced with hyphens.

The slice name is used as:
- The FABRIC slice name (visible in the FABRIC portal)
- The identifier for the web tunnel
- The prefix in log messages

### Fixed Configuration

The following settings are built into the weave and not user-configurable:

| Setting | Value | Reason |
|---------|-------|--------|
| Site selection | Auto (first with enough resources) | Simplifies deployment |
| Monitor cores/RAM | 4 / 16 GB | Prometheus + Grafana need headroom |
| Worker cores/RAM | 2 / 8 GB | Minimal for node_exporter |
| Monitor disk | 50 GB | 30 days of Prometheus time-series data |
| Worker disk | 10 GB | node_exporter has negligible disk needs |
| Scrape interval | 15 seconds | Balance between resolution and overhead |
| Data retention | 30 days | Reasonable for experiment duration |
| Grafana access | Anonymous viewer | No login needed for dashboards |
| Health check interval | 30 seconds | Detect failures promptly |

---

## Deployment Phases

The deployment is split into two internal phases managed by `prometheus_monitor.py`:

### Phase 1: Start (Slice Creation)

1. **Site selection**: Queries FABRIC for sites with at least 8 cores,
   32 GB RAM, and 70 GB disk available. Uses the first qualifying site.

2. **Slice creation**: Creates a 3-node slice using the FABlib API:
   - `monitor`: 4 cores, 16 GB RAM, 50 GB disk, Ubuntu 22.04
   - `worker1`: 2 cores, 8 GB RAM, 10 GB disk, Ubuntu 22.04
   - `worker2`: 2 cores, 8 GB RAM, 10 GB disk, Ubuntu 22.04
   - All three nodes get a FABNetv4 (IPv4) interface

3. **Submission**: Submits the slice to FABRIC and waits for all nodes to
   reach SSH-accessible state.

4. **Dataplane setup**: Runs `post_boot_config()` to bring up the FABNetv4
   interfaces on all nodes.

5. **Software installation** (parallel across all nodes):
   - Monitor node: Runs `tools/setup-monitor.sh` — installs node_exporter,
     Prometheus, and Grafana
   - Worker nodes: Run `tools/setup-exporter.sh` — installs node_exporter

### Phase 2: Configure (Service Setup)

1. **IP collection**: Detects the FABNetv4 IP address for each node (from
   the FABlib interface data, or by SSH-ing to the node as fallback).

2. **Prometheus configuration**: Generates `prometheus.yml` with static
   targets for all nodes' node_exporter endpoints (e.g., `["10.135.144.2:9100",
   "10.135.144.3:9100", "10.135.144.4:9100"]`).

3. **Config upload**: Uploads to the monitor node:
   - `prometheus.yml` — target configuration
   - `grafana-datasource.yml` — Prometheus datasource definition
   - `grafana-dashboard-provider.yml` — dashboard file provider
   - `resource-utilization.json` — pre-built Grafana dashboard

4. **Service reload**: Restarts Prometheus and Grafana with the new
   configuration.

5. **Home dashboard**: Sets the resource utilization dashboard as Grafana's
   home dashboard via the Grafana API.

6. **Target verification**: Polls Prometheus `/api/v1/targets` until all
   node_exporter targets report "up" status (up to 2 minutes).

7. **Web tunnel**: Creates an SSH tunnel from the monitor node's port 3000
   to a local port, making Grafana accessible from the LoomAI Apps tab.

8. **Status report**: Saves `results/monitoring-status.json` with all node
   IPs, service URLs, and tunnel information.

---

## Grafana Dashboard

The weave includes a pre-built Grafana dashboard named **"FABRIC Node Resource
Utilization"** with the following panels:

### System Overview

| Panel | Metric | Description |
|-------|--------|-------------|
| **CPU Usage** | `100 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100` | Per-node CPU utilization percentage. Thresholds: green (<70%), yellow (70-90%), red (>90%) |
| **Memory Usage** | `(1 - MemAvailable/MemTotal) * 100` | Per-node memory utilization. Thresholds: green (<70%), yellow (70-90%), red (>90%) |
| **System Load** | `node_load1`, `node_load5` | 1-minute and 5-minute load averages |
| **Disk Usage** | `(1 - avail/size) * 100` for root filesystem | Gauge showing disk utilization. Thresholds: green (<70%), yellow (70-85%), orange (85-95%), red (>95%) |

### Network I/O

| Panel | Metric | Description |
|-------|--------|-------------|
| **Network Receive** | `rate(node_network_receive_bytes_total[5m])` | Incoming network traffic in bytes/sec (excludes loopback, veth, docker, bridge interfaces) |
| **Network Transmit** | `rate(node_network_transmit_bytes_total[5m])` | Outgoing network traffic in bytes/sec |

### Disk & I/O

| Panel | Metric | Description |
|-------|--------|-------------|
| **Disk I/O Read** | `rate(node_disk_read_bytes_total[5m])` | Disk read throughput in bytes/sec (excludes device-mapper devices) |
| **Disk I/O Write** | `rate(node_disk_written_bytes_total[5m])` | Disk write throughput in bytes/sec |

### Stats

| Panel | Metric | Description |
|-------|--------|-------------|
| **CPU Cores** | `count(node_cpu_seconds_total{mode="idle"})` | Number of CPU cores per node |
| **Total Memory** | `node_memory_MemTotal_bytes` | Total memory per node |
| **Uptime** | `time() - node_boot_time_seconds` | Time since last boot |
| **Filesystem Free** | `node_filesystem_avail_bytes{mountpoint="/"}` | Free disk space on root filesystem |

### Dashboard Features

- **Instance selector**: Multi-select variable to filter panels by node
  (e.g., show only the monitor node, or only workers)
- **Auto-refresh**: Dashboard refreshes every 10 seconds
- **Time range**: Default view is the last 1 hour, adjustable in Grafana
- **Responsive layout**: Panels are arranged in a grid that adapts to screen width

---

## Accessing Grafana

### Via LoomAI Apps Tab (Recommended)

The weave automatically creates a web tunnel. After deployment:

1. Click the **Apps** tab in the LoomAI WebUI
2. Click **Grafana Dashboard** to open the dashboard in a new tab
3. No login is required — anonymous viewer access is enabled

### Via Direct URL

If you have SSH access to the monitor node, Grafana is available at:
```
http://<monitor-fabnet-ip>:3000
```

The monitor node's FABNetv4 IP is printed in the deployment log and saved
in `results/monitoring-status.json`.

### Via Manual Tunnel

If the automatic tunnel creation fails, create one manually:

1. Go to the **Apps** tab in LoomAI
2. Click **Create Tunnel**
3. Set:
   - Slice: your slice name
   - Node: `monitor`
   - Remote port: `3000`
   - Label: `Grafana Dashboard`

### Grafana Admin Access

For advanced configuration (creating new dashboards, adding alerts):
- Username: `admin`
- Password: `admin`

Anonymous users have viewer-only access. Admin login is only needed for
editing dashboards or changing Grafana configuration.

---

## Prometheus Metrics

Prometheus scrapes node_exporter on all three nodes every 15 seconds. The
full list of available metrics can be found at:

```
http://<monitor-fabnet-ip>:9090/targets    # View scrape targets and health
http://<monitor-fabnet-ip>:9090/graph      # Query metrics with PromQL
http://<any-node-ip>:9100/metrics          # Raw node_exporter metrics
```

### Commonly Used Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `node_cpu_seconds_total` | counter | CPU time in each mode (idle, user, system, etc.) |
| `node_memory_MemTotal_bytes` | gauge | Total installed memory |
| `node_memory_MemAvailable_bytes` | gauge | Memory available for applications |
| `node_filesystem_avail_bytes` | gauge | Free space on filesystems |
| `node_filesystem_size_bytes` | gauge | Total size of filesystems |
| `node_network_receive_bytes_total` | counter | Total bytes received per interface |
| `node_network_transmit_bytes_total` | counter | Total bytes transmitted per interface |
| `node_disk_read_bytes_total` | counter | Total bytes read from disk |
| `node_disk_written_bytes_total` | counter | Total bytes written to disk |
| `node_load1` | gauge | 1-minute system load average |
| `node_load5` | gauge | 5-minute system load average |
| `node_load15` | gauge | 15-minute system load average |
| `node_boot_time_seconds` | gauge | Unix timestamp of last boot |
| `node_uname_info` | gauge | System information labels (hostname, kernel, etc.) |

### PromQL Examples

```promql
# CPU utilization per node (percentage)
100 - (avg by(instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)

# Memory utilization per node (percentage)
(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100

# Network throughput (bytes/sec, receive)
rate(node_network_receive_bytes_total{device!~"lo|veth.*"}[5m])

# Disk I/O (bytes/sec, write)
rate(node_disk_written_bytes_total[5m])

# Top CPU consumer (by instance)
topk(1, 100 - (avg by(instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100))
```

---

## Monitoring Loop

After deployment, the weave enters a health-check loop that runs every 30
seconds until you click **Stop**. Each cycle:

1. Connects to FABRIC and loads the slice
2. Checks that the slice state is `StableOK`
3. SSH-es to each node and runs `echo ok` to verify connectivity
4. If all checks pass, prints a status line and sleeps 30 seconds
5. If any check fails:
   - Transient errors (auth timeout, SSH hiccup): logs a warning, retries
     next cycle
   - Persistent errors (slice state not StableOK): triggers cleanup and
     deletes the slice

This ensures the slice is automatically cleaned up if something goes wrong,
and you can see the current health status in the log output.

---

## Included Notebooks

### notebooks/tutorial.ipynb — Interactive Tutorial

A Jupyter notebook that teaches you how to:

- Query Prometheus metrics programmatically using the HTTP API
- Load and visualize CPU, memory, and network data with matplotlib
- Build custom analysis beyond what the Grafana dashboard provides
- Understand the monitoring stack's architecture and components

Open in JupyterLab: Start JupyterLab from the LoomAI WebUI, then navigate
to `my_artifacts/Prometheus_Grafana_Monitor/notebooks/tutorial.ipynb`.

---

## File Structure

```
Prometheus_Grafana_Monitor/
├── weave.json                          # Weave metadata, args, topology
├── weave.sh                            # Entry point (bash orchestrator)
├── prometheus_monitor.py               # Lifecycle manager (Python)
├── README.md                           # This file
├── build/                              # Pre-built config files
│   ├── grafana-datasource.yml          # Prometheus datasource for Grafana
│   ├── grafana-dashboard-provider.yml  # Dashboard file provider config
│   └── resource-utilization.json       # Pre-built Grafana dashboard (16 panels)
├── tools/                              # Node setup scripts
│   ├── setup-monitor.sh                # Monitor: Prometheus + Grafana + node_exporter
│   └── setup-exporter.sh               # Workers: node_exporter only
├── notebooks/                          # Jupyter notebooks
│   └── tutorial.ipynb                  # Interactive tutorial and guide
└── results/                            # Generated during runs
    └── monitoring-status.json          # Node IPs, service URLs, tunnel info
```

---

## Troubleshooting

### Deployment Fails: "No site found with enough resources"

**Symptom**: The script exits immediately with a resource error.

**Cause**: No single FABRIC site has 8 cores, 32 GB RAM, and 70 GB disk
available.

**Fix**: Wait for resources to free up, or try again later. FABRIC site
availability fluctuates as other users create and delete slices. You can
check current availability with the `/sites` command in the LoomAI AI
assistant.

### Grafana Dashboard Shows "No Data"

**Symptom**: Panels display "No data" even though the deployment succeeded.

**Cause**: Prometheus hasn't finished its first scrape cycle, or the
datasource isn't configured correctly.

**Fix**:
1. Wait 30 seconds for the first scrape to complete
2. In Grafana, check Settings > Data Sources — Prometheus should be listed
   and show a green "Data source is working" when you click "Test"
3. Check that the instance variable at the top of the dashboard has values
   selected

### Web Tunnel Not Created

**Symptom**: Deployment log shows "WARNING: Could not create tunnel".

**Cause**: The LoomAI backend's tunnel API may not be available.

**Fix**: Create the tunnel manually from the Apps tab:
- Slice: your slice name
- Node: `monitor`
- Remote port: `3000`
- Label: `Grafana Dashboard`

### FABRIC Token Expired

**Symptom**: The monitoring loop logs "WARNING: Could not connect to FABRIC
(transient)" repeatedly.

**Fix**: Go to the **Configure** view in the LoomAI WebUI and refresh your
FABRIC token. The monitoring loop tolerates transient auth errors and will
resume normal operation once the token is valid again.

### node_exporter Target Shows "Down" in Prometheus

**Symptom**: Prometheus targets page shows one or more node_exporter
targets as "down".

**Cause**: FABNetv4 connectivity issue between the monitor and the worker
node, or node_exporter crashed on the worker.

**Fix**:
1. SSH to the worker node: check `systemctl status node_exporter`
2. Verify the FABNetv4 IP: `ip -4 addr show | grep 10\\.`
3. From the monitor node, try: `curl http://<worker-ip>:9100/metrics`
4. If the route is missing, add it: `sudo ip route add 10.128.0.0/10 dev <iface>`

### Slice Gets Deleted Unexpectedly

**Symptom**: The slice disappears while the weave is running.

**Cause**: The monitoring loop detected a persistent health check failure
(slice state not StableOK) and triggered cleanup.

**Fix**: Check the log output for the specific error. Common causes:
- FABRIC maintenance window
- Slice lease expired (default 24 hours)
- Site outage

Re-run the weave to create a fresh slice.

### High CPU or Memory Usage on Monitor Node

**Symptom**: The monitor node shows high resource utilization.

**Cause**: Prometheus uses memory proportional to the number of time series
and the ingestion rate. With 3 nodes and 15s scrape interval, this should be
minimal (~200 MB RAM, <5% CPU).

**Fix**: If resources are tight, the monitor node has 4 cores and 16 GB RAM
which is more than sufficient for this workload. Check if other processes
are consuming resources by SSH-ing to the node.

---

## Extending the Stack

### Adding More Worker Nodes

To monitor additional nodes from other slices:

1. Install node_exporter on the target node (use the `tools/setup-exporter.sh`
   script as a reference)
2. Ensure FABNetv4 connectivity between the monitor and target nodes
3. SSH to the monitor node and edit `/etc/prometheus/prometheus.yml`
4. Add the new target to the `node` job's `static_configs.targets` list
5. Reload Prometheus: `sudo systemctl restart prometheus`

### Adding Custom Metrics

node_exporter exposes hundreds of metrics out of the box. To add
application-specific metrics:

1. Instrument your application with a Prometheus client library
   (available for Python, Go, Java, etc.)
2. Expose a `/metrics` endpoint on a known port
3. Add a new scrape job to `/etc/prometheus/prometheus.yml` on the monitor
4. Create a new Grafana dashboard panel for the custom metrics

### Creating Custom Dashboards

1. Log in to Grafana as admin (admin/admin)
2. Click **+** > **Dashboard** > **Add visualization**
3. Select the **Prometheus** datasource
4. Write your PromQL query
5. Save the dashboard

### Changing the Scrape Interval

SSH to the monitor node and edit `/etc/prometheus/prometheus.yml`:

```yaml
global:
  scrape_interval: 5s  # Change from 15s to 5s for higher resolution
```

Then restart: `sudo systemctl restart prometheus`

Note: shorter intervals increase storage usage and network overhead.

### Increasing Data Retention

By default, Prometheus retains data for 30 days. To change this, SSH to the
monitor node and edit the systemd service:

```bash
sudo systemctl edit prometheus
```

Add:
```ini
[Service]
ExecStart=
ExecStart=/usr/local/bin/prometheus \
  --config.file=/etc/prometheus/prometheus.yml \
  --storage.tsdb.path=/var/lib/prometheus \
  --web.listen-address=0.0.0.0:9090 \
  --storage.tsdb.retention.time=90d
```

Then restart: `sudo systemctl restart prometheus`
