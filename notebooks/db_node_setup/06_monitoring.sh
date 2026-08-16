#!/usr/bin/env bash
# Step 6: monitoring (notebook cells 23-25).
#   a) install node_exporter on every SWARM node (agents + database)
#   b) provision the monitor VM with Prometheus + Grafana using the
#      pre-generated plan/prometheus.yml
#   c) verify scrape targets
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

MON_DIR="$ROOT_DIR/Prometheus_Grafana_Monitor"

banner "Step 6a: node_exporter on SWARM nodes"

setup_exporter() {
    local n="$1"
    npush "$MON_DIR/tools/setup-exporter.sh" "$n" "setup-exporter.sh" \
        && nssh "$n" "chmod +x setup-exporter.sh && sudo ./setup-exporter.sh"
}

run_nodes setup_exporter $(swarm_nodes)

banner "Step 6b: provision monitor VM ($MONITOR_NODE)"

# Stage monitoring-config locally, then ship as one tarball
MC="$STATE_DIR/monitoring-config"
rm -rf "$MC"
mkdir -p "$MC/grafana-provisioning/datasources" "$MC/grafana-provisioning/dashboards"
cp "$PLAN_DIR/prometheus.yml"                    "$MC/prometheus.yml"
cp "$MON_DIR/build/grafana-datasource.yml"       "$MC/grafana-provisioning/datasources/datasource.yml"
cp "$MON_DIR/build/grafana-dashboard-provider.yml" "$MC/grafana-provisioning/dashboards/dashboard.yml"
cp "$MON_DIR/build/resource-utilization.json"    "$MC/grafana-provisioning/dashboards/resource-utilization.json"

npush_dir "$MC" "$MONITOR_NODE"
npush "$MON_DIR/tools/setup-monitor.sh" "$MONITOR_NODE" "setup-monitor.sh"
# setup-monitor.sh reads ~/monitoring-config as root, so stage it there
nssh "$MONITOR_NODE" "sudo rm -rf /root/monitoring-config && sudo cp -r monitoring-config /root/monitoring-config"
nssh "$MONITOR_NODE" "chmod +x setup-monitor.sh && sudo ./setup-monitor.sh" \
    >"$LOG_DIR/monitor-setup.log" 2>&1
echo "Monitor provisioned (log: $LOG_DIR/monitor-setup.log)"

banner "Step 6c: verify scrape targets"
echo "Waiting 30s for a scrape cycle..."
sleep 30
nssh "$MONITOR_NODE" "curl -s localhost:9090/api/v1/targets | python3 -c \"import json,sys; ts=json.load(sys.stdin)['data']['activeTargets']; up=sum(1 for t in ts if t['health']=='up'); print(f'{up}/{len(ts)} targets up'); [print(' DOWN:', t['labels'].get('node', t['scrapeUrl'])) for t in ts if t['health']!='up']\""

MON_IP="$(awk -v h="$MONITOR_NODE" '$2==h{print $1; exit}' "$PLAN_DIR/etc_hosts.txt")"
echo
echo "Prometheus: http://$MON_IP:9090  (from any node in either slice)"
echo "Grafana:    http://$MON_IP:3000  (anonymous viewer, no login)"
echo "From your laptop, tunnel via the db node:  ssh -L 3000:$MON_IP:3000 -L 9090:$MON_IP:9090 swarm"
