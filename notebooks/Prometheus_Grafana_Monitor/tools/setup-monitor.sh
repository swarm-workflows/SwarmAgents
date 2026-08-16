#!/bin/bash
set -ex

# ── Wait for FABNetv4 dataplane interface ────────────────────────────
# The dataplane NIC is NOT enp3s0/ens3/eth0 (those are management).
# Look for a 10.x.x.x IP on a non-management interface.
echo "### PROGRESS: Waiting for FABNetv4 interface"
FABNET_IP=""
for i in $(seq 1 60); do
  FABNET_IP=$(ip -4 addr show | grep -v '127.0.0.1' | grep 'inet ' \
    | grep -v -E 'enp3s0|ens3|eth0' \
    | grep -oP '\d+\.\d+\.\d+\.\d+(?=/\d+)' | head -1)
  if [ -n "$FABNET_IP" ]; then
    echo "FABNetv4 IP: $FABNET_IP"
    break
  fi
  sleep 5
done

if [ -z "$FABNET_IP" ]; then
  echo "WARNING: FABNetv4 dataplane interface not found after 5 minutes, continuing anyway"
fi

# ── Install node_exporter (monitor is also a scrape target) ──────────
echo "### PROGRESS: Installing node_exporter on monitor"
NE_VER="1.8.2"
cd /tmp
curl -fsSLO "https://github.com/prometheus/node_exporter/releases/download/v${NE_VER}/node_exporter-${NE_VER}.linux-amd64.tar.gz"
tar xzf "node_exporter-${NE_VER}.linux-amd64.tar.gz"
sudo cp "node_exporter-${NE_VER}.linux-amd64/node_exporter" /usr/local/bin/
sudo chmod +x /usr/local/bin/node_exporter

sudo tee /etc/systemd/system/node_exporter.service > /dev/null <<'EOF'
[Unit]
Description=Prometheus Node Exporter
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=nobody
ExecStart=/usr/local/bin/node_exporter \
  --web.listen-address=0.0.0.0:9100
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now node_exporter

# ── Install Prometheus ────────────────────────────────────────────────
echo "### PROGRESS: Installing Prometheus"
PROM_VER="2.53.3"
cd /tmp
curl -fsSLO "https://github.com/prometheus/prometheus/releases/download/v${PROM_VER}/prometheus-${PROM_VER}.linux-amd64.tar.gz"
tar xzf "prometheus-${PROM_VER}.linux-amd64.tar.gz"
sudo cp "prometheus-${PROM_VER}.linux-amd64/prometheus" /usr/local/bin/
sudo cp "prometheus-${PROM_VER}.linux-amd64/promtool"   /usr/local/bin/
sudo mkdir -p /etc/prometheus /var/lib/prometheus

# Placeholder config — deploy.sh will upload the real one with targets
if [ -f ~/monitoring-config/prometheus.yml ]; then
  sudo cp ~/monitoring-config/prometheus.yml /etc/prometheus/prometheus.yml
  echo "Using pre-configured prometheus.yml"
else
  sudo tee /etc/prometheus/prometheus.yml > /dev/null <<'PROMEOF'
global:
  scrape_interval: 15s
  evaluation_interval: 15s
scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets: ['localhost:9090']
  - job_name: node
    static_configs:
      - targets: ['localhost:9100']
PROMEOF
fi

sudo tee /etc/systemd/system/prometheus.service > /dev/null <<'SVCEOF'
[Unit]
Description=Prometheus
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=nobody
ExecStart=/usr/local/bin/prometheus \
  --config.file=/etc/prometheus/prometheus.yml \
  --storage.tsdb.path=/var/lib/prometheus \
  --web.listen-address=0.0.0.0:9090 \
  --storage.tsdb.retention.time=30d
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
SVCEOF

sudo chown -R nobody: /var/lib/prometheus /etc/prometheus
sudo systemctl daemon-reload
sudo systemctl enable --now prometheus

# ── Install Grafana ───────────────────────────────────────────────────
echo "### PROGRESS: Installing Grafana"
sudo apt-get update -qq
sudo apt-get install -y -qq apt-transport-https software-properties-common wget
wget -q -O /tmp/grafana.gpg.key https://apt.grafana.com/gpg.key
cat /tmp/grafana.gpg.key | sudo gpg --dearmor -o /usr/share/keyrings/grafana.gpg
echo "deb [signed-by=/usr/share/keyrings/grafana.gpg] https://apt.grafana.com stable main" | \
  sudo tee /etc/apt/sources.list.d/grafana.list
sudo apt-get update -qq
sudo apt-get install -y -qq grafana

# ── Provision Grafana from pre-uploaded configs (if deploy.sh ran) ───
echo "### PROGRESS: Configuring Grafana"

CONFIG_DIR=~/monitoring-config/grafana-provisioning

# Datasource: Prometheus at localhost:9090
sudo mkdir -p /etc/grafana/provisioning/datasources
if [ -f "$CONFIG_DIR/datasources/datasource.yml" ]; then
  sudo cp "$CONFIG_DIR/datasources/datasource.yml" /etc/grafana/provisioning/datasources/
  echo "  Datasource provisioned from deploy config"
else
  sudo tee /etc/grafana/provisioning/datasources/prometheus.yml > /dev/null <<'DSEOF'
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    uid: prometheus
    access: proxy
    url: http://localhost:9090
    isDefault: true
    editable: true
DSEOF
fi

# Dashboard provider + dashboard JSON
sudo mkdir -p /etc/grafana/provisioning/dashboards
if [ -f "$CONFIG_DIR/dashboards/dashboard.yml" ]; then
  sudo cp "$CONFIG_DIR/dashboards/dashboard.yml" /etc/grafana/provisioning/dashboards/
fi
if [ -f "$CONFIG_DIR/dashboards/resource-utilization.json" ]; then
  sudo cp "$CONFIG_DIR/dashboards/resource-utilization.json" /etc/grafana/provisioning/dashboards/
  echo "  Resource utilization dashboard provisioned"
fi

# Enable anonymous access and iframe embedding
sudo sed -i '/^\[security\]/,/^\[/{/^allow_embedding/d}' /etc/grafana/grafana.ini
sudo sed -i '/^\[auth.anonymous\]/,/^\[/{/^enabled\|^org_role/d}' /etc/grafana/grafana.ini

sudo tee -a /etc/grafana/grafana.ini > /dev/null <<'INIEOF'

[security]
allow_embedding = true

[auth.anonymous]
enabled = true
org_role = Viewer
INIEOF

sudo systemctl enable --now grafana-server

# ── Set dashboard as Grafana home ─────────────────────────────────────
(
  for i in $(seq 1 60); do
    curl -sf http://localhost:3000/api/health > /dev/null 2>&1 && break
    sleep 5
  done

  DASH_UID=$(curl -sf "http://localhost:3000/api/search?type=dash-db" \
    -u admin:admin 2>/dev/null | \
    grep -oP '"uid"\s*:\s*"\K[^"]+' | head -1)

  if [ -n "$DASH_UID" ]; then
    curl -sf -X PUT http://localhost:3000/api/org/preferences \
      -H 'Content-Type: application/json' \
      -u admin:admin \
      -d "{\"homeDashboardUID\":\"$DASH_UID\"}" > /dev/null
    echo "Grafana: dashboard set as home (UID=$DASH_UID)"
  fi
) &

echo ""
echo "=== Monitor setup complete ==="
echo "FABNet IP:   ${FABNET_IP:-not yet assigned}"
echo "Prometheus:  http://${FABNET_IP:-<this-node>}:9090"
echo "Grafana:     http://${FABNET_IP:-<this-node>}:3000  (anonymous viewer, no login)"
