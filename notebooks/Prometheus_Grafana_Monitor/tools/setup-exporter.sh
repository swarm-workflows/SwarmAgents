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

# ── Install node_exporter ─────────────────────────────────────────────
echo "### PROGRESS: Installing node_exporter"
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
echo "### PROGRESS: node_exporter running on port 9100"

sleep 2
curl -sf http://localhost:9100/metrics | head -3 || echo "WARNING: node_exporter not yet responding"

echo "=== node_exporter running (FABNet IP: ${FABNET_IP:-unknown}) ==="
