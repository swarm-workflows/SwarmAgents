#!/bin/bash
#
# Prometheus Grafana Monitor - Weave Orchestrator
#
# This script is the entry point when you click "Run" in the WebUI.
# It calls prometheus_monitor.py with four commands:
#   start     - Create and provision the 3-node slice, run boot configs
#   configure - Set up routes, Prometheus targets, Grafana dashboard, tunnel
#   monitor   - Check slice and service health every 30 seconds
#   stop      - Delete the slice (called when you click "Stop")
#
# The script runs until you click Stop or a failure is detected.
#

SLICE_NAME="${SLICE_NAME:-${1:-prometheus-monitor}}"

# Clean the name: only letters, numbers, and hyphens allowed
SLICE_NAME=$(echo "$SLICE_NAME" | sed 's/[^a-zA-Z0-9-]/-/g' | sed 's/--*/-/g' | sed 's/^-//;s/-$//')
if [ -z "$SLICE_NAME" ]; then
  echo "ERROR: SLICE_NAME not set" >&2
  exit 1
fi

SCRIPT="prometheus_monitor.py"

# --- Graceful shutdown handler ---
# When you click "Stop" in the WebUI, this function runs.
cleanup() {
  echo ""
  echo "### PROGRESS: Stop requested — cleaning up..."
  python3 "$SCRIPT" stop "$SLICE_NAME" 2>&1 || true
  echo "### PROGRESS: Done."
  exit 0
}

trap cleanup SIGTERM SIGINT

# --- Create, provision, and configure in one shot ---
if ! python3 "$SCRIPT" deploy "$SLICE_NAME"; then
  echo "ERROR: Failed to deploy monitoring stack"
  exit 1
fi

# --- Monitor loop ---
# Runs every 30 seconds until you click Stop or a failure is detected.
echo "### PROGRESS: Monitoring (click Stop to tear down)..."
while true; do
  if ! python3 "$SCRIPT" monitor "$SLICE_NAME"; then
    echo "ERROR: Monitor detected a problem — cleaning up..."
    python3 "$SCRIPT" stop "$SLICE_NAME" 2>&1 || true
    exit 1
  fi
  sleep 30 &
  wait $! 2>/dev/null || true
done
