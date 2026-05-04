#!/bin/bash
set -e
cd /root/SwarmAgents

echo "=== Starting Hierarchical Tests (10 runs) ==="
python3.11 batch_tests_v2.py \
  --runs 10 \
  --base-out runs/pegasus-hierarchical \
  --mode remote \
  --agent-type resource \
  --agents 30 \
  --topology hierarchical \
  --hierarchical-level1-agent-type resource \
  --jobs 547 \
  --db-host database \
  --fit-all \
  --agents-per-host 1 \
  --shutdown-after-seconds 900 \
  --sleep-between 10

echo "=== Hierarchical Tests Complete ==="
echo "=== Starting Mesh Tests (10 runs) ==="
python3.11 batch_tests_v2.py \
  --runs 10 \
  --base-out runs/pegasus-mesh \
  --mode remote \
  --agent-type resource \
  --agents 30 \
  --topology mesh \
  --jobs 547 \
  --db-host database \
  --fit-all \
  --agents-per-host 1 \
  --shutdown-after-seconds 900 \
  --sleep-between 10

echo "=== All Tests Complete ==="
