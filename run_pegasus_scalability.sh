#!/bin/bash
set -e
cd /root/SwarmAgents

echo "=== Starting Hierarchical Weak Scaling Tests ==="
echo "=== Weak scaling: jobs proportional to agents (~18 jobs/agent) ==="

# Save original jobs directory
cp -r jobs/ jobs_547_orig/

echo "=== 60 Agents (2 per VM), 1094 jobs - 10 runs ==="
rm -rf jobs/
cp -r jobs_1094/ jobs/
python3.11 batch_tests_v2.py \
  --runs 10 \
  --base-out runs/pegasus-hierarchical-60 \
  --mode remote \
  --agent-type resource \
  --agents 60 \
  --topology hierarchical \
  --hierarchical-level1-agent-type resource \
  --jobs 1094 \
  --db-host database \
  --fit-all \
  --agents-per-host 2 \
  --shutdown-after-seconds 900 \
  --sleep-between 10

echo "=== 60 Agent Tests Complete ==="

echo "=== 120 Agents (4 per VM), 2188 jobs - 10 runs ==="
rm -rf jobs/
cp -r jobs_2188/ jobs/
python3.11 batch_tests_v2.py \
  --runs 10 \
  --base-out runs/pegasus-hierarchical-120 \
  --mode remote \
  --agent-type resource \
  --agents 120 \
  --topology hierarchical \
  --hierarchical-level1-agent-type resource \
  --jobs 2188 \
  --db-host database \
  --fit-all \
  --agents-per-host 4 \
  --shutdown-after-seconds 900 \
  --sleep-between 10

echo "=== 120 Agent Tests Complete ==="

# Restore original jobs directory
rm -rf jobs/
cp -r jobs_547_orig/ jobs/
rm -rf jobs_547_orig/

echo "=== All Weak Scaling Tests Complete ==="
