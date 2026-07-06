#!/bin/bash
# Scenario C driver: launch run, kill group-4 leaves (agents 21-25) after
# startup, restart them mid-run. Usage: scenario_c_driver.sh <run-name>
# NOTE: `wait` must target explicit pids — a bare `wait` also blocks on the
# backgrounded run_test.py for the whole run.
set -ux
RUN=$1
cd /root/SwarmAgents
nohup python3.11 run_test.py --mode remote --agent-type resource --agents 30 \
  --agents-per-host 1 --topology hierarchical --hierarchical-level1-agent-type resource \
  --co-parents 5 --jobs 231 --jobs-per-proposal 10 --jobs-per-interval 1 --job-interval 1 \
  --pegasus-profiles pegasus-data/all_profiles_nodtn.txt --pegasus-input-type text \
  --db-host database --agent-hosts-file agent_hosts_30.txt --shutdown-after-seconds 1200 \
  --run-dir "runs/$RUN" > "runs/$RUN.launch.log" 2>&1 &
echo "$(date +%s) run_launched" > "runs/$RUN.events"
sleep 75
pids=()
for i in 21 22 23 24 25; do
  timeout 30 ssh -n -o BatchMode=yes -o ConnectTimeout=10 "agent-$i" \
    'pkill -f "^python3.11 main.py"; true' &
  pids+=($!)
done
wait "${pids[@]}"
echo "$(date +%s) group4_killed" >> "runs/$RUN.events"
sleep 105
pids=()
for i in 21 22 23 24 25; do
  timeout 30 ssh -n -o BatchMode=yes -o ConnectTimeout=10 "agent-$i" \
    "cd /root/SwarmAgents && nohup bash ./swarm-multi-start.sh resource 1 hierarchical 231 database 10 --use-config-dir --start-offset $((i-1)) > agent_${i}_restart.log 2>&1 &" &
  pids+=($!)
done
wait "${pids[@]}"
echo "$(date +%s) group4_restarted" >> "runs/$RUN.events"
