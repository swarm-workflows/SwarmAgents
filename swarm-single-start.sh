#!/bin/bash
num_agents=$1
base_index=0
pkill -f "main.py swarm-single"
#python3 kafka_cleanup.py --topic single
rm -f shutdown
python3 generate_configs.py $num_agents ./config_swarm_single.yml .
python3 kafka_cleanup.py --topic single --agents $num_agents --broker zoo-0:9092 --redis zoo-0
rm -rf swarm-single
mkdir -p swarm-single swarm-single

# Launch the Python commands for each agent
for i in $(seq 0 $(($num_agents - 1))); do
    #python3 main.py swarm-single $(($base_index + $i)) 100 &
    python3 main.py swarm-single $(($base_index + $i + 1)) 100  $num_agents topo &
done

