#!/bin/bash
num_agents=$1
base_index=0
pkill -f "main.py swarm-multi"
#python3 kafka_cleanup.py --topic multi
rm -f shutdown
python3 generate_configs.py $num_agents ./config_swarm_multi.yml .
python3 kafka_cleanup.py --topic multi --agents $num_agents --broker zoo-0:9092 --redis zoo-0
rm -rf swarm-multi
mkdir -p swarm-multi swarm-multi

# Launch the Python commands for each agent
for i in $(seq 0 $(($num_agents - 1))); do
    #python3 main.py swarm-multi $(($base_index + $i)) 100 &
    python3 main.py swarm-multi $(($base_index + $i + 1)) 100  $num_agents topo &
done

