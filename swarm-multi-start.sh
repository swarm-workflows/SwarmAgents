#!/bin/bash

pkill -f "main.py swarm-multi"
python3.11 kafka_cleanup.py --topic agent-swarm-multi
rm -rf swarm-multi
rm -rf swarm-multi
mkdir -p swarm-multi swarm-multi
python3.11 main.py swarm-multi 0 100 &
python3.11 main.py swarm-multi 1 100 &
python3.11 main.py swarm-multi 2 100 &
#python3.11 main.py swarm-multi 3 100 &
#python3.11 main.py swarm-multi 4 100 &
#python3.11 main.py swarm-multi 5 100 &
#python3.11 main.py swarm-multi 6 100 &
#python3.11 main.py swarm-multi 7 100 &
#python3.11 main.py swarm-multi 8 100 &
#python3.11 main.py swarm-multi 9 100 &

