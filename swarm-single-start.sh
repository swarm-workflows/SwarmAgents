#!/bin/bash

pkill -f "main.py swarm-single"
python3.11 kafka_cleanup.py --topic agent-swarm-single
rm -rf swarm-single
rm -rf swarm-single
mkdir -p swarm-single swarm-single
python3.11 main.py swarm-single 0 100 &
python3.11 main.py swarm-single 1 100 &
python3.11 main.py swarm-single 2 100 &
#python3.11 main.py swarm-single 3 100 &
#python3.11 main.py swarm-single 4 100 &
#python3.11 main.py swarm-single 5 100 &
#python3.11 main.py swarm-single 6 100 &
#python3.11 main.py swarm-single 7 100 &
#python3.11 main.py swarm-single 8 100 &
#python3.11 main.py swarm-single 9 100 &

