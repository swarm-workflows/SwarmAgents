#!/bin/bash

pkill -f "main.py pbft"
python3.11 kafka_cleanup.py --topic agent-pbft
rm -rf logs-pbft
rm -rf pbft
mkdir -p pbft logs-pbft
python3.11 main.py pbft 0 100 &
python3.11 main.py pbft 1 100 &
python3.11 main.py pbft 2 100 &
#python3.11 main.py pbft 3 100 &
#python3.11 main.py pbft 4 100 &
#python3.11 main.py pbft 5 100 &
#python3.11 main.py pbft 6 100 &
#python3.11 main.py pbft 7 100 &
#python3.11 main.py pbft 8 100 &
#python3.11 main.py pbft 9 100 &

