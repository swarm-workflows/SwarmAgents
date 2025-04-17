#!/bin/bash
touch shutdown
sleep 30
pkill -f "main.py swarm-multi"

