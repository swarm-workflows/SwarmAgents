#!/bin/bash
touch shutdown
sleep 180
pkill -f "main_llm.py"

