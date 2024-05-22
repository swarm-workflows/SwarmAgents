#!/bin/bash

# Check if the PID file exists
if [ -f agent_pids.txt ]; then
  # Read the PIDs from the file
  pids=$(cat agent_pids.txt)
  
  # Loop through each PID and kill the process
  for pid in $pids; do
    kill $pid
    echo "Killed process with PID $pid"
  done
  
  # Remove the PID file
  rm agent_pids.txt
else
  echo "No PID file found. No processes to kill."
fi

