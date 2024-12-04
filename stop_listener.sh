#!/bin/bash

# Script name
SCRIPT="bybit_listener.py"

# Function to print messages with timestamp
echo_msg() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

echo_msg "Stopping Bybit Listener..."

# Find the process ID of the running script
PID=$(pgrep -f "$SCRIPT")

if [ -n "$PID" ]; then
    # Kill the process
    kill "$PID"
    echo_msg "Bybit Listener (PID: $PID) stopped."
else
    echo_msg "Bybit Listener is not running."
fi
