#!/bin/bash

# Function to print messages with timestamp
echo_msg() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

echo_msg "Restarting Flask web application and WebSocket Listener..."

# Stop existing services
./stop_listener.sh

# Start services
./start_listener.sh

echo_msg "All services restarted successfully."
