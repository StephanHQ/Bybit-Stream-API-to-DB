#!/bin/bash

# Variables
SCRIPT="bybit_websocket_listener.py"  # Updated to the WebSocket listener script
LOG_FILE="$(pwd)/logs/listener.log"
VENV_DIR="$(pwd)/env"
PROJECT_DIR="$(pwd)"

# Function to print messages with timestamp
echo_msg() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

echo_msg "Starting Bybit WebSocket Listener..."

# Activate virtual environment
if [ -f "$VENV_DIR/bin/activate" ]; then
    source "$VENV_DIR/bin/activate"
    echo_msg "Using Python: $(which python)"
else
    echo_msg "Virtual environment not found at $VENV_DIR."
    exit 1
fi

# Navigate to the project directory
if [ -d "$PROJECT_DIR" ]; then
    cd "$PROJECT_DIR"
else
    echo_msg "Project directory $PROJECT_DIR not found."
    exit 1
fi

# Ensure log directory exists
mkdir -p "$(dirname "$LOG_FILE")"

# Check if the WebSocket listener is already running
if pgrep -f "$SCRIPT" > /dev/null
then
    echo_msg "Bybit WebSocket Listener is already running. Restarting..."
    pkill -f "$SCRIPT"
    sleep 2
else
    echo_msg "Bybit WebSocket Listener is not running. Starting..."
fi

# Start the WebSocket listener script
nohup python "$SCRIPT" >> "$LOG_FILE" 2>&1 &
if [ $? -eq 0 ]; then
    echo_msg "Bybit WebSocket Listener started successfully."
else
    echo_msg "Failed to start Bybit WebSocket Listener. Check $LOG_FILE for details."
fi
