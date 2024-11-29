#!/bin/bash

# Variables
SCRIPT="bybit-listener.py"
LOG_FILE="$(pwd)/logs/listener.log"
VENV_DIR="$(pwd)/env"
PROJECT_DIR="$(pwd)"

# Function to print messages with timestamp
echo_msg() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

echo_msg "Starting Bybit Listener..."

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

# Check if the process is already running
if ps aux | grep "[p]ython .*${SCRIPT}" > /dev/null
then
    echo_msg "Bybit Listener is already running. Restarting..."
    pkill -f "python .*${SCRIPT}"
    sleep 2
fi

# Start the listener script
nohup python "$SCRIPT" >> "$LOG_FILE" 2>&1 &
if [ $? -eq 0 ]; then
    echo_msg "Bybit Listener started successfully."
else
    echo_msg "Failed to start Bybit Listener. Check $LOG_FILE for details."
fi
