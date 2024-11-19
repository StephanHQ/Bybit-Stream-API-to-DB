#!/bin/bash

# Variables
SCRIPT="bybit-listener.py"
VENV_DIR="$(pwd)/env"         # Absolute path to the virtual environment
PROJECT_DIR="$(pwd)"          # Absolute path to the project directory
LOG_FILE="./logs/listener.log"

# Function to print messages with timestamp
echo_msg() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

echo_msg "Restarting Bybit Listener..."

# Activate virtual environment
source "$VENV_DIR/bin/activate"

# Navigate to the project directory
cd "$PROJECT_DIR"

# Stop the listener if it's running
if pgrep -f "$SCRIPT" > /dev/null
then
    pkill -f "$SCRIPT"
    echo_msg "Bybit Listener stopped."
    sleep 2
fi

# Start the listener
nohup python "$SCRIPT" >> "$LOG_FILE" 2>&1 &
echo_msg "Bybit Listener restarted."
