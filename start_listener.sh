#!/bin/bash

# Variables
SCRIPT="bybit-listener.py"
LOG_FILE="./logs/listener.log"
VENV_DIR="$(pwd)/env"         # Absolute path to the virtual environment
PROJECT_DIR="$(pwd)"          # Absolute path to the project directory

# Function to print messages with timestamp
echo_msg() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

echo_msg "Starting Bybit Listener..."

# Activate virtual environment
source "$VENV_DIR/bin/activate"

# Navigate to the project directory (optional if already there)
cd "$PROJECT_DIR"

# Start the listener script using nohup if not already running
if pgrep -f "$SCRIPT" > /dev/null
then
    echo_msg "Bybit Listener is already running."
else
    nohup python "$SCRIPT" >> "$LOG_FILE" 2>&1 &
    echo_msg "Bybit Listener started."
fi
