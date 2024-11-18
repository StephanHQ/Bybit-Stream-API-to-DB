# bybit_listener/start_listener.sh

#!/bin/bash

# Variables
VENV_DIR="env"
SCRIPT="bybit_listener.py"
LOG_FILE="logs/listener.log"

# Function to print messages with timestamp
echo_msg() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

echo_msg "Starting Bybit Listener..."

# Activate virtual environment
source "$VENV_DIR/bin/activate"

# Navigate to the project directory
cd "$(dirname "$0")"

# Start the listener script using nohup if not already running
if pgrep -f "$SCRIPT" > /dev/null
then
    echo_msg "Bybit Listener is already running."
else
    nohup python "$SCRIPT" >> "$LOG_FILE" 2>&1 &
    echo_msg "Bybit Listener started."
fi
