# bybit_listener/restart_listener.sh

#!/bin/bash

# Variables
SCRIPT_NAME="bybit_listener.py"
VENV_DIR="env"

# Function to print messages with timestamp
echo_msg() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

echo_msg "Checking if Bybit Listener is running..."

# Activate virtual environment
source "$VENV_DIR/bin/activate"

# Navigate to the project directory
cd "$(dirname "$0")"

# Check if the listener is running
if pgrep -f "$SCRIPT_NAME" > /dev/null
then
    echo_msg "Bybit Listener is already running."
else
    echo_msg "Bybit Listener is not running. Starting listener..."
    ./start_listener.sh
fi
