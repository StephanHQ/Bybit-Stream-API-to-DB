#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Variables
VENV_DIR="env"
STORAGE_PATH="./parquet_data"
LOG_DIR="./logs"
LOG_FILE="$LOG_DIR/listener.log"

# Function to print messages with timestamp
echo_msg() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

echo_msg "Starting setup..."

# Create necessary directories
echo_msg "Creating directories..."
mkdir -p "$STORAGE_PATH"
mkdir -p "$LOG_DIR"

# Navigate to the project directory
cd "$(dirname "$0")"

# Create virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo_msg "Creating Python virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

# Activate virtual environment
echo_msg "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

# Upgrade pip
echo_msg "Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo_msg "Installing dependencies..."
pip install -r requirements.txt

# Make start and restart scripts executable
chmod +x start_listener.sh
chmod +x restart_listener.sh

echo_msg "Setup completed successfully."
