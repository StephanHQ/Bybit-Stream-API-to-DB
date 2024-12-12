#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Function to print messages with timestamp
echo_msg() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

echo_msg "Starting setup..."

# Determine the directory where the script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"

# Define other directories based on PROJECT_DIR
VENV_DIR="$PROJECT_DIR/env"
LOG_DIR="$PROJECT_DIR/logs"
LISTENER_LOG="$LOG_DIR/listener.log"
GUNICORN_LOG="$LOG_DIR/gunicorn.log"
STORAGE_PATH="$PROJECT_DIR/storage"

# Create necessary directories with restrictive permissions
echo_msg "Creating directories..."
mkdir -p "$STORAGE_PATH" && chmod 700 "$STORAGE_PATH"
mkdir -p "$LOG_DIR" && chmod 700 "$LOG_DIR"

# Navigate to the project directory
cd "$PROJECT_DIR"

# Create virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo_msg "Creating Python virtual environment..."
    python3 -m venv "$VENV_DIR"
    echo_msg "Virtual environment created."
else
    echo_msg "Virtual environment already exists."
fi

# Activate virtual environment
echo_msg "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

# Upgrade pip, setuptools, and wheel
echo_msg "Upgrading pip, setuptools, and wheel..."
pip install --upgrade pip setuptools wheel

# Install remaining dependencies
echo_msg "Installing dependencies..."
pip install -r "$PROJECT_DIR/requirements.txt"

# Make start and restart scripts executable
echo_msg "Setting permissions for scripts..."
chmod 700 "$PROJECT_DIR/start_listener.sh"
chmod 700 "$PROJECT_DIR/restart_listener.sh"
chmod 700 "$PROJECT_DIR/stop_listener.sh"

echo_msg "Setup completed successfully."
