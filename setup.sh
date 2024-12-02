#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Variables (modify these as per your environment)
VENV_DIR="env"
LOG_DIR="./logs"
LOG_FILE="$LOG_DIR/listener.log"
STORAGE_PATH="./storage"  # Defined STORAGE_PATH

# Function to print messages with timestamp
echo_msg() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

echo_msg "Starting setup..."

# Create necessary directories with restrictive permissions
echo_msg "Creating directories..."
mkdir -p "$STORAGE_PATH" && chmod 700 "$STORAGE_PATH"
mkdir -p "$LOG_DIR" && chmod 700 "$LOG_DIR"

# Navigate to the project directory
cd "$(dirname "$0")"

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

# Ensure necessary build tools are installed (for macOS)
echo_msg "Installing necessary build tools..."
if ! xcode-select --print-path &>/dev/null; then
    echo_msg "Installing Command Line Tools..."
    xcode-select --install
else
    echo_msg "Command Line Tools already installed."
fi

# Upgrade pip, setuptools, and wheel
echo_msg "Upgrading pip, setuptools, and wheel..."
pip install --upgrade pip setuptools wheel

# Install remaining dependencies
echo_msg "Installing remaining dependencies..."
pip install -r requirements.txt

# Make start and restart scripts executable
echo_msg "Setting permissions for start and restart scripts..."
chmod 700 start_listener.sh
chmod 700 restart_listener.sh

echo_msg "Setup completed successfully."
