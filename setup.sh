#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e
set -x  # Enable debugging

# Variables (modify these as per your environment)
PROJECT_DIR="/home/osgawcom/ichhaberecht.com/bybit_stream_data"
VENV_DIR="$PROJECT_DIR/env"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/listener.log"
STORAGE_PATH="$PROJECT_DIR/storage"
REQUIRED_PYTHON_VERSION="3.8"

# Function to print messages with timestamp
echo_msg() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Function to check Python version
check_python_version() {
    local python_version
    python_version=$(python3 --version 2>&1 | awk '{print $2}')
    if [[ ! $python_version =~ $REQUIRED_PYTHON_VERSION ]]; then
        echo_msg "Python version $python_version is not compatible. Required: $REQUIRED_PYTHON_VERSION or higher."
        echo_msg "Please upgrade Python before proceeding."
        exit 1
    fi
}

# Function to set permissions
set_permissions() {
    local target="$1"
    local permissions="$2"

    if [ -e "$target" ]; then
        chmod "$permissions" "$target"
        echo_msg "Set permissions $permissions on $target"
    else
        echo_msg "Target $target does not exist, skipping permission setting."
    fi
}

echo_msg "Starting setup..."

# Check Python version
check_python_version

# Create necessary directories with restrictive permissions
echo_msg "Creating directories..."
mkdir -p "$STORAGE_PATH"
set_permissions "$STORAGE_PATH" 700

mkdir -p "$LOG_DIR"
set_permissions "$LOG_DIR" 700

# Navigate to the project directory
cd "$PROJECT_DIR"

# Create or recreate virtual environment
if [ ! -d "$VENV_DIR" ]; then
    echo_msg "Creating Python virtual environment..."
    python3 -m venv "$VENV_DIR" || {
        echo_msg "Virtual environment creation failed. Attempting to upgrade pip and setuptools globally."
        sudo python3 -m ensurepip --upgrade || {
            echo_msg "ensurepip failed. Please ensure Python installation is complete."
            exit 1
        }
        python3 -m venv "$VENV_DIR"
    }
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
if [ -f "$PROJECT_DIR/requirements.txt" ]; then
    echo_msg "Installing dependencies..."
    pip install -r "$PROJECT_DIR/requirements.txt"
else
    echo_msg "No requirements.txt found. Skipping dependency installation."
fi

# Set permissions for start and restart scripts
echo_msg "Setting permissions for start and restart scripts..."
set_permissions "$PROJECT_DIR/start_listener.sh" 700
set_permissions "$PROJECT_DIR/restart_listener.sh" 700

echo_msg "Setup completed successfully."
