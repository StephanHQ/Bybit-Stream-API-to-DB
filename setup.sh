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
REQUIRED_PYTHON_VERSION="3.12"
PYTHON_EXEC="python3.12"
PYTHON_INSTALL_DIR="/usr/local"
PYTHON_SOURCE_URL="https://www.python.org/ftp/python/3.12.0/Python-3.12.0.tgz"
PYTHON_SOURCE_DIR="/tmp/python3.12"

# Function to print messages with timestamp
echo_msg() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Function to check if Python 3.12 is installed
check_python_installation() {
    if ! command -v $PYTHON_EXEC &> /dev/null; then
        echo_msg "$PYTHON_EXEC is not installed."
        echo_msg "Installing Python $REQUIRED_PYTHON_VERSION..."
        install_python
    else
        local python_version
        python_version=$($PYTHON_EXEC --version 2>&1 | awk '{print $2}')
        if [[ ! $python_version =~ ^3\.12(\.[0-9]+)?$ ]]; then
            echo_msg "Python version $python_version is not compatible. Required: $REQUIRED_PYTHON_VERSION."
            echo_msg "Reinstalling Python $REQUIRED_PYTHON_VERSION..."
            install_python
        else
            echo_msg "Python $REQUIRED_PYTHON_VERSION is already installed."
        fi
    fi
}

# Function to install Python 3.12 without sudo
install_python() {
    echo_msg "Installing dependencies for Python build..."
    
    # Check for 'sudo' availability
    if command -v sudo &> /dev/null; then
        SUDO="sudo"
    else
        echo_msg "sudo is not available. Attempting installation without it."
        SUDO=""
    fi

    $SUDO yum groupinstall -y "Development Tools" || echo_msg "Failed to install development tools."
    $SUDO yum install -y gcc gcc-c++ zlib-devel bzip2 bzip2-devel readline-devel \
        sqlite sqlite-devel openssl-devel xz xz-devel libffi-devel wget || echo_msg "Failed to install dependencies."

    echo_msg "Downloading Python $REQUIRED_PYTHON_VERSION source..."
    mkdir -p $PYTHON_SOURCE_DIR
    wget -O /tmp/Python.tgz $PYTHON_SOURCE_URL
    tar -xzf /tmp/Python.tgz -C $PYTHON_SOURCE_DIR --strip-components=1

    echo_msg "Building and installing Python $REQUIRED_PYTHON_VERSION..."
    cd $PYTHON_SOURCE_DIR

    # Fix permissions for the source directory
    chmod -R u+x $PYTHON_SOURCE_DIR
    chmod -R u+r $PYTHON_SOURCE_DIR

    ./configure --enable-optimizations --prefix=$PYTHON_INSTALL_DIR || { echo_msg "Configure failed."; exit 1; }
    make -j$(nproc) || { echo_msg "Make failed."; exit 1; }
    $SUDO make altinstall || { echo_msg "Altinstall failed."; exit 1; }

    echo_msg "Python $REQUIRED_PYTHON_VERSION installed successfully."
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

# Check and install Python version
check_python_installation

# Create necessary directories with restrictive permissions
echo_msg "Creating directories..."
mkdir -p "$STORAGE_PATH"
set_permissions "$STORAGE_PATH" 700

mkdir -p "$LOG_DIR"
set_permissions "$LOG_DIR" 700

# Navigate to the project directory
cd "$PROJECT_DIR"

# Create or recreate virtual environment using Python 3.12 explicitly
if [ ! -d "$VENV_DIR" ]; then
    echo_msg "Creating Python virtual environment with $PYTHON_EXEC..."
    $PYTHON_EXEC -m venv "$VENV_DIR" || {
        echo_msg "Virtual environment creation failed. Please ensure Python installation is complete."
        exit 1
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
