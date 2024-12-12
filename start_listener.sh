#!/bin/bash

# Variables
SCRIPT="bybit_websocket_listener.py"  # WebSocket listener script
FLASK_APP_MODULE="bybit_flask:app"    # Gunicorn requires module:app
VENV_DIR="$(pwd)/env"                 # Path to the virtual environment
PROJECT_DIR="$(pwd)"                  # Path to the project directory
LISTENER_LOG="./logs/listener.log"
GUNICORN_LOG="./logs/gunicorn.log"

# Function to print messages with timestamp
echo_msg() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

if [ "$PRODUCTION" = "true" ]; then
    echo_msg "Production mode detected. Starting Flask app with Passenger (cPanel managed)."
    # In production, Passenger handles the Flask app. Ensure cPanel is configured correctly.
    # Start the WebSocket Listener if required and allowed by cPanel.
    # Note: cPanel might have restrictions on running persistent background processes.
    
    # Example: Starting WebSocket Listener via a background process
    # Adjust as per cPanel's capabilities and policies.
    echo_msg "Starting Bybit WebSocket Listener..."
    nohup python "$PROJECT_DIR/$SCRIPT" > "$LISTENER_LOG" 2>&1 &
    LISTENER_PID=$!
    echo_msg "WebSocket Listener started with PID $LISTENER_PID."
else
    echo_msg "Local mode detected. Starting Flask web application with Gunicorn."

    # Activate virtual environment
    source "$VENV_DIR/bin/activate"

    # Start Gunicorn in the background
    nohup gunicorn -w 4 -b 127.0.0.1:8000 "$FLASK_APP_MODULE" > "$GUNICORN_LOG" 2>&1 &
    GUNICORN_PID=$!
    echo_msg "Gunicorn started with PID $GUNICORN_PID."

    echo_msg "Starting Bybit WebSocket Listener..."

    # Start the WebSocket Listener in the background
    nohup python "$PROJECT_DIR/$SCRIPT" > "$LISTENER_LOG" 2>&1 &
    LISTENER_PID=$!
    echo_msg "WebSocket Listener started with PID $LISTENER_PID."

    echo_msg "All services started successfully."
fi
