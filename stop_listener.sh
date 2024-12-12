#!/bin/bash

# Function to print messages with timestamp
echo_msg() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

echo_msg "Stopping Flask web application and WebSocket Listener..."

if [ "$PRODUCTION" = "true" ]; then
    echo_msg "Production mode detected. Stopping WebSocket Listener if running."

    # Find and kill WebSocket listener processes
    LISTENER_PIDS=$(pgrep -f "bybit_websocket_listener.py")
    if [ -n "$LISTENER_PIDS" ]; then
        kill $LISTENER_PIDS
        echo_msg "WebSocket Listener processes (PIDs: $LISTENER_PIDS) stopped."
    else
        echo_msg "No WebSocket Listener processes found."
    fi
else
    echo_msg "Local mode detected. Stopping Gunicorn and WebSocket Listener."

    # Find and kill Gunicorn processes
    GUNICORN_PIDS=$(pgrep -f "gunicorn")
    if [ -n "$GUNICORN_PIDS" ]; then
        kill $GUNICORN_PIDS
        echo_msg "Gunicorn processes (PIDs: $GUNICORN_PIDS) stopped."
    else
        echo_msg "No Gunicorn processes found."
    fi

    # Find and kill WebSocket listener processes
    LISTENER_PIDS=$(pgrep -f "bybit_websocket_listener.py")
    if [ -n "$LISTENER_PIDS" ]; then
        kill $LISTENER_PIDS
        echo_msg "WebSocket Listener processes (PIDs: $LISTENER_PIDS) stopped."
    else
        echo_msg "No WebSocket Listener processes found."
    fi
fi

echo_msg "All services stopped successfully."
