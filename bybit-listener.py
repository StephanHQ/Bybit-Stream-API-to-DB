# === bybit-listener.py ===
import asyncio
import json
import os
from os import sep as os_sep
import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK, WebSocketException
from datetime import datetime, timezone
import gzip
import shutil
import aiofiles
from asyncio import Queue
from flask import Flask, render_template, send_from_directory, abort, request, Response
from threading import Thread
from urllib.parse import unquote
from functools import wraps
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from dotenv import load_dotenv  # Optional for environment variables

# Load environment variables from .env file (Optional but recommended)
load_dotenv()

# === Configuration ===

# Toggle authentication
USE_AUTH = False  # Set to True to enable authentication

# Directories for storing CSV files and logs
CSV_FOLDER = "bybit_stream_data"
LOGS_FOLDER = "logs"
os.makedirs(CSV_FOLDER, exist_ok=True)
os.makedirs(LOGS_FOLDER, exist_ok=True)

LOG_FILE_BASE = os.path.join(LOGS_FOLDER, "logs")
LOG_FILE_SIZE_LIMIT = 10 * 1024 * 1024  # 10 MB

# Space limit for the CSV folder (15GB in bytes)
CSV_FOLDER_SIZE_LIMIT = 15 * 1024 * 1024 * 1024  # 15 GB

# Load topics from symbols.json
SYMBOLS_FILE = "symbols.json"
if not os.path.exists(SYMBOLS_FILE):
    raise FileNotFoundError(f"Symbols file '{SYMBOLS_FILE}' not found.")

with open(SYMBOLS_FILE, "r") as f:
    TOPICS = json.load(f)

# WebSocket URLs for each channel_type
CHANNEL_URLS = {
    "linear": "wss://stream.bybit.com/v5/public/linear",
    "spot": "wss://stream.bybit.com/v5/public/spot",
    "options": "wss://stream.bybit.com/v5/public/option",
    "inverse": "wss://stream.bybit.com/v5/public/inverse"
}

# Flatten topics for a specific channel type
def generate_subscription_args(topics_json, channel_type):
    args = []
    if channel_type in topics_json:
        for symbol, topic_list in topics_json[channel_type].items():
            for topic in topic_list:
                args.append(f"{topic}.{symbol}")
    return args

# Function to get the current UTC date in YYYY-MM-DD format
def get_current_utc_date():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

# Dictionary to hold message queues
message_queues = {}

# Function to save messages to CSV in structured subdirectories
async def save_message(channel_type, topic, message):
    key = (channel_type, topic)
    if key not in message_queues:
        message_queues[key] = Queue()
        asyncio.create_task(process_queue(channel_type, topic, message_queues[key]))
    await message_queues[key].put(message)

async def process_queue(channel_type, topic, queue):
    while True:
        try:
            utc_date = get_current_utc_date()

            # Extract the symbol from the topic
            # Example: "orderbook.1.BTCUSDT" -> "BTCUSDT"
            symbol = topic.split(".")[-1]
            topic_base = ".".join(topic.split(".")[:-1])

            channel_folder = os.path.join(CSV_FOLDER, channel_type)
            symbol_folder = os.path.join(channel_folder, symbol)  # New layer for symbol
            topic_folder = os.path.join(symbol_folder, topic_base)  # Topic folder under symbol

            # Ensure the directories exist
            os.makedirs(topic_folder, exist_ok=True)

            # Define the CSV file path
            csv_file = os.path.join(topic_folder, f"{utc_date}_{topic}.csv")

            buffer = []
            BUFFER_LIMIT = 100  # Adjust as needed
            WRITE_INTERVAL = 1  # Seconds

            while True:
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=WRITE_INTERVAL)
                    buffer.append(json.dumps(message))
                    if len(buffer) >= BUFFER_LIMIT:
                        async with aiofiles.open(csv_file, mode="a") as file:
                            await file.write("\n".join(buffer) + "\n")
                        buffer.clear()
                except asyncio.TimeoutError:
                    if buffer:
                        async with aiofiles.open(csv_file, mode="a") as file:
                            await file.write("\n".join(buffer) + "\n")
                        buffer.clear()
        except Exception as e:
            print(f"Error in processing queue for {channel_type}.{topic}: {e}")
            await asyncio.sleep(1)  # Prevent tight loop on error

# Function to compress old CSV files to .csv.gz
async def compress_old_csv_files_periodically():
    while True:
        try:
            utc_date = get_current_utc_date()
            for channel_type in TOPICS.keys():
                channel_folder = os.path.join(CSV_FOLDER, channel_type)
                for root, _, files in os.walk(channel_folder):
                    for file in files:
                        if file.endswith(".csv") and not file.startswith(utc_date):
                            csv_path = os.path.join(root, file)
                            gz_path = f"{csv_path}.gz"

                            # Compress the file asynchronously
                            await asyncio.to_thread(compress_file, csv_path, gz_path)

                            # Remove the original CSV file
                            os.remove(csv_path)
                            print(f"Compressed and removed: {csv_path}")

            # Run compression once a day
            await asyncio.sleep(86400)  # 24 hours
        except Exception as e:
            print(f"Error in compression task: {e}")
            await asyncio.sleep(60)  # Retry after a minute on error

def compress_file(csv_path, gz_path):
    try:
        with open(csv_path, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    except Exception as e:
        print(f"Failed to compress {csv_path}: {e}")

# Function to manage log rotation and save logs
async def log_unknown_message(message):
    current_log_file = f"{LOG_FILE_BASE}.logs"  # Changed from .txt to .logs

    try:
        # Check log file size asynchronously
        if os.path.exists(current_log_file):
            size = await asyncio.to_thread(os.path.getsize, current_log_file)
            if size >= LOG_FILE_SIZE_LIMIT:
                await rotate_logs()

        # Write message to the current log file asynchronously
        async with aiofiles.open(current_log_file, mode="a") as log_file:
            await log_file.write(json.dumps(message) + "\n")
    except Exception as e:
        print(f"Error logging unknown message: {e}")

# Function to rotate logs
async def rotate_logs():
    try:
        for i in range(4, 0, -1):  # Keep 5 log files: logs.logs, logs.1.logs, ..., logs.4.logs
            old_log = f"{LOG_FILE_BASE}.{i}.logs"  # Changed from .txt to .logs
            new_log = f"{LOG_FILE_BASE}.{i + 1}.logs"  # Changed from .txt to .logs
            if os.path.exists(old_log):
                await asyncio.to_thread(os.rename, old_log, new_log)

        # Rename the current log file to logs.1.logs
        current_log_file = f"{LOG_FILE_BASE}.logs"  # Changed from .txt to .logs
        rotated_log_file = f"{LOG_FILE_BASE}.1.logs"  # Changed from .txt to .logs
        if os.path.exists(current_log_file):
            await asyncio.to_thread(os.rename, current_log_file, rotated_log_file)
        print("Log rotation completed.")
    except Exception as e:
        print(f"Error during log rotation: {e}")

# Function to calculate folder size
def calculate_folder_size(folder_path):
    total_size = 0
    for dirpath, _, filenames in os.walk(folder_path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            if os.path.isfile(file_path):
                try:
                    total_size += os.path.getsize(file_path)
                except OSError as e:
                    print(f"Error accessing {file_path}: {e}")
    return total_size

# Function to delete oldest files until space limit is under the threshold
async def enforce_folder_size_limit():
    while True:
        try:
            folder_size = calculate_folder_size(CSV_FOLDER)
            if folder_size > CSV_FOLDER_SIZE_LIMIT:
                print(f"Folder size {folder_size / (1024**3):.2f} GB exceeds limit of 15 GB. Initiating cleanup...")

                # Get all files in the folder with their modification times
                file_paths = []
                for dirpath, _, filenames in os.walk(CSV_FOLDER):
                    for filename in filenames:
                        file_path = os.path.join(dirpath, filename)
                        if os.path.isfile(file_path):
                            try:
                                mtime = os.path.getmtime(file_path)
                                file_paths.append((file_path, mtime))
                            except OSError as e:
                                print(f"Error accessing {file_path}: {e}")

                # Sort files by modification time (oldest first)
                file_paths.sort(key=lambda x: x[1])

                # Delete files until the size is under the limit
                for file_path, _ in file_paths:
                    try:
                        file_size = os.path.getsize(file_path)
                        os.remove(file_path)
                        folder_size -= file_size
                        print(f"Deleted: {file_path}. Freed {file_size / (1024**2):.2f} MB. Remaining size: {folder_size / (1024**3):.2f} GB.")
                        if folder_size <= CSV_FOLDER_SIZE_LIMIT:
                            print("Folder size is now within the limit.")
                            break
                    except OSError as e:
                        print(f"Error deleting {file_path}: {e}")

            # Check again in 10 minutes
            await asyncio.sleep(600)  # 10 minutes
        except Exception as e:
            print(f"Error in enforcing folder size limit: {e}")
            await asyncio.sleep(60)  # Retry after 1 minute on error

# WebSocket connection handler for a specific channel type
async def connect_and_listen(channel_type):
    url = CHANNEL_URLS[channel_type]
    subscription_args = generate_subscription_args(TOPICS, channel_type)

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=20,  # Send pings every 20 seconds
                ping_timeout=10,   # Wait up to 10 seconds for a pong
                close_timeout=5     # Close timeout
            ) as websocket:
                print(f"Connected to {channel_type} channel WebSocket.")

                # Subscribe to topics
                await websocket.send(json.dumps({
                    "op": "subscribe",
                    "args": subscription_args,
                }))

                # Receive confirmation
                response = await websocket.recv()
                # Suppress the subscription response message by commenting out the print
                # print(f"Subscription Response for {channel_type}: {response}")

                # Keep the connection alive
                async def send_heartbeat():
                    while True:
                        try:
                            await asyncio.sleep(20)
                            await websocket.send(json.dumps({"op": "ping"}))
                            print(f"Sent heartbeat for {channel_type}.")
                        except WebSocketException as e:
                            print(f"Heartbeat failed for {channel_type}: {e}")
                            break  # Exit the loop if sending fails
                        except Exception as e:
                            print(f"Unexpected error in heartbeat for {channel_type}: {e}")
                            break

                # Start heartbeat in a separate task
                heartbeat_task = asyncio.create_task(send_heartbeat())

                # Listen for messages
                async for message in websocket:
                    try:
                        data = json.loads(message)
                        topic = data.get("topic", "unknown")

                        if topic == "unknown":
                            await log_unknown_message(data)
                        else:
                            await save_message(channel_type, topic, data)
                    except json.JSONDecodeError:
                        print(f"Received non-JSON message on {channel_type} channel: {message}")
                    except Exception as e:
                        print(f"Error processing message on {channel_type} channel: {e}")

        except (ConnectionClosedError, ConnectionClosedOK) as e:
            print(f"Connection lost on {channel_type} channel: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)  # Reconnect after a delay
        except WebSocketException as e:
            print(f"WebSocket error on {channel_type} channel: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)  # Reconnect after a delay
        except Exception as e:
            print(f"Unexpected error on {channel_type} channel: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)  # Reconnect after a delay

# Initialize Flask app
app = Flask(__name__)

# Initialize Flask-Limiter
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per day", "50 per hour"]
)

# === Authentication Setup ===

# Define your credentials for Basic Authentication (Used only if USE_AUTH is True)
USERNAME = os.getenv('USERNAME', 'admin')        # Change to your desired username
PASSWORD = os.getenv('PASSWORD', 'password')    # Change to your desired password

def check_auth(username, password):
    """Check if a username/password combination is valid."""
    return username == USERNAME and password == PASSWORD

def authenticate():
    """Sends a 401 response that enables basic auth"""
    return Response(
        'Could not verify your access level for that URL.\n'
        'You have to login with proper credentials', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    """Decorator to handle authentication logic."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if USE_AUTH:
            auth = request.authorization
            if not auth or not check_auth(auth.username, auth.password):
                return authenticate()
        return f(*args, **kwargs)
    return decorated

# Security: Prevent directory traversal by ensuring all paths are within CSV_FOLDER
def secure_path(path):
    # Join the CSV_FOLDER with the requested path
    base_path = os.path.abspath(CSV_FOLDER)
    safe_path = os.path.abspath(os.path.join(base_path, path))
    # Ensure the resulting path is within CSV_FOLDER
    if not safe_path.startswith(base_path):
        abort(403)  # Forbidden
    return safe_path

# === Error Handlers ===

@app.errorhandler(404)
def not_found(error):
    return render_template('error.html', message="Resource Not Found.", description="The requested resource could not be found."), 404

@app.errorhandler(403)
def forbidden(error):
    return render_template('error.html', message="Forbidden", description="You don't have permission to access this resource."), 403

@app.errorhandler(401)
def unauthorized(error):
    return render_template('error.html', message="Unauthorized", description="Please provide valid credentials to access this resource."), 401

@app.errorhandler(429)
def ratelimit_handler(error):
    return render_template('error.html', message="Rate Limit Exceeded", description="You have made too many requests. Please try again later."), 429

@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html', message="Internal Server Error", description="An unexpected error has occurred. Please try again later."), 500

# === Routes ===

# Home route - lists the top-level directories
@app.route('/')
@requires_auth
@limiter.limit("10 per minute")
def index():
    return list_directory('')

# Dynamic route to list directories and files
@app.route('/browse/<path:subpath>')
@requires_auth
@limiter.limit("10 per minute")
def list_directory(subpath):
    # Decode the URL-encoded path
    subpath = unquote(subpath)
    safe_path = secure_path(subpath)

    if not os.path.exists(safe_path):
        abort(404)

    if not os.path.isdir(safe_path):
        abort(404)

    # List directories and files
    items = os.listdir(safe_path)
    dirs = []
    files = []
    for item in sorted(items):
        item_path = os.path.join(safe_path, item)
        if os.path.isdir(item_path):
            dirs.append(item)
        elif os.path.isfile(item_path) and (item.endswith('.csv') or item.endswith('.csv.gz')):
            file_stat = os.stat(item_path)
            files.append({
                'name': item,
                'size': f"{file_stat.st_size / (1024**2):.2f} MB",
                'modified': datetime.fromtimestamp(file_stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
            })

    # Calculate the relative path for navigation
    relative_path = os.path.relpath(safe_path, os.path.abspath(CSV_FOLDER))
    if relative_path == '.':
        relative_path = ''

    return render_template(
        'directory.html',
        dirs=dirs,
        files=files,
        current_path=relative_path,
        path_separator=os_sep  # Pass the path separator to the template
    )

# Route to download files
@app.route('/download/<path:filepath>')
@requires_auth
@limiter.limit("10 per minute")
def download_file(filepath):
    # Decode the URL-encoded path
    filepath = unquote(filepath)
    safe_path = secure_path(filepath)

    if not os.path.exists(safe_path):
        abort(404)

    if not os.path.isfile(safe_path):
        abort(404)

    # Extract directory and filename
    directory = os.path.dirname(safe_path)
    filename = os.path.basename(safe_path)

    return send_from_directory(directory, filename, as_attachment=True)

# === Flask Application Runner ===

# Function to run Flask app in a separate thread
def run_flask():
    # For production, consider using a production-ready server like Gunicorn
    app.run(host='127.0.0.1', port=8000, debug=False)  # Set debug=True for development

# Run the Flask server in a separate thread alongside the WebSocket listener
async def main():
    # Start compression task
    compression_task = asyncio.create_task(compress_old_csv_files_periodically())

    # Start folder size management task
    size_limit_task = asyncio.create_task(enforce_folder_size_limit())

    # Start WebSocket listeners
    websocket_tasks = [
        connect_and_listen("linear"),
        connect_and_listen("spot"),
        connect_and_listen("inverse"),
    ]

    # Start Flask in a separate thread
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()

    # Gather all tasks including compression and size limit enforcement
    await asyncio.gather(*websocket_tasks, compression_task, size_limit_task)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("WebSocket client stopped.")
