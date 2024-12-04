# bybit_websocket_listener.py

import asyncio
import json
import os
import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK, WebSocketException
from datetime import datetime, timezone
import gzip
import shutil
import aiofiles
from asyncio import Queue

# === Configuration ===

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

# === Functions ===

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
            old_log = f"{LOG_FILE_BASE}.{i}.logs"
            new_log = f"{LOG_FILE_BASE}.{i + 1}.logs"
            if os.path.exists(old_log):
                await asyncio.to_thread(os.rename, old_log, new_log)

        # Rename the current log file to logs.1.logs
        current_log_file = f"{LOG_FILE_BASE}.logs"
        rotated_log_file = f"{LOG_FILE_BASE}.1.logs"
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
                close_timeout=5    # Close timeout
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

# === Main Function ===

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

    # Gather all tasks including compression and size limit enforcement
    await asyncio.gather(*websocket_tasks, compression_task, size_limit_task)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("WebSocket listener stopped.")
