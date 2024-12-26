# bybit_websocket_listener.py

import asyncio
import json
import os
import websockets
from websockets.exceptions import (
    ConnectionClosedError,
    ConnectionClosedOK,
    WebSocketException,
)
from datetime import datetime, timezone
import gzip
import shutil
import aiofiles
from asyncio import Queue, TaskGroup
from concurrent.futures import ThreadPoolExecutor
import logging
from logging.handlers import RotatingFileHandler

# === Configuration ===

# Load configuration from config.json
CONFIG_FILE = "config.json"
if not os.path.exists(CONFIG_FILE):
    raise FileNotFoundError(f"Configuration file '{CONFIG_FILE}' not found.")

with open(CONFIG_FILE, "r") as f:
    config = json.load(f)

CSV_FOLDER = config.get("CSV_FOLDER", "bybit_stream_data")
LOGS_FOLDER = config.get("LOGS_FOLDER", "logs")
LOG_FILE_NAME = config.get("LOG_FILE_NAME", "app.log")
LOG_FILE_SIZE_LIMIT = config.get("LOG_FILE_SIZE_LIMIT_MB", 10) * 1024 * 1024  # in bytes
LOG_FILE_BACKUP_COUNT = config.get("LOG_FILE_BACKUP_COUNT", 5)
CSV_FOLDER_SIZE_LIMIT = config.get("CSV_FOLDER_SIZE_LIMIT_GB", 15) * 1024 * 1024 * 1024  # in bytes
BUFFER_LIMIT = config.get("BUFFER_LIMIT", 100)
WRITE_INTERVAL = config.get("WRITE_INTERVAL_SEC", 1)  # seconds
PING_INTERVAL = config.get("PING_INTERVAL_SEC", 20)  # seconds
PING_TIMEOUT = config.get("PING_TIMEOUT_SEC", 10)  # seconds
WEBSOCKET_PING_INTERVAL = config.get("WEBSOCKET_PING_INTERVAL_SEC", 20)  # seconds
WEBSOCKET_PING_TIMEOUT = config.get("WEBSOCKET_PING_TIMEOUT_SEC", 10)  # seconds
WEBSOCKET_CLOSE_TIMEOUT = config.get("WEBSOCKET_CLOSE_TIMEOUT_SEC", 5)  # seconds
RECONNECT_INITIAL_BACKOFF = config.get("RECONNECT_INITIAL_BACKOFF_SEC", 1)  # seconds
RECONNECT_MAX_BACKOFF = config.get("RECONNECT_MAX_BACKOFF_SEC", 60)  # seconds
COMPRESSION_SLEEP_INTERVAL = config.get("COMPRESSION_SLEEP_INTERVAL_SEC", 86400)  # 24 hours
FOLDER_SIZE_CHECK_INTERVAL = config.get("FOLDER_SIZE_CHECK_INTERVAL_SEC", 600)  # 10 minutes
LOG_INTERVAL = config.get("LOG_INTERVAL", 100)  # Log every 100 messages

os.makedirs(CSV_FOLDER, exist_ok=True)
os.makedirs(LOGS_FOLDER, exist_ok=True)

LOG_FILE_PATH = os.path.join(LOGS_FOLDER, LOG_FILE_NAME)

# Setup logging with RotatingFileHandler
logger = logging.getLogger("BybitWebSocketListener")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(
    LOG_FILE_PATH,
    maxBytes=LOG_FILE_SIZE_LIMIT,
    backupCount=LOG_FILE_BACKUP_COUNT,
)
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)

# Initialize ThreadPoolExecutor for blocking I/O
executor = ThreadPoolExecutor()

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
    "inverse": "wss://stream.bybit.com/v5/public/inverse",
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

# Counter for throttled logging
log_counter = 0

# Function to save messages to CSV in structured subdirectories
async def save_message(channel_type, topic, message):
    key = (channel_type, topic)
    if key not in message_queues:
        message_queues[key] = Queue()
        asyncio.create_task(process_queue(channel_type, topic, message_queues[key]))
    await message_queues[key].put(message)

async def process_queue(channel_type, topic, queue):
    global log_counter
    last_utc_date = get_current_utc_date()  # Initialize with the current date
    symbol = topic.split(".")[-1]
    topic_base = ".".join(topic.split(".")[:-1])

    channel_folder = os.path.join(CSV_FOLDER, channel_type)
    symbol_folder = os.path.join(channel_folder, symbol)  # New layer for symbol
    topic_folder = os.path.join(symbol_folder, topic_base)  # Topic folder under symbol

    # Ensure the directories exist
    os.makedirs(topic_folder, exist_ok=True)

    buffer = []
    write_interval = WRITE_INTERVAL  # Seconds

    while True:
        try:
            current_utc_date = get_current_utc_date()

            # Check if the date has changed
            if current_utc_date != last_utc_date:
                # Write any remaining buffer to the old file
                if buffer:
                    old_csv_file = os.path.join(topic_folder, f"{last_utc_date}_{topic}.csv")
                    await write_buffer_to_file(old_csv_file, buffer)
                    log_counter += len(buffer)
                    if log_counter >= LOG_INTERVAL:
                        logger.info(f"Written {log_counter} messages to {old_csv_file}")
                        log_counter = 0
                    buffer.clear()

                # Update the date and reset buffer
                last_utc_date = current_utc_date
                logger.info(f"Date changed. Switching to new CSV file for {current_utc_date}.")

            # Define the CSV file path based on the current date
            csv_file = os.path.join(topic_folder, f"{current_utc_date}_{topic}.csv")

            try:
                message = await asyncio.wait_for(queue.get(), timeout=write_interval)
                buffer.append(json.dumps(message))
                if len(buffer) >= BUFFER_LIMIT:
                    await write_buffer_to_file(csv_file, buffer)
                    log_counter += len(buffer)
                    if log_counter >= LOG_INTERVAL:
                        logger.info(f"Written {log_counter} messages to {csv_file}")
                        log_counter = 0
                    buffer.clear()
            except asyncio.TimeoutError:
                if buffer:
                    await write_buffer_to_file(csv_file, buffer)
                    log_counter += len(buffer)
                    if log_counter >= LOG_INTERVAL:
                        logger.info(f"Written {log_counter} messages to {csv_file}")
                        log_counter = 0
                    buffer.clear()

        except Exception as e:
            logger.error(f"Error in processing queue for {channel_type}.{topic}: {e}")
            await asyncio.sleep(1)  # Prevent tight loop on error

async def write_buffer_to_file(csv_file, buffer):
    try:
        async with aiofiles.open(csv_file, mode="a") as file:
            await file.write("\n".join(buffer) + "\n")
    except Exception as e:
        logger.error(f"Failed to write buffer to {csv_file}: {e}")

# Function to compress old CSV files to .csv.gz
async def compress_old_csv_files_periodically():
    while True:
        try:
            await compress_old_csv_files()
            await asyncio.sleep(COMPRESSION_SLEEP_INTERVAL)
        except Exception as e:
            logger.error(f"Error in compression task: {e}")
            await asyncio.sleep(60)  # Retry after a minute on error

# Function to compress all old CSV files
async def compress_old_csv_files():
    utc_date = get_current_utc_date()
    csv_files = await get_all_csv_files(CSV_FOLDER)
    old_csv_files = [f for f in csv_files if not os.path.basename(f).startswith(utc_date)]

    if old_csv_files:
        logger.info(f"Compressing {len(old_csv_files)} old CSV files.")
        await asyncio.gather(*[compress_and_remove_file(f) for f in old_csv_files])
    else:
        logger.info("No old CSV files to compress.")

# Helper function to get all CSV files
async def get_all_csv_files(base_folder):
    files = []
    try:
        async for root, dirs, filenames in async_walk(base_folder):
            for filename in filenames:
                if filename.endswith(".csv"):
                    files.append(os.path.join(root, filename))
    except Exception as e:
        logger.error(f"Error scanning directory {base_folder}: {e}")
    return files

# Asynchronous directory walk using asyncio and aiofiles
async def async_walk(top):
    for root, dirs, files in os.walk(top):
        yield root, dirs, files

# Helper function to compress a single file and remove the original
async def compress_and_remove_file(csv_path):
    gz_path = f"{csv_path}.gz"
    try:
        await compress_file_async(csv_path, gz_path)
        os.remove(csv_path)
        logger.info(f"Compressed and removed: {csv_path}")
    except Exception as e:
        logger.error(f"Failed to compress {csv_path}: {e}")

# Asynchronously compress a file using aiofiles and gzip
async def compress_file_async(csv_path, gz_path):
    try:
        async with aiofiles.open(csv_path, "rb") as f_in, aiofiles.open(gz_path, "wb") as f_out:
            # Since gzip module is not asynchronous, use run_in_executor
            await asyncio.get_running_loop().run_in_executor(
                executor, compress_file, csv_path, gz_path
            )
    except Exception as e:
        logger.error(f"Error compressing {csv_path} to {gz_path}: {e}")

def compress_file(csv_path, gz_path):
    try:
        with open(csv_path, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    except Exception as e:
        logger.error(f"Error compressing {csv_path}: {e}")

# Function to compress all old CSV files at startup
async def compress_old_csv_files_on_startup():
    try:
        logger.info("Starting initial compression of old CSV files.")
        await compress_old_csv_files()
        logger.info("Initial compression of old CSV files completed.")
    except Exception as e:
        logger.error(f"Error during initial compression of old CSV files: {e}")

# Function to log unknown messages
async def log_unknown_message(message):
    try:
        logger.warning(f"Unknown message received: {json.dumps(message)}")
    except Exception as e:
        logger.error(f"Error logging unknown message: {e}")

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
                    logger.error(f"Error accessing {file_path}: {e}")
    return total_size

# Global variable to track folder size
current_folder_size = 0

# Function to initialize folder size
async def initialize_folder_size():
    global current_folder_size
    loop = asyncio.get_running_loop()
    current_folder_size = await loop.run_in_executor(
        executor, calculate_folder_size, CSV_FOLDER
    )
    logger.info(f"Initial folder size: {current_folder_size / (1024**3):.2f} GB")

# Function to delete oldest files until space limit is under the threshold
async def enforce_folder_size_limit():
    while True:
        try:
            global current_folder_size
            if current_folder_size > CSV_FOLDER_SIZE_LIMIT:
                logger.info(
                    f"Folder size {current_folder_size / (1024**3):.2f} GB exceeds limit of {CSV_FOLDER_SIZE_LIMIT / (1024**3):.2f} GB. Initiating cleanup..."
                )

                # Get all files with their modification times
                file_paths = await get_all_files_with_mtime(CSV_FOLDER)

                # Sort files by modification time (oldest first)
                file_paths.sort(key=lambda x: x[1])

                # Delete files until the size is under the limit
                for file_path, _ in file_paths:
                    try:
                        file_size = os.path.getsize(file_path)
                        await asyncio.get_running_loop().run_in_executor(
                            executor, os.remove, file_path
                        )
                        current_folder_size -= file_size
                        logger.info(
                            f"Deleted: {file_path}. Freed {file_size / (1024**2):.2f} MB. Remaining size: {current_folder_size / (1024**3):.2f} GB."
                        )
                        if current_folder_size <= CSV_FOLDER_SIZE_LIMIT:
                            logger.info("Folder size is now within the limit.")
                            break
                    except OSError as e:
                        logger.error(f"Error deleting {file_path}: {e}")

            # Wait before next check
            await asyncio.sleep(FOLDER_SIZE_CHECK_INTERVAL)
        except Exception as e:
            logger.error(f"Error in enforcing folder size limit: {e}")
            await asyncio.sleep(60)  # Retry after 1 minute on error

# Helper function to get all files with their modification times
async def get_all_files_with_mtime(base_folder):
    files = []
    try:
        async for root, dirs, filenames in async_walk(base_folder):
            for filename in filenames:
                file_path = os.path.join(root, filename)
                if os.path.isfile(file_path):
                    try:
                        mtime = os.path.getmtime(file_path)
                        files.append((file_path, mtime))
                    except OSError as e:
                        logger.error(f"Error accessing {file_path}: {e}")
    except Exception as e:
        logger.error(f"Error scanning directory {base_folder}: {e}")
    return files

# Function to manage WebSocket connections with exponential backoff
async def connect_and_listen(channel_type):
    url = CHANNEL_URLS[channel_type]
    subscription_args = generate_subscription_args(TOPICS, channel_type)
    backoff = RECONNECT_INITIAL_BACKOFF

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=WEBSOCKET_PING_INTERVAL,
                ping_timeout=WEBSOCKET_PING_TIMEOUT,
                close_timeout=WEBSOCKET_CLOSE_TIMEOUT,
            ) as websocket:
                logger.info(f"Connected to {channel_type} channel WebSocket.")
                backoff = RECONNECT_INITIAL_BACKOFF  # Reset backoff after successful connection

                # Subscribe to topics
                subscribe_message = {
                    "op": "subscribe",
                    "args": subscription_args,
                }
                await websocket.send(json.dumps(subscribe_message))
                logger.info(f"Subscribed to {channel_type} topics: {subscription_args}")

                # Receive subscription confirmation
                try:
                    response = await asyncio.wait_for(websocket.recv(), timeout=10)
                    logger.info(f"Subscription response for {channel_type}: {response}")
                except asyncio.TimeoutError:
                    logger.warning(f"No subscription confirmation received for {channel_type}.")

                # Start heartbeat
                heartbeat_task = asyncio.create_task(send_heartbeat(channel_type, websocket))

                # Listen for messages
                async for message in websocket:
                    try:
                        data = json.loads(message)
                        topic = data.get("topic", "unknown")

                        if topic == "unknown":
                            await log_unknown_message(data)
                        else:
                            await save_message(channel_type, topic, data)
                            # Update folder size based on message processing if needed
                            # This can be implemented as per specific requirements
                    except json.JSONDecodeError:
                        logger.warning(f"Received non-JSON message on {channel_type} channel: {message}")
                    except Exception as e:
                        logger.error(f"Error processing message on {channel_type} channel: {e}")

        except (ConnectionClosedError, ConnectionClosedOK, WebSocketException) as e:
            logger.warning(f"Connection lost on {channel_type} channel: {e}. Reconnecting in {backoff} seconds...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_MAX_BACKOFF)
        except Exception as e:
            logger.error(f"Unexpected error on {channel_type} channel: {e}. Reconnecting in {backoff} seconds...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_MAX_BACKOFF)

async def send_heartbeat(channel_type, websocket):
    try:
        while True:
            await asyncio.sleep(PING_INTERVAL)
            await websocket.send(json.dumps({"op": "ping"}))
            logger.info(f"Sent heartbeat for {channel_type}.")
    except (WebSocketException, asyncio.CancelledError) as e:
        logger.warning(f"Heartbeat failed for {channel_type}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in heartbeat for {channel_type}: {e}")

# === Main Function ===

async def main():
    # Initialize folder size
    await initialize_folder_size()

    # Compress old CSV files at startup
    await compress_old_csv_files_on_startup()

    # Start compression task and folder size enforcement task using TaskGroup
    async with TaskGroup() as tg:
        tg.create_task(compress_old_csv_files_periodically())
        tg.create_task(enforce_folder_size_limit())
        tg.create_task(connect_and_listen("linear"))
        tg.create_task(connect_and_listen("spot"))
        tg.create_task(connect_and_listen("inverse"))
        # Uncomment the following line if you have 'options' channel_type
        # tg.create_task(connect_and_listen("options"))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("WebSocket listener stopped by user.")
    except Exception as e:
        logger.error(f"Application terminated with error: {e}")
