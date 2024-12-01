import asyncio
import json
import os
import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK, WebSocketException
from datetime import datetime, timezone
import gzip
import shutil

# Directories for storing CSV files and logs
CSV_FOLDER = "bybit_stream_data"
LOGS_FOLDER = "logs"
os.makedirs(CSV_FOLDER, exist_ok=True)
os.makedirs(LOGS_FOLDER, exist_ok=True)

LOG_FILE_BASE = os.path.join(LOGS_FOLDER, "logs")
LOG_FILE_SIZE_LIMIT = 10 * 1024 * 1024  # 10 MB

# Topics structured as JSON with channel_type
TOPICS = {
    "linear": {
        "BTCUSDT": [
            "orderbook.1",    # Level 1 orderbook
            "publicTrade",    # Public trades
            "kline.5",        # 5-minute Kline (candlestick) data
            "liquidation",    # Liquidation updates
            "tickers"         # Price ticker updates
        ],
        "ETHUSDT": [
            "orderbook.1",    # Level 1 orderbook
            "publicTrade"     # Public trades
        ]
    },
    "spot": {
        "BTCUSDT": [
            "tickers",        # Ticker updates for spot markets
            "publicTrade"     # Public trades for spot markets
        ]
    },
    "inverse": {
        "BTCUSD": [
            "orderbook.1",    # Level 1 orderbook
            "publicTrade",    # Public trades
            "kline.1",        # 1-minute Kline (candlestick) data
            "liquidation"     # Liquidation updates
        ]
    }
}

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

# Compress old CSV files to .csv.gz
def compress_old_csv_files(channel_type, utc_date):
    channel_folder = os.path.join(CSV_FOLDER, channel_type)
    for root, _, files in os.walk(channel_folder):
        for file in files:
            if file.endswith(".csv") and not file.startswith(utc_date):
                csv_path = os.path.join(root, file)
                gz_path = f"{csv_path}.gz"

                # Compress the file
                with open(csv_path, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
                
                # Remove the original CSV file
                os.remove(csv_path)

# Function to save messages to CSV in structured subdirectories
def save_message(channel_type, topic, message):
    # Get current UTC date
    utc_date = get_current_utc_date()

    # Compress old files if necessary
    compress_old_csv_files(channel_type, utc_date)

    # Create subdirectory for channel_type
    channel_folder = os.path.join(CSV_FOLDER, channel_type)
    os.makedirs(channel_folder, exist_ok=True)

    # Create subdirectory for topic within the channel_type folder
    topic_folder = os.path.join(channel_folder, topic.split(".")[0])  # Use only the base topic
    os.makedirs(topic_folder, exist_ok=True)

    # Create CSV file path with UTC date in the name
    csv_file = os.path.join(topic_folder, f"{utc_date}_{topic}.csv")

    # Save the message
    with open(csv_file, mode="a", newline="") as file:
        file.write(json.dumps(message) + "\n")

# Function to manage log rotation and save logs
def log_unknown_message(message):
    current_log_file = f"{LOG_FILE_BASE}.txt"

    # Check log file size and rotate if needed
    if os.path.exists(current_log_file) and os.path.getsize(current_log_file) >= LOG_FILE_SIZE_LIMIT:
        rotate_logs()

    # Write message to the current log file
    with open(current_log_file, mode="a", newline="") as log_file:
        log_file.write(json.dumps(message) + "\n")

# Function to rotate logs
def rotate_logs():
    for i in range(4, 0, -1):  # Keep 5 log files: logs.txt, logs.1.txt, ..., logs.4.txt
        old_log = f"{LOG_FILE_BASE}.{i}.txt"
        new_log = f"{LOG_FILE_BASE}.{i + 1}.txt"
        if os.path.exists(old_log):
            os.rename(old_log, new_log)

    # Rename the current log file to logs.1.txt
    current_log_file = f"{LOG_FILE_BASE}.txt"
    rotated_log_file = f"{LOG_FILE_BASE}.1.txt"
    if os.path.exists(current_log_file):
        os.rename(current_log_file, rotated_log_file)

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
                print(f"Subscription Response for {channel_type}: {response}")

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

                # Start heartbeat in a separate task
                heartbeat_task = asyncio.create_task(send_heartbeat())

                # Listen for messages
                async for message in websocket:
                    data = json.loads(message)
                    topic = data.get("topic", "unknown")

                    if topic == "unknown":  # Log unknown messages
                        log_unknown_message(data)
                    else:
                        save_message(channel_type, topic, data)

        except (ConnectionClosedError, ConnectionClosedOK) as e:
            print(f"Connection lost on {channel_type} channel: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)  # Reconnect after a delay
        except WebSocketException as e:
            print(f"WebSocket error on {channel_type} channel: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)  # Reconnect after a delay
        except Exception as e:
            print(f"Unexpected error on {channel_type} channel: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)  # Reconnect after a delay

# Run the WebSocket client for all channel types
async def main():
    tasks = [
        connect_and_listen("linear"),
        connect_and_listen("spot"),
        connect_and_listen("inverse"),
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("WebSocket client stopped.")
