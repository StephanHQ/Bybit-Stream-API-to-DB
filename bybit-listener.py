import json
import yaml
import logging
from logging.handlers import RotatingFileHandler
import os
import threading
from pybit.unified_trading import WebSocket
from websocket import WebSocketConnectionClosedException
from time import sleep
import pandas as pd
from datetime import datetime, timezone
import signal
import sys
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# Global logger
logger = None

def load_config(config_path='config.yaml'):
    """Load YAML configuration file."""
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def load_symbols(symbols_path='symbols.json'):
    """Load JSON symbols file."""
    with open(symbols_path, 'r') as file:
        return json.load(file)['symbols']

def setup_logging(log_file):
    """Set up logging with rotation and console output."""
    log_dir = os.path.dirname(log_file)
    os.makedirs(log_dir, exist_ok=True)

    # Rotating File Handler
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=5,
        encoding='utf-8'
    )
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Set to INFO to suppress DEBUG logs in console
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)

    # Logger Configuration
    logger = logging.getLogger("BybitListener")
    logger.setLevel(logging.DEBUG)  # Set logger to DEBUG level to capture all logs
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

class OrderBookBuffer:
    """Thread-safe buffer for storing incoming order book messages."""
    def __init__(self):
        self.lock = threading.Lock()
        self.buffer = []

    def append(self, message):
        with self.lock:
            self.buffer.append(message)

    def get_and_clear(self):
        with self.lock:
            data = self.buffer.copy()
            self.buffer.clear()
        return data

def handle_message(message, buffer, symbols_map):
    """Callback function to handle incoming messages."""
    # Log the raw message
    logger.debug(f"Raw message received: {message}")

    # Extract symbol from the message
    if 'data' in message and 'symbol' in message['data']:
        symbol = message['data']['symbol']
    elif 'topic' in message and '.' in message['topic']:
        _, symbol = message['topic'].split('.', 1)
    else:
        symbol = 'Unknown'

    logger.debug(f"Extracted symbol before normalization: {symbol}")

    # Remove '50.' prefix if present
    if symbol.startswith("50."):
        symbol = symbol[3:]
        logger.debug(f"Symbol after stripping prefix: {symbol}")

    # Get channel_type and depth from symbols_map
    channel_type = symbols_map.get(symbol, {}).get('channel_type', 'Unknown')
    depth = symbols_map.get(symbol, {}).get('depth', 'Unknown')

    logger.debug(f"Channel type: {channel_type}, Depth: {depth}")

    # Add `symbol`, `channel_type`, and `depth` to the message
    message['symbol'] = symbol
    message['channel_type'] = channel_type
    message['depth'] = depth

    # Append the modified message to the buffer
    buffer.append(message)

    # Log the buffered message at DEBUG level
    logger.debug(f"Buffered message for symbol {symbol} (channel_type: {channel_type}, depth: {depth}): {message}")

def save_buffer_to_parquet(buffer, storage_path, symbols_map):
    """Save buffered data to Parquet files with an enhanced folder structure."""
    data = buffer.get_and_clear()
    if not data:
        logger.info("No data to save.")
        return

    # Convert buffer to DataFrame
    df = pd.DataFrame(data)

    if 'symbol' not in df.columns or 'channel_type' not in df.columns or 'depth' not in df.columns:
        logger.error("Data does not contain required columns ('symbol', 'channel_type', 'depth'). Cannot save.")
        return

    # Ensure storage path exists
    os.makedirs(storage_path, exist_ok=True)

    # Save data for each combination of symbol and channel_type
    for (symbol, channel_type, depth), group_df in df.groupby(['symbol', 'channel_type', 'depth']):
        if symbol == 'Unknown' or channel_type == 'Unknown':
            logger.warning("Encountered messages with unknown symbols or channel types. Skipping.")
            continue

        # Create nested folder structure
        folder_path = os.path.join(storage_path, symbol, channel_type)
        os.makedirs(folder_path, exist_ok=True)

        # Generate filename with additional parameters
        current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        filename = f"{symbol}_{channel_type}_{current_date}_orderbook_depth-{depth}.parquet"
        file_path = os.path.join(folder_path, filename)

        try:
            # Check if file exists
            if os.path.exists(file_path):
                # Read existing data
                existing_df = pd.read_parquet(file_path, engine='fastparquet')
                # Concatenate new data
                combined_df = pd.concat([existing_df, group_df], ignore_index=True)
                # Save back to Parquet
                combined_df.to_parquet(file_path, engine='fastparquet', compression='snappy')
                logger.info(f"Appended {len(group_df)} records for {symbol}/{channel_type} to {file_path}")
            else:
                # Save the symbol-specific DataFrame to Parquet
                group_df.to_parquet(file_path, engine='fastparquet', compression='snappy')
                logger.info(f"Saved {len(group_df)} records for {symbol}/{channel_type} to {file_path}")
        except Exception as e:
            logger.error(f"Failed to save Parquet file for {symbol}/{channel_type}: {e}")

def trigger_saver(buffer, storage_path, trigger_file, symbols_map):
    """Thread function to monitor the trigger file and save data on-demand."""
    logger.info("Trigger saver thread started. Monitoring for save triggers.")
    while True:
        if os.path.exists(trigger_file):
            logger.info("Trigger detected. Saving buffered data to Parquet.")
            save_buffer_to_parquet(buffer, storage_path, symbols_map)
            # Remove the trigger file after processing
            try:
                os.remove(trigger_file)
                logger.info("Trigger file removed after saving.")
            except Exception as e:
                logger.error(f"Failed to remove trigger file: {e}")
        sleep(1)  # Check every second

def scheduled_save(buffer, storage_path, symbols_map):
    """Scheduled job to save buffer to Parquet."""
    logger.info("Scheduled save triggered. Saving buffered data to Parquet.")
    save_buffer_to_parquet(buffer, storage_path, symbols_map)

def signal_handler(sig, frame):
    """Handle termination signals for graceful shutdown."""
    logger.info("Termination signal received. Shutting down gracefully...")
    save_buffer_to_parquet(buffer, storage_path, symbols_map)
    scheduler.shutdown()
    sys.exit(0)

def websocket_worker(symbol, buffer, symbols_map):
    """WebSocket worker to handle WebSocket connections."""
    while True:
        try:
            ws = WebSocket(testnet=symbol.get('testnet', True), channel_type=symbol['channel_type'])
            ws.orderbook_stream(symbol['depth'], symbol['name'], lambda msg: handle_message(msg, buffer, symbols_map))
            logger.info(f"Subscribed to {symbol['name']} (channel_type: {symbol['channel_type']})")
            while True:
                sleep(1)
        except WebSocketConnectionClosedException as e:
            logger.error(f"WebSocket connection closed: {e}. Reconnecting in 5 seconds...")
            sleep(5)
        except Exception as e:
            logger.error(f"Unexpected error: {e}. Reconnecting in 5 seconds...")
            sleep(5)

def main():
    global logger, buffer, storage_path, symbols_map, scheduler

    # Load configurations
    config = load_config()
    symbols = load_symbols()

    # Map symbols to their configurations
    symbols_map = {s['name']: s for s in symbols}

    # Set up logging
    logger = setup_logging(config.get('log_file', './logs/listener.log'))
    logger.info("Starting Bybit Listener...")

    # Initialize buffer
    buffer = OrderBookBuffer()

    # Data storage path
    storage_path = config.get('parquet_storage_path', './parquet_data/')

    # Trigger file path
    trigger_file = config.get('trigger_file', './save_now.trigger')

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start trigger saver thread
    saver_thread = threading.Thread(target=trigger_saver, args=(buffer, storage_path, trigger_file, symbols_map))
    saver_thread.daemon = True
    saver_thread.start()

    # Initialize APScheduler for daily save at UTC 00:00
    scheduler = BackgroundScheduler(timezone='UTC')
    scheduler.add_job(
        scheduled_save,
        CronTrigger(hour=0, minute=0),
        args=[buffer, storage_path, symbols_map]
    )
    scheduler.start()
    logger.info("Scheduler for daily save at UTC 00:00 started.")

    # Initialize WebSocket connections
    websocket_threads = []

    for symbol in symbols:
        thread = threading.Thread(
            target=websocket_worker,
            args=(symbol, buffer, symbols_map)
        )
        thread.daemon = True
        thread.start()
        websocket_threads.append(thread)

    # Keep the main thread alive
    try:
        while True:
            sleep(10)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Shutting down gracefully...")
        save_buffer_to_parquet(buffer, storage_path, symbols_map)
        scheduler.shutdown()
        sys.exit(0)

if __name__ == "__main__":
    main()
