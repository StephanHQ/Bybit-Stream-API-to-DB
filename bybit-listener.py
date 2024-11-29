# bybit_listener.py

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
from datetime import datetime
import signal
import sys
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import timezone

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

def handle_message(message, buffer):
    """Callback function to handle incoming messages."""
    # Extract symbol from data or topic
    if 'data' in message and 'symbol' in message['data']:
        symbol = message['data']['symbol']
    elif 'topic' in message and '.' in message['topic']:
        _, symbol = message['topic'].split('.', 1)
    else:
        symbol = 'Unknown'
    
    # Add 'symbol' as a top-level key in the message
    message['symbol'] = symbol
    
    # Append the modified message to the buffer
    buffer.append(message)
    
    # Log the buffered message at DEBUG level
    logger.debug(f"Buffered message for symbol {symbol}: {message}")

def save_buffer_to_parquet(buffer, storage_path):
    """Save buffered data to Parquet files, one for each symbol."""
    data = buffer.get_and_clear()
    if not data:
        logger.info("No data to save.")
        return

    # Convert buffer to DataFrame
    df = pd.DataFrame(data)

    if 'symbol' not in df.columns:
        logger.error("Data does not contain a 'symbol' column. Cannot save.")
        return

    # Ensure storage path exists
    os.makedirs(storage_path, exist_ok=True)

    # Save data for each symbol
    for symbol, symbol_df in df.groupby('symbol'):
        if symbol == 'Unknown':
            logger.warning("Encountered messages with unknown symbols. Skipping.")
            continue

        # Generate filename with current date
        current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        filename = f"{symbol}_orderbook_{current_date}.parquet"
        file_path = os.path.join(storage_path, filename)

        try:
            # Save the symbol-specific DataFrame to Parquet using fastparquet
            symbol_df.to_parquet(file_path, engine='fastparquet', compression='snappy', index=False)
            logger.info(f"Saved {len(symbol_df)} records for {symbol} to {file_path}")
        except Exception as e:
            logger.error(f"Failed to save Parquet file for {symbol}: {e}")

def trigger_saver(buffer, storage_path, trigger_file):
    """Thread function to monitor the trigger file and save data on-demand."""
    logger.info("Trigger saver thread started. Monitoring for save triggers.")
    while True:
        if os.path.exists(trigger_file):
            logger.info("Trigger detected. Saving buffered data to Parquet.")
            save_buffer_to_parquet(buffer, storage_path)
            # Remove the trigger file after processing
            try:
                os.remove(trigger_file)
                logger.info("Trigger file removed after saving.")
            except Exception as e:
                logger.error(f"Failed to remove trigger file: {e}")
        sleep(1)  # Check every second

def scheduled_save(buffer, storage_path):
    """Scheduled job to save buffer to Parquet."""
    logger.info("Scheduled save triggered. Saving buffered data to Parquet.")
    save_buffer_to_parquet(buffer, storage_path)

def signal_handler(sig, frame):
    """Handle termination signals for graceful shutdown."""
    logger.info("Termination signal received. Shutting down gracefully...")
    save_buffer_to_parquet(buffer, storage_path)
    sys.exit(0)

def main():
    global logger, buffer, storage_path

    # Load configurations
    config = load_config()
    symbols = load_symbols()

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
    saver_thread = threading.Thread(target=trigger_saver, args=(buffer, storage_path, trigger_file))
    saver_thread.daemon = True
    saver_thread.start()

    # Initialize APScheduler for daily save at UTC 00:00
    scheduler = BackgroundScheduler(timezone='UTC')
    scheduler.add_job(scheduled_save, CronTrigger(hour=0, minute=0), args=[buffer, storage_path])
    scheduler.start()
    logger.info("Scheduler for daily save at UTC 00:00 started.")

    # Group symbols by (testnet, channel_type) to manage multiple WebSocket connections if needed
    from collections import defaultdict
    symbol_groups = defaultdict(list)
    for symbol in symbols:
        key = (symbol.get('testnet', True), symbol.get('channel_type', 'linear'))
        symbol_groups[key].append(symbol)

    # Initialize WebSocket connections for each group
    websocket_threads = []

    for (testnet, channel_type), group_symbols in symbol_groups.items():
        def websocket_worker(symbols, testnet, channel_type):
            while True:
                try:
                    ws = WebSocket(
                        testnet=testnet,
                        channel_type=channel_type,
                    )
                    symbols_names = [s['name'] for s in symbols]
                    logger.info(f"Connected to Bybit WebSocket (Testnet: {testnet}, Channel Type: {channel_type}) for symbols: {[s['name'] for s in symbols]}")
    
                    # Subscribe to order book streams for the group symbols
                    for symbol in symbols:
                        name = symbol['name']
                        depth = symbol.get('depth', 50)
                        logger.info(f"Subscribing to order book for symbol: {name} with depth {depth}")
                        ws.orderbook_stream(depth, name, lambda msg: handle_message(msg, buffer))
    
                    logger.info(f"Subscribed to all order book streams for symbols: {[s['name'] for s in symbols]}")
    
                    # Keep the thread alive to continue receiving messages
                    while True:
                        sleep(1)
    
                except WebSocketConnectionClosedException as e:
                    logger.error(f"WebSocket connection closed: {e}. Reconnecting in 5 seconds...")
                    sleep(5)
                except Exception as e:
                    logger.error(f"Unexpected error: {e}. Reconnecting in 5 seconds...")
                    sleep(5)
    
        thread = threading.Thread(target=websocket_worker, args=(group_symbols, testnet, channel_type))
        thread.daemon = True
        thread.start()
        websocket_threads.append(thread)

    # Keep the main thread alive
    try:
        while True:
            sleep(10)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Shutting down gracefully...")
        save_buffer_to_parquet(buffer, storage_path)
        scheduler.shutdown()
        sys.exit(0)

if __name__ == "__main__":
    main()
