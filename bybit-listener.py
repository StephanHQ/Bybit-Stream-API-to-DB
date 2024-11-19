# bybit_listener/bybit_listener.py

import websocket
import json
import pandas as pd
import threading
import schedule
import time
import yaml
import json
import os
from datetime import datetime
import pytz
import logging
import glob

# Load configuration
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

with open('symbols.json', 'r') as f:
    symbols_config = json.load(f)

# Setup logging
logging.basicConfig(
    filename=config['log_file'],
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class BybitListener:
    def __init__(self, websocket_url, symbols, storage_path, timezone, max_storage_gb=3):
        self.websocket_url = websocket_url
        self.symbols = symbols
        self.storage_path = storage_path
        self.timezone = pytz.timezone(timezone)
        self.data = {}
        self.lock = threading.Lock()
        self.max_storage_bytes = max_storage_gb * (1024 ** 3)  # Convert GB to bytes
        self.initialize_data_storage()

    def initialize_data_storage(self):
        for symbol in self.symbols:
            self.data[symbol['name']] = {}
            for category in symbol['categories']:
                self.data[symbol['name']][category] = []

    def on_message(self, ws, message):
        msg = json.loads(message)
        # Example structure handling; adjust based on actual Bybit message format
        if 'topic' in msg:
            topic = msg['topic']
            for symbol in self.symbols:
                if topic.startswith(symbol['name']):
                    category = topic.split('.')[1]
                    if category in symbol['categories']:
                        with self.lock:
                            self.data[symbol['name']][category].append(msg['data'])
        else:
            logging.warning("Received message without 'topic': %s", message)

    def on_error(self, ws, error):
        logging.error("WebSocket error: %s", error)

    def on_close(self, ws, close_status_code, close_msg):
        logging.info("WebSocket closed with code: %s, message: %s", close_status_code, close_msg)

    def on_open(self, ws):
        subscribe_args = []
        for symbol in self.symbols:
            for category in symbol['categories']:
                subscribe_args.append(f"{category}.{symbol['name']}")
        subscribe_message = {
            "op": "subscribe",
            "args": subscribe_args
        }
        ws.send(json.dumps(subscribe_message))
        logging.info("Subscribed to streams: %s", subscribe_args)

    def run(self):
        while True:
            try:
                ws = websocket.WebSocketApp(
                    self.websocket_url,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close
                )
                ws.run_forever()
            except Exception as e:
                logging.error("Exception in WebSocket connection: %s", e)
                time.sleep(5)  # Wait before reconnecting

    def save_data(self):
        with self.lock:
            current_date = datetime.now(self.timezone).strftime("%Y-%m-%d")
            for symbol, categories in self.data.items():
                for category, records in categories.items():
                    if records:
                        df = pd.DataFrame(records)
                        # Define directory for the symbol and category
                        dir_path = os.path.join(self.storage_path, symbol, category)
                        os.makedirs(dir_path, exist_ok=True)
                        file_path = os.path.join(dir_path, f"{current_date}.parquet")
                        df.to_parquet(file_path, engine='pyarrow', compression='snappy')
                        logging.info("Saved %d records to %s", len(records), file_path)
                        # Clear the data after saving
                        self.data[symbol][category] = []
            logging.info("Data saved for date: %s", current_date)
        
        # After saving, manage storage to ensure it doesn't exceed the limit
        self.manage_storage()

    def manage_storage(self):
        total_size = 0
        parquet_files = []

        # Traverse through all parquet files in storage_path
        for root, dirs, files in os.walk(self.storage_path):
            for file in files:
                if file.endswith('.parquet'):
                    file_path = os.path.join(root, file)
                    try:
                        file_size = os.path.getsize(file_path)
                        total_size += file_size
                        parquet_files.append((file_path, os.path.getmtime(file_path)))
                    except OSError as e:
                        logging.error("Error accessing file %s: %s", file_path, e)

        logging.info("Total parquet storage size: %.2f GB", total_size / (1024 ** 3))

        # If total size exceeds the maximum allowed, delete oldest files
        if total_size > self.max_storage_bytes:
            # Sort files by modification time (oldest first)
            parquet_files.sort(key=lambda x: x[1])
            for file_path, mod_time in parquet_files:
                try:
                    file_size = os.path.getsize(file_path)
                    os.remove(file_path)
                    total_size -= file_size
                    logging.info("Deleted old parquet file: %s to manage storage.", file_path)
                    if total_size <= self.max_storage_bytes:
                        break
                except OSError as e:
                    logging.error("Error deleting file %s: %s", file_path, e)

            logging.info("Storage management completed. Current size: %.2f GB", total_size / (1024 ** 3))

def schedule_saving(listener, save_time, timezone):
    def job():
        listener.save_data()
    
    schedule_time = save_time
    schedule.every().day.at(schedule_time).do(job)
    logging.info("Scheduled daily save at %s %s", schedule_time, timezone)

    while True:
        schedule.run_pending()
        time.sleep(1)

def main():
    listener = BybitListener(
        websocket_url=config['websocket_url'],
        symbols=symbols_config['symbols'],
        storage_path=config['parquet_storage_path'],
        timezone=config['timezone'],
        max_storage_gb=3  # Set maximum storage to 3 GB
    )

    # Start WebSocket listener in a separate thread
    ws_thread = threading.Thread(target=listener.run)
    ws_thread.daemon = True
    ws_thread.start()
    logging.info("WebSocket listener started.")

    # Schedule data saving
    scheduler_thread = threading.Thread(target=schedule_saving, args=(listener, config['save_time'], config['timezone']))
    scheduler_thread.daemon = True
    scheduler_thread.start()
    logging.info("Scheduler thread started.")

    # Keep the main thread alive
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        logging.info("Shutting down BybitListener.")

if __name__ == "__main__":
    main()
