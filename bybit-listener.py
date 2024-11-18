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
    def __init__(self, websocket_url, symbols, storage_path, timezone):
        self.websocket_url = websocket_url
        self.symbols = symbols
        self.storage_path = storage_path
        self.timezone = pytz.timezone(timezone)
        self.data = {}
        self.lock = threading.Lock()
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
        timezone=config['timezone']
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
    while True:
        time.sleep(10)

if __name__ == "__main__":
    main()
