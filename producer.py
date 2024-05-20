from pyspark.sql import SparkSession
from kafka import KafkaProducer
import json
import time
import websocket
import random
import logging

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaProducerApp") \
    .getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
    logging.FileHandler("/home/hadoop/new_dir/logfile.log"),
    logging.StreamHandler()
])

# Kafka broker address
bootstrap_servers = ['ip-10-0-31-206.ec2.internal:9092']

# Kafka topic for the symbol
topic = 'symbol_topic'
topic2 = 'symbol_topic2'

# Creating Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Websocket URL
socket_url = "wss://stream.data.alpaca.markets/v2/iex"

# Function to send message to Kafka
def send_to_kafka(message, topic):
    producer.send(topic, value=message)
    logging.info(f"Produced message: {message}")

# Websocket on_message callback function
def on_message(ws, message):
        logging.info(f"Received message: {message}")
    # for message in messages:
        
        data = json.loads(message)
        
        if isinstance(data, list) and len(data) > 0:
            for item in data:
                message_type = item.get("T")
                
                if message_type == "q":  # Quote message
                    quote_data = {
                        'message_type': "q",
                        'symbol': item.get("S"),
                        'ask_exchange_code': item.get("ax"),
                        'ask_price': item.get("ap"),
                        'ask_size': item.get("as"),
                        'bid_exchange_code': item.get("bx"),
                        'bid_price': item.get("bp"),
                        'bid_size': item.get("bs"),
                        'timestamp': item.get("t"),
                        'tape': item.get("z"),
                        'random': random.randint(0, 100)
                    }
                    send_to_kafka(quote_data, topic)
                    logging.info(f"Sent to Kafka quote data1: {quote_data}")
                
                elif message_type == "b":  # Bar message
                    bar_data = {
                        'message_type': "b",
                        'symbol': item.get("S"),
                        'open_price': item.get("o"),
                        'high_price': item.get("h"),
                        'low_price': item.get("l"),
                        'close_price': item.get("c"),
                        'volume': item.get("v"),
                        'timestamp': item.get("t")
                    }
                    send_to_kafka(bar_data, topic2)
                    logging.info(f"Sent to Kafka bar data2: {bar_data}")
                
                else:
                    # Handle metadata or other message types
                    logging.info(f"Received metadata or unknown message type: {data}")
            # else:
            #     # Handle cases where data is not a list or is empty
            #     logging.warning(f"Unexpected message format: {data}")


    
# Websocket on_open callback function
def on_open(ws):
    logging.info("Opened connection")
    auth_data = {
        "action": "auth",
        "key": "PK4GRC339YQJ0OQKTCLO",
        "secret": "QSGanmcvp1ezlHA41gIGL6G4v7ys8SuCIKf0Gd3P"
    }
    ws.send(json.dumps(auth_data))
    listen_message = {
        "action": "subscribe",
          
        # "trades": ["AAPL"],
        #"quotes": ["AAPL"],
        #"quotes":["FAKEPACA"],
        "bars": ["AAPL", "SPY", "DIA","MSFT","TSLA","META","GOOG"]
        # "bars":["AAPL"],
        # "bars":["MSFT"]
        #"bars": ["FAKEPACA"]
    }
    ws.send(json.dumps(listen_message))
    logging.info(f"Sent subscription message: {listen_message}")

# Websocket on_close callback function
def on_close(ws):
    logging.info("Closed connection")

# Main function to start WebSocket and Kafka producer
def main():
    # Creating websocket instance
    ws = websocket.WebSocketApp(socket_url, on_open=on_open, on_message=on_message, on_close=on_close)
    # Running websocket
    ws.run_forever()

if __name__ == "__main__":
    main()
