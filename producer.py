from kafka import KafkaProducer
import json
import time
import websocket
import random


# Kafka broker address
bootstrap_servers = ['ip-10-0-17-149.ec2.internal:9092']

# Kafka topic for the symbol
topic = 'symbol_topic'
topic2 = 'symbol_topic2'

# Creating Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Websocket URL
socket_url = "wss://stream.data.alpaca.markets/v2/test"
# socket_url = "wss://stream.data.alpaca.markets/v2/iex"
# Function to send message to Kafka
def send_to_kafka(message,topic):
    producer.send(topic, value=message)
    print(f"Produced message: {message}")

# Websocket on_message callback function
def on_message(ws, message):
    print(message)
    data = json.loads(message)
    message_type = data[0]["T"]
        
    if message_type == "q":  # Quote message
        quote_data = {
            'message_type' :"q",
            'symbol': data[0]["S"],
            'ask_exchange_code': data[0]["ax"],
            'ask_price': data[0]["ap"],
            'ask_size': data[0]["as"],
            'bid_exchange_code': data[0]["bx"],
            'bid_price': data[0]["bp"],
            'bid_size': data[0]["bs"],
            'timestamp': data[0]["t"],
            'tape': data[0]["z"],
            'random': random.randint(0, 100)
        }
        send_to_kafka(quote_data,topic)
        print("sent to kafka quote data1", quote_data)
        
    elif message_type =="b" :  # Bar message
        bar_data = {
            'message_type' :"b",
            'symbol': data[0]["S"],
            'open_price': data[0]["o"],
            'high_price': data[0]["h"],
            'low_price': data[0]["l"],
            'close_price': data[0]["c"],
            'volume': data[0]["v"],
            'timestamp': data[0]["t"]
        }
        send_to_kafka(bar_data,topic2)
        print("sent to kafka bar data2", bar_data)
# Websocket on_open callback function
def on_open(ws):
    print("Opened connection")
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
        "quotes":["FAKEPACA"],
        #"bars": ["AAPL", "SPY", "DIA","MSFT","TSLA","META","GOOG"]
        "bars": ["FAKEPACA"]
    }
    ws.send(json.dumps(listen_message))
    print(listen_message)

# Websocket on_close callback function
def on_close(ws):
    print("Closed connection")

if __name__ == "__main__":
    # Creating Kafka producer
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # Creating websocket instance
    ws = websocket.WebSocketApp(socket_url, on_open=on_open, on_message=on_message, on_close=on_close)
    
    # Running websocket
    ws.run_forever()
