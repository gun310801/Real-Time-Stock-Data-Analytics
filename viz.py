import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
import plotly.graph_objs as go
from plotly.subplots import make_subplots

# Kafka bootstrap servers
kafka_bootstrap_servers = ['ip-10-0-31-206.ec2.internal:9092']

# Kafka topics
kafka_output_topic = "visual_topic"
kafka_output_topic2 = "symbol_topic7"

# Initialize Kafka consumers for each topic
consumer = KafkaConsumer(kafka_output_topic, bootstrap_servers=kafka_bootstrap_servers, auto_offset_reset='earliest')
consumer2 = KafkaConsumer(kafka_output_topic2, bootstrap_servers=kafka_bootstrap_servers, auto_offset_reset='earliest')

# Initialize empty DataFrames to store data
data = pd.DataFrame(columns=['timestamp', 'bid_ask_spread'])
data2 = pd.DataFrame(columns=['timestamp', 'bid_price', 'ask_price'])
imbalance_data = pd.DataFrame(columns=['timestamp', 'order_imbalance'])

# Dictionary to store OHLCV data for each symbol
ohlcv_data = {}

# Function to update line and bar plots
def update_plot(data, chart):
    chart.line_chart(data.set_index('timestamp'))
    

# Page 1: Bid-Ask Spread and Order Imbalance
def page1():
    missing_quotes = 0
    total_quotes = 0
    dqi_container = st.empty()

    st.title('Bid-Ask Spread')
    chart1 = st.line_chart(data)
    st.title('Bid-Ask')
    chart2 = st.line_chart(data2)
    st.title('Order Imbalance')
    
    chart3 = st.bar_chart(imbalance_data)
    for message in consumer:
        try:
            msg = json.loads(message.value.decode('utf-8'))

            # Ensure msg is a dictionary
            if not isinstance(msg, dict):
                st.error("Received message is not a dictionary")
                continue

            # Check if all necessary keys are present
            if 'timestamp' in msg and 'bid_ask_spread' in msg and 'bid_price' in msg and 'ask_price' in msg and 'order_imbalance' in msg:
                timestamp = msg['timestamp']
                bid_ask_spread = msg['bid_ask_spread']
                bid_price = msg['bid_price']
                ask_price = msg['ask_price']
                order_imbalance = msg['order_imbalance']
                total_quotes = total_quotes + 1
                with dqi_container:
                    
                    st.write("DQI:",missing_quotes / total_quotes)
                # Update dataframes and charts
                data.loc[len(data)] = [timestamp, bid_ask_spread]
                update_plot(data, chart1)

                data2.loc[len(data2)] = [timestamp, bid_price, ask_price]
                update_plot(data2, chart2)

                imbalance_data.loc[len(imbalance_data)] = [timestamp, order_imbalance]
                update_plot(imbalance_data, chart3)
               
            else:
                missing_quotes = missing_quotes + 1
                total_quotes = total_quotes + 1
                with dqi_container:
                   st.write("DQI:",missing_quotes / total_quotes)
                st.warning("Received message is missing required fields")
        except json.JSONDecodeError:
            st.error("Error decoding JSON from Kafka message")

# Page 2: Candlestick Chart and Line Chart with Volume
def page2():
    st.title('Candlestick Chart and Line Chart with Volume')
    symbol_charts = {}
   
    # Create placeholders for each symbol
    for message in consumer2:
        try:
            msg = json.loads(message.value.decode('utf-8'))

            if 'symbol' in msg:
                symbol = msg['symbol']
                if symbol not in symbol_charts:
                    # Create a title for the symbol chart
                    st.subheader(f'Charts for {symbol}')
                    symbol_charts[symbol] = {
                        'candlestick': st.plotly_chart(go.Figure(), use_container_width=True),
                        'line_volume': st.plotly_chart(go.Figure(), use_container_width=True)
                    }

                if symbol not in ohlcv_data:
                    ohlcv_data[symbol] = pd.DataFrame(columns=['timestamp', 'open_price', 'high_price', 'low_price', 'close_price', 'volume'])

                # Append new data to the DataFrame for the symbol
                new_data = {
                    'timestamp': msg.get('timestamp'),
                    'open_price': msg.get('open_price'),
                    'high_price': msg.get('high_price'),
                    'low_price': msg.get('low_price'),
                    'close_price': msg.get('close_price'),
                    'volume': msg.get('volume')
                }

                # Check if all necessary keys are present
                if all(new_data.values()):
                    ohlcv_data[symbol] = pd.concat([ohlcv_data[symbol], pd.DataFrame([new_data])], ignore_index=True)
                 
               
                    # Candlestick chart
                    candlestick_fig = go.Figure(data=[go.Candlestick(
                        x=ohlcv_data[symbol]['timestamp'],
                        open=ohlcv_data[symbol]['open_price'],
                        high=ohlcv_data[symbol]['high_price'],
                        low=ohlcv_data[symbol]['low_price'],
                        close=ohlcv_data[symbol]['close_price'],
                        name='Candlestick'
                    )])
                    symbol_charts[symbol]['candlestick'].plotly_chart(candlestick_fig, use_container_width=True)

                    # Line chart for closing prices and bar chart for volume
                    line_volume_fig = make_subplots(specs=[[{"secondary_y": True}]])
                    
                    # Line chart for closing prices
                    line_volume_fig.add_trace(go.Scatter(
                        x=ohlcv_data[symbol]['timestamp'],
                        y=ohlcv_data[symbol]['close_price'],
                        mode='lines',
                        name='Closing Prices',
                        line=dict(color='blue')
                    ), secondary_y=False)

                    # Bar chart for volume
                    line_volume_fig.add_trace(go.Bar(
                        x=ohlcv_data[symbol]['timestamp'],
                        y=ohlcv_data[symbol]['volume'],
                        name='Volume',
                        marker=dict(color='gray'),
                        opacity=0.3  # Adjust the opacity of the bars
                    ), secondary_y=True)

                    # Set y-axes titles
                    line_volume_fig.update_yaxes(title_text="Closing Prices", secondary_y=False)
                    line_volume_fig.update_yaxes(title_text="Volume", secondary_y=True)

                    # Adjust the bar height by setting a range for the secondary y-axis
                    max_volume = ohlcv_data[symbol]['volume'].max()
                    line_volume_fig.update_yaxes(range=[0, max_volume * 0.5], secondary_y=True)

                    line_volume_fig.update_layout(title_text=f'Closing Prices and Volume for {symbol}', showlegend=False)
                    symbol_charts[symbol]['line_volume'].plotly_chart(line_volume_fig, use_container_width=True)
                else:
                    
                     st.warning(f"Received message for symbol {symbol} is missing required fields: {new_data}")
        except json.JSONDecodeError:
            st.error("Error decoding JSON from Kafka message")

# Streamlit app
def main():
    st.sidebar.title('Navigation')
    page = st.sidebar.selectbox('Go to:', ['Bid-Ask Spread and Order Imbalance', 'Candlestick Chart'])

    if page == 'Bid-Ask Spread and Order Imbalance':
        page1()
    elif page == 'Candlestick Chart':
        page2()

if __name__ == "__main__":
    main()
