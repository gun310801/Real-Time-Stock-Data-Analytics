# Analytics on Stock Market Data Live Stream using AWS

## Data :

 Live Market Data:
 1. During Market Hours
 - Quote Data for AAPL
 - Bar Data for AAPL, MSFT, GOOG, SPY, META, DIA, TSLA
 2. Outside Market Hours 
 - Quote Data for FAKEPACA
 - Bar Data for FAKEPACA 

Websocket : 
wss://stream.data.alpaca.markets/v2/iex
wss://stream.data.alpaca.markets/v2/test

## AWS

1. Create EMR Cluster
2. Edit EC2 Security Groups - give access to local machine's ip
3. SSH EMR Cluster on local machine
4. Set up kafka
5. Set up Producer.py
6. Set up Consumer.py
7. Set up Visualizer (Viz.py)
8. Set up Log Visualizer (log_viz.py)
9. Ganglia (data monitoring)
