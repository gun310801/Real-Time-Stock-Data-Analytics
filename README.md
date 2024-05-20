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
- wss://stream.data.alpaca.markets/v2/iex
- wss://stream.data.alpaca.markets/v2/test

## AWS

1. Create EMR Cluster
- Configure Cluster
- Connect to Primary node using ssh


2. Edit EC2 Security Groups - give access to local machine's ip


3. SSH EMR Cluster on local machine
- Navigate to the aws-key.pem directory
- ssh into the emr cluster


4. Set up kafka
- pip install kafka-python
- pip install boto3
- pip install websocket-client streamlit watchdog
-  wget https://downloads.apache.org/kafka/3.5.2/kafka_2.13-3.5.2.tgz
- tar -xzf kafka_2.13-3.5.2.tgz
- cd kafka_2.13-3.5.2
- nano config/server.properties
- look for advertized listeners and change it to ec2's host ip for master node
- look for zookeeper connect and change it to ec2's host ip for master node
- bin/kafka-server-start.sh config/server.properties


5. Set up Producer.py
- vim producer.py
- mkdir -p /home/hadoop/producer1
- nano /home/hadoop/producer1/log4j.properties


6. Set up Consumer.py
7. Set up Visualizer (Viz.py)
8. Set up Log Visualizer (log_viz.py)
9. Ganglia (data monitoring)
    - look for ganglia application under emr
    - ssh tunneling to port 8050
    - create a SOCKS5 proxy on the browser
    - look for master/dns url 
