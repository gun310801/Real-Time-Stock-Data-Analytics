from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col,  to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, IntegerType
from kafka import KafkaConsumer, KafkaProducer
import boto3
import json

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

# Kafka broker address
kafka_bootstrap_servers = ['ip-x-x-x-x.ec2.internal:9092']

# Kafka topics
kafka_input_topic = "symbol_topic"
kafka_output_topic = "visual_topic"  # New Kafka topic for processed data

s3_bucket = "bdp-stock-analytics-3110"
s3_key_prefix = "stock-data-from-socket-2/"
s3_key_prefix_logs = "logfiles/logs/"

# Create a Kafka consumer
consumer = KafkaConsumer(kafka_input_topic,
                         bootstrap_servers=kafka_bootstrap_servers)

# Define schema for incoming data from Kafka
quote_schema = StructType() \
        .add("message_type", StringType()) \
        .add("symbol", StringType()) \
        .add("ask_exchange_code", StringType()) \
        .add("ask_price", DoubleType()) \
        .add("ask_size", IntegerType()) \
        .add("bid_exchange_code", StringType()) \
        .add("bid_price", DoubleType()) \
        .add("bid_size", IntegerType()) \
        .add("timestamp", StringType()) \
        

# Read data from Kafka into a DataFrame
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", ",".join(kafka_bootstrap_servers)) \
    .option("subscribe", kafka_input_topic) \
    .load()

# Parse JSON data and select required fields
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), quote_schema).alias("data1")) \
    .select("data1.*")
df = df.withColumn("bid_ask_spread", (col("ask_price") - col("bid_price")) / col("ask_price"))
df = df.withColumn("order_imbalance", (col("ask_size") - col("bid_size")) / (col("ask_size") + col("bid_size")))

# Define the function to write processed data to Kafka
def send_to_kafka(df, epoch_id):
    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
    # Convert DataFrame to JSON and send each row to Kafka
    for row in df.toJSON().collect():
        producer.send(kafka_output_topic, value=row.encode('utf-8'))

# Define the function to write the data to S3 (unchanged from your original code)
def write_to_s3(df, epoch_id):
    s3_client = boto3.client("s3")
    for symbol in df.select("symbol").distinct().collect():
        symbol_name = symbol["symbol"]
        s3_key = f"{s3_key_prefix}/{symbol_name}/quote_data/"
        df_symbol = df.filter(col("symbol") == symbol_name)
        df_symbol.write \
            .format("parquet") \
            .mode("append") \
            .save(f"s3://{s3_bucket}/{s3_key}")

# Start streaming queries for both writing to S3 and sending to Kafka
query_s3 = df.writeStream \
    .foreachBatch(write_to_s3) \
    .outputMode("append") \
    .start()

query_kafka = df.writeStream \
    .foreachBatch(send_to_kafka) \
    .outputMode("append") \
    .start()

# Wait for the streaming queries to finish
query_s3.awaitTermination()
query_kafka.awaitTermination()
