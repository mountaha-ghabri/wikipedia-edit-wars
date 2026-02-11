"""
SCRIPT 5: Kafka Streaming Simulation
Demonstrates: Spark Structured Streaming
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import time
from kafka import KafkaProducer
import threading

# ============================================================================
# PRODUCER: Simulate real-time Wikipedia edits
# ============================================================================
def produce_edits():
    """Produce simulated edit events to Kafka"""
    producer = KafkaProducer(
        bootstrap_servers=['kafka:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print("📡 Kafka Producer started...")
    
    # Read sample data
    import glob
    files = glob.glob("/opt/spark/project/data/bronze/wikipedia/*.json")
    
    if not files:
        print("⚠️  No data files found")
        return
    
    with open(files[0]) as f:
        sample_edits = json.load(f)
    
    # Stream edits at 10/second
    for i, edit in enumerate(sample_edits):
        producer.send('wikipedia-edits', value=edit)
        time.sleep(0.1)
        
        if i % 100 == 0:
            print(f"   Sent {i} edit events...")
    
    print("✅ Producer finished")

# ============================================================================
# CONSUMER: Spark Structured Streaming
# ============================================================================
def consume_and_analyze():
    """Consume and analyze streaming edits"""
    
    spark = SparkSession.builder \
        .appName("Wikipedia_Streaming") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    
    print("="*80)
    print("SPARK STRUCTURED STREAMING")
    print("="*80)
    
    # Define schema
    schema = StructType([
        StructField("page_title", StringType()),
        StructField("user", StringType()),
        StructField("timestamp", StringType()),
        StructField("size", IntegerType()),
        StructField("is_minor", BooleanType()),
        StructField("is_bot", BooleanType())
    ])
    
    # Read from Kafka
    df_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "wikipedia-edits") \
        .load()
    
    # Parse JSON
    df_parsed = df_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Windowed aggregations (5-minute windows)
    df_agg = df_parsed \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(
            window("timestamp", "5 minutes"),
            "page_title"
        ).agg(
            count("*").alias("edit_count"),
            countDistinct("user").alias("unique_editors")
        )
    
    # Write to console
    query = df_agg.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    print("✅ Streaming query started")
    
    query.awaitTermination(timeout=300)  # Run for 5 minutes
    
    spark.stop()

# ============================================================================
# MAIN
# ============================================================================
if __name__ == "__main__":
    # Start producer in background
    producer_thread = threading.Thread(target=produce_edits)
    producer_thread.start()
    
    # Wait for producer to start
    time.sleep(5)
    
    # Start consumer
    consume_and_analyze()
    
    producer_thread.join()