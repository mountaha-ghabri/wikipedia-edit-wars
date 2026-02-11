from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
import time
import sys

# 1. SETUP WITH PERFORMANCE TUNING
spark = SparkSession.builder \
    .appName("WikiCapstonePipeline") \
    .config("spark.sql.shuffle.partitions", "100") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .getOrCreate()

def start_overnight_process():
    try:
        print("📥 Phase 1: Ingesting 10M+ Records...")
        # Reading from your existing Silver data
        df = spark.read.parquet("/opt/spark/project/data/silver/wiki_delta/*.parquet")

        print("🧹 Phase 2: Cleaning & Temporal Engineering...")
        # Requirement: Data Prep & Time Series [cite: 1, 2]
        processed_df = df.select("user", "size", "timestamp") \
            .withColumn("timestamp", F.to_timestamp("timestamp")) \
            .withColumn("hour", F.hour("timestamp")) \
            .withColumn("day", F.date_format("timestamp", "yyyy-MM-dd")) \
            .dropna()

        print("🤖 Phase 3: ML Clustering (Segmentation)...")
        # Requirement: Feature Engineering & MLlib
        user_features = processed_df.groupBy("user").agg(
            F.count("*").alias("total_edits"),
            F.avg("size").alias("avg_edit_size")
        )

        assembler = VectorAssembler(inputCols=["total_edits", "avg_edit_size"], outputCol="features")
        feature_vector = assembler.transform(user_features)
        
        scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
        scaled_df = scaler.fit(feature_vector).transform(feature_vector)

        # K-Means for User Segmentation
        kmeans = KMeans(featuresCol="scaledFeatures", k=3, seed=1)
        model = kmeans.fit(scaled_df)
        predictions = model.transform(scaled_df)

        print("💾 Phase 4: Sinking to Gold Layer (Partitioned for Scalability)...")
        # Requirement: Lakehouse design & Performance Optimization
        final_output = processed_df.join(predictions.select("user", "prediction"), "user")
        
        final_output.write.mode("overwrite") \
            .partitionBy("prediction") \
            .parquet("/opt/spark/project/data/gold/wiki_intelligence")

        print("✅ Pipeline Successfully Finished. Enjoy your sleep.")

    except Exception as e:
        print(f"⚠️ Error: {e}. Retrying in 60 seconds...")
        time.sleep(60)
        start_overnight_process()

if __name__ == "__main__":
    start_overnight_process()