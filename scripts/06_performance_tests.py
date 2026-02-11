"""
SCRIPT 6: Performance Benchmarking
Tests: Partitioning, Caching, Resource configs
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

spark = SparkSession.builder \
    .appName("Performance_Tests") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

print("="*80)
print("PERFORMANCE BENCHMARKING")
print("="*80)

SILVER_PATH = "/opt/spark/project/data/silver/wikipedia_clean"
import os

if not os.path.exists(SILVER_PATH):
    print(f"ERROR: SILVER_PATH does not exist: {SILVER_PATH}\nRun the Bronze->Silver ETL first.")
    spark.stop()
    raise SystemExit(1)

# ============================================================================
# TEST 1: Impact of Partitioning
# ============================================================================
print("\n" + "="*80)
print("TEST 1: PARTITIONING IMPACT")
print("="*80)

partition_configs = [1, 4, 8, 16]

for num_partitions in partition_configs:
    df = spark.read.parquet(SILVER_PATH).repartition(num_partitions)
    
    start = time.time()
    count = df.count()
    duration = time.time() - start
    
    print(f"Partitions: {num_partitions:2d} | Time: {duration:6.2f}s | Records: {count:,}")

# ============================================================================
# TEST 2: Caching Impact
# ============================================================================
print("\n" + "="*80)
print("TEST 2: CACHING IMPACT")
print("="*80)

df = spark.read.parquet(SILVER_PATH)

# Without cache
start = time.time()
df.filter(col("is_bot") == 1).count()
time_no_cache = time.time() - start

# With cache
df.cache()
start = time.time()
df.filter(col("is_bot") == 1).count()
time_with_cache = time.time() - start

print(f"Without cache: {time_no_cache:.2f}s")
print(f"With cache:    {time_with_cache:.2f}s")
print(f"Speedup:       {time_no_cache/time_with_cache:.2f}x")

spark.stop()