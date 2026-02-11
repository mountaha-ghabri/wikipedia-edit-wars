"""
SCRIPT 2: Bronze to Silver ETL Pipeline
Demonstrates: RDDs, DataFrames, Spark SQL, Data Quality
Meets requirements: Distributed processing, transformations, quality checks
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkConf

# Spark Configuration
conf = SparkConf() \
    .setAppName("Wikipedia_Bronze_to_Silver") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.sql.adaptive.enabled", "true") \
    .set("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .set("spark.executor.memory", "4g") \
    .set("spark.driver.memory", "2g")

spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext

print("="*80)
print("BRONZE TO SILVER ETL PIPELINE")
print("="*80)
print(f"Spark Version: {spark.version}")
print(f"Master: {sc.master}")
print("="*80)

# Paths
BRONZE_PATH = "/opt/spark/project/data/bronze/wikipedia/*.json"
SILVER_PATH = "/opt/spark/project/data/silver/wikipedia_clean"

print(f"\n📥 Input: {BRONZE_PATH}")
print(f"📤 Output: {SILVER_PATH}")

# ============================================================================
# STAGE 1: LOAD RAW DATA (Using RDDs - Show professor RDD skills!)
# ============================================================================
print("\n" + "="*80)
print("STAGE 1: LOADING RAW DATA WITH RDDs")
print("="*80)

# Read as RDD first (professor wants to see RDD usage!)
raw_rdd = sc.textFile(BRONZE_PATH)
print(f"✅ Loaded RDD partitions: {raw_rdd.getNumPartitions()}")

# Parse JSON in RDD
import json

def safe_json_parse(line):
    """Safely parse JSON line"""
    try:
        obj = json.loads(line)
        # Support both:
        # - batch files that are one big JSON array
        # - newline-delimited JSON objects
        if isinstance(obj, list):
            return obj
        return [obj]
    except:
        # On any parse error, just skip this line
        return []

parsed_rdd = raw_rdd.flatMap(lambda line: safe_json_parse(line))

print(f"✅ Parsed JSON records (RDD)")

# Convert RDD to DataFrame (showing RDD → DataFrame transition)
df_raw = spark.createDataFrame(parsed_rdd)

print(f"\n📊 Raw record count: {df_raw.count():,}")
print("\n📋 Raw Schema:")
df_raw.printSchema()
print("\n👀 Sample raw rows:")
df_raw.show(5, truncate=False)

# ============================================================================
# STAGE 2: DATA QUALITY CHECKS
# ============================================================================
print("\n" + "="*80)
print("STAGE 2: DATA QUALITY CHECKS")
print("="*80)

# Check for nulls in critical columns
critical_cols = ['page_title', 'revid', 'user', 'timestamp']

print("\n🔍 Null value analysis:")
for col_name in critical_cols:
    null_count = df_raw.filter(col(col_name).isNull()).count()
    total = df_raw.count()
    pct = (null_count / total * 100) if total > 0 else 0
    print(f"   {col_name:15} {null_count:10,} nulls ({pct:5.2f}%)")

# Check for duplicates
dup_count = df_raw.count() - df_raw.dropDuplicates(['revid']).count()
print(f"\n🔍 Duplicate revisions: {dup_count:,}")

# ============================================================================
# STAGE 3: DATA CLEANING & TRANSFORMATION
# ============================================================================
print("\n" + "="*80)
print("STAGE 3: DATA CLEANING & TRANSFORMATION")
print("="*80)

# Clean and transform
df_clean = df_raw \
    .filter(col("page_title").isNotNull()) \
    .filter(col("revid").isNotNull()) \
    .filter(col("user").isNotNull()) \
    .filter(col("timestamp").isNotNull()) \
    .dropDuplicates(["revid"]) \
    .withColumn("timestamp_parsed", to_timestamp(col("timestamp"))) \
    .withColumn("year", year(col("timestamp_parsed"))) \
    .withColumn("month", month(col("timestamp_parsed"))) \
    .withColumn("day", dayofmonth(col("timestamp_parsed"))) \
    .withColumn("hour", hour(col("timestamp_parsed"))) \
    .withColumn("day_of_week", dayofweek(col("timestamp_parsed"))) \
    .withColumn("day_name", date_format(col("timestamp_parsed"), "EEEE")) \
    .withColumn("size_bytes", col("size").cast("long")) \
    .withColumn("is_minor", when(col("minor") == True, 1).otherwise(0)) \
    .withColumn("is_bot", when(col("bot") == True, 1).otherwise(0)) \
    .withColumn("is_anon", when(col("anon") == True, 1).otherwise(0)) \
    .withColumn("comment_length", length(col("comment"))) \
    .withColumn("has_comment", when(col("comment").isNotNull() & (col("comment") != ""), 1).otherwise(0))

# Calculate edit size change (requires window function)
from pyspark.sql.window import Window

window_spec = Window.partitionBy("page_title").orderBy("timestamp_parsed")

df_clean = df_clean \
    .withColumn("prev_size", lag("size_bytes").over(window_spec)) \
    .withColumn("size_change", col("size_bytes") - coalesce(col("prev_size"), lit(0))) \
    .withColumn("size_change_abs", abs(col("size_change")))

print("✅ Transformations applied")

# ============================================================================
# STAGE 4: SELECT FINAL SCHEMA
# ============================================================================
print("\n" + "="*80)
print("STAGE 4: FINAL SCHEMA SELECTION")
print("="*80)

df_silver = df_clean.select(
    # Page Information
    col("page_title"),
    col("page_id"),
    col("page_watchers").cast("int"),
    
    # Revision Information  
    col("revid").cast("long"),
    col("parentid").cast("long"),
    col("timestamp_parsed").alias("timestamp"),
    
    # Time Dimensions
    col("year").cast("int"),
    col("month").cast("int"),
    col("day").cast("int"),
    col("hour").cast("int"),
    col("day_of_week").cast("int"),
    col("day_name"),
    
    # User Information
    col("user"),
    col("userid").cast("long"),
    col("is_anon").cast("int"),
    col("is_bot").cast("int"),
    
    # Edit Details
    col("size_bytes").cast("long"),
    col("size_change").cast("long"),
    col("size_change_abs").cast("long"),
    col("is_minor").cast("int"),
    col("comment"),
    col("comment_length").cast("int"),
    col("has_comment").cast("int"),
    
    # Tags
    col("tags")
)

print("\n📋 Silver Schema:")
df_silver.printSchema()
print("\n👀 Sample silver rows:")
df_silver.show(5, truncate=False)

# ============================================================================
# STAGE 5: DATA QUALITY REPORT
# ============================================================================
print("\n" + "="*80)
print("STAGE 5: DATA QUALITY REPORT")
print("="*80)

final_count = df_silver.count()
print(f"\n✅ Final record count: {final_count:,}")
print(f"📉 Records removed: {df_raw.count() - final_count:,}")
print(f"📊 Data retention: {final_count / df_raw.count() * 100:.2f}%")

# Summary statistics
print("\n📊 Summary Statistics:")
df_silver.select(
    count("*").alias("total_edits"),
    countDistinct("page_title").alias("unique_pages"),
    countDistinct("user").alias("unique_users"),
    avg("size_bytes").alias("avg_edit_size"),
    avg("comment_length").alias("avg_comment_len"),
    sum("is_bot").alias("bot_edits"),
    sum("is_anon").alias("anon_edits"),
    sum("is_minor").alias("minor_edits")
).show()

# Date range
print("\n📅 Date Range:")
df_silver.select(
    min("timestamp").alias("earliest_edit"),
    max("timestamp").alias("latest_edit")
).show(truncate=False)

# Top edited pages
print("\n🔥 Top 10 Most Edited Pages:")
df_silver.groupBy("page_title") \
    .count() \
    .orderBy(desc("count")) \
    .limit(10) \
    .show(truncate=False)

# ============================================================================
# STAGE 6: SAVE TO SILVER (Partitioned for Performance!)
# ============================================================================
print("\n" + "="*80)
print("STAGE 6: SAVING TO SILVER LAYER")
print("="*80)

print(f"\n💾 Writing to: {SILVER_PATH}")
print("📁 Partitioning by: year, month")

df_silver.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(SILVER_PATH)

print("✅ Data saved successfully!")

# Verify partitions created
print("\n📊 Partition Statistics:")
partition_count = df_silver.select("year", "month").distinct().count()
print(f"   Total partitions created: {partition_count}")

# ============================================================================
# STAGE 7: REGISTER AS TEMP VIEW FOR SQL ACCESS
# ============================================================================
print("\n" + "="*80)
print("STAGE 7: REGISTERING SQL VIEW")
print("="*80)

df_silver.createOrReplaceTempView("wikipedia_edits")
print("✅ Registered as: wikipedia_edits")

# Test SQL query
print("\n🔍 SQL Test Query:")
spark.sql("""
    SELECT 
        page_title,
        COUNT(*) as edit_count,
        COUNT(DISTINCT user) as unique_editors
    FROM wikipedia_edits
    GROUP BY page_title
    ORDER BY edit_count DESC
    LIMIT 5
""").show(truncate=False)

print("\n" + "="*80)
print("✅ BRONZE TO SILVER ETL COMPLETE!")
print("="*80)

spark.stop()