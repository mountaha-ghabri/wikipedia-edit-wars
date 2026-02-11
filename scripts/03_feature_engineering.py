"""
SCRIPT 3 FIXED: Advanced Feature Engineering
Fixes:
  - fillna(0) instead of na.drop() everywhere
  - coalesce() for all lag() / window outputs so nulls become 0
  - Correct time_since_last_edit (unix_timestamp difference, not raw cast)
  - is_revert_candidate uses safe coalesce on lagged value
  - hour_category / edit_magnitude remain as strings (StringIndexer handles them in ML)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, avg, sum as spark_sum,
    when, row_number, lag, coalesce, lit, abs as spark_abs,
    unix_timestamp, desc
)
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Wikipedia_Feature_Engineering_FIXED") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("FEATURE ENGINEERING PIPELINE  (FIXED)")
print("=" * 80)

SILVER_PATH   = "/opt/spark/project/data/silver/wikipedia_clean"
FEATURES_PATH = "/opt/spark/project/data/silver/wikipedia_features"

df = spark.read.parquet(SILVER_PATH)
df.cache()
total = df.count()
print(f"Records loaded: {total:,}")

df.createOrReplaceTempView("wikipedia_edits")

# ─────────────────────────────────────────────────────────────────────────────
# FEATURE SET 1 – USER BEHAVIOUR (5 features)
# ─────────────────────────────────────────────────────────────────────────────
print("\n── Feature Set 1: User Behaviour ──")

user_stats = df.groupBy("user").agg(
    count("*").alias("user_total_edits"),
    countDistinct("page_title").alias("user_pages_edited"),
    avg("size_change_abs").alias("user_avg_edit_size"),
    spark_sum("is_minor").alias("user_minor_edit_count"),
    (spark_sum("is_minor") / count("*")).alias("user_minor_edit_ratio"),
)

df = df.join(user_stats, on="user", how="left")
# fillna for joined aggregates (users with no match → shouldn't happen, but safe)
df = df.fillna(0, subset=[
    "user_total_edits", "user_pages_edited",
    "user_avg_edit_size", "user_minor_edit_count", "user_minor_edit_ratio"
])
print("✅  5 user features added")

# ─────────────────────────────────────────────────────────────────────────────
# FEATURE SET 2 – PAGE FEATURES (6 features)
# ─────────────────────────────────────────────────────────────────────────────
print("\n── Feature Set 2: Page Features ──")

page_stats = df.groupBy("page_title").agg(
    count("*").alias("page_total_edits"),
    countDistinct("user").alias("page_unique_editors"),
    avg("size_change_abs").alias("page_avg_edit_size"),
    spark_sum(when(col("size_change") > 0, 1).otherwise(0)).alias("page_additions"),
    spark_sum(when(col("size_change") < 0, 1).otherwise(0)).alias("page_deletions"),
).withColumn(
    "page_controversy_score",
    col("page_total_edits") * col("page_unique_editors") / 1000.0
)

df = df.join(page_stats, on="page_title", how="left")
df = df.fillna(0, subset=[
    "page_total_edits", "page_unique_editors", "page_avg_edit_size",
    "page_additions", "page_deletions", "page_controversy_score"
])
print("✅  6 page features added")

# ─────────────────────────────────────────────────────────────────────────────
# FEATURE SET 3 – TEMPORAL FEATURES (4 features)
# ─────────────────────────────────────────────────────────────────────────────
print("\n── Feature Set 3: Temporal Features ──")

df = df \
    .withColumn("is_weekend",
                when(col("day_of_week").isin([1, 7]), 1).otherwise(0)) \
    .withColumn("is_business_hours",
                when((col("hour") >= 9) & (col("hour") <= 17), 1).otherwise(0)) \
    .withColumn("is_late_night",
                when((col("hour") >= 22) | (col("hour") <= 5), 1).otherwise(0)) \
    .withColumn("hour_category",
                when((col("hour") >= 6)  & (col("hour") < 12), "morning")
               .when((col("hour") >= 12) & (col("hour") < 18), "afternoon")
               .when((col("hour") >= 18) & (col("hour") < 22), "evening")
               .otherwise("night"))

print("✅  4 temporal features added")

# ─────────────────────────────────────────────────────────────────────────────
# FEATURE SET 4 – EDIT CHARACTERISTICS (5 features)
# ─────────────────────────────────────────────────────────────────────────────
print("\n── Feature Set 4: Edit Characteristics ──")

df = df \
    .withColumn("is_major_edit",
                when(col("size_change_abs") > 1000, 1).otherwise(0)) \
    .withColumn("is_deletion",
                when(col("size_change") < -100, 1).otherwise(0)) \
    .withColumn("is_addition",
                when(col("size_change") > 100, 1).otherwise(0)) \
    .withColumn("edit_magnitude",
                when(col("size_change_abs") < 100,  "small")
               .when(col("size_change_abs") < 1000, "medium")
               .otherwise("large")) \
    .withColumn("has_long_comment",
                when(col("comment_length") > 50, 1).otherwise(0))

print("✅  5 edit characteristics added")

# ─────────────────────────────────────────────────────────────────────────────
# FEATURE SET 5 – SEQUENCE FEATURES (5 features)
#   FIXED: use unix_timestamp for time diffs; coalesce all lag() outputs
# ─────────────────────────────────────────────────────────────────────────────
print("\n── Feature Set 5: Sequence Features (Window) ──")

user_window = Window.partitionBy("user").orderBy("timestamp")
page_window = Window.partitionBy("page_title").orderBy("timestamp")

df = df \
    .withColumn("user_edit_sequence",
                row_number().over(user_window)) \
    .withColumn("time_since_last_edit_sec",
                # ← FIXED: real elapsed seconds, null-safe (first edit of a user → 0)
                coalesce(
                    unix_timestamp(col("timestamp")) -
                    lag(unix_timestamp(col("timestamp"))).over(user_window),
                    lit(0)
                ).cast("long")) \
    .withColumn("user_is_rapid_editor",
                when(col("time_since_last_edit_sec").between(1, 60), 1).otherwise(0))

df = df \
    .withColumn("page_edit_sequence",
                row_number().over(page_window)) \
    .withColumn("is_revert_candidate",
                # ← FIXED: coalesce so first edit in page window doesn't produce null
                when(
                    (col("size_change") < 0) &
                    (spark_abs(col("size_change")) >
                     0.8 * coalesce(spark_abs(lag("size_change").over(page_window)), lit(0))),
                    1
                ).otherwise(0))

# Fill any remaining nulls from window ops on edge partitions
df = df.fillna(0, subset=[
    "user_edit_sequence", "time_since_last_edit_sec", "user_is_rapid_editor",
    "page_edit_sequence", "is_revert_candidate"
])
print("✅  5 sequence features added")

# ─────────────────────────────────────────────────────────────────────────────
# FEATURE SET 6 – DERIVED METRICS (3 features)
# ─────────────────────────────────────────────────────────────────────────────
print("\n── Feature Set 6: Derived Metrics ──")

df = df \
    .withColumn("edit_efficiency",
                col("size_change_abs") / (col("comment_length") + 1)) \
    .withColumn("user_specialization",
                lit(1.0) / (col("user_pages_edited") + 1)) \
    .withColumn("page_edit_density",
                col("page_total_edits") / (col("page_unique_editors") + 1))

df = df.fillna(0, subset=["edit_efficiency", "user_specialization", "page_edit_density"])
print("✅  3 derived metrics added")

# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
ALL_FEATURES = [
    # User (5)
    "user_total_edits", "user_pages_edited", "user_avg_edit_size",
    "user_minor_edit_count", "user_minor_edit_ratio",
    # Page (6)
    "page_total_edits", "page_unique_editors", "page_avg_edit_size",
    "page_additions", "page_deletions", "page_controversy_score",
    # Temporal (4)
    "is_weekend", "is_business_hours", "is_late_night", "hour_category",
    # Edit characteristics (5)
    "is_major_edit", "is_deletion", "is_addition", "edit_magnitude", "has_long_comment",
    # Sequence (5)
    "user_edit_sequence", "time_since_last_edit_sec", "user_is_rapid_editor",
    "page_edit_sequence", "is_revert_candidate",
    # Derived (3)
    "edit_efficiency", "user_specialization", "page_edit_density",
]

print(f"\n✅ Total features: {len(ALL_FEATURES)}")

# ─────────────────────────────────────────────────────────────────────────────
# NULL CHECK  (after all fillna — should all be 0)
# ─────────────────────────────────────────────────────────────────────────────
print("\n── Null counts (should all be 0 after fillna) ──")
from pyspark.sql.functions import count as spark_count
null_report = df.select([
    spark_count(when(col(c).isNull(), c)).alias(c) for c in ALL_FEATURES
])
null_report.show(truncate=False)

# ─────────────────────────────────────────────────────────────────────────────
# SAVE
# ─────────────────────────────────────────────────────────────────────────────
print("\n── Saving Feature Dataset ──")
df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(FEATURES_PATH)

final_count = df.count()
print(f"✅ Saved {final_count:,} records to: {FEATURES_PATH}")

# Quick stats
print("\n── Edit Magnitude Distribution ──")
df.groupBy("edit_magnitude").count().orderBy(desc("count")).show()

print("\n── Hour Category Distribution ──")
df.groupBy("hour_category").count().orderBy(desc("count")).show()

print("\n" + "=" * 80)
print(f"✅ FEATURE ENGINEERING COMPLETE  ({len(ALL_FEATURES)} features)")
print("=" * 80)

spark.stop()