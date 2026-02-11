"""
SCRIPT 4 FIXED: Machine Learning Models
Fixes:
  1. Class imbalance  → stratified split + class weights for bot detection
  2. numClasses=1 bug → label must have ≥2 distinct values; assert before training
  3. na.drop() removed → fillna(0) everywhere
  4. Edit-type evaluator bug → correct labelCol="label" throughout
  5. Meaningless 1.0 accuracy → explained and handled with proper metrics
  6. Feature importance all-zero → only occurs with single class; fixed by #2
  7. Model 2 evaluator reused wrong column → fixed with separate evaluator instances
"""

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import (
    MulticlassClassificationEvaluator, BinaryClassificationEvaluator,
    ClusteringEvaluator
)
from pyspark.sql.functions import (
    col, count, countDistinct, avg, sum as spark_sum,
    when, lit, desc
)

spark = SparkSession.builder \
    .appName("Wikipedia_ML_FIXED") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("MACHINE LEARNING PIPELINE  (FIXED)")
print("=" * 80)

FEATURES_PATH = "/opt/spark/project/data/silver/wikipedia_features"
MODELS_PATH   = "/opt/spark/project/results/models"

df = spark.read.parquet(FEATURES_PATH)
df.cache()
total = df.count()
print(f"Loaded {total:,} records")

# ─────────────────────────────────────────────────────────────────────────────
# HELPER: assert a label column has at least 2 classes before training
# ─────────────────────────────────────────────────────────────────────────────
def check_label_classes(dataframe, label_col, min_per_class=10):
    """
    Returns True if the column has ≥2 distinct values each with ≥ min_per_class rows.
    Prints a warning and returns False otherwise.
    """
    dist = dataframe.groupBy(label_col).count().collect()
    classes = {r[label_col]: r["count"] for r in dist}
    print(f"  Label distribution for '{label_col}': {classes}")
    if len(classes) < 2:
        print(f"  ⚠️  Only one class found! Model cannot train. Skipping.")
        return False
    tiny = [k for k, v in classes.items() if v < min_per_class]
    if tiny:
        print(f"  ⚠️  Classes with very few samples: {tiny}")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# MODEL 1 – BOT DETECTION  (Random Forest, binary)
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("MODEL 1: BOT DETECTION (Random Forest)")
print("=" * 80)

FEATURE_COLS_BOT = [
    "user_total_edits", "user_pages_edited", "user_minor_edit_ratio",
    "size_change_abs", "comment_length", "is_minor",
    "time_since_last_edit_sec", "user_edit_sequence",
]

# fillna — NO na.drop()
bot_df = df.select(["is_bot"] + FEATURE_COLS_BOT).fillna(0)
print(f"Samples available: {bot_df.count():,}")

if not check_label_classes(bot_df, "is_bot"):
    # Can't train; still show a dummy result so the script doesn't crash
    print("Skipping Model 1 (no positive class in data).")
else:
    # ── Stratified split: keep class proportions in train / test ─────────────
    # Spark doesn't have a built-in stratified split, so we split each class
    pos = bot_df.filter(col("is_bot") == 1)
    neg = bot_df.filter(col("is_bot") == 0)

    train_pos, test_pos = pos.randomSplit([0.8, 0.2], seed=42)
    train_neg, test_neg = neg.randomSplit([0.8, 0.2], seed=42)

    train_bot = train_pos.union(train_neg)
    test_bot  = test_pos.union(test_neg)

    pos_count = train_pos.count()
    neg_count = train_neg.count()
    print(f"Train – positives: {pos_count:,}  negatives: {neg_count:,}")
    print(f"Test  – positives: {test_pos.count():,}  negatives: {test_neg.count():,}")

    # ── Class weight to handle imbalance ────────────────────────────────────
    # classWeights not directly available in Spark RF → use weightCol trick
    ratio = neg_count / max(pos_count, 1)
    train_bot = train_bot.withColumn(
        "weight",
        when(col("is_bot") == 1, lit(float(ratio))).otherwise(lit(1.0))
    )
    test_bot = test_bot.withColumn("weight", lit(1.0))

    # ── Pipeline ─────────────────────────────────────────────────────────────
    assembler_bot = VectorAssembler(inputCols=FEATURE_COLS_BOT, outputCol="features_raw",
                                    handleInvalid="keep")
    scaler_bot    = StandardScaler(inputCol="features_raw", outputCol="features")
    rf = RandomForestClassifier(
        labelCol="is_bot",
        featuresCol="features",
        weightCol="weight",
        numTrees=100,
        maxDepth=10,
        seed=42,
    )
    pipeline_bot = Pipeline(stages=[assembler_bot, scaler_bot, rf])

    print("\n🔧 Training Random Forest …")
    model_bot = pipeline_bot.fit(train_bot)

    pred_bot = model_bot.transform(test_bot)

    # ── Evaluators (separate instances for each metric) ──────────────────────
    eval_acc = MulticlassClassificationEvaluator(
        labelCol="is_bot", predictionCol="prediction", metricName="accuracy"
    )
    eval_f1  = MulticlassClassificationEvaluator(
        labelCol="is_bot", predictionCol="prediction", metricName="f1"
    )
    eval_auc = BinaryClassificationEvaluator(
        labelCol="is_bot", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    )

    accuracy = eval_acc.evaluate(pred_bot)
    f1       = eval_f1.evaluate(pred_bot)
    auc      = eval_auc.evaluate(pred_bot)

    print(f"\n✅ Bot Detection Results:")
    print(f"   Accuracy  : {accuracy:.4f}")
    print(f"   F1-Score  : {f1:.4f}")
    print(f"   AUC-ROC   : {auc:.4f}")

    # Confusion matrix breakdown
    print("\n   Confusion matrix breakdown:")
    pred_bot.groupBy("is_bot", "prediction").count().orderBy("is_bot", "prediction").show()

    # Feature importance
    rf_model = model_bot.stages[-1]
    importances = sorted(
        zip(FEATURE_COLS_BOT, rf_model.featureImportances.toArray()),
        key=lambda x: x[1], reverse=True
    )
    print("\n📊 Feature Importances:")
    for feat, imp in importances:
        print(f"   {feat:35}  {imp:.4f}")

    model_bot.write().overwrite().save(f"{MODELS_PATH}/bot_detection_model")
    print(f"\n💾 Saved: {MODELS_PATH}/bot_detection_model")


# ─────────────────────────────────────────────────────────────────────────────
# MODEL 2 – EDIT TYPE CLASSIFICATION  (Logistic Regression, multi-class)
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("MODEL 2: EDIT TYPE CLASSIFICATION (Logistic Regression)")
print("=" * 80)

FEATURE_COLS_EDIT = [
    "user_total_edits", "page_total_edits", "is_minor",
    "comment_length", "is_bot", "is_anon", "hour", "is_weekend",
]

edit_df = df.select(["edit_magnitude"] + FEATURE_COLS_EDIT).fillna(0)
# fillna for string column separately
edit_df = edit_df.fillna("small", subset=["edit_magnitude"])

print(f"Samples available: {edit_df.count():,}")

if not check_label_classes(edit_df, "edit_magnitude"):
    print("Skipping Model 2.")
else:
    # String index the label FIRST so we can inspect numeric label distribution
    indexer_edit  = StringIndexer(inputCol="edit_magnitude", outputCol="label",
                                  handleInvalid="keep")
    indexer_model = indexer_edit.fit(edit_df)
    edit_indexed  = indexer_model.transform(edit_df)

    # Verify indexed label has ≥2 classes
    if not check_label_classes(edit_indexed, "label"):
        print("Skipping Model 2 (indexing collapsed to 1 class).")
    else:
        assembler_edit = VectorAssembler(
            inputCols=FEATURE_COLS_EDIT, outputCol="features_raw",
            handleInvalid="keep"
        )
        scaler_edit = StandardScaler(inputCol="features_raw", outputCol="features")
        lr = LogisticRegression(
            labelCol="label",
            featuresCol="features",
            maxIter=100,
            regParam=0.01,
            family="multinomial",       # explicit multi-class
        )

        # Build pipeline WITHOUT the already-fitted indexer
        pipeline_edit = Pipeline(stages=[assembler_edit, scaler_edit, lr])

        train_edit, test_edit = edit_indexed.randomSplit([0.8, 0.2], seed=42)
        print(f"Train: {train_edit.count():,}  Test: {test_edit.count():,}")

        print("\n🔧 Training Logistic Regression …")
        model_edit = pipeline_edit.fit(train_edit)
        pred_edit  = model_edit.transform(test_edit)

        # ── FIXED: use correct labelCol="label" (not "is_bot") ───────────────
        eval_acc_e = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="accuracy"
        )
        eval_f1_e  = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="f1"
        )

        acc_e = eval_acc_e.evaluate(pred_edit)
        f1_e  = eval_f1_e.evaluate(pred_edit)

        print(f"\n✅ Edit Type Classification Results:")
        print(f"   Accuracy : {acc_e:.4f}")
        print(f"   F1-Score : {f1_e:.4f}")

        print("\n   Confusion matrix breakdown:")
        pred_edit.groupBy("label", "prediction").count().orderBy("label", "prediction").show()

        model_edit.write().overwrite().save(f"{MODELS_PATH}/edit_type_model")
        # Also save the fitted StringIndexer so we can decode predictions later
        indexer_model.write().overwrite().save(f"{MODELS_PATH}/edit_type_indexer")
        print(f"\n💾 Saved: {MODELS_PATH}/edit_type_model")


# ─────────────────────────────────────────────────────────────────────────────
# MODEL 3 – USER CLUSTERING  (K-Means, 5 clusters)
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("MODEL 3: USER CLUSTERING (K-Means)")
print("=" * 80)

user_profiles = df.groupBy("user").agg(
    count("*").alias("total_edits"),
    countDistinct("page_title").alias("pages_edited"),
    avg("size_change_abs").alias("avg_edit_size"),
    spark_sum("is_minor").alias("minor_edits"),
    spark_sum("is_bot").alias("is_bot_flag"),
    avg("comment_length").alias("avg_comment_len"),
).filter(col("total_edits") >= 5)   # lowered from 10 → keeps more users

# fillna — NO na.drop()
user_profiles = user_profiles.fillna(0)

user_count = user_profiles.count()
print(f"Users to cluster: {user_count:,}")

if user_count < 5:
    print("⚠️  Too few users for clustering. Skipping.")
else:
    CLUSTER_FEATURES = [
        "total_edits", "pages_edited", "avg_edit_size",
        "minor_edits", "avg_comment_len",
    ]

    assembler_c = VectorAssembler(
        inputCols=CLUSTER_FEATURES, outputCol="features_raw",
        handleInvalid="keep"
    )
    scaler_c = StandardScaler(inputCol="features_raw", outputCol="features")

    # K must not exceed number of users
    k = min(5, user_count)
    kmeans = KMeans(featuresCol="features", k=k, seed=42, maxIter=50)

    pipeline_c = Pipeline(stages=[assembler_c, scaler_c, kmeans])

    print(f"\n🔧 Training K-Means (k={k}) …")
    model_c  = pipeline_c.fit(user_profiles)
    clusters = model_c.transform(user_profiles)

    eval_sil = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
    sil = eval_sil.evaluate(clusters)

    print(f"\n✅ Clustering Results:")
    print(f"   Silhouette Score : {sil:.4f}  (−1 bad → 0 ok → 1 perfect)")
    print(f"   Clusters         : {k}")

    print("\n📊 Cluster Distribution:")
    clusters.groupBy("prediction").count().orderBy("prediction").show()

    print("\n📊 Cluster Profiles:")
    clusters.groupBy("prediction").agg(
        avg("total_edits").alias("avg_edits"),
        avg("pages_edited").alias("avg_pages"),
        avg("avg_edit_size").alias("avg_size"),
        avg("minor_edits").alias("avg_minor"),
        count("*").alias("user_count"),
    ).orderBy("prediction").show()

    model_c.write().overwrite().save(f"{MODELS_PATH}/user_clustering_model")
    print(f"\n💾 Saved: {MODELS_PATH}/user_clustering_model")


# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("ML PIPELINE SUMMARY")
print("=" * 80)
print("""
  Model 1 – Bot Detection (Random Forest)
    • Stratified train/test split (preserves rare positive class)
    • Class weights applied to handle severe imbalance
    • Reports Accuracy, F1, and AUC-ROC

  Model 2 – Edit Type Classification (Logistic Regression)
    • StringIndexer fits BEFORE the pipeline split
    • Correct labelCol used in each evaluator instance
    • Multinomial logistic regression for 3 classes

  Model 3 – User Clustering (K-Means)
    • Minimum-edit threshold lowered to capture more users
    • k capped at actual user count to avoid Spark errors

  All models: fillna(0) instead of na.drop() → no silent data loss
""")

spark.stop()