"""
05_extract_kpis_FAST.py
Lightweight KPI extractor — no .cache(), sampled where possible, single-pass aggregations.
Won't hang or OOM on 11M rows with limited RAM.

Run:
    docker exec spark-master /opt/spark/bin/spark-submit \
        /opt/spark/project/scripts/05_extract_kpis_FAST.py
"""

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
    ClusteringEvaluator,
)
from pyspark.sql import functions as F
import json, os, time

spark = SparkSession.builder \
    .appName("KPI_Fast") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

SILVER   = "/opt/spark/project/data/silver/wikipedia_clean"
FEATURES = "/opt/spark/project/data/silver/wikipedia_features"
MODELS   = {
    "bot":        "/opt/spark/project/results/models/bot_detection_model",
    "edit_type":  "/opt/spark/project/results/models/edit_type_model",
    "clustering": "/opt/spark/project/results/models/user_clustering_model",
}
OUT_DIR  = "/opt/spark/project/results/kpis"
os.makedirs(OUT_DIR, exist_ok=True)

kpis = {}

def banner(msg):
    print(f"\n{'='*60}\n  {msg}\n{'='*60}", flush=True)

def safe(val, decimals=4):
    try:
        return round(float(val), decimals)
    except Exception:
        return None

# ─────────────────────────────────────────────────────────────────────────────
# 0. DATASET KPIs — single aggregation pass, no cache
# ─────────────────────────────────────────────────────────────────────────────
banner("0. DATASET KPIs  (single-pass aggregation)")
t0 = time.time()

silver = spark.read.parquet(SILVER)

# Everything in ONE agg call — one scan of the data
agg_row = silver.agg(
    F.count("*").alias("total_edits"),
    F.countDistinct("page_title").alias("total_pages"),
    F.countDistinct("user").alias("total_users"),
    F.sum(F.col("bot").cast("int")).alias("bot_edits"),
    F.sum(F.col("anon").cast("int")).alias("anon_edits"),
    F.sum(F.col("minor").cast("int")).alias("minor_edits"),
    F.avg("size").alias("avg_edit_size"),
).first()

total = agg_row["total_edits"]
print(f"  total_edits  : {total:,}")
print(f"  total_pages  : {agg_row['total_pages']:,}")
print(f"  total_users  : {agg_row['total_users']:,}")
print(f"  bot_edits    : {agg_row['bot_edits']:,}  ({agg_row['bot_edits']/total*100:.2f}%)")

dataset_kpis = {
    "total_edits":  int(agg_row["total_edits"]),
    "total_pages":  int(agg_row["total_pages"]),
    "total_users":  int(agg_row["total_users"]),
    "bot_edits":    int(agg_row["bot_edits"]),
    "anon_edits":   int(agg_row["anon_edits"]),
    "minor_edits":  int(agg_row["minor_edits"]),
    "bot_pct":      safe(agg_row["bot_edits"] / total * 100, 2),
    "anon_pct":     safe(agg_row["anon_edits"] / total * 100, 2),
    "minor_pct":    safe(agg_row["minor_edits"] / total * 100, 2),
    "avg_edit_size": safe(agg_row["avg_edit_size"], 1),
}

# Top 10 pages — separate scan but fast groupBy
top_pages = (silver.groupBy("page_title").count()
             .orderBy(F.desc("count")).limit(10).collect())
dataset_kpis["top_10_pages"] = [{"page": r.page_title, "edits": r["count"]} for r in top_pages]
print(f"  top page: {top_pages[0].page_title} ({top_pages[0]['count']:,} edits)")

# Top 10 users
top_users = (silver.groupBy("user").count()
             .orderBy(F.desc("count")).limit(10).collect())
dataset_kpis["top_10_users"] = [{"user": r.user, "edits": r["count"]} for r in top_users]

# Edits by hour
hourly = (silver.withColumn("hour", F.hour("timestamp"))
                .groupBy("hour").count()
                .orderBy("hour").collect())
dataset_kpis["edits_by_hour"] = [{"hour": r.hour, "edits": r["count"]} for r in hourly]

kpis["dataset"] = dataset_kpis
print(f"  Done in {time.time()-t0:.1f}s")

# ─────────────────────────────────────────────────────────────────────────────
# Load a SAMPLE of features for ML eval — 10% max, no cache
# ─────────────────────────────────────────────────────────────────────────────
banner("Loading 10% feature sample for ML eval")
t0 = time.time()

# Use a 10% sample — fast to load, enough for reliable metrics
feat_sample = spark.read.parquet(FEATURES).sample(0.10, seed=42)
feat_count  = feat_sample.count()
print(f"  Sample rows: {feat_count:,}")
print(f"  Done in {time.time()-t0:.1f}s")

# ─────────────────────────────────────────────────────────────────────────────
# 1. BOT DETECTION KPIs
# ─────────────────────────────────────────────────────────────────────────────
banner("1. BOT DETECTION")
t0 = time.time()

try:
    bot_model = PipelineModel.load(MODELS["bot"])

    # Use stratified mini-sample: up to 50k pos + 50k neg
    all_feats = spark.read.parquet(FEATURES)
    pos = all_feats.filter(F.col("is_bot") == 1).limit(50_000)
    neg = all_feats.filter(F.col("is_bot") == 0).limit(50_000)
    eval_bot = pos.union(neg)

    preds = bot_model.transform(eval_bot)

    auc  = BinaryClassificationEvaluator(
        labelCol="is_bot", rawPredictionCol="rawPrediction",
        metricName="areaUnderROC").evaluate(preds)
    aupr = BinaryClassificationEvaluator(
        labelCol="is_bot", rawPredictionCol="rawPrediction",
        metricName="areaUnderPR").evaluate(preds)

    mc = MulticlassClassificationEvaluator(labelCol="is_bot", predictionCol="prediction")
    acc  = mc.evaluate(preds, {mc.metricName: "accuracy"})
    f1   = mc.evaluate(preds, {mc.metricName: "f1"})
    prec = mc.evaluate(preds, {mc.metricName: "weightedPrecision"})
    rec  = mc.evaluate(preds, {mc.metricName: "weightedRecall"})

    cm = preds.groupBy("is_bot", "prediction").count().collect()
    cm_dict = {f"act{int(r.is_bot)}_pred{int(r.prediction)}": r["count"] for r in cm}

    rf = bot_model.stages[-1]
    assembler = bot_model.stages[0]
    feat_names = assembler.getInputCols()
    importances = rf.featureImportances.toArray().tolist()
    top_feats = sorted(zip(feat_names, importances), key=lambda x: -x[1])[:10]

    kpis["bot_detection"] = {
        "auc_roc":       safe(auc),
        "auc_pr":        safe(aupr),
        "accuracy":      safe(acc),
        "f1_score":      safe(f1),
        "precision":     safe(prec),
        "recall":        safe(rec),
        "confusion_matrix": cm_dict,
        "top_features":  [{"feature": n, "importance": safe(v, 5)} for n, v in top_feats],
    }
    print(f"  AUC-ROC={auc:.4f}  AUC-PR={aupr:.4f}  Acc={acc:.4f}  F1={f1:.4f}")
    print(f"  Top feature: {top_feats[0]}")
    print(f"  Done in {time.time()-t0:.1f}s")

except Exception as e:
    import traceback; traceback.print_exc()
    kpis["bot_detection"] = {"error": str(e)}

# ─────────────────────────────────────────────────────────────────────────────
# 2. EDIT TYPE KPIs
# ─────────────────────────────────────────────────────────────────────────────
banner("2. EDIT TYPE CLASSIFICATION")
t0 = time.time()

try:
    edit_model = PipelineModel.load(MODELS["edit_type"])
    preds2 = edit_model.transform(feat_sample)

    mc2 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    acc2  = mc2.evaluate(preds2, {mc2.metricName: "accuracy"})
    f1_2  = mc2.evaluate(preds2, {mc2.metricName: "f1"})
    prec2 = mc2.evaluate(preds2, {mc2.metricName: "weightedPrecision"})
    rec2  = mc2.evaluate(preds2, {mc2.metricName: "weightedRecall"})

    pred_dist = preds2.groupBy("prediction").count().orderBy("prediction").collect()
    cm2 = preds2.groupBy("label", "prediction").count().collect()
    cm2_dict = {f"act{int(r.label)}_pred{int(r.prediction)}": r["count"] for r in cm2}

    kpis["edit_type"] = {
        "accuracy":    safe(acc2),
        "f1_score":    safe(f1_2),
        "precision":   safe(prec2),
        "recall":      safe(rec2),
        "confusion_matrix": cm2_dict,
        "prediction_distribution": [
            {"class": int(r.prediction), "count": r["count"]} for r in pred_dist
        ],
    }
    print(f"  Acc={acc2:.4f}  F1={f1_2:.4f}  Prec={prec2:.4f}  Rec={rec2:.4f}")
    print(f"  Done in {time.time()-t0:.1f}s")

except Exception as e:
    import traceback; traceback.print_exc()
    kpis["edit_type"] = {"error": str(e)}

# ─────────────────────────────────────────────────────────────────────────────
# 3. USER CLUSTERING KPIs
# ─────────────────────────────────────────────────────────────────────────────
banner("3. USER CLUSTERING")
t0 = time.time()

try:
    cluster_model = PipelineModel.load(MODELS["clustering"])

    # Build user features from the sample
    user_feats = (feat_sample.groupBy("user").agg(
        F.count("*").alias("total_edits"),
        F.avg("size_bytes").alias("avg_edit_size"),
        F.sum(F.col("is_bot").cast("int")).alias("bot_edit_count"),
        F.sum(F.col("is_minor").cast("int")).alias("minor_edit_count"),
        F.avg("edits_last_hour").alias("avg_hourly_rate"),
        F.countDistinct("page_title").alias("unique_pages"),
    ).filter(F.col("total_edits") >= 3).fillna(0))

    preds3 = cluster_model.transform(user_feats)

    # Silhouette — try both common feature col names
    sil = None
    for fcol in ["scaledFeatures", "features"]:
        try:
            sil = ClusteringEvaluator(
                featuresCol=fcol, predictionCol="prediction"
            ).evaluate(preds3)
            break
        except Exception:
            pass

    cluster_sizes = preds3.groupBy("prediction").count().orderBy("prediction").collect()

    cluster_stats = (preds3.groupBy("prediction").agg(
        F.count("*").alias("size"),
        F.avg("total_edits").alias("avg_edits"),
        F.avg("avg_edit_size").alias("avg_size"),
        F.avg("unique_pages").alias("avg_pages"),
        F.avg("bot_edit_count").alias("avg_bot_edits"),
    ).orderBy("prediction").collect())

    kmeans = cluster_model.stages[-1]
    centers = kmeans.clusterCenters()

    kpis["user_clustering"] = {
        "silhouette_score": safe(sil) if sil is not None else "n/a",
        "num_clusters":     len(centers),
        "cluster_sizes":    [{"cluster": int(r.prediction), "size": r["count"]}
                             for r in cluster_sizes],
        "cluster_profiles": [
            {
                "cluster":       int(r.prediction),
                "size":          int(r["size"]),
                "avg_edits":     safe(r.avg_edits, 2),
                "avg_edit_size": safe(r.avg_size, 1),
                "avg_pages":     safe(r.avg_pages, 2),
                "avg_bot_edits": safe(r.avg_bot_edits, 2),
            }
            for r in cluster_stats
        ],
    }
    print(f"  Silhouette={sil:.4f}  k={len(centers)}" if sil else f"  k={len(centers)}")
    for r in cluster_stats:
        print(f"    C{int(r.prediction)}: {int(r['size']):>6,} users | "
              f"avg_edits={float(r.avg_edits):.1f} | avg_size={float(r.avg_size):.0f}")
    print(f"  Done in {time.time()-t0:.1f}s")

except Exception as e:
    import traceback; traceback.print_exc()
    kpis["user_clustering"] = {"error": str(e)}

# ─────────────────────────────────────────────────────────────────────────────
# 4. EDIT WAR / REVERT KPIs
# ─────────────────────────────────────────────────────────────────────────────
banner("4. EDIT WAR KPIs")
t0 = time.time()

try:
    all_feats = spark.read.parquet(FEATURES)
    ew = {}

    if "is_revert_candidate" in all_feats.columns:
        rev_agg = all_feats.agg(
            F.count("*").alias("total"),
            F.sum(F.col("is_revert_candidate").cast("int")).alias("reverts"),
        ).first()
        ew["total_revert_candidates"] = int(rev_agg["reverts"])
        ew["revert_rate"] = safe(rev_agg["reverts"] / rev_agg["total"], 4)

        top_rev = (all_feats.filter(F.col("is_revert_candidate") == 1)
                   .groupBy("page_title").count()
                   .orderBy(F.desc("count")).limit(10).collect())
        ew["top_revert_pages"] = [{"page": r.page_title, "reverts": r["count"]}
                                  for r in top_rev]
        print(f"  Revert rate: {ew['revert_rate']*100:.2f}%  "
              f"({ew['total_revert_candidates']:,} candidates)")
    else:
        ew["note"] = "is_revert_candidate not in features"
        print("  Column not found — skipped")

    # Edit velocity per page (top controversial pages)
    velocity = (all_feats.groupBy("page_title").agg(
        F.count("*").alias("edits"),
        F.min("timestamp").alias("first"),
        F.max("timestamp").alias("last"),
    ).withColumn("days",
        (F.unix_timestamp("last") - F.unix_timestamp("first")) / 86400.0
    ).filter(F.col("days") > 1)
     .withColumn("epd", F.col("edits") / F.col("days"))
     .orderBy(F.desc("epd")).limit(10).collect())

    ew["top_velocity_pages"] = [
        {"page": r.page_title,
         "edits_per_day": safe(r.epd, 1),
         "total_edits": int(r.edits)}
        for r in velocity
    ]
    kpis["edit_wars"] = ew
    print(f"  Done in {time.time()-t0:.1f}s")

except Exception as e:
    import traceback; traceback.print_exc()
    kpis["edit_wars"] = {"error": str(e)}

# ─────────────────────────────────────────────────────────────────────────────
# Save
# ─────────────────────────────────────────────────────────────────────────────
banner("Saving KPIs")

out_path = os.path.join(OUT_DIR, "all_kpis.json")
with open(out_path, "w") as f:
    json.dump(kpis, f, indent=2, default=str)
print(f"  Saved → {out_path}")

# ── Summary card ─────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("  KPI SUMMARY CARD")
print("="*60)
d = kpis.get("dataset", {})
print(f"  Dataset      : {d.get('total_edits',0):>12,} edits | "
      f"{d.get('total_pages',0):,} pages | "
      f"{d.get('total_users',0):,} users")
print(f"               : {d.get('bot_pct',0):.2f}% bot | "
      f"{d.get('anon_pct',0):.2f}% anon | "
      f"{d.get('minor_pct',0):.2f}% minor")

b = kpis.get("bot_detection", {})
if "auc_roc" in b:
    print(f"  Bot Detect   : AUC-ROC={b['auc_roc']}  AUC-PR={b['auc_pr']}  "
          f"F1={b['f1_score']}  Acc={b['accuracy']}")

e = kpis.get("edit_type", {})
if "accuracy" in e:
    print(f"  Edit Type    : Acc={e['accuracy']}  F1={e['f1_score']}  "
          f"Prec={e['precision']}  Rec={e['recall']}")

c = kpis.get("user_clustering", {})
if "silhouette_score" in c:
    print(f"  Clustering   : Silhouette={c['silhouette_score']}  k={c['num_clusters']}")

w = kpis.get("edit_wars", {})
if "revert_rate" in w:
    print(f"  Edit Wars    : {w['revert_rate']*100:.2f}% revert rate  "
          f"({w['total_revert_candidates']:,} candidates)")
print("="*60)

spark.stop()
print("\nDone. KPIs saved to:", out_path)