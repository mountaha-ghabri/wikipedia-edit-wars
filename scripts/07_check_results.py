from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

spark = SparkSession.builder.appName("Report_Generator").getOrCreate()

# These paths are now 100% verified from your ls -R output
MODELS = {
    "Bot Detection": "/opt/spark/project/results/models/bot_detection_model",
    "Edit Type": "/opt/spark/project/results/models/edit_type_model",
    "User Clustering": "/opt/spark/project/results/models/user_clustering_model"
}

print("\n" + "="*60)
print("             FINAL ML MODEL AUDIT REPORT")
print("="*60)

for name, path in MODELS.items():
    try:
        model = PipelineModel.load(path)
        # The actual AI model is the last stage (stage 2)
        actual_model = model.stages[2]
        print(f"✅ {name:15} | Status: LOADED | Type: {type(actual_model).__name__}")
    except Exception as e:
        print(f"❌ {name:15} | Status: ERROR  | Path: {path}")

print("="*60)
spark.stop()