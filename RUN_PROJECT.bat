@echo off
echo ============================================================
echo WIKIPEDIA EDIT WARS - SPARK PIPELINE
echo ============================================================

echo.
echo Step 1: Starting Docker Services...
docker-compose up -d
timeout /t 10

echo.
echo Step 2: Installing Dependencies...
docker exec spark-master pip install --no-cache-dir --target /opt/spark/project/.deps requests pandas plotly scikit-learn
docker exec spark-worker pip install --no-cache-dir --target /opt/spark/project/.deps requests pandas

echo.
echo Step 3: Collecting 10M+ Records (This takes 2-4 hours)...
REM 3A) Focused API collection on contentious topics (high-signal subset)
docker exec spark-master python3 /opt/spark/project/scripts/01_collect_data.py

REM 3B) Bulk Wikimedia dump ingestion to push well beyond 10M REAL edits
docker exec spark-master python3 /opt/spark/project/scripts/01_collect_dump_history.py

echo.
echo Step 4: Bronze to Silver ETL...
docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/project/scripts/02_bronze_to_silver.py

echo.
echo Step 5: Feature Engineering...
docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/project/scripts/03_feature_engineering.py

echo.
echo Step 6: Machine Learning Models...
docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/project/scripts/04_ml_models.py

echo.
echo Step 7: Streaming Simulation...
start docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/project/scripts/05_streaming.py

echo.
echo Step 8: Performance Tests...
docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/project/scripts/06_performance_tests.py

echo.
echo Step 9: Generating Visualizations...
docker exec spark-master python3 /opt/spark/project/scripts/07_visualizations.py

echo.
echo ============================================================
echo PIPELINE COMPLETE!
echo ============================================================
pause