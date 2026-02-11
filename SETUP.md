# Wikipedia Edit Wars - Spark Pipeline

## 📋 Project Overview

This project implements a **distributed data processing pipeline** using Apache Spark to analyze and detect edit wars on Wikipedia. It processes 10M+ records through multiple ETL stages, performs feature engineering, applies machine learning models, and includes real-time streaming simulation with performance testing and visualization.

---

## 🏗️ Architecture & Pipeline Stages

### **Stage 1: Data Collection** (`01_collect_data.py`)

- **Purpose**: Gather raw Wikipedia edit history data
- **Scale**: 10M+ records
- **Duration**: 2-4 hours
- **Output**: Bronze layer (raw data)
- **Tools**: Python requests library for Wikipedia API calls

### **Stage 2: Bronze to Silver ETL** (`02_bronze_to_silver.py`)

- **Purpose**: Clean and transform raw data
- **Transformations**:
  - Remove duplicates
  - Validate data types
  - Handle missing values
  - Standardize formats
- **Output**: Silver layer (cleaned, structured data)
- **Execution**: Spark Submit (distributed processing)

### **Stage 3: Feature Engineering** (`03_feature_engineering.py`)

- **Purpose**: Create meaningful features for ML models
- **Features Generated**:
  - Edit frequency metrics
  - User interaction patterns
  - Temporal features (time-based patterns)
  - Edit conflict indicators
- **Output**: Gold layer (feature-enriched data)
- **Execution**: Spark Submit (distributed processing)

### **Stage 4: Machine Learning Models** (`04_ml_models.py`)

- **Purpose**: Build and train predictive models
- **Models**: Scikit-learn integrated with Spark
- **Tasks**:
  - Classification (edit war detection)
  - Pattern recognition
  - Anomaly detection
- **Output**: Trained models + predictions
- **Execution**: Spark Submit

### **Stage 5: Streaming Simulation** (`05_streaming.py`)

- **Purpose**: Simulate real-time streaming data processing
- **Use Case**: Live edit war detection
- **Technology**: Spark Structured Streaming
- **Execution**: Spark Submit (background process)

### **Stage 6: Performance Testing** (`06_performance_tests.py`)

- **Purpose**: Benchmark pipeline performance
- **Metrics**:
  - Processing throughput
  - Latency measurements
  - Resource utilization
  - Query optimization analysis
- **Output**: Performance reports
- **Execution**: Spark Submit

### **Stage 7: Visualization** (`07_visualizations.py`)

- **Purpose**: Generate insights and reports
- **Library**: Plotly (interactive visualizations)
- **Outputs**:
  - Edit war trends
  - User behavior patterns
  - Model performance dashboards
  - Streaming metrics
- **Format**: HTML/Interactive dashboards

---

## 🛠️ Technology Stack

| Component                       | Technology              | Justification                                                                       |
| ------------------------------- | ----------------------- | ----------------------------------------------------------------------------------- |
| **Big Data Processing**   | Apache Spark            | Distributed processing of 10M+ records; handles complex transformations efficiently |
| **Orchestration**         | Docker & Docker Compose | Consistent environment across machines; easy multi-node Spark cluster setup         |
| **Data Cleaning**         | Python + Pandas         | Powerful DataFrame operations; integrated with Spark for ETL                        |
| **ML Framework**          | Scikit-learn            | Battle-tested algorithms; easy integration with Spark pipelines                     |
| **Visualization**         | Plotly                  | Interactive, web-based dashboards; better than static matplotlib for exploration    |
| **API Calls**             | Python Requests         | Lightweight, reliable HTTP client for Wikipedia API                                 |
| **Dependency Management** | `.deps/` folder       | Centralized dependency isolation; ensures reproducibility                           |

---

## 📊 Data Flow (Medallion Architecture)

```
┌─────────────────────────────────────────────────────────────┐
│ BRONZE LAYER (Raw Data)                                      │
│ - Wikipedia API responses                                    │
│ - Unstructured, unvalidated                                 │
│ - 10M+ records                                              │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│ SILVER LAYER (Cleaned & Transformed)                        │
│ - Deduplicated, validated data                              │
│ - Consistent schema and formats                             │
│ - Ready for analysis                                        │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│ GOLD LAYER (Feature-Enriched Data)                          │
│ - Engineered features                                       │
│ - ML-ready datasets                                         │
│ - Business insights                                         │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│ ANALYSIS & INTELLIGENCE LAYER                               │
│ - ML Predictions & Classifications                          │
│ - Performance Metrics                                       │
│ - Visualizations & Reports                                  │
└─────────────────────────────────────────────────────────────┘
```

---

## ⚡ Why Spark for This Project?

1. **Scale**: Efficiently processes 10M+ Wikipedia records
2. **Distributed Processing**: Parallelizes computation across worker nodes
3. **Ecosystem**: Integrates with Pandas, Scikit-learn, and streaming
4. **Performance**: In-memory caching for iterative algorithms
5. **Fault-Tolerance**: Handles node failures gracefully
6. **Flexibility**: Supports both batch and streaming workloads

---

## 🚀 Execution Flow

```bash
# Start Docker services (Spark master + worker nodes)
docker-compose up -d

# Install dependencies in containers
pip install --target /opt/spark/project/.deps [packages]

# Execute pipeline stages sequentially (except streaming - runs in background)
01_collect_data.py       → 02_bronze_to_silver.py   → 03_feature_engineering.py
      ↓                          ↓                           ↓
   (2-4 hrs)             (Data Cleaning)          (Feature Creation)
                                                          ↓
                    Combined with ML ← ← ← ← ← ← ← ← ← ←
                         ↓
      04_ml_models.py  &  05_streaming.py  &  06_performance_tests.py
           ↓                    ↓                      ↓
      (Training)         (Live Streaming)         (Benchmarking)
                                ↓
                    07_visualizations.py
                         ↓
                   (HTML Dashboards)
```

---

## 📁 Project Structure

```
sleeptrial/
├── RUN_PROJECT.bat              # Main orchestration script (Windows)
├── docker-compose.yml           # Spark cluster configuration
├── requirements.txt             # Python dependencies
├── SETUP.md                     # Installation guide
├── .deps/                       # Local Python packages
│   ├── numpy/
│   ├── pandas/
│   ├── scikit-learn/
│   ├── plotly/
│   └── ...
├── config/                      # Configuration files
├── data/                        # Data storage (Bronze/Silver layers)
├── results/                     # Output results & models
└── scripts/
    ├── 01_collect_data.py       # Data collection
    ├── 02_bronze_to_silver.py   # Data cleaning
    ├── 03_feature_engineering.py # Feature creation
    ├── 04_ml_models.py          # ML training & prediction
    ├── 05_streaming.py          # Real-time streaming
    ├── 06_performance_tests.py  # Benchmarking
    └── 07_visualizations.py     # Reporting & dashboards
```

---

## 🔧 Key Dependencies

| Package                | Version | Purpose                        |
| ---------------------- | ------- | ------------------------------ |
| **numpy**        | 1.24.4  | Numerical computations for ML  |
| **pandas**       | 2.0.3   | DataFrames & data manipulation |
| **scikit-learn** | Latest  | ML algorithms & preprocessing  |
| **plotly**       | Latest  | Interactive visualizations     |
| **narwhals**     | 1.42.1  | DataFrame compatibility layer  |
| **requests**     | Latest  | HTTP client for API calls      |

---

## 📊 Expected Outputs

1. **Bronze Data**: Raw Wikipedia edit records
2. **Silver Data**: Cleaned, deduplicated dataset
3. **Gold Data**: Feature-enriched dataset
4. **ML Models**: Serialized models for edit war detection
5. **Predictions**: Classification results on test data
6. **Performance Reports**: Throughput, latency, resource metrics
7. **Visualizations**: Interactive Plotly dashboards

---

## ⏱️ Execution Timeline

| Stage                  | Approx Duration     | Notes                            |
| ---------------------- | ------------------- | -------------------------------- |
| 1. Data Collection     | 2-4 hours           | API rate limiting applies        |
| 2. Bronze → Silver    | 15-30 min           | Depends on data size             |
| 3. Feature Engineering | 30-60 min           | Transformations on full dataset  |
| 4. ML Training         | 30-120 min          | Hyperparameter tuning if enabled |
| 5. Streaming (async)   | Continuous          | Runs in background               |
| 6. Performance Tests   | 10-20 min           | Focused benchmarking             |
| 7. Visualizations      | 5-10 min            | Dashboard generation             |
| **Total**        | **4-6 hours** | Including data collection        |

---

## 🎯 Use Cases

### **Edit War Detection**

- Identify Wikipedia pages with frequent revert cycles
- Flag controversial topics requiring moderation

### **User Behavior Analysis**

- Profile editor patterns and conflict propensity
- Detect vandalism vs. legitimate disagreements

### **Trend Analysis**

- Track edit activity over time
- Identify hot topics and emerging conflicts

### **Real-time Monitoring**

- Stream processing for live edit war alerts
- Dashboard for community managers

---

## 🔐 Design Justifications

### **Why Docker?**

- Ensures reproducibility across environments
- Simplifies multi-node Spark cluster setup
- Isolates dependencies (`.deps/` folder)

### **Why Medallion Architecture (Bronze/Silver/Gold)?**

- **Separation of concerns**: Raw vs. processed data
- **Data lineage**: Track transformations
- **Scalability**: Each layer can be optimized independently
- **Reusability**: Silver layer feeds multiple gold layers

### **Why Spark instead of pandas-only?**

- Pandas limited to single-machine memory
- Spark handles horizontal scaling
- Better performance for 10M+ records
- Built-in streaming support

### **Why Plotly for visualization?**

- Interactive dashboards (better UX than static plots)
- Web-based (no matplotlib display needed in Docker)
- Publication-quality output

---

## 📝 Related Files

- [Setup Instructions](SETUP.md) - Installation and configuration
- [Requirements](requirements.txt) - Python package versions
- [Docker Configuration](docker-compose.yml) - Spark cluster setup

---

**Last Updated**: 2024
**Project Type**: Distributed Data Processing Pipeline
**Status**: Production-Ready
