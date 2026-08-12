# Distributed Analysis of Kubernetes Microservice Logs for Failure Detection

**Academic Project - Data Intensive & Scalable Systems Module**

A fully Dockerized, end-to-end pipeline using Apache Spark to analyse microservice telemetry
for cross-service failure propagation, anomaly detection, and scalability evaluation.

---

## Research Questions

| RQ  | Question                                                                     | Module                        |
|-----|------------------------------------------------------------------------------|-------------------------------|
| RQ1 | How can distributed analysis identify cross-service failure propagation?     | `cross_service_analysis.py`   |
| RQ2 | How effectively can distributed processing identify abnormal failure patterns? | `failure_detection.py`      |
| RQ3 | How does distributed Spark processing scale with increasing data volumes?   | `scalability_analysis.py`      |

---

## Architecture

```
┌──────────┐     ┌─────────────────┐     ┌────────────────────────────┐
│  Dataset │ --> │  MinIO (Blob)   │ --> │  Apache Spark Cluster      │
│  (CSV)   │     │  s3a://bucket/  │     │  ┌──────────┬────────────┐ │
└──────────┘     └─────────────────┘     │  │  Master  │ Worker × N │ │
                                         │  └──────────┴────────────┘ │
                                         └───────────┬────────────────┘
                                                     │
                    ┌────────────────────────────────┼──────────────────────────────┐
                    │                                                              │
                    │  ┌──────────────────┐   ┌──────────────────┐                  │
                    │  │  Preprocessing   │   │ Cross-Service    │                  │
                    │  │  (Clean, Join,   │-->│ Analysis (RQ1)   │                  │
                    │  │   Features)      │   └───────┬-─────────┘                  │
                    │  └──────────────────┘           │                            │
                    │                                 |                          │
                    │                    ┌──────────────────┐                      │
                    │                    │ Failure Det.     │                      │
                    │                    │ Anomalies (RQ2)  │                      │
                    │                    └────────┬─────────┘                      │
                    │                             │                                │
                    │              ┌──────────────┼──────────────┐                 │
                    │              |              |              |                 │
                    │  ┌──────────────────┐  ┌──────────┐  ┌──────────────┐        │
                    │  │ Scalability      │  │PostgreSQL│  │Visualization │        │
                    │  │ Experiments(RQ3) │  │(Results) │  │(Plots)        │       │
                    │  └──────────────────┘  └──────────┘  └──────────────┘       │
                    └─────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
project-root/
│
|-- Infrastructure & Deployment
│   |-- Docker
│   |-- Docker Compose
│   |-- Environment Configuration
│
|-- Configuration
│   |-- Pipeline Configuration
│   |-- Spark Configuration
│
|-- Application Modules
│   |-- Shared Infrastructure
│   │   |-- Utilities & Logging
│   │   |-- Database Abstraction
│   │
│   |-- Data Management
│   │   |-- Data Seeding
│   │   |- Data Ingestion
│   │
│   |-- Data Processing
│   │   |- Preprocessing & Feature Engineering
│   │
│   |-- Research Analysis
│   │   |-- Cross-Service Propagation Analysis
│   │   |-- Failure & Anomaly Detection
│   │   |-- Scalability Analysis
│   │
│   |-- Visualization
│   │   -- Charts & Plot Generation
│   │
│   |-- Dashboard
│   │   |-- Interactive Streamlit Dashboard
│   │
│  
│
|-- Automation & Scripts
│   |-- Application Entrypoints
│   |-- Pipeline Execution
│   |--Dataset Management
│   |-- Visualization Utilities
│
|-- Database
│   |-- PostgreSQL Schema & Initialization
│

```

---

## Module Summary

| # | Module                      | Purpose                                                  | RQ       | Output                              |
|---|-----------------------------|----------------------------------------------------------|----------|-------------------------------------|
| 1 | `ingestion.py`              | Generate/upload sample data to MinIO blob storage        | -        | MinIO: `s3a://bucket/raw/*.csv`     |
| 2 | `preprocessing.py`          | Spark-based cleaning, joining, feature engineering       | RQ1,2,3  | MinIO + PostgreSQL: unified Parquet |
| 3 | `cross_service_analysis.py` | Service dependency graph, failure propagation chains     | RQ1      | PostgreSQL: `cross_service_pairs`   |
| 4 | `failure_detection.py`      | Z-score anomaly detection, failure pattern clustering    | RQ2      | PostgreSQL: `anomaly_scores`        |
| 5 | `scalability_analysis.py`   | Benchmark pipeline at scale (100K–10M+ rows)             | RQ3      | PostgreSQL: `scalability_metrics`   |
| 6 | `visualization.py`          | Generate plots: heatmaps, time series, scaling curves    | RQ1,2,3  | PNG files in `/output/`             |

---

## Quick Start - Choose Your Launch Mode

Three ways to run the dashboard, from lightest to heaviest:

### Mode 1: SQLite Local (Zero Dependencies)

No Docker, no PostgreSQL, no Spark. Everything runs in-process with a SQLite database.

```bash
pip install -r requirements.txt
python run_streamlit.py --local --browser
```

Or via Makefile:
```bash
make dash-sqlite
```

**What happens:** Creates a `dashboard.db` file, seeds it with 5,000 rows of realistic telemetry data across all 7 tables, generates placeholder plots, and launches the dashboard at http://localhost:8501.

**Best for:** Quick demos, development, offline use, CI/testing.

---

### Mode 2: PostgreSQL Local (Lightweight)

Uses Docker only for PostgreSQL. The dashboard connects to it directly - no Spark or MinIO needed.

```bash
# Start PostgreSQL
docker compose up -d postgres

# Seed data + launch dashboard
python run_streamlit.py --browser
```

Or via Makefile:
```bash
make dash-local
```

**What happens:** Connects to PostgreSQL at `localhost:5432`, creates the schema, seeds all tables with realistic sample data, and launches the dashboard.

**Best for:** Testing the PostgreSQL backend, preparing for the full pipeline, connecting to an existing PG instance.

**Custom PG credentials:**
```bash
python run_streamlit.py --pg-host myhost --pg-user myuser --pg-pass mypass --pg-db mydb --browser
```

---

### Mode 3: Full Docker Pipeline (Production)

Runs everything - MinIO, Spark Master + Workers, PostgreSQL, and the full 6-module pipeline.

**Prerequisites:** Docker & Docker Compose v2+, 8 GB RAM, 10 GB disk.

**Quick-test** (1 worker, 100K rows, ~2 min):
```bash
make dev
```

**Production** (4 workers, 1M rows, full scalability experiments):
```bash
make prod
```

**Dashboard only** (after pipeline has run):
```bash
make dash
```

This starts:
- **MinIO** (blob storage) on ports `9000`/`9001`
- **Spark Master** on port `8080` (Web UI)
- **Spark Workers** connected to master
- **PostgreSQL** on port `5432`
- **Pipeline Runner** - executes all 6 modules automatically

The `docker-compose.override.yml` auto-merges with `docker-compose.yml` to apply smaller dev settings. To skip the override, use `-f docker-compose.yml` explicitly.

**Watch progress:**
```bash
docker-compose logs -f pipeline
```

**View results:**

| What                   | Where                                              |
|------------------------|----------------------------------------------------|
| **Streamlit Dashboard** | http://localhost:8501                               |
| **Plots**              | `./output/*.png` (on your machine)                 |
| **MinIO Console**      | http://localhost:9001 (minioadmin/minioadmin)      |
| **Spark Master UI**    | http://localhost:8080                              |
| **PostgreSQL**         | `localhost:5432` (sparkuser/sparkpass/microservice_analysis) |

**Query PostgreSQL:**
```bash
docker exec -it postgres psql -U sparkuser -d microservice_analysis
```

**Scale workers:**
```bash
docker-compose up -d --scale spark-worker=4
```

**Stop everything:**
```bash
docker-compose down
```

---

## Running Individual Modules

You can run specific modules inside the pipeline container:

```bash
# Run only ingestion
docker-compose run --rm pipeline python -m modules.ingestion

# Run only preprocessing
docker-compose run --rm pipeline python -m modules.preprocessing

# Run only scalability (RQ3) with custom sizes
docker-compose run --rm -e SAMPLE_DATA_ROWS=500000 pipeline python -m modules.scalability_analysis

# Run only visualization
docker-compose run --rm pipeline python -m modules.visualization
```

---

## Configuration

Edit `config/config.yaml` to adjust:

- **Ingestion**: Bucket name, file list
- **Preprocessing**: Null threshold, output format
- **Cross-Service Analysis**: Window duration, correlation threshold
- **Failure Detection**: Z-score threshold, error rate threshold
- **Scalability**: Data sizes, worker configs, repetitions
- **Visualization**: Output directory, DPI, color palette

---

## Database Tables

| Table                  | Module | Description                                       |
|------------------------|--------|---------------------------------------------------|
| `raw_telemetry`        | 1      | Raw ingested traces (sample)                      |
| `processed_telemetry`  | 2      | Cleaned, joined, feature-engineered data          |
| `cross_service_pairs`  | 3      | Service-to-service propagation scores (RQ1)       |
| `propagation_chains`   | 3      | Temporal failure cascade chains (RQ1)             |
| `error_correlations`   | 3      | Pairwise error-rate correlations (RQ1)            |
| `anomaly_scores`       | 4      | Per-bucket anomaly flags & scores (RQ2)           |
| `failure_patterns`     | 4      | Pattern type distribution per service (RQ2)       |
| `scalability_metrics`  | 5      | Per-size benchmark timings & speed-up (RQ3)       |

---

## Scalability Experiments (RQ3)

The pipeline tests these data sizes by default:

| Rows       | Purpose     |
|------------|-------------|
| 1,000,000  | Baseline    |
| 5,000,000  | 5× growth   |
| 10,000,000 | 10× growth  |
| 25,000,000 | 25× growth  |
| 50,000,000 | 50× growth  |

Each benchmark records:
- GroupBy + Aggregation time
- Window function (lag) time
- Join operation time
- Full shuffle time
- Total execution time

From these, we compute:
- **Speed-up** (S = T_baseline / T_current)
- **Scalability Efficiency** (E = S / data_ratio)
- **Throughput** (rows/sec)

---

## RQ-to-Module Mapping

```
RQ1 (Cross-service propagation)
  ├── Module 3: cross_service_analysis.py
  │   ├── build_service_dependency_graph()
  │   ├── detect_propagation_chains()
  │   └── correlate_cross_service_errors()
  └── Module 6: plot_propagation_heatmap()

RQ2 (Abnormal failure patterns)
  ├── Module 4: failure_detection.py
  │   ├── compute_error_rate_timeseries()
  │   ├── detect_error_rate_anomalies()
  │   ├── detect_latency_anomalies()
  │   ├── detect_resource_anomalies()
  │   └── cluster_failure_patterns()
  └── Module 6: plot_anomaly_timeseries(), plot_failure_pattern_distribution()

RQ3 (Spark scalability)
  ├── Module 5: scalability_analysis.py
  │   ├── generate_scaled_dataset()
  │   ├── run_benchmark()
  │   └── compute_scalability_metrics()
  └── Module 6: plot_scaling_curves(), plot_scalability_efficiency()
```

---

## Implementation Notes

1. **Sample Data**: The project auto-generates realistic microservice telemetry matching the Kaggle dataset schema. If you have the real dataset, place CSV files in `./data/`.
2. **Six Modules Only**: The project uses exactly 6 Python modules as specified - no extra microservices or unnecessary files.
3. **Academically Manageable**: Each module has ~5–6 core functions with clear inputs/outputs, making it easy to understand and modify.
4. **Distributed First**: All data processing goes through Spark DataFrames, with MinIO as the blob storage layer.
5. **Reproducible**: Docker Compose ensures identical environments. All parameters are in `config.yaml`.

---

## Potential Issues & Mitigations

| Risk                            | Mitigation                                              |
|---------------------------------|---------------------------------------------------------|
| Docker memory insufficient       | Reduce `SPARK_WORKER_MEMORY` in `.env` or scale to 1 worker |
| Spark job timeout on large data | Reduce `data_sizes` in `config.yaml`                     |
| PostgreSQL connection refused   | Pipeline retries automatically (30 attempts)            |
| MinIO bucket not created        | `minio-init` service handles this automatically         |
| Plot generation fails (no data) | Visualization generates informative placeholder plots   |

---

## License

Academic project - no license required.
