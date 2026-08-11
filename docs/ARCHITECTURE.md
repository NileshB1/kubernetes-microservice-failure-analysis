# Architecture & Data Flow

## Table of Contents

1. [System Overview](#system-overview)
2. [Infrastructure Topology](#infrastructure-topology)
3. [Data Flow — End to End](#data-flow--end-to-end)
4. [Module Deep Dives](#module-deep-dives)
   - [Module 1: Ingestion](#module-1-data-ingestion)
   - [Module 2: Preprocessing](#module-2-data-preprocessing)
   - [Module 3: Cross-Service Analysis (RQ1)](#module-3-cross-service-failure-propagation-analysis-rq1)
   - [Module 4: Failure Detection (RQ2)](#module-4-abnormal-failure-pattern-detection-rq2)
   - [Module 5: Scalability Analysis (RQ3)](#module-5-spark-scalability-analysis-rq3)
   - [Module 6: Visualization](#module-6-visualization)
5. [Database Schema](#database-schema)
6. [Dashboard Architecture](#dashboard-architecture)
7. [Tech Stack Rationale](#tech-stack-rationale)
8. [Test Strategy](#test-strategy)

---

## System Overview

This project implements a **distributed data-intensive pipeline** for analysing Kubernetes microservice telemetry. It answers three research questions about failure propagation, anomaly detection, and scalability using Apache Spark as the distributed compute engine, MinIO as blob storage, and PostgreSQL as the results database.

### Research Questions

| RQ  | Question | Approach |
|-----|----------|----------|
| **RQ1** | How can distributed analysis identify cross-service failure propagation? | Spark self-joins, window functions (`lag`/`lead`), and pairwise correlation |
| **RQ2** | How effectively can distributed processing identify abnormal failure patterns? | Multi-signal z-score anomaly detection + pattern clustering |
| **RQ3** | How does distributed Spark processing scale with increasing data volumes? | Controlled benchmarks at 5 data sizes, measuring speed-up and efficiency |

---

## Infrastructure Topology

```
┌──────────────────────────────────────────────────────────────┐
│                      Docker Compose                          │
│                                                              │
│  ┌─────────┐   ┌──────────────┐   ┌───────────────────┐     │
│  │  MinIO  │   │ Spark Master │   │ Spark Worker × N   │     │
│  │ :9000   │   │ :7077 :8080  │   │ (local[2] in dev) │     │
│  │ (S3 API)│   │ (REST API)   │   │                   │     │
│  └────┬────┘   └──────┬───────┘   └─────────┬─────────┘     │
│       │               │                     │               │
│       │    ┌──────────┴─────────────────────┘               │
│       │    │  Spark Driver (pipeline container)             │
│       │    │    • Reads CSV from MinIO via s3a://            │
│       │    │    • Processes with Spark DataFrames           │
│       │    │    • Writes results to PostgreSQL via JDBC     │
│       │    │    • Writes Parquet back to MinIO              │
│       │    └──────────────────┬─────────────────────┘       │
│       │                       │                             │
│  ┌────┴───────────────────────┴──────┐                      │
│  │         PostgreSQL :5432          │                      │
│  │   • Results database (7 tables)   │                      │
│  │   • Schema defined in sql/init.sql│                      │
│  └────────────────┬──────────────────┘                      │
│                   │                                         │
│  ┌────────────────┴──────────────────┐                      │
│  │      Streamlit Dashboard :8501    │                      │
│  │   • Reads from PostgreSQL/SQLite  │                      │
│  │   • Interactive visualisations    │                      │
│  │   • Pipeline controls & log viewer│                      │
│  └───────────────────────────────────┘                      │
└──────────────────────────────────────────────────────────────┘
```

### Service Communication

| From → To | Protocol | Purpose |
|-----------|----------|---------|
| Pipeline → MinIO | S3 API (s3a://) | Read raw CSV, write processed Parquet |
| Pipeline → PostgreSQL | JDBC | Write analysis results |
| Pipeline → Spark Master | Spark RPC (:7077) | Submit jobs, coordinate executors |
| Spark Executors → MinIO | S3 API | Distributed read/write of data |
| Dashboard → PostgreSQL | psycopg2 (libpq) | Read results for display |
| Dashboard → Spark Master | HTTP REST (:8080) | Fetch cluster metrics |

---

## Data Flow — End to End

### Pipeline Path (Docker Mode)

```
Step 1: Ingestion
  Python generates 5 CSV files (100K–1M rows)
    → Uploads to MinIO: s3a://microservice-logs/raw/*.csv

Step 2: Preprocessing
  Spark reads all 5 CSVs from MinIO
    → clean_and_validate() — deduplicates, filters nulls/negatives
    → join_datasets() — 4× inner join on span_id across datasets
    → engineer_features() — adds 6 derived columns (is_failure, latency_bucket, etc.)
    → Writes unified Parquet to MinIO + PostgreSQL

Step 3: Cross-Service Analysis (RQ1)
  Spark reads unified Parquet
    → build_service_dependency_graph() — self-join parent_span_id→span_id
    → detect_propagation_chains() — lead() window function, filter by time window
    → correlate_cross_service_errors() — pairwise join + stat.corr()
    → Writes 3 tables to PostgreSQL + Parquet to MinIO

Step 4: Failure Detection (RQ2)
  Spark reads unified Parquet
    → compute_error_rate_timeseries() — windowed groupBy per service
    → detect_*_anomalies() — 3× z-score detection (error, latency, resource)
    → unify_anomalies() — outer join, composite score (0–3), overall flag (≥2)
    → cluster_failure_patterns() — group by signal combination → 7 pattern types
    → Writes 2 tables to PostgreSQL + Parquet to MinIO

Step 5: Scalability Analysis (RQ3)
  For each of 5 data sizes (100K → 1M in dev, 1M → 50M in prod):
    → generate_scaled_dataset() — spark.range() + random columns
    → run_benchmark() — measures GroupBy, Window, Join, Shuffle timing
    → Record throughput, compute speed-up and efficiency
    → Writes to PostgreSQL

Step 6: Visualization
  Reads all PostgreSQL tables
    → Generates 6 PNG plots (heatmaps, time series, scaling curves)
    → Saves to /output/ directory
```

### Lightweight Path (SQLite Mode)

```
run_streamlit.py --local
  → Creates dashboard.db (SQLite)
  → Seeds all 7 tables with 5K+ realistic rows
  → Generates placeholder plots
  → Launches Streamlit dashboard (no Docker, no Spark, no PostgreSQL)
```

---

## Module Deep Dives

### Module 1: Data Ingestion

**File:** `modules/ingestion.py`  
**Spark:** No (pure Python, generates CSV)  
**Output:** 5 CSV files → MinIO `s3a://microservice-logs/raw/`

Generates realistic Kubernetes microservice telemetry matching the dataset schema:

| CSV File | Columns | Description |
|----------|---------|-------------|
| `trace_service_name.csv` | trace_id, service_name, span_id, parent_span_id, namespace, pod_id, node_id | Trace topology |
| `trace_response_times.csv` | span_id, response_time_ms, wait_time_ms, processing_time_ms, network_latency_ms | Response metrics |
| `trace_request_times.csv` | span_id, start_time, end_time, duration_ms, http_method, endpoint | Request timing |
| `resource_usage.csv` | pod_id, timestamp, cpu_usage_mcores, memory_usage_mb, network_rx_bytes, network_tx_bytes, disk_io_read_bytes, disk_io_write_bytes | Resource metrics |
| `status_codes.csv` | span_id, status_code, error_message, is_error | HTTP status |

**Key design decisions:**
- Span IDs are sequential for deterministic joins during preprocessing
- ~8% of rows are failures (status ≥ 500) to create realistic propagation patterns
- ~20 distinct service names drawn from a pool of common microservices
- Parent span IDs follow a linked-list pattern within traces for dependency graph inference

---

### Module 2: Data Preprocessing

**File:** `modules/preprocessing.py`  
**Spark:** Yes (heavy — all 5 CSVs processed concurrently)  
**Input:** 5 CSV files from MinIO  
**Output:** Unified Parquet → MinIO + PostgreSQL `processed_telemetry`

#### Pipeline Steps

```
Raw CSVs (5 files)
    │
    ▼
clean_and_validate()
    ├── Drop duplicate span_ids (keep first)
    ├── Filter null span_ids
    ├── Filter empty parent_span_ids
    ├── Filter negative response_times & processing_times
    ├── Parse ISO-8601 timestamps → TimestampType
    ├── Filter null timestamps
    ├── Filter negative CPU usage
    └── Return dict of 5 cleaned DataFrames
    │
    ▼
join_datasets()
    ├── Inner join service + response on span_id
    ├── Inner join result + request on span_id
    ├── Inner join result + status on span_id
    ├── Left join result + resource on pod_id
    └── Returns single unified DataFrame
    │
    ▼
engineer_features()
    ├── is_failure:         status_code ≥ 500 → 1, else 0
    ├── is_latency_spike:   response_time_ms > 2000 → 1, else 0
    ├── latency_bucket:     low (<100), medium (<500), high (<2000), critical (≥2000)
    ├── error_category:     success (2xx), client_error (4xx), server_error (5xx)
    ├── hour_of_day:        hour() extracted from start_time_ts
    └── cpu_memory_ratio:   cpu_usage_mcores / memory_usage_mb
```

**Spark operations used:**
- `dropDuplicates(["span_id"])` — deduplication
- `filter()` with complex boolean expressions
- `to_timestamp()` — ISO-8601 parsing
- 4 sequential inner joins + 1 left join
- `withColumn()` chains for feature engineering

---

### Module 3: Cross-Service Failure Propagation Analysis (RQ1)

**File:** `modules/cross_service_analysis.py`  
**Spark:** Yes (self-joins, window functions, correlations)  
**Input:** Unified Parquet from MinIO  
**Output:** 3 PostgreSQL tables + Parquet in MinIO

#### RQ1: "How can distributed analysis identify cross-service failure propagation?"

This module answers RQ1 through three complementary analyses:

#### Analysis 1: Service Dependency Graph

```
build_service_dependency_graph()
    │
    ├── Self-join: parent.span_id = child.parent_span_id
    │   (extracts caller→callee from trace topology)
    │
    ├── Group by (caller_service, callee_service)
    │   ├── call_count, caller_error_count, callee_error_count
    │   └── co_failure_count (both failed)
    │
    └── propagation_score = co_failure_count / callee_error_count
        (0.0 = no propagation, 1.0 = all callee failures preceded by caller failure)
```

**Why this works:** Microservice traces capture parent-child relationships via `parent_span_id`. By self-joining the unified DataFrame, we reconstruct the call graph without needing explicit service dependency configuration.

#### Analysis 2: Propagation Chains

```
detect_propagation_chains()
    │
    ├── Filter to failures only (is_failure = 1)
    │
    ├── Window: PARTITION BY trace_id ORDER BY start_time_ts
    │   └── lead(service_name, 1) → next_service
    │   └── lead(is_failure, 1) → next_is_failure
    │   └── lag in seconds: unix_timestamp(next) - unix_timestamp(current)
    │
    ├── Filter: next_is_failure = 1 AND lag ≤ time_window_sec AND service ≠ next_service
    │
    └── Output: (trace_id, source_service, target_service, propagation_lag_sec, depth=2)
```

**Why this works:** Temporal ordering within traces reveals cascading failures. If service A fails at t=0 and service B fails at t=45s within the same trace, we infer a propagation chain. The time window (default 60s) prevents false positives from unrelated failures.

#### Analysis 3: Error Correlation

```
correlate_cross_service_errors()
    │
    ├── Compute per-service per-minute error rates
    │   └── window("start_time_ts", "1 minute") → minute_ts
    │
    ├── For each service pair (A, B):
    │   ├── Join error-rate time series on minute_ts
    │   ├── Filter: n ≥ 5 overlapping minutes
    │   └── Compute stat.corr(error_rate_a, error_rate_b)
    │
    └── Filter: |correlation| ≥ min_correlation (default 0.3)
```

**Why this works:** Even when services don't have direct parent-child relationships in traces, their error rates may correlate over time — indicating shared root causes (e.g., network congestion, node failure, shared dependency failure).

**Spark operations used:**
- Self-join with aliased DataFrames
- `lead()` window function for temporal lookahead
- `groupBy().agg()` with conditional `sum(when(...))`
- `stat.corr()` for Pearson correlation
- `Window.partitionBy().orderBy()` for trace-level ordering

---

### Module 4: Abnormal Failure Pattern Detection (RQ2)

**File:** `modules/failure_detection.py`  
**Spark:** Yes (groupBy, window functions, z-score statistics)  
**Input:** Unified Parquet from MinIO  
**Output:** 2 PostgreSQL tables + Parquet in MinIO

#### RQ2: "How effectively can distributed processing identify abnormal failure patterns?"

#### Step 1: Time Series Construction

```
compute_error_rate_timeseries(window_minutes=15)
    │
    ├── window("start_time_ts", "15 minutes") → time_bucket
    │
    └── Group by (service_name, time_bucket)
        ├── COUNT(*) → total_requests
        ├── SUM(is_failure) → error_count
        ├── AVG(response_time_ms) → avg_latency_ms
        ├── AVG(cpu_usage_mcores) → avg_cpu_mcores
        └── AVG(memory_usage_mb) → avg_memory_mb
```

#### Step 2: Multi-Signal Z-Score Detection

For each of three signals — error rate, latency, and resource usage:

```
detect_*_anomalies(zscore_threshold=3.0)
    │
    ├── Per-service baseline:
    │   ├── mean = AVG(signal) over all time buckets
    │   └── stddev = STDDEV(signal) over all time buckets
    │
    ├── Join baseline to time series
    │   └── zscore = (value - mean) / stddev
    │
    └── Flag: |zscore| > threshold → is_anomaly_* = 1
```

#### Step 3: Anomaly Unification

```
unify_anomalies()
    │
    ├── Outer join all 3 anomaly DataFrames on (service_name, time_bucket)
    │
    ├── Composite score: anomaly_score = sum of 3 binary flags (0–3)
    │
    └── Overall flag: is_anomaly_overall = (anomaly_score ≥ 2)
        (at least 2 out of 3 signals must indicate anomaly)
```

**Why 2 out of 3?** Single-signal anomalies can be noise (e.g., a brief latency spike from GC pause). Requiring at least 2 signals reduces false positives while catching real multi-dimensional anomalies.

#### Step 4: Pattern Clustering

```
cluster_failure_patterns()
    │
    └── Map signal combinations to 7 pattern types:
        ├── full_failure:          error + latency + resource
        ├── cascading_failure:     error + latency
        ├── resource_exhaustion:   latency + resource
        ├── error_resource_link:   error + resource
        ├── error_surge:           error only
        ├── latency_spike:         latency only
        └── resource_pressure:     resource only
```

Each pattern records the service, occurrence count, and average severity for downstream analysis.

**Spark operations used:**
- `window().getField("start")` for time bucketing
- `groupBy().agg(mean(), stddev())` for per-service baselines
- Outer joins for combining anomaly types
- Conditional `when().otherwise()` chains for pattern classification
- `fillna(0)` for null handling after outer joins

---

### Module 5: Spark Scalability Analysis (RQ3)

**File:** `modules/scalability_analysis.py`  
**Spark:** Yes (generates data, runs benchmarks, records timing)  
**Input:** Config-driven data sizes  
**Output:** PostgreSQL `scalability_metrics` table

#### RQ3: "How does distributed Spark processing scale with increasing data volumes?"

#### Experiment Design

```
run_scalability_experiments()
    │
    ├── For each data_size in [100K, 500K, 1M, ...]:
    │   │
    │   ├── generate_scaled_dataset()
    │   │   └── spark.range(target_rows) → add random columns → write Parquet
    │   │
    │   └── For rep in 1..repetitions:
    │       └── run_benchmark()
    │           ├── GroupBy + Aggregation:  per-service stats
    │           ├── Window Function:        lead() over trace_id
    │           ├── Join:                   sample join against full dataset
    │           └── Shuffle:                repartition(16, "service_name")
```

#### Benchmark Operations

| Operation | What it measures | Why it matters |
|-----------|-----------------|----------------|
| **GroupBy + Agg** | `groupBy("service_name").agg(count, sum, avg, stddev)` | Core analytics primitive; tests shuffle + aggregation scaling |
| **Window Function** | `lead("service_name")` partitioned by trace_id | Tests window partitioning + sort within partitions |
| **Join** | 1% sample inner join against full dataset on trace_id | Tests broadcast vs shuffle join + network transfer |
| **Shuffle** | `repartition(16, "service_name")` | Tests full data redistribution across the cluster |

#### Derived Metrics

```
For each data size:
  throughput_rows_per_sec = input_rows / total_sec
  speedup_vs_baseline     = baseline_time / current_time
  scalability_efficiency  = speedup / (current_size / baseline_size)
```

- **Speed-up > data_ratio** → super-linear scaling (parallelism benefit)
- **Speed-up ≈ data_ratio** → near-linear scaling (ideal)
- **Speed-up < data_ratio** → sub-linear scaling (overhead dominates)

**Expected scaling characteristic for this workload:** Near-linear for GroupBy and Shuffle operations (embarrassingly parallel), sub-linear for Window functions and Joins (require data co-location or broadcast).

**Spark operations used:**
- `spark.range()` for efficient distributed data generation
- `rand()`, `randn()`, `array().getItem()` for realistic column distributions
- `repartition()` for controlled shuffle
- Python `time.time()` for wall-clock benchmarking

---

### Module 6: Visualization

**File:** `modules/visualization.py`  
**Spark:** No (reads from PostgreSQL, renders with matplotlib/seaborn)  
**Input:** PostgreSQL results tables  
**Output:** 6 PNG plots in `/output/`

| Plot | RQ | Data Source | Chart Type |
|------|-----|-------------|------------|
| `rq1_propagation_heatmap.png` | RQ1 | `cross_service_pairs` | Heatmap (caller × callee, color = propagation_score) |
| `rq2_anomaly_timeseries.png` | RQ2 | `anomaly_scores` | Multi-line time series per service |
| `rq2_failure_patterns.png` | RQ2 | `failure_patterns` | Grouped bar chart (pattern type per service) |
| `rq3_scaling_curves.png` | RQ3 | `scalability_metrics` | Log-log scatter (data size vs time) |
| `rq3_scalability_efficiency.png` | RQ3 | `scalability_metrics` | Dual-axis (speed-up + efficiency vs data ratio) |
| `dashboard_summary.png` | All | All tables | Composite dashboard summary |

---

## Database Schema

### PostgreSQL (Docker Pipeline Mode)

```sql
-- Core telemetry (Module 2)
processed_telemetry (
    span_id, trace_id, service_name, pod_id, node_id, namespace,
    response_time_ms, wait_time_ms, processing_time_ms, network_latency_ms,
    start_time_ts, end_time_ts, duration_ms, http_method, endpoint,
    status_code, error_message, is_error,
    cpu_usage_mcores, memory_usage_mb, network_rx_bytes, network_tx_bytes,
    disk_io_read_bytes, disk_io_write_bytes,
    -- Engineered features
    is_failure, is_latency_spike, latency_bucket, error_category,
    hour_of_day, cpu_memory_ratio
)

-- RQ1 outputs (Module 3)
cross_service_pairs (
    caller_service, callee_service, call_count,
    caller_error_count, callee_error_count, co_failure_count,
    avg_callee_latency_ms, propagation_score
)

propagation_chains (
    trace_id, source_service, target_service,
    source_timestamp, target_timestamp,
    propagation_lag_sec, propagation_depth
)

error_correlations (
    service_a, service_b, error_correlation, sample_size
)

-- RQ2 outputs (Module 4)
anomaly_scores (
    service_name, time_bucket,
    is_anomaly_error, is_anomaly_latency, is_anomaly_resource,
    anomaly_score, is_anomaly_overall
)

failure_patterns (
    service_name, pattern_type, occurrence_count, avg_severity
)

-- RQ3 outputs (Module 5)
scalability_metrics (
    data_size, repetition,
    groupby_agg_sec, window_fn_sec, join_sec, shuffle_sec, total_sec,
    throughput_rows_per_sec, speedup_vs_baseline,
    baseline_size, baseline_time_sec
)
```

### SQLite (Local Mode)

Identical schema with SQLite-compatible types (INTEGER, REAL, TEXT). No Parquet/MinIO — all data lives in `dashboard.db`.

---

## Dashboard Architecture

**File:** `modules/dashboard.py` (Streamlit)  
**Backend:** PostgreSQL or SQLite via `modules/db_adapter.py`  
**Port:** `8501`

### Tab Structure

| Tab | Content | Data Source |
|-----|---------|-------------|
| 🏠 **Overview** | 4 metric cards, architecture diagram, pipeline progress, image gallery | All tables |
| 🔗 **RQ1 · Propagation** | Propagation heatmap, top chains bar chart, error correlation matrix, details table | `cross_service_pairs`, `propagation_chains`, `error_correlations` |
| 🔴 **RQ2 · Anomalies** | Anomaly metrics, anomalies-per-service chart, pattern distribution pie, signal breakdown, details table | `anomaly_scores`, `failure_patterns` |
| ⚡ **RQ3 · Scalability** | Execution time curve, throughput chart, speed-up/efficiency dual-axis, operation breakdown, raw data | `scalability_metrics` |
| 🖥️ **Spark Cluster** | Real-time cluster metrics via Spark REST API (:8080) | Spark Master API |
| 📋 **Logs** | Live pipeline log viewer with level filtering, search, highlighting | `/output/pipeline.log` |

### Database Abstraction Layer

```
modules/db_adapter.py
    ├── DB_MODE detection (env var or auto-detect)
    ├── get_connection()     → psycopg2 or sqlite3
    ├── run_query()          → DataFrame (unified API)
    ├── get_table_names()    → cross-backend table listing
    ├── get_table_row_count()→ cross-backend row counting
    └── check_connection()   → connectivity test
```

### Theme System

CSS custom properties with `[data-theme="dark"]` and `[data-theme="light"]` attribute selectors. A `st.toggle` in the sidebar triggers JavaScript injection to update `sessionStorage` and the `data-theme` attribute, with Streamlit handling the rerender.

---

## Tech Stack Rationale

| Technology | Role | Why |
|------------|------|-----|
| **Apache Spark 3.5** | Distributed compute | De facto standard for large-scale data processing; DataFrame API maps naturally to SQL-like analytics |
| **MinIO** | Blob storage | S3-compatible API, lightweight, perfect for local/Docker development without AWS dependency |
| **PostgreSQL** | Results store | Mature, well-supported, excellent JDBC connector for Spark; SQL standard for dashboard queries |
| **SQLite** | Lightweight fallback | Zero-config embedded database; enables the dashboard to run without any infrastructure |
| **Streamlit** | Dashboard | Python-native, rapid prototyping, built-in caching, wide layout support |
| **Plotly** | Interactive charts | Richer than matplotlib for dashboards; `plotly_chart()` integrates natively with Streamlit |
| **Docker Compose** | Orchestration | Reproducible multi-service environment; single-command full-stack deployment |
| **Bitnami Spark image** | Base image | Official Spark distribution with Java 17, well-maintained, no custom JVM config needed |

### Why Not...

| Rejected | Reason |
|----------|--------|
| **Kubernetes for orchestration** | Overkill for an academic project; Docker Compose provides the same reproducibility with less complexity |
| **Kafka for streaming** | Research questions focus on batch analysis of historical telemetry; streaming would add complexity without value |
| **Elasticsearch** | Spark + PostgreSQL covers both compute and storage needs; adding another service increases operational burden |
| **Airflow for scheduling** | The pipeline runs as a single end-to-end job; no DAG scheduling needed |
| **dbt for transforms** | Spark DataFrames provide equivalent transformation capabilities within the distributed engine |

---

## Test Strategy

### Test Pyramid

```
        ┌──────┐
        │ E2E  │  Full pipeline in Docker (make test-docker)
        │──────│
       ┌┴──────┴┐
       │ Spark  │  ~85 tests across preprocessing, cross_service, failure_detection, scalability
       │────────│
      ┌┴────────┴┐
      │  Unit    │  127 tests (local_seeder: 62, db_adapter: 40, ingestion: 25)
      └──────────┘
```

### Test Files

| File | Count | Type | Requires |
|------|-------|------|----------|
| `test_local_seeder.py` | 62 | Unit | SQLite only |
| `test_db_adapter.py` | 40 | Unit | SQLite + psycopg2 mocks |
| `test_ingestion.py` | 25 | Unit | Nothing |
| `test_preprocessing.py` | 22 | Spark | PySpark + Java |
| `test_cross_service_analysis.py` | 30 | Spark | PySpark + Java |
| `test_failure_detection.py` | 24 | Spark | PySpark + Java |
| `test_scalability_analysis.py` | 25 | Spark | PySpark + Java |
| **Total** | **~228** | | |

### Running Tests

```bash
# Fast: 127 non-Spark tests (no Docker, <10s)
make test-ci

# Full: all ~228 tests inside Docker (includes Spark)
make test-docker
```

---

## Configuration Reference

All pipeline parameters live in `config/config.yaml`:

```yaml
ingestion:
  bucket_name: microservice-logs
  files: [trace_service_name.csv, trace_response_times.csv, ...]

preprocessing:
  null_threshold: 0.5
  output_format: parquet

cross_service_analysis:
  window_duration_seconds: 300
  propagation_time_window_seconds: 60
  min_correlation_threshold: 0.3

failure_detection:
  anomaly_zscore_threshold: 3.0
  error_rate_threshold: 0.05
  rolling_window_minutes: 15

scalability:
  data_sizes: [1000000, 5000000, 10000000, 25000000, 50000000]
  repetitions: 3

visualization:
  dpi: 150
  figsize: [12, 8]
```

Environment variables (set in `.env` or `docker-compose.yml`) override specific values at runtime (e.g., `SAMPLE_DATA_ROWS`, `SCALABILITY_DATA_SIZES`).
