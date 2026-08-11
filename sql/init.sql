-- ============================================================
-- PostgreSQL Schema Initialization
-- Distributed Analysis of Kubernetes Microservice Logs
-- ============================================================

-- Note: Raw telemetry is stored in MinIO blob storage (s3a://microservice-logs/raw/).
-- Only processed/analysis results are persisted in PostgreSQL.

-- Processed & feature-engineered telemetry
CREATE TABLE IF NOT EXISTS processed_telemetry (
    id              SERIAL PRIMARY KEY,
    trace_id        VARCHAR(64),
    service_name    VARCHAR(128),
    span_id         VARCHAR(64),
    parent_span_id  VARCHAR(64),
    namespace       VARCHAR(64),
    pod_id          VARCHAR(64),
    node_id         VARCHAR(64),
    response_time_ms DOUBLE PRECISION,
    wait_time_ms    DOUBLE PRECISION,
    processing_time_ms DOUBLE PRECISION,
    network_latency_ms DOUBLE PRECISION,
    start_time_ts   TIMESTAMP,
    end_time_ts     TIMESTAMP,
    duration_ms     DOUBLE PRECISION,
    http_method     VARCHAR(16),
    endpoint        VARCHAR(256),
    status_code     INTEGER,
    error_message   TEXT,
    is_failure      INTEGER,
    is_latency_spike INTEGER,
    latency_bucket  VARCHAR(16),
    error_category  VARCHAR(32),
    hour_of_day     INTEGER,
    cpu_usage_mcores DOUBLE PRECISION,
    memory_usage_mb DOUBLE PRECISION,
    cpu_memory_ratio DOUBLE PRECISION,
    processed_at    TIMESTAMP DEFAULT NOW()
);

-- RQ1: Cross-service failure propagation pairs
CREATE TABLE IF NOT EXISTS cross_service_pairs (
    id                SERIAL PRIMARY KEY,
    caller_service    VARCHAR(128),
    callee_service    VARCHAR(128),
    call_count        BIGINT,
    caller_error_count BIGINT,
    callee_error_count BIGINT,
    co_failure_count  BIGINT,
    avg_callee_latency_ms DOUBLE PRECISION,
    propagation_score DOUBLE PRECISION,
    analyzed_at       TIMESTAMP DEFAULT NOW()
);

-- RQ1: Propagation chains
CREATE TABLE IF NOT EXISTS propagation_chains (
    id                SERIAL PRIMARY KEY,
    trace_id          VARCHAR(64),
    source_service    VARCHAR(128),
    target_service    VARCHAR(128),
    source_timestamp  TIMESTAMP,
    target_timestamp  TIMESTAMP,
    propagation_lag_sec DOUBLE PRECISION,
    propagation_depth INTEGER,
    detected_at       TIMESTAMP DEFAULT NOW()
);

-- RQ1: Error correlations
CREATE TABLE IF NOT EXISTS error_correlations (
    id                SERIAL PRIMARY KEY,
    service_a         VARCHAR(128),
    service_b         VARCHAR(128),
    error_correlation DOUBLE PRECISION,
    sample_size       BIGINT,
    computed_at       TIMESTAMP DEFAULT NOW()
);

-- RQ2: Anomaly scores
CREATE TABLE IF NOT EXISTS anomaly_scores (
    id                SERIAL PRIMARY KEY,
    service_name      VARCHAR(128),
    time_bucket       TIMESTAMP,
    is_anomaly_error  INTEGER,
    is_anomaly_latency INTEGER,
    is_anomaly_resource INTEGER,
    anomaly_score     INTEGER,
    is_anomaly_overall INTEGER,
    computed_at       TIMESTAMP DEFAULT NOW()
);

-- RQ2: Failure patterns
CREATE TABLE IF NOT EXISTS failure_patterns (
    id                SERIAL PRIMARY KEY,
    service_name      VARCHAR(128),
    pattern_type      VARCHAR(64),
    occurrence_count  BIGINT,
    avg_severity      DOUBLE PRECISION,
    computed_at       TIMESTAMP DEFAULT NOW()
);

-- RQ3: Scalability metrics
CREATE TABLE IF NOT EXISTS scalability_metrics (
    id                SERIAL PRIMARY KEY,
    label             VARCHAR(128),
    input_rows        BIGINT,
    groupby_agg_sec   DOUBLE PRECISION,
    window_fn_sec     DOUBLE PRECISION,
    join_sec          DOUBLE PRECISION,
    shuffle_sec       DOUBLE PRECISION,
    total_sec         DOUBLE PRECISION,
    data_size         BIGINT,
    repetition        INTEGER,
    throughput_rows_per_sec DOUBLE PRECISION,
    speedup_vs_baseline DOUBLE PRECISION,
    baseline_size     BIGINT,
    baseline_time_sec DOUBLE PRECISION,
    recorded_at       TIMESTAMP DEFAULT NOW()
);

-- Indexes for analytical queries
CREATE INDEX IF NOT EXISTS idx_anomaly_scores_service ON anomaly_scores (service_name);
CREATE INDEX IF NOT EXISTS idx_anomaly_scores_bucket ON anomaly_scores (time_bucket);
CREATE INDEX IF NOT EXISTS idx_anomaly_scores_overall ON anomaly_scores (is_anomaly_overall);
CREATE INDEX IF NOT EXISTS idx_cross_service_pairs_score ON cross_service_pairs (propagation_score DESC);
CREATE INDEX IF NOT EXISTS idx_scalability_metrics_size ON scalability_metrics (data_size);
CREATE INDEX IF NOT EXISTS idx_failure_patterns_type ON failure_patterns (pattern_type);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO sparkuser;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO sparkuser;
