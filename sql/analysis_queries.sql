
-- Spark SQL analysis - the declarative half of the pipeline

SELECT
    service_name,
    COUNT(*) AS span_count,
    ROUND(AVG(response_time_ms), 3) AS mean_latency_ms,
    ROUND(PERCENTILE_APPROX(response_time_ms, 0.50), 3) AS p50_latency_ms,
    ROUND(PERCENTILE_APPROX(response_time_ms, 0.95), 3) AS p95_latency_ms,
    ROUND(PERCENTILE_APPROX(response_time_ms, 0.99), 3) AS p99_latency_ms,
    ROUND(MAX(response_time_ms), 3)  AS max_latency_ms, SUM(is_failure) AS failed_spans,
    ROUND(100.0 * SUM(is_failure) / COUNT(*), 3) AS failure_rate_pct
FROM telemetry GROUP BY service_name ORDER BY failure_rate_pct DESC

-- @name endpoint_hotspots
-- The slowest operations, restricted to those with enough traffic to be
-- meaningful
SELECT
    service_name, endpoint,
    COUNT(*) AS calls,
    ROUND(AVG(response_time_ms), 3) AS mean_latency_ms,
    ROUND(PERCENTILE_APPROX(response_time_ms, 0.95), 3) AS p95_latency_ms,
    SUM(is_failure) AS failed_calls,
    ROUND(100.0 * SUM(is_failure) / COUNT(*), 3)  AS failure_rate_pct
FROM telemetry GROUP BY service_name, endpoint HAVING COUNT(*) >= 100
ORDER BY p95_latency_ms DESC LIMIT 50


-- How deep call chains run. Each trace is reduced to its span count,
-- then those counts are bucketed 
SELECT
    span_count_bucket,
    COUNT(*) AS trace_count, SUM(failed_spans) AS failed_spans
FROM (
    SELECT
        trace_id,
        CASE
            WHEN COUNT(*) = 1  THEN '1 span'
            WHEN COUNT(*) <= 5 THEN '2-5 spans'
            WHEN COUNT(*) <= 10 THEN '6-10 spans'
            WHEN COUNT(*) <= 20 THEN '11-20 spans'
            ELSE '21+ spans'
        END  AS span_count_bucket,
        SUM(is_failure) AS failed_spans
    FROM telemetry
    GROUP BY trace_id
)
GROUP BY span_count_bucket ORDER BY trace_count DESC


-- Ranks services by failure rate using a window function, and flags the
-- worst quartile.
SELECT
    service_name,  span_count,
    failed_spans,  failure_rate_pct,
    RANK()  OVER (ORDER BY failure_rate_pct DESC) AS failure_rank,
    NTILE(4) OVER (ORDER BY failure_rate_pct DESC) AS failure_quartile
FROM (
    SELECT
        service_name, COUNT(*) AS span_count,
        SUM(is_failure) AS failed_spans,
        ROUND(100.0 * SUM(is_failure) / COUNT(*), 3) AS failure_rate_pct
    FROM telemetry  GROUP BY service_name  HAVING COUNT(*) >= 50
)
ORDER BY failure_rank


-- Failure rate per service per hour, with the previous hour alongside it
-- so a change is visible without a second query.
SELECT
    service_name, bucket_hour,
    spans, failed, failure_rate_pct,
    LAG(failure_rate_pct) OVER (
        PARTITION BY service_name ORDER BY bucket_hour
    ) AS previous_hour_failure_rate_pct
FROM (
    SELECT
        service_name,
        DATE_TRUNC('HOUR', start_time_ts)  AS bucket_hour,
        COUNT(*) AS spans,
        SUM(is_failure) AS failed,
        ROUND(100.0 * SUM(is_failure) / COUNT(*), 3) AS failure_rate_pct
    FROM telemetry WHERE start_time_ts IS NOT NULL
    GROUP BY service_name, DATE_TRUNC('HOUR', start_time_ts)
)
ORDER BY service_name, bucket_hour


-- Did the pipeline flag the services where faults were actually
-- injected? Each injected fault is matched against the failure activity
-- of its own service inside the injection window, so the answer is
-- measured rather than asserted

SELECT
    g.inject_service, g.inject_type,
    COUNT(DISTINCT g.inject_timestamp) AS injections,
    SUM(m.spans_in_window) AS spans_observed,
    SUM(m.failures_in_window) AS failures_observed,
    ROUND(100.0 * SUM(m.failures_in_window) / NULLIF(SUM(m.spans_in_window), 0), 3
    ) AS failure_rate_in_window_pct
FROM ground_truth g
LEFT JOIN (
    SELECT
        service_name, UNIX_TIMESTAMP(start_time_ts) AS span_epoch,
        COUNT(*) AS spans_in_window,
        SUM(is_failure) AS failures_in_window
    FROM telemetry WHERE start_time_ts IS NOT NULL
    GROUP BY service_name, UNIX_TIMESTAMP(start_time_ts)
) m
  ON  m.service_name = g.inject_service
  -- The Nezha injections run for roughly five minutes from the recorded
  -- timestamp. The window is closed at 300 seconds accordingly.
  AND m.span_epoch BETWEEN g.inject_timestamp AND g.inject_timestamp + 300
GROUP BY g.inject_service, g.inject_type ORDER BY failure_rate_in_window_pct DESC
