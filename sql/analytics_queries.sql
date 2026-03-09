-- FitLake Gold Layer Analytics Queries
-- Run via: duckdb (with Iceberg extension) or any SQL client connected to Snowflake

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Daily Recovery Summary — last 30 days
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    user_id,
    date,
    recovery_score,
    recovery_state,
    hrv_component,
    sleep_component,
    rhr_component,
    ROUND(hrv_rmssd, 1)   AS hrv_rmssd_ms,
    ROUND(hrv_baseline, 1) AS hrv_7d_baseline_ms
FROM fitlake.gold.daily_recovery
WHERE date >= CURRENT_DATE - INTERVAL 30 DAYS
ORDER BY user_id, date DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Strain vs Next-Day Recovery Correlation
--    Key insight: high strain today → lower recovery tomorrow
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    s.user_id,
    s.date              AS strain_date,
    s.strain_score,
    s.strain_category,
    r.recovery_score    AS next_day_recovery,
    r.recovery_state    AS next_day_state,
    ROUND(r.recovery_score - LAG(r.recovery_score) OVER (
        PARTITION BY r.user_id ORDER BY r.date
    ), 1) AS recovery_delta
FROM fitlake.gold.user_strain_summary s
JOIN fitlake.gold.daily_recovery r
    ON s.user_id = r.user_id
    AND r.date = s.date + INTERVAL 1 DAY
ORDER BY s.user_id, s.date;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Weekly Recovery Patterns (day-of-week analysis)
--    Insight: recovery is typically lowest on Monday (after weekend activity)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    DAYNAME(date)                                  AS day_of_week,
    DAYOFWEEK(date)                                AS dow_num,
    ROUND(AVG(recovery_score), 1)                  AS avg_recovery,
    ROUND(AVG(hrv_rmssd), 1)                       AS avg_hrv_ms,
    COUNT(*)                                       AS sample_days,
    ROUND(SUM(CASE WHEN recovery_state = 'Green' THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 1)
                                                   AS pct_green_days
FROM fitlake.gold.daily_recovery
GROUP BY DAYNAME(date), DAYOFWEEK(date)
ORDER BY dow_num;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. HRV Trend Detection (improving vs declining)
--    Uses 7-day vs 28-day moving average comparison
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    user_id,
    date,
    hrv_rmssd,
    AVG(hrv_rmssd) OVER (
        PARTITION BY user_id ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS hrv_7d_avg,
    AVG(hrv_rmssd) OVER (
        PARTITION BY user_id ORDER BY date
        ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
    ) AS hrv_28d_avg,
    CASE
        WHEN AVG(hrv_rmssd) OVER (PARTITION BY user_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
           > AVG(hrv_rmssd) OVER (PARTITION BY user_id ORDER BY date ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)
        THEN 'Improving'
        ELSE 'Declining'
    END AS hrv_trend
FROM fitlake.gold.daily_recovery
ORDER BY user_id, date;


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. Sleep Quality Impact on Recovery
--    Shows how sleep debt correlates with recovery score
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    s.user_id,
    s.date,
    s.total_sleep_hours,
    s.sleep_efficiency_pct,
    s.sleep_debt_hours,
    s.sleep_quality_tier,
    r.recovery_score,
    r.recovery_state
FROM fitlake.gold.sleep_analytics s
JOIN fitlake.gold.daily_recovery r
    ON s.user_id = r.user_id AND s.date = r.date
ORDER BY s.user_id, s.date;


-- ─────────────────────────────────────────────────────────────────────────────
-- 6. User Percentile Benchmarks
--    "Your recovery is better than X% of users"
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    user_id,
    ROUND(avg_recovery, 1)       AS avg_recovery_score,
    ROUND(avg_hrv, 1)            AS avg_hrv_ms,
    days_tracked,
    ROUND(recovery_percentile, 1) AS recovery_percentile,
    CASE
        WHEN recovery_percentile >= 75 THEN 'Elite'
        WHEN recovery_percentile >= 50 THEN 'Above Average'
        WHEN recovery_percentile >= 25 THEN 'Average'
        ELSE 'Below Average'
    END AS performance_tier
FROM fitlake.gold.cohort_benchmarks
ORDER BY recovery_percentile DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 7. Iceberg Time Travel — Compare recovery scores 30 days ago vs now
--    Demonstrates Iceberg's built-in versioning capability
-- ─────────────────────────────────────────────────────────────────────────────
-- Current snapshot:
SELECT user_id, AVG(recovery_score) AS current_avg
FROM fitlake.gold.daily_recovery
GROUP BY user_id;

-- As of 30 days ago (snapshot-based time travel):
-- SELECT user_id, AVG(recovery_score) AS historical_avg
-- FROM fitlake.gold.daily_recovery FOR SYSTEM_TIME AS OF (CURRENT_TIMESTAMP - INTERVAL 30 DAYS)
-- GROUP BY user_id;


-- ─────────────────────────────────────────────────────────────────────────────
-- 8. Anomaly Detection — Unusual sensor readings
--    Records that were flagged and quarantined during Silver processing
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    source_table,
    DATE_TRUNC('week', CAST(date AS DATE)) AS week,
    COUNT(*)                               AS quarantined_records,
    COUNT(DISTINCT user_id)                AS affected_users,
    STRING_AGG(DISTINCT rejection_reason, ' | ')
        FILTER (WHERE rejection_reason != '')
                                           AS rejection_reasons
FROM fitlake.silver.quarantine
GROUP BY source_table, DATE_TRUNC('week', CAST(date AS DATE))
ORDER BY week DESC;
