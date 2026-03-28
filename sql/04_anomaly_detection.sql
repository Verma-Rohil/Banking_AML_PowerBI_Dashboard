-- ============================================================
-- 04_anomaly_detection.sql
-- SQL-Based Anomaly Detection for AML Monitoring
-- Database: MySQL 8.0+ | Schema: aml_monitoring
-- ============================================================

USE aml_monitoring;

-- ============================================================
-- ANOMALY 1: Transaction Velocity Detection
-- (CTE + Window Function)
-- Flags customers with unusually high transaction frequency
-- ============================================================
WITH hourly_activity AS (
    SELECT 
        customer_id,
        transaction_day,
        transaction_hour,
        COUNT(*) AS txn_count,
        SUM(amount) AS hourly_total
    FROM fact_transactions
    GROUP BY customer_id, transaction_day, transaction_hour
),
velocity_stats AS (
    SELECT 
        customer_id,
        transaction_day,
        transaction_hour,
        txn_count,
        hourly_total,
        AVG(txn_count) OVER (PARTITION BY customer_id) AS avg_hourly_txn,
        STDDEV(txn_count) OVER (PARTITION BY customer_id) AS std_hourly_txn
    FROM hourly_activity
)
SELECT 
    customer_id,
    transaction_day,
    transaction_hour,
    txn_count,
    hourly_total,
    avg_hourly_txn,
    std_hourly_txn,
    ROUND((txn_count - avg_hourly_txn) / NULLIF(std_hourly_txn, 0), 2) AS velocity_z_score,
    'VELOCITY_ALERT' AS alert_type,
    CASE 
        WHEN (txn_count - avg_hourly_txn) / NULLIF(std_hourly_txn, 0) > 3 THEN 'CRITICAL'
        WHEN (txn_count - avg_hourly_txn) / NULLIF(std_hourly_txn, 0) > 2 THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS severity
FROM velocity_stats
WHERE txn_count > avg_hourly_txn + 2 * COALESCE(std_hourly_txn, 0)
ORDER BY velocity_z_score DESC
LIMIT 100;

-- ============================================================
-- ANOMALY 2: Rapid Balance Drain Detection
-- (Window Function: LAG)
-- Detects accounts rapidly depleting their balance (money laundering indicator)
-- ============================================================
WITH balance_tracking AS (
    SELECT 
        customer_id,
        transaction_id,
        step,
        transaction_type,
        amount,
        old_balance_orig,
        new_balance_orig,
        LAG(old_balance_orig, 1) OVER (PARTITION BY customer_id ORDER BY step) AS prev_balance,
        LAG(step, 1) OVER (PARTITION BY customer_id ORDER BY step) AS prev_step,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY step) AS txn_sequence,
        is_fraud
    FROM fact_transactions
    WHERE transaction_type IN ('TRANSFER', 'CASH_OUT')
),
drain_analysis AS (
    SELECT 
        *,
        CASE 
            WHEN old_balance_orig > 0 
            THEN ROUND((old_balance_orig - new_balance_orig) / old_balance_orig * 100, 2)
            ELSE 0 
        END AS drain_percentage,
        step - COALESCE(prev_step, step) AS steps_between_txns
    FROM balance_tracking
)
SELECT 
    customer_id,
    transaction_id,
    transaction_type,
    amount,
    old_balance_orig,
    new_balance_orig,
    drain_percentage,
    steps_between_txns,
    is_fraud,
    'BALANCE_DRAIN' AS alert_type,
    CASE 
        WHEN drain_percentage >= 95 AND steps_between_txns <= 1 THEN 'CRITICAL'
        WHEN drain_percentage >= 80 THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS severity
FROM drain_analysis
WHERE drain_percentage > 80
ORDER BY drain_percentage DESC
LIMIT 200;

-- ============================================================
-- ANOMALY 3: Rolling Average Deviation Detection
-- (Window Function: Framed Aggregates)
-- Flags transactions significantly above a customer's rolling average
-- ============================================================
WITH rolling_stats AS (
    SELECT 
        transaction_id,
        customer_id,
        step,
        amount,
        transaction_type,
        is_fraud,
        AVG(amount) OVER (
            PARTITION BY customer_id 
            ORDER BY step 
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS rolling_avg,
        STDDEV(amount) OVER (
            PARTITION BY customer_id 
            ORDER BY step 
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS rolling_std,
        COUNT(*) OVER (
            PARTITION BY customer_id 
            ORDER BY step 
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS window_size
    FROM fact_transactions
)
SELECT 
    transaction_id,
    customer_id,
    amount,
    transaction_type,
    is_fraud,
    ROUND(rolling_avg, 2) AS rolling_avg,
    ROUND(rolling_std, 2) AS rolling_std,
    ROUND((amount - rolling_avg) / NULLIF(rolling_std, 0), 2) AS z_score,
    'ROLLING_DEVIATION' AS alert_type,
    CASE 
        WHEN (amount - rolling_avg) / NULLIF(rolling_std, 0) > 5 THEN 'CRITICAL'
        WHEN (amount - rolling_avg) / NULLIF(rolling_std, 0) > 3 THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS severity
FROM rolling_stats
WHERE window_size >= 5  -- need at least 5 prior transactions
  AND rolling_std > 0
  AND (amount - rolling_avg) / rolling_std > 3
ORDER BY z_score DESC
LIMIT 200;

-- ============================================================
-- ANOMALY 4: Large Round Amount Detection
-- Suspicious round-number transactions (structuring indicator)
-- ============================================================
SELECT 
    transaction_id,
    customer_id,
    recipient_id,
    transaction_type,
    amount,
    is_fraud,
    old_balance_orig,
    new_balance_orig,
    'ROUND_AMOUNT' AS alert_type,
    CASE 
        WHEN amount >= 1000000 AND MOD(amount, 100000) = 0 THEN 'CRITICAL'
        WHEN amount >= 100000 AND MOD(amount, 10000) = 0 THEN 'HIGH'
        WHEN amount >= 10000 AND MOD(amount, 1000) = 0 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity
FROM fact_transactions
WHERE amount >= 10000
  AND MOD(amount, 1000) = 0
  AND transaction_type IN ('TRANSFER', 'CASH_OUT')
ORDER BY amount DESC
LIMIT 200;

-- ============================================================
-- ANOMALY 5: Recipient Concentration (Fan-Out Detection)
-- Customers sending to many unique recipients rapidly
-- ============================================================
WITH daily_recipients AS (
    SELECT 
        customer_id,
        transaction_day,
        COUNT(DISTINCT recipient_id) AS unique_recipients,
        COUNT(*) AS txn_count,
        SUM(amount) AS daily_total
    FROM fact_transactions
    WHERE transaction_type IN ('TRANSFER', 'CASH_OUT', 'PAYMENT')
    GROUP BY customer_id, transaction_day
),
recipient_stats AS (
    SELECT 
        *,
        AVG(unique_recipients) OVER (PARTITION BY customer_id) AS avg_daily_recipients,
        STDDEV(unique_recipients) OVER (PARTITION BY customer_id) AS std_daily_recipients
    FROM daily_recipients
)
SELECT 
    customer_id,
    transaction_day,
    unique_recipients,
    txn_count,
    daily_total,
    ROUND(avg_daily_recipients, 2) AS avg_daily_recipients,
    'FAN_OUT' AS alert_type,
    CASE 
        WHEN unique_recipients > 10 THEN 'CRITICAL'
        WHEN unique_recipients > 5 THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS severity
FROM recipient_stats
WHERE unique_recipients > avg_daily_recipients + 2 * COALESCE(std_daily_recipients, 0)
  AND unique_recipients >= 3
ORDER BY unique_recipients DESC
LIMIT 100;

-- ============================================================
-- ANOMALY 6: Mismatch Detection
-- Amount doesn't match balance changes (data integrity / tampering)
-- ============================================================
SELECT 
    transaction_id,
    customer_id,
    transaction_type,
    amount,
    old_balance_orig,
    new_balance_orig,
    (old_balance_orig - new_balance_orig) AS expected_debit,
    ABS(amount - (old_balance_orig - new_balance_orig)) AS mismatch_amount,
    is_fraud,
    'BALANCE_MISMATCH' AS alert_type,
    'HIGH' AS severity
FROM fact_transactions
WHERE transaction_type IN ('TRANSFER', 'CASH_OUT')
  AND old_balance_orig > 0
  AND ABS(amount - (old_balance_orig - new_balance_orig)) > 1  -- allow for rounding
ORDER BY mismatch_amount DESC
LIMIT 200;

-- ============================================================
-- SUMMARY: Anomaly detection coverage analysis
-- How many fraud cases does each method catch?
-- ============================================================
SELECT 
    'Total Fraud Transactions' AS metric,
    COUNT(*) AS count
FROM fact_transactions WHERE is_fraud = 1

UNION ALL

SELECT 
    'Fraud with balance drain > 80%',
    COUNT(*)
FROM fact_transactions 
WHERE is_fraud = 1 
  AND old_balance_orig > 0 
  AND (old_balance_orig - new_balance_orig) / old_balance_orig > 0.80

UNION ALL

SELECT 
    'Fraud with zero final balance',
    COUNT(*)
FROM fact_transactions 
WHERE is_fraud = 1 AND new_balance_orig = 0

UNION ALL

SELECT 
    'Fraud in TRANSFER type',
    COUNT(*)
FROM fact_transactions 
WHERE is_fraud = 1 AND transaction_type = 'TRANSFER'

UNION ALL

SELECT 
    'Fraud in CASH_OUT type',
    COUNT(*)
FROM fact_transactions 
WHERE is_fraud = 1 AND transaction_type = 'CASH_OUT';
