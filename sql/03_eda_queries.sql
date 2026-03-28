-- ============================================================
-- 03_eda_queries.sql
-- Exploratory Data Analysis Queries
-- Database: MySQL 8.0+ | Schema: aml_monitoring
-- ============================================================

USE aml_monitoring;

-- ============================================================
-- 1. Transaction Volume by Type
-- Skills: GROUP BY, COUNT, Aggregate Functions
-- ============================================================
SELECT 
    transaction_type,
    COUNT(*) AS txn_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_transactions), 2) AS pct_of_total,
    ROUND(SUM(amount), 2) AS total_volume,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(MIN(amount), 2) AS min_amount,
    ROUND(MAX(amount), 2) AS max_amount,
    ROUND(STDDEV(amount), 2) AS std_amount
FROM fact_transactions
GROUP BY transaction_type
ORDER BY txn_count DESC;

-- ============================================================
-- 2. Fraud Rate by Transaction Type
-- Skills: CASE WHEN, AVG, Conditional Aggregation
-- ============================================================
SELECT 
    transaction_type,
    COUNT(*) AS total_txns,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_txns,
    SUM(CASE WHEN is_fraud = 0 THEN 1 ELSE 0 END) AS legit_txns,
    ROUND(AVG(CASE WHEN is_fraud = 1 THEN 1.0 ELSE 0.0 END) * 100, 4) AS fraud_rate_pct,
    ROUND(AVG(CASE WHEN is_fraud = 1 THEN amount ELSE NULL END), 2) AS avg_fraud_amount,
    ROUND(AVG(CASE WHEN is_fraud = 0 THEN amount ELSE NULL END), 2) AS avg_legit_amount,
    ROUND(
        AVG(CASE WHEN is_fraud = 1 THEN amount ELSE NULL END) / 
        NULLIF(AVG(CASE WHEN is_fraud = 0 THEN amount ELSE NULL END), 0), 
    2) AS fraud_to_legit_ratio
FROM fact_transactions
GROUP BY transaction_type
ORDER BY fraud_rate_pct DESC;

-- ============================================================
-- 3. Hourly Transaction Patterns (Time-of-Day Analysis)
-- Skills: MOD arithmetic, GROUP BY derived column
-- ============================================================
SELECT 
    transaction_hour,
    COUNT(*) AS txn_count,
    ROUND(SUM(amount), 2) AS total_volume,
    ROUND(AVG(amount), 2) AS avg_amount,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(AVG(CASE WHEN is_fraud = 1 THEN 1.0 ELSE 0.0 END) * 100, 4) AS fraud_rate_pct
FROM fact_transactions
GROUP BY transaction_hour
ORDER BY transaction_hour;

-- ============================================================
-- 4. Daily Transaction Trends
-- Skills: GROUP BY, Window Functions (Running Totals)
-- ============================================================
WITH daily_stats AS (
    SELECT 
        transaction_day,
        COUNT(*) AS daily_txns,
        ROUND(SUM(amount), 2) AS daily_volume,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS daily_fraud
    FROM fact_transactions
    GROUP BY transaction_day
)
SELECT 
    transaction_day,
    daily_txns,
    daily_volume,
    daily_fraud,
    ROUND(daily_fraud * 100.0 / daily_txns, 4) AS daily_fraud_rate,
    -- Running totals
    SUM(daily_txns) OVER (ORDER BY transaction_day) AS cumulative_txns,
    SUM(daily_volume) OVER (ORDER BY transaction_day) AS cumulative_volume,
    SUM(daily_fraud) OVER (ORDER BY transaction_day) AS cumulative_fraud,
    -- Moving averages (7-day)
    ROUND(AVG(daily_txns) OVER (
        ORDER BY transaction_day 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 0) AS ma_7day_txns,
    ROUND(AVG(daily_volume) OVER (
        ORDER BY transaction_day 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS ma_7day_volume
FROM daily_stats
ORDER BY transaction_day;

-- ============================================================
-- 5. Amount Distribution Percentiles
-- Skills: NTILE, Window Functions, Percentile Analysis
-- ============================================================
WITH amount_percentiles AS (
    SELECT 
        transaction_type,
        amount,
        NTILE(100) OVER (PARTITION BY transaction_type ORDER BY amount) AS percentile
    FROM fact_transactions
)
SELECT 
    transaction_type,
    percentile,
    ROUND(MIN(amount), 2) AS min_amount,
    ROUND(MAX(amount), 2) AS max_amount,
    ROUND(AVG(amount), 2) AS avg_amount,
    COUNT(*) AS count_in_bucket
FROM amount_percentiles
WHERE percentile IN (25, 50, 75, 90, 95, 99)
GROUP BY transaction_type, percentile
ORDER BY transaction_type, percentile;

-- ============================================================
-- 6. Top 20 Customers by Transaction Volume
-- Skills: RANK(), DENSE_RANK(), Window Functions
-- ============================================================
WITH customer_volume AS (
    SELECT 
        customer_id,
        COUNT(*) AS txn_count,
        ROUND(SUM(amount), 2) AS total_volume,
        ROUND(AVG(amount), 2) AS avg_amount,
        MAX(amount) AS max_single_txn,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
        COUNT(DISTINCT transaction_type) AS types_used,
        DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS volume_rank,
        DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS frequency_rank
    FROM fact_transactions
    GROUP BY customer_id
)
SELECT *
FROM customer_volume
WHERE volume_rank <= 20
ORDER BY volume_rank;

-- ============================================================
-- 7. Fraud Transactions Deep Dive
-- Skills: Subqueries, Multiple Aggregations
-- ============================================================
SELECT 
    ft.transaction_type,
    COUNT(*) AS fraud_txns,
    ROUND(AVG(ft.amount), 2) AS avg_fraud_amount,
    ROUND(MAX(ft.amount), 2) AS max_fraud_amount,
    ROUND(SUM(ft.amount), 2) AS total_fraud_volume,
    -- Compare with overall averages
    ROUND(AVG(ft.amount) / (
        SELECT AVG(amount) FROM fact_transactions WHERE transaction_type = ft.transaction_type
    ), 2) AS fraud_vs_overall_ratio,
    -- Balance drain analysis
    ROUND(AVG(ft.old_balance_orig - ft.new_balance_orig), 2) AS avg_balance_drained,
    ROUND(AVG(
        CASE WHEN ft.old_balance_orig > 0 
        THEN (ft.old_balance_orig - ft.new_balance_orig) / ft.old_balance_orig * 100 
        ELSE 0 END
    ), 2) AS avg_drain_pct
FROM fact_transactions ft
WHERE ft.is_fraud = 1
GROUP BY ft.transaction_type
ORDER BY fraud_txns DESC;

-- ============================================================
-- 8. Flagged vs Actual Fraud Analysis
-- Skills: Cross-tabulation, System effectiveness
-- ============================================================
SELECT 
    is_fraud,
    is_flagged_fraud,
    COUNT(*) AS count,
    ROUND(AVG(amount), 2) AS avg_amount,
    CASE
        WHEN is_fraud = 1 AND is_flagged_fraud = 1 THEN 'TRUE POSITIVE (correctly flagged)'
        WHEN is_fraud = 1 AND is_flagged_fraud = 0 THEN 'FALSE NEGATIVE (missed fraud)'
        WHEN is_fraud = 0 AND is_flagged_fraud = 1 THEN 'FALSE POSITIVE (wrongly flagged)'
        WHEN is_fraud = 0 AND is_flagged_fraud = 0 THEN 'TRUE NEGATIVE (correctly cleared)'
    END AS classification
FROM fact_transactions
GROUP BY is_fraud, is_flagged_fraud
ORDER BY is_fraud DESC, is_flagged_fraud DESC;

-- ============================================================
-- 9. Balance Anomalies (Zero Balance After Transaction)
-- Skills: Conditional Filtering, Pattern Detection
-- ============================================================
SELECT 
    transaction_type,
    COUNT(*) AS zero_balance_txns,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(AVG(old_balance_orig), 2) AS avg_old_balance,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_among_zero_balance,
    ROUND(AVG(CASE WHEN is_fraud = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS fraud_rate_pct
FROM fact_transactions
WHERE new_balance_orig = 0 AND old_balance_orig > 0
GROUP BY transaction_type
ORDER BY fraud_rate_pct DESC;

-- ============================================================
-- 10. Amount Bands Analysis
-- Skills: CASE WHEN for bucketing, Distribution analysis
-- ============================================================
SELECT 
    CASE 
        WHEN amount < 100 THEN '0-100'
        WHEN amount < 1000 THEN '100-1K'
        WHEN amount < 10000 THEN '1K-10K'
        WHEN amount < 100000 THEN '10K-100K'
        WHEN amount < 1000000 THEN '100K-1M'
        ELSE '1M+'
    END AS amount_band,
    COUNT(*) AS txn_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_transactions), 2) AS pct_of_total,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(AVG(CASE WHEN is_fraud = 1 THEN 1.0 ELSE 0.0 END) * 100, 4) AS fraud_rate_pct
FROM fact_transactions
GROUP BY amount_band
ORDER BY MIN(amount);
