-- ============================================================
-- 02_data_loading.sql
-- Data Loading & Validation Queries for MySQL
-- Run AFTER ETL pipeline has loaded data into fact_transactions
-- ============================================================

USE aml_monitoring;

-- ============================================================
-- SECTION 1: Verify data load
-- ============================================================

-- Check total row count (should be ~6.3M)
SELECT COUNT(*) AS total_rows FROM fact_transactions;

-- Check for NULL values in critical columns
SELECT 
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer,
    SUM(CASE WHEN recipient_id IS NULL THEN 1 ELSE 0 END) AS null_recipient,
    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS null_amount,
    SUM(CASE WHEN transaction_type IS NULL THEN 1 ELSE 0 END) AS null_type,
    SUM(CASE WHEN is_fraud IS NULL THEN 1 ELSE 0 END) AS null_fraud
FROM fact_transactions;

-- Check data types and ranges
SELECT 
    MIN(amount) AS min_amount,
    MAX(amount) AS max_amount,
    AVG(amount) AS avg_amount,
    MIN(step) AS min_step,
    MAX(step) AS max_step,
    COUNT(DISTINCT transaction_type) AS unique_types,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(DISTINCT recipient_id) AS unique_recipients
FROM fact_transactions;

-- Distribution of transaction types
SELECT 
    transaction_type,
    COUNT(*) AS txn_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_transactions), 2) AS percentage,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(SUM(amount), 2) AS total_volume
FROM fact_transactions
GROUP BY transaction_type
ORDER BY txn_count DESC;

-- Fraud distribution
SELECT 
    is_fraud,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_transactions), 4) AS percentage
FROM fact_transactions
GROUP BY is_fraud;

-- ============================================================
-- SECTION 2: Populate dim_customers (aggregated profiles)
-- ============================================================

INSERT INTO dim_customers (
    customer_id, total_transactions, total_amount, avg_transaction,
    max_transaction, fraud_count, first_seen_step, last_seen_step
)
SELECT 
    customer_id,
    COUNT(*) AS total_transactions,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(amount), 2) AS avg_transaction,
    ROUND(MAX(amount), 2) AS max_transaction,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
    MIN(step) AS first_seen_step,
    MAX(step) AS last_seen_step
FROM fact_transactions
GROUP BY customer_id
ON DUPLICATE KEY UPDATE
    total_transactions = VALUES(total_transactions),
    total_amount = VALUES(total_amount),
    avg_transaction = VALUES(avg_transaction),
    max_transaction = VALUES(max_transaction),
    fraud_count = VALUES(fraud_count),
    first_seen_step = VALUES(first_seen_step),
    last_seen_step = VALUES(last_seen_step);

-- ============================================================
-- SECTION 3: Update risk tiers in dim_customers
-- ============================================================

-- Update risk tiers based on multi-factor scoring
UPDATE dim_customers
SET 
    risk_score = LEAST(100, (
        (CASE WHEN fraud_count > 0 THEN 40 ELSE 0 END) +
        (CASE 
            WHEN max_transaction / NULLIF(avg_transaction, 0) > 10 THEN 25 
            WHEN max_transaction / NULLIF(avg_transaction, 0) > 5 THEN 15 
            ELSE 5 
        END) +
        (CASE 
            WHEN total_amount > 1000000 THEN 20 
            WHEN total_amount > 500000 THEN 10 
            ELSE 5 
        END) +
        (CASE WHEN total_transactions >= 20 THEN 15 ELSE 5 END)
    )),
    risk_tier = CASE
        WHEN fraud_count > 0 THEN 'CRITICAL'
        WHEN max_transaction / NULLIF(avg_transaction, 0) > 10 AND total_amount > 1000000 THEN 'HIGH'
        WHEN max_transaction / NULLIF(avg_transaction, 0) > 5 OR total_amount > 500000 THEN 'MEDIUM'
        ELSE 'LOW'
    END;

-- ============================================================
-- SECTION 4: Verification queries after dim population
-- ============================================================

-- Customer dimension summary
SELECT 
    risk_tier,
    COUNT(*) AS customer_count,
    ROUND(AVG(total_amount), 2) AS avg_total_amount,
    ROUND(AVG(risk_score), 1) AS avg_risk_score
FROM dim_customers
GROUP BY risk_tier
ORDER BY FIELD(risk_tier, 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW');

-- Check dim_transaction_types
SELECT * FROM dim_transaction_types ORDER BY risk_weight DESC;

-- Verify derived columns
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN transaction_hour IS NULL THEN 1 ELSE 0 END) AS null_hours,
    SUM(CASE WHEN transaction_day IS NULL THEN 1 ELSE 0 END) AS null_days,
    SUM(CASE WHEN balance_change IS NULL THEN 1 ELSE 0 END) AS null_balance_change
FROM fact_transactions;

-- Sample data check (first 10 rows)
SELECT * FROM fact_transactions LIMIT 10;
SELECT * FROM dim_customers ORDER BY risk_score DESC LIMIT 10;
