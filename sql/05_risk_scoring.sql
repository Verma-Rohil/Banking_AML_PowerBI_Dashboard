-- ============================================================
-- 05_risk_scoring.sql
-- Multi-Factor Customer Risk Scoring System
-- Database: MySQL 8.0+ | Schema: aml_monitoring
-- ============================================================

USE aml_monitoring;

-- ============================================================
-- 1. Comprehensive Customer Risk Factor Analysis
-- Skills: CTE, CASE WHEN, Multi-factor scoring, Aggregation
-- ============================================================
WITH customer_risk_factors AS (
    SELECT 
        customer_id,
        COUNT(*) AS total_txns,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_txns,
        MAX(amount) AS max_single_txn,
        ROUND(AVG(amount), 2) AS avg_txn,
        ROUND(SUM(amount), 2) AS total_volume,
        COUNT(DISTINCT transaction_type) AS type_diversity,
        COUNT(DISTINCT recipient_id) AS unique_recipients,
        ROUND(MAX(amount) / NULLIF(AVG(amount), 0), 2) AS max_to_avg_ratio,
        -- Balance drain metrics
        SUM(CASE WHEN new_balance_orig = 0 AND old_balance_orig > 0 THEN 1 ELSE 0 END) AS full_drain_count,
        -- Time span of activity
        MAX(step) - MIN(step) AS activity_span_hours,
        -- Transfer/CashOut concentration
        ROUND(
            SUM(CASE WHEN transaction_type IN ('TRANSFER', 'CASH_OUT') THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2) AS high_risk_type_pct
    FROM fact_transactions
    GROUP BY customer_id
),
risk_scored AS (
    SELECT 
        *,
        -- ========== RISK SCORE CALCULATION (0-100) ==========
        -- Factor 1: Known Fraud (40 pts max)
        (CASE WHEN fraud_txns > 0 THEN 40 ELSE 0 END) +
        -- Factor 2: Transaction Amount Anomaly (25 pts max)
        (CASE 
            WHEN max_to_avg_ratio > 50 THEN 25
            WHEN max_to_avg_ratio > 20 THEN 20
            WHEN max_to_avg_ratio > 10 THEN 15
            WHEN max_to_avg_ratio > 5 THEN 10
            ELSE 3
        END) +
        -- Factor 3: Volume Risk (15 pts max)
        (CASE 
            WHEN total_volume > 5000000 THEN 15
            WHEN total_volume > 1000000 THEN 12
            WHEN total_volume > 500000 THEN 8
            ELSE 3
        END) +
        -- Factor 4: Balance Drain Behavior (10 pts max)
        (CASE 
            WHEN full_drain_count > 3 THEN 10
            WHEN full_drain_count > 1 THEN 7
            WHEN full_drain_count = 1 THEN 4
            ELSE 0
        END) +
        -- Factor 5: High-risk Type Concentration (10 pts max)
        (CASE 
            WHEN high_risk_type_pct > 80 THEN 10
            WHEN high_risk_type_pct > 50 THEN 7
            ELSE 3
        END) AS raw_risk_score
    FROM customer_risk_factors
)
SELECT 
    customer_id,
    total_txns,
    fraud_txns,
    total_volume,
    max_single_txn,
    avg_txn,
    max_to_avg_ratio,
    unique_recipients,
    full_drain_count,
    high_risk_type_pct,
    LEAST(raw_risk_score, 100) AS risk_score,
    CASE
        WHEN fraud_txns > 0 THEN 'CRITICAL'
        WHEN raw_risk_score >= 60 THEN 'HIGH'
        WHEN raw_risk_score >= 35 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_tier
FROM risk_scored
ORDER BY raw_risk_score DESC;

-- ============================================================
-- 2. Risk Tier Distribution Summary
-- ============================================================
WITH scored_customers AS (
    SELECT 
        customer_id,
        CASE
            WHEN SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) > 0 THEN 'CRITICAL'
            WHEN MAX(amount) / NULLIF(AVG(amount), 0) > 10 AND SUM(amount) > 1000000 THEN 'HIGH'
            WHEN MAX(amount) / NULLIF(AVG(amount), 0) > 5 OR SUM(amount) > 500000 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_tier
    FROM fact_transactions
    GROUP BY customer_id
)
SELECT 
    risk_tier,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM scored_customers), 2) AS pct_of_customers
FROM scored_customers
GROUP BY risk_tier
ORDER BY FIELD(risk_tier, 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW');

-- ============================================================
-- 3. Recipient Risk Scoring (Who receives suspicious money?)
-- ============================================================
WITH recipient_risk AS (
    SELECT 
        recipient_id,
        COUNT(*) AS incoming_txns,
        ROUND(SUM(amount), 2) AS total_received,
        ROUND(AVG(amount), 2) AS avg_received,
        COUNT(DISTINCT customer_id) AS unique_senders,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_received,
        SUM(CASE WHEN transaction_type = 'TRANSFER' THEN 1 ELSE 0 END) AS transfer_count,
        SUM(CASE WHEN transaction_type = 'CASH_OUT' THEN 1 ELSE 0 END) AS cashout_count
    FROM fact_transactions
    WHERE transaction_type IN ('TRANSFER', 'CASH_OUT')
    GROUP BY recipient_id
)
SELECT 
    recipient_id,
    incoming_txns,
    total_received,
    avg_received,
    unique_senders,
    fraud_received,
    CASE 
        WHEN fraud_received > 0 THEN 'CRITICAL'
        WHEN unique_senders > 20 AND total_received > 1000000 THEN 'HIGH'
        WHEN unique_senders > 10 OR total_received > 500000 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS recipient_risk_tier
FROM recipient_risk
ORDER BY fraud_received DESC, total_received DESC
LIMIT 100;

-- ============================================================
-- 4. Peer Group Comparison (Within-Group Anomaly)
-- Skills: Window function for percentile within group
-- ============================================================
WITH customer_stats AS (
    SELECT 
        customer_id,
        COUNT(*) AS txn_count,
        SUM(amount) AS total_amount,
        AVG(amount) AS avg_amount,
        NTILE(10) OVER (ORDER BY SUM(amount)) AS volume_decile,
        NTILE(10) OVER (ORDER BY COUNT(*)) AS frequency_decile
    FROM fact_transactions
    GROUP BY customer_id
)
SELECT 
    cs.customer_id,
    cs.txn_count,
    cs.total_amount,
    cs.volume_decile,
    cs.frequency_decile,
    dc.risk_tier,
    dc.fraud_count,
    CASE 
        WHEN cs.volume_decile >= 9 AND cs.frequency_decile >= 9 THEN 'OUTLIER - High Volume & Frequency'
        WHEN cs.volume_decile >= 9 THEN 'OUTLIER - High Volume'
        WHEN cs.frequency_decile >= 9 THEN 'OUTLIER - High Frequency'
        ELSE 'NORMAL'
    END AS peer_comparison
FROM customer_stats cs
LEFT JOIN dim_customers dc ON cs.customer_id = dc.customer_id
WHERE cs.volume_decile >= 9 OR cs.frequency_decile >= 9
ORDER BY cs.total_amount DESC
LIMIT 50;
