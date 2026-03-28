-- ============================================================
-- 06_monitoring_reports.sql
-- Daily, Weekly & Monthly Compliance Monitoring Reports
-- Database: MySQL 8.0+ | Schema: aml_monitoring
-- ============================================================

USE aml_monitoring;

-- ============================================================
-- REPORT 1: Daily Transaction Summary
-- Skills: CTE, Aggregation, Day-over-Day comparison
-- ============================================================
WITH daily_summary AS (
    SELECT 
        transaction_day,
        COUNT(*) AS total_txns,
        ROUND(SUM(amount), 2) AS total_volume,
        ROUND(AVG(amount), 2) AS avg_amount,
        ROUND(MAX(amount), 2) AS max_amount,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
        ROUND(AVG(CASE WHEN is_fraud = 1 THEN 1.0 ELSE 0.0 END) * 100, 4) AS fraud_rate_pct,
        COUNT(DISTINCT customer_id) AS active_customers,
        SUM(CASE WHEN transaction_type = 'TRANSFER' THEN amount ELSE 0 END) AS transfer_volume,
        SUM(CASE WHEN transaction_type = 'CASH_OUT' THEN amount ELSE 0 END) AS cashout_volume
    FROM fact_transactions
    GROUP BY transaction_day
)
SELECT 
    transaction_day,
    total_txns,
    total_volume,
    avg_amount,
    fraud_count,
    fraud_rate_pct,
    active_customers,
    -- Day-over-Day Change
    LAG(total_txns) OVER (ORDER BY transaction_day) AS prev_day_txns,
    ROUND(
        (total_txns - LAG(total_txns) OVER (ORDER BY transaction_day)) * 100.0 /
        NULLIF(LAG(total_txns) OVER (ORDER BY transaction_day), 0), 2
    ) AS txn_change_pct,
    -- Volume change
    ROUND(
        (total_volume - LAG(total_volume) OVER (ORDER BY transaction_day)) * 100.0 /
        NULLIF(LAG(total_volume) OVER (ORDER BY transaction_day), 0), 2
    ) AS volume_change_pct,
    -- 7-day Moving Averages
    ROUND(AVG(total_txns) OVER (ORDER BY transaction_day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0) AS ma7_txns,
    ROUND(AVG(fraud_count) OVER (ORDER BY transaction_day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS ma7_fraud
FROM daily_summary
ORDER BY transaction_day;

-- ============================================================
-- REPORT 2: Transaction Type Breakdown by Day
-- Skills: PIVOT-style query, Conditional Aggregation
-- ============================================================
SELECT 
    transaction_day,
    SUM(CASE WHEN transaction_type = 'CASH_IN' THEN 1 ELSE 0 END) AS cash_in_count,
    SUM(CASE WHEN transaction_type = 'CASH_OUT' THEN 1 ELSE 0 END) AS cash_out_count,
    SUM(CASE WHEN transaction_type = 'TRANSFER' THEN 1 ELSE 0 END) AS transfer_count,
    SUM(CASE WHEN transaction_type = 'PAYMENT' THEN 1 ELSE 0 END) AS payment_count,
    SUM(CASE WHEN transaction_type = 'DEBIT' THEN 1 ELSE 0 END) AS debit_count,
    -- Volume by type
    ROUND(SUM(CASE WHEN transaction_type = 'TRANSFER' THEN amount ELSE 0 END), 2) AS transfer_volume,
    ROUND(SUM(CASE WHEN transaction_type = 'CASH_OUT' THEN amount ELSE 0 END), 2) AS cashout_volume,
    -- Fraud by high-risk types
    SUM(CASE WHEN transaction_type IN ('TRANSFER', 'CASH_OUT') AND is_fraud = 1 THEN 1 ELSE 0 END) AS high_risk_fraud
FROM fact_transactions
GROUP BY transaction_day
ORDER BY transaction_day;

-- ============================================================
-- REPORT 3: Alert Summary Dashboard
-- Skills: JOINs, Aggregation, Alert effectiveness
-- ============================================================
SELECT 
    a.alert_type,
    a.severity,
    COUNT(*) AS total_alerts,
    SUM(CASE WHEN a.reviewed = 1 THEN 1 ELSE 0 END) AS reviewed_count,
    ROUND(AVG(CASE WHEN a.reviewed = 1 THEN 1.0 ELSE 0.0 END) * 100, 1) AS review_rate_pct,
    SUM(CASE WHEN a.false_positive = 1 THEN 1 ELSE 0 END) AS false_positives,
    SUM(CASE WHEN a.false_positive = 0 THEN 1 ELSE 0 END) AS true_alerts,
    ROUND(
        SUM(CASE WHEN a.false_positive = 1 THEN 1 ELSE 0 END) * 100.0 /
        NULLIF(SUM(CASE WHEN a.false_positive IS NOT NULL THEN 1 ELSE 0 END), 0), 1
    ) AS false_positive_rate_pct,
    ROUND(AVG(ft.amount), 2) AS avg_alerted_amount
FROM alerts a
LEFT JOIN fact_transactions ft ON a.transaction_id = ft.transaction_id
GROUP BY a.alert_type, a.severity
ORDER BY a.alert_type, FIELD(a.severity, 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW');

-- ============================================================
-- REPORT 4: Customer Risk Tier Distribution Over Time
-- Skills: Window function, Trend analysis
-- ============================================================
WITH customer_daily_risk AS (
    SELECT 
        ft.transaction_day,
        dc.risk_tier,
        COUNT(DISTINCT ft.customer_id) AS active_customers,
        ROUND(SUM(ft.amount), 2) AS total_volume,
        SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count
    FROM fact_transactions ft
    JOIN dim_customers dc ON ft.customer_id = dc.customer_id
    GROUP BY ft.transaction_day, dc.risk_tier
)
SELECT 
    transaction_day,
    risk_tier,
    active_customers,
    total_volume,
    fraud_count,
    -- Running total of fraud by tier
    SUM(fraud_count) OVER (PARTITION BY risk_tier ORDER BY transaction_day) AS cumulative_fraud
FROM customer_daily_risk
ORDER BY transaction_day, FIELD(risk_tier, 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW');

-- ============================================================
-- REPORT 5: Compliance KPI Scorecard
-- Skills: Multiple subqueries, KPI calculation
-- ============================================================
SELECT 
    -- Transaction KPIs
    (SELECT COUNT(*) FROM fact_transactions) AS total_transactions,
    (SELECT ROUND(SUM(amount), 2) FROM fact_transactions) AS total_volume,
    (SELECT ROUND(AVG(amount), 2) FROM fact_transactions) AS avg_transaction_amount,
    
    -- Fraud KPIs
    (SELECT COUNT(*) FROM fact_transactions WHERE is_fraud = 1) AS total_fraud_txns,
    (SELECT ROUND(AVG(CASE WHEN is_fraud = 1 THEN 1.0 ELSE 0.0 END) * 100, 4) FROM fact_transactions) AS overall_fraud_rate_pct,
    (SELECT ROUND(SUM(CASE WHEN is_fraud = 1 THEN amount ELSE 0 END), 2) FROM fact_transactions) AS total_fraud_volume,
    
    -- Alert KPIs
    (SELECT COUNT(*) FROM alerts) AS total_alerts_generated,
    (SELECT COUNT(*) FROM alerts WHERE reviewed = 1) AS alerts_reviewed,
    (SELECT ROUND(AVG(CASE WHEN false_positive = 1 THEN 1.0 ELSE 0.0 END) * 100, 1) FROM alerts WHERE false_positive IS NOT NULL) AS false_positive_rate_pct,
    
    -- Customer Risk KPIs
    (SELECT COUNT(*) FROM dim_customers WHERE risk_tier = 'CRITICAL') AS critical_risk_customers,
    (SELECT COUNT(*) FROM dim_customers WHERE risk_tier = 'HIGH') AS high_risk_customers,
    (SELECT COUNT(*) FROM dim_customers WHERE risk_tier = 'MEDIUM') AS medium_risk_customers,
    (SELECT COUNT(*) FROM dim_customers WHERE risk_tier = 'LOW') AS low_risk_customers,
    
    -- Detection KPIs
    (SELECT COUNT(*) FROM fact_transactions WHERE is_fraud = 1 AND is_flagged_fraud = 1) AS correctly_flagged,
    (SELECT COUNT(*) FROM fact_transactions WHERE is_fraud = 1 AND is_flagged_fraud = 0) AS missed_fraud,
    (SELECT ROUND(
        COUNT(CASE WHEN is_fraud = 1 AND is_flagged_fraud = 1 THEN 1 END) * 100.0 /
        NULLIF(COUNT(CASE WHEN is_fraud = 1 THEN 1 END), 0), 2
    ) FROM fact_transactions) AS detection_rate_pct;

-- ============================================================
-- REPORT 6: Top 20 Highest-Risk Transactions (for review queue)
-- Skills: Multi-table JOIN, Composite scoring
-- ============================================================
SELECT 
    ft.transaction_id,
    ft.customer_id,
    ft.recipient_id,
    ft.transaction_type,
    ft.amount,
    ft.old_balance_orig,
    ft.new_balance_orig,
    ft.is_fraud,
    dc.risk_tier AS customer_risk_tier,
    dc.risk_score AS customer_risk_score,
    dt.risk_weight AS type_risk_weight,
    -- Composite transaction risk score
    ROUND(
        dc.risk_score * dt.risk_weight * 
        (ft.amount / NULLIF(dc.avg_transaction, 0)), 2
    ) AS transaction_risk_score
FROM fact_transactions ft
LEFT JOIN dim_customers dc ON ft.customer_id = dc.customer_id
LEFT JOIN dim_transaction_types dt ON ft.transaction_type = dt.type_name
WHERE dc.risk_tier IN ('CRITICAL', 'HIGH')
ORDER BY transaction_risk_score DESC
LIMIT 20;
