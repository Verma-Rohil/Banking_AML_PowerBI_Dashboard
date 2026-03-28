-- ============================================================
-- 07_stored_procedures.sql
-- Stored Procedures & Views for Automated Monitoring
-- Database: MySQL 8.0+ | Schema: aml_monitoring
-- ============================================================

USE aml_monitoring;

-- ============================================================
-- PROCEDURE 1: Generate Daily Monitoring Report
-- Aggregates daily stats and inserts into reporting table
-- ============================================================
DROP PROCEDURE IF EXISTS sp_generate_daily_report;

DELIMITER //
CREATE PROCEDURE sp_generate_daily_report(IN p_report_day INT)
BEGIN
    DECLARE v_alert_count INT DEFAULT 0;
    DECLARE v_high_risk_count INT DEFAULT 0;
    
    -- Get alert count for the day (based on transactions from that day)
    SELECT COUNT(*) INTO v_alert_count
    FROM alerts a
    JOIN fact_transactions ft ON a.transaction_id = ft.transaction_id
    WHERE ft.transaction_day = p_report_day;
    
    -- Get high risk customer count
    SELECT COUNT(*) INTO v_high_risk_count
    FROM dim_customers
    WHERE risk_tier IN ('CRITICAL', 'HIGH');
    
    -- Insert/Update daily report
    INSERT INTO daily_monitoring_report (
        report_day, total_txns, total_volume, fraud_count,
        fraud_rate, avg_txn_amount, max_txn_amount,
        alert_count, high_risk_customers
    )
    SELECT 
        p_report_day,
        COUNT(*),
        ROUND(SUM(amount), 2),
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END),
        ROUND(AVG(CASE WHEN is_fraud = 1 THEN 1.0 ELSE 0.0 END) * 100, 4),
        ROUND(AVG(amount), 2),
        ROUND(MAX(amount), 2),
        v_alert_count,
        v_high_risk_count
    FROM fact_transactions
    WHERE transaction_day = p_report_day
    ON DUPLICATE KEY UPDATE
        total_txns = VALUES(total_txns),
        total_volume = VALUES(total_volume),
        fraud_count = VALUES(fraud_count),
        fraud_rate = VALUES(fraud_rate),
        avg_txn_amount = VALUES(avg_txn_amount),
        max_txn_amount = VALUES(max_txn_amount),
        alert_count = VALUES(alert_count),
        high_risk_customers = VALUES(high_risk_customers),
        report_generated_at = CURRENT_TIMESTAMP;
        
    SELECT CONCAT('Daily report generated for day: ', p_report_day) AS status;
END //
DELIMITER ;

-- Usage: CALL sp_generate_daily_report(1);

-- ============================================================
-- PROCEDURE 2: Batch Generate Reports for All Days
-- ============================================================
DROP PROCEDURE IF EXISTS sp_generate_all_daily_reports;

DELIMITER //
CREATE PROCEDURE sp_generate_all_daily_reports()
BEGIN
    DECLARE v_min_day INT;
    DECLARE v_max_day INT;
    DECLARE v_current_day INT;
    
    SELECT MIN(transaction_day), MAX(transaction_day)
    INTO v_min_day, v_max_day
    FROM fact_transactions;
    
    SET v_current_day = v_min_day;
    
    WHILE v_current_day <= v_max_day DO
        CALL sp_generate_daily_report(v_current_day);
        SET v_current_day = v_current_day + 1;
    END WHILE;
    
    SELECT CONCAT('Generated reports for days ', v_min_day, ' to ', v_max_day) AS status;
END //
DELIMITER ;

-- Usage: CALL sp_generate_all_daily_reports();

-- ============================================================
-- PROCEDURE 3: Update Customer Risk Scores
-- Recalculates risk tiers for all customers
-- ============================================================
DROP PROCEDURE IF EXISTS sp_update_customer_risk;

DELIMITER //
CREATE PROCEDURE sp_update_customer_risk()
BEGIN
    -- Refresh dim_customers aggregates
    INSERT INTO dim_customers (
        customer_id, total_transactions, total_amount, avg_transaction,
        max_transaction, fraud_count, first_seen_step, last_seen_step
    )
    SELECT 
        customer_id,
        COUNT(*),
        ROUND(SUM(amount), 2),
        ROUND(AVG(amount), 2),
        ROUND(MAX(amount), 2),
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END),
        MIN(step),
        MAX(step)
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
    
    -- Update risk scores
    UPDATE dim_customers
    SET 
        risk_score = LEAST(100, (
            (CASE WHEN fraud_count > 0 THEN 40 ELSE 0 END) +
            (CASE 
                WHEN max_transaction / NULLIF(avg_transaction, 0) > 50 THEN 25
                WHEN max_transaction / NULLIF(avg_transaction, 0) > 10 THEN 15
                WHEN max_transaction / NULLIF(avg_transaction, 0) > 5 THEN 10
                ELSE 3
            END) +
            (CASE 
                WHEN total_amount > 5000000 THEN 15
                WHEN total_amount > 1000000 THEN 12
                WHEN total_amount > 500000 THEN 8
                ELSE 3
            END) +
            (CASE WHEN total_transactions >= 20 THEN 15 ELSE 5 END)
        )),
        risk_tier = CASE
            WHEN fraud_count > 0 THEN 'CRITICAL'
            WHEN max_transaction / NULLIF(avg_transaction, 0) > 10 AND total_amount > 1000000 THEN 'HIGH'
            WHEN max_transaction / NULLIF(avg_transaction, 0) > 5 OR total_amount > 500000 THEN 'MEDIUM'
            ELSE 'LOW'
        END;
    
    SELECT 
        risk_tier,
        COUNT(*) AS customer_count,
        ROUND(AVG(risk_score), 1) AS avg_score
    FROM dim_customers
    GROUP BY risk_tier
    ORDER BY FIELD(risk_tier, 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW');
END //
DELIMITER ;

-- Usage: CALL sp_update_customer_risk();

-- ============================================================
-- PROCEDURE 4: Insert Alerts from SQL-based Detection
-- ============================================================
DROP PROCEDURE IF EXISTS sp_detect_high_amount_anomalies;

DELIMITER //
CREATE PROCEDURE sp_detect_high_amount_anomalies(IN p_z_threshold DECIMAL(5,2))
BEGIN
    -- Insert alerts for transactions with amount > customer avg + threshold * std
    INSERT INTO alerts (transaction_id, customer_id, alert_type, severity, alert_description, detection_method)
    SELECT 
        ft.transaction_id,
        ft.customer_id,
        'HIGH_AMOUNT',
        CASE 
            WHEN ft.amount > dc.avg_transaction + 5 * GREATEST(dc.max_transaction - dc.avg_transaction, 1) THEN 'CRITICAL'
            WHEN ft.amount > dc.avg_transaction + 3 * GREATEST(dc.max_transaction - dc.avg_transaction, 1) THEN 'HIGH'
            ELSE 'MEDIUM'
        END,
        CONCAT('Transaction amount ', ROUND(ft.amount, 2), 
               ' exceeds customer avg ', ROUND(dc.avg_transaction, 2),
               ' by ', ROUND((ft.amount - dc.avg_transaction) / NULLIF(dc.avg_transaction, 0) * 100, 1), '%'),
        'SQL_HIGH_AMOUNT'
    FROM fact_transactions ft
    JOIN dim_customers dc ON ft.customer_id = dc.customer_id
    WHERE ft.amount > dc.avg_transaction * p_z_threshold
      AND dc.total_transactions >= 3
      AND ft.transaction_id NOT IN (SELECT transaction_id FROM alerts WHERE alert_type = 'HIGH_AMOUNT' AND transaction_id IS NOT NULL);
    
    SELECT ROW_COUNT() AS new_alerts_inserted;
END //
DELIMITER ;

-- Usage: CALL sp_detect_high_amount_anomalies(5.0);

-- ============================================================
-- VIEW 1: Active Alert Queue (for dashboard)
-- ============================================================
CREATE OR REPLACE VIEW vw_active_alerts AS
SELECT 
    a.alert_id,
    a.transaction_id,
    a.customer_id,
    a.alert_type,
    a.severity,
    a.alert_description,
    a.detection_method,
    a.alert_timestamp,
    ft.amount,
    ft.transaction_type,
    ft.old_balance_orig,
    ft.new_balance_orig,
    dc.risk_tier AS customer_risk_tier,
    dc.risk_score AS customer_risk_score,
    dc.total_transactions AS customer_total_txns
FROM alerts a
LEFT JOIN fact_transactions ft ON a.transaction_id = ft.transaction_id
LEFT JOIN dim_customers dc ON a.customer_id = dc.customer_id
WHERE a.reviewed = 0
ORDER BY 
    FIELD(a.severity, 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW'),
    a.alert_timestamp DESC;

-- ============================================================
-- VIEW 2: Daily Dashboard Summary
-- ============================================================
CREATE OR REPLACE VIEW vw_daily_dashboard AS
SELECT 
    dmr.*,
    ROUND(
        (dmr.total_txns - LAG(dmr.total_txns) OVER (ORDER BY dmr.report_day)) * 100.0 /
        NULLIF(LAG(dmr.total_txns) OVER (ORDER BY dmr.report_day), 0), 2
    ) AS txn_change_pct,
    ROUND(
        (dmr.fraud_count - LAG(dmr.fraud_count) OVER (ORDER BY dmr.report_day)) * 100.0 /
        NULLIF(LAG(dmr.fraud_count) OVER (ORDER BY dmr.report_day), 0), 2
    ) AS fraud_change_pct
FROM daily_monitoring_report dmr
ORDER BY dmr.report_day;

-- ============================================================
-- VIEW 3: Customer 360 View (for drill-through)
-- ============================================================
CREATE OR REPLACE VIEW vw_customer_360 AS
SELECT 
    dc.customer_id,
    dc.risk_tier,
    dc.risk_score,
    dc.total_transactions,
    dc.total_amount,
    dc.avg_transaction,
    dc.max_transaction,
    dc.fraud_count,
    dc.first_seen_step,
    dc.last_seen_step,
    (dc.last_seen_step - dc.first_seen_step) AS activity_span_hours,
    -- Alert summary
    (SELECT COUNT(*) FROM alerts a WHERE a.customer_id = dc.customer_id) AS total_alerts,
    (SELECT COUNT(*) FROM alerts a WHERE a.customer_id = dc.customer_id AND a.severity = 'CRITICAL') AS critical_alerts,
    -- Transaction type distribution
    (SELECT COUNT(*) FROM fact_transactions ft WHERE ft.customer_id = dc.customer_id AND ft.transaction_type = 'TRANSFER') AS transfer_count,
    (SELECT COUNT(*) FROM fact_transactions ft WHERE ft.customer_id = dc.customer_id AND ft.transaction_type = 'CASH_OUT') AS cashout_count,
    (SELECT COUNT(DISTINCT ft.recipient_id) FROM fact_transactions ft WHERE ft.customer_id = dc.customer_id) AS unique_recipients
FROM dim_customers dc;
