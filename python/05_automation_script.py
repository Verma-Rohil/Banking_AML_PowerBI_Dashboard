"""
05_automation_script.py
=======================
Automated Daily ETL & Alert Generation Pipeline
Can be scheduled via Windows Task Scheduler or cron.

Usage:
    python python/05_automation_script.py

What it does:
    1. Checks for new data files in data/raw/
    2. Cleans & validates new data
    3. Loads into MySQL
    4. Runs anomaly detection
    5. Inserts alerts into alerts table
    6. Generates daily summary report
    7. Exports report to Excel
    8. Logs execution to monitoring_log.txt
"""

import pandas as pd
import numpy as np
from scipy import stats
from sqlalchemy import create_engine, text
from datetime import datetime
import os
import glob
import logging
import traceback

# ============================================================
# CONFIGURATION
# ============================================================
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'password123',
    'database': 'aml_monitoring',
    'port': 3306
}

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
RAW_DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, 'data', 'processed')
REPORTS_DIR = os.path.join(BASE_DIR, 'reports')
LOG_FILE = os.path.join(BASE_DIR, 'monitoring_log.txt')

# Create directories
for d in [RAW_DATA_DIR, PROCESSED_DATA_DIR, REPORTS_DIR]:
    os.makedirs(d, exist_ok=True)

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_engine():
    return create_engine(
        f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )


# ============================================================
# STEP 1: Check for new data files
# ============================================================
def check_new_files():
    """Check for new CSV files in data/raw/ directory."""
    logger.info("STEP 1: Checking for new data files...")
    
    new_files = glob.glob(os.path.join(RAW_DATA_DIR, '*.csv'))
    processed_log = os.path.join(PROCESSED_DATA_DIR, 'processed_files.txt')
    
    # Read already processed files
    processed_files = set()
    if os.path.exists(processed_log):
        with open(processed_log, 'r') as f:
            processed_files = set(f.read().strip().split('\n'))
    
    # Filter to only new files
    unprocessed = [f for f in new_files if os.path.basename(f) not in processed_files]
    
    logger.info(f"   Found {len(new_files)} total CSV files, {len(unprocessed)} new/unprocessed")
    return unprocessed


# ============================================================
# STEP 2: Clean & Validate Data
# ============================================================
def clean_data(df):
    """Clean and validate incoming transaction data."""
    logger.info("STEP 2: Cleaning & validating data...")
    
    initial_rows = len(df)
    
    # Rename columns
    column_mapping = {
        'step': 'step', 'type': 'transaction_type', 'amount': 'amount',
        'nameOrig': 'customer_id', 'oldbalanceOrg': 'old_balance_orig',
        'newbalanceOrig': 'new_balance_orig', 'nameDest': 'recipient_id',
        'oldbalanceDest': 'old_balance_dest', 'newbalanceDest': 'new_balance_dest',
        'isFraud': 'is_fraud', 'isFlaggedFraud': 'is_flagged_fraud'
    }
    df = df.rename(columns=column_mapping)
    
    # Validation: remove invalid rows
    df = df[df['amount'] > 0]
    df = df.dropna(subset=['customer_id', 'recipient_id', 'transaction_type'])
    
    # Fill NaN balances with 0
    balance_cols = ['old_balance_orig', 'new_balance_orig', 'old_balance_dest', 'new_balance_dest']
    df[balance_cols] = df[balance_cols].fillna(0)
    
    # Feature engineering
    df['transaction_hour'] = df['step'] % 24
    df['transaction_day'] = df['step'] // 24
    df['balance_change'] = df['new_balance_orig'] - df['old_balance_orig']
    df['amount_to_balance_ratio'] = np.where(
        df['old_balance_orig'] > 0,
        np.round(df['amount'] / df['old_balance_orig'], 4),
        None
    )
    
    cleaned_rows = len(df)
    removed = initial_rows - cleaned_rows
    logger.info(f"   Initial: {initial_rows:,}  Cleaned: {cleaned_rows:,} (removed {removed:,})")
    
    return df


# ============================================================
# STEP 3: Load into MySQL
# ============================================================
def load_to_database(df, engine):
    """Load cleaned data into MySQL fact_transactions table."""
    logger.info("STEP 3: Loading data into MySQL...")
    
    columns = [
        'step', 'transaction_type', 'amount', 'customer_id',
        'old_balance_orig', 'new_balance_orig', 'recipient_id',
        'old_balance_dest', 'new_balance_dest', 'is_fraud',
        'is_flagged_fraud', 'transaction_hour', 'transaction_day',
        'balance_change', 'amount_to_balance_ratio'
    ]
    
    df[columns].to_sql(
        'fact_transactions', engine,
        if_exists='append', index=False,
        method='multi', chunksize=5000
    )
    
    logger.info(f"    Loaded {len(df):,} rows into fact_transactions")


# ============================================================
# STEP 4: Run Anomaly Detection
# ============================================================
def run_anomaly_detection(df, engine):
    """Run Z-Score anomaly detection on new data."""
    logger.info("STEP 4: Running anomaly detection...")
    
    alerts = []
    
    # Z-Score detection per customer
    customer_stats = df.groupby('customer_id')['amount'].agg(['mean', 'std']).reset_index()
    customer_stats.columns = ['customer_id', 'cust_mean', 'cust_std']
    
    df_merged = df.merge(customer_stats, on='customer_id', how='left')
    df_merged['z_score'] = np.where(
        df_merged['cust_std'] > 0,
        (df_merged['amount'] - df_merged['cust_mean']) / df_merged['cust_std'],
        0
    )
    
    anomalies = df_merged[df_merged['z_score'].abs() > 3]
    
    for _, row in anomalies.iterrows():
        severity = 'CRITICAL' if abs(row['z_score']) > 5 else 'HIGH' if abs(row['z_score']) > 3 else 'MEDIUM'
        alerts.append({
            'transaction_id': int(row.get('transaction_id', 0)),
            'customer_id': row['customer_id'],
            'alert_type': 'Z_SCORE_AUTO',
            'severity': severity,
            'alert_description': f"Auto-detected: Z-score={row['z_score']:.2f}, Amount={row['amount']:.2f}",
            'detection_method': 'PYTHON_AUTO_ZSCORE'
        })
    
    logger.info(f"   Detected {len(alerts):,} anomalies")
    return alerts


# ============================================================
# STEP 5: Insert Alerts
# ============================================================
def insert_alerts(alerts, engine):
    """Insert detected alerts into the database."""
    logger.info("STEP 5: Inserting alerts...")
    
    if alerts:
        alerts_df = pd.DataFrame(alerts)
        alerts_df.to_sql('alerts', engine, if_exists='append', index=False, 
                         method='multi', chunksize=1000)
        logger.info(f"    Inserted {len(alerts):,} alerts")
    else:
        logger.info("   No new alerts to insert")


# ============================================================
# STEP 6: Generate Daily Summary Report
# ============================================================
def generate_daily_report(engine):
    """Generate daily summary and call stored procedure."""
    logger.info("STEP 6: Generating daily summary report...")
    
    with engine.connect() as conn:
        # Get the latest day in the data
        result = conn.execute(text("SELECT MAX(transaction_day) FROM fact_transactions"))
        max_day = result.scalar()
        
        if max_day is not None:
            # Call stored procedure
            raw_conn = engine.raw_connection()
            try:
                cursor = raw_conn.cursor()
                cursor.callproc('sp_generate_daily_report', [max_day])
                raw_conn.commit()
                cursor.close()
            finally:
                raw_conn.close()
            logger.info(f"    Generated report for day {max_day}")
        
        # Get the report
        report = pd.read_sql("""
            SELECT * FROM daily_monitoring_report 
            ORDER BY report_day DESC LIMIT 5
        """, engine)
        
    return report


# ============================================================
# STEP 7: Export Report to Excel
# ============================================================
def export_to_excel(report, engine):
    """Export daily report and key metrics to Excel."""
    logger.info("STEP 7: Exporting report to Excel...")
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    excel_path = os.path.join(REPORTS_DIR, f'daily_report_{timestamp}.xlsx')
    
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        # Daily monitoring report
        report.to_excel(writer, sheet_name='Daily Summary', index=False)
        
        # Risk tier distribution
        risk_dist = pd.read_sql("""
            SELECT risk_tier, COUNT(*) AS customer_count,
                   ROUND(AVG(risk_score), 1) AS avg_score,
                   ROUND(SUM(total_amount), 2) AS total_volume
            FROM dim_customers
            GROUP BY risk_tier
        """, engine)
        risk_dist.to_excel(writer, sheet_name='Risk Distribution', index=False)
        
        # Recent alerts
        alerts = pd.read_sql("""
            SELECT alert_type, severity, alert_description, 
                   detection_method, alert_timestamp
            FROM alerts
            ORDER BY alert_timestamp DESC
            LIMIT 100
        """, engine)
        alerts.to_excel(writer, sheet_name='Recent Alerts', index=False)
    
    logger.info(f"    Report saved: {excel_path}")
    return excel_path


# ============================================================
# STEP 8: Log Execution
# ============================================================
def log_execution_summary(files_processed, rows_loaded, alerts_count, report_path):
    """Log final execution summary."""
    logger.info("\n" + "=" * 60)
    logger.info(" EXECUTION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"   Timestamp:        {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"   Files Processed:  {files_processed}")
    logger.info(f"   Rows Loaded:      {rows_loaded:,}")
    logger.info(f"   Alerts Generated: {alerts_count:,}")
    logger.info(f"   Report Exported:  {report_path}")
    logger.info("=" * 60)


# ============================================================
# MAIN PIPELINE
# ============================================================
def main():
    """Execute the complete automated daily pipeline."""
    logger.info(" Starting Automated Daily AML Monitoring Pipeline")
    logger.info("=" * 60)
    
    engine = get_engine()
    total_rows = int(0)
    total_alerts = int(0)
    report_path = "N/A"
    files_count = int(0)
    
    try:
        # Step 1: Check for new files
        new_files = check_new_files()
        
        if not new_files:
            logger.info("  No new files to process. Running report generation only...")
        else:
            for filepath in new_files:
                files_count += 1
                filename = os.path.basename(filepath)
                logger.info(f"\n Processing: {filename}")
                
                # Step 2: Clean
                raw_df = pd.read_csv(filepath, chunksize=100000)
                for chunk in raw_df:
                    cleaned = clean_data(chunk)
                    
                    # Step 3: Load
                    load_to_database(cleaned, engine)
                    total_rows += len(cleaned)
                    
                    # Step 4: Detect anomalies
                    alerts = run_anomaly_detection(cleaned, engine)
                    total_alerts += len(alerts)
                    
                    # Step 5: Insert alerts
                    insert_alerts(alerts, engine)
                
                # Mark file as processed
                processed_log = os.path.join(PROCESSED_DATA_DIR, 'processed_files.txt')
                with open(processed_log, 'a') as f:
                    f.write(filename + '\n')
        
        # Step 6: Generate report (always runs)
        report = generate_daily_report(engine)
        
        # Step 7: Export to Excel
        report_path = export_to_excel(report, engine)
        
        # Step 8: Summary
        log_execution_summary(files_count, total_rows, total_alerts, report_path)
        
        logger.info("\n Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"\n Pipeline failed: {str(e)}")
        logger.error(traceback.format_exc())
        raise


if __name__ == '__main__':
    main()

