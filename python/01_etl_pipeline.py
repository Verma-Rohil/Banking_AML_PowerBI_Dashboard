"""
01_etl_pipeline.py
==================
ETL Pipeline: CSV  MySQL Database
Loads PaySim financial transaction data into MySQL with feature engineering.

Usage:
    python python/01_etl_pipeline.py

Prerequisites:
    1. MySQL database 'aml_monitoring' created
    2. Schema tables created (run sql/01_schema_design.sql first)
    3. PaySim CSV file in data/raw/ directory
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import mysql.connector
import os
import time
import logging

# ============================================================
# CONFIGURATION
# ============================================================
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',               # Update with your MySQL username
    'password': 'password123',
    'database': 'aml_monitoring',
    'port': 3306
}

# File paths
RAW_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'raw')
PROCESSED_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'processed')
CSV_FILENAME = 'PS_20174392719_1491204439457_log.csv'  # PaySim dataset

# ETL settings
CHUNK_SIZE = 50000  # Number of rows per batch insert
LOG_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'etl_log.txt')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================================
# DATABASE CONNECTION
# ============================================================
def get_engine():
    """Create SQLAlchemy engine for MySQL connection."""
    connection_string = (
        f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    engine = create_engine(connection_string, echo=False)
    return engine


def test_connection():
    """Test database connection."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            logger.info(" Database connection successful!")
            return True
    except Exception as e:
        logger.error(f" Database connection failed: {e}")
        logger.error("Please check your DB_CONFIG settings at the top of this file.")
        return False


# ============================================================
# DATA CLEANING & FEATURE ENGINEERING
# ============================================================
def clean_and_engineer(df):
    """
    Clean raw data and create derived features.
    
    Input columns from PaySim:
        step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig,
        nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud
    
    Output: DataFrame with renamed columns + derived features
    """
    # Rename columns to match our schema
    df = df.rename(columns={
        'step': 'step',
        'type': 'transaction_type',
        'amount': 'amount',
        'nameOrig': 'customer_id',
        'oldbalanceOrg': 'old_balance_orig',
        'newbalanceOrig': 'new_balance_orig',
        'nameDest': 'recipient_id',
        'oldbalanceDest': 'old_balance_dest',
        'newbalanceDest': 'new_balance_dest',
        'isFraud': 'is_fraud',
        'isFlaggedFraud': 'is_flagged_fraud'
    })
    
    # === Data Cleaning ===
    # Remove rows with negative amounts (if any)
    df = df[df['amount'] > 0]
    
    # Handle NaN values
    df['old_balance_orig'] = df['old_balance_orig'].fillna(0)
    df['new_balance_orig'] = df['new_balance_orig'].fillna(0)
    df['old_balance_dest'] = df['old_balance_dest'].fillna(0)
    df['new_balance_dest'] = df['new_balance_dest'].fillna(0)
    
    # === Feature Engineering ===
    # Transaction hour (step % 24  simulates hour of day)
    df['transaction_hour'] = df['step'] % 24
    
    # Transaction day (step // 24  simulates day number)
    df['transaction_day'] = df['step'] // 24
    
    # Balance change (how much the originator's balance changed)
    df['balance_change'] = df['new_balance_orig'] - df['old_balance_orig']
    
    # Amount to balance ratio (how large is the txn relative to balance?)
    df['amount_to_balance_ratio'] = np.where(
        df['old_balance_orig'] > 0,
        np.round(df['amount'] / df['old_balance_orig'], 4),
        None
    )
    
    # Convert fraud columns to int
    df['is_fraud'] = df['is_fraud'].astype(int)
    df['is_flagged_fraud'] = df['is_flagged_fraud'].astype(int)
    
    return df


# ============================================================
# LOAD DATA INTO MYSQL
# ============================================================
def load_to_mysql(csv_path, engine):
    """
    Load CSV into MySQL in chunks.
    Uses pandas to_sql with 'append' mode for batch inserts.
    """
    logger.info(f" Reading CSV: {csv_path}")
    
    # Get total rows for progress tracking
    total_rows = sum(1 for _ in open(csv_path, encoding='utf-8')) - 1  # minus header
    logger.info(f" Total rows to process: {total_rows:,}")
    
    rows_loaded = 0
    chunk_num = 0
    start_time = time.time()
    
    # Read and process in chunks
    for chunk in pd.read_csv(csv_path, chunksize=CHUNK_SIZE):
        chunk_num += 1
        
        # Clean and engineer features
        processed_chunk = clean_and_engineer(chunk)
        
        # Drop the auto-increment column (MySQL handles it)
        columns_to_insert = [
            'step', 'transaction_type', 'amount', 'customer_id',
            'old_balance_orig', 'new_balance_orig', 'recipient_id',
            'old_balance_dest', 'new_balance_dest', 'is_fraud',
            'is_flagged_fraud', 'transaction_hour', 'transaction_day',
            'balance_change', 'amount_to_balance_ratio'
        ]
        
        # Insert into MySQL
        processed_chunk[columns_to_insert].to_sql(
            'fact_transactions',
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=5000  # Sub-chunk for INSERT statements
        )
        
        rows_loaded += len(processed_chunk)
        elapsed = time.time() - start_time
        rate = rows_loaded / elapsed if elapsed > 0 else 0
        progress = (rows_loaded / total_rows) * 100
        
        logger.info(
            f"  Chunk {chunk_num:3d} | "
            f"Loaded: {rows_loaded:>10,} / {total_rows:,} ({progress:.1f}%) | "
            f"Rate: {rate:,.0f} rows/sec"
        )
    
    elapsed = time.time() - start_time
    logger.info(f" Data loading complete! {rows_loaded:,} rows in {elapsed:.1f} seconds")
    return rows_loaded


# ============================================================
# POST-LOAD: Populate Dimension Tables
# ============================================================
def populate_dimensions(engine):
    """Run SQL to populate dim_customers from fact_transactions."""
    logger.info(" Populating dim_customers...")
    
    sql_file = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), 
        'sql', '02_data_loading.sql'
    )
    
    with engine.connect() as conn:
        # Populate dim_customers
        conn.execute(text("""
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
                last_seen_step = VALUES(last_seen_step)
        """))
        conn.commit()
        
        # Update risk tiers
        conn.execute(text("""
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
                END
        """))
        conn.commit()
        
        # Verify
        result = conn.execute(text("""
            SELECT risk_tier, COUNT(*) AS count 
            FROM dim_customers 
            GROUP BY risk_tier 
            ORDER BY FIELD(risk_tier, 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW')
        """))
        logger.info(" Customer Risk Tier Distribution:")
        for row in result:
            logger.info(f"   {row[0]}: {row[1]:,} customers")
    
    logger.info(" Dimension tables populated!")


# ============================================================
# DATA QUALITY REPORT
# ============================================================
def generate_quality_report(engine):
    """Generate a data quality summary after loading."""
    logger.info("\n" + "=" * 60)
    logger.info(" DATA QUALITY REPORT")
    logger.info("=" * 60)
    
    with engine.connect() as conn:
        # Total rows
        result = conn.execute(text("SELECT COUNT(*) FROM fact_transactions"))
        total = result.scalar()
        logger.info(f"Total transactions loaded: {total:,}")
        
        # Transaction type distribution
        result = conn.execute(text("""
            SELECT transaction_type, COUNT(*) as cnt, 
                   ROUND(AVG(amount), 2) as avg_amt
            FROM fact_transactions 
            GROUP BY transaction_type 
            ORDER BY cnt DESC
        """))
        logger.info("\nTransaction Type Distribution:")
        for row in result:
            logger.info(f"   {row[0]:12s}: {row[1]:>10,} txns (avg: ${row[2]:>12,.2f})")
        
        # Fraud summary
        result = conn.execute(text("""
            SELECT is_fraud, COUNT(*) as cnt 
            FROM fact_transactions 
            GROUP BY is_fraud
        """))
        logger.info("\nFraud Distribution:")
        for row in result:
            label = "Fraud" if row[0] == 1 else "Legitimate"
            logger.info(f"   {label}: {row[1]:,}")
        
        # Null check
        result = conn.execute(text("""
            SELECT 
                SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_cust,
                SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) as null_amt,
                SUM(CASE WHEN transaction_hour IS NULL THEN 1 ELSE 0 END) as null_hour
            FROM fact_transactions
        """))
        row = result.fetchone()
        logger.info(f"\nNull Values: customer_id={row[0]}, amount={row[1]}, transaction_hour={row[2]}")
    
    logger.info("=" * 60)


# ============================================================
# MAIN ETL PIPELINE
# ============================================================
def main():
    """Main ETL pipeline execution."""
    logger.info(" Starting AML Transaction Monitoring ETL Pipeline")
    logger.info("=" * 60)
    
    # Step 1: Test connection
    if not test_connection():
        return
    
    engine = get_engine()
    
    # Step 2: Check for CSV file
    csv_path = os.path.join(RAW_DATA_DIR, CSV_FILENAME)
    if not os.path.exists(csv_path):
        logger.error(f" CSV file not found: {csv_path}")
        logger.error(f"Please download the PaySim dataset from Kaggle and place it in: {RAW_DATA_DIR}")
        logger.error("URL: https://www.kaggle.com/datasets/ealaxi/paysim1")
        
        # Create the data directories if they don't exist
        os.makedirs(RAW_DATA_DIR, exist_ok=True)
        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
        logger.info(f" Created data directories: {RAW_DATA_DIR}, {PROCESSED_DATA_DIR}")
        return
    
    # Step 3: Load data into MySQL
    rows_loaded = load_to_mysql(csv_path, engine)
    
    # Step 4: Populate dimension tables
    populate_dimensions(engine)
    
    # Step 5: Generate quality report
    generate_quality_report(engine)
    
    logger.info("\n ETL Pipeline completed successfully!")
    logger.info(f"   Total rows loaded: {rows_loaded:,}")
    logger.info("   Next steps: Run the EDA analysis (python/02_eda_analysis.py)")


if __name__ == '__main__':
    main()

