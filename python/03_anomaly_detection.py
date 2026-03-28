"""
03_anomaly_detection.py
=======================
Statistical Anomaly Detection for Financial Transactions
Methods: Z-Score, IQR, Rolling Window

Usage:
    python python/03_anomaly_detection.py

Output:
    - Anomaly detection results printed to console
    - Alerts inserted into database
    - Performance metrics (precision, recall) for each method
"""

import pandas as pd
import numpy as np
from scipy import stats
from sqlalchemy import create_engine, text
from sklearn.metrics import classification_report, confusion_matrix
import os
import warnings
warnings.filterwarnings('ignore')

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


def get_engine():
    return create_engine(
        f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )


def load_data(engine, sample_size=None):
    """Load transaction data from MySQL."""
    query = """
        SELECT transaction_id, customer_id, transaction_type, amount,
               old_balance_orig, new_balance_orig, balance_change,
               amount_to_balance_ratio, is_fraud, step,
               transaction_hour, transaction_day
        FROM fact_transactions
    """
    if sample_size:
        query += f" ORDER BY RAND() LIMIT {sample_size}"
    
    print(f" Loading data from MySQL...")
    df = pd.read_sql(query, engine)
    print(f"   Loaded {len(df):,} transactions ({df['is_fraud'].sum():,} fraud)")
    return df


# ============================================================
# METHOD 1: Z-Score Anomaly Detection
# ============================================================
def zscore_detection(df, threshold=3.0):
    """
    Flag transactions where the amount Z-score exceeds threshold
    within each customer's transaction history.
    """
    print(f"\n{'='*60}")
    print(f"METHOD 1: Z-Score Detection (threshold = {threshold})")
    print(f"{'='*60}")
    
    # Calculate per-customer mean and std
    customer_stats = df.groupby('customer_id')['amount'].agg(['mean', 'std']).reset_index()
    customer_stats.columns = ['customer_id', 'cust_mean', 'cust_std']
    
    # Merge back
    df_z = df.merge(customer_stats, on='customer_id', how='left')
    
    # Calculate Z-score (handle zero std)
    df_z['z_score'] = np.where(
        df_z['cust_std'] > 0,
        (df_z['amount'] - df_z['cust_mean']) / df_z['cust_std'],
        0
    )
    
    # Flag anomalies
    df_z['is_anomaly_zscore'] = (df_z['z_score'].abs() > threshold).astype(int)
    
    # Results
    anomalies = df_z[df_z['is_anomaly_zscore'] == 1]
    print(f"   Total anomalies flagged: {len(anomalies):,}")
    print(f"   True fraud in anomalies: {anomalies['is_fraud'].sum():,}")
    print(f"   Anomaly rate: {len(anomalies)/len(df)*100:.3f}%")
    
    # Precision/Recall vs actual fraud
    if df_z['is_fraud'].sum() > 0:
        print(f"\n   Classification Report (Z-Score vs Actual Fraud):")
        print(classification_report(
            df_z['is_fraud'], df_z['is_anomaly_zscore'],
            target_names=['Legitimate', 'Fraud'],
            zero_division=0
        ))
    
    return df_z


# ============================================================
# METHOD 2: IQR (Interquartile Range) Detection
# ============================================================
def iqr_detection(df, multiplier=1.5):
    """
    Flag transactions outside the IQR bounds per transaction type.
    """
    print(f"\n{'='*60}")
    print(f"METHOD 2: IQR Detection (multiplier = {multiplier})")
    print(f"{'='*60}")
    
    df_iqr = df.copy()
    df_iqr['is_anomaly_iqr'] = 0
    
    type_stats = []
    
    for txn_type in df_iqr['transaction_type'].unique():
        mask = df_iqr['transaction_type'] == txn_type
        amounts = df_iqr.loc[mask, 'amount']
        
        Q1 = amounts.quantile(0.25)
        Q3 = amounts.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - multiplier * IQR
        upper_bound = Q3 + multiplier * IQR
        
        # Flag outliers
        outlier_mask = mask & ((df_iqr['amount'] < lower_bound) | (df_iqr['amount'] > upper_bound))
        df_iqr.loc[outlier_mask, 'is_anomaly_iqr'] = 1
        
        outlier_count = len(df_iqr[outlier_mask])
        fraud_in_outliers = df_iqr.loc[outlier_mask, 'is_fraud'].sum()
        
        type_stats.append({
            'type': txn_type,
            'Q1': Q1, 'Q3': Q3, 'IQR': IQR,
            'lower': max(lower_bound, 0), 'upper': upper_bound,
            'outliers': outlier_count,
            'fraud_in_outliers': fraud_in_outliers
        })
    
    # Print per-type results
    stats_df = pd.DataFrame(type_stats)
    print(f"\n   Per-Type IQR Bounds:")
    for _, row in stats_df.iterrows():
        print(f"   {row['type']:12s} | Q1: {row['Q1']:>12,.2f} | Q3: {row['Q3']:>12,.2f} | "
              f"Outliers: {row['outliers']:>8,} | Fraud: {row['fraud_in_outliers']:>6,}")
    
    anomalies = df_iqr[df_iqr['is_anomaly_iqr'] == 1]
    print(f"\n   Total anomalies flagged: {len(anomalies):,}")
    print(f"   True fraud in anomalies: {anomalies['is_fraud'].sum():,}")
    
    if df_iqr['is_fraud'].sum() > 0:
        print(f"\n   Classification Report (IQR vs Actual Fraud):")
        print(classification_report(
            df_iqr['is_fraud'], df_iqr['is_anomaly_iqr'],
            target_names=['Legitimate', 'Fraud'],
            zero_division=0
        ))
    
    return df_iqr


# ============================================================
# METHOD 3: Rolling Window Anomaly Detection
# ============================================================
def rolling_window_detection(df, window=10, threshold=2.0):
    """
    Flag transactions where amount exceeds rolling_mean + threshold * rolling_std.
    Detects behavioral changes (sudden large transactions).
    """
    print(f"\n{'='*60}")
    print(f"METHOD 3: Rolling Window Detection (window={window}, threshold={threshold})")
    print(f"{'='*60}")
    
    # Sort by customer and time
    df_roll = df.sort_values(['customer_id', 'step']).copy()
    
    # Calculate rolling stats per customer
    grouped = df_roll.groupby('customer_id')['amount']
    df_roll['rolling_mean'] = grouped.transform(
        lambda x: x.rolling(window=window, min_periods=3).mean().shift(1)
    )
    df_roll['rolling_std'] = grouped.transform(
        lambda x: x.rolling(window=window, min_periods=3).std().shift(1)
    )
    
    # Flag anomalies (where amount > rolling_mean + threshold * rolling_std)
    df_roll['is_anomaly_rolling'] = 0
    valid_mask = df_roll['rolling_mean'].notna() & (df_roll['rolling_std'] > 0)
    anomaly_mask = valid_mask & (
        df_roll['amount'] > df_roll['rolling_mean'] + threshold * df_roll['rolling_std']
    )
    df_roll.loc[anomaly_mask, 'is_anomaly_rolling'] = 1
    
    # Results
    valid_df = df_roll[valid_mask]
    anomalies = valid_df[valid_df['is_anomaly_rolling'] == 1]
    print(f"   Transactions with enough history: {len(valid_df):,}")
    print(f"   Anomalies flagged: {len(anomalies):,}")
    print(f"   True fraud in anomalies: {anomalies['is_fraud'].sum():,}")
    
    if valid_df['is_fraud'].sum() > 0:
        print(f"\n   Classification Report (Rolling Window vs Actual Fraud):")
        print(classification_report(
            valid_df['is_fraud'], valid_df['is_anomaly_rolling'],
            target_names=['Legitimate', 'Fraud'],
            zero_division=0
        ))
    
    return df_roll


# ============================================================
# ENSEMBLE: Combine All Methods
# ============================================================
def ensemble_detection(df_z, df_iqr, df_roll):
    """
    Combine results from all 3 methods.
    A transaction flagged by 2+ methods is considered high confidence.
    """
    print(f"\n{'='*60}")
    print(f"ENSEMBLE: Combined Multi-Method Detection")
    print(f"{'='*60}")
    
    # Merge on transaction_id
    combined = df_z[['transaction_id', 'is_fraud', 'is_anomaly_zscore']].copy()
    combined = combined.merge(
        df_iqr[['transaction_id', 'is_anomaly_iqr']],
        on='transaction_id', how='left'
    )
    combined = combined.merge(
        df_roll[['transaction_id', 'is_anomaly_rolling']],
        on='transaction_id', how='left'
    )
    
    # Fill NaN with 0
    combined = combined.fillna(0)
    
    # Ensemble score (how many methods flagged it)
    combined['methods_flagged'] = (
        combined['is_anomaly_zscore'] + 
        combined['is_anomaly_iqr'] + 
        combined['is_anomaly_rolling']
    ).astype(int)
    
    # High-confidence anomaly (flagged by 2+ methods)
    combined['is_anomaly_ensemble'] = (combined['methods_flagged'] >= 2).astype(int)
    
    print(f"\n   Methods-Flagged Distribution:")
    for n in range(4):
        count = len(combined[combined['methods_flagged'] == n])
        fraud = combined[combined['methods_flagged'] == n]['is_fraud'].sum()
        print(f"   {n} methods: {count:>10,} transactions ({fraud:,} fraud)")
    
    ensemble_anomalies = combined[combined['is_anomaly_ensemble'] == 1]
    print(f"\n   Ensemble anomalies (2+ methods): {len(ensemble_anomalies):,}")
    print(f"   True fraud in ensemble: {ensemble_anomalies['is_fraud'].sum():,}")
    
    if combined['is_fraud'].sum() > 0:
        print(f"\n   Classification Report (Ensemble vs Actual Fraud):")
        print(classification_report(
            combined['is_fraud'], combined['is_anomaly_ensemble'],
            target_names=['Legitimate', 'Fraud'],
            zero_division=0
        ))
    
    return combined


# ============================================================
# INSERT ALERTS INTO DATABASE
# ============================================================
def insert_alerts(engine, df_z, df_iqr, df_roll):
    """Insert detected anomalies as alerts into the alerts table."""
    print(f"\n{'='*60}")
    print(f"Inserting Alerts into Database...")
    print(f"{'='*60}")
    
    alerts = []
    
    # Z-Score alerts
    z_anomalies = df_z[df_z['is_anomaly_zscore'] == 1]
    for _, row in z_anomalies.head(500).iterrows():
        alerts.append({
            'transaction_id': int(row['transaction_id']),
            'customer_id': row['customer_id'],
            'alert_type': 'Z_SCORE',
            'severity': 'CRITICAL' if abs(row['z_score']) > 5 else 'HIGH' if abs(row['z_score']) > 3 else 'MEDIUM',
            'alert_description': f"Z-score={row['z_score']:.2f}, Amount={row['amount']:.2f}",
            'detection_method': 'PYTHON_ZSCORE'
        })
    
    # IQR alerts
    iqr_anomalies = df_iqr[df_iqr['is_anomaly_iqr'] == 1]
    for _, row in iqr_anomalies.head(500).iterrows():
        alerts.append({
            'transaction_id': int(row['transaction_id']),
            'customer_id': row['customer_id'],
            'alert_type': 'IQR_OUTLIER',
            'severity': 'HIGH',
            'alert_description': f"IQR outlier: Amount={row['amount']:.2f}, Type={row['transaction_type']}",
            'detection_method': 'PYTHON_IQR'
        })
    
    # Rolling window alerts
    roll_anomalies = df_roll[df_roll.get('is_anomaly_rolling', 0) == 1]
    for _, row in roll_anomalies.head(500).iterrows():
        alerts.append({
            'transaction_id': int(row['transaction_id']),
            'customer_id': row['customer_id'],
            'alert_type': 'ROLLING_DEVIATION',
            'severity': 'MEDIUM',
            'alert_description': f"Amount={row['amount']:.2f} exceeded rolling avg",
            'detection_method': 'PYTHON_ROLLING'
        })
    
    if alerts:
        alerts_df = pd.DataFrame(alerts)
        alerts_df.to_sql('alerts', engine, if_exists='append', index=False, method='multi', chunksize=1000)
        print(f"    Inserted {len(alerts_df):,} alerts")
    else:
        print("    No alerts to insert")


# ============================================================
# MAIN
# ============================================================
def main():
    """Run all anomaly detection methods."""
    print(" Anomaly Detection Pipeline")
    print("=" * 60)
    
    engine = get_engine()
    
    # Load data (sample for faster processing; remove limit for full run)
    df = load_data(engine, sample_size=500000)
    
    # Run all 3 methods
    df_z = zscore_detection(df, threshold=3.0)
    df_iqr = iqr_detection(df, multiplier=1.5)
    df_roll = rolling_window_detection(df, window=10, threshold=2.0)
    
    # Ensemble
    combined = ensemble_detection(df_z, df_iqr, df_roll)
    
    # Insert alerts
    insert_alerts(engine, df_z, df_iqr, df_roll)
    
    print(f"\n{'='*60}")
    print(" Anomaly Detection Complete!")
    print("   Next steps: Run hypothesis tests (python/04_statistical_tests.py)")


if __name__ == '__main__':
    main()

