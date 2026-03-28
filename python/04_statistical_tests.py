"""
04_statistical_tests.py
=======================
Hypothesis Testing for AML Transaction Analysis
Tests: Chi-Square, Welch's t-test, Point-Biserial Correlation, Goodness of Fit

Usage:
    python python/04_statistical_tests.py
"""

import pandas as pd
import numpy as np
from scipy import stats
from sqlalchemy import create_engine, text
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

SIGNIFICANCE_LEVEL = 0.05


def get_engine():
    return create_engine(
        f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )


def print_test_result(test_name, statistic, p_value, result_text, alpha=SIGNIFICANCE_LEVEL):
    """Pretty-print hypothesis test results."""
    print(f"\n{''*60}")
    print(f" {test_name}")
    print(f"{''*60}")
    print(f"   Test Statistic: {statistic:.4f}")
    print(f"   P-value:        {p_value:.2e}")
    print(f"   Significance:    = {alpha}")
    verdict = " REJECT H" if p_value < alpha else " FAIL TO REJECT H"
    print(f"   Result:         {verdict}")
    print(f"   Interpretation: {result_text}")


# ============================================================
# TEST 1: Chi-Square Test  Fraud Rate by Transaction Type
# H: Fraud rate is equal across all transaction types
# H: Fraud rate differs across transaction types
# ============================================================
def test_fraud_rate_by_type(engine):
    """Chi-Square Test of Independence: Fraud  Transaction Type."""
    print(f"\n{'='*60}")
    print("TEST 1: Chi-Square  Fraud Rate by Transaction Type")
    print(f"{'='*60}")
    print("H: Fraud rate is independent of transaction type")
    print("H: Fraud rate depends on transaction type")
    
    df = pd.read_sql("""
        SELECT transaction_type, is_fraud, COUNT(*) AS cnt
        FROM fact_transactions
        GROUP BY transaction_type, is_fraud
    """, engine)
    
    # Create contingency table
    contingency = df.pivot_table(values='cnt', index='transaction_type', 
                                  columns='is_fraud', fill_value=0)
    
    print(f"\n   Contingency Table:")
    print(f"   {'Type':<12} {'Legitimate':>12} {'Fraud':>10} {'Fraud Rate':>12}")
    for idx, row in contingency.iterrows():
        total = row.sum()
        fraud_rate = row.get(1, 0) / total * 100 if total > 0 else 0
        print(f"   {idx:<12} {row.get(0, 0):>12,.0f} {row.get(1, 0):>10,.0f} {fraud_rate:>11.4f}%")
    
    # Chi-square test
    chi2, p_value, dof, expected = stats.chi2_contingency(contingency)
    
    result = ("Fraud rate is SIGNIFICANTLY DIFFERENT across transaction types. "
              "TRANSFER and CASH_OUT have substantially higher fraud rates." 
              if p_value < SIGNIFICANCE_LEVEL else
              "No significant difference in fraud rates across transaction types.")
    
    print_test_result("Chi-Square Test of Independence", chi2, p_value, result)
    print(f"   Degrees of Freedom: {dof}")
    
    return chi2, p_value


# ============================================================
# TEST 2: Welch's t-test  Amount Difference: Fraud vs Legitimate
# H: Mean transaction amount is the same for fraud and legitimate
# H: Mean transaction amount differs
# ============================================================
def test_amount_difference(engine):
    """Welch's t-test: Fraud vs Legitimate amounts."""
    print(f"\n{'='*60}")
    print("TEST 2: Welch's t-test  Fraud vs Legitimate Amounts")
    print(f"{'='*60}")
    print("H: Mean amount(fraud) = Mean amount(legitimate)")
    print("H: Mean amount(fraud)  Mean amount(legitimate)")
    
    # Load fraud and legitimate amounts
    fraud_amounts = pd.read_sql("""
        SELECT amount FROM fact_transactions WHERE is_fraud = 1
    """, engine)['amount']
    
    legit_amounts = pd.read_sql("""
        SELECT amount FROM fact_transactions WHERE is_fraud = 0 
        ORDER BY RAND() LIMIT 500000
    """, engine)['amount']
    
    print(f"\n   Descriptive Statistics:")
    print(f"   {'Metric':<20} {'Legitimate':>15} {'Fraud':>15}")
    print(f"   {'Count':<20} {len(legit_amounts):>15,} {len(fraud_amounts):>15,}")
    print(f"   {'Mean':<20} {legit_amounts.mean():>15,.2f} {fraud_amounts.mean():>15,.2f}")
    print(f"   {'Median':<20} {legit_amounts.median():>15,.2f} {fraud_amounts.median():>15,.2f}")
    print(f"   {'Std Dev':<20} {legit_amounts.std():>15,.2f} {fraud_amounts.std():>15,.2f}")
    print(f"   {'Min':<20} {legit_amounts.min():>15,.2f} {fraud_amounts.min():>15,.2f}")
    print(f"   {'Max':<20} {legit_amounts.max():>15,.2f} {fraud_amounts.max():>15,.2f}")
    
    # Welch's t-test (unequal variances)
    t_stat, p_value = stats.ttest_ind(fraud_amounts, legit_amounts, equal_var=False)
    
    # Effect size (Cohen's d)
    pooled_std = np.sqrt((fraud_amounts.std()**2 + legit_amounts.std()**2) / 2)
    cohens_d = (fraud_amounts.mean() - legit_amounts.mean()) / pooled_std
    
    result = (f"Fraudulent transactions have SIGNIFICANTLY different amounts. "
              f"Cohen's d = {cohens_d:.3f} ({'large' if abs(cohens_d) > 0.8 else 'medium' if abs(cohens_d) > 0.5 else 'small'} effect size)."
              if p_value < SIGNIFICANCE_LEVEL else
              "No significant difference in amounts between fraud and legitimate.")
    
    print_test_result("Welch's t-test (unequal variances)", t_stat, p_value, result)
    print(f"   Cohen's d (effect size): {cohens_d:.4f}")
    
    return t_stat, p_value, cohens_d


# ============================================================
# TEST 3: Point-Biserial Correlation  Balance Drain & Fraud
# H: No correlation between balance drain percentage and fraud
# H: There is a correlation
# ============================================================
def test_balance_drain_correlation(engine):
    """Point-Biserial Correlation: Balance drain % vs Fraud."""
    print(f"\n{'='*60}")
    print("TEST 3: Point-Biserial  Balance Drain & Fraud Correlation")
    print(f"{'='*60}")
    print("H: No correlation between balance drain and fraud ( = 0)")
    print("H: There is a correlation between balance drain and fraud (  0)")
    
    df = pd.read_sql("""
        SELECT 
            is_fraud,
            CASE 
                WHEN old_balance_orig > 0 
                THEN (old_balance_orig - new_balance_orig) / old_balance_orig
                ELSE 0 
            END AS drain_ratio
        FROM fact_transactions
        WHERE transaction_type IN ('TRANSFER', 'CASH_OUT')
          AND old_balance_orig > 0
        ORDER BY RAND() LIMIT 500000
    """, engine)
    
    # Point-biserial correlation
    corr, p_value = stats.pointbiserialr(df['is_fraud'], df['drain_ratio'])
    
    # Additional stats
    fraud_drain = df[df['is_fraud'] == 1]['drain_ratio']
    legit_drain = df[df['is_fraud'] == 0]['drain_ratio']
    
    print(f"\n   Average Drain Ratio:")
    print(f"   Legitimate: {legit_drain.mean():.4f} (std: {legit_drain.std():.4f})")
    print(f"   Fraud:      {fraud_drain.mean():.4f} (std: {fraud_drain.std():.4f})")
    print(f"   % of fraud with >80% drain: {(fraud_drain > 0.8).mean()*100:.1f}%")
    print(f"   % of legit with >80% drain: {(legit_drain > 0.8).mean()*100:.1f}%")
    
    result = (f"SIGNIFICANT correlation (r = {corr:.4f}) between balance drain and fraud. "
              f"Fraudulent transactions tend to drain accounts more completely."
              if p_value < SIGNIFICANCE_LEVEL else
              "No significant correlation between balance drain and fraud.")
    
    print_test_result("Point-Biserial Correlation", corr, p_value, result)
    
    return corr, p_value


# ============================================================
# TEST 4: Chi-Square Goodness of Fit  Time-of-Day & Fraud
# H: Fraud is uniformly distributed across hours
# H: Fraud is NOT uniformly distributed
# ============================================================
def test_fraud_time_distribution(engine):
    """Chi-Square Goodness of Fit: Fraud distribution across hours."""
    print(f"\n{'='*60}")
    print("TEST 4: Chi-Square Goodness of Fit  Fraud by Hour")
    print(f"{'='*60}")
    print("H: Fraud occurs uniformly across all hours")
    print("H: Fraud is concentrated in certain hours")
    
    df = pd.read_sql("""
        SELECT transaction_hour, COUNT(*) AS fraud_count
        FROM fact_transactions
        WHERE is_fraud = 1
        GROUP BY transaction_hour
        ORDER BY transaction_hour
    """, engine)
    
    observed = df['fraud_count'].values
    total_fraud = observed.sum()
    expected = np.full(len(observed), total_fraud / len(observed))
    
    print(f"\n   Fraud Distribution by Hour:")
    print(f"   {'Hour':<6} {'Observed':>10} {'Expected':>10} {'Ratio':>8}")
    for _, row in df.iterrows():
        exp = total_fraud / len(df)
        ratio = row['fraud_count'] / exp
        marker = " " if ratio > 1.2 or ratio < 0.8 else ""
        print(f"   {row['transaction_hour']:>4}   {row['fraud_count']:>10,}   {exp:>10,.1f}   {ratio:>7.2f}x{marker}")
    
    # Chi-square goodness of fit
    chi2, p_value = stats.chisquare(observed, expected)
    
    result = ("Fraud is NOT uniformly distributed across hours. "
              "Certain hours show significantly higher/lower fraud activity."
              if p_value < SIGNIFICANCE_LEVEL else
              "Fraud appears to be uniformly distributed across hours.")
    
    print_test_result("Chi-Square Goodness of Fit", chi2, p_value, result)
    print(f"   Total fraud transactions: {total_fraud:,}")
    
    return chi2, p_value


# ============================================================
# SUMMARY TABLE
# ============================================================
def print_summary(results):
    """Print summary of all hypothesis tests."""
    print(f"\n{'='*60}")
    print(" HYPOTHESIS TESTING SUMMARY")
    print(f"{'='*60}")
    print(f"\n   {'#':<4} {'Test':<35} {'Statistic':>10} {'P-value':>12} {'Result':>15}")
    print(f"   {''*76}")
    
    for i, (name, stat, p_val) in enumerate(results, 1):
        verdict = "REJECT H" if p_val < SIGNIFICANCE_LEVEL else "FAIL TO REJECT"
        print(f"   {i:<4} {name:<35} {stat:>10.4f} {p_val:>12.2e} {verdict:>15}")
    
    print(f"\n   Significance level:  = {SIGNIFICANCE_LEVEL}")
    print(f"   Tests rejecting H: {sum(1 for _, _, p in results if p < SIGNIFICANCE_LEVEL)} / {len(results)}")


# ============================================================
# MAIN
# ============================================================
def main():
    """Run all statistical hypothesis tests."""
    print(" Statistical Hypothesis Testing Suite")
    print("=" * 60)
    
    engine = get_engine()
    results = []
    
    # Test 1: Chi-Square  Fraud rate by type
    chi2, p = test_fraud_rate_by_type(engine)
    results.append(("Fraud Rate  Txn Type", chi2, p))
    
    # Test 2: Welch's t-test  Amount difference
    t_stat, p, _ = test_amount_difference(engine)
    results.append(("Amount: Fraud vs Legit", t_stat, p))
    
    # Test 3: Point-Biserial  Balance drain
    corr, p = test_balance_drain_correlation(engine)
    results.append(("Balance Drain  Fraud", corr, p))
    
    # Test 4: Goodness of Fit  Hourly distribution
    chi2, p = test_fraud_time_distribution(engine)
    results.append(("Fraud  Hour of Day", chi2, p))
    
    # Summary
    print_summary(results)
    
    print(f"\n All hypothesis tests completed!")
    print("   Next steps: Run automation script (python/05_automation_script.py)")


if __name__ == '__main__':
    main()

