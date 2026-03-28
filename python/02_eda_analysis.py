"""
02_eda_analysis.py
==================
Exploratory Data Analysis with Visualizations
Generates publication-quality plots for the AML monitoring project.

Usage:
    python python/02_eda_analysis.py

Output:
    Saves plots to plots/ directory
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from sqlalchemy import create_engine, text
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

PLOTS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'plots')
os.makedirs(PLOTS_DIR, exist_ok=True)

# Style configuration
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")
COLORS = {
    'primary': '#1a73e8',
    'fraud': '#d32f2f',
    'legit': '#2e7d32',
    'accent': '#f57c00',
    'dark': '#263238',
    'light': '#eceff1'
}


def get_engine():
    """Create SQLAlchemy engine."""
    return create_engine(
        f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )


# ============================================================
# PLOT 1: Transaction Amount Distribution
# ============================================================
def plot_amount_distribution(engine):
    """Histogram + box plot of transaction amounts."""
    print(" Generating: Amount Distribution...")
    
    df = pd.read_sql("""
        SELECT amount, transaction_type, is_fraud 
        FROM fact_transactions 
        WHERE amount < 1000000
        ORDER BY RAND() LIMIT 500000
    """, engine)
    
    fig, axes = plt.subplots(2, 1, figsize=(14, 10), gridspec_kw={'height_ratios': [3, 1]})
    
    # Histogram
    axes[0].hist(df[df['is_fraud'] == 0]['amount'], bins=100, alpha=0.7, 
                 color=COLORS['legit'], label='Legitimate', density=True)
    axes[0].hist(df[df['is_fraud'] == 1]['amount'], bins=100, alpha=0.7, 
                 color=COLORS['fraud'], label='Fraudulent', density=True)
    axes[0].set_title('Transaction Amount Distribution: Fraud vs Legitimate', fontsize=16, fontweight='bold')
    axes[0].set_xlabel('Amount', fontsize=12)
    axes[0].set_ylabel('Density', fontsize=12)
    axes[0].legend(fontsize=12)
    axes[0].xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'${x:,.0f}'))
    
    # Box plot by type
    type_order = ['CASH_IN', 'PAYMENT', 'DEBIT', 'TRANSFER', 'CASH_OUT']
    df_types = df[df['transaction_type'].isin(type_order)]
    sns.boxplot(data=df_types, x='transaction_type', y='amount', ax=axes[1],
                order=type_order, palette='Set2', showfliers=False)
    axes[1].set_title('Amount Distribution by Transaction Type (outliers removed)', fontsize=13)
    axes[1].set_xlabel('Transaction Type', fontsize=12)
    axes[1].set_ylabel('Amount', fontsize=12)
    axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'${x:,.0f}'))
    
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'amount_distribution.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("    Saved: plots/amount_distribution.png")


# ============================================================
# PLOT 2: Fraud vs Non-Fraud Comparison
# ============================================================
def plot_fraud_comparison(engine):
    """Violin plot comparing fraud and legitimate transactions."""
    print(" Generating: Fraud Comparison...")
    
    df = pd.read_sql("""
        SELECT amount, transaction_type, is_fraud 
        FROM fact_transactions 
        WHERE transaction_type IN ('TRANSFER', 'CASH_OUT')
          AND amount < 2000000
        ORDER BY RAND() LIMIT 200000
    """, engine)
    
    df['label'] = df['is_fraud'].map({0: 'Legitimate', 1: 'Fraudulent'})
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    
    for idx, txn_type in enumerate(['TRANSFER', 'CASH_OUT']):
        subset = df[df['transaction_type'] == txn_type]
        sns.violinplot(data=subset, x='label', y='amount', ax=axes[idx],
                       palette=[COLORS['legit'], COLORS['fraud']], inner='quartile')
        axes[idx].set_title(f'{txn_type} Transactions', fontsize=14, fontweight='bold')
        axes[idx].set_xlabel('')
        axes[idx].set_ylabel('Amount', fontsize=12)
        axes[idx].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        # Add count annotations
        for label_val, x_pos in [('Legitimate', 0), ('Fraudulent', 1)]:
            count = len(subset[subset['label'] == label_val])
            axes[idx].text(x_pos, axes[idx].get_ylim()[1] * 0.95, f'n={count:,}', 
                          ha='center', fontsize=10, style='italic')
    
    fig.suptitle('Fraud vs Legitimate Transaction Amounts', fontsize=16, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'fraud_comparison.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("    Saved: plots/fraud_comparison.png")


# ============================================================
# PLOT 3: Correlation Heatmap
# ============================================================
def plot_correlation_heatmap(engine):
    """Correlation matrix of numerical features."""
    print(" Generating: Correlation Heatmap...")
    
    df = pd.read_sql("""
        SELECT amount, old_balance_orig, new_balance_orig,
               old_balance_dest, new_balance_dest,
               balance_change, is_fraud
        FROM fact_transactions 
        ORDER BY RAND() LIMIT 500000
    """, engine)
    
    corr_matrix = df.corr()
    
    fig, ax = plt.subplots(figsize=(10, 8))
    mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
    sns.heatmap(corr_matrix, mask=mask, annot=True, fmt='.3f',
                cmap='RdYlBu_r', center=0, ax=ax,
                linewidths=0.5, square=True,
                vmin=-1, vmax=1,
                annot_kws={'size': 11})
    ax.set_title('Feature Correlation Matrix', fontsize=16, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'correlation_heatmap.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("    Saved: plots/correlation_heatmap.png")


# ============================================================
# PLOT 4: Hourly Transaction Pattern
# ============================================================
def plot_hourly_pattern(engine):
    """Time-of-day transaction pattern with fraud overlay."""
    print(" Generating: Hourly Pattern...")
    
    df = pd.read_sql("""
        SELECT 
            transaction_hour,
            COUNT(*) AS total_txns,
            SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_txns,
            ROUND(SUM(amount), 2) AS total_volume
        FROM fact_transactions
        GROUP BY transaction_hour
        ORDER BY transaction_hour
    """, engine)
    
    fig, ax1 = plt.subplots(figsize=(14, 7))
    
    # Bar chart for total transactions
    bars = ax1.bar(df['transaction_hour'], df['total_txns'], 
                   color=COLORS['primary'], alpha=0.7, label='Total Transactions')
    ax1.set_xlabel('Hour of Day', fontsize=13)
    ax1.set_ylabel('Transaction Count', fontsize=13, color=COLORS['primary'])
    ax1.tick_params(axis='y', labelcolor=COLORS['primary'])
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'{x:,.0f}'))
    
    # Line chart for fraud on secondary axis
    ax2 = ax1.twinx()
    ax2.plot(df['transaction_hour'], df['fraud_txns'], 
             color=COLORS['fraud'], linewidth=2.5, marker='o', markersize=6,
             label='Fraud Transactions')
    ax2.set_ylabel('Fraud Count', fontsize=13, color=COLORS['fraud'])
    ax2.tick_params(axis='y', labelcolor=COLORS['fraud'])
    
    ax1.set_title('Transaction Volume & Fraud by Hour of Day', fontsize=16, fontweight='bold')
    ax1.set_xticks(range(24))
    
    # Combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right', fontsize=11)
    
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'hourly_pattern.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("    Saved: plots/hourly_pattern.png")


# ============================================================
# PLOT 5: Transaction Type Breakdown
# ============================================================
def plot_type_breakdown(engine):
    """Pie chart + bar chart of transaction types with fraud rates."""
    print(" Generating: Type Breakdown...")
    
    df = pd.read_sql("""
        SELECT 
            transaction_type,
            COUNT(*) AS txn_count,
            SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
            ROUND(AVG(CASE WHEN is_fraud = 1 THEN 1.0 ELSE 0.0 END) * 100, 4) AS fraud_rate
        FROM fact_transactions
        GROUP BY transaction_type
        ORDER BY txn_count DESC
    """, engine)
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    
    # Pie chart - transaction distribution
    colors_pie = sns.color_palette('Set2', len(df))
    wedges, texts, autotexts = axes[0].pie(
        df['txn_count'], labels=df['transaction_type'],
        autopct='%1.1f%%', colors=colors_pie, startangle=90,
        textprops={'fontsize': 11}
    )
    for autotext in autotexts:
        autotext.set_fontweight('bold')
    axes[0].set_title('Transaction Type Distribution', fontsize=14, fontweight='bold')
    
    # Bar chart - fraud rate by type
    bar_colors = [COLORS['fraud'] if rate > 0 else COLORS['legit'] for rate in df['fraud_rate']]
    bars = axes[1].bar(df['transaction_type'], df['fraud_rate'], color=bar_colors, alpha=0.8)
    axes[1].set_title('Fraud Rate by Transaction Type (%)', fontsize=14, fontweight='bold')
    axes[1].set_ylabel('Fraud Rate (%)', fontsize=12)
    axes[1].set_xlabel('Transaction Type', fontsize=12)
    
    # Add value labels on bars
    for bar, rate in zip(bars, df['fraud_rate']):
        if rate > 0:
            axes[1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
                        f'{rate:.3f}%', ha='center', fontsize=10, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'type_breakdown.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("    Saved: plots/type_breakdown.png")


# ============================================================
# PLOT 6: Fraud Amount Heatmap by Type and Hour
# ============================================================
def plot_fraud_heatmap(engine):
    """Heatmap of fraud occurrences by transaction type and hour."""
    print(" Generating: Fraud Heatmap...")
    
    df = pd.read_sql("""
        SELECT 
            transaction_type,
            transaction_hour,
            SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count
        FROM fact_transactions
        GROUP BY transaction_type, transaction_hour
    """, engine)
    
    pivot = df.pivot_table(values='fraud_count', index='transaction_type', 
                           columns='transaction_hour', fill_value=0)
    
    fig, ax = plt.subplots(figsize=(16, 6))
    sns.heatmap(pivot, annot=True, fmt='g', cmap='YlOrRd', ax=ax,
                linewidths=0.5, cbar_kws={'label': 'Fraud Count'})
    ax.set_title('Fraud Occurrences: Transaction Type  Hour of Day', fontsize=16, fontweight='bold')
    ax.set_xlabel('Hour of Day', fontsize=13)
    ax.set_ylabel('Transaction Type', fontsize=13)
    
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'fraud_heatmap.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("    Saved: plots/fraud_heatmap.png")


# ============================================================
# PLOT 7: Balance Drain Analysis
# ============================================================
def plot_balance_drain(engine):
    """Analysis of balance drain patterns in fraudulent transactions."""
    print(" Generating: Balance Drain Analysis...")
    
    df = pd.read_sql("""
        SELECT 
            amount,
            old_balance_orig,
            new_balance_orig,
            is_fraud,
            transaction_type
        FROM fact_transactions
        WHERE transaction_type IN ('TRANSFER', 'CASH_OUT')
          AND old_balance_orig > 0
        ORDER BY RAND() LIMIT 100000
    """, engine)
    
    df['drain_pct'] = ((df['old_balance_orig'] - df['new_balance_orig']) / df['old_balance_orig']) * 100
    df['label'] = df['is_fraud'].map({0: 'Legitimate', 1: 'Fraudulent'})
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    
    # Histogram of drain percentage
    axes[0].hist(df[df['is_fraud'] == 0]['drain_pct'].clip(0, 100), bins=50, 
                 alpha=0.6, color=COLORS['legit'], label='Legitimate', density=True)
    axes[0].hist(df[df['is_fraud'] == 1]['drain_pct'].clip(0, 100), bins=50, 
                 alpha=0.6, color=COLORS['fraud'], label='Fraudulent', density=True)
    axes[0].set_title('Balance Drain Distribution', fontsize=14, fontweight='bold')
    axes[0].set_xlabel('Balance Drained (%)', fontsize=12)
    axes[0].set_ylabel('Density', fontsize=12)
    axes[0].legend(fontsize=11)
    
    # Scatter: amount vs old_balance (fraud highlighted)
    legit = df[df['is_fraud'] == 0].sample(min(5000, len(df[df['is_fraud'] == 0])))
    fraud = df[df['is_fraud'] == 1]
    
    axes[1].scatter(legit['old_balance_orig'], legit['amount'], 
                    alpha=0.1, s=5, color=COLORS['legit'], label='Legitimate')
    axes[1].scatter(fraud['old_balance_orig'], fraud['amount'], 
                    alpha=0.5, s=15, color=COLORS['fraud'], label='Fraudulent')
    axes[1].set_title('Amount vs Balance (Fraud Highlighted)', fontsize=14, fontweight='bold')
    axes[1].set_xlabel('Old Balance', fontsize=12)
    axes[1].set_ylabel('Transaction Amount', fontsize=12)
    axes[1].legend(fontsize=11)
    axes[1].set_xscale('log')
    axes[1].set_yscale('log')
    
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'balance_drain.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("    Saved: plots/balance_drain.png")


# ============================================================
# MAIN
# ============================================================
def main():
    """Run all EDA visualizations."""
    print(" Starting Exploratory Data Analysis")
    print("=" * 50)
    
    engine = get_engine()
    
    # Test connection
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM fact_transactions"))
            count = result.scalar()
            print(f" Connected! fact_transactions has {count:,} rows")
    except Exception as e:
        print(f" Connection failed: {e}")
        return
    
    # Generate all plots
    plot_amount_distribution(engine)
    plot_fraud_comparison(engine)
    plot_correlation_heatmap(engine)
    plot_hourly_pattern(engine)
    plot_type_breakdown(engine)
    plot_fraud_heatmap(engine)
    plot_balance_drain(engine)
    
    print("\n" + "=" * 50)
    print(f" All plots saved to: {PLOTS_DIR}")
    print("   7 visualizations generated successfully!")


if __name__ == '__main__':
    main()

