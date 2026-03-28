# Financial Transaction Monitoring & AML/KYC Compliance System

## The Problem Statement

Modern banks and fintech companies process millions of transactions per day. It is humanly impossible to manually monitor every single transfer for money laundering, terrorism financing, or outright fraud. Traditional compliance systems rely on rigid, hard-coded rules—like "flag everything over $10,000". These outdated rules are easily bypassed by smart criminals and end up generating a massive volume of "false alarms" that overwhelm compliance teams.

## Why This Issue Must Be Cured (The Impact)

When financial institutions fail to catch money laundering, the stakes are existential. 
- **Massive Fines & Reputational Ruin:** Regulatory agencies (like FinCEN or the RBI) regularly levy multi-million dollar fines against banks with weak monitoring.
- **Operational Chokehold:** Those rigid "false alarms" force highly paid compliance analysts to waste hundreds of hours investigating perfectly legitimate transactions. 

Ultimately, inefficient monitoring costs banks exorbitant amounts of money and creates a backdoor for devastating financial crimes.

## The Solution

I built a completely automated, end-to-end **Anti-Money Laundering (AML) and Know Your Customer (KYC)** transaction monitoring system. 

Instead of relying solely on baseline rules, this system:
1. Automates the ingestion of millions of daily transaction logs.
2. Uses advanced **statistical anomaly detection algorithms** to catch suspicious behavior (like sudden spikes in volume or transactions occurring at weird hours).
3. Evaluates every single user with a **multi-factor risk scoring engine** (0-100 scale).
4. Feeds flagged transactions directly into a sleek, real-time **compliance dashboard** so human analysts can investigate the true threats immediately.

## Tools Required

To bring this solution to life, I engineered a complete data pipeline utilizing modern analytics infrastructure:

| Technology | Purpose in Project |
| :--- | :--- |
| **Python 3.10+** | **The Analytical Engine:** Used `pandas`, `numpy`, and `scipy` for large-scale Extract, Transform, Load (ETL) processing. Handled automated data cleaning and implemented statistical anomaly detection (Z-Score, IQR, Rolling Windows). |
| **MySQL 8.0** | **The Data Warehouse:** Designed an optimized **Star Schema** with 11 performance indexes. Built out advanced SQL components including window functions, views, and automated stored procedures to handle massive data querying efficiently. |
| **Power BI** | **The Operational View:** Developed an interactive, 4-page dark-themed compliance dashboard mapped directly to the database views to simulate real-world analyst tools. |
| **PaySim Dataset** | **The Raw Material:** A Kaggle dataset simulating 1.14 million+ real-world mobile money transactions. |

## Project Structure

```text
financial_aml_project/
├── sql/                          # 7 SQL files
│   ├── 01_schema_design.sql      # Star schema: fact + dimension tables
│   ├── 02_data_loading.sql       # Data validation & dim population
│   ├── 03_eda_queries.sql        # 10 EDA queries
│   ├── 04_anomaly_detection.sql  # 6 anomaly detection methods
│   ├── 05_risk_scoring.sql       # Multi-factor risk scoring
│   ├── 06_monitoring_reports.sql # Daily/weekly compliance reports
│   └── 07_stored_procedures.sql  # 4 stored procs + 3 views
├── python/                       # 5 Python scripts
│   ├── 01_etl_pipeline.py        # CSV → MySQL ETL (chunked loading)
│   ├── 02_eda_analysis.py        # 7 publication-quality visualizations
│   ├── 03_anomaly_detection.py   # Z-Score, IQR, Rolling Window + Ensemble
│   ├── 04_statistical_tests.py   # 4 hypothesis tests
│   └── 05_automation_script.py   # Automated daily monitoring pipeline
├── plots/                        # Generated visualizations
├── powerbi/                      # Power BI dashboard (.pbix) + theme JSON
├── docs/                         # Documentation + dashboard screenshots
├── data/
│   ├── raw/                      # Original CSV (not on github)
│   └── processed/                # Cleaned exports
├── requirements.txt              # Python dependencies
└── .gitignore
```

## The Final Product: Dashboard Snapshots

The end result is an operational Power BI dashboard made for real compliance analysts. 

### Page 1: Executive Overview
High-level KPIs including total volume, fraud counts, and system-wide fraud rates at a glance.

![Executive Overview](docs/screenshots/01_executive_overview.png)

### Page 2: Alert Triage
The operational queue where analysts filter, prioritize, and investigate flagged anomalies.

![Alert Triage](docs/screenshots/02_alert_triage.png)

### Page 3: Customer 360 (Drill-Through)
A deep-dive profile of specific customers, showing their risk tier, history, and transaction networks.

![Customer 360](docs/screenshots/03_customer_360.png)

### Page 4: Monitoring Trends & Patterns
Temporal breakdown showing fraud concentration by time-of-day, identifying high-risk windows.

![Monitoring Trends](docs/screenshots/04_monitoring_trends.png)

## Key Observations

Throughout the development and analysis phases of this project, several critical patterns emerged regarding fraudulent financial behavior:

1. **Transaction Type Concentration:** The vast majority of illicit activities (money laundering and fraud) within the dataset are heavily concentrated in just two transaction types: `TRANSFER` and `CASH_OUT`. Standard payments and low-value transfers show minimal anomalous behavior.
2. **The "Pass-Through" Pattern:** A common laundering technique observed is the immediate cash-out following a large transfer. Malicious actors typically move funds into an account and almost instantly withdraw them to minimize traceability, a pattern our rolling-window anomaly detection effectively catches.
3. **Temporal Anomalies:** Time-series analysis reveals that high-risk transactions frequently occur outside of standard business hours or in sudden, unpredictable spikes that deviate significantly from a customer's established baseline behavior.
4. **Efficiency of Risk Scoring over Static Rules:** By implementing a statistical, multi-factor risk scoring engine rather than rigid rules (e.g., "flag every transaction > $10,000"), the system successfully identified sophisticated structured transactions (smurfing) while significantly reducing the false-positive rate that plagues traditional compliance queues.
