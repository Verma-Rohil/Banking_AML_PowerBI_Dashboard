# 🏦 Financial Transaction Monitoring & AML/KYC Compliance System

## ⚠️ The Problem Statement

Modern banks and fintech companies process millions of transactions per day. It is humanly impossible to manually monitor every single transfer for money laundering, terrorism financing, or outright fraud. Traditional compliance systems rely on rigid, hard-coded rules—like "flag everything over $10,000". These outdated rules are easily bypassed by smart criminals and end up generating a massive volume of "false alarms" that overwhelm compliance teams.

## 🚨 Why This Issue Must Be Cured (The Impact)

When financial institutions fail to catch money laundering, the stakes are existential. 
- **Massive Fines & Reputational Ruin:** Regulatory agencies (like FinCEN or the RBI) regularly levy multi-million dollar fines against banks with weak monitoring.
- **Operational Chokehold:** Those rigid "false alarms" force highly paid compliance analysts to waste hundreds of hours investigating perfectly legitimate transactions. 
Ultimately, inefficient monitoring costs banks exorbitant amounts of money and creates a backdoor for devastating financial crimes.

## 💡 The Solution

I built a completely automated, end-to-end **Anti-Money Laundering (AML) and Know Your Customer (KYC)** transaction monitoring system. 

Instead of relying solely on baseline rules, this system:
1. Automates the ingestion of millions of daily transaction logs.
2. Uses advanced **statistical anomaly detection algorithms** to catch suspicious behavior (like sudden spikes in volume or transactions occurring at weird hours).
3. Evaluates every single user with a **multi-factor risk scoring engine** (0-100 scale).
4. Feeds flagged transactions directly into a sleek, real-time **compliance dashboard** so human analysts can investigate the true threats immediately.

## 🛠️ Resources Needed (Tech Stack & Data)

To bring this solution to life, I utilized a robust, modern data stack:
- **Dataset:** The Kaggle PaySim dataset, simulating 1.14 million+ real-world mobile money transactions.
- **Python (The Brain):** Used `pandas` and `scipy` for Extract, Transform, Load (ETL) processing, automated data cleaning, and running the statistical detection algorithms (Z-Score, IQR, Rolling Windows).
- **MySQL (The Engine):** Designed an optimized **Star Schema** data warehouse built with advanced SQL concepts like window functions, views, and stored procedures to handle massive querying efficiently.
- **Power BI (The Eyes):** Developed an interactive, 4-page dark-themed compliance dashboard mapped directly to the database.

## 🚀 Steps Taken

Here is the step-by-step workflow of how I engineered this system from the ground up:

1. **Data Ingestion & Cleaning (ETL):** I wrote Python scripts to process the massive 470 MB dataset in manageable chunks, cleaning dirty data and engineering new features (like `balance_change`) before batch-loading it into the database.
2. **Database Architecture:** I built a highly-optimized Star Schema in MySQL. By separating the data into a central `fact_transactions` table and surrounding `dimension` tables, analytical queries became incredibly fast.
3. **Exploratory Data Analysis (EDA):** I ran a deep dive using SQL and Python visualization libraries to uncover patterns. *Key Finding: Fraud almost exclusively happened in `TRANSFER` and `CASH_OUT` transactions, noticeably surging during the off-hours of 2:00 AM.*
4. **Intelligent Anomaly Detection:** I implemented multi-layered detection models rather than simple rules. If a user normally transfers $50 and suddenly transfers $50,000, the Z-Score engine immediately flags the deviation.
5. **Customer Risk Scoring:** I authored a SQL engine that calculates a dynamic risk score (0-100) for every user, mapping them to actionable tiers (Low, Medium, High, Critical).
6. **Automation & Reporting:** I wrapped the pipeline in an automated script that refreshes data, runs detections, logs alerts into a review queue, and generates daily Excel compliance reports.

## 📊 The Final Product: Dashboard Snapshots

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
