# Ecommerce Pipeline

A production-style data engineering pipeline built on **Databricks** and **Delta Lake**, implementing the **Medallion Architecture** (Bronze → Silver → Gold) to transform raw e-commerce CSVs into business-ready aggregation tables, a BI dashboard, and automated data quality monitoring.

---

## Phase 1: Medallion Architecture & Core Pipeline

### What Was Built

Designed and implemented a full Medallion pipeline that takes raw CSV files through three layers of transformation and lands them in Delta tables queryable by BI tools.

### Medallion Architecture

```
CSV Files
    │
    ▼
┌─────────────────────────────────────┐
│  BRONZE  — Raw Ingest               │
│  No transformation. All columns     │
│  stored as strings. Source of truth.│
└─────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────┐
│  SILVER  — Cleanse & Join           │
│  Types cast, nulls removed, all     │
│  four tables joined into one fact   │
│  table.                             │
└─────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────┐
│  GOLD  — Business Aggregations      │
│  Summary tables ready for BI tools  │
│  and dashboards.                    │
└─────────────────────────────────────┘
```

Each layer builds on the previous one. Re-running `pipeline.py` truncates and reloads each layer so results are always consistent (**fully idempotent**).

### 8 Delta Tables

| Layer | Table | Rows | Description |
|---|---|---|---|
| Bronze | `bronze_customers` | 200 | Raw customer records — name, email, country, signup date |
| Bronze | `bronze_products` | 50 | Raw product catalog — name, category, base price |
| Bronze | `bronze_orders` | 1,000 | Raw order headers — order date, status |
| Bronze | `bronze_order_items` | 2,000 | Raw line items — quantity, unit price, total price |
| Silver | `silver_order_items` | 2,000 | All four bronze tables joined and cleansed into a single enriched fact table |
| Gold | `gold_revenue_by_category` | 7 | Total revenue, units sold, and order count per product category |
| Gold | `gold_top_customers` | 100 | Top 100 customers ranked by lifetime spend |
| Gold | `gold_monthly_order_trends` | 7 | Monthly order volume and revenue over the trailing 6 months |

Silver transformations applied:
- Dates cast from string to `DATE`
- Prices and quantities cast to `DOUBLE` / `INT`
- Emails lowercased and trimmed
- Rows with null keys, zero prices, or zero quantities filtered out

### Databricks SQL Dashboard

Built a live dashboard in Databricks SQL connected directly to the Gold tables, visualising:
- Revenue breakdown by product category
- Top customers by lifetime spend
- Monthly order trend
- Return rate and revenue lost per category

### Delta Lake Features

**Idempotency** — every table is truncated before each load, guaranteeing clean results on re-runs with no duplicate rows.

**Time Travel** — Delta Lake automatically versions every table on every write:

```sql
-- Query silver table as it looked after the first pipeline run
SELECT * FROM workspace.ecommerce.silver_order_items VERSION AS OF 0;

-- Query by timestamp
SELECT * FROM workspace.ecommerce.silver_order_items
TIMESTAMP AS OF '2024-01-01 00:00:00';
```

**RESTORE** — roll any table back to a prior version:

```sql
RESTORE TABLE workspace.ecommerce.silver_order_items TO VERSION AS OF 0;
```

**Delta History** — full audit log of every change:

```sql
DESCRIBE HISTORY workspace.ecommerce.silver_order_items;
```

---

## Phase 2: Data Quality, PySpark & Pipeline Monitoring

### PySpark Exploration in Databricks Notebooks

Explored the pipeline tables interactively using PySpark in a Databricks notebook (`Phase2_PySpark_Exploration.ipynb`). Covered DataFrame operations, schema inspection, aggregations, and window functions on top of the Delta tables built by `pipeline.py`.

### Data Quality Checks

Added an automated quality gate to `pipeline.py` that runs after Bronze ingestion and blocks Silver if any check fails. **27 checks** run across all 4 Bronze tables on every pipeline execution:

| Check type | What it validates |
|---|---|
| Row count | Each table meets a minimum row threshold |
| Duplicate primary keys | `customer_id`, `product_id`, `order_id`, `item_id` are unique |
| Null percentage | Every column in every table is 0% null |

Results are printed as a PASS/FAIL report before Silver runs. A single failure raises an error, logs a `FAILED` record to `pipeline_runs`, and halts the pipeline.

### Customer Segmentation

Two new tables built in the PySpark notebook to analyse revenue by customer tenure:

**`silver_customers_enriched`** — extends the silver layer with a derived `tenure_segment` based on `signup_date`:

| Segment | Definition |
|---|---|
| New | Signed up within the last 6 months |
| Growing | Signed up 6–18 months ago |
| Loyal | Signed up more than 18 months ago |

**`gold_customer_segments`** — aggregates revenue, order count, and average order value grouped by tenure segment.

**Key finding:** the **Growing segment** (customers 6–18 months old) generates the highest revenue per person, outspending both newer and longer-tenured cohorts.

### Pipeline Run Logging

Every pipeline execution — success or failure — appends one row to `workspace.ecommerce.pipeline_runs`:

| Column | Description |
|---|---|
| `run_id` | Unique UUID per run |
| `run_timestamp` | UTC start time |
| `status` | `SUCCESS` or `FAILED` |
| `layer_reached` | Last layer attempted: `BRONZE`, `QUALITY_CHECK`, `SILVER`, or `GOLD` |
| `failed_checks` | Semicolon-separated list of failing check descriptions; `NULL` on success |
| `rows_bronze_*` | Row counts for each Bronze table at time of run |
| `duration_seconds` | Wall-clock runtime |

Query run history at any time:

```sql
SELECT run_timestamp, status, layer_reached, failed_checks, duration_seconds
FROM workspace.ecommerce.pipeline_runs
ORDER BY run_timestamp;
```

---

## All Tables

| Layer | Table | Phase | Description |
|---|---|---|---|
| Bronze | `bronze_customers` | 1 | Raw customer records |
| Bronze | `bronze_products` | 1 | Raw product catalog |
| Bronze | `bronze_orders` | 1 | Raw order headers |
| Bronze | `bronze_order_items` | 1 | Raw line items |
| Silver | `silver_order_items` | 1 | Cleansed and joined fact table |
| Gold | `gold_revenue_by_category` | 1 | Revenue and units by product category |
| Gold | `gold_top_customers` | 1 | Top 100 customers by lifetime spend |
| Gold | `gold_monthly_order_trends` | 1 | Monthly revenue and order volume |
| Gold | `gold_return_analysis` | 2 | Return rate and revenue lost by category |
| Silver | `silver_customers_enriched` | 2 | Customers with tenure segment labels |
| Gold | `gold_customer_segments` | 2 | Revenue aggregated by tenure segment |
| Logging | `pipeline_runs` | 2 | One row per pipeline execution |

---

## Key Findings

| Finding | Value |
|---|---|
| Highest return rate | **Home & Kitchen — 16.03%** |
| Most revenue lost to returns | **Electronics — $52,323** |
| Highest order volume category | Sports — 335 orders |
| Top customer lifetime spend | Ashley Pena — $14,494 across 10 orders |
| Electronics return rate | 12.87% — lower than Home & Kitchen but highest absolute revenue lost due to high item prices |
| Highest revenue per customer | **Growing segment** — customers 6–18 months old outspend both newer and longer-tenured cohorts |

---

## Tech Stack

| Component | Technology |
|---|---|
| Cloud Data Platform | Databricks (SQL Warehouse + Unity Catalog) |
| Storage Format | Delta Lake |
| Orchestration | Python (`pipeline.py`) |
| SQL Execution | Databricks SQL Connector for Python |
| Notebook Exploration | PySpark (Databricks Notebooks) |
| Data Generation | Faker |
| Data Manipulation | pandas |
| Dashboard | Databricks SQL Dashboards |

---

## Setup

### Prerequisites
- Python 3.9+
- A Databricks workspace with a running SQL Warehouse
- A Databricks personal access token

### 1. Clone the repo

```bash
git clone https://github.com/venkatchittoor/ecommerce-pipeline.git
cd ecommerce-pipeline
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure credentials

```bash
cp .env.example .env
```

Edit `.env` and fill in your Databricks credentials:

```
DATABRICKS_HOST=https://<your-workspace>.azuredatabricks.net
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<your-warehouse-id>
DATABRICKS_TOKEN=<your-personal-access-token>
```

The pipeline auto-detects a writable catalog at runtime. To override, add:

```
DATABRICKS_CATALOG=main
DATABRICKS_SCHEMA=ecommerce
```

### 4. Generate sample data

```bash
python generate_data.py
```

This creates four CSV files in the project root: `customers.csv`, `products.csv`, `orders.csv`, `order_items.csv`.

### 5. Run the pipeline

```bash
python pipeline.py
```

The pipeline will:
- Connect to your Databricks SQL Warehouse
- Create the `ecommerce` schema if it does not exist
- Run Bronze → Data Quality → Silver → Gold
- Print a 27-check quality report before Silver runs
- Log the run result to `pipeline_runs`
- Print row counts and 5-row previews for each table

---

## Project Structure

```
ecommerce-pipeline/
├── generate_data.py                    # Synthetic data generator (Faker-based)
├── pipeline.py                         # Main pipeline — Bronze, Silver, Gold layers + logging
├── Phase2_PySpark_Exploration.ipynb  # PySpark notebook — segmentation, quality checks, time travel
├── requirements.txt                    # Python dependencies
├── .env.example                        # Credentials template
├── PROGRESS.md                         # Session-by-session progress log
├── customers.csv                       # Generated: 200 customer records
├── products.csv                        # Generated: 50 product records
├── orders.csv                          # Generated: 1,000 order headers
└── order_items.csv                     # Generated: 2,000 order line items
```

---

## Author

**Venkat Chittoor** — [github.com/venkatchittoor](https://github.com/venkatchittoor)
