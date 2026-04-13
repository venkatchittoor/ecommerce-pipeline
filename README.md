# Ecommerce Pipeline

A production-style data engineering pipeline built on **Databricks** and **Delta Lake**, implementing the **Medallion Architecture** (Bronze → Silver → Gold) to transform raw e-commerce CSVs into business-ready aggregation tables and a BI dashboard.

---

## What It Does

1. Generates realistic synthetic e-commerce data (customers, products, orders, order items)
2. Ingests raw CSVs into Delta tables with zero transformation (Bronze)
3. Cleanses, casts, and joins the raw tables into a single enriched fact table (Silver)
4. Aggregates the fact table into four business metric tables consumed by a dashboard (Gold)

The pipeline is fully **idempotent** — it can be re-run at any time and produces the same result.

---

## Medallion Architecture

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

Each layer builds on the previous one. Re-running the pipeline truncates and reloads each layer so results are always consistent.

---

## Tables

### Bronze (4 tables) — Raw Ingest

| Table | Source File | Rows | Description |
|---|---|---|---|
| `bronze_customers` | customers.csv | 200 | Raw customer records — name, email, country, signup date |
| `bronze_products` | products.csv | 50 | Raw product catalog — name, category, base price |
| `bronze_orders` | orders.csv | 1,000 | Raw order headers — order date, status |
| `bronze_order_items` | order_items.csv | 2,000 | Raw line items — quantity, unit price, total price |

All columns stored as strings exactly as they appear in the source files. No filtering, casting, or joining at this layer.

### Silver (1 table) — Cleansed & Joined

| Table | Rows | Description |
|---|---|---|
| `silver_order_items` | 2,000 | All four bronze tables joined into a single enriched fact table with correct data types, trimmed strings, and null rows removed |

Transformations applied:
- Dates cast from string to `DATE`
- Prices and quantities cast to `DOUBLE` / `INT`
- Emails lowercased and trimmed
- Rows with null keys, zero prices, or zero quantities filtered out

### Gold (4 tables) — Business Aggregations

| Table | Rows | Description |
|---|---|---|
| `gold_revenue_by_category` | 7 | Total revenue, units sold, and order count per product category (returns excluded) |
| `gold_top_customers` | 100 | Top 100 customers ranked by lifetime spend (returns excluded) |
| `gold_monthly_order_trends` | 7 | Monthly order volume and revenue trend over the trailing 6 months |
| `gold_return_analysis` | 7 | Return rate and total revenue lost per product category |

---

## Delta Lake Features Demonstrated

### Idempotency
Every table is truncated before each load. Re-running `pipeline.py` always produces a clean, consistent result with no duplicate rows.

### Time Travel
Delta Lake automatically versions every table on each write. You can query any previous version:

```sql
-- Query silver table as it looked after the first pipeline run
SELECT * FROM workspace.ecommerce.silver_order_items VERSION AS OF 0;

-- Query by timestamp
SELECT * FROM workspace.ecommerce.silver_order_items
TIMESTAMP AS OF '2024-01-01 00:00:00';
```

### RESTORE
Roll a table back to any prior version:

```sql
-- Restore silver table to version 0
RESTORE TABLE workspace.ecommerce.silver_order_items TO VERSION AS OF 0;
```

### Delta History
Inspect the full audit log of changes to any table:

```sql
DESCRIBE HISTORY workspace.ecommerce.silver_order_items;
```

---

## Key Findings

| Finding | Value |
|---|---|
| Highest return rate | **Home & Kitchen — 16.03%** |
| Most revenue lost to returns | **Electronics — $52,323** |
| Highest order volume category | Sports — 335 orders |
| Top customer lifetime spend | Ashley Pena — $14,494 across 10 orders |
| Electronics return rate | 12.87% — lower than Home & Kitchen but highest absolute revenue lost due to high item prices |

---

## Tech Stack

| Component | Technology |
|---|---|
| Cloud Data Platform | Databricks (SQL Warehouse + Unity Catalog) |
| Storage Format | Delta Lake |
| Orchestration | Python (`pipeline.py`) |
| SQL Execution | Databricks SQL Connector for Python |
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
- Run all three layers (Bronze → Silver → Gold)
- Print row counts and 5-row previews for each table

---

## Project Structure

```
ecommerce-pipeline/
├── generate_data.py      # Synthetic data generator (Faker-based)
├── pipeline.py           # Main pipeline — Bronze, Silver, Gold layers
├── requirements.txt      # Python dependencies
├── .env.example          # Credentials template
├── PROGRESS.md           # Session-by-session progress log
├── customers.csv         # Generated: 200 customer records
├── products.csv          # Generated: 50 product records
├── orders.csv            # Generated: 1,000 order headers
└── order_items.csv       # Generated: 2,000 order line items
```

---

## Author

**Venkat Chittoor** — [github.com/venkatchittoor](https://github.com/venkatchittoor)
