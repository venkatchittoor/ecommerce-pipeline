# Ecommerce Pipeline — Progress Log

## Phase 1 — Medallion Architecture & Core Pipeline

### What Was Built

**Medallion Architecture (Bronze → Silver → Gold)**

A full Databricks medallion pipeline ingesting e-commerce CSVs and producing business-ready aggregation tables via a Databricks SQL Warehouse.

---

### Bronze Layer — Raw Ingest (4 tables)

| Table | Source | Rows |
|---|---|---|
| `bronze_customers` | customers.csv | 200 |
| `bronze_products` | products.csv | 50 |
| `bronze_orders` | orders.csv | 1,000 |
| `bronze_order_items` | order_items.csv | 2,000 |

- No transformation — all columns ingested as raw strings
- Tables are truncated and reloaded on each run (idempotent)

---

### Silver Layer — Cleansed & Joined (1 table)

| Table | Rows |
|---|---|
| `silver_order_items` | 2,000 |

- All four bronze tables joined into a single enriched fact table
- Data types cast (dates, decimals, integers)
- Emails lowercased and trimmed; nulls and zero-price rows filtered out

---

### Gold Layer — Business Aggregations (4 tables)

| Table | Description | Rows |
|---|---|---|
| `gold_revenue_by_category` | Total revenue, units, and order count per product category (returns excluded) | 7 |
| `gold_top_customers` | Top 100 customers ranked by lifetime spend (returns excluded) | 100 |
| `gold_monthly_order_trends` | Monthly order volume and revenue trend | 7 |
| `gold_return_analysis` | Return rate and revenue lost per product category | 7 |

---

### E-Commerce Performance Dashboard (5 panels)

Built in Databricks SQL dashboards against the Gold tables:

1. **Revenue by Category** — bar chart from `gold_revenue_by_category`
2. **Top Customers by Spend** — table from `gold_top_customers`
3. **Monthly Order Trends** — line chart from `gold_monthly_order_trends`
4. **Return Rate by Category** — bar chart from `gold_return_analysis`
5. **Revenue Lost to Returns** — bar chart from `gold_return_analysis`

---

### Key Findings

- **Home & Kitchen** has the highest return rate at **16.03%**
- **Electronics** accounts for the most revenue lost to returns: **$52,323** despite a 12.87% return rate — driven by high item prices
- **Sports** has the highest order volume (335 orders) with a relatively low return rate of 12.54%
- Top customer (Ashley Pena) spent **$14,494** across 10 orders

---

## Phase 3 — Streaming Simulation & Anomaly Detection

### What Was Built

**Stream Simulator**

Built `stream_simulator.py` — a standalone script that generates realistic e-commerce order events and writes one JSON file per event to `streaming_data/` every 3 seconds. Events include UUID, customer/product IDs, quantity, price, timestamp, and status (90% completed / 10% returned). Runs indefinitely until Ctrl+C.

---

### Micro-Batch Streaming Pipeline

Built a PySpark Structured Streaming pipeline (`Phase3_Streaming_AnomalyDetection.ipynb`) that reads JSON events from `streaming_data/` and lands them into a Delta table in micro-batches:

| Metric | Value |
|---|---|
| Events processed | 350 |
| Batches | 5 |
| Destination table | `bronze_orders_stream` |
| Trigger mode | `availableNow` (micro-batch) |

---

### Anomaly Detection — 2σ Threshold

Applied a 2 standard deviation threshold to `total_price` across all streaming orders to flag statistically unusual transactions:

| Metric | Value |
|---|---|
| Orders analysed | 350 |
| Anomalies flagged | 15 |
| Anomaly rate | 4.3% |
| Revenue at risk | $63,754 |

Flagged orders persisted with deviation scores into `gold_stream_anomalies` for downstream alerting.

---

### Key Findings

- **4.3% anomaly rate** in streaming orders — 15 of 350 events flagged by 2σ threshold
- **$63,754 revenue at risk** across the 15 anomalous orders
- **Customer 94** placed 3 anomalous high-value orders — pattern consistent with a potential fraud signal
- README updated with full Phase 3 documentation
