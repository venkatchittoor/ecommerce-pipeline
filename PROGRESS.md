# Ecommerce Pipeline — Progress Log

## Session 1 — Bronze / Silver / Gold Pipeline

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
