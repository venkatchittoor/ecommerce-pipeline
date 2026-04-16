# Ecommerce Pipeline

A production data engineering project built on **Databricks** and **Delta Lake**, progressing from a **Medallion Architecture** pipeline (Bronze → Silver → Gold) through PySpark data quality checks, customer segmentation, real-time streaming with anomaly detection, and a **natural language data assistant** powered by the Claude API. The pipeline runs as a scheduled **Databricks Job** on serverless compute, pulling code directly from GitHub and auto-detecting credentials at runtime — no manual cluster management or credential configuration required.

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

Explored the pipeline tables interactively using PySpark in a Databricks notebook (`Phase2_PySpark_DataQuality_CustomerSegmentation.ipynb`). Covered DataFrame operations, schema inspection, aggregations, and window functions on top of the Delta tables built by `pipeline.py`.

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

## Phase 3: Streaming Simulation & Anomaly Detection

### Event Stream Simulator

`stream_simulator.py` generates realistic e-commerce order events locally, writing one JSON file to `streaming_data/` every 3 seconds. Each event contains a UUID, random customer and product, quantity, price, timestamp, and status (90% completed / 10% returned). Run it independently to feed the streaming pipeline:

```bash
python stream_simulator.py
# Press Ctrl+C to stop
```

### Micro-Batch Streaming Pipeline

Built a PySpark Structured Streaming pipeline in `Phase3_Streaming_AnomalyDetection.ipynb` that reads JSON events from `streaming_data/` and lands them into a Delta table in micro-batches:

| Metric | Value |
|---|---|
| Events processed | 350 |
| Batches | 5 |
| Destination table | `bronze_orders_stream` |
| Trigger mode | `availableNow` (micro-batch) |

### Anomaly Detection

Applied a **2 standard deviation (2σ) threshold** to `total_price` across the streaming orders to flag statistically unusual transactions:

| Metric | Value |
|---|---|
| Orders analysed | 350 |
| Anomalies flagged | 15 |
| Anomaly rate | 4.3% |
| Revenue at risk | **$63,754** |

Anomalies are persisted with their deviation scores in `gold_stream_anomalies`, enabling downstream alerting and investigation.

**Key finding:** Customer 94 placed 3 anomalous high-value orders — a pattern consistent with a potential fraud signal.

---

## Phase 4: Natural Language Data Assistant

### ask_data.py

`ask_data.py` is a command-line data assistant that accepts plain-English questions, translates them to SQL using Claude, runs them against Databricks, and narrates the results in three sentences. No SQL knowledge required.

```bash
python ask_data.py
```

### Two-Call Claude Pattern

Each question triggers exactly two Claude API calls:

| Call | Input | Output |
|---|---|---|
| **Call 1 — SQL generation** | Natural language question + table schema | A valid SQL query |
| **Call 2 — Narrative** | Original question + SQL + query results | 3-sentence plain-English answer |

Claude autonomously picks the right table, writes the correct JOIN or aggregation, and spots patterns not explicitly asked for.

### Prompt Caching

The table definitions (the full schema context passed in `SYSTEM_PROMPT`) are marked with `cache_control: ephemeral`. After the first call in a session, Anthropic caches this block and subsequent calls skip re-processing it — reducing latency and API cost for every follow-up question.

### Tables Available for Querying

All 14 pipeline tables are exposed to the assistant:

| Table | Layer | Description |
|---|---|---|
| `bronze_customers` | Bronze | Customer records — name, email, country, signup date |
| `bronze_products` | Bronze | Product catalog — name, category, price |
| `bronze_orders` | Bronze | Order headers — date, status |
| `bronze_order_items` | Bronze | Line items — quantity, unit price, total price |
| `silver_order_items` | Silver | All four Bronze tables joined and cleansed |
| `gold_revenue_by_category` | Gold | Revenue, units, and order count per category |
| `gold_top_customers` | Gold | Top 100 customers by lifetime spend |
| `gold_monthly_order_trends` | Gold | Monthly revenue and order volume |
| `gold_return_analysis` | Gold | Return rate and revenue lost by category |
| `silver_customers_enriched` | Silver | Customers with tenure segment labels |
| `gold_customer_segments` | Gold | Revenue aggregated by tenure segment |
| `pipeline_runs` | Logging | One row per pipeline execution |
| `bronze_orders_stream` | Bronze | Streaming order events from micro-batch pipeline |
| `gold_stream_anomalies` | Gold | Anomalous orders flagged by 2σ threshold |

### Example Questions and Answers

**"Which customers in the growing segment have spent the most?"**
> Ashley Pena leads the Growing segment at $14,494 across 23 orders, well ahead of the next-ranked customer. The top Growing segment customers show strong repeat purchase behaviour with high average order values. Nicole Chambers stands out as a high-potential re-engagement target with only 2 orders but one of the highest average order values in the cohort.

**"Are there any suspicious orders I should investigate?"**
> 15 anomalies were flagged by the 2σ detection model, representing 4.3% of all streaming orders. The highest single deviation was $3,510 above the mean threshold, suggesting a potentially fraudulent or data-entry error transaction. Customer 94 appears in multiple flagged records and warrants priority review.

**"Compare return rates across all categories"**
> Home & Kitchen leads all categories with a 16.03% return rate, the highest in the dataset. Electronics follows at 12.87% — a lower rate but the highest absolute revenue lost at $52,323 due to high item prices. Sports has the highest order volume (335 orders) but a comparatively lower return rate.

---

## Phase 5: Databricks Jobs & Orchestration

### Scheduled Databricks Job

`pipeline.py` is configured as a scheduled Databricks Job, running the full Medallion pipeline (Bronze → Silver → Gold) automatically every day without manual intervention.

| Setting | Value |
|---|---|
| Compute | Serverless |
| Schedule | Daily at 6 AM CDT |
| Source | GitHub — `main` branch |
| Notifications | Email on start, success, and failure |

### Serverless Compute

The Job runs on Databricks Serverless compute — no cluster to configure, size, or manage. Databricks provisions and tears down resources automatically for each run.

### GitHub Integration

The Job is connected directly to this repository. Every run pulls the latest code from the `main` branch, so pushing a change here is all that's needed to deploy an update — no manual file uploads or workspace edits.

### Auto-Credential Detection

`pipeline.py` detects its execution context at startup and selects the appropriate credential source:

| Context | Credential source |
|---|---|
| Databricks Job | `WorkspaceClient()` resolves host; notebook context API provides token; first available SQL Warehouse discovered via `w.warehouses.list()` |
| Local | `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN` loaded from `.env` |

No environment variables need to be configured on the cluster — the SDK handles everything automatically.

### CSV File Handling

The four source CSV files are generated at runtime rather than bundled with the repo:

| Context | Write location |
|---|---|
| Databricks Job | `/tmp/` — writable scratch space available on every cluster |
| Local | Current working directory (`.`) |

`generate_data.generate(output_dir=...)` and `pipeline.py`'s Bronze loader both use the same path, keeping generation and ingestion in sync across both environments.

### Run History

Every execution is logged to the `pipeline_runs` Delta table (status, layer reached, row counts, duration) and is also visible in the Databricks Workflows UI with full stdout logs, timing, and re-run controls.

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
| Bronze | `bronze_orders_stream` | 3 | Streaming order events landed via micro-batch pipeline |
| Gold | `gold_stream_anomalies` | 3 | Anomalous orders flagged by 2σ threshold detection |

---

## Key Findings

| Finding | Value |
|---|---|
| Highest return rate | **Home & Kitchen — 16.03%** |
| Most revenue lost to returns | **Electronics — $52,323** |
| Highest order volume category | Sports — 335 orders |
| Top customer lifetime spend | Ashley Pena — $14,494 across 23 orders |
| Electronics return rate | 12.87% — lower than Home & Kitchen but highest absolute revenue lost due to high item prices |
| Highest revenue per customer | **Growing segment** — customers 6–18 months old outspend both newer and longer-tenured cohorts |
| Streaming anomaly rate | **4.3%** of orders flagged by 2σ detection — $63,754 revenue at risk |
| Top fraud signal | Customer 94 placed 3 anomalous high-value orders across streaming data |
| High-potential re-engagement target | Nicole Chambers — only 2 orders but one of the highest average order values in the Growing segment |

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
| AI Layer | Claude API (`claude-sonnet-4-6`) — two-call pattern: SQL generation + result narration |
| Prompt Caching | Anthropic `cache_control: ephemeral` — table schema cached after first call to reduce API cost |

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
├── generate_data.py                                          # Synthetic data generator (Faker-based)
├── pipeline.py                                               # Main pipeline — Bronze, Silver, Gold layers + logging
├── stream_simulator.py                                       # Local event generator — writes JSON to streaming_data/
├── ask_data.py                                               # Phase 4 — natural language data assistant (two-call Claude pattern)
├── Phase2_PySpark_DataQuality_CustomerSegmentation.ipynb    # Phase 2 notebook
├── Phase3_Streaming_AnomalyDetection.ipynb                  # Phase 3 notebook
├── requirements.txt                                          # Python dependencies
├── .env.example                                              # Credentials template
├── PROGRESS.md                                               # Phase-by-phase progress log
├── customers.csv                                             # Generated: 200 customer records
├── products.csv                                              # Generated: 50 product records
├── orders.csv                                                # Generated: 1,000 order headers
└── order_items.csv                                           # Generated: 2,000 order line items
```

### Notebooks

| Notebook | Phase | Contents |
|---|---|---|
| `Phase2_PySpark_DataQuality_CustomerSegmentation.ipynb` | 2 | PySpark exploration, data quality checks, customer tenure segmentation |
| `Phase3_Streaming_AnomalyDetection.ipynb` | 3 | Micro-batch streaming pipeline, anomaly detection using 2σ threshold |

---

## Author

**Venkat Chittoor** — [github.com/venkatchittoor](https://github.com/venkatchittoor)
