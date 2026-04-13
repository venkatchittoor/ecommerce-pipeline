"""
pipeline.py
E-Commerce Orders Pipeline — Medallion Architecture (Bronze → Silver → Gold)

Prerequisites
─────────────
1. Copy .env.example → .env and fill in your Databricks credentials.
2. Run generate_data.py to produce the four CSV files.
3. pip install -r requirements.txt

The pipeline uses the Databricks SQL Connector to execute SQL against a
Databricks SQL Warehouse.  Each layer builds on the previous one:

  Bronze  – raw ingest (CSVs → Delta tables, no transformation)
  Silver  – cleansed, typed, and joined data
  Gold    – aggregated business metrics ready for BI / reporting
"""

import os
import textwrap

import pandas as pd
from databricks import sql
from dotenv import load_dotenv
from tabulate import tabulate

# ── Environment ───────────────────────────────────────────────────────────────

load_dotenv()

DATABRICKS_HOST      = os.environ["DATABRICKS_HOST"]
DATABRICKS_HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
DATABRICKS_TOKEN     = os.environ["DATABRICKS_TOKEN"]

# All tables live in this catalog + schema (Unity Catalog style).
# Override via DATABRICKS_CATALOG / DATABRICKS_SCHEMA in .env, or let the
# pipeline auto-detect a writable catalog at runtime.
CATALOG = os.environ.get("DATABRICKS_CATALOG", "main")
SCHEMA  = os.environ.get("DATABRICKS_SCHEMA", "ecommerce")


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_connection():
    """Return an open Databricks SQL connection."""
    return sql.connect(
        server_hostname=DATABRICKS_HOST.replace("https://", ""),
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    )


def run_sql(cursor, statement: str, *, description: str = "") -> None:
    """Execute a SQL statement and print its description."""
    if description:
        print(f"    {description}")
    cursor.execute(textwrap.dedent(statement))


def upload_csv_to_table(cursor, df: pd.DataFrame, table: str) -> None:
    """
    Upload a pandas DataFrame to a Delta table by inserting rows in batches.
    This avoids needing file-system / DBFS access from the client.
    """
    cols      = ", ".join(df.columns)
    row_count = len(df)
    BATCH     = 200

    for start in range(0, row_count, BATCH):
        chunk  = df.iloc[start : start + BATCH]
        values = ", ".join(
            "(" + ", ".join(
                "NULL" if pd.isna(v) else
                f"'{str(v).replace(chr(39), chr(39)*2)}'"   # escape single quotes
                for v in row
            ) + ")"
            for row in chunk.itertuples(index=False)
        )
        cursor.execute(f"INSERT INTO {table} ({cols}) VALUES {values}")

    print(f"      → {row_count:,} rows loaded into {table}")


def preview(cursor, table: str, n: int = 5) -> None:
    """Print the first n rows of a table."""
    cursor.execute(f"SELECT * FROM {table} LIMIT {n}")
    rows    = cursor.fetchall()
    headers = [d[0] for d in cursor.description]
    print(tabulate(rows, headers=headers, tablefmt="rounded_outline"))


# ═════════════════════════════════════════════════════════════════════════════
# BRONZE LAYER — Raw Ingest
# Goal  : land all four CSVs into Delta tables with zero transformation.
#         Column names and data types reflect the source files exactly.
# ═════════════════════════════════════════════════════════════════════════════

BRONZE_DDL = {
    "bronze_customers": """
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.bronze_customers (
            customer_id  BIGINT,
            name         STRING,
            email        STRING,
            country      STRING,
            signup_date  STRING      -- kept as raw string; parsed in Silver
        )
        USING DELTA
        TBLPROPERTIES ('layer' = 'bronze')
    """,
    "bronze_products": """
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.bronze_products (
            product_id   BIGINT,
            name         STRING,
            category     STRING,
            base_price   STRING      -- kept as raw string; cast in Silver
        )
        USING DELTA
        TBLPROPERTIES ('layer' = 'bronze')
    """,
    "bronze_orders": """
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.bronze_orders (
            order_id     BIGINT,
            customer_id  BIGINT,
            order_date   STRING,
            status       STRING
        )
        USING DELTA
        TBLPROPERTIES ('layer' = 'bronze')
    """,
    "bronze_order_items": """
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.bronze_order_items (
            item_id      BIGINT,
            order_id     BIGINT,
            product_id   BIGINT,
            quantity     BIGINT,
            unit_price   STRING,
            total_price  STRING
        )
        USING DELTA
        TBLPROPERTIES ('layer' = 'bronze')
    """,
}

CSV_TABLE_MAP = {
    "customers.csv":   "bronze_customers",
    "products.csv":    "bronze_products",
    "orders.csv":      "bronze_orders",
    "order_items.csv": "bronze_order_items",
}


def run_bronze(cursor) -> None:
    print("\n" + "═" * 60)
    print("  BRONZE — Raw Ingest")
    print("═" * 60)

    # 1. Ensure the schema exists
    run_sql(cursor, f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}",
            description=f"Ensuring schema {CATALOG}.{SCHEMA} exists …")

    # 2. Create tables (idempotent)
    for table_name, ddl in BRONZE_DDL.items():
        full_name = f"{CATALOG}.{SCHEMA}.{table_name}"
        run_sql(
            cursor,
            ddl.format(catalog=CATALOG, schema=SCHEMA),
            description=f"Creating {full_name} …",
        )
        # Truncate so the pipeline is idempotent on re-runs
        run_sql(cursor, f"TRUNCATE TABLE {full_name}",
                description=f"Truncating {full_name} …")

    # 3. Load CSV data
    for csv_file, table_name in CSV_TABLE_MAP.items():
        full_name = f"{CATALOG}.{SCHEMA}.{table_name}"
        print(f"\n    Loading {csv_file} → {full_name}")
        if not os.path.exists(csv_file):
            raise FileNotFoundError(
                f"{csv_file} not found. Run generate_data.py first."
            )
        df = pd.read_csv(csv_file, dtype=str)   # ingest everything as strings
        upload_csv_to_table(cursor, df, full_name)

    print("\n  Bronze layer complete.")


# ═════════════════════════════════════════════════════════════════════════════
# SILVER LAYER — Cleansed & Joined
# Goal  : remove nulls, cast to correct data types, and produce a single
#         enriched order-items fact table with customer and product context.
# ═════════════════════════════════════════════════════════════════════════════

SILVER_ORDER_ITEMS_DDL = """
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.silver_order_items (
        item_id       BIGINT,
        order_id      BIGINT,
        customer_id   BIGINT,
        customer_name STRING,
        email         STRING,
        country       STRING,
        signup_date   DATE,
        product_id    BIGINT,
        product_name  STRING,
        category      STRING,
        base_price    DOUBLE,
        order_date    DATE,
        status        STRING,
        quantity      INT,
        unit_price    DOUBLE,
        total_price   DOUBLE
    )
    USING DELTA
    TBLPROPERTIES ('layer' = 'silver')
"""

SILVER_TRANSFORM_SQL = """
    INSERT INTO {catalog}.{schema}.silver_order_items

    SELECT
        oi.item_id                          AS item_id,
        oi.order_id                         AS order_id,
        c.customer_id                       AS customer_id,
        c.name                              AS customer_name,
        LOWER(TRIM(c.email))                AS email,
        TRIM(c.country)                     AS country,
        TO_DATE(c.signup_date, 'yyyy-MM-dd') AS signup_date,
        p.product_id                        AS product_id,
        TRIM(p.name)                        AS product_name,
        TRIM(p.category)                    AS category,
        CAST(p.base_price  AS DOUBLE)       AS base_price,
        TO_DATE(o.order_date, 'yyyy-MM-dd') AS order_date,
        LOWER(TRIM(o.status))               AS status,
        CAST(oi.quantity   AS INT)          AS quantity,
        CAST(oi.unit_price  AS DOUBLE)      AS unit_price,
        CAST(oi.total_price AS DOUBLE)      AS total_price

    FROM       {catalog}.{schema}.bronze_order_items  oi
    INNER JOIN {catalog}.{schema}.bronze_orders       o  ON oi.order_id   = o.order_id
    INNER JOIN {catalog}.{schema}.bronze_customers    c  ON o.customer_id  = c.customer_id
    INNER JOIN {catalog}.{schema}.bronze_products     p  ON oi.product_id  = p.product_id

    -- Drop any row that is missing a critical key or numeric field
    WHERE oi.item_id    IS NOT NULL
      AND oi.order_id   IS NOT NULL
      AND o.customer_id IS NOT NULL
      AND oi.product_id IS NOT NULL
      AND oi.unit_price IS NOT NULL
      AND oi.quantity   IS NOT NULL
      AND CAST(oi.unit_price  AS DOUBLE) > 0
      AND CAST(oi.quantity    AS INT)    > 0
"""


def run_silver(cursor) -> None:
    print("\n" + "═" * 60)
    print("  SILVER — Cleanse & Join")
    print("═" * 60)

    full_name = f"{CATALOG}.{SCHEMA}.silver_order_items"
    fmt = {"catalog": CATALOG, "schema": SCHEMA}

    run_sql(cursor, SILVER_ORDER_ITEMS_DDL.format(**fmt),
            description=f"Creating {full_name} …")
    run_sql(cursor, f"TRUNCATE TABLE {full_name}",
            description=f"Truncating {full_name} …")
    run_sql(cursor, SILVER_TRANSFORM_SQL.format(**fmt),
            description="Joining & cleansing bronze tables → silver …")

    # Row-count sanity check
    cursor.execute(f"SELECT COUNT(*) FROM {full_name}")
    count = cursor.fetchone()[0]
    print(f"\n    silver_order_items row count: {count:,}")

    print("\n    Preview (5 rows):")
    preview(cursor, full_name)

    print("\n  Silver layer complete.")


# ═════════════════════════════════════════════════════════════════════════════
# GOLD LAYER — Aggregated Business Metrics
# Goal  : produce three business-ready summary tables consumed by BI tools.
#
#   gold_revenue_by_category   – total revenue and units sold per product category
#   gold_top_customers         – top customers ranked by lifetime spend
#   gold_monthly_order_trends  – monthly order volume and revenue trend
# ═════════════════════════════════════════════════════════════════════════════

GOLD_TABLES = {
    # 1. Revenue breakdown by product category
    "gold_revenue_by_category": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS {catalog}.{schema}.gold_revenue_by_category (
                category        STRING,
                total_revenue   DOUBLE,
                total_units     BIGINT,
                avg_order_value DOUBLE,
                order_count     BIGINT
            )
            USING DELTA
            TBLPROPERTIES ('layer' = 'gold')
        """,
        "insert": """
            INSERT INTO {catalog}.{schema}.gold_revenue_by_category
            SELECT
                category,
                ROUND(SUM(total_price),  2)                   AS total_revenue,
                SUM(quantity)                                  AS total_units,
                ROUND(AVG(total_price),  2)                   AS avg_order_value,
                COUNT(DISTINCT order_id)                       AS order_count
            FROM   {catalog}.{schema}.silver_order_items
            WHERE  status != 'returned'         -- exclude returned orders from revenue
            GROUP  BY category
            ORDER  BY total_revenue DESC
        """,
        "description": "Aggregating revenue by product category …",
    },

    # 2. Top customers by lifetime spend (all time, excluding returns)
    "gold_top_customers": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS {catalog}.{schema}.gold_top_customers (
                customer_id     BIGINT,
                customer_name   STRING,
                email           STRING,
                country         STRING,
                total_spend     DOUBLE,
                order_count     BIGINT,
                avg_order_value DOUBLE,
                first_order     DATE,
                last_order      DATE
            )
            USING DELTA
            TBLPROPERTIES ('layer' = 'gold')
        """,
        "insert": """
            INSERT INTO {catalog}.{schema}.gold_top_customers
            SELECT
                customer_id,
                customer_name,
                email,
                country,
                ROUND(SUM(total_price),          2) AS total_spend,
                COUNT(DISTINCT order_id)            AS order_count,
                ROUND(AVG(total_price),          2) AS avg_order_value,
                MIN(order_date)                     AS first_order,
                MAX(order_date)                     AS last_order
            FROM   {catalog}.{schema}.silver_order_items
            WHERE  status != 'returned'
            GROUP  BY customer_id, customer_name, email, country
            ORDER  BY total_spend DESC
            LIMIT  100
        """,
        "description": "Computing top customers by lifetime spend …",
    },

    # 3. Monthly order volume and revenue trend
    "gold_monthly_order_trends": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS {catalog}.{schema}.gold_monthly_order_trends (
                year            INT,
                month           INT,
                month_label     STRING,
                total_revenue   DOUBLE,
                order_count     BIGINT,
                item_count      BIGINT,
                avg_order_value DOUBLE
            )
            USING DELTA
            TBLPROPERTIES ('layer' = 'gold')
        """,
        "insert": """
            INSERT INTO {catalog}.{schema}.gold_monthly_order_trends
            SELECT
                YEAR(order_date)                             AS year,
                MONTH(order_date)                            AS month,
                DATE_FORMAT(order_date, 'MMM yyyy')          AS month_label,
                ROUND(SUM(total_price),              2)      AS total_revenue,
                COUNT(DISTINCT order_id)                     AS order_count,
                SUM(quantity)                                AS item_count,
                ROUND(SUM(total_price)
                      / NULLIF(COUNT(DISTINCT order_id), 0), 2) AS avg_order_value
            FROM   {catalog}.{schema}.silver_order_items
            WHERE  status != 'returned'
            GROUP  BY year, month, month_label
            ORDER  BY year, month
        """,
        "description": "Computing monthly order trends …",
    },

    # 4. Return analysis by product category
    "gold_return_analysis": {
        "ddl": """
            CREATE TABLE IF NOT EXISTS {catalog}.{schema}.gold_return_analysis (
                category             STRING,
                total_orders         BIGINT,
                total_returns        BIGINT,
                return_rate          DOUBLE,
                total_revenue_lost   DOUBLE
            )
            USING DELTA
            TBLPROPERTIES ('layer' = 'gold')
        """,
        "insert": """
            INSERT INTO {catalog}.{schema}.gold_return_analysis
            SELECT
                category,
                COUNT(DISTINCT order_id)                                          AS total_orders,
                COUNT(DISTINCT CASE WHEN status = 'returned' THEN order_id END)   AS total_returns,
                ROUND(
                    COUNT(DISTINCT CASE WHEN status = 'returned' THEN order_id END)
                    * 100.0
                    / NULLIF(COUNT(DISTINCT order_id), 0),
                    2
                )                                                                 AS return_rate,
                ROUND(
                    SUM(CASE WHEN status = 'returned' THEN total_price ELSE 0 END),
                    2
                )                                                                 AS total_revenue_lost
            FROM   {catalog}.{schema}.silver_order_items
            GROUP  BY category
            ORDER  BY total_revenue_lost DESC
        """,
        "description": "Computing return analysis by product category …",
    },
}


def run_gold(cursor) -> None:
    print("\n" + "═" * 60)
    print("  GOLD — Business Aggregations")
    print("═" * 60)

    fmt = {"catalog": CATALOG, "schema": SCHEMA}

    for table_name, spec in GOLD_TABLES.items():
        full_name = f"{CATALOG}.{SCHEMA}.{table_name}"

        run_sql(cursor, spec["ddl"].format(**fmt),
                description=f"Creating {full_name} …")
        run_sql(cursor, f"TRUNCATE TABLE {full_name}",
                description=f"Truncating {full_name} …")
        run_sql(cursor, spec["insert"].format(**fmt),
                description=f"    {spec['description']}")

        cursor.execute(f"SELECT COUNT(*) FROM {full_name}")
        count = cursor.fetchone()[0]
        print(f"      → {count:,} rows")

        print(f"\n    Preview of {table_name} (5 rows):")
        preview(cursor, full_name)
        print()

    print("  Gold layer complete.")


# ═════════════════════════════════════════════════════════════════════════════
# Entry point
# ═════════════════════════════════════════════════════════════════════════════

_SKIP_CATALOGS = {"samples", "system", "__databricks_internal"}


def detect_catalog(cursor) -> str:
    """
    Return a writable catalog by probing each candidate with a CREATE SCHEMA
    (then dropping it if created).  Prefers 'main', then 'hive_metastore',
    then any other non-system catalog.
    Raises RuntimeError if no writable catalog can be found.
    """
    from databricks.sql.exc import ServerOperationError

    cursor.execute("SHOW CATALOGS")
    all_catalogs = [row[0] for row in cursor.fetchall()]

    # Build probe order: preferred names first, then everything else
    preferred = [c for c in ("main", "hive_metastore") if c in all_catalogs]
    rest = [c for c in all_catalogs if c not in preferred and c not in _SKIP_CATALOGS]
    probe_order = preferred + rest

    test_schema = f"{SCHEMA}__probe__"
    for catalog in probe_order:
        try:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{test_schema}")
            cursor.execute(f"DROP SCHEMA IF EXISTS {catalog}.{test_schema}")
            return catalog
        except ServerOperationError:
            continue

    raise RuntimeError(
        f"No writable catalog found. Available: {all_catalogs}. "
        "Set CATALOG manually in pipeline.py or add DATABRICKS_CATALOG to .env."
    )


def main() -> None:
    global CATALOG
    print("\nConnecting to Databricks …")
    with get_connection() as conn:
        with conn.cursor() as cursor:
            CATALOG = detect_catalog(cursor)
            print(f"  Using catalog: {CATALOG}")
            run_bronze(cursor)
            run_silver(cursor)
            run_gold(cursor)

    print("\n" + "═" * 60)
    print("  Pipeline finished successfully.")
    print("═" * 60 + "\n")


if __name__ == "__main__":
    main()
