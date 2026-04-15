"""
ask_data.py
Natural language data assistant for the ecommerce pipeline.

Ask a question in plain English — Claude translates it to SQL, runs it against
Databricks, then narrates the result in 3 sentences.

Usage
─────
    python ask_data.py

Type "exit" to quit.

Requires
────────
    ANTHROPIC_API_KEY, DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN
    in .env (or already set in the environment).
"""

import os
from dotenv import load_dotenv
import anthropic
from databricks import sql as dbsql
from tabulate import tabulate

load_dotenv()

# ── Anthropic client ──────────────────────────────────────────────────────────
client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
MODEL  = "claude-sonnet-4-6"

# ── Databricks connection params ──────────────────────────────────────────────
DB_HOST      = os.environ["DATABRICKS_HOST"].replace("https://", "")
DB_HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
DB_TOKEN     = os.environ["DATABRICKS_TOKEN"]

# ── Stable system prompt — good prompt-cache candidate ───────────────────────
SYSTEM_PROMPT = """\
You are a SQL expert for a Databricks SQL Warehouse.
The catalog is `workspace` and the schema is `ecommerce`.
Always qualify table names as `workspace.ecommerce.<table_name>`.

Available tables:

bronze_customers
  customer_id, name, email, country, signup_date

bronze_products
  product_id, name, category, price

bronze_orders
  order_id, customer_id, order_date, status

bronze_order_items
  item_id, order_id, product_id, quantity, unit_price, total_price

silver_order_items
  item_id, order_id, order_date, status,
  customer_id, name AS customer_name, email, country, signup_date,
  product_id, name AS product_name, category, unit_price, quantity, total_price

gold_revenue_by_category
  category, total_revenue, total_units, avg_order_value, order_count

gold_top_customers
  customer_id, customer_name, email, country,
  total_spend, order_count, avg_order_value, first_order, last_order

gold_monthly_order_trends
  year, month, month_label, total_revenue, order_count, item_count, avg_order_value

gold_return_analysis
  category, total_orders, total_returns, return_rate, total_revenue_lost

silver_customers_enriched
  customer_id, name, email, country, signup_date, tenure_segment

gold_customer_segments
  customer_id, name, country, tenure_segment,
  days_since_signup, total_spend, total_orders, avg_order_value

pipeline_runs
  run_id, run_timestamp, status, layer_reached,
  failed_checks, rows_bronze_customers, rows_bronze_products,
  rows_bronze_orders, rows_bronze_order_items, duration_seconds

bronze_orders_stream
  event_id, customer_id, product_id, quantity,
  unit_price, total_price, order_timestamp, status

gold_stream_anomalies
  event_id, customer_id, product_id, quantity, unit_price, total_price,
  deviation, anomaly_threshold, status, processed_at

Respond with ONLY a valid SQL query — no explanation, no markdown, no comments.\
"""


def get_sql(question: str) -> str:
    """Ask Claude to translate a natural-language question into SQL."""
    response = client.messages.create(
        model=MODEL,
        max_tokens=512,
        system=[
            {
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": question}],
    )
    return response.content[0].text.strip()


def run_sql(query: str) -> tuple[list[dict], list[str]]:
    """Execute SQL on Databricks and return (rows_as_dicts, column_names)."""
    with dbsql.connect(
        server_hostname=DB_HOST,
        http_path=DB_HTTP_PATH,
        access_token=DB_TOKEN,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return rows, columns


def get_narrative(question: str, sql: str, rows: list[dict], columns: list[str]) -> str:
    """Ask Claude to narrate the query results in 3 sentences."""
    results_text = tabulate(rows, headers="keys", tablefmt="simple", floatfmt=".2f")

    user_content = (
        f"Original question: {question}\n\n"
        f"SQL used:\n{sql}\n\n"
        f"Results:\n{results_text}\n\n"
        "Write a 3-sentence plain-English narrative answering the question based on these results. "
        "Be specific — include numbers from the data."
    )

    response = client.messages.create(
        model=MODEL,
        max_tokens=300,
        system=[
            {
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": user_content}],
    )
    return response.content[0].text.strip()


def main() -> None:
    print("─" * 60)
    print("  Ecommerce Data Assistant  (type 'exit' to quit)")
    print("─" * 60)

    while True:
        print()
        question = input("Question: ").strip()
        if not question:
            continue
        if question.lower() == "exit":
            print("Goodbye.")
            break

        # ── Step 1: generate SQL ──────────────────────────────────────────
        print("\nGenerating SQL...")
        try:
            sql = get_sql(question)
        except Exception as e:
            print(f"[Claude error] {e}")
            continue

        print(f"\nSQL:\n{sql}")

        # ── Step 2: run against Databricks ───────────────────────────────
        print("\nRunning query...")
        try:
            rows, columns = run_sql(sql)
        except Exception as e:
            print(f"[Databricks error] {e}")
            continue

        if not rows:
            print("\nResults: (no rows returned)")
            continue

        print(f"\nResults ({len(rows)} row{'s' if len(rows) != 1 else ''}):")
        print(tabulate(rows, headers="keys", tablefmt="simple", floatfmt=".2f"))

        # ── Step 3: narrate ───────────────────────────────────────────────
        print("\nGenerating narrative...")
        try:
            narrative = get_narrative(question, sql, rows, columns)
        except Exception as e:
            print(f"[Claude error] {e}")
            continue

        print(f"\nAnswer:\n{narrative}")
        print("\n" + "─" * 60)


if __name__ == "__main__":
    main()
