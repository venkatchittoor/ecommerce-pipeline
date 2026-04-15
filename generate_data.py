"""
generate_data.py
Generates realistic sample CSV data for the e-commerce orders pipeline.
"""

import csv
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

# ── Config ──────────────────────────────────────────────────────────────────
NUM_CUSTOMERS   = 200
NUM_PRODUCTS    = 50
NUM_ORDERS      = 1000
NUM_ORDER_ITEMS = 2000
ORDER_DAYS_BACK = 180

CATEGORIES = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Sports", "Beauty", "Toys"]
ORDER_STATUSES = ["completed", "returned", "pending"]
STATUS_WEIGHTS  = [0.75, 0.15, 0.10]   # realistic distribution

PRICE_RANGES = {
    "Electronics":    (49.99,  999.99),
    "Clothing":       (9.99,   149.99),
    "Home & Kitchen": (14.99,  299.99),
    "Books":          (4.99,    49.99),
    "Sports":         (19.99,  249.99),
    "Beauty":         (7.99,    89.99),
    "Toys":           (9.99,   119.99),
}


# ── Generators ───────────────────────────────────────────────────────────────

def generate_customers(n: int) -> list[dict]:
    seen_emails: set[str] = set()
    rows = []
    for i in range(1, n + 1):
        email = fake.unique.email()
        seen_emails.add(email)
        rows.append({
            "customer_id":  i,
            "name":         fake.name(),
            "email":        email,
            "country":      fake.country(),
            "signup_date":  fake.date_between(start_date="-3y", end_date="today").isoformat(),
        })
    return rows


def generate_products(n: int) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        category = random.choice(CATEGORIES)
        lo, hi = PRICE_RANGES[category]
        rows.append({
            "product_id":   i,
            "name":         fake.catch_phrase().title(),
            "category":     category,
            "base_price":   round(random.uniform(lo, hi), 2),
        })
    return rows


def generate_orders(n: int, customer_ids: list[int]) -> list[dict]:
    today = datetime.today().date()
    cutoff = today - timedelta(days=ORDER_DAYS_BACK)
    rows = []
    for i in range(1, n + 1):
        order_date = fake.date_between(start_date=cutoff, end_date=today)
        rows.append({
            "order_id":    i,
            "customer_id": random.choice(customer_ids),
            "order_date":  order_date.isoformat(),
            "status":      random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS, k=1)[0],
        })
    return rows


def generate_order_items(
    n: int,
    order_ids: list[int],
    products: list[dict],
) -> list[dict]:
    product_map = {p["product_id"]: p["base_price"] for p in products}
    product_ids = list(product_map.keys())
    rows = []
    for i in range(1, n + 1):
        product_id = random.choice(product_ids)
        quantity   = random.randint(1, 5)
        # apply a small random price variation (±10 %) to simulate discounts/surcharges
        unit_price = round(product_map[product_id] * random.uniform(0.90, 1.10), 2)
        rows.append({
            "item_id":     i,
            "order_id":    random.choice(order_ids),
            "product_id":  product_id,
            "quantity":    quantity,
            "unit_price":  unit_price,
            "total_price": round(unit_price * quantity, 2),
        })
    return rows


# ── CSV writer ───────────────────────────────────────────────────────────────

def write_csv(filename: str, rows: list[dict]) -> None:
    if not rows:
        return
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  wrote {len(rows):>5,} rows → {filename}")


# ── Main ─────────────────────────────────────────────────────────────────────

def generate() -> None:
    """Generate all four CSV files. Safe to import and call programmatically."""
    print("Generating sample data …\n")

    customers    = generate_customers(NUM_CUSTOMERS)
    products     = generate_products(NUM_PRODUCTS)
    customer_ids = [c["customer_id"] for c in customers]
    orders       = generate_orders(NUM_ORDERS, customer_ids)
    order_ids    = [o["order_id"] for o in orders]
    order_items  = generate_order_items(NUM_ORDER_ITEMS, order_ids, products)

    write_csv("customers.csv",   customers)
    write_csv("products.csv",    products)
    write_csv("orders.csv",      orders)
    write_csv("order_items.csv", order_items)

    print("\nDone. All CSV files are ready.")


def main() -> None:
    generate()


if __name__ == "__main__":
    main()
