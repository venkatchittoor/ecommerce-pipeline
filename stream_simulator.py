"""
stream_simulator.py
Simulates a live e-commerce event stream by writing one JSON order event
every 3 seconds to the streaming_data/ folder.

Usage
─────
    python stream_simulator.py

Press Ctrl+C to stop.
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

OUTPUT_DIR = "streaming_data"

# Realistic unit prices keyed by product_id (1-50)
# Generated once at startup so prices are consistent across events.
random.seed(42)
PRODUCT_PRICES = {pid: round(random.uniform(5.0, 999.0), 2) for pid in range(1, 51)}
random.seed()   # re-seed with system entropy for the actual simulation


def generate_event() -> dict:
    product_id = random.randint(1, 50)
    quantity   = random.randint(1, 10)
    unit_price = PRODUCT_PRICES[product_id]
    total_price = round(unit_price * quantity, 2)
    status      = "returned" if random.random() < 0.10 else "completed"

    return {
        "event_id":        str(uuid.uuid4()),
        "customer_id":     random.randint(1, 200),
        "product_id":      product_id,
        "quantity":        quantity,
        "unit_price":      unit_price,
        "total_price":     total_price,
        "order_timestamp": datetime.now(timezone.utc).isoformat(),
        "status":          status,
    }


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Stream simulator started — writing events to {OUTPUT_DIR}/")
    print("Press Ctrl+C to stop.\n")

    event_count = 0
    try:
        while True:
            event      = generate_event()
            filename   = f"{OUTPUT_DIR}/event_{event['event_id']}.json"
            with open(filename, "w") as f:
                json.dump(event, f, indent=2)

            event_count += 1
            ts = event["order_timestamp"]
            print(
                f"[{event_count:>4}] {ts}  "
                f"customer={event['customer_id']:>3}  "
                f"product={event['product_id']:>2}  "
                f"qty={event['quantity']}  "
                f"total=${event['total_price']:>8.2f}  "
                f"status={event['status']}"
            )
            time.sleep(3)

    except KeyboardInterrupt:
        print(f"\nStopped after {event_count} event(s). Files are in {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
