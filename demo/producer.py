"""Demo producer — generates a stream of inserts, updates, and deletes into a DuckLake table.

Requires a Postgres metadata backend (see docker-compose.yaml).

Usage:
    just demo-up          # start Postgres
    just demo-producer    # run this script
"""

from __future__ import annotations

import os
import random
import signal
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from pyducklake import Catalog
from pyducklake.schema import Schema, optional, required
from pyducklake.types import DoubleType, IntegerType, StringType, TimestampTZType

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PG_CONN = os.environ.get(
    "DUCKLAKE_META",
    "postgres:dbname=ducklake host=localhost port=5433 user=ducklake password=ducklake",
)
DATA_PATH = os.environ.get("DUCKLAKE_DATA_PATH", str(Path(__file__).resolve().parent / "data"))
CATALOG_NAME = "demo"
TABLE_NAME = "events"
FLUSH_INTERVAL = 3.0  # seconds between batches
BATCH_SIZE = 5  # events per batch

# ---------------------------------------------------------------------------
# Fake data
# ---------------------------------------------------------------------------

USERS = [f"user_{i:04d}" for i in range(50)]
EVENT_TYPES = ["pageview", "click", "purchase", "signup", "logout", "error"]
PAGES = ["/home", "/pricing", "/docs", "/blog", "/settings", "/dashboard", "/api", "/signup"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]
STATUSES = ["ok", "warning", "error"]

shutdown = False


def _handle_signal(sig: int, frame: object) -> None:
    global shutdown  # noqa: PLW0603
    shutdown = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def generate_event(seq: int) -> dict[str, object]:
    return {
        "id": seq,
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(EVENT_TYPES),
        "user_id": random.choice(USERS),
        "page": random.choice(PAGES),
        "browser": random.choice(BROWSERS),
        "status": random.choice(STATUSES),
        "amount": round(random.uniform(0, 500), 2) if random.random() < 0.3 else 0.0,
        "ts": datetime.now(UTC),
    }


def events_to_arrow(events: list[dict[str, object]]) -> pa.Table:
    return pa.table(
        {
            "id": pa.array([e["id"] for e in events], type=pa.int32()),
            "event_id": pa.array([e["event_id"] for e in events], type=pa.string()),
            "event_type": pa.array([e["event_type"] for e in events], type=pa.string()),
            "user_id": pa.array([e["user_id"] for e in events], type=pa.string()),
            "page": pa.array([e["page"] for e in events], type=pa.string()),
            "browser": pa.array([e["browser"] for e in events], type=pa.string()),
            "status": pa.array([e["status"] for e in events], type=pa.string()),
            "amount": pa.array([e["amount"] for e in events], type=pa.float64()),
            "ts": pa.array([e["ts"] for e in events], type=pa.timestamp("us", tz="UTC")),
        }
    )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def main() -> None:
    Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

    print(f"Connecting to DuckLake  meta={PG_CONN}  data={DATA_PATH}")
    catalog = Catalog(CATALOG_NAME, PG_CONN, data_path=DATA_PATH)

    schema = Schema.of(
        required("id", IntegerType()),
        required("event_id", StringType()),
        required("event_type", StringType()),
        required("user_id", StringType()),
        optional("page", StringType()),
        optional("browser", StringType()),
        optional("status", StringType()),
        optional("amount", DoubleType()),
        optional("ts", TimestampTZType()),
    )

    if catalog.table_exists(TABLE_NAME):
        table = catalog.load_table(TABLE_NAME)
        print(f"Loaded existing table {TABLE_NAME}")
    else:
        table = catalog.create_table(TABLE_NAME, schema)
        print(f"Created table {TABLE_NAME}")

    seq = 0
    total = 0

    while not shutdown:
        # --- Inserts (every batch) ---
        events = [generate_event(seq + i) for i in range(BATCH_SIZE)]
        seq += BATCH_SIZE
        table.append(events_to_arrow(events))
        total += len(events)
        print(f"[{datetime.now(UTC).strftime('%H:%M:%S')}] +{len(events)} inserts  (total={total})")

        time.sleep(FLUSH_INTERVAL)
        if shutdown:
            break

        # --- Delete (every other batch, ~30% chance) ---
        if random.random() < 0.3:
            delete_id = random.randint(0, max(seq - 1, 0))
            try:
                table.delete(f"id = {delete_id}")
                print(f"[{datetime.now(UTC).strftime('%H:%M:%S')}] -1 delete   id={delete_id}")
            except Exception:
                pass  # row may already be deleted

        time.sleep(FLUSH_INTERVAL)
        if shutdown:
            break

        # --- Update (every other batch, ~40% chance) ---
        if random.random() < 0.4:
            update_id = random.randint(0, max(seq - 1, 0))
            new_status = random.choice(STATUSES)
            new_amount = round(random.uniform(0, 999), 2)
            try:
                catalog.connection.execute(
                    f"UPDATE {CATALOG_NAME}.main.{TABLE_NAME} "
                    f"SET status = '{new_status}', amount = {new_amount} "
                    f"WHERE id = {update_id}"
                )
                print(
                    f"[{datetime.now(UTC).strftime('%H:%M:%S')}] "
                    f"Δ1 update   id={update_id} status={new_status} amount={new_amount}"
                )
            except Exception:
                pass  # row may not exist

        time.sleep(FLUSH_INTERVAL)

    print(f"\nShutdown. Produced {total} events total.")


if __name__ == "__main__":
    main()
