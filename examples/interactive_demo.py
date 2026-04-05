"""Ducktail interactive demo — live TUI with simulated background writes.

Run with:
    uv run python examples/interactive_demo.py

A background thread inserts and deletes random rows every 2 seconds while
the main thread runs the interactive Rich TUI.  Press Ctrl+C to stop.
"""

from __future__ import annotations

import random
import tempfile
import threading
import time
from pathlib import Path

import pyarrow as pa
from pyducklake import Catalog
from pyducklake.schema import Schema, optional, required
from pyducklake.types import DoubleType, IntegerType, StringType

from ducktail.config import TailConfig
from ducktail.interactive import run_interactive
from ducktail.tailer import Tailer

NAMES = ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Heidi"]


def background_writer(catalog_name: str, meta_path: str, data_path: str) -> None:
    """Simulate external writes to the orders table."""
    catalog = Catalog(catalog_name, meta_path, data_path=data_path)
    table = catalog.load_table("orders")
    next_id = 100

    while True:
        time.sleep(2)
        try:
            # Insert a random row
            table.append(
                pa.table(
                    {
                        "id": [next_id],
                        "name": [random.choice(NAMES)],
                        "amount": [round(random.uniform(10.0, 500.0), 2)],
                    }
                )
            )
            next_id += 1

            # Occasionally delete a row
            if random.random() < 0.3 and next_id > 102:
                target_id = random.randint(100, next_id - 2)
                table.delete(f"id = {target_id}")
        except Exception:
            # Table may have concurrent access issues in demo; keep going.
            pass


def main() -> None:
    tmpdir = tempfile.mkdtemp(prefix="ducktail_demo_")
    meta_path = str(Path(tmpdir) / "meta.duckdb")

    catalog = Catalog("lake", meta_path, data_path=tmpdir)

    schema = Schema.of(
        required("id", IntegerType()),
        optional("name", StringType()),
        optional("amount", DoubleType()),
    )
    table = catalog.create_table("orders", schema)

    # Seed with a few rows
    table.append(
        pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "amount": [100.0, 50.0, 75.0],
            }
        )
    )

    # Start background writer thread
    writer = threading.Thread(
        target=background_writer,
        args=("lake", meta_path, tmpdir),
        daemon=True,
    )
    writer.start()

    # Build tailer and config, then run the interactive TUI
    tailer = Tailer(table=table, poll_interval=1.0)

    config = TailConfig(
        catalog_connection=meta_path,
        data_path=tmpdir,
        catalog_name="lake",
        namespace="main",
        table_name="orders",
        poll_interval=1.0,
        output_mode="interactive",
    )

    run_interactive(tailer, config)


if __name__ == "__main__":
    main()
