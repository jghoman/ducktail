"""Ducktail quickstart — demonstrates CDC tailing of a DuckLake table.

Run with:
    uv run python examples/quickstart.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pyarrow as pa
from pyducklake import Catalog
from pyducklake.schema import Schema, optional, required
from pyducklake.types import DoubleType, IntegerType, StringType

from ducktail.formatter import format_changeset
from ducktail.tailer import Tailer


def main() -> None:
    # --- Setup: create a temp catalog and table ---
    tmpdir = tempfile.mkdtemp(prefix="ducktail_qs_")
    meta_path = str(Path(tmpdir) / "meta.duckdb")

    catalog = Catalog("lake", meta_path, data_path=tmpdir)

    schema = Schema.of(
        required("id", IntegerType()),
        optional("name", StringType()),
        optional("amount", DoubleType()),
    )
    table = catalog.create_table("orders", schema)

    # --- Initial data (establishes the first snapshot) ---
    table.append(
        pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "amount": [100.0, 50.0, 75.0],
            }
        )
    )

    # --- Create tailer and establish baseline ---
    tailer = Tailer(table=table)
    tailer.poll()  # baseline — records current snapshot, returns None
    print(f"Baseline snapshot: {tailer.last_snapshot_id}")
    print()

    # --- Insert new rows ---
    print("=== Inserting rows ===")
    table.append(
        pa.table(
            {
                "id": [4, 5],
                "name": ["Dave", "Eve"],
                "amount": [200.0, 125.0],
            }
        )
    )

    changeset = tailer.poll()
    if changeset is not None:
        for line in format_changeset(changeset):
            print(line)
    print()

    # --- Delete a row ---
    print("=== Deleting id=2 ===")
    table.delete("id = 2")

    changeset = tailer.poll()
    if changeset is not None:
        for line in format_changeset(changeset):
            print(line)
    print()

    # --- Update a row ---
    print("=== Updating id=1 amount 100 -> 150 ===")
    table.upsert(
        pa.table(
            {
                "id": [1],
                "name": ["Alice"],
                "amount": [150.0],
            }
        ),
        join_cols=["id"],
    )

    changeset = tailer.poll()
    if changeset is not None:
        for line in format_changeset(changeset):
            print(line)
    print()

    print(f"Final snapshot: {tailer.last_snapshot_id}")
    print(f"Temp dir: {tmpdir}")


if __name__ == "__main__":
    main()
