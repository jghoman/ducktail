from __future__ import annotations

import pathlib

import pyarrow as pa
import pytest
from pyducklake import Catalog
from pyducklake.schema import Schema, optional, required
from pyducklake.table import Table
from pyducklake.types import IntegerType, StringType

from ducktail.formatter import format_changeset, iter_change_events
from ducktail.tailer import Tailer

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def lake(tmp_path: pathlib.Path) -> Catalog:
    meta_path = str(tmp_path / "meta.duckdb")
    data_path = str(tmp_path / "data")
    catalog = Catalog("lake", meta_path, data_path=data_path)
    yield catalog  # type: ignore[misc]
    catalog.close()


@pytest.fixture()
def test_table(lake: Catalog) -> tuple[Catalog, Table]:
    schema = Schema.of(
        required("id", IntegerType()),
        optional("name", StringType()),
    )
    table = lake.create_table(("main", "test_table"), schema)
    return lake, table


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_tailer_detects_inserts(test_table: tuple[Catalog, Table]) -> None:
    """Insert rows and verify tailer.poll() returns a ChangeSet with inserts."""
    _catalog, table = test_table

    tailer = Tailer(table)
    # First poll establishes baseline snapshot (returns None).
    assert tailer.poll() is None

    table.append(pa.table({"id": [1, 2], "name": ["alice", "bob"]}))

    changeset = tailer.poll()
    assert changeset is not None

    inserts = changeset.inserts()
    assert inserts.num_rows == 2

    ids = sorted(inserts.column("id").to_pylist())
    assert ids == [1, 2]


def test_tailer_detects_deletes(test_table: tuple[Catalog, Table]) -> None:
    """Insert then delete a row; verify the tailer sees the delete."""
    _catalog, table = test_table

    table.append(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}))

    tailer = Tailer(table)
    assert tailer.poll() is None  # baseline

    table.delete("id = 2")

    changeset = tailer.poll()
    assert changeset is not None

    deletes = changeset.deletes()
    assert deletes.num_rows == 1
    assert deletes.column("id")[0].as_py() == 2


def test_tailer_detects_updates(test_table: tuple[Catalog, Table]) -> None:
    """Insert rows, update via SQL, verify the tailer picks up the change."""
    catalog, table = test_table

    table.append(pa.table({"id": [1, 2], "name": ["alice", "bob"]}))

    tailer = Tailer(table)
    assert tailer.poll() is None  # baseline

    catalog.connection.execute("UPDATE lake.main.test_table SET name = 'alicia' WHERE id = 1")

    changeset = tailer.poll()
    assert changeset is not None
    assert changeset.num_rows > 0

    # The changeset should contain update pre/post images or a delete+insert pair.
    # Either way, 'alicia' must appear somewhere in the result.
    arrow = changeset.to_arrow()
    names_in_changeset = arrow.column("name").to_pylist()
    assert "alicia" in names_in_changeset


def test_formatter_renders_changes(test_table: tuple[Catalog, Table]) -> None:
    """Insert data, get CDC changes, format them, and verify '+' prefixed lines."""
    _catalog, table = test_table

    table.append(pa.table({"id": [10, 20], "name": ["x", "y"]}))

    tailer = Tailer(table)
    assert tailer.poll() is None  # baseline

    table.append(pa.table({"id": [30], "name": ["z"]}))

    changeset = tailer.poll()
    assert changeset is not None

    lines = format_changeset(changeset)
    assert len(lines) == 1
    for line in lines:
        assert line.startswith("+ ")
    # Verify actual content appears (values are repr-formatted)
    combined = " ".join(lines)
    assert "id=30" in combined
    assert "name='z'" in combined


def test_tailer_column_projection(test_table: tuple[Catalog, Table]) -> None:
    """Verify columns parameter filters output to only requested columns."""
    _catalog, table = test_table

    table.append(pa.table({"id": [1], "name": ["alice"]}))

    tailer = Tailer(table, columns=("name",))
    assert tailer.poll() is None  # baseline

    table.append(pa.table({"id": [2], "name": ["bob"]}))

    changeset = tailer.poll()
    assert changeset is not None

    arrow = changeset.to_arrow()
    user_cols = [c for c in arrow.column_names if c not in {"snapshot_id", "rowid", "change_type"}]
    assert user_cols == ["name"]


def test_tailer_no_changes(test_table: tuple[Catalog, Table]) -> None:
    """Verify poll() returns None when nothing has changed since baseline."""
    _catalog, table = test_table

    table.append(pa.table({"id": [1], "name": ["alice"]}))

    tailer = Tailer(table)
    assert tailer.poll() is None  # baseline
    assert tailer.poll() is None  # no changes


def test_tailer_no_redelivery(test_table: tuple[Catalog, Table]) -> None:
    """Regression: the baseline snapshot's changes must not be delivered.

    ducklake_table_changes treats its start bound as inclusive, so polling
    (last, current) would redeliver the last processed snapshot's changes on
    every advance — including the entire pre-baseline history on the first
    delivery. The tailer must query (last + 1, current).
    """
    _catalog, table = test_table

    table.append(pa.table({"id": [1], "name": ["alice"]}))

    tailer = Tailer(table)
    assert tailer.poll() is None  # baseline AFTER the first append

    table.append(pa.table({"id": [2], "name": ["bob"]}))
    changeset = tailer.poll()
    assert changeset is not None
    # Exactly the new row — no phantom re-delivery of id=1 from the baseline
    assert changeset.num_rows == 1
    inserts = changeset.inserts()
    assert inserts.num_rows == 1
    assert inserts.column("id")[0].as_py() == 2

    table.append(pa.table({"id": [3], "name": ["carol"]}))
    changeset = tailer.poll()
    assert changeset is not None
    # id=2 must not be redelivered either
    assert changeset.num_rows == 1
    assert changeset.inserts().column("id")[0].as_py() == 3


def test_tailer_double_update_renders_both_deltas(test_table: tuple[Catalog, Table]) -> None:
    """A row updated twice in one poll window renders two deltas (A→B, B→C)."""
    catalog, table = test_table

    table.append(pa.table({"id": [1], "name": ["a"]}))

    tailer = Tailer(table)
    assert tailer.poll() is None  # baseline

    catalog.connection.execute("UPDATE lake.main.test_table SET name = 'b' WHERE id = 1")
    catalog.connection.execute("UPDATE lake.main.test_table SET name = 'c' WHERE id = 1")

    changeset = tailer.poll()
    assert changeset is not None
    events = list(iter_change_events(changeset))
    assert events == [
        ("update", "name: 'a' → 'b'"),
        ("update", "name: 'b' → 'c'"),
    ]
