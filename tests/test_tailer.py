from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pyarrow as pa
from pyducklake.cdc import ChangeSet
from pyducklake.snapshot import Snapshot

from ducktail.tailer import Tailer


def _snapshot(sid: int) -> Snapshot:
    return Snapshot(snapshot_id=sid, timestamp=datetime(2026, 1, 1, 0, 0, sid))


def _changeset() -> ChangeSet:
    return ChangeSet(
        pa.table({"id": [1, 2], "change_type": ["insert", "insert"]}),
        change_type_col="change_type",
    )


def _mock_table(snapshot: Snapshot | None = None) -> MagicMock:
    table = MagicMock()
    table.current_snapshot.return_value = snapshot
    table.table_changes.return_value = _changeset()
    return table


class TestPoll:
    def test_first_poll_returns_none(self):
        table = _mock_table(_snapshot(1))
        tailer = Tailer(table)
        assert tailer.poll() is None

    def test_same_snapshot_returns_none(self):
        table = _mock_table(_snapshot(1))
        tailer = Tailer(table)
        tailer.poll()  # baseline
        assert tailer.poll() is None

    def test_new_snapshot_returns_changeset(self):
        table = _mock_table(_snapshot(1))
        tailer = Tailer(table)
        tailer.poll()  # baseline

        table.current_snapshot.return_value = _snapshot(2)
        result = tailer.poll()
        assert result is not None
        table.table_changes.assert_called_once_with(1, 2, columns=None, filter_expr=None)

    def test_empty_table_returns_none(self):
        table = _mock_table(None)
        tailer = Tailer(table)
        assert tailer.poll() is None

    def test_transition_from_no_snapshot_to_snapshot(self):
        table = _mock_table(None)
        tailer = Tailer(table)
        tailer.poll()  # baseline with no snapshot

        table.current_snapshot.return_value = _snapshot(1)
        result = tailer.poll()
        # First real snapshot after empty — records it, returns None
        assert result is None
        assert tailer.last_snapshot_id == 1

    def test_columns_passed_through(self):
        table = _mock_table(_snapshot(1))
        tailer = Tailer(table, columns=("a", "b"))
        tailer.poll()

        table.current_snapshot.return_value = _snapshot(2)
        tailer.poll()
        table.table_changes.assert_called_once_with(1, 2, columns=("a", "b"), filter_expr=None)

    def test_filter_expr_passed_through(self):
        table = _mock_table(_snapshot(1))
        tailer = Tailer(table, filter_expr="id > 5")
        tailer.poll()

        table.current_snapshot.return_value = _snapshot(2)
        tailer.poll()
        table.table_changes.assert_called_once_with(1, 2, columns=None, filter_expr="id > 5")

    def test_columns_and_filter_expr_passed_through(self):
        table = _mock_table(_snapshot(1))
        tailer = Tailer(table, columns=("x",), filter_expr="x > 0")
        tailer.poll()

        table.current_snapshot.return_value = _snapshot(2)
        tailer.poll()
        table.table_changes.assert_called_once_with(1, 2, columns=("x",), filter_expr="x > 0")


class TestLastSnapshotId:
    def test_none_before_first_poll(self):
        table = _mock_table(_snapshot(1))
        tailer = Tailer(table)
        assert tailer.last_snapshot_id is None

    def test_set_after_first_poll(self):
        table = _mock_table(_snapshot(1))
        tailer = Tailer(table)
        tailer.poll()
        assert tailer.last_snapshot_id == 1

    def test_advances_after_change(self):
        table = _mock_table(_snapshot(1))
        tailer = Tailer(table)
        tailer.poll()

        table.current_snapshot.return_value = _snapshot(3)
        tailer.poll()
        assert tailer.last_snapshot_id == 3

    def test_none_for_empty_table(self):
        table = _mock_table(None)
        tailer = Tailer(table)
        tailer.poll()
        assert tailer.last_snapshot_id is None
