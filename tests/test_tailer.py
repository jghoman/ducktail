from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from pyducklake.cdc import ChangeSet
from pyducklake.snapshot import Snapshot

from ducktail.tailer import Tailer


def _snapshot(sid: int) -> Snapshot:
    return Snapshot(snapshot_id=sid, timestamp=datetime(2026, 1, 1, 0, 0, sid))


def _changeset(rows: int = 2) -> ChangeSet:
    return ChangeSet(
        pa.table({"id": list(range(rows)), "change_type": ["insert"] * rows}),
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
        # table_changes start bound is inclusive: query (last + 1, current)
        # so the baseline snapshot's changes are not redelivered.
        table.table_changes.assert_called_once_with(2, 2, columns=None, filter_expr=None)

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
        table.table_changes.assert_called_once_with(2, 2, columns=("a", "b"), filter_expr=None)

    def test_filter_expr_passed_through(self):
        table = _mock_table(_snapshot(1))
        tailer = Tailer(table, filter_expr="id > 5")
        tailer.poll()

        table.current_snapshot.return_value = _snapshot(2)
        tailer.poll()
        table.table_changes.assert_called_once_with(2, 2, columns=None, filter_expr="id > 5")

    def test_columns_and_filter_expr_passed_through(self):
        table = _mock_table(_snapshot(1))
        tailer = Tailer(table, columns=("x",), filter_expr="x > 0")
        tailer.poll()

        table.current_snapshot.return_value = _snapshot(2)
        tailer.poll()
        table.table_changes.assert_called_once_with(2, 2, columns=("x",), filter_expr="x > 0")

    def test_empty_changeset_returns_none_but_advances(self):
        """Catalog-wide snapshots: another table's commit yields an empty set."""
        table = _mock_table(_snapshot(1))
        table.table_changes.return_value = _changeset(rows=0)
        tailer = Tailer(table)
        tailer.poll()  # baseline

        table.current_snapshot.return_value = _snapshot(2)
        assert tailer.poll() is None
        assert tailer.last_snapshot_id == 2


class TestSnapshotWindow:
    def test_range_capped_per_poll(self):
        table = _mock_table(_snapshot(1))
        tailer = Tailer(table, max_snapshots_per_poll=3)
        tailer.poll()  # baseline at 1

        table.current_snapshot.return_value = _snapshot(10)
        tailer.poll()
        # (last + 1, min(current, last + window)) = (2, 4)
        table.table_changes.assert_called_once_with(2, 4, columns=None, filter_expr=None)
        assert tailer.last_snapshot_id == 4

    def test_window_catch_up_over_subsequent_polls(self):
        table = _mock_table(_snapshot(1))
        tailer = Tailer(table, max_snapshots_per_poll=3)
        tailer.poll()  # baseline at 1

        table.current_snapshot.return_value = _snapshot(10)
        tailer.poll()
        tailer.poll()
        assert table.table_changes.call_args_list[-1].args[:2] == (5, 7)
        tailer.poll()
        assert table.table_changes.call_args_list[-1].args[:2] == (8, 10)
        assert tailer.last_snapshot_id == 10

    def test_max_snapshots_per_poll_must_be_positive(self):
        table = _mock_table(_snapshot(1))
        with pytest.raises(ValueError, match="max_snapshots_per_poll"):
            Tailer(table, max_snapshots_per_poll=0)


class TestErrorHandling:
    def test_snapshot_not_advanced_when_table_changes_raises(self):
        """At-least-once: a failed poll must retry the same range next time."""
        table = _mock_table(_snapshot(1))
        tailer = Tailer(table, poll_interval=0)
        tailer.poll()  # baseline

        table.current_snapshot.return_value = _snapshot(2)
        table.table_changes.side_effect = RuntimeError("catalog gone")
        with pytest.raises(RuntimeError, match="catalog gone"):
            tailer.poll()
        assert tailer.last_snapshot_id == 1  # unchanged

        table.table_changes.side_effect = None
        assert tailer.poll() is not None
        table.table_changes.assert_called_with(2, 2, columns=None, filter_expr=None)

    def test_tail_retries_after_error(self):
        table = _mock_table(_snapshot(1))
        tailer = Tailer(table, poll_interval=0)
        tailer.poll()  # baseline

        table.current_snapshot.return_value = _snapshot(2)
        table.table_changes.side_effect = [RuntimeError("transient"), _changeset()]

        errors: list[Exception] = []
        gen = tailer.tail(on_error=errors.append)
        result = next(gen)
        assert result is not None
        assert len(errors) == 1
        assert isinstance(errors[0], RuntimeError)


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
