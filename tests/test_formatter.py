from __future__ import annotations

import pyarrow as pa
from pyducklake.cdc import ChangeSet

from ducktail.formatter import format_changeset, format_row, iter_change_events


def _make_changeset(snapshot_ids, rowids, change_types, **user_columns) -> ChangeSet:
    data = {
        "snapshot_id": snapshot_ids,
        "rowid": rowids,
        "change_type": change_types,
        **user_columns,
    }
    return ChangeSet(pa.table(data))


class TestFormatRow:
    def test_basic(self):
        row = {"id": 1, "name": "alice"}
        assert format_row(row) == "id=1 name='alice'"

    def test_excludes_metadata(self):
        row = {"snapshot_id": 1, "rowid": 0, "change_type": "insert", "id": 1}
        assert format_row(row) == "id=1"

    def test_column_projection(self):
        row = {"id": 1, "name": "alice", "age": 30}
        assert format_row(row, columns=["name"]) == "name='alice'"

    def test_none_value(self):
        row = {"id": 1, "name": None}
        assert format_row(row) == "id=1 name=None"

    def test_value_with_newline_is_escaped(self):
        row = {"id": 1, "name": "a\nb"}
        line = format_row(row)
        assert "\n" not in line
        assert line == "id=1 name='a\\nb'"

    def test_string_with_spaces_is_quoted(self):
        row = {"id": 1, "name": "hello world"}
        assert format_row(row) == "id=1 name='hello world'"


class TestFormatInserts:
    def test_single_insert(self):
        cs = _make_changeset([1], [0], ["insert"], id=[1], name=["alice"])
        lines = format_changeset(cs)
        assert lines == ["+ id=1 name='alice'"]

    def test_multiple_inserts(self):
        cs = _make_changeset(
            [1, 1],
            [0, 1],
            ["insert", "insert"],
            id=[1, 2],
            name=["alice", "bob"],
        )
        lines = format_changeset(cs)
        assert len(lines) == 2
        assert lines[0] == "+ id=1 name='alice'"
        assert lines[1] == "+ id=2 name='bob'"


class TestFormatDeletes:
    def test_single_delete(self):
        cs = _make_changeset([1], [0], ["delete"], id=[2], name=["bob"])
        lines = format_changeset(cs)
        assert lines == ["- id=2 name='bob'"]


class TestFormatUpdates:
    def test_single_update(self):
        cs = _make_changeset(
            [1, 1],
            [0, 0],
            ["update_preimage", "update_postimage"],
            id=[1, 1],
            name=["alice", "alicia"],
        )
        lines = format_changeset(cs)
        assert len(lines) == 1
        assert lines[0] == "Δ name: 'alice' → 'alicia'"

    def test_update_multiple_changed_cols(self):
        cs = _make_changeset(
            [1, 1],
            [0, 0],
            ["update_preimage", "update_postimage"],
            id=[1, 1],
            name=["alice", "alicia"],
            age=[30, 31],
        )
        lines = format_changeset(cs)
        assert len(lines) == 1
        assert "name: 'alice' → 'alicia'" in lines[0]
        assert "age: 30 → 31" in lines[0]
        assert lines[0].startswith("Δ ")

    def test_double_update_same_rowid_pairs_by_snapshot(self):
        """A row updated twice in one window yields one delta per snapshot.

        Mirrors the real CDC row order observed from ducklake_table_changes:
        all postimages first, then preimages. Pairing must use
        (rowid, snapshot_id) — pairing by rowid alone mis-pairs.
        """
        cs = _make_changeset(
            [6, 7, 6, 7],
            [0, 0, 0, 0],
            ["update_postimage", "update_postimage", "update_preimage", "update_preimage"],
            id=[1, 1, 1, 1],
            v=[200, 300, 100, 200],
        )
        events = list(iter_change_events(cs))
        assert events == [
            ("update", "v: 100 → 200"),
            ("update", "v: 200 → 300"),
        ]


class TestColumnProjection:
    def test_inserts_with_columns(self):
        cs = _make_changeset([1], [0], ["insert"], id=[1], name=["alice"], age=[30])
        lines = format_changeset(cs, columns=["name"])
        assert lines == ["+ name='alice'"]

    def test_updates_with_columns(self):
        cs = _make_changeset(
            [1, 1],
            [0, 0],
            ["update_preimage", "update_postimage"],
            id=[1, 1],
            name=["alice", "alicia"],
            age=[30, 31],
        )
        lines = format_changeset(cs, columns=["name"])
        assert lines == ["Δ name: 'alice' → 'alicia'"]


class TestEmptyChangeset:
    def test_empty(self):
        cs = _make_changeset([], [], [])
        lines = format_changeset(cs)
        assert lines == []


class TestMixedChangeset:
    def test_inserts_and_deletes(self):
        cs = _make_changeset(
            [1, 1],
            [0, 1],
            ["insert", "delete"],
            id=[1, 2],
            name=["alice", "bob"],
        )
        lines = format_changeset(cs)
        assert len(lines) == 2
        assert lines[0].startswith("+ ")
        assert lines[1].startswith("- ")
