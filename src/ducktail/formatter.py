from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Literal

from pyducklake.cdc import ChangeSet

METADATA_COLS = {"snapshot_id", "rowid", "change_type"}

ChangeKind = Literal["insert", "delete", "update"]

KIND_PREFIX: dict[ChangeKind, str] = {"insert": "+", "delete": "-", "update": "Δ"}
KIND_STYLE: dict[ChangeKind, str] = {"insert": "green", "delete": "red", "update": "yellow"}


def _fmt_value(value: Any) -> str:
    """Render a value unambiguously: quoted strings, escaped newlines, visible None."""
    return repr(value)


def format_row(row: dict[str, Any], columns: list[str] | None = None) -> str:
    """Format a single row dict as col1=val1 col2=val2 ..."""
    keys = columns if columns is not None else [k for k in row if k not in METADATA_COLS]
    return " ".join(f"{k}={_fmt_value(row[k])}" for k in keys if k in row)


def _iter_rows(table: Any, columns: list[str] | None) -> Iterator[dict[str, Any]]:
    """Yield row dicts from an Arrow table, restricted to display columns."""
    cols = columns if columns is not None else [c for c in table.column_names if c not in METADATA_COLS]
    yield from table.select(cols).to_pylist()


def iter_change_events(changeset: ChangeSet, columns: list[str] | None = None) -> Iterator[tuple[ChangeKind, str]]:
    """Yield (kind, detail) for each change in the set, in CDC order."""
    for row in _iter_rows(changeset.inserts(), columns):
        yield "insert", format_row(row, columns)
    for row in _iter_rows(changeset.deletes(), columns):
        yield "delete", format_row(row, columns)

    # Pair update pre/post images by (rowid, snapshot_id), not rowid alone:
    # a row updated twice within one window produces one pair per snapshot.
    # (pyducklake's ChangeSet.updates() pairs by rowid and mis-pairs doubles.)
    pre = changeset.update_preimages()
    post = changeset.update_postimages()
    if pre.num_rows == 0 or post.num_rows == 0:
        return
    post_by_key = {(row["rowid"], row["snapshot_id"]): row for row in post.to_pylist()}
    for pre_row in pre.to_pylist():
        post_row = post_by_key.get((pre_row["rowid"], pre_row["snapshot_id"]))
        if post_row is None:
            continue
        cols = columns if columns is not None else [k for k in pre_row if k not in METADATA_COLS]
        changed: list[tuple[str, Any, Any]] = []
        for col in cols:
            if col in pre_row and pre_row[col] != post_row.get(col):
                changed.append((col, pre_row[col], post_row.get(col)))
        if changed:
            parts = ", ".join(f"{col}: {_fmt_value(old)} → {_fmt_value(new)}" for col, old, new in changed)
            yield "update", parts


def format_changeset(changeset: ChangeSet, columns: list[str] | None = None) -> list[str]:
    """Render a ChangeSet into text lines for terminal output."""
    return [f"{KIND_PREFIX[kind]} {text}" for kind, text in iter_change_events(changeset, columns)]
