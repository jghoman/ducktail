from __future__ import annotations

from typing import Any

from pyducklake.cdc import ChangeSet

METADATA_COLS = {"snapshot_id", "rowid", "change_type"}


def format_row(row: dict[str, Any], columns: list[str] | None = None) -> str:
    """Format a single row dict as col1=val1 col2=val2 ..."""
    keys = columns if columns is not None else [k for k in row if k not in METADATA_COLS]
    return " ".join(f"{k}={row[k]}" for k in keys if k in row)


def format_changeset(changeset: ChangeSet, columns: list[str] | None = None) -> list[str]:
    """Render a ChangeSet into text lines for terminal output."""
    lines: list[str] = []

    inserts = changeset.inserts()
    if inserts.num_rows > 0:
        user_cols = columns or [c for c in inserts.column_names if c not in METADATA_COLS]
        for i in range(inserts.num_rows):
            row = {col: inserts.column(col)[i].as_py() for col in user_cols}
            lines.append(f"+ {format_row(row, user_cols)}")

    deletes = changeset.deletes()
    if deletes.num_rows > 0:
        user_cols = columns or [c for c in deletes.column_names if c not in METADATA_COLS]
        for i in range(deletes.num_rows):
            row = {col: deletes.column(col)[i].as_py() for col in user_cols}
            lines.append(f"- {format_row(row, user_cols)}")

    updates = changeset.updates()
    for pre, post in updates:
        user_cols = columns or [k for k in pre if k not in METADATA_COLS]
        changed = [(col, pre[col], post[col]) for col in user_cols if pre.get(col) != post.get(col)]
        if changed:
            parts = ", ".join(f"{col}: {old} \u2192 {new}" for col, old, new in changed)
            lines.append(f"\u0394 {parts}")

    return lines
