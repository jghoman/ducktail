"""Interactive Rich Live display for ducktail."""

from __future__ import annotations

import os
import time
from collections import deque
from datetime import UTC, datetime

from rich.console import Console
from rich.live import Live
from rich.table import Table

from ducktail.config import TailConfig
from ducktail.formatter import METADATA_COLS, format_row
from ducktail.tailer import Tailer


def _make_table(
    title: str,
    changes: deque[tuple[str, str, str, str]],
) -> Table:
    """Build a Rich Table from the current change buffer.

    Each entry in *changes* is (style, type_label, timestamp, details).
    """
    table = Table(title=title, expand=True)
    table.add_column("Type", width=4, no_wrap=True)
    table.add_column("Timestamp", width=26, no_wrap=True)
    table.add_column("Details", ratio=1)

    for style, type_label, ts, details in changes:
        table.add_row(type_label, ts, details, style=style)

    return table


def run_interactive(tailer: Tailer, config: TailConfig) -> None:
    """Run interactive TUI with Rich Live display."""
    title = f"Ducktail — {config.namespace}.{config.table_name}"
    columns: list[str] | None = list(config.columns) if config.columns else None

    # Size the rolling buffer to fit the terminal, leaving room for
    # the table header (title + column headers + borders = ~4 lines).
    term_rows = os.get_terminal_size().lines
    max_rows = max(term_rows - 4, 10)
    changes: deque[tuple[str, str, str, str]] = deque(maxlen=max_rows)

    # Establish baseline snapshot before entering the live loop.
    tailer.poll()

    console = Console()
    console.clear()

    try:
        with Live(_make_table(title, changes), console=console, refresh_per_second=4) as live:
            while True:
                changeset = tailer.poll()
                if changeset is not None:
                    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")

                    inserts = changeset.inserts()
                    if inserts.num_rows > 0:
                        user_cols = columns or [c for c in inserts.column_names if c not in METADATA_COLS]
                        for i in range(inserts.num_rows):
                            row = {col: inserts.column(col)[i].as_py() for col in user_cols}
                            changes.append(
                                (
                                    "green",
                                    "+",
                                    now,
                                    format_row(row, user_cols),
                                )
                            )

                    deletes = changeset.deletes()
                    if deletes.num_rows > 0:
                        user_cols = columns or [c for c in deletes.column_names if c not in METADATA_COLS]
                        for i in range(deletes.num_rows):
                            row = {col: deletes.column(col)[i].as_py() for col in user_cols}
                            changes.append(
                                (
                                    "red",
                                    "-",
                                    now,
                                    format_row(row, user_cols),
                                )
                            )

                    for pre, post in changeset.updates():
                        user_cols = columns or [k for k in pre if k not in METADATA_COLS]
                        changed = [(col, pre[col], post[col]) for col in user_cols if pre.get(col) != post.get(col)]
                        if changed:
                            parts = ", ".join(f"{col}: {old} \u2192 {new}" for col, old, new in changed)
                            changes.append(("yellow", "\u0394", now, parts))

                    live.update(_make_table(title, changes))

                time.sleep(tailer.poll_interval)
    except KeyboardInterrupt:
        pass
