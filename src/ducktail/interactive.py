"""Interactive Rich Live display for ducktail."""

from __future__ import annotations

import shutil
import signal
import sys
from collections import deque
from datetime import UTC, datetime
from types import FrameType

from rich.console import Console
from rich.live import Live
from rich.table import Table

from ducktail.config import TailConfig
from ducktail.formatter import KIND_PREFIX, KIND_STYLE, iter_change_events
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


def _utc_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _exit_on_sigterm(signum: int, frame: FrameType | None) -> None:
    # Raising SystemExit inside the Live context lets __exit__ restore the
    # terminal (cursor, alt-screen) before the process dies.
    sys.exit(0)


def run_interactive(tailer: Tailer, config: TailConfig) -> None:
    """Run interactive TUI with Rich Live display."""
    title = f"Ducktail — {config.namespace}.{config.table_name}"
    columns: list[str] | None = list(config.columns) if config.columns else None

    # Size the rolling buffer to fit the terminal, leaving room for
    # the table header (title + column headers + borders = ~4 lines).
    # shutil's fallback keeps this working when stdout is not a TTY.
    term_rows = shutil.get_terminal_size(fallback=(80, 24)).lines
    max_rows = max(term_rows - 4, 10)
    changes: deque[tuple[str, str, str, str]] = deque(maxlen=max_rows)

    signal.signal(signal.SIGTERM, _exit_on_sigterm)

    console = Console()
    console.clear()

    try:
        with Live(_make_table(title, changes), console=console, refresh_per_second=4) as live:

            def _on_error(exc: Exception) -> None:
                changes.append(("bold red", "!", _utc_now(), f"poll error: {exc} (retrying)"))
                live.update(_make_table(title, changes))

            for changeset in tailer.tail(on_error=_on_error):
                now = _utc_now()
                for kind, text in iter_change_events(changeset, columns):
                    changes.append((KIND_STYLE[kind], KIND_PREFIX[kind], now, text))
                live.update(_make_table(title, changes))
    except KeyboardInterrupt:
        pass
