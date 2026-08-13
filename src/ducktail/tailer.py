from __future__ import annotations

import time
from collections.abc import Callable, Iterator

from pyducklake import Table
from pyducklake.cdc import ChangeSet
from pyducklake.snapshot import Snapshot

DEFAULT_MAX_SNAPSHOTS_PER_POLL = 100


class Tailer:
    """CDC tailer that polls a DuckLake table for changes."""

    def __init__(
        self,
        table: Table,
        poll_interval: float = 1.0,
        columns: tuple[str, ...] | None = None,
        filter_expr: str | None = None,
        max_snapshots_per_poll: int = DEFAULT_MAX_SNAPSHOTS_PER_POLL,
    ) -> None:
        if max_snapshots_per_poll < 1:
            raise ValueError("max_snapshots_per_poll must be >= 1")
        self._table = table
        self._poll_interval = poll_interval
        self._columns = columns
        self._filter_expr = filter_expr
        self._max_snapshots_per_poll = max_snapshots_per_poll
        self._last_snapshot_id: int | None = None
        self._initialized: bool = False

    @property
    def poll_interval(self) -> float:
        return self._poll_interval

    @property
    def last_snapshot_id(self) -> int | None:
        return self._last_snapshot_id

    def poll(self) -> ChangeSet | None:
        """Check for new snapshots and return changes if found.

        On first call, records the current snapshot as baseline and returns None.
        On subsequent calls, returns a ChangeSet if the snapshot advanced and the
        table actually changed. DuckLake snapshots are catalog-wide, so an
        advanced snapshot does not imply this table changed; empty results are
        consumed silently.

        ``table_changes`` treats its start bound as inclusive, so each poll
        queries ``(last + 1, end)`` to avoid redelivering the previously
        processed snapshot's changes. The range is capped at
        ``max_snapshots_per_poll`` so a long catch-up cannot produce one huge
        changeset; the remainder is picked up on subsequent polls.
        """
        snapshot: Snapshot | None = self._table.current_snapshot()

        if not self._initialized:
            self._initialized = True
            self._last_snapshot_id = snapshot.snapshot_id if snapshot else None
            return None

        if snapshot is None:
            return None
        if self._last_snapshot_id is None:
            # No baseline was ever recorded (the catalog had no snapshots at
            # first poll). Baseline now rather than replaying unknown history.
            self._last_snapshot_id = snapshot.snapshot_id
            return None

        if snapshot.snapshot_id == self._last_snapshot_id:
            return None

        start = self._last_snapshot_id + 1
        end = min(snapshot.snapshot_id, start + self._max_snapshots_per_poll - 1)

        changeset = self._table.table_changes(
            start,
            end,
            columns=self._columns,
            filter_expr=self._filter_expr,
        )
        self._last_snapshot_id = end
        if changeset.num_rows == 0:
            return None
        return changeset

    def tail(self, on_error: Callable[[Exception], None] | None = None) -> Iterator[ChangeSet]:
        """Infinite generator that yields ChangeSets when changes occur.

        Poll errors are reported via *on_error* and retried after the poll
        interval instead of killing the tail.
        """
        while True:
            try:
                changeset = self.poll()
            except Exception as exc:
                if on_error is not None:
                    on_error(exc)
            else:
                if changeset is not None:
                    yield changeset
            time.sleep(self._poll_interval)
