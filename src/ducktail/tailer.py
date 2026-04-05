from __future__ import annotations

import time
from collections.abc import Iterator

from pyducklake import Table
from pyducklake.cdc import ChangeSet
from pyducklake.snapshot import Snapshot


class Tailer:
    """CDC tailer that polls a DuckLake table for changes."""

    def __init__(
        self,
        table: Table,
        poll_interval: float = 1.0,
        columns: tuple[str, ...] | None = None,
        filter_expr: str | None = None,
    ) -> None:
        self._table = table
        self._poll_interval = poll_interval
        self._columns = columns
        self._filter_expr = filter_expr
        self._last_snapshot_id: int | None = None
        self._initialized: bool = False

    @property
    def last_snapshot_id(self) -> int | None:
        return self._last_snapshot_id

    def poll(self) -> ChangeSet | None:
        """Check for new snapshot and return changes if found.

        On first call, records the current snapshot as baseline and returns None.
        On subsequent calls, returns a ChangeSet if the snapshot has advanced.
        """
        snapshot: Snapshot | None = self._table.current_snapshot()

        if not self._initialized:
            self._initialized = True
            self._last_snapshot_id = snapshot.snapshot_id if snapshot else None
            return None

        if snapshot is None or self._last_snapshot_id is None:
            if snapshot is not None and self._last_snapshot_id is None:
                self._last_snapshot_id = snapshot.snapshot_id
            return None

        if snapshot.snapshot_id == self._last_snapshot_id:
            return None

        changeset = self._table.table_changes(
            self._last_snapshot_id,
            snapshot.snapshot_id,
            columns=self._columns,
            filter_expr=self._filter_expr,
        )
        self._last_snapshot_id = snapshot.snapshot_id
        return changeset

    def tail(self) -> Iterator[ChangeSet]:
        """Infinite generator that yields ChangeSets when changes occur."""
        while True:
            changeset = self.poll()
            if changeset is not None:
                yield changeset
            time.sleep(self._poll_interval)
