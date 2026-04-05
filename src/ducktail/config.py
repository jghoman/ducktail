from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class TailConfig:
    catalog_connection: str
    table_name: str
    data_path: str = "."
    catalog_name: str = "lake"
    namespace: str = "main"
    poll_interval: float = 1.0
    columns: tuple[str, ...] | None = None
    filter_expr: str | None = None
    output_mode: Literal["pager", "interactive"] = "pager"

    @property
    def table_identifier(self) -> tuple[str, str]:
        return (self.namespace, self.table_name)
