from __future__ import annotations

import pytest

from ducktail.config import TailConfig


def test_default_values():
    cfg = TailConfig(catalog_connection="meta.duckdb", table_name="events")
    assert cfg.catalog_connection == "meta.duckdb"
    assert cfg.table_name == "events"
    assert cfg.data_path == "."
    assert cfg.catalog_name == "lake"
    assert cfg.namespace == "main"
    assert cfg.poll_interval == 1.0
    assert cfg.columns is None
    assert cfg.filter_expr is None
    assert cfg.output_mode == "pager"


def test_custom_values():
    cfg = TailConfig(
        catalog_connection="postgres:dbname=mydb host=localhost",
        table_name="orders",
        data_path="/data",
        catalog_name="prod",
        namespace="sales",
        poll_interval=5.0,
        columns=("id", "amount"),
        filter_expr="amount > 100",
        output_mode="interactive",
    )
    assert cfg.catalog_connection == "postgres:dbname=mydb host=localhost"
    assert cfg.table_name == "orders"
    assert cfg.data_path == "/data"
    assert cfg.catalog_name == "prod"
    assert cfg.namespace == "sales"
    assert cfg.poll_interval == 5.0
    assert cfg.columns == ("id", "amount")
    assert cfg.filter_expr == "amount > 100"
    assert cfg.output_mode == "interactive"


def test_table_identifier():
    cfg = TailConfig(
        catalog_connection="meta.duckdb",
        table_name="events",
        namespace="analytics",
    )
    assert cfg.table_identifier == ("analytics", "events")


def test_frozen_immutability():
    cfg = TailConfig(catalog_connection="meta.duckdb", table_name="events")
    with pytest.raises(AttributeError):
        cfg.table_name = "other"
