from __future__ import annotations

import sys

import click
from pyducklake import Catalog

from ducktail.config import TailConfig
from ducktail.formatter import format_changeset
from ducktail.tailer import Tailer


@click.group()
@click.version_option()
def cli() -> None:
    """Ducktail — Tail DuckLake tables via CDC."""


@cli.command()
@click.argument("table_name")
@click.option("--catalog", "-c", default="lake", help="Catalog name.")
@click.option("--connection", "-C", required=True, help="Catalog connection string (e.g. meta.duckdb, postgres:...).")
@click.option("--data-path", "-d", default=".", help="Data path for DuckLake files.")
@click.option("--namespace", "-n", default="main", help="Table namespace.")
@click.option("--interval", "-i", default=1.0, type=float, help="Poll interval in seconds.")
@click.option("--columns", "-col", multiple=True, help="Columns to display (repeat for multiple).")
@click.option("--filter", "-f", "filter_expr", default=None, help="Filter expression.")
@click.option("--mode", "-m", type=click.Choice(["pager", "interactive"]), default="pager", help="Output mode.")
def tail(
    table_name: str,
    catalog: str,
    connection: str,
    data_path: str,
    namespace: str,
    interval: float,
    columns: tuple[str, ...],
    filter_expr: str | None,
    mode: str,
) -> None:
    """Tail a DuckLake table for changes."""
    config = TailConfig(
        catalog_connection=connection,
        data_path=data_path,
        catalog_name=catalog,
        namespace=namespace,
        table_name=table_name,
        poll_interval=interval,
        columns=columns if columns else None,
        filter_expr=filter_expr,
        output_mode=mode,  # type: ignore[arg-type]
    )

    cat = Catalog(config.catalog_name, config.catalog_connection, data_path=config.data_path)
    tbl = cat.load_table((config.namespace, config.table_name))

    tailer = Tailer(
        table=tbl,
        poll_interval=config.poll_interval,
        columns=config.columns,
        filter_expr=config.filter_expr,
    )

    if config.output_mode == "interactive":
        _tail_interactive(tailer, config)
    else:
        _tail_pager(tailer, config)


def _tail_pager(tailer: Tailer, config: TailConfig) -> None:
    """Stream formatted changes to stdout."""
    click.echo(f"Tailing {config.namespace}.{config.table_name} (Ctrl+C to stop)", err=True)
    try:
        for changeset in tailer.tail():
            user_cols = list(config.columns) if config.columns else None
            lines = format_changeset(changeset, columns=user_cols)
            for line in lines:
                click.echo(line)
            sys.stdout.flush()
    except KeyboardInterrupt:
        click.echo("\nStopped.", err=True)


def _tail_interactive(tailer: Tailer, config: TailConfig) -> None:
    """Interactive Rich Live display of changes."""
    from ducktail.interactive import run_interactive

    run_interactive(tailer, config)
