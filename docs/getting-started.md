# Getting Started

## Prerequisites

- Python 3.12+
- A DuckLake catalog (DuckDB file or Postgres metadata backend)

## Install

```bash
pip install ducktail
```

Or with uv:

```bash
uv add ducktail
```

## Your first tail

Point ducktail at any existing DuckLake table:

```bash
ducktail tail my_table -C meta.duckdb
```

The process blocks and polls for changes once per second. When rows are
inserted, deleted, or updated in the table, ducktail prints them:

```
+ id=1 name=Alice amount=100.0
+ id=2 name=Bob amount=50.0
- id=1 name=Alice amount=100.0
Δ amount: 50.0 → 75.0
```

Press `Ctrl+C` to stop.

## Connecting to different backends

### DuckDB metadata (local file)

```bash
ducktail tail events -C meta.duckdb -d /path/to/data
```

### Postgres metadata

```bash
ducktail tail events \
    -C "postgres:dbname=ducklake host=localhost user=ducklake password=ducklake" \
    -d /path/to/data
```

## What's next

- [Usage Guide](usage.md) — filtering, column projection, output modes
- [Interactive Mode](interactive.md) — the full-screen TUI
- [Demo](demo.md) — try ducktail with a live producer
