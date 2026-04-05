# Usage Guide

## Command syntax

```
ducktail tail [OPTIONS] TABLE_NAME
```

`TABLE_NAME` is a positional argument — the name of the DuckLake table to tail.

## Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--connection` | `-C` | *(required)* | Catalog connection string |
| `--catalog` | `-c` | `lake` | Catalog name |
| `--data-path` | `-d` | `.` | Path to DuckLake data files |
| `--namespace` | `-n` | `main` | Table namespace |
| `--interval` | `-i` | `1.0` | Poll interval (seconds) |
| `--columns` | `-col` | *(all)* | Columns to show (repeat for multiple) |
| `--filter` | `-f` | *(none)* | CDC filter expression |
| `--mode` | `-m` | `pager` | `pager` or `interactive` |

## Output format

Each line represents one change, prefixed by a symbol:

| Symbol | Meaning |
|--------|---------|
| `+` | Row inserted |
| `-` | Row deleted |
| `Δ` | Row updated — shows `column: old → new` for each changed field |

Inserts and deletes show all user columns as `key=value` pairs:

```
+ id=1 name=Alice status=active amount=100.0
- id=3 name=Carol status=inactive amount=0.0
```

Updates show only the columns that changed:

```
Δ status: active → inactive, amount: 100.0 → 0.0
```

## Column projection

Use `-col` to limit which columns are displayed. Repeat for multiple columns:

```bash
ducktail tail orders -C meta.duckdb -col id -col status -col amount
```

Output will only include those columns:

```
+ id=1 status=active amount=100.0
- id=3 status=inactive amount=0.0
Δ status: active → inactive
```

Column projection is applied at the CDC query level, so it also reduces the
amount of data read from disk.

## Server-side filtering

Use `-f` to push a filter expression into the CDC query:

```bash
ducktail tail orders -C meta.duckdb -f "amount > 100"
```

Only changes to rows matching the filter are returned. The filter uses DuckDB
SQL expression syntax.

## Poll interval

By default, ducktail polls once per second. Adjust with `-i`:

```bash
# Poll every 200ms (high frequency)
ducktail tail events -C meta.duckdb -i 0.2

# Poll every 10 seconds (low overhead)
ducktail tail events -C meta.duckdb -i 10
```

## Composing with Unix tools

Pager mode writes to stdout, so standard Unix pipelines work:

```bash
# Inserts only
ducktail tail orders -C meta.duckdb | grep '^\+'

# Deletes only
ducktail tail orders -C meta.duckdb | grep '^\-'

# Count changes per second
ducktail tail orders -C meta.duckdb | pv -l > /dev/null

# Log to a file while watching
ducktail tail orders -C meta.duckdb | tee changes.log

# Timestamp each line
ducktail tail orders -C meta.duckdb | ts '%Y-%m-%d %H:%M:%S'
```

## How it works

Ducktail uses DuckLake's CDC (Change Data Capture) facility. On each poll cycle:

1. Query the table's current snapshot ID
2. If it has advanced since the last poll, call `table_changes(last_snapshot, current_snapshot)`
3. The returned `ChangeSet` contains inserts, deletes, and update pre/post images
4. Format and emit each change

The first poll establishes a baseline snapshot — no changes are emitted. Only
changes occurring *after* ducktail starts are shown.
