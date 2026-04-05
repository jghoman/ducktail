# ducktail

`tail -f` for [DuckLake](https://ducklake.select) tables.

Watch inserts, deletes, and updates flow through a DuckLake table in real time,
powered by DuckLake's built-in CDC.

```
$ ducktail tail orders -C meta.duckdb

+ id=1 name=Alice amount=100.0
+ id=2 name=Bob amount=50.0
- id=2 name=Bob amount=50.0
Δ amount: 100.0 → 150.0
```

## Install

```bash
pip install ducktail        # or: uv add ducktail
```

Requires Python 3.12+ and DuckDB 1.4+ (ducklake extension loads automatically).

## Usage

### Basic

```bash
ducktail tail TABLE -C CONNECTION
```

```bash
# DuckDB metadata catalog
ducktail tail events -C meta.duckdb -d /data

# Postgres metadata catalog
ducktail tail events -C "postgres:dbname=ducklake host=localhost user=ducklake password=ducklake"
```

### Interactive mode

Full-screen TUI with color-coded rows (green inserts, red deletes, yellow updates):

```bash
ducktail tail events -C meta.duckdb -m interactive
```

### Filter what you see

```bash
# Only specific columns
ducktail tail events -C meta.duckdb -col status -col amount

# Server-side filter (pushed into the CDC query)
ducktail tail events -C meta.duckdb -f "amount > 100"

# Faster polling
ducktail tail events -C meta.duckdb -i 0.5
```

### Compose with Unix tools

Pager mode (the default) writes one line per change to stdout, so it plays
nicely with the usual suspects:

```bash
# Inserts only
ducktail tail orders -C meta.duckdb | grep '^\+'

# Deletes only
ducktail tail orders -C meta.duckdb | grep '^\-'

# Count changes per second
ducktail tail orders -C meta.duckdb | pv -l > /dev/null
```

## Options

```
ducktail tail [OPTIONS] TABLE_NAME
```

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `TABLE_NAME` | | *(required)* | Table to tail |
| `--connection` | `-C` | *(required)* | Catalog connection string |
| `--catalog` | `-c` | `lake` | Catalog name |
| `--data-path` | `-d` | `.` | Path to DuckLake data files |
| `--namespace` | `-n` | `main` | Table namespace |
| `--interval` | `-i` | `1.0` | Poll interval (seconds) |
| `--columns` | `-col` | *(all)* | Columns to show (repeat for multiple) |
| `--filter` | `-f` | *(none)* | CDC filter expression |
| `--mode` | `-m` | `pager` | `pager` or `interactive` |

## Change symbols

| Symbol | Color | Meaning |
|--------|-------|---------|
| `+` | Green | Row inserted |
| `-` | Red | Row deleted |
| `Δ` | Yellow | Row updated — shows `column: old → new` for each changed field |

## Try the demo

The repo includes a Docker-based demo that stands up a Postgres metadata catalog
and a producer that streams a mix of inserts, deletes, and updates.

```bash
# Terminal 1 — infrastructure + producer
just demo-up
just demo-producer

# Terminal 2 — watch it happen
just demo-tail                # pager mode
just demo-tail-interactive    # or interactive mode

# Cleanup
just demo-down
```

## Development

Requires [flox](https://flox.dev/) and [Docker](https://www.docker.com/) (for integration tests and demo).

```bash
flox activate
just sync       # install deps
just test       # unit tests
just ci         # format + lint + typecheck + unit tests
```

| Recipe | What it does |
|--------|-------------|
| `just sync` | Install dependencies |
| `just fmt` | Format code |
| `just lint` | Lint |
| `just typecheck` | mypy strict |
| `just test` | Unit tests |
| `just test-integration` | Integration tests |
| `just test-all` | Both |
| `just ci` | fmt-check + lint + typecheck + test |

## License

Apache License 2.0
